//go:build integration

package data

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/notify/internal/biz"
	"github.com/lanyulei/reborn/backend/notify/internal/data/channel"

	natsserver "github.com/nats-io/nats-server/v2/server"
)

// startNats 内嵌 JetStream 服务器（临时存储目录，随机端口）。
func startNats(t *testing.T) *NatsBackbone {
	t.Helper()
	srv, err := natsserver.NewServer(&natsserver.Options{Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("embedded nats-server not ready")
	}
	t.Cleanup(srv.Shutdown)
	nb, cleanup, err := ConnectNats(context.Background(), NatsOptions{
		URL: srv.ClientURL(), StreamPrefix: "T" + time.Now().Format("150405"), AckWait: 2 * time.Second, MaxDeliver: 3,
	})
	if err != nil {
		t.Fatalf("ConnectNats() error = %v", err)
	}
	t.Cleanup(cleanup)
	return nb
}

// natsPipeline NATS 骨干装配：relay + router(nats source) + sender(nats dispatch repo)。
type natsPipeline struct {
	relay  biz.Relayer
	router *biz.RouterUsecase
	sender *biz.SenderUsecase
}

func newNatsPipeline(t *testing.T, d *Data, nb *NatsBackbone, adapter biz.ChannelSender) *natsPipeline {
	t.Helper()
	// 路由只绑 webhook 渠道：每事件恰 1 个 dispatch，断言按条计数。
	if _, err := d.Pool.Exec(context.Background(), "UPDATE notify_route SET channel_ids = $1::uuid[] WHERE id = $2", []string{idChanHook}, idRoute); err != nil {
		t.Fatal(err)
	}
	cache := newCache(t, d)
	events := &natsEventSource{data: d, nb: nb, poll: 300 * time.Millisecond}
	dispatches := &natsDispatchRepo{dispatchRepo: NewDispatchRepo(d), nb: nb, poll: 300 * time.Millisecond, inflight: map[string]natsMsg{}}
	return &natsPipeline{
		relay:  &natsRelay{data: d, nb: nb, log: testLogger(), staleAfter: 2 * nb.opts.AckWait},
		router: biz.NewRouterUsecase(events, cache, testLogger()),
		sender: biz.NewSenderUsecase(dispatches, cache, NewRateBucketRepo(d), adapter, biz.NewRenderer(), testLogger()).
			WithOptions(biz.SenderOptions{Lease: 3 * time.Second, SendTimeout: 2 * time.Second, Concurrency: 8}),
	}
}

// drain 反复驱动一步直到连续两轮空闲或超时。
func drain(t *testing.T, step func(context.Context) (int, error), timeout time.Duration) int {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	total, idle := 0, 0
	for time.Now().Before(deadline) && idle < 2 {
		n, err := step(ctx)
		if err != nil {
			t.Fatal(err)
		}
		total += n
		if n == 0 {
			idle++
		} else {
			idle = 0
		}
	}
	return total
}

// TestIntegration_NatsEndToEnd relay → router(nats) → sender(nats) → webhook 收到签名请求；
// 重投幂等（同一事件游标投两次只路由一次）；不可重试失败死信后 ack。
func TestIntegration_NatsEndToEnd(t *testing.T) {
	d := newIntegrationData(t)
	nb := startNats(t)
	hook := newHookServer(t)
	seedConfig(t, d, hook.URL+"/hook")
	ctx := context.Background()
	p := newNatsPipeline(t, d, nb, channel.NewWebhookSender().WithInsecure())

	const ev = "018f3e1a-2201-7000-8000-000000000001"
	seedEvent(t, d, ev, "monitor.alert.firing", 2, "", nil)
	// 手工重复发布同一游标（模拟 relay 重复/重投）：路由只发生一次。
	if n, err := p.relay.RelayOnce(ctx, 100); err != nil || n != 1 {
		t.Fatalf("RelayOnce = %d, %v (want 1 event published)", n, err)
	}
	if count(t, d, "SELECT count(*) FROM notify_event WHERE id = $1 AND publish_state = 1 AND status = 1", ev) != 1 {
		t.Fatal("relay must mark event published without touching status")
	}
	f, err := (&natsRelay{nb: nb}).publish(ctx, "notify.event.default", cursor{ID: ev}, ev+":dup")
	if err != nil || awaitPub(ctx, f) != nil {
		t.Fatalf("duplicate publish: %v", err)
	}
	routed := drain(t, func(ctx context.Context) (int, error) { return p.router.RouteOnce(ctx, 100) }, 10*time.Second)
	if routed != 1 || count(t, d, "SELECT count(*) FROM notify_event WHERE id = $1 AND status = 2", ev) != 1 {
		t.Fatalf("routed = %d (want exactly 1 despite duplicate message)", routed)
	}
	if count(t, d, "SELECT count(*) FROM notify_dispatch WHERE event_id = $1 AND status = 1 AND publish_state = 0", ev) != 1 {
		t.Fatal("router must insert an unpublished pending dispatch")
	}
	if n, err := p.relay.RelayOnce(ctx, 100); err != nil || n != 1 {
		t.Fatalf("RelayOnce(dispatch) = %d, %v", n, err)
	}
	sent := drain(t, func(ctx context.Context) (int, error) { return p.sender.SendOnce(ctx, 100) }, 10*time.Second)
	if sent != 1 || count(t, d, "SELECT count(*) FROM notify_dispatch WHERE event_id = $1 AND status = 3", ev) != 1 {
		t.Fatalf("sent = %d, dispatch not sent", sent)
	}
	calls := hook.calls()
	if len(calls) != 1 || calls[0].body["event_id"] != ev {
		t.Fatalf("webhook calls = %d", len(calls))
	}
	// 消息已 ack：再拉不到任何任务游标。
	if n, err := p.sender.SendOnce(ctx, 100); err != nil || n != 0 {
		t.Fatalf("acked dispatch must not be redelivered: %d %v", n, err)
	}

	// 不可重试失败（400）→ 死信并 ack。
	hook.status.Store(http.StatusBadRequest)
	const ev2 = "018f3e1a-2201-7000-8000-000000000002"
	seedEvent(t, d, ev2, "monitor.alert.firing", 2, "", nil)
	_, _ = p.relay.RelayOnce(ctx, 100)
	drain(t, func(ctx context.Context) (int, error) { return p.router.RouteOnce(ctx, 100) }, 10*time.Second)
	_, _ = p.relay.RelayOnce(ctx, 100)
	drain(t, func(ctx context.Context) (int, error) { return p.sender.SendOnce(ctx, 100) }, 10*time.Second)
	if count(t, d, "SELECT count(*) FROM notify_dispatch WHERE event_id = $1 AND status = 5", ev2) != 1 ||
		count(t, d, "SELECT count(*) FROM notify_dlq WHERE event_id = $1", ev2) != 1 {
		t.Fatal("non-retryable failure must dead-letter under nats backbone")
	}
}

// TestIntegration_NatsRetryAndCrashRecovery 可重试失败按退避 Nak 后重投成功；sender 崩溃
// （声明后不落账）→ 租约回收 → 消息重投 → 再次声明成功；超期（消息丢失）行由 relay 回拨重发。
func TestIntegration_NatsRetryAndCrashRecovery(t *testing.T) {
	d := newIntegrationData(t)
	nb := startNats(t)
	hook := newHookServer(t)
	seedConfig(t, d, hook.URL+"/hook")
	ctx := context.Background()
	p := newNatsPipeline(t, d, nb, channel.NewWebhookSender().WithInsecure())

	// 可重试失败：503 → status 4 + Nak(退避)；退避表最小 30s，这里把 next_retry_time 拨回并
	// 让消息立即重投（AckWait 2s 内 Nak 的延迟由 PG 的到期时间决定，提前到达会被 settleMissed 延后）。
	hook.status.Store(http.StatusServiceUnavailable)
	const ev = "018f3e1a-2202-7000-8000-000000000001"
	seedEvent(t, d, ev, "monitor.alert.firing", 2, "", nil)
	_, _ = p.relay.RelayOnce(ctx, 100)
	drain(t, func(ctx context.Context) (int, error) { return p.router.RouteOnce(ctx, 100) }, 10*time.Second)
	_, _ = p.relay.RelayOnce(ctx, 100)
	drain(t, func(ctx context.Context) (int, error) { return p.sender.SendOnce(ctx, 100) }, 10*time.Second)
	if count(t, d, "SELECT count(*) FROM notify_dispatch WHERE event_id = $1 AND status = 4 AND attempts = 1", ev) != 1 {
		t.Fatal("retryable failure must land in failed with next_retry_time")
	}
	hook.status.Store(http.StatusOK)
	// 把退避拨回到期并回拨发布态：等价于"消息丢失后 relay 超期重发"。
	_, _ = d.Pool.Exec(ctx, "UPDATE notify_dispatch SET next_retry_time = now() - interval '10 second', publish_state = 1 WHERE event_id = $1", ev)
	relay := &natsRelay{data: d, nb: nb, log: testLogger(), staleAfter: time.Second}
	if n, err := relay.requeueStale(ctx, 100); err != nil || n != 1 {
		t.Fatalf("requeueStale = %d, %v", n, err)
	}
	if n, err := relay.RelayOnce(ctx, 100); err != nil || n < 1 {
		t.Fatalf("republish = %d, %v", n, err)
	}
	drain(t, func(ctx context.Context) (int, error) { return p.sender.SendOnce(ctx, 100) }, 10*time.Second)
	if count(t, d, "SELECT count(*) FROM notify_dispatch WHERE event_id = $1 AND status = 3 AND attempts = 2", ev) != 1 {
		t.Fatal("retry must succeed on second attempt")
	}

	// 崩溃模拟：另一个 sender 声明后不落账（消息未 ack、PG 行 sending）；租约 3s 过期 →
	// 回收器转 failed → AckWait 2s 后消息重投 → 正常 sender 声明并发送成功。
	const ev2 = "018f3e1a-2202-7000-8000-000000000002"
	seedEvent(t, d, ev2, "monitor.alert.firing", 2, "", nil)
	_, _ = p.relay.RelayOnce(ctx, 100)
	drain(t, func(ctx context.Context) (int, error) { return p.router.RouteOnce(ctx, 100) }, 10*time.Second)
	_, _ = p.relay.RelayOnce(ctx, 100)
	crashed := &natsDispatchRepo{dispatchRepo: NewDispatchRepo(d), nb: nb, poll: time.Second, inflight: map[string]natsMsg{}}
	claimed, err := crashed.Claim(ctx, 10, 3*time.Second, nil)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("crashed claim = %d, %v", len(claimed), err)
	}
	time.Sleep(3500 * time.Millisecond) // 租约与 AckWait 均过期
	if n, err := p.sender.ReapOnce(ctx, 100); err != nil || n != 1 {
		t.Fatalf("ReapOnce = %d, %v", n, err)
	}
	if count(t, d, "SELECT count(*) FROM notify_dispatch WHERE event_id = $1 AND status = 4 AND publish_state = 1", ev2) != 1 {
		t.Fatal("reaped lease must become failed (publish_state untouched: the unacked message will be redelivered)")
	}
	// 回收后的退避（≥30s）在测试里拨回到期；AckWait 已过，原消息重投给正常 sender。
	_, _ = d.Pool.Exec(ctx, "UPDATE notify_dispatch SET next_retry_time = now() WHERE event_id = $1", ev2)
	drain(t, func(ctx context.Context) (int, error) { return p.sender.SendOnce(ctx, 100) }, 15*time.Second)
	if count(t, d, "SELECT count(*) FROM notify_dispatch WHERE event_id = $1 AND status = 3", ev2) != 1 {
		t.Fatal("dispatch of a crashed sender must be redelivered and sent")
	}
	// 崩溃任务只真正投递一次（第一个事件的 503 → 重试属同 send_key 的合法二次请求）。
	n := 0
	for _, c := range hook.calls() {
		if c.body["event_id"] == ev2 {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("crashed dispatch delivered %d times, want 1", n)
	}
}

// TestIntegration_NatsLoadReconciliation 1 万事件：两 router + 两 sender 并发消费，对账
// publish_state / status / send_key 唯一。
func TestIntegration_NatsLoadReconciliation(t *testing.T) {
	if testing.Short() {
		t.Skip("load test skipped in -short")
	}
	d := newIntegrationData(t)
	nb := startNats(t)
	seedConfig(t, d, "http://127.0.0.1:9/hook")
	ctx := context.Background()
	_, _ = d.Pool.Exec(ctx, "UPDATE notify_channel SET rate_limit = '{\"qps\":0,\"burst\":0}'")
	adapter := &countingSender{keys: map[string]int{}}
	const total = 5000
	ingress := NewIngressRepo(d)
	for b := 0; b < total/500; b++ {
		events := make([]*biz.Event, 0, 500)
		for i := 0; i < 500; i++ {
			events = append(events, &biz.Event{BizID: "default", EventType: "monitor.alert.firing", Severity: 2, Title: "load", Source: biz.EventSourceAPI})
		}
		if _, err := biz.NewIngressUsecase(ingress).Accept(ctx, &biz.Producer{BizID: "default"}, events); err != nil {
			t.Fatal(err)
		}
	}
	p1, p2 := newNatsPipeline(t, d, nb, adapter), newNatsPipeline(t, d, nb, adapter)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	run := func(step func(context.Context) (int, error)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := step(ctx); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	run(func(ctx context.Context) (int, error) { return p1.relay.RelayOnce(ctx, 500) })
	run(func(ctx context.Context) (int, error) { return p1.router.RouteOnce(ctx, 500) })
	run(func(ctx context.Context) (int, error) { return p2.router.RouteOnce(ctx, 500) })
	run(func(ctx context.Context) (int, error) { return p1.sender.SendOnce(ctx, 100) })
	run(func(ctx context.Context) (int, error) { return p2.sender.SendOnce(ctx, 100) })
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if count(t, d, "SELECT count(*) FROM notify_dispatch WHERE status = 3") == total {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	if count(t, d, "SELECT count(*) FROM notify_event WHERE publish_state = 0") != 0 || count(t, d, "SELECT count(*) FROM notify_event WHERE status = 2") != total {
		t.Fatal("not all events routed")
	}
	if count(t, d, "SELECT count(*) FROM notify_dispatch WHERE status <> 3") != 0 || count(t, d, "SELECT count(*) FROM notify_dispatch WHERE publish_state = 0") != 0 {
		t.Fatalf("dispatch terminal states: non-sent=%d unpublished=%d",
			count(t, d, "SELECT count(*) FROM notify_dispatch WHERE status <> 3"), count(t, d, "SELECT count(*) FROM notify_dispatch WHERE publish_state = 0"))
	}
	if int(adapter.total.Load()) != total || len(adapter.keys) != total {
		t.Fatalf("sends = %d, distinct keys = %d, want %d each", adapter.total.Load(), len(adapter.keys), total)
	}
}

// TestIntegration_RelayClaimLease 认领租约即多副本互斥：租约未到期的行不被重复认领；
// 到期未落账（relay 崩溃）的行可被再次认领并发布；落账后租约清空。
func TestIntegration_RelayClaimLease(t *testing.T) {
	d := newIntegrationData(t)
	nb := startNats(t)
	seedConfig(t, d, "http://127.0.0.1:9/hook")
	ctx := context.Background()
	relay := &natsRelay{data: d, nb: nb, log: testLogger(), staleAfter: time.Minute}

	const ev = "018f3e1a-2203-7000-8000-000000000001"
	seedEvent(t, d, ev, "monitor.alert.firing", 2, "", nil)
	// 另一副本已认领（发布在途）。
	if _, err := d.Pool.Exec(ctx, "UPDATE notify_event SET relay_claim_until = now() + interval '1 minute' WHERE id = $1", ev); err != nil {
		t.Fatal(err)
	}
	if n, err := relay.RelayOnce(ctx, 100); err != nil || n != 0 {
		t.Fatalf("RelayOnce(claimed by other) = %d, %v; want 0", n, err)
	}
	// 该副本崩溃：租约到期后被接手。
	if _, err := d.Pool.Exec(ctx, "UPDATE notify_event SET relay_claim_until = now() - interval '1 second' WHERE id = $1", ev); err != nil {
		t.Fatal(err)
	}
	if n, err := relay.RelayOnce(ctx, 100); err != nil || n != 1 {
		t.Fatalf("RelayOnce(expired claim) = %d, %v; want 1", n, err)
	}
	if count(t, d, "SELECT count(*) FROM notify_event WHERE id = $1 AND publish_state = 1 AND relay_claim_until IS NULL", ev) != 1 {
		t.Fatal("published event must be marked and its claim cleared")
	}

	// 任务同理。
	const dsp = "018f3e1a-2203-7000-8000-0000000000d1"
	seedDispatch(t, d, dsp, ev, idChanHook, 1, time.Now().Add(-time.Second))
	if _, err := d.Pool.Exec(ctx, "UPDATE notify_dispatch SET relay_claim_until = now() + interval '1 minute' WHERE id = $1", dsp); err != nil {
		t.Fatal(err)
	}
	if n, err := relay.publishDispatches(ctx, 100); err != nil || n != 0 {
		t.Fatalf("publishDispatches(claimed by other) = %d, %v; want 0", n, err)
	}
	if _, err := d.Pool.Exec(ctx, "UPDATE notify_dispatch SET relay_claim_until = now() - interval '1 second' WHERE id = $1", dsp); err != nil {
		t.Fatal(err)
	}
	if n, err := relay.publishDispatches(ctx, 100); err != nil || n != 1 {
		t.Fatalf("publishDispatches(expired claim) = %d, %v; want 1", n, err)
	}
	if count(t, d, "SELECT count(*) FROM notify_dispatch WHERE id = $1 AND publish_state = 1 AND relay_claim_until IS NULL", dsp) != 1 {
		t.Fatal("published dispatch must be marked and its claim cleared")
	}
}
