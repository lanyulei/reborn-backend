//go:build integration

package data

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/notify/internal/biz"
	"github.com/lanyulei/reborn/backend/notify/internal/data/channel"
)

// hookServer 记录收到的 webhook 请求，可按 send_key 去重、可切换响应码。
type hookServer struct {
	*httptest.Server
	mu     sync.Mutex
	got    []hookCall
	status atomic.Int32
}

type hookCall struct {
	headers http.Header
	body    map[string]any
}

func newHookServer(t *testing.T) *hookServer {
	t.Helper()
	h := &hookServer{}
	h.status.Store(http.StatusOK)
	h.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		if !channel.VerifySignature("hook-secret", r.Header.Get(channel.HeaderTimestamp), raw, r.Header.Get(channel.HeaderSignature)) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		h.mu.Lock()
		h.got = append(h.got, hookCall{headers: r.Header.Clone(), body: body})
		h.mu.Unlock()
		w.WriteHeader(int(h.status.Load()))
	}))
	t.Cleanup(h.Close)
	return h
}

func (h *hookServer) calls() []hookCall {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]hookCall(nil), h.got...)
}

// pipeline 一组真实仓储装配的用例（无后台循环，测试按步驱动）。
type pipeline struct {
	router *biz.RouterUsecase
	sender *biz.SenderUsecase
	dlq    *biz.DlqUsecase
	repo   *dispatchRepo
}

func newPipeline(t *testing.T, d *Data, adapter biz.ChannelSender) *pipeline {
	t.Helper()
	cache := newCache(t, d)
	repo := NewDispatchRepo(d)
	return &pipeline{
		router: biz.NewRouterUsecase(NewEventRepo(d), cache, testLogger()),
		sender: biz.NewSenderUsecase(repo, cache, NewRateBucketRepo(d), adapter, biz.NewRenderer(), testLogger()).
			WithOptions(biz.SenderOptions{Lease: time.Minute, SendTimeout: 2 * time.Second, Concurrency: 8}),
		dlq:  biz.NewDlqUsecase(NewDlqRepo(d), testLogger()),
		repo: repo,
	}
}

// TestIntegration_EndToEndWebhook 一条通知真正送达：事件直插 → router → sender →
// webhook 收到带签名的请求；dispatch sent、delivery_log ok；随后重试/死信/重放链路。
func TestIntegration_EndToEndWebhook(t *testing.T) {
	d := newIntegrationData(t)
	hook := newHookServer(t)
	// 只保留 webhook 渠道的路由（email 渠道无 SMTP 服务，避免干扰）。
	seedConfig(t, d, hook.URL+"/hook")
	ctx := context.Background()
	if _, err := d.Pool.Exec(ctx, "UPDATE notify_route SET channel_ids = $1::uuid[] WHERE id = $2", []string{idChanHook}, idRoute); err != nil {
		t.Fatal(err)
	}
	p := newPipeline(t, d, channel.NewWebhookSender().WithInsecure())

	const ev = "018f3e1a-1201-7000-8000-000000000001"
	seedEvent(t, d, ev, "monitor.alert.firing", 3, "", nil)
	if n, err := p.router.RouteOnce(ctx, 100); err != nil || n != 1 {
		t.Fatalf("RouteOnce = %d, %v", n, err)
	}
	if n, err := p.sender.SendOnce(ctx, 100); err != nil || n != 1 {
		t.Fatalf("SendOnce = %d, %v", n, err)
	}
	calls := hook.calls()
	if len(calls) != 1 {
		t.Fatalf("webhook calls = %d, want 1", len(calls))
	}
	c := calls[0]
	if c.body["event_id"] != ev || c.body["subject"] != "[default] title-"+ev || c.body["body"] != "sev=3 count=1" ||
		c.headers.Get("X-Source") != "core" || c.headers.Get(channel.HeaderSendKey) == "" {
		t.Fatalf("webhook call = %+v", c)
	}
	var status int
	var content []byte
	var templateCol *string
	if err := d.Pool.QueryRow(ctx, "SELECT status, content, template_id::text FROM notify_dispatch WHERE event_id = $1", ev).Scan(&status, &content, &templateCol); err != nil || status != 3 {
		t.Fatalf("dispatch status = %d, %v", status, err)
	}
	dc, _ := biz.DecodeContent(content)
	if dc.TemplateID != idTpl || dc.TemplateVersion != 2 || dc.Subject == "" || templateCol == nil || *templateCol != idTpl {
		t.Fatalf("content snapshot = %+v, template_id column = %v", dc, templateCol)
	}
	if count(t, d, "SELECT count(*) FROM notify_delivery_log WHERE ok AND latency_ms >= 0") != 1 {
		t.Fatal("delivery log missing")
	}

	// 重试：接收端 503 → failed + 退避；把到期时间拨回 → 再发成功。
	hook.status.Store(http.StatusServiceUnavailable)
	const ev2 = "018f3e1a-1201-7000-8000-000000000002"
	seedEvent(t, d, ev2, "monitor.alert.firing", 2, "", nil)
	if n, err := p.router.RouteOnce(ctx, 100); err != nil || n != 1 {
		t.Fatalf("RouteOnce(ev2) = %d, %v", n, err)
	}
	if n, err := p.sender.SendOnce(ctx, 100); err != nil || n != 1 {
		t.Fatalf("SendOnce(ev2) = %d, %v", n, err)
	}
	if count(t, d, "SELECT count(*) FROM notify_dispatch WHERE event_id = $1 AND status = 4 AND attempts = 1 AND next_retry_time > now() + interval '20 seconds'", ev2) != 1 {
		dumpDispatches(t, d, ev2)
		t.Fatal("retryable failure should schedule backoff")
	}
	hook.status.Store(http.StatusOK)
	_, _ = d.Pool.Exec(ctx, "UPDATE notify_dispatch SET next_retry_time = now() WHERE event_id = $1", ev2)
	_, _ = p.sender.SendOnce(ctx, 100)
	if count(t, d, "SELECT count(*) FROM notify_dispatch WHERE event_id = $1 AND status = 3 AND attempts = 2", ev2) != 1 ||
		count(t, d, "SELECT count(*) FROM notify_delivery_log WHERE dispatch_id = (SELECT id FROM notify_dispatch WHERE event_id = $1)", ev2) != 2 {
		t.Fatal("retry should succeed on second attempt with two log rows")
	}
	// 同一 dispatch 两次尝试 send_key 相同（接收端可去重）。
	calls = hook.calls()
	if len(calls) != 3 || calls[1].headers.Get(channel.HeaderSendKey) != calls[2].headers.Get(channel.HeaderSendKey) {
		t.Fatalf("send_key must be stable across retries: %d calls", len(calls))
	}

	// 死信 + 重放：接收端 400（不可重试）→ dead + dlq；控制台标记 pending → worker 重放 → 新 dispatch 发送成功。
	hook.status.Store(http.StatusBadRequest)
	const ev3 = "018f3e1a-1201-7000-8000-000000000003"
	seedEvent(t, d, ev3, "monitor.alert.firing", 2, "", nil)
	_, _ = p.router.RouteOnce(ctx, 100)
	_, _ = p.sender.SendOnce(ctx, 100)
	if count(t, d, "SELECT count(*) FROM notify_dispatch WHERE event_id = $1 AND status = 5", ev3) != 1 ||
		count(t, d, "SELECT count(*) FROM notify_dlq WHERE event_id = $1 AND replay_state = 1 AND attempts = 1", ev3) != 1 {
		t.Fatal("non-retryable failure should dead-letter immediately")
	}
	_, _ = d.Pool.Exec(ctx, "UPDATE notify_dlq SET replay_state = 2 WHERE event_id = $1 AND replay_state = 1", ev3) // core ReplayDeadLetter 语义
	hook.status.Store(http.StatusOK)
	if n, err := p.dlq.ReplayOnce(ctx, 10); err != nil || n != 1 {
		t.Fatalf("ReplayOnce = %d, %v", n, err)
	}
	if n, err := p.sender.SendOnce(ctx, 100); err != nil || n != 1 {
		rows, _ := d.Pool.Query(ctx, "SELECT id, status, attempts, next_retry_time, last_error, now() FROM notify_dispatch WHERE event_id = $1", ev3)
		for rows.Next() {
			var id string
			var st, at int
			var nrt, now time.Time
			var le *string
			_ = rows.Scan(&id, &st, &at, &nrt, &le, &now)
			t.Logf("dispatch %s status=%d attempts=%d next=%v now=%v err=%v", id, st, at, nrt, now, le)
		}
		rows.Close()
		t.Fatalf("SendOnce(replayed) = %d, %v", n, err)
	}
	if count(t, d, "SELECT count(*) FROM notify_dispatch WHERE event_id = $1 AND status = 3", ev3) != 1 ||
		count(t, d, "SELECT count(*) FROM notify_dlq WHERE event_id = $1 AND replay_state = 3", ev3) != 1 {
		t.Fatal("replayed dispatch should be delivered")
	}
	// 重放的新 dispatch 带新 send_key（刻意再投）。
	calls = hook.calls()
	last := calls[len(calls)-1].headers.Get(channel.HeaderSendKey)
	for _, c := range calls[:len(calls)-1] {
		if c.headers.Get(channel.HeaderSendKey) == last {
			t.Fatal("replay must use a fresh send_key")
		}
	}

	// 配置生效：停用渠道 → 事件路由后无目标 → failed（不生成任务）。
	_, _ = d.Pool.Exec(ctx, "UPDATE notify_channel SET enabled = false WHERE id = $1", idChanHook)
	_, _ = d.Pool.Exec(ctx, "UPDATE notify_cfg_version SET version = version + 1 WHERE table_name = 'notify_channel'")
	cache := newCache(t, d)
	router2 := biz.NewRouterUsecase(NewEventRepo(d), cache, testLogger())
	const ev4 = "018f3e1a-1201-7000-8000-000000000004"
	seedEvent(t, d, ev4, "monitor.alert.firing", 2, "", nil)
	_, _ = router2.RouteOnce(ctx, 100)
	if count(t, d, "SELECT status FROM notify_event WHERE id = $1", ev4) != 4 || count(t, d, "SELECT count(*) FROM notify_dispatch WHERE event_id = $1", ev4) != 0 {
		t.Fatal("disabled channel must be skipped by routing")
	}
}

// countingSender 内存适配器：统计发送与 send_key 去重（负载对账）。
type countingSender struct {
	mu    sync.Mutex
	keys  map[string]int
	total atomic.Int64
}

func (s *countingSender) Supports(string) bool { return true }
func (s *countingSender) Send(_ context.Context, _ *biz.Channel, msg biz.RenderedMessage) (biz.SendResult, error) {
	s.total.Add(1)
	s.mu.Lock()
	s.keys[msg.SendKey]++
	s.mu.Unlock()
	return biz.SendResult{ProviderMsgID: msg.SendKey}, nil
}

// TestIntegration_LoadReconciliation 1 万事件（批量 ingress 落库）→ 两个 router 与
// 两个 sender 实例并发消费 → 对账：事件全部交付、dispatch 全部终态、无重复发送。
func TestIntegration_LoadReconciliation(t *testing.T) {
	if testing.Short() {
		t.Skip("load test skipped in -short")
	}
	d := newIntegrationData(t)
	seedConfig(t, d, "http://127.0.0.1:9/hook")
	ctx := context.Background()
	_, _ = d.Pool.Exec(ctx, "UPDATE notify_route SET channel_ids = $1::uuid[] WHERE id = $2", []string{idChanHook, idChanEmail}, idRoute)
	_, _ = d.Pool.Exec(ctx, "UPDATE notify_channel SET rate_limit = '{\"qps\":0,\"burst\":0}'") // 本测试不测限流
	adapter := &countingSender{keys: map[string]int{}}

	const total = 10_000
	ingress := NewIngressRepo(d)
	start := time.Now()
	for b := 0; b < total/500; b++ {
		events := make([]*biz.Event, 0, 500)
		for i := 0; i < 500; i++ {
			events = append(events, &biz.Event{
				BizID: "default", EventType: "monitor.alert.firing", Severity: 2, Title: "load",
				Recipients: []string{"a@example.com"}, Source: biz.EventSourceAPI, Status: 1, NextPublishTime: time.Now(), CreateTime: time.Now(),
			})
		}
		uc := biz.NewIngressUsecase(ingress)
		if _, err := uc.Accept(ctx, &biz.Producer{BizID: "default"}, events); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("ingress 10k events in %v", time.Since(start))

	runAll := func(n int, step func(context.Context, int) (int, error), batch int) {
		var wg sync.WaitGroup
		for w := 0; w < n; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					k, err := step(ctx, batch)
					if err != nil {
						t.Error(err)
						return
					}
					if k == 0 {
						return
					}
				}
			}()
		}
		wg.Wait()
	}
	start = time.Now()
	p1, p2 := newPipeline(t, d, adapter), newPipeline(t, d, adapter)
	routers := []*biz.RouterUsecase{p1.router, p2.router}
	runAll(2, func(ctx context.Context, batch int) (int, error) {
		return routers[time.Now().UnixNano()%2].RouteOnce(ctx, batch)
	}, 500)
	t.Logf("routed in %v", time.Since(start))
	start = time.Now()
	senders := []*biz.SenderUsecase{p1.sender, p2.sender}
	runAll(2, func(ctx context.Context, batch int) (int, error) {
		return senders[time.Now().UnixNano()%2].SendOnce(ctx, batch)
	}, 100)
	t.Logf("sent in %v", time.Since(start))

	// 对账（设计 §13 ①/④）：published = events；dispatch 全部 sent；每个 send_key 恰发一次。
	if count(t, d, "SELECT count(*) FROM notify_event WHERE publish_state = 0") != 0 ||
		count(t, d, "SELECT count(*) FROM notify_event WHERE status = 2") != total {
		t.Fatal("not all events routed")
	}
	// 每事件 2 渠道：webhook(url) + email(explicit a@example.com)。
	if count(t, d, "SELECT count(*) FROM notify_dispatch") != total*2 || count(t, d, "SELECT count(*) FROM notify_dispatch WHERE status <> 3") != 0 {
		t.Fatalf("dispatch terminal states: total=%d non-sent=%d", count(t, d, "SELECT count(*) FROM notify_dispatch"),
			count(t, d, "SELECT count(*) FROM notify_dispatch WHERE status <> 3"))
	}
	if int(adapter.total.Load()) != total*2 || len(adapter.keys) != total*2 {
		t.Fatalf("sends = %d, distinct keys = %d, want %d each", adapter.total.Load(), len(adapter.keys), total*2)
	}
	if count(t, d, "SELECT count(*) FROM notify_delivery_log WHERE ok") != total*2 {
		t.Fatal("delivery log count mismatch")
	}
}

func dumpDispatches(t *testing.T, d *Data, eventID string) {
	t.Helper()
	rows, err := d.Pool.Query(context.Background(),
		"SELECT id, status, attempts, next_retry_time, last_error, now() FROM notify_dispatch WHERE event_id = $1", eventID)
	if err != nil {
		t.Log(err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var st, at int
		var nrt, now time.Time
		var le *string
		_ = rows.Scan(&id, &st, &at, &nrt, &le, &now)
		t.Logf("dispatch %s status=%d attempts=%d next=%v now=%v err=%v", id, st, at, nrt, now, le)
	}
	logs, _ := d.Pool.Query(context.Background(),
		"SELECT attempt, ok, error FROM notify_delivery_log WHERE dispatch_id IN (SELECT id FROM notify_dispatch WHERE event_id = $1) ORDER BY attempt", eventID)
	defer logs.Close()
	for logs.Next() {
		var at int
		var ok bool
		var e *string
		_ = logs.Scan(&at, &ok, &e)
		t.Logf("  log attempt=%d ok=%v err=%v", at, ok, e)
	}
}
