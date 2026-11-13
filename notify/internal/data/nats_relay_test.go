package data

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type fakeFuture struct {
	ok  chan *jetstream.PubAck
	err chan error
}

func newFakeFuture() *fakeFuture {
	return &fakeFuture{ok: make(chan *jetstream.PubAck, 1), err: make(chan error, 1)}
}

func (f *fakeFuture) Ok() <-chan *jetstream.PubAck { return f.ok }
func (f *fakeFuture) Err() <-chan error            { return f.err }
func (f *fakeFuture) Msg() *nats.Msg               { return nil }

// TestAwaitAll_SharedDeadline 整批 ack 等待共用一个时限：N 条未 ack 的总等待 ≈ 一个时限
// （不是 N 个），时限前后到达的 ack / 错误都被如实归位。
func TestAwaitAll_SharedDeadline(t *testing.T) {
	t.Parallel()
	acked, failed, late := newFakeFuture(), newFakeFuture(), newFakeFuture()
	acked.ok <- &jetstream.PubAck{}
	boom := errors.New("boom")
	failed.err <- boom
	late.ok <- &jetstream.PubAck{} // 排在超时的 future 之后：时限到后仍被非阻塞检查认领
	prePublish := errors.New("publish failed")
	futures := []jetstream.PubAckFuture{acked, failed, nil, newFakeFuture(), newFakeFuture(), newFakeFuture(), late}
	errs := make([]error, len(futures))
	errs[2] = prePublish
	start := time.Now()
	awaitAll(context.Background(), futures, errs, 50*time.Millisecond)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("awaitAll took %v, want one shared deadline", elapsed)
	}
	if errs[0] != nil || !errors.Is(errs[1], boom) || !errors.Is(errs[2], prePublish) || errs[6] != nil {
		t.Fatalf("errs = %v", errs)
	}
	for _, i := range []int{3, 4, 5} {
		if !errors.Is(errs[i], errPublishAckTimeout) {
			t.Fatalf("errs[%d] = %v, want ack timeout", i, errs[i])
		}
	}
}

func TestAwaitAll_ContextCancelled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	futures := []jetstream.PubAckFuture{newFakeFuture(), newFakeFuture()}
	errs := make([]error, 2)
	awaitAll(ctx, futures, errs, time.Minute)
	if !errors.Is(errs[0], context.Canceled) || errs[1] == nil {
		t.Fatalf("errs = %v", errs)
	}
}

// TestDispatchMsgID 放回（attempts 不变、到期时刻改变）后的重新发布必须是新去重键；
// 同一到期时刻的重复发布保持同键（窗口去重）。
func TestDispatchMsgID(t *testing.T) {
	t.Parallel()
	due := time.Unix(1_800_000_000, 0)
	a := dispatchMsgID("id", 1, due)
	if a != dispatchMsgID("id", 1, due) {
		t.Fatal("same generation must dedup")
	}
	if a == dispatchMsgID("id", 1, due.Add(5*time.Second)) || a == dispatchMsgID("id", 2, due) {
		t.Fatal("released / retried dispatch must get a fresh message id")
	}
	if publishBackoff(0) != 5*time.Second || publishBackoff(100) != 5*time.Minute {
		t.Fatalf("publishBackoff = %v, %v", publishBackoff(0), publishBackoff(100))
	}
}

// TestRelayRound 一轮按「认领 → 发布 → 等 ack → 落账」推进：成功 / 失败的行如实分流给
// settle（围栏字段原样带回），返回成功条数；落账只发生在全部 ack 等待结束之后。
func TestRelayRound(t *testing.T) {
	t.Parallel()
	due := time.Unix(1_800_000_000, 0)
	items := []relayItem{
		{cursor: cursor{ID: "a"}, msgID: "a:0", attempts: 0, due: due},
		{cursor: cursor{ID: "b"}, msgID: "b:1", attempts: 1, due: due},
		{cursor: cursor{ID: "c"}, msgID: "c:2", attempts: 2, due: due},
		{cursor: cursor{ID: "d"}, msgID: "d:0", attempts: 0, due: due},
	}
	boom := errors.New("nats down")
	var steps []string
	claim := func(context.Context) ([]relayItem, error) {
		steps = append(steps, "claim")
		return items, nil
	}
	publish := func(it relayItem) (jetstream.PubAckFuture, error) {
		steps = append(steps, "publish:"+it.msgID)
		f := newFakeFuture()
		switch it.ID {
		case "a", "c":
			f.ok <- &jetstream.PubAck{}
		case "b":
			return nil, boom // 发布本身失败
		} // d：ack 永不到达 → 超时
		return f, nil
	}
	var gotOK, gotFailed []relayItem
	var gotErr error
	settle := func(_ context.Context, ok, failed []relayItem, firstErr error) error {
		steps = append(steps, "settle")
		gotOK, gotFailed, gotErr = ok, failed, firstErr
		return nil
	}
	n, err := relayRound(context.Background(), 30*time.Millisecond, claim, publish, settle)
	if err != nil || n != 2 {
		t.Fatalf("relayRound() = %d, %v; want 2 published", n, err)
	}
	want := []string{"claim", "publish:a:0", "publish:b:1", "publish:c:2", "publish:d:0", "settle"}
	if len(steps) != len(want) {
		t.Fatalf("steps = %v", steps)
	}
	for i := range want {
		if steps[i] != want[i] {
			t.Fatalf("steps = %v, want %v", steps, want)
		}
	}
	if len(gotOK) != 2 || gotOK[0].ID != "a" || gotOK[1].ID != "c" || gotOK[1].attempts != 2 || !gotOK[1].due.Equal(due) {
		t.Fatalf("ok = %+v", gotOK)
	}
	if len(gotFailed) != 2 || gotFailed[0].ID != "b" || gotFailed[1].ID != "d" || !errors.Is(gotErr, boom) {
		t.Fatalf("failed = %+v, firstErr = %v", gotFailed, gotErr)
	}
}

// TestRelayRound_IdleAndErrors 无可认领行：不发布不落账、返回 0（relay 循环照常休眠，不空转）；
// 认领出错原样返回；落账出错时仍报告已发布条数（行靠租约到期以同一去重键重发）。
func TestRelayRound_IdleAndErrors(t *testing.T) {
	t.Parallel()
	never := func(relayItem) (jetstream.PubAckFuture, error) {
		t.Fatal("publish must not be called")
		return nil, nil
	}
	neverSettle := func(context.Context, []relayItem, []relayItem, error) error {
		t.Fatal("settle must not be called")
		return nil
	}
	if n, err := relayRound(context.Background(), time.Second, func(context.Context) ([]relayItem, error) { return nil, nil }, never, neverSettle); n != 0 || err != nil {
		t.Fatalf("idle round = %d, %v", n, err)
	}
	boom := errors.New("pg down")
	if n, err := relayRound(context.Background(), time.Second, func(context.Context) ([]relayItem, error) { return nil, boom }, never, neverSettle); n != 0 || !errors.Is(err, boom) {
		t.Fatalf("claim error round = %d, %v", n, err)
	}
	acked := func(relayItem) (jetstream.PubAckFuture, error) {
		f := newFakeFuture()
		f.ok <- &jetstream.PubAck{}
		return f, nil
	}
	n, err := relayRound(context.Background(), time.Second,
		func(context.Context) ([]relayItem, error) { return []relayItem{{cursor: cursor{ID: "a"}}}, nil }, acked,
		func(context.Context, []relayItem, []relayItem, error) error { return boom })
	if n != 1 || !errors.Is(err, boom) {
		t.Fatalf("settle error round = %d, %v", n, err)
	}
}

// TestFailBackoff 连续全失败的轮次指数退避；部分失败按当前计数退避且不升级；有成功即清零。
func TestFailBackoff(t *testing.T) {
	t.Parallel()
	var streak atomic.Int32
	if d := failBackoff(&streak, false); d != 5*time.Second || streak.Load() != 1 {
		t.Fatalf("first failure = %v, streak %d", d, streak.Load())
	}
	if d := failBackoff(&streak, false); d != 10*time.Second || streak.Load() != 2 {
		t.Fatalf("second failure = %v, streak %d", d, streak.Load())
	}
	if d := failBackoff(&streak, true); d != 5*time.Second || streak.Load() != 0 {
		t.Fatalf("partial failure = %v, streak %d", d, streak.Load())
	}
	for range 20 {
		failBackoff(&streak, false)
	}
	if d := failBackoff(&streak, false); d != 5*time.Minute {
		t.Fatalf("capped backoff = %v", d)
	}
}

func TestFenceArgs(t *testing.T) {
	t.Parallel()
	ct, due := time.Unix(1_800_000_000, 0), time.Unix(1_800_000_100, 0)
	ids, cts, attempts, dues := fenceArgs([]relayItem{{cursor: cursor{ID: "a", CreateTime: ct}, attempts: 3, due: due}})
	if len(ids) != 1 || ids[0] != "a" || !cts[0].Equal(ct) || attempts[0] != 3 || !dues[0].Equal(due) {
		t.Fatalf("fenceArgs = %v %v %v %v", ids, cts, attempts, dues)
	}
}
