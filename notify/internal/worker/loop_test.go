package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/loop"
)

// TestLoop_StopDrainsInFlight Stop 关闭准入后，在途迭代须在未被取消的 ctx 上跑完
// （设计 §8「处理完在途 → 退出」），且不再开始新迭代。
func TestLoop_StopDrainsInFlight(t *testing.T) {
	t.Parallel()
	var runs atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	var seenCancelled atomic.Bool
	l := NewLoop("t", time.Hour, testLogger(), nil, func(ctx context.Context) (int, error) {
		if runs.Add(1) == 1 {
			close(started)
			<-release
			seenCancelled.Store(ctx.Err() != nil)
		}
		return 1, nil // 忙：若准入未关闭会立刻进入下一轮
	})
	done := make(chan error, 1)
	go func() { done <- l.Start(context.Background()) }()
	<-started
	stopped := make(chan struct{})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = l.Stop(ctx)
		close(stopped)
	}()
	select {
	case <-stopped:
		t.Fatal("Stop must wait for the in-flight iteration")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return after iteration finished")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if seenCancelled.Load() {
		t.Fatal("in-flight iteration must not observe cancellation during drain")
	}
	if n := runs.Load(); n != 1 {
		t.Fatalf("no new iteration may start after Stop, runs = %d", n)
	}
}

// TestLoop_StopTimeoutAborts 停机时限用尽才硬中止在途迭代。
func TestLoop_StopTimeoutAborts(t *testing.T) {
	t.Parallel()
	aborted := make(chan struct{})
	l := NewLoop("t", time.Hour, testLogger(), nil, func(ctx context.Context) (int, error) {
		<-ctx.Done()
		close(aborted)
		return 0, ctx.Err()
	})
	go func() { _ = l.Start(context.Background()) }()
	time.Sleep(20 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := l.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-aborted:
	case <-time.After(time.Second):
		t.Fatal("iteration must be aborted after stop timeout")
	}
}

// TestLoop_ErrorBackoff 出错退避且 Stop 可打断等待。
func TestLoop_ErrorBackoff(t *testing.T) {
	t.Parallel()
	var runs atomic.Int32
	l := NewLoop("t", 10*time.Millisecond, testLogger(), nil, func(context.Context) (int, error) {
		runs.Add(1)
		return 0, errors.New("boom")
	})
	go func() { _ = l.Start(context.Background()) }()
	time.Sleep(120 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := l.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	// 10ms 周期无退避 ≈ 12 次；指数退避（20/40/80ms…）应明显更少。
	if n := runs.Load(); n < 2 || n > 6 {
		t.Fatalf("runs = %d, want exponential backoff", n)
	}
}

// lockerStub 可控的 Locker：记录获取/释放次数，并经 kit/loop.LockProbe 暴露可注入
// 故障的持锁连接探针（对应 dbx.TryAdvisoryLock 的行为）。
type lockerStub struct {
	acquired, released int
	held               bool
	connErr            error
}

func (l *lockerStub) TryLock(ctx context.Context, _ string) (func(), bool, error) {
	if l.held {
		return nil, false, nil
	}
	l.held = true
	l.acquired++
	if probe := loop.LockProbeFromContext(ctx); probe != nil {
		probe.Check = func(context.Context) error { return l.connErr }
	}
	return func() { l.held = false; l.released++ }, true, nil
}

// TestLeaderLoop_DropsLeadershipWhenLockConnLost 持锁连接断开（PG 已随会话释放锁）
// 时，下一轮不得继续以主身份执行：先放弃领导权再重新竞争。
func TestLeaderLoop_DropsLeadershipWhenLockConnLost(t *testing.T) {
	t.Parallel()
	locker := &lockerStub{}
	ll := &leaderLoop{key: "k", locker: locker, log: testLogger()}
	runs := 0
	fn := func(context.Context) (int, error) { runs++; return 1, nil }
	ctx := context.Background()
	for range 2 {
		if n, err := ll.run(ctx, fn); err != nil || n != 1 {
			t.Fatalf("run() = %d, %v", n, err)
		}
	}
	if locker.acquired != 1 || runs != 2 {
		t.Fatalf("healthy leader must keep the lock: acquired=%d runs=%d", locker.acquired, runs)
	}

	// 连接断开，且锁已被他人接任：本实例必须失主且本轮不执行 fn。
	locker.connErr = errors.New("conn closed")
	other := &lockerStub{held: true}
	ll.locker = other
	if n, err := ll.run(ctx, fn); err != nil || n != 0 {
		t.Fatalf("run() after conn loss = %d, %v; want (0, nil)", n, err)
	}
	if runs != 2 || locker.released != 1 || ll.held {
		t.Fatalf("lost lock conn must drop leadership without running fn: runs=%d released=%d held=%v", runs, locker.released, ll.held)
	}

	// 锁重新可得：再次竞争成功后恢复执行。
	other.held = false
	if n, err := ll.run(ctx, fn); err != nil || n != 1 || other.acquired != 1 || runs != 3 {
		t.Fatalf("re-acquire: n=%d err=%v acquired=%d runs=%d", n, err, other.acquired, runs)
	}

	// fn 出错同样放弃领导权。
	boom := errors.New("boom")
	if _, err := ll.run(ctx, func(context.Context) (int, error) { return 0, boom }); !errors.Is(err, boom) || ll.held || other.released != 1 {
		t.Fatalf("fn error must release leadership: err=%v held=%v released=%d", err, ll.held, other.released)
	}
}
