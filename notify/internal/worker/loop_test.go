package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
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
