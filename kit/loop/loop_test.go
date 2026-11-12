package loop

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

type countRec struct{ runs, items atomic.Int64 }

func (r *countRec) Record(_ context.Context, _ string, items int, _ error) {
	r.runs.Add(1)
	r.items.Add(int64(items))
}

func TestLoop_BusyIdleAndStop(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	rec := &countRec{}
	l := New("t", time.Hour, testLogger(), rec, func(context.Context) (int, error) {
		n := calls.Add(1)
		if n <= 3 {
			return 1, nil // 忙：立即下一轮
		}
		return 0, nil // 闲：等待 1h → Stop 必须能打断
	})
	done := make(chan error, 1)
	go func() { done <- l.Start(context.Background()) }()
	deadline := time.Now().Add(5 * time.Second)
	for calls.Load() < 4 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := l.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 4 || rec.runs.Load() != 4 || rec.items.Load() != 3 {
		t.Fatalf("calls=%d runs=%d items=%d", calls.Load(), rec.runs.Load(), rec.items.Load())
	}
	if err := l.Stop(ctx); err != nil {
		t.Fatal("second Stop must be no-op")
	}
}

func TestLoop_ErrorBackoff(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	l := New("t", 10*time.Millisecond, testLogger(), nil, func(context.Context) (int, error) {
		calls.Add(1)
		return 0, errors.New("boom")
	})
	go func() { _ = l.Start(context.Background()) }()
	time.Sleep(150 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = l.Stop(ctx)
	// 退避：10ms → 20 → 40 → 80 → 160：150ms 内最多 4~5 次，而非 15 次。
	if n := calls.Load(); n < 2 || n > 6 {
		t.Fatalf("expected backoff, got %d calls", n)
	}
}

type fakeLocker struct {
	held     bool
	released int
	fail     bool
}

func (f *fakeLocker) TryLock(context.Context, string) (func(), bool, error) {
	if f.fail {
		return nil, false, errors.New("db down")
	}
	if f.held {
		return nil, false, nil
	}
	f.held = true
	return func() { f.held = false; f.released++ }, true, nil
}

func TestLeader(t *testing.T) {
	t.Parallel()
	lk := &fakeLocker{held: true}
	ld := NewLeader("k", lk, testLogger())
	ran := 0
	fn := func(context.Context) (int, error) { ran++; return 1, nil }
	if n, err := ld.Run(context.Background(), fn); n != 0 || err != nil || ran != 0 {
		t.Fatal("must not run while someone else holds the lock")
	}
	lk.held = false
	if n, err := ld.Run(context.Background(), fn); n != 1 || err != nil || !lk.held {
		t.Fatalf("leader must run after acquiring: n=%d err=%v", n, err)
	}
	if _, err := ld.Run(context.Background(), func(context.Context) (int, error) { return 0, errors.New("x") }); err == nil || lk.held || lk.released != 1 {
		t.Fatal("error must release the lock")
	}
	lk.fail = true
	if _, err := ld.Run(context.Background(), fn); err == nil {
		t.Fatal("lock error must propagate")
	}
	ld.Release()
}
