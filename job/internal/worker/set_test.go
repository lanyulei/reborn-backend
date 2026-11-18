package worker

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/job/internal/biz"
	"github.com/lanyulei/reborn/backend/job/internal/biztest"
	"github.com/lanyulei/reborn/backend/job/internal/conf"
	"github.com/lanyulei/reborn/backend/kit/jobcontract"

	"google.golang.org/protobuf/types/known/durationpb"
)

// stubLocker 恒可获取的锁。
type stubLocker struct{ locks int }

func (l *stubLocker) TryLock(context.Context, string) (func(), bool, error) {
	l.locks++
	return func() {}, true, nil
}

// recorder 记录各循环的迭代结果（并发安全：循环 goroutine 写、测试 goroutine 读）。
type recorder struct {
	mu    sync.Mutex
	items map[string]int
}

func (r *recorder) Record(_ context.Context, name string, items int, _ error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.items[name] += items
}

func (r *recorder) get(name string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.items[name]
}

func TestNewSet(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, obs := biztest.NewStore(), biztest.NewObserver()
	uc := biz.NewRunnerUsecase(store, store, store, store, store, &biztest.Opener{}, obs, logger)
	house := biz.NewHousekeepUsecase(store, obs, logger)
	locker, rec := &stubLocker{}, &recorder{items: map[string]int{}}

	// 一条执行中的执行（租约已过期）与一条已结束、日志很旧的执行。
	const runnerID = "018f0000-0000-7000-8000-0000000000aa"
	store.AddExecution(biztest.Execution{ID: "018f0000-0000-7000-8000-000000000001"})
	store.AddExecution(biztest.Execution{ID: "018f0000-0000-7000-8000-000000000002"})
	for _, token := range []string{"t1", "t2"} {
		if c, err := store.ClaimNext(context.Background(), runnerID, "", token, time.Minute); err != nil || c == nil {
			t.Fatalf("claim: %v %v", c, err)
		}
	}
	report := func(biz.ExecState) (*biz.ReportMutation, error) {
		return &biz.ReportMutation{Chunks: []jobcontract.LogChunk{{Seq: 1, Content: "x"}}, LogSeq: 1, LogBytes: 1, Lease: time.Minute}, nil
	}
	if _, err := store.Report(context.Background(), "018f0000-0000-7000-8000-000000000002", report); err != nil {
		t.Fatal(err)
	}
	if err := store.Finish(context.Background(), "018f0000-0000-7000-8000-000000000002", func(biz.ExecState) (*biz.FinishMutation, error) {
		return &biz.FinishMutation{Status: jobcontract.StatusSuccess}, nil
	}); err != nil {
		t.Fatal(err)
	}
	store.Advance(2 * time.Hour)

	// 保留期 1h（显式配置）；周期取配置值。
	set := NewSet(&conf.Engine{
		SweepInterval: durationpb.New(10 * time.Millisecond), LogPurgeInterval: durationpb.New(10 * time.Millisecond),
		LogRetention: durationpb.New(time.Hour),
	}, uc, house, locker, rec, logger)
	if len(set.Engine) != 2 || set.Engine[0].Name() != "lease-sweep" || set.Engine[1].Name() != "log-purge" {
		t.Fatalf("loops: %+v", set.Engine)
	}
	// NewSet 给租约清扫设了启动宽限（一个租约）：刚启动时即便有过期的租约也不清扫。
	if n, err := house.SweepLeases(context.Background()); err != nil || n != 0 {
		t.Fatalf("sweep right after start = %d, %v; want the startup grace to hold it back", n, err)
	}
	// 宽限期已过（这里直接去掉宽限，其余参数同 NewSet 所设）。
	house.WithOptions(biz.HousekeepOptions{LogRetention: time.Hour})
	for _, l := range set.Engine {
		done := make(chan error, 1)
		go func() { done <- l.Start(context.Background()) }()
		deadline := time.Now().Add(10 * time.Second)
		for rec.get(l.Name()) == 0 && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if err := l.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if rec.get("lease-sweep") != 1 || rec.get("log-purge") != 1 {
		t.Fatalf("loop items: sweep %d purge %d", rec.get("lease-sweep"), rec.get("log-purge"))
	}
	if got := store.Get("018f0000-0000-7000-8000-000000000001"); got.Status != jobcontract.StatusUnknown || got.Error != biz.LeaseExpiredReason {
		t.Fatalf("sweeper loop did not expire the lease: %+v", got)
	}
	if got := store.Get("018f0000-0000-7000-8000-000000000002"); len(got.Logs) != 0 {
		t.Fatalf("purge loop left %d chunks", len(got.Logs))
	}
	// 日志清理经选主锁执行；租约清扫不选主。
	if locker.locks != 1 {
		t.Fatalf("locker used %d times, want 1 (log purge only)", locker.locks)
	}

	// log_retention 显式 0s = 永久保留；未设置 = 缺省 90 天。
	store.AddExecution(biztest.Execution{ID: "018f0000-0000-7000-8000-000000000003"})
	if _, err := store.ClaimNext(context.Background(), runnerID, "", "t3", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Report(context.Background(), "018f0000-0000-7000-8000-000000000003", report); err != nil {
		t.Fatal(err)
	}
	if err := store.Finish(context.Background(), "018f0000-0000-7000-8000-000000000003", func(biz.ExecState) (*biz.FinishMutation, error) {
		return &biz.FinishMutation{Status: jobcontract.StatusSuccess}, nil
	}); err != nil {
		t.Fatal(err)
	}
	store.Advance(89 * 24 * time.Hour)
	NewSet(&conf.Engine{LogRetention: durationpb.New(0)}, uc, house, locker, rec, logger)
	if n, err := house.PurgeLogs(context.Background()); err != nil || n != 0 {
		t.Fatalf("retention 0s purged %d, %v", n, err)
	}
	NewSet(&conf.Engine{}, uc, house, locker, rec, logger)
	if n, _ := house.PurgeLogs(context.Background()); n != 0 {
		t.Fatalf("default retention purged %d chunks after 89 days", n)
	}
	store.Advance(2 * 24 * time.Hour)
	if n, _ := house.PurgeLogs(context.Background()); n != 1 {
		t.Fatalf("default retention purged %d chunks after 91 days, want 1", n)
	}
}
