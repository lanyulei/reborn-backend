package biz

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/monitor/internal/promtest"
)

type fakePlannerRepo struct {
	results []MaterializeResult
	err     error
	calls   int
	health  []HealthResult
	writes  int
	// records 记录规则任务物化的结果（空 = 没有变化）；recoveries WriteHealth 报告的「异常 → 正常」；
	// events 随之写入的恢复事件。
	records    []MaterializeResult
	recoveries []HealthRecovery
	events     []*EngineEventInsert
}

func (f *fakePlannerRepo) MaterializeRecords(context.Context, int) (MaterializeResult, error) {
	if len(f.records) == 0 {
		return MaterializeResult{}, f.err
	}
	r := f.records[0]
	f.records = f.records[1:]
	return r, f.err
}

func (f *fakePlannerRepo) Materialize(context.Context, int) (MaterializeResult, error) {
	f.calls++
	if f.err != nil {
		return MaterializeResult{}, f.err
	}
	r := f.results[0]
	f.results = f.results[1:]
	return r, nil
}

func (f *fakePlannerRepo) WriteHealth(_ context.Context, results []HealthResult,
	recovered func(HealthRecovery) (*EngineEventInsert, error),
) (int, error) {
	f.writes++
	f.health = results
	for _, r := range f.recoveries {
		ev, err := recovered(r)
		if err != nil {
			return 0, err
		}
		f.events = append(f.events, ev)
	}
	return len(f.recoveries), f.err
}

func TestPlannerMaterialize(t *testing.T) {
	repo := &fakePlannerRepo{results: []MaterializeResult{{Added: 3, Removed: 2, ClosedAlerts: 1}, {Removed: 0}, {}}}
	uc := NewPlannerUsecase(repo, nil, &fakeObserver{}, quietLogger()).WithOptions(PlannerOptions{MaterializeBatch: 2})
	// 删满一批：返回 > 0，循环立即再跑一轮。
	if n, err := uc.Materialize(context.Background()); err != nil || n != 2 {
		t.Fatalf("n = %d, err = %v", n, err)
	}
	if n, err := uc.Materialize(context.Background()); err != nil || n != 0 {
		t.Fatalf("n = %d, err = %v", n, err)
	}
	repo.err = errors.New("db down")
	if _, err := uc.Materialize(context.Background()); err == nil {
		t.Fatal("want an error")
	}
}

func TestPlannerProbe(t *testing.T) {
	up, down := promtest.New(t), promtest.New(t)
	down.SetDown(true)
	disabled := testDatasource("018f3e1a-0000-7000-8000-0000000000d3", up.URL)
	disabled.Enabled = false
	sealed := testDatasource("018f3e1a-0000-7000-8000-0000000000d4", up.URL)
	sealed.AuthType, sealed.Secret = monitorcontract.AuthBasic, []byte("x")
	sealed.Username = "u"
	store := newFakeStore(t, baseRule(), testDatasource(testDSID, up.URL), testDatasource(testDS2, down.URL), disabled, sealed)
	cache, cleanup := NewConfigCache(store, fakeOpener{err: errors.New("no key")}, &fakeObserver{}, quietLogger())
	defer cleanup()
	repo := &fakePlannerRepo{}
	uc := NewPlannerUsecase(repo, cache, &fakeObserver{}, quietLogger()).WithOptions(PlannerOptions{ProbeTimeout: 2 * time.Second, ProbeConcurrency: 2})
	if n, err := uc.Probe(context.Background()); err != nil || n != 0 {
		t.Fatalf("n = %d, err = %v", n, err)
	}
	got := map[string]string{}
	for _, h := range repo.health {
		got[h.DatasourceID] = h.Error
	}
	if len(got) != 3 {
		t.Fatalf("only enabled datasources are probed, got %v", got)
	}
	if got[testDSID] != "" {
		t.Fatalf("healthy datasource reported %q", got[testDSID])
	}
	if !strings.Contains(got[testDS2], "http 503") {
		t.Fatalf("down datasource reported %q", got[testDS2])
	}
	if got[sealed.ID] != "datasource credential unavailable: master key missing or mismatched" {
		t.Fatalf("sealed datasource reported %q", got[sealed.ID])
	}
	if up.Hits("1") != 1 || down.Hits("1") != 1 {
		t.Fatalf("hits = %d / %d", up.Hits("1"), down.Hits("1"))
	}

	// 停机中被打断的探测不写回（否则会把健康的数据源记成异常）。
	writes := repo.writes
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := uc.Probe(ctx); err == nil || repo.writes != writes {
		t.Fatalf("a cancelled probe round must not be written back (err = %v)", err)
	}
}

// E16：探活失败只在状态变化时记日志（故障 → WARN 一条，恢复 → INFO 一条），不逐轮刷屏。
func TestPlannerProbeLogsTransitionsOnly(t *testing.T) {
	prom := promtest.New(t)
	store := newFakeStore(t, baseRule(), testDatasource(testDSID, prom.URL))
	cache, cleanup := NewConfigCache(store, fakeOpener{}, &fakeObserver{}, quietLogger())
	defer cleanup()
	logger, logs := captureLogger()
	uc := NewPlannerUsecase(&fakePlannerRepo{}, cache, &fakeObserver{}, logger).WithOptions(PlannerOptions{ProbeTimeout: time.Second})
	probe := func(n int) {
		t.Helper()
		for range n {
			if _, err := uc.Probe(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
	}
	probe(2)
	if logs.String() != "" {
		t.Fatalf("healthy probes must be silent:\n%s", logs)
	}
	prom.SetDown(true)
	probe(3)
	if n := logs.count("level=WARN", "datasource health probe is failing", "datasource_id="+testDSID, "http 503"); n != 1 {
		t.Fatalf("failure warnings = %d, want 1\n%s", n, logs)
	}
	prom.SetDown(false)
	probe(2)
	if n := logs.count("level=INFO", "datasource health probe recovered"); n != 1 || logs.count("level=WARN") != 1 {
		t.Fatalf("recovery lines = %d\n%s", n, logs)
	}
}

func TestPlannerProbeTimeout(t *testing.T) {
	slow := promtest.New(t)
	slow.SetDelay(2 * time.Second)
	store := newFakeStore(t, baseRule(), testDatasource(testDSID, slow.URL))
	cache, cleanup := NewConfigCache(store, fakeOpener{}, &fakeObserver{}, quietLogger())
	defer cleanup()
	repo := &fakePlannerRepo{}
	uc := NewPlannerUsecase(repo, cache, &fakeObserver{}, quietLogger()).WithOptions(PlannerOptions{ProbeTimeout: 50 * time.Millisecond})
	start := time.Now()
	if _, err := uc.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > time.Second || len(repo.health) != 1 || !strings.Contains(repo.health[0].Error, "timeout") {
		t.Fatalf("took %v, health = %+v", time.Since(start), repo.health)
	}
}

type fakeMaintenanceRepo struct {
	ensureAhead  int
	ensureErr    error
	dropped      int
	defaultRows  int
	alerts       int
	outbox       int
	engineEvents int
	commands     int
	calls        map[string]int
	lastRetained map[string]time.Duration
}

func (f *fakeMaintenanceRepo) hit(name string, retention time.Duration) {
	if f.calls == nil {
		f.calls, f.lastRetained = map[string]int{}, map[string]time.Duration{}
	}
	f.calls[name]++
	f.lastRetained[name] = retention
}

func take(have *int, limit int) int {
	n := min(*have, limit)
	*have -= n
	return n
}

func (f *fakeMaintenanceRepo) EnsurePartitions(_ context.Context, ahead int) error {
	f.hit("ensure", 0)
	f.ensureAhead = ahead
	return f.ensureErr
}

func (f *fakeMaintenanceRepo) DropExpiredPartitions(_ context.Context, retention time.Duration) (int, error) {
	f.hit("drop", retention)
	return f.dropped, nil
}

func (f *fakeMaintenanceRepo) PurgeDefaultPartition(_ context.Context, retention time.Duration, limit int) (int, error) {
	f.hit("default", retention)
	return take(&f.defaultRows, limit), nil
}

func (f *fakeMaintenanceRepo) PurgeAlerts(_ context.Context, retention time.Duration, limit int) (int, error) {
	f.hit("alerts", retention)
	return take(&f.alerts, limit), nil
}

func (f *fakeMaintenanceRepo) PurgeOutbox(_ context.Context, retention time.Duration, limit int) (int, error) {
	f.hit("outbox", retention)
	return take(&f.outbox, limit), nil
}

func (f *fakeMaintenanceRepo) PurgeEngineEvents(_ context.Context, retention time.Duration, limit int) (int, error) {
	f.hit("engine_events", retention)
	return take(&f.engineEvents, limit), nil
}

func (f *fakeMaintenanceRepo) PurgeCommands(_ context.Context, retention time.Duration, limit int) (int, error) {
	f.hit("commands", retention)
	return take(&f.commands, limit), nil
}

func TestMaintenanceRunOnce(t *testing.T) {
	t.Run("defaults and batching", func(t *testing.T) {
		repo := &fakeMaintenanceRepo{alerts: 2500, outbox: 10, defaultRows: 1000}
		uc := NewMaintenanceUsecase(repo, repo, quietLogger())
		if err := uc.RunOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		if repo.ensureAhead != 2 || repo.lastRetained["drop"] != 7*24*time.Hour || repo.lastRetained["alerts"] != 180*24*time.Hour {
			t.Fatalf("defaults not applied: %+v", repo)
		}
		// 2500 行 / 每批 1000 → 3 批；恰好一批的要再跑一批确认清空。
		if repo.calls["alerts"] != 3 || repo.calls["outbox"] != 1 || repo.calls["default"] != 2 || repo.alerts != 0 {
			t.Fatalf("calls = %v", repo.calls)
		}
	})
	t.Run("alert retention 0 disables row purging but not partitions", func(t *testing.T) {
		repo := &fakeMaintenanceRepo{alerts: 5}
		uc := NewMaintenanceUsecase(repo, repo, quietLogger()).WithOptions(MaintenanceOptions{PartitionsAhead: 5, EvalRecordRetention: time.Hour})
		if err := uc.RunOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		if repo.calls["alerts"] != 0 || repo.calls["outbox"] != 0 || repo.calls["ensure"] != 1 || repo.ensureAhead != 5 || repo.lastRetained["drop"] != time.Hour {
			t.Fatalf("calls = %v", repo.calls)
		}
	})
	t.Run("batches per run are bounded", func(t *testing.T) {
		repo := &fakeMaintenanceRepo{alerts: 1000}
		uc := NewMaintenanceUsecase(repo, repo, quietLogger()).WithOptions(MaintenanceOptions{AlertRetention: time.Hour, PurgeBatch: 10, MaxBatches: 3})
		if err := uc.RunOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		if repo.calls["alerts"] != 3 || repo.alerts != 970 {
			t.Fatalf("calls = %v, left = %d", repo.calls, repo.alerts)
		}
	})
	t.Run("one failing step does not stop the others", func(t *testing.T) {
		repo := &fakeMaintenanceRepo{ensureErr: errors.New("lock timeout"), alerts: 1}
		uc := NewMaintenanceUsecase(repo, repo, quietLogger())
		err := uc.RunOnce(context.Background())
		if err == nil || !strings.Contains(err.Error(), "lock timeout") || repo.calls["alerts"] != 1 || repo.calls["drop"] != 1 {
			t.Fatalf("err = %v, calls = %v", err, repo.calls)
		}
	})
}
