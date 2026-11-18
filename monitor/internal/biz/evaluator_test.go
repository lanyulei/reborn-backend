package biz

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/kit/monitorpipeline"
	"github.com/lanyulei/reborn/backend/monitor/internal/promtest"
)

const testDS2 = "018f3e1a-0000-7000-8000-00000000000d"

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// logBuffer 并发安全的日志缓冲：测试据此断言「记了哪些 WARN」。
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// count 含有全部 parts 的日志行数。
func (b *logBuffer) count(parts ...string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
lines:
	for line := range strings.SplitSeq(b.buf.String(), "\n") {
		if line == "" {
			continue
		}
		for _, part := range parts {
			if !strings.Contains(line, part) {
				continue lines
			}
		}
		n++
	}
	return n
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func captureLogger() (*slog.Logger, *logBuffer) {
	buf := &logBuffer{}
	return slog.New(slog.NewTextHandler(buf, nil)), buf
}

type released struct {
	task  *Task
	delay time.Duration
}

// fakeStore 全部仓储接口的内存实现：判定状态与告警落在每个数据源各自的 sim 里。
type fakeStore struct {
	t  *testing.T
	mu sync.Mutex

	rule        *Rule // nil = 规则不存在
	now         time.Time
	versions    CfgVersions
	datasources []*Datasource
	mutes       []*Mute
	cfgErr      error

	due       []*Task
	released  []released
	sims      map[string]*sim
	rounds    []Round
	commitErr error
	commits   int
	listed    int
	// inTx 当前处于 Commit 的事务之内的调用数（decide 执行期间）。
	inTx atomic.Int32
	// written 各数据源任务行记录的上一轮结果（Claim 带回）。
	written map[string]Round
	// muteLimit 最近一次 ListMutes 收到的 limit。
	muteLimit int
	// postponed 各数据源任务行上的「被顺延过」标记（Release 写入、Claim 带回、完成一轮评估清除）。
	postponed map[string]bool
	// 第二期的配置：抑制规则、升级策略、资源索引的行、工作日历；active 是抑制快照读到的活跃告警。
	inhibits    []monitorpipeline.InhibitRule
	escalations []*Escalation
	cis         []CIRow
	calendar    monitorcontract.CalendarSnapshot
	active      []ActiveAlert
	activeErr   error
	// ciCalls ListCIs 收到的 since（零值 = 整表）；escalationLoads Commit 被要求读升级进度的次数。
	ciCalls         []time.Time
	escalationLoads int
	// errorSince / errorCount 各数据源任务行上连续查询错误的起点与次数。
	errorSince map[string]time.Time
	errorCount map[string]int32
}

func newFakeStore(t *testing.T, rule *Rule, datasources ...*Datasource) *fakeStore {
	return &fakeStore{
		t: t, rule: rule, now: t0, datasources: datasources, sims: map[string]*sim{},
		versions: CfgVersions{Datasource: 1, Mute: 1, RuleGroup: 1, Escalation: 1, Inhibit: 1, CIIndex: 1, Calendar: 1},
	}
}

func (f *fakeStore) sim(datasourceID string) *sim {
	if f.sims[datasourceID] == nil {
		f.sims[datasourceID] = newSim(f.t, f.rule)
	}
	return f.sims[datasourceID]
}

func (f *fakeStore) Versions(context.Context) (CfgVersions, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.versions, f.cfgErr
}

func (f *fakeStore) ListDatasources(context.Context) ([]*Datasource, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listed++
	out := make([]*Datasource, len(f.datasources))
	for i, d := range f.datasources {
		c := *d
		out[i] = &c
	}
	return out, f.cfgErr
}

func (f *fakeStore) ListMutes(_ context.Context, limit int) ([]*Mute, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.muteLimit = limit
	return f.mutes[:min(len(f.mutes), limit)], f.cfgErr
}

func (f *fakeStore) ListInhibits(_ context.Context, limit int) ([]monitorpipeline.InhibitRule, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.inhibits[:min(len(f.inhibits), limit)]), f.cfgErr
}

func (f *fakeStore) ListEscalations(_ context.Context, limit int) ([]*Escalation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.escalations[:min(len(f.escalations), limit)]), f.cfgErr
}

// ListCIs 与数据层同一口径：since 为零值返回有效的行（按 ID），否则返回 sync_time 晚于 since 的行（含无效的）。
func (f *fakeStore) ListCIs(_ context.Context, since time.Time, limit int) ([]CIRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ciCalls = append(f.ciCalls, since)
	var out []CIRow
	for _, row := range f.cis {
		if (since.IsZero() && row.Valid) || (!since.IsZero() && row.SyncTime.After(since)) {
			row.CI.Tags, row.CI.UpstreamIDs = slices.Clone(row.CI.Tags), slices.Clone(row.CI.UpstreamIDs)
			out = append(out, row)
		}
	}
	slices.SortFunc(out, func(a, b CIRow) int {
		if since.IsZero() {
			return strings.Compare(a.CI.ID, b.CI.ID)
		}
		return a.SyncTime.Compare(b.SyncTime)
	})
	return out[:min(len(out), limit)], f.cfgErr
}

func (f *fakeStore) LoadCalendar(context.Context) (monitorcontract.CalendarSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calendar, f.cfgErr
}

func (f *fakeStore) ListActiveAlerts(_ context.Context, _ bool, limit int) ([]ActiveAlert, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.active[:min(len(f.active), limit)]), f.activeErr
}

func (f *fakeStore) LoadRule(context.Context, string) (*Rule, time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.rule == nil {
		return nil, time.Time{}, ErrRuleNotFound
	}
	return f.rule, f.now, nil
}

func (f *fakeStore) Claim(_ context.Context, token string, _ int, _, _, _ time.Duration) ([]*Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	tasks := f.due
	f.due = nil
	for _, t := range tasks {
		t.Token = token
		t.Postponed = f.postponed[t.DatasourceID]
		t.ErrorSince, t.ErrorCount = f.errorSince[t.DatasourceID], f.errorCount[t.DatasourceID]
		if last, ok := f.written[t.DatasourceID]; ok {
			t.LastStatus, t.LastEval = last.Status, last.Now
		}
	}
	return tasks, nil
}

// wrote 记下一轮写回任务行的结果（调用方持锁）。
func (f *fakeStore) wrote(datasourceID string, round *Round) {
	if f.written == nil {
		f.written = map[string]Round{}
	}
	f.written[datasourceID] = *round
	f.rounds = append(f.rounds, *round)
	delete(f.postponed, datasourceID)
	// 连续查询错误的起点与次数：与数据层的任务写回同一口径。
	switch round.Status {
	case monitorcontract.EvalError:
		if f.errorSince == nil {
			f.errorSince, f.errorCount = map[string]time.Time{}, map[string]int32{}
		}
		if f.errorSince[datasourceID].IsZero() {
			f.errorSince[datasourceID] = round.Now
		}
		f.errorCount[datasourceID]++
	case monitorcontract.EvalInactive:
	default:
		delete(f.errorSince, datasourceID)
		delete(f.errorCount, datasourceID)
	}
}

func (f *fakeStore) Release(_ context.Context, _ string, tasks []*Task, delay time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, t := range tasks {
		f.released = append(f.released, released{t, delay})
		if f.postponed == nil {
			f.postponed = map[string]bool{}
		}
		f.postponed[t.DatasourceID] = t.Postponed
	}
	return nil
}

func (f *fakeStore) Snapshot(_ context.Context, task *Task) ([]SeriesState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sim(task.DatasourceID).prev(), nil
}

func (f *fakeStore) Commit(_ context.Context, task *Task, escalation bool, decide func([]SeriesState) (*Round, error)) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commits++
	if escalation {
		f.escalationLoads++
	}
	if f.commitErr != nil {
		return f.commitErr
	}
	s := f.sim(task.DatasourceID)
	f.inTx.Add(1)
	round, err := decide(s.prev())
	f.inTx.Add(-1)
	if err != nil {
		return err
	}
	s.apply(round.Now, round.Transition)
	f.wrote(task.DatasourceID, round)
	return nil
}

func (f *fakeStore) Record(_ context.Context, task *Task, round *Round) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if round.Transition != nil {
		f.t.Errorf("Record must not carry a transition")
	}
	f.wrote(task.DatasourceID, round)
	return nil
}

func (f *fakeStore) last() Round {
	f.t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.rounds) == 0 {
		f.t.Fatal("no round was written")
	}
	return f.rounds[len(f.rounds)-1]
}

type fakeOpener struct {
	secrets map[string]string
	err     error
}

func (o fakeOpener) Open(ciphertext []byte) (string, error) {
	if o.err != nil {
		return "", o.err
	}
	return o.secrets[string(ciphertext)], nil
}

type fakeObserver struct {
	mu       sync.Mutex
	results  []string
	longest  time.Duration
	outbox   map[int32]int
	mutesCut int
	// 第二期：各类缓存截断次数、成环的告警数、记录规则的结果、指令的结果、自监控事件数。
	truncated map[string]int
	cycles    int
	records   []string
	written   int
	commands  map[string]int
	engineEvs map[string]int
}

func (o *fakeObserver) AddTruncated(_ context.Context, what string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.truncated == nil {
		o.truncated = map[string]int{}
	}
	o.truncated[what]++
}

func (o *fakeObserver) AddInhibitCycles(_ context.Context, n int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.cycles += n
}

func (o *fakeObserver) ObserveRecord(_ context.Context, result string, _ time.Duration, series int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.records = append(o.records, result)
	o.written += series
}

func (o *fakeObserver) AddCommand(_ context.Context, result string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.commands == nil {
		o.commands = map[string]int{}
	}
	o.commands[result]++
}

func (o *fakeObserver) AddEngineEvents(_ context.Context, typ string, n int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.engineEvs == nil {
		o.engineEvs = map[string]int{}
	}
	o.engineEvs[typ] += n
}

func (o *fakeObserver) AddMutesTruncated(context.Context) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.mutesCut++
}

func (o *fakeObserver) ObserveEval(_ context.Context, result string, duration, _ time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.longest = max(o.longest, duration)
	o.results = append(o.results, result)
}

func (o *fakeObserver) AddOutbox(_ context.Context, kind int32, n int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.outbox == nil {
		o.outbox = map[int32]int{}
	}
	o.outbox[kind] += n
}

func (o *fakeObserver) lastResult() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.results) == 0 {
		return ""
	}
	return o.results[len(o.results)-1]
}

type harness struct {
	t     *testing.T
	store *fakeStore
	prom  *promtest.Server
	obs   *fakeObserver
	cache *ConfigCache
	inh   *InhibitCache
	stats *InstanceStats
	uc    *EvaluatorUsecase
	logs  *logBuffer
}

func testDatasource(id, url string) *Datasource {
	return &Datasource{ID: id, Name: "prom-" + id[len(id)-1:], URL: url, AuthType: monitorcontract.AuthNone, Timeout: 2 * time.Second, Enabled: true, Version: 1}
}

func newHarness(t *testing.T, rule *Rule, opener SecretOpener, mod ...func(*Datasource)) *harness {
	t.Helper()
	prom := promtest.New(t)
	ds := testDatasource(testDSID, prom.URL)
	for _, m := range mod {
		m(ds)
	}
	store := newFakeStore(t, rule, ds)
	if opener == nil {
		opener = fakeOpener{}
	}
	obs := &fakeObserver{}
	logger, logs := captureLogger()
	cache, cleanup := NewConfigCache(store, opener, obs, logger)
	t.Cleanup(cleanup)
	// 抑制快照每次都重读：测试改了活跃告警 / 抑制规则后下一轮即生效。
	inh := NewInhibitCache(store, cache, obs, logger).WithOptions(InhibitOptions{Refresh: time.Nanosecond})
	stats := NewInstanceStats()
	uc := NewEvaluatorUsecase(store, store, store, cache, inh, stats, obs, logger)
	var n atomic.Int64
	uc.newID = func() (string, error) { return fmt.Sprintf("gen-%d", n.Add(1)), nil }
	return &harness{t: t, store: store, prom: prom, obs: obs, cache: cache, inh: inh, stats: stats, uc: uc, logs: logs}
}

// run 在 now 评估数据源 testDSID 的任务一轮。
func (h *harness) run(now time.Time) {
	h.t.Helper()
	h.runTasks(context.Background(), now, testDSID)
}

func (h *harness) runTasks(ctx context.Context, now time.Time, datasourceIDs ...string) {
	h.t.Helper()
	h.store.mu.Lock()
	h.store.now = now
	for _, id := range datasourceIDs {
		h.store.due = append(h.store.due, &Task{RuleID: testRuleID, DatasourceID: id})
	}
	h.store.mu.Unlock()
	n, err := h.uc.RunOnce(ctx, "tok")
	if err != nil {
		h.t.Fatalf("RunOnce() error = %v", err)
	}
	if n != len(datasourceIDs) {
		h.t.Fatalf("RunOnce() = %d, want %d", n, len(datasourceIDs))
	}
}

func TestEvaluatorHappyPath(t *testing.T) {
	h := newHarness(t, baseRule(func(r *Rule) { r.RecoveryExpr = `cpu{instance="{{ $labels.instance }}"}` }), nil)
	h.prom.Set("cpu > 85", promtest.S(91, "__name__", "cpu", "instance", "a"))
	h.prom.Set(`cpu{instance="a"}`, promtest.S(12, "instance", "a"))

	h.run(at(0))
	r := h.store.last()
	if r.Status != monitorcontract.EvalOK || r.Series != 1 || r.Anomalies != 1 || len(r.Detail.Queries) != 1 || r.Detail.Queries[0].Series != 1 {
		t.Fatalf("round 1 = %+v", r)
	}
	if r.Detail.Points[0].Step != monitorcontract.StepPending || r.Detail.Points[0].Labels["__name__"] != "" {
		t.Fatalf("round 1 points = %+v", r.Detail.Points)
	}
	if h.obs.lastResult() != ResultOK {
		t.Fatalf("result = %q", h.obs.lastResult())
	}

	h.run(at(60))
	r = h.store.last()
	s := h.store.sim(testDSID)
	if r.Status != monitorcontract.EvalFiring || r.Transition.FiringCount != 1 || s.alert().Value != 91 {
		t.Fatalf("round 2 = %+v", r)
	}
	if r.Alert.RuleName != "cpu high" || r.Alert.GroupID != testGroup || r.Alert.DatasourceName == "" || r.Exprs[0] != "cpu > 85" {
		t.Fatalf("round meta = %+v %v", r.Alert, r.Exprs)
	}
	if h.obs.lastResult() != ResultFiring || h.obs.outbox[monitorcontract.NotifyKindFiring] != 1 {
		t.Fatalf("observer = %+v", h.obs)
	}
	s.dispatch(at(60))

	// 序列消失：恢复，并带上恢复取值。
	h.prom.Set("cpu > 85")
	h.run(at(120))
	r = h.store.last()
	a := s.alert()
	if r.Status != monitorcontract.EvalNoData || r.Recovered != 1 || a.Status != monitorcontract.AlertResolved || !a.HasRecovery || a.RecoveryValue != 12 {
		t.Fatalf("round 3 = %+v, alert = %+v", r, a)
	}
	if h.prom.Hits(`cpu{instance="a"}`) != 1 || h.obs.outbox[monitorcontract.NotifyKindResolved] != 1 || h.obs.lastResult() != ResultNoData {
		t.Fatalf("recovery query hits = %d, observer = %+v", h.prom.Hits(`cpu{instance="a"}`), h.obs)
	}
}

func TestEvaluatorRecoveryExprFailureDoesNotBlockResolution(t *testing.T) {
	for name, setup := range map[string]func(h *harness){
		"query error": func(h *harness) {
			h.prom.SetError(`up{instance="a"}`, http.StatusUnprocessableEntity, "execution", "boom")
		},
		"empty result":    func(*harness) {},
		"broken template": func(h *harness) { h.store.rule.RecoveryExpr = "{{ nope }}" },
		"blank rendering": func(h *harness) { h.store.rule.RecoveryExpr = "{{ $labels.missing }}" },
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, baseRule(func(r *Rule) { r.For, r.RecoveryExpr = 0, `up{instance="{{ $labels.instance }}"}` }), nil)
			h.prom.Set("cpu > 85", promtest.S(91, "instance", "a"))
			h.run(at(0))
			setup(h)
			h.prom.Set("cpu > 85")
			h.run(at(60))
			if a := h.store.sim(testDSID).alert(); a.Status != monitorcontract.AlertResolved || a.HasRecovery {
				t.Fatalf("alert = %+v", a)
			}
		})
	}
}

func TestEvaluatorQueryErrorChangesNothing(t *testing.T) {
	two := func(r *Rule) {
		r.For = 0
		r.Queries = append(r.Queries, monitorcontract.Query{Expr: "cpu > 95", Severity: monitorcontract.SeverityCritical})
	}
	tests := []struct {
		name    string
		rule    *Rule
		breakIt func(h *harness)
		wantErr string
	}{
		{"upstream error", baseRule(func(r *Rule) { r.For = 0 }), func(h *harness) {
			h.prom.SetError("cpu > 85", http.StatusUnprocessableEntity, "execution", "query timed out in expression evaluation")
		}, "query 0: promclient: execution (http 422): query timed out"},
		{"datasource down", baseRule(func(r *Rule) { r.For = 0 }), func(h *harness) { h.prom.SetDown(true) }, "query 0: promclient: http (http 503)"},
		{"one of two queries fails", baseRule(two), func(h *harness) {
			h.prom.SetError("cpu > 95", http.StatusBadRequest, "bad_data", "parse error")
		}, "query 1: promclient: bad_data"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, tt.rule, nil)
			h.prom.Set("cpu > 85", promtest.S(91, "instance", "a"))
			h.run(at(0))
			s := h.store.sim(testDSID)
			s.dispatch(at(0))
			before, commits := s.only(), h.store.commits

			// 查询出错的同时序列「看不到了」：绝不能据此判恢复。
			tt.breakIt(h)
			h.run(at(600))
			r := h.store.last()
			if r.Status != monitorcontract.EvalError || !strings.HasPrefix(r.Error, tt.wantErr) || r.Transition != nil {
				t.Fatalf("round = status %d error %q transition %v", r.Status, r.Error, r.Transition)
			}
			if h.store.commits != commits {
				t.Fatalf("a failed round must not open a state transaction")
			}
			after := s.only()
			if after.State != before.State || !after.LastSeen.Equal(before.LastSeen) || s.alert().Status != monitorcontract.AlertFiring || len(s.outbox) != 1 {
				t.Fatalf("state changed on query error: %+v → %+v", before, after)
			}
			if h.obs.lastResult() != ResultError || len(h.store.released) != 0 {
				t.Fatalf("result = %q, released = %d", h.obs.lastResult(), len(h.store.released))
			}
			var failed int
			for _, d := range r.Detail.Queries {
				if d.Error != "" {
					failed++
				}
			}
			if failed == 0 || len(r.Detail.Queries) != len(tt.rule.Queries) {
				t.Fatalf("detail queries = %+v", r.Detail.Queries)
			}
		})
	}
}

func TestEvaluatorOutsideEffectiveWindow(t *testing.T) {
	// t0 是周二 08:00 UTC；规则只在周三生效。
	h := newHarness(t, baseRule(func(r *Rule) {
		r.For = 0
		r.Effective = monitorcontract.Effective{Timezone: "UTC", Windows: []monitorcontract.TimeWindow{{Days: []int32{3}, Start: "00:00", End: "00:00"}}}
	}), nil)
	h.prom.Set("cpu > 85", promtest.S(91, "instance", "a"))
	h.run(at(0))
	r := h.store.last()
	if r.Status != monitorcontract.EvalInactive || r.Transition != nil || h.prom.TotalHits() != 0 || h.store.commits != 0 {
		t.Fatalf("round = %+v, hits = %d", r, h.prom.TotalHits())
	}
	if h.obs.lastResult() != ResultInactive {
		t.Fatalf("result = %q", h.obs.lastResult())
	}
	// 进入生效时间（周三）后照常评估；再离开时已有告警保持原状。
	h.run(at(24 * 3600))
	s := h.store.sim(testDSID)
	if s.alert().Status != monitorcontract.AlertFiring {
		t.Fatalf("alert = %+v", s.alert())
	}
	h.prom.Set("cpu > 85")
	h.run(at(48 * 3600))
	if r = h.store.last(); r.Status != monitorcontract.EvalInactive || s.alert().Status != monitorcontract.AlertFiring {
		t.Fatalf("leaving the window must not touch state: %+v", r)
	}
}

func TestEvaluatorSecretHandling(t *testing.T) {
	t.Run("master key unavailable: only datasources that need a secret fail", func(t *testing.T) {
		h := newHarness(t, baseRule(func(r *Rule) { r.For = 0 }), fakeOpener{err: errors.New("credential master key unavailable")},
			func(d *Datasource) { d.AuthType, d.Secret = monitorcontract.AuthBearer, []byte("sealed-token") })
		open := testDatasource(testDS2, h.prom.URL)
		h.store.datasources = append(h.store.datasources, open)
		h.prom.Set("cpu > 85", promtest.S(91, "instance", "a"))
		h.runTasks(context.Background(), at(0), testDSID, testDS2)

		var failed, ok Round
		for _, r := range h.store.rounds {
			if r.Status == monitorcontract.EvalError {
				failed = r
			} else {
				ok = r
			}
		}
		if failed.Error != "datasource credential unavailable: master key missing or mismatched" || strings.Contains(failed.Error, "sealed-token") {
			t.Fatalf("failed round error = %q", failed.Error)
		}
		if ok.Status != monitorcontract.EvalFiring || len(h.store.sim(testDS2).alerts) != 1 || len(h.store.sim(testDSID).alerts) != 0 {
			t.Fatalf("the datasource without a secret must keep working: %+v", ok)
		}
		if h.prom.TotalHits() != 1 {
			t.Fatalf("the failing datasource must not be queried, hits = %d", h.prom.TotalHits())
		}
	})
	t.Run("secret is decrypted and sent, never recorded", func(t *testing.T) {
		h := newHarness(t, baseRule(func(r *Rule) { r.For = 0 }), fakeOpener{secrets: map[string]string{"sealed": "s3cr3t-token"}},
			func(d *Datasource) {
				d.AuthType, d.Secret, d.Headers = monitorcontract.AuthBearer, []byte("sealed"), map[string]string{"X-Scope-OrgID": "tenant-1"}
			})
		h.prom.SetError("cpu > 85", http.StatusUnauthorized, "unauthorized", "token s3cr3t-token rejected")
		h.run(at(0))
		hdr := h.prom.LastHeader()
		if hdr.Get("Authorization") != "Bearer s3cr3t-token" || hdr.Get("X-Scope-OrgID") != "tenant-1" {
			t.Fatalf("headers = %v", hdr)
		}
		if r := h.store.last(); r.Status != monitorcontract.EvalError || strings.Contains(r.Error, "s3cr3t") || !strings.Contains(r.Error, "***") {
			t.Fatalf("secret leaked or not masked: %q", r.Error)
		}
	})
	t.Run("invalid datasource url", func(t *testing.T) {
		h := newHarness(t, baseRule(), nil, func(d *Datasource) { d.URL = "ftp://user:pw@example/" })
		h.run(at(0))
		if r := h.store.last(); r.Status != monitorcontract.EvalError || !strings.HasPrefix(r.Error, "datasource config invalid:") || strings.Contains(r.Error, "pw") {
			t.Fatalf("error = %q", r.Error)
		}
	})
}

func TestEvaluatorUnevaluableTasksAreReleased(t *testing.T) {
	for name, breakIt := range map[string]func(h *harness){
		"rule deleted":        func(h *harness) { h.store.rule = nil },
		"rule disabled":       func(h *harness) { h.store.rule.Active = false },
		"datasource disabled": func(h *harness) { h.store.datasources[0].Enabled = false },
		"datasource deleted":  func(h *harness) { h.store.datasources = nil },
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, baseRule(), nil)
			breakIt(h)
			h.run(at(0))
			if len(h.store.rounds) != 0 || h.store.commits != 0 || h.prom.TotalHits() != 0 {
				t.Fatalf("nothing may be evaluated or written")
			}
			if len(h.store.released) != 1 || h.store.released[0].delay != retryDelay || h.obs.lastResult() != ResultSkipped {
				t.Fatalf("released = %+v, result = %q", h.store.released, h.obs.lastResult())
			}
		})
	}
}

func TestEvaluatorWriteBackFailures(t *testing.T) {
	t.Run("lease lost: round discarded, task not touched", func(t *testing.T) {
		h := newHarness(t, baseRule(), nil)
		h.prom.Set("cpu > 85", promtest.S(91, "instance", "a"))
		h.store.commitErr = ErrLeaseLost
		h.run(at(0))
		if h.obs.lastResult() != ResultLeaseLost || len(h.store.released) != 0 || len(h.store.rounds) != 0 {
			t.Fatalf("result = %q, released = %d", h.obs.lastResult(), len(h.store.released))
		}
	})
	t.Run("database failure: task released for a retry, loop keeps going", func(t *testing.T) {
		h := newHarness(t, baseRule(), nil)
		h.store.commitErr = errors.New("connection reset")
		h.run(at(0))
		if h.obs.lastResult() != ResultFailed || len(h.store.released) != 1 || h.store.released[0].delay != retryDelay {
			t.Fatalf("result = %q, released = %+v", h.obs.lastResult(), h.store.released)
		}
	})
}

func TestEvaluatorRejectsBadRuleData(t *testing.T) {
	h := newHarness(t, baseRule(func(r *Rule) { r.Queries[0].Severity = 9 }), nil)
	h.run(at(0))
	if r := h.store.last(); r.Status != monitorcontract.EvalError || r.Error != "rule query 0 has an invalid severity" || h.prom.TotalHits() != 0 {
		t.Fatalf("round = %+v", r)
	}
	h.store.rule.Queries = nil
	h.run(at(60))
	if r := h.store.last(); r.Error != "rule has no valid queries" {
		t.Fatalf("round = %+v", r)
	}
}

func TestEvaluatorGracefulStop(t *testing.T) {
	t.Run("cancelled before starting: claimed tasks go straight back", func(t *testing.T) {
		h := newHarness(t, baseRule(), nil)
		if err := h.cache.Refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		h.store.datasources = append(h.store.datasources, testDatasource(testDS2, h.prom.URL))
		h.runTasks(ctx, at(0), testDSID, testDS2)
		if len(h.store.released) != 2 || h.store.released[0].delay != 0 || len(h.store.rounds) != 0 || h.prom.TotalHits() != 0 {
			t.Fatalf("released = %+v, rounds = %d", h.store.released, len(h.store.rounds))
		}
	})
	t.Run("cancelled mid-flight: evaluations already started finish normally", func(t *testing.T) {
		h := newHarness(t, baseRule(func(r *Rule) { r.For = 0 }), nil)
		h.store.datasources = append(h.store.datasources, testDatasource(testDS2, h.prom.URL))
		h.prom.Set("cpu > 85", promtest.S(91, "instance", "a"))
		h.prom.SetDelay(150 * time.Millisecond)
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(40*time.Millisecond, cancel)
		h.runTasks(ctx, at(0), testDSID, testDS2)
		// 被取消的查询不得落成一轮查询错误。
		if len(h.store.rounds) != 2 || h.store.rounds[0].Status != monitorcontract.EvalFiring || h.store.rounds[1].Status != monitorcontract.EvalFiring {
			t.Fatalf("in-flight rounds must complete normally, got %+v", h.store.rounds)
		}
		if len(h.store.released) != 0 {
			t.Fatalf("released = %+v", h.store.released)
		}
	})
}

// E7：一批任务并发评估，一个慢任务不让同批其余任务排队（队头阻塞）。
func TestEvaluatorEvaluatesABatchConcurrently(t *testing.T) {
	h := newHarness(t, baseRule(func(r *Rule) { r.For = 0 }), nil)
	ids := []string{testDSID, testDS2, "018f3e1a-0000-7000-8000-0000000000e1", "018f3e1a-0000-7000-8000-0000000000e2"}
	for _, id := range ids[1:] {
		h.store.datasources = append(h.store.datasources, testDatasource(id, h.prom.URL))
	}
	h.prom.Set("cpu > 85", promtest.S(91, "instance", "a"))
	h.prom.SetDelay(200 * time.Millisecond)
	start := time.Now()
	h.runTasks(context.Background(), at(0), ids...)
	if got := h.prom.MaxInFlight(); got != len(ids) {
		t.Fatalf("max in-flight queries = %d, want %d (tasks of one batch must run concurrently)", got, len(ids))
	}
	if elapsed := time.Since(start); elapsed > 700*time.Millisecond {
		t.Fatalf("batch took %v: tasks ran one after another", elapsed)
	}
	if len(h.store.rounds) != len(ids) {
		t.Fatalf("rounds = %d", len(h.store.rounds))
	}
}

func TestEvaluatorMutesComeFromTheCache(t *testing.T) {
	h := newHarness(t, baseRule(func(r *Rule) { r.For = 0 }), nil)
	h.store.mutes = []*Mute{onceMute("m1", monitorcontract.MuteModeNotify, -60, 600)}
	h.prom.Set("cpu > 85", promtest.S(91, "instance", "a"))
	h.run(at(0))
	if a := h.store.sim(testDSID).alert(); !a.Muted || a.MuteID != "m1" {
		t.Fatalf("alert = %+v", a)
	}
	if r := h.store.last(); r.Detail.Points[0].Step != monitorcontract.StepMutedNotify || r.Detail.Points[0].MuteID != "m1" {
		t.Fatalf("points = %+v", r.Detail.Points)
	}
}

func TestConfigCache(t *testing.T) {
	prom := promtest.New(t)
	store := newFakeStore(t, baseRule(), testDatasource(testDSID, prom.URL), testDatasource(testDS2, prom.URL))
	cache, cleanup := NewConfigCache(store, fakeOpener{}, &fakeObserver{}, quietLogger())
	defer cleanup()
	ctx := context.Background()
	if cache.Datasource(testDSID) != nil || cache.EnabledDatasources() != nil || cache.Mutes(store.rule, testDSID, t0) != nil {
		t.Fatal("an unloaded cache must be empty")
	}
	store.cfgErr = errors.New("db down")
	if err := cache.Refresh(ctx); err == nil {
		t.Fatal("the first load must surface the error")
	}
	store.cfgErr = nil
	reload := func() {
		t.Helper()
		cache.lastPoll.Store(0) // 跳过轮询限频
		if err := cache.Refresh(ctx); err != nil {
			t.Fatal(err)
		}
	}
	reload()
	h1, h2 := cache.Datasource(testDSID), cache.Datasource(testDS2)
	if h1 == nil || h2 == nil || len(cache.EnabledDatasources()) != 2 || h1.Timeout() != 2*time.Second {
		t.Fatalf("handles = %v %v", h1, h2)
	}
	if err := h1.Ping(ctx); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}

	// 限频：轮询间隔内不读库。
	listed := store.listed
	if err := cache.Refresh(ctx); err != nil || store.listed != listed {
		t.Fatalf("refresh within the poll interval must be a no-op")
	}
	// 版本号未变：不重读。
	reload()
	if store.listed != listed || cache.Datasource(testDSID) != h1 {
		t.Fatalf("unchanged version must keep the snapshot")
	}
	// 数据源版本变化：只重建变更的那个，未变的沿用（复用连接）。
	store.versions.Datasource++
	store.datasources[0].Version, store.datasources[0].Enabled = 2, false
	reload()
	if cache.Datasource(testDSID) == h1 || cache.Datasource(testDS2) != h2 {
		t.Fatalf("only the changed datasource must be rebuilt")
	}
	if got := cache.Datasource(testDSID); got.Unavailable() != "datasource is disabled" || len(cache.EnabledDatasources()) != 1 {
		t.Fatalf("disabled handle = %+v", got)
	}
	if _, err := cache.Datasource(testDSID).Query(ctx, time.Time{}, "up"); err == nil {
		t.Fatal("a disabled datasource must not be queried")
	}
	// 旧 Handle 上在途的查询仍可完成。
	if err := h1.Ping(ctx); err != nil {
		t.Fatalf("a replaced handle must keep working: %v", err)
	}
	// 数据源被删除。
	store.versions.Datasource++
	store.datasources = store.datasources[1:]
	reload()
	if cache.Datasource(testDSID) != nil {
		t.Fatal("deleted datasource must disappear")
	}

	// 屏蔽规则：屏蔽版本或分组版本变化都会重读。
	if cache.Mutes(store.rule, testDSID, t0) != nil {
		t.Fatal("no mutes yet")
	}
	store.mutes = []*Mute{onceMute("m1", monitorcontract.MuteModeNotify, -60, 60)}
	reload()
	if cache.Mutes(store.rule, testDSID, t0) != nil {
		t.Fatal("mutes must not reload without a version bump")
	}
	store.versions.RuleGroup++
	reload()
	if id, mode := cache.Mutes(store.rule, testDSID, t0).Match(nil, 2); id != "m1" || mode != monitorcontract.MuteModeNotify {
		t.Fatalf("mute not loaded: %q %d", id, mode)
	}
	store.mutes = nil
	store.versions.Mute++
	reload()
	if cache.Mutes(store.rule, testDSID, t0) != nil {
		t.Fatal("mute must be gone")
	}

	// 刷新失败保留旧快照。
	store.versions.Datasource++
	store.cfgErr = errors.New("db down")
	reload()
	if cache.Datasource(testDS2) != h2 {
		t.Fatal("a failed refresh must keep the previous snapshot")
	}
	// 兜底：到了强制重读周期，版本号未变也整体重读。
	store.cfgErr = nil
	store.versions.Datasource--
	listed = store.listed
	snap := *cache.snap.Load()
	snap.loadedAt = time.Now().Add(-2 * cacheForceReload)
	cache.snap.Store(&snap)
	reload()
	if store.listed == listed {
		t.Fatal("periodic full reload did not happen")
	}
}

// 多条循环同时启动：只有一个真正读库，其余等它完成后拿到同一份快照。
func TestConfigCacheConcurrentFirstLoad(t *testing.T) {
	prom := promtest.New(t)
	store := newFakeStore(t, baseRule(), testDatasource(testDSID, prom.URL))
	cache, cleanup := NewConfigCache(store, fakeOpener{}, &fakeObserver{}, quietLogger())
	defer cleanup()
	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = cache.Refresh(context.Background())
			if errs[i] == nil && cache.Datasource(testDSID) == nil {
				errs[i] = errors.New("Refresh returned before the snapshot was visible")
			}
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("caller %d: %v", i, err)
		}
	}
	if store.listed != 1 {
		t.Fatalf("datasources were loaded %d times, want 1", store.listed)
	}
	// 从未加载成功且等待方的 ctx 已结束：返回 ctx 的错误，不无限等。
	empty, cleanup2 := NewConfigCache(store, fakeOpener{}, &fakeObserver{}, quietLogger())
	defer cleanup2()
	empty.refreshing.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := empty.Refresh(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestConfigCacheClampsTimeoutAndSeries(t *testing.T) {
	prom := promtest.New(t)
	ds := testDatasource(testDSID, prom.URL)
	ds.Timeout = 25 * time.Second
	store := newFakeStore(t, baseRule(), ds)
	cache, cleanup := NewConfigCache(store, fakeOpener{}, &fakeObserver{}, quietLogger())
	defer cleanup()
	cache.WithOptions(CacheOptions{MaxSeries: 2, QueryTimeoutMax: 3 * time.Second})
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	h := cache.Datasource(testDSID)
	if h.Timeout() != 3*time.Second {
		t.Fatalf("timeout = %v", h.Timeout())
	}
	prom.Set("up", promtest.S(1, "i", "a"), promtest.S(1, "i", "b"), promtest.S(1, "i", "c"))
	res, err := h.Query(context.Background(), time.Time{}, "up")
	if err != nil || len(res.Series) != 2 || !res.Truncated {
		t.Fatalf("res = %+v, err = %v", res, err)
	}
}

// E1：附加信息在事务之外渲染。Commit 的 decide（事务内）只做纯计算；E1 的攻击模板被校验拒绝，
// 该项落成 <template error>，评估照常完成。
func TestEvaluatorRendersAnnotationsOutsideTheTransaction(t *testing.T) {
	h := newHarness(t, baseRule(func(r *Rule) {
		r.For = 0
		r.Annotations = map[string]string{
			"summary":   "{{ $labels.instance }} = {{ $value }}",
			"assign":    `{{$labels = 9000000000000000000}}{{range $labels}}{{end}}`,
			"redeclare": `{{$labels := 9000000000000000000}}{{range $labels}}{{end}}`,
		}
	}), nil)
	var renders, inTx atomic.Int32
	h.uc.render = func(source map[string]string, samples []*Sample) map[string]map[string]string {
		renders.Add(1)
		inTx.Add(h.store.inTx.Load())
		return renderAnnotations(source, samples)
	}
	h.prom.Set("cpu > 85", promtest.S(91, "instance", "a"))
	done := make(chan struct{})
	go func() { defer close(done); h.run(at(0)) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the round did not finish: a malicious template is being executed")
	}
	if renders.Load() != 1 || inTx.Load() != 0 {
		t.Fatalf("renders = %d, of which inside the transaction = %d", renders.Load(), inTx.Load())
	}
	// 第一次提交带着「缺附加信息」回滚，渲染后第二次提交成功。
	if h.store.commits != 2 || len(h.store.rounds) != 1 {
		t.Fatalf("commits = %d, rounds = %d", h.store.commits, len(h.store.rounds))
	}
	a := h.store.sim(testDSID).alert()
	if a.Annotations["summary"] != "a = 91" || a.Annotations["assign"] != templateError || a.Annotations["redeclare"] != templateError {
		t.Fatalf("annotations = %v", a.Annotations)
	}
	if h.obs.lastResult() != ResultFiring {
		t.Fatalf("result = %q", h.obs.lastResult())
	}
	// 没有新事件的轮次：一次提交，不渲染。
	h.run(at(60))
	if renders.Load() != 1 || h.store.commits != 3 {
		t.Fatalf("steady round: renders = %d, commits = %d", renders.Load(), h.store.commits)
	}
	// 渲染结果一直对不上（不应发生）：重试有界，按失败处理并归还任务，而不是死循环。
	h.uc.render = func(map[string]string, []*Sample) map[string]map[string]string { return nil }
	h.prom.Set("cpu > 85", promtest.S(91, "instance", "a"), promtest.S(92, "instance", "b"))
	commits := h.store.commits
	h.run(at(120))
	if h.store.commits-commits != maxCommitAttempts || h.obs.lastResult() != ResultFailed || len(h.store.released) != 1 {
		t.Fatalf("commits = %d, result = %q, released = %d", h.store.commits-commits, h.obs.lastResult(), len(h.store.released))
	}
}

// E2 / E3：查询被截断或上游声明是部分结果时，没返回的序列既不恢复也不删除状态；任务与判定记录标注原因。
func TestEvaluatorIncompleteResultsNeverResolveOrDeleteState(t *testing.T) {
	series := func(names ...string) []promtest.Sample {
		out := make([]promtest.Sample, len(names))
		for i, n := range names {
			out[i] = promtest.S(90, "instance", n)
		}
		return out
	}
	for name, tc := range map[string]struct {
		// breakIt 让下一轮的结果不完整，且看不到 a（告警中）与 p（待触发）。
		breakIt  func(h *harness)
		wantNote string
		check    func(d monitorcontract.EvalQueryDetail) bool
	}{
		"truncated by max_series": {
			// 上限 3：新冒出来的 0a 0b 0c 按标签排序排在前面，把 a 与 p 挤出结果。
			breakIt:  func(h *harness) { h.prom.Set("cpu > 85", series("p", "a", "0c", "0b", "0a")...) },
			wantNote: "incomplete result: query 0 truncated to 3 series; series not returned were left unchanged",
			check:    func(d monitorcontract.EvalQueryDetail) bool { return d.Truncated && !d.Partial && d.Series == 3 },
		},
		"partial result (VictoriaMetrics isPartial)": {
			breakIt: func(h *harness) {
				h.prom.Set("cpu > 85")
				h.prom.SetPartial(true)
			},
			wantNote: "incomplete result: query 0 returned a partial result; series not returned were left unchanged",
			check:    func(d monitorcontract.EvalQueryDetail) bool { return d.Partial && !d.Truncated },
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, baseRule(func(r *Rule) { r.RecoveryExpr = `cpu{instance="{{ $labels.instance }}"}` }), nil)
			h.cache.WithOptions(CacheOptions{MaxSeries: 3})
			s := h.store.sim(testDSID)
			// a 先触发；p 之后出现，停在待触发。
			h.prom.Set("cpu > 85", series("a")...)
			h.run(at(0))
			h.run(at(60))
			s.dispatch(at(60))
			h.prom.Set("cpu > 85", series("a", "p")...)
			h.run(at(90))
			if len(s.states) != 2 || len(s.alerts) != 1 || s.alert().Status != monitorcontract.AlertFiring {
				t.Fatalf("precondition: states = %d, alerts = %d", len(s.states), len(s.alerts))
			}
			alertA := s.alert()
			before := map[string]SeriesState{}
			for fp, st := range s.states {
				before[fp] = st
			}

			tc.breakIt(h)
			for _, sec := range []int{150, 210, 270} {
				h.run(at(sec))
				r := h.store.last()
				if r.Error != tc.wantNote || r.Recovered != 0 || !tc.check(r.Detail.Queries[0]) {
					t.Fatalf("t+%d: round error = %q, recovered = %d, query = %+v", sec, r.Error, r.Recovered, r.Detail.Queries[0])
				}
				// 评估结果照常给出（有告警 = 告警中），不是查询错误。
				if r.Status != monitorcontract.EvalFiring || r.Transition == nil || h.obs.lastResult() != ResultFiring {
					t.Fatalf("t+%d: status = %d, result = %q", sec, r.Status, h.obs.lastResult())
				}
				if len(r.Transition.AlertResolves) != 0 || len(r.Transition.StateDeletes) != 0 {
					t.Fatalf("t+%d: an incomplete round resolved or deleted state: %+v", sec, r.Transition)
				}
				if alertA.Status != monitorcontract.AlertFiring {
					t.Fatalf("t+%d: the alert was resolved by an incomplete result", sec)
				}
				for fp, was := range before {
					now, ok := s.states[fp]
					if !ok || now.State != was.State || !now.LastSeen.Equal(was.LastSeen) || !now.RecoverSince.Equal(was.RecoverSince) {
						t.Fatalf("t+%d: state %s changed: %+v → %+v (exists = %v)", sec, fp, was, now, ok)
					}
				}
			}
			for _, o := range s.outbox {
				if o.Kind == monitorcontract.NotifyKindResolved {
					t.Fatalf("a recovery notification was queued: %+v", o)
				}
			}
			if h.prom.Hits(`cpu{instance="a"}`) != 0 {
				t.Fatalf("recovery value queries must not run on an incomplete round")
			}
			// 限频：进入不完整状态只记一条 WARN。
			if n := h.logs.count("level=WARN", "incomplete result"); n != 1 {
				t.Fatalf("incomplete-result warnings = %d, want 1\n%s", n, h.logs)
			}

			// 结果恢复完整且两条序列确实不在了：这时才恢复 / 清理。
			h.prom.SetPartial(false)
			h.prom.Set("cpu > 85")
			h.run(at(330))
			r := h.store.last()
			if r.Error != "" || int(r.Recovered) != len(s.alerts) || len(s.states) != 0 || alertA.Status != monitorcontract.AlertResolved {
				t.Fatalf("complete round: error = %q, recovered = %d, states = %d", r.Error, r.Recovered, len(s.states))
			}
			if n := h.logs.count("level=INFO", "complete results again"); n != 1 {
				t.Fatalf("recovery log lines = %d", n)
			}
		})
	}
}

// E11：标量 / 字符串结果与区间向量一样不是可判定的结果（标量恒有值，会无条件触发）。
func TestEvaluatorRejectsNonVectorResults(t *testing.T) {
	h := newHarness(t, baseRule(func(r *Rule) { r.For = 0; r.Queries[0].Expr = "time()" }), nil)
	h.prom.SetScalar("time()", 1.7e9)
	h.run(at(0))
	r := h.store.last()
	if r.Status != monitorcontract.EvalError || r.Error != "query 0: query must return an instant vector, got a scalar" || r.Transition != nil {
		t.Fatalf("round = status %d error %q", r.Status, r.Error)
	}
	if len(h.store.sim(testDSID).alerts) != 0 || h.store.commits != 0 || h.obs.lastResult() != ResultError {
		t.Fatalf("a scalar result must not fire anything")
	}
	// 规则自身的问题：进入查询错误的那一轮记一条 WARN，之后不再逐轮记。
	h.run(at(60))
	h.run(at(120))
	if n := h.logs.count("level=WARN", "rule evaluation is failing", "got a scalar"); n != 1 {
		t.Fatalf("warnings = %d, want 1\n%s", n, h.logs)
	}
}

// E16：数据源故障 / 查询超时有 WARN，但按数据源限频，不逐轮、不逐规则刷屏；恢复有 INFO。
func TestEvaluatorLogsDatasourceFailuresOncePerDatasource(t *testing.T) {
	h := newHarness(t, baseRule(func(r *Rule) { r.For = 0 }), nil, func(d *Datasource) { d.Timeout = time.Second })
	h.prom.Set("cpu > 85", promtest.S(91, "instance", "a"))
	h.run(at(0))
	if h.logs.count("level=WARN") != 0 {
		t.Fatalf("a healthy round must not warn:\n%s", h.logs)
	}
	h.prom.SetDown(true)
	for _, sec := range []int{60, 120, 180} {
		h.run(at(sec))
	}
	if n := h.logs.count("level=WARN", "datasource queries are failing", "datasource_id="+testDSID, "http 503"); n != 1 {
		t.Fatalf("datasource failure warnings = %d, want 1\n%s", n, h.logs)
	}
	h.prom.SetDown(false)
	h.run(at(240))
	if n := h.logs.count("level=INFO", "rule evaluation recovered"); n != 1 {
		t.Fatalf("recovery log lines = %d\n%s", n, h.logs)
	}
	// 查询超时同属数据源侧故障：十分钟内不再重复提醒。
	h.prom.SetDelay(3 * time.Second)
	h.run(at(300))
	if r := h.store.last(); r.Status != monitorcontract.EvalError || !strings.Contains(r.Error, "timeout") {
		t.Fatalf("round = %+v", r)
	}
	if n := h.logs.count("level=WARN", "datasource queries are failing"); n != 1 {
		t.Fatalf("warnings = %d, want 1 (rate limited)\n%s", n, h.logs)
	}
	// 提醒间隔过后再次记录。
	h.uc.upstream.failing[testDSID] = time.Now().Add(-failureRemind)
	h.run(at(360))
	if n := h.logs.count("level=WARN", "datasource queries are failing", "timeout"); n != 1 {
		t.Fatalf("reminder warnings = %d, want 1\n%s", n, h.logs)
	}
}

// E12：探活走专用槽位。评估查询把数据源的并发槽占满时，探活照常完成；排不上槽的评估任务归还重试，
// 不落查询错误。
func TestProbeIsNotStarvedByEvaluationQueries(t *testing.T) {
	h := newHarness(t, baseRule(func(r *Rule) { r.For = 0 }), nil, func(d *Datasource) { d.Timeout = time.Second })
	if err := h.cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	handle := h.cache.Datasource(testDSID)
	// 占满评估查询的全部并发槽（等同于 8 条慢查询在途）。
	for range cap(handle.slots) {
		handle.slots <- struct{}{}
	}
	repo := &fakePlannerRepo{}
	planner := NewPlannerUsecase(repo, h.cache, h.obs, quietLogger()).WithOptions(PlannerOptions{ProbeTimeout: 500 * time.Millisecond})
	start := time.Now()
	if _, err := planner.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(repo.health) != 1 || repo.health[0].Error != "" {
		t.Fatalf("a busy datasource was reported unhealthy: %+v", repo.health)
	}
	if time.Since(start) > 400*time.Millisecond {
		t.Fatalf("the probe waited %v behind evaluation queries", time.Since(start))
	}
	// 同一时刻的评估任务排不上槽：归还稍后重试，不记查询错误（不重置计时、不把数据源记成故障）。
	h.prom.Set("cpu > 85", promtest.S(91, "instance", "a"))
	h.run(at(0))
	if len(h.store.rounds) != 0 || h.obs.lastResult() != ResultBusy || len(h.store.released) != 1 || h.store.released[0].delay != retryDelay {
		t.Fatalf("rounds = %d, result = %q, released = %+v", len(h.store.rounds), h.obs.lastResult(), h.store.released)
	}
	if n := h.logs.count("level=WARN", "query slots are saturated"); n != 1 {
		t.Fatalf("saturation warnings = %d\n%s", n, h.logs)
	}
	for range cap(handle.slots) {
		<-handle.slots
	}
	h.run(at(60))
	if r := h.store.last(); r.Status != monitorcontract.EvalFiring {
		t.Fatalf("round after the slots were freed = %+v", r)
	}
}

// 链路本地 / 云元数据地址不是数据源：评估查询在连接之前被拒绝，记为查询错误。
func TestEvaluatorRefusesLinkLocalDatasources(t *testing.T) {
	h := newHarness(t, baseRule(func(r *Rule) { r.For = 0 }), nil, func(d *Datasource) { d.URL = "http://169.254.169.254/" })
	start := time.Now()
	h.run(at(0))
	r := h.store.last()
	if r.Status != monitorcontract.EvalError || !strings.Contains(r.Error, "promclient: blocked") ||
		!strings.Contains(r.Error, "169.254.169.254 is not allowed") || r.Transition != nil {
		t.Fatalf("round = status %d error %q", r.Status, r.Error)
	}
	// 被拒绝发生在 connect 之前：不会等到连接超时，也不会有任何请求发出。
	if time.Since(start) > time.Second || h.prom.TotalHits() != 0 {
		t.Fatalf("took %v, hits = %d", time.Since(start), h.prom.TotalHits())
	}
	repo := &fakePlannerRepo{}
	if _, err := NewPlannerUsecase(repo, h.cache, h.obs, quietLogger()).Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(repo.health) != 1 || !strings.Contains(repo.health[0].Error, "promclient: blocked") {
		t.Fatalf("health = %+v", repo.health)
	}
}

// E15：屏蔽规则超过加载上限时不再静默截断：WARN + 指标，保留先创建的。
func TestConfigCacheReportsTruncatedMutes(t *testing.T) {
	prom := promtest.New(t)
	store := newFakeStore(t, baseRule(), testDatasource(testDSID, prom.URL))
	for i := range MaxMutes + 5 {
		store.mutes = append(store.mutes, onceMute(fmt.Sprintf("m%06d", i), monitorcontract.MuteModeNotify, -60, 600, func(m *Mute) {
			m.Matchers = []monitorcontract.Matcher{{Label: "n", Op: monitorcontract.OpEqual, Value: fmt.Sprint(i)}}
		}))
	}
	obs := &fakeObserver{}
	logger, logs := captureLogger()
	cache, cleanup := NewConfigCache(store, fakeOpener{}, obs, logger)
	defer cleanup()
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.muteLimit != MaxMutes+1 {
		t.Fatalf("ListMutes limit = %d, want one more than the cap to detect the overflow", store.muteLimit)
	}
	if obs.mutesCut != 1 || logs.count("level=WARN", "too many active mute rules", fmt.Sprint(MaxMutes)) != 1 {
		t.Fatalf("truncation must be reported: metric = %d\n%s", obs.mutesCut, logs)
	}
	mutes := cache.Mutes(store.rule, testDSID, t0)
	if id, _ := mutes.Match(map[string]string{"n": fmt.Sprint(MaxMutes - 1)}, 2); id == "" {
		t.Fatal("the last rule within the limit must be in effect")
	}
	if id, _ := mutes.Match(map[string]string{"n": fmt.Sprint(MaxMutes)}, 2); id != "" {
		t.Fatal("rules beyond the limit are not loaded")
	}
	// 恰好在上限内：不告警。
	store.mutes = store.mutes[:MaxMutes]
	store.versions.Mute++
	cache.lastPoll.Store(0)
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if obs.mutesCut != 1 {
		t.Fatalf("a load within the limit must not be reported, metric = %d", obs.mutesCut)
	}
}

// E7 / E17：每轮的下一次评估时刻带抖动，任务到期的相位不会锁在上次评估它的实例的轮询节拍上。
func TestNextIntervalJitter(t *testing.T) {
	seen := map[time.Duration]struct{}{}
	for range 200 {
		d := nextInterval(15 * time.Second)
		if d < 15*time.Second || d > 15*time.Second+evalJitter {
			t.Fatalf("nextInterval = %v, want within [15s, 15s + %v]", d, evalJitter)
		}
		seen[d] = struct{}{}
	}
	if len(seen) < 100 {
		t.Fatalf("only %d distinct values in 200 draws: no jitter", len(seen))
	}
	// 抖动不超过周期的 1/10；脏数据的周期被夹到下限。
	if d := nextInterval(2 * time.Second); d < 2*time.Second || d > 2200*time.Millisecond {
		t.Fatalf("nextInterval(2s) = %v", d)
	}
	if d := nextInterval(0); d < minInterval || d > minInterval+minInterval/10 {
		t.Fatalf("nextInterval(0) = %v", d)
	}
}

func TestFailureLog(t *testing.T) {
	var l failureLog
	now := time.Now()
	steps := []struct {
		failed bool
		after  time.Duration
		want   bool
	}{
		{false, 0, false},                // 一直正常：不记
		{true, 0, true},                  // 正常 → 故障
		{true, time.Minute, false},       // 故障持续：静默
		{true, failureRemind, true},      // 到了提醒间隔
		{true, failureRemind + 1, false}, // 之后又静默
		{false, failureRemind + 2, true}, // 故障 → 正常
		{false, failureRemind + 3, false},
	}
	for i, st := range steps {
		if got := l.observe("k", st.failed, now.Add(st.after)); got != st.want {
			t.Fatalf("step %d: observe(failed = %v) = %v, want %v", i, st.failed, got, st.want)
		}
	}
	if len(l.failing) != 0 {
		t.Fatalf("healthy objects must not be tracked: %v", l.failing)
	}
	if !l.remind("d", now) || l.remind("d", now.Add(time.Minute)) || !l.remind("d", now.Add(failureRemind)) || !l.remind("other", now) {
		t.Fatal("remind must allow one line per object per interval")
	}
}
