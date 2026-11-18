package biz

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/kit/monitorpipeline"
	"github.com/lanyulei/reborn/backend/kit/promclient"
	"github.com/lanyulei/reborn/backend/monitor/internal/promtest"
)

// —— 配置缓存：资源索引 / 日历 / 抑制规则 / 升级策略 ——

func ciRow(id, ip, env string, sync time.Time, valid bool) CIRow {
	return CIRow{CI: hostCI(id, "host-"+id, ip, env, "running"), Valid: valid, SyncTime: sync}
}

func workCalendar() monitorcontract.CalendarSnapshot {
	return monitorcontract.CalendarSnapshot{
		Timezone: "Asia/Shanghai", Weekdays: []int32{1, 2, 3, 4, 5},
		Segments: []monitorcontract.CalendarSegment{{Start: "09:00", End: "18:00"}},
	}
}

func TestConfigCacheP2Caches(t *testing.T) {
	prom := promtest.New(t)
	store := newFakeStore(t, baseRule(), testDatasource(testDSID, prom.URL))
	obs := &fakeObserver{}
	cache, cleanup := NewConfigCache(store, fakeOpener{}, obs, quietLogger())
	defer cleanup()
	ctx := context.Background()
	if cache.CIs() != nil || cache.Calendar() != nil || cache.Inhibitor().Len() != 0 || cache.Escalation("e1") != nil || cache.Loaded() {
		t.Fatal("an unloaded cache must be empty")
	}
	reload := func() {
		t.Helper()
		cache.lastPoll.Store(0)
		if err := cache.Refresh(ctx); err != nil {
			t.Fatal(err)
		}
	}
	store.cis = []CIRow{ciRow("ci-1", "10.0.0.1", "prod", at(0), true), ciRow("ci-2", "10.0.0.2", "prod", at(0), true), ciRow("ci-9", "10.0.0.9", "prod", at(0), false)}
	reload()
	if !cache.Loaded() || cache.CIs().Len() != 2 || cache.CIs().Get("ci-9") != nil || len(store.ciCalls) != 1 || !store.ciCalls[0].IsZero() {
		t.Fatalf("first load: len = %d, calls = %v", cache.CIs().Len(), store.ciCalls)
	}
	if cache.Calendar() != nil {
		t.Fatal("an unmaterialised calendar means no calendar")
	}
	first := cache.CIs()

	t.Run("unchanged versions read nothing", func(t *testing.T) {
		reload()
		if len(store.ciCalls) != 1 || cache.CIs() != first {
			t.Fatalf("calls = %v", store.ciCalls)
		}
	})
	t.Run("ci index reloads incrementally on a version bump", func(t *testing.T) {
		store.mu.Lock()
		store.cis[0] = ciRow("ci-1", "10.0.0.1", "staging", at(60), true)              // 改了环境
		store.cis[1].Valid, store.cis[1].SyncTime = false, at(61)                      // 退役
		store.cis = append(store.cis, ciRow("ci-3", "10.0.0.3", "prod", at(62), true)) // 新增
		store.versions.CIIndex++
		store.mu.Unlock()
		reload()
		if len(store.ciCalls) != 2 || !store.ciCalls[1].Equal(at(0).Add(-ciSyncOverlap)) {
			t.Fatalf("the second read must be incremental from the cursor: %v", store.ciCalls)
		}
		cis := cache.CIs()
		if cis.Len() != 2 || cis.Get("ci-1").Env != "staging" || cis.Get("ci-2") != nil || cis.Get("ci-3") == nil {
			t.Fatalf("index after the incremental load: len = %d", cis.Len())
		}
		// 再 bump 一次但没有任何行变化：读了（回退窗口内的行），索引原样沿用。
		store.mu.Lock()
		store.versions.CIIndex++
		store.mu.Unlock()
		reload()
		if len(store.ciCalls) != 3 || cache.CIs() != cis {
			t.Fatalf("an incremental load without changes must keep the index: calls = %d", len(store.ciCalls))
		}
	})
	t.Run("a periodic full reload catches what increments cannot see", func(t *testing.T) {
		store.mu.Lock()
		store.cis = store.cis[:1] // 行被物理删除：增量读不到
		store.versions.CIIndex++  // 整表重读搭在下一次重读上（版本变化，或每分钟的兜底重读）
		store.mu.Unlock()
		cache.ci.fullAt = time.Now().Add(-ciFullReload)
		reload()
		if last := store.ciCalls[len(store.ciCalls)-1]; !last.IsZero() || cache.CIs().Len() != 1 {
			t.Fatalf("want a full reload: since = %v, len = %d", last, cache.CIs().Len())
		}
	})
	t.Run("memory is bounded by max_ci", func(t *testing.T) {
		small, done := NewConfigCache(store, fakeOpener{}, obs, quietLogger())
		defer done()
		small.WithOptions(CacheOptions{MaxCI: 2})
		store.mu.Lock()
		store.cis = []CIRow{ciRow("ci-3", "10.0.0.3", "prod", at(0), true), ciRow("ci-1", "10.0.0.1", "prod", at(0), true), ciRow("ci-2", "10.0.0.2", "prod", at(0), true)}
		store.mu.Unlock()
		if err := small.Refresh(ctx); err != nil {
			t.Fatal(err)
		}
		if small.CIs().Len() != 2 || small.CIs().Get("ci-3") != nil || obs.truncated[TruncatedCIs] != 1 {
			t.Fatalf("len = %d, truncated = %v", small.CIs().Len(), obs.truncated)
		}
	})
	t.Run("calendar", func(t *testing.T) {
		store.mu.Lock()
		store.calendar = workCalendar()
		store.versions.Calendar++
		store.mu.Unlock()
		reload()
		cal := cache.Calendar()
		// t0 = 周二 08:00 UTC = 16:00 上海。
		if cal == nil || !cal.Working(t0) || cal.Working(t0.Add(3*time.Hour)) {
			t.Fatalf("calendar = %v", cal)
		}
		store.mu.Lock()
		store.calendar.Timezone = "Nowhere/Invalid"
		store.versions.Calendar++
		store.mu.Unlock()
		reload()
		if cache.Calendar() != nil {
			t.Fatal("an invalid calendar means no calendar")
		}
	})
	t.Run("inhibit rules follow their own version and the group version", func(t *testing.T) {
		store.mu.Lock()
		store.inhibits = []monitorpipeline.InhibitRule{inhibitRule("i1", "a", "b", "host"), {ID: "broken"}}
		store.versions.Inhibit++
		store.mu.Unlock()
		reload()
		if cache.Inhibitor().Len() != 1 {
			t.Fatalf("valid inhibit rules = %d", cache.Inhibitor().Len())
		}
		store.mu.Lock()
		store.inhibits = nil
		store.versions.RuleGroup++
		store.mu.Unlock()
		reload()
		if cache.Inhibitor().Len() != 0 {
			t.Fatal("a group change must reload the inhibit rules")
		}
	})
	t.Run("escalation policies", func(t *testing.T) {
		good := escalationPolicy(monitorcontract.EscalationModeStack)
		for i := range good.Levels {
			good.Levels[i].Targets.UserIDs = []string{"u1"}
		}
		bad := *good
		bad.Levels = []monitorcontract.EscalationLevel{{Trigger: 9, AfterM: 1}}
		store.mu.Lock()
		store.escalations = []*Escalation{{ID: "e-good", Policy: *good}, {ID: "e-bad", Policy: bad}}
		store.versions.Escalation++
		store.mu.Unlock()
		reload()
		if p := cache.Escalation("e-good"); p == nil || len(p.Levels) != 2 {
			t.Fatalf("policy = %+v", p)
		}
		// 不合法的、不存在（已停用 / 已删除）的、未引用的：一律按没有策略。
		if cache.Escalation("e-bad") != nil || cache.Escalation("e-gone") != nil || cache.Escalation("") != nil {
			t.Fatal("invalid or missing policies must resolve to nil")
		}
	})
}

func TestInhibitCache(t *testing.T) {
	h := newHarness(t, baseRule(), nil)
	ctx := context.Background()
	h.store.inhibits = []monitorpipeline.InhibitRule{inhibitRule("i1", "a", "b", "host"), inhibitRule("i2", "b", "a", "host")}
	if err := h.cache.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if h.inh.View() != nil {
		t.Fatal("no view before the first load")
	}
	h.store.activeErr = errors.New("db down")
	if err := h.inh.Refresh(ctx); err == nil || h.inh.View() != nil {
		t.Fatalf("the first load must surface the error: %v", err)
	}
	h.store.activeErr = nil
	// 数据层按指纹顺序返回：缓存按事件 ID 排序后再算（环上最早触发的那个胜出）。
	h.store.active = []ActiveAlert{active("2", "kind", "b", "host", "h"), active("1", "kind", "a", "host", "h")}
	time.Sleep(time.Millisecond)
	if err := h.inh.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	v := h.inh.View()
	if got := inhibitedIDs(v, "1", "2"); got != "2" || v.Size() != 2 || h.obs.cycles != 1 {
		t.Fatalf("inhibited = %s, size = %d, cycles = %d", got, v.Size(), h.obs.cycles)
	}
	if h.logs.count("inhibit rules form a cycle") != 1 {
		t.Fatalf("logs = %s", h.logs.String())
	}
	// 之后的失败保留旧结论。
	h.store.activeErr = errors.New("db down")
	time.Sleep(time.Millisecond)
	if err := h.inh.Refresh(ctx); err != nil || h.inh.View() != v {
		t.Fatalf("a failed refresh must keep the previous view: %v", err)
	}
	h.store.activeErr = nil
	// 超过装入上限：记指标与日志。
	h.inh.WithOptions(InhibitOptions{MaxAlerts: 1})
	time.Sleep(time.Millisecond)
	if err := h.inh.Refresh(ctx); err != nil || h.inh.View().Size() != 1 || h.obs.truncated[TruncatedInhibitAlerts] != 1 {
		t.Fatalf("size = %d, truncated = %v, err = %v", h.inh.View().Size(), h.obs.truncated, err)
	}
	// 刷新周期之内不读库。
	h.inh.WithOptions(InhibitOptions{Refresh: time.Hour, MaxAlerts: 100})
	h.store.active = nil
	if err := h.inh.Refresh(ctx); err != nil || h.inh.View().Size() != 1 {
		t.Fatal("a refresh within the interval must be a no-op")
	}
}

// —— 评估器：第二期的输入来自缓存 ——

func TestEvaluatorWaitsForTheInhibitSnapshot(t *testing.T) {
	h := newHarness(t, baseRule(func(r *Rule) { r.For = 0 }), nil)
	h.prom.Set("cpu > 85", promtest.S(91, "instance", "a"))
	h.store.activeErr = errors.New("db down")
	h.store.due = []*Task{{RuleID: testRuleID, DatasourceID: testDSID}}
	// 没有抑制结论就评估，会把已被抑制的告警当成「抑制解除」补发通知：宁可不评估。
	if n, err := h.uc.RunOnce(context.Background(), "tok"); err == nil || n != 0 || len(h.store.due) != 1 || h.prom.TotalHits() != 0 {
		t.Fatalf("RunOnce() = %d, %v; tasks left = %d", n, err, len(h.store.due))
	}
	h.store.activeErr = nil
	if n, err := h.uc.RunOnce(context.Background(), "tok"); err != nil || n != 1 {
		t.Fatalf("RunOnce() = %d, %v", n, err)
	}
}

func TestEvaluatorWorkCalendar(t *testing.T) {
	for _, tc := range []struct {
		mode             string
		calendar         bool
		atWork, offHours int32 // 期望的评估结果：周二 16:00 / 19:00（上海）
	}{
		{monitorcontract.EffectiveModeWorkTime, true, monitorcontract.EvalFiring, monitorcontract.EvalInactive},
		{monitorcontract.EffectiveModeOffTime, true, monitorcontract.EvalInactive, monitorcontract.EvalFiring},
		// 日历尚未物化：按日历生效的规则恒生效（宁可多评估，不漏告警）。
		{monitorcontract.EffectiveModeWorkTime, false, monitorcontract.EvalFiring, monitorcontract.EvalFiring},
		{monitorcontract.EffectiveModeOffTime, false, monitorcontract.EvalFiring, monitorcontract.EvalFiring},
	} {
		t.Run(fmt.Sprintf("%s/calendar=%v", tc.mode, tc.calendar), func(t *testing.T) {
			h := newHarness(t, baseRule(func(r *Rule) { r.For, r.Effective = 0, monitorcontract.Effective{Mode: tc.mode} }), nil)
			if tc.calendar {
				h.store.calendar = workCalendar()
			}
			h.prom.Set("cpu > 85", promtest.S(91, "instance", "a"))
			h.run(t0)
			if got := h.store.last().Status; got != tc.atWork {
				t.Fatalf("16:00 status = %d, want %d", got, tc.atWork)
			}
			h.run(t0.Add(3 * time.Hour))
			if got := h.store.last().Status; got != tc.offHours {
				t.Fatalf("19:00 status = %d, want %d", got, tc.offHours)
			}
		})
	}
}

func TestEvaluatorRunsTheWholePipelineFromItsCaches(t *testing.T) {
	policy := escalationPolicy(monitorcontract.EscalationModeStack)
	for i := range policy.Levels {
		policy.Levels[i].Targets.UserIDs = []string{"u1"}
	}
	h := newHarness(t, baseRule(func(r *Rule) {
		r.For, r.EscalationID = 0, "esc-1"
		r.Labels = map[string]string{"kind": "cpu"}
		r.Annotations = map[string]string{"summary": "{{ $labels.instance }} in {{ $labels.ci_env }}"}
	}), nil)
	h.store.cis = []CIRow{ciRow("ci-1", "10.0.0.1", "prod", at(0), true)}
	h.store.escalations = []*Escalation{{ID: "esc-1", Policy: *policy}}
	h.store.inhibits = []monitorpipeline.InhibitRule{inhibitRule("i1", "down", "cpu", "instance")}
	h.store.active = []ActiveAlert{active("src-1", "kind", "down", "instance", "10.0.0.1:9100")}
	h.prom.Set("cpu > 85", promtest.S(91, "instance", "10.0.0.1:9100"))
	s := h.store.sim(testDSID)
	s.escalation = policy

	h.run(atM(0))
	a := s.alert()
	// 富化（附加信息能读到 ci_ 标签）、关联、抑制都生效；被抑制的事件不入箱。
	if a.CIID != "ci-1" || a.Labels["ci_env"] != "prod" || a.Annotations["summary"] != "10.0.0.1:9100 in prod" {
		t.Fatalf("alert = %+v", a)
	}
	if !a.Inhibited || a.Inhibit.AlertID != "src-1" || len(s.outbox) != 0 || h.store.inTx.Load() != 0 {
		t.Fatalf("alert = %+v, outbox = %v", a, s.kinds())
	}
	if h.store.escalationLoads == 0 {
		t.Fatal("a rule with an escalation policy must ask the data layer for the escalation progress")
	}
	// 源告警恢复：解除后通知；满 15 分钟未认领：升级（发件箱 kind 4，指标按 escalation 计）。
	h.store.active = nil
	h.run(atM(1))
	s.dispatch(atM(1))
	h.run(atM(16))
	if got := s.kinds(); !equalStrings(got, []string{"1/1", "4/100001"}) || h.obs.outbox[monitorcontract.NotifyKindEscalation] != 1 {
		t.Fatalf("outbox = %v, metrics = %v", got, h.obs.outbox)
	}
	if got := h.store.last().Transition.Outbox[0]; got.Level != 1 {
		t.Fatalf("outbox row = %+v", got)
	}
	// 策略被停用（缓存里没有了）：按没有策略处理，也不再读升级进度。
	loads := h.store.escalationLoads
	h.store.mu.Lock()
	h.store.escalations = nil
	h.store.versions.Escalation++
	h.store.mu.Unlock()
	h.cache.lastPoll.Store(0)
	s.dispatch(atM(16))
	h.run(atM(60))
	if h.store.escalationLoads != loads || len(s.kinds()) != 2 {
		t.Fatalf("a disabled policy still escalated: loads %d → %d, outbox = %v", loads, h.store.escalationLoads, s.kinds())
	}
}

func TestEvaluatorRejectsABrokenPipelineConfig(t *testing.T) {
	h := newHarness(t, baseRule(func(r *Rule) {
		r.Pipeline.Relabel = []monitorcontract.RelabelStep{{Action: "explode"}}
	}), nil)
	h.prom.Set("cpu > 85", promtest.S(91, "instance", "a"))
	h.run(at(0))
	r := h.store.last()
	if r.Status != monitorcontract.EvalError || !strings.HasPrefix(r.Error, "rule pipeline config is invalid") || h.prom.TotalHits() != 0 {
		t.Fatalf("round = %+v", r)
	}
}

func TestEvaluatorQueuesRuleRecoveredAfterAnErrorStreak(t *testing.T) {
	h := newHarness(t, baseRule(func(r *Rule) { r.For = 0 }), nil)
	h.prom.SetError("cpu > 85", http.StatusUnprocessableEntity, "bad_data", "parse error")
	h.run(at(0))
	h.run(at(60))
	if r := h.store.last(); r.Status != monitorcontract.EvalError || r.EngineEvent != nil {
		t.Fatalf("round = %+v", r)
	}
	h.prom.Set("cpu > 85", promtest.S(91, "instance", "a"))
	h.run(at(120))
	ev := h.store.last().EngineEvent
	if ev == nil || ev.Type != monitorcontract.EventEngineRuleRecovered || ev.ObjectID != testRuleID {
		t.Fatalf("event = %+v", ev)
	}
	// 与故障事件用同一个起始时刻配对，并且只在故障事件发过时才写。
	since := fmt.Sprint(at(0).UnixMicro())
	wantKey := monitorcontract.EventEngineRuleRecovered + ":" + testRuleID + ":" + testDSID + ":" + since
	wantReq := monitorcontract.EventEngineRuleFailing + ":" + testRuleID + ":" + testDSID + ":" + since
	if ev.DedupKey != wantKey || ev.Requires != wantReq {
		t.Fatalf("keys = %q / %q", ev.DedupKey, ev.Requires)
	}
	if d := ev.Detail; d.ObjectType != monitorcontract.EngineObjectRule || d.ObjectName != "cpu high" || d.Failures != 2 ||
		d.Duration != "2 分钟" || d.Since != "2026-11-17 08:00:00 +00:00" || d.DatasourceName == "" {
		t.Fatalf("detail = %+v", d)
	}
	// 下一轮：任务行上的错误记录已清，不再写。
	h.run(at(180))
	if h.store.last().EngineEvent != nil {
		t.Fatal("recovered event queued twice")
	}
}

// —— 记录规则 ——

type fakeRecordRepo struct {
	mu        sync.Mutex
	rule      *RecordRule
	now       time.Time
	due       []*Task
	released  []released
	finished  []RecordResult
	finishErr error
}

func (f *fakeRecordRepo) Claim(_ context.Context, token string, _ int, _, _, _ time.Duration) ([]*Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	tasks := f.due
	f.due = nil
	for _, t := range tasks {
		t.Token = token
	}
	return tasks, nil
}

func (f *fakeRecordRepo) Release(_ context.Context, _ string, tasks []*Task, delay time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, t := range tasks {
		f.released = append(f.released, released{t, delay})
	}
	return nil
}

func (f *fakeRecordRepo) LoadRecordRule(context.Context, string) (*RecordRule, time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.rule == nil {
		return nil, time.Time{}, ErrRuleNotFound
	}
	r := *f.rule
	return &r, f.now, nil
}

func (f *fakeRecordRepo) Finish(_ context.Context, _ *Task, res *RecordResult) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.finishErr != nil {
		return f.finishErr
	}
	f.finished = append(f.finished, *res)
	return nil
}

type recordHarness struct {
	t     *testing.T
	repo  *fakeRecordRepo
	prom  *promtest.Server
	recv  *promtest.WriteReceiver
	obs   *fakeObserver
	uc    *RecordUsecase
	stats *InstanceStats
}

const recordExpr = "sum by (instance) (rate(http_requests_total[5m]))"

func newRecordHarness(t *testing.T, maxSeries int, mod ...func(*Datasource)) *recordHarness {
	t.Helper()
	prom, recv := promtest.New(t), promtest.NewWriteReceiver(t)
	ds := testDatasource(testDSID, prom.URL)
	ds.RemoteWriteURL = recv.URL
	for _, m := range mod {
		m(ds)
	}
	store := newFakeStore(t, baseRule(), ds)
	obs := &fakeObserver{}
	cache, cleanup := NewConfigCache(store, fakeOpener{}, obs, quietLogger())
	t.Cleanup(cleanup)
	cache.WithOptions(CacheOptions{MaxSeries: maxSeries})
	repo := &fakeRecordRepo{now: t0, rule: &RecordRule{
		ID: testRuleID, Name: "instance:http_requests:rate5m", DatasourceID: testDSID, Expr: recordExpr, Interval: time.Minute,
		Labels: map[string]string{"source": "reborn", "job": ""}, Active: true,
	}}
	stats := NewInstanceStats()
	return &recordHarness{
		t: t, repo: repo, prom: prom, recv: recv, obs: obs, stats: stats,
		uc: NewRecordUsecase(repo, cache, stats, obs, quietLogger()),
	}
}

// run 执行一轮并返回写回任务的结果（没有写回时为 nil）与指标里的结果标签。
func (h *recordHarness) run() (*RecordResult, string) {
	h.t.Helper()
	h.repo.mu.Lock()
	before := len(h.repo.finished)
	h.repo.due = []*Task{{RuleID: testRuleID, DatasourceID: testDSID}}
	h.repo.mu.Unlock()
	if n, err := h.uc.RunOnce(context.Background(), "tok"); err != nil || n != 1 {
		h.t.Fatalf("RunOnce() = %d, %v", n, err)
	}
	h.repo.mu.Lock()
	defer h.repo.mu.Unlock()
	result := h.obs.records[len(h.obs.records)-1]
	if len(h.repo.finished) == before {
		return nil, result
	}
	return &h.repo.finished[len(h.repo.finished)-1], result
}

func TestRecordUsecase(t *testing.T) {
	t.Run("writes the result under the rule's metric name and labels", func(t *testing.T) {
		h := newRecordHarness(t, 0)
		h.prom.Set(recordExpr,
			promtest.S(1.5, "__name__", "ignored", "instance", "a", "job", "api"),
			promtest.S(2.5, "instance", "b", "job", "api"))
		res, result := h.run()
		if res == nil || res.Status != monitorcontract.RecordOK || res.Series != 2 || res.Error != "" || result != ResultOK {
			t.Fatalf("result = %+v, %s", res, result)
		}
		if res.Interval < time.Minute || res.Interval > time.Minute+evalJitter {
			t.Fatalf("next run in %v", res.Interval)
		}
		got := h.recv.Series()
		if len(h.recv.Problems()) != 0 || len(got) != 2 {
			t.Fatalf("problems = %v, series = %+v", h.recv.Problems(), got)
		}
		slices.SortFunc(got, func(a, b promtest.WrittenSeries) int {
			return strings.Compare(a.Labels["instance"], b.Labels["instance"])
		})
		// 原指标名被换掉；规则标签覆盖进来；取值为空的规则标签把该标签删掉。
		want := map[string]string{"__name__": "instance:http_requests:rate5m", "instance": "a", "source": "reborn"}
		if fmt.Sprint(got[0].Labels) != fmt.Sprint(want) || got[0].Value != 1.5 || got[1].Value != 2.5 || got[0].TimestampMS <= 0 {
			t.Fatalf("series = %+v", got)
		}
		if h.obs.written != 2 || h.stats.evals.Load() != 1 || h.stats.fails.Load() != 0 {
			t.Fatalf("written = %d, stats = %d / %d", h.obs.written, h.stats.evals.Load(), h.stats.fails.Load())
		}
	})
	t.Run("scalar results are written as a single series", func(t *testing.T) {
		h := newRecordHarness(t, 0)
		h.prom.SetScalar(recordExpr, 42)
		res, _ := h.run()
		got := h.recv.Series()
		if res.Status != monitorcontract.RecordOK || len(got) != 1 || got[0].Value != 42 ||
			fmt.Sprint(got[0].Labels) != fmt.Sprint(map[string]string{"__name__": "instance:http_requests:rate5m", "source": "reborn"}) {
			t.Fatalf("result = %+v, series = %+v", res, got)
		}
	})
	t.Run("no data writes nothing", func(t *testing.T) {
		h := newRecordHarness(t, 0)
		res, result := h.run()
		if res.Status != monitorcontract.RecordNoData || result != ResultNoData || len(h.recv.Requests()) != 0 {
			t.Fatalf("result = %+v, requests = %d", res, len(h.recv.Requests()))
		}
	})
	t.Run("query errors", func(t *testing.T) {
		h := newRecordHarness(t, 0)
		h.prom.SetError(recordExpr, http.StatusUnprocessableEntity, "bad_data", "parse error")
		res, result := h.run()
		if res.Status != monitorcontract.RecordQueryError || result != ResultError || !strings.Contains(res.Error, "parse error") || len(h.recv.Requests()) != 0 {
			t.Fatalf("result = %+v", res)
		}
		if h.stats.fails.Load() != 1 {
			t.Fatalf("fails = %d", h.stats.fails.Load())
		}
	})
	t.Run("an incomplete result is never written, not even partly", func(t *testing.T) {
		h := newRecordHarness(t, 2)
		h.prom.Set(recordExpr, promtest.S(1, "instance", "a"), promtest.S(2, "instance", "b"), promtest.S(3, "instance", "c"))
		res, result := h.run()
		if res.Status != monitorcontract.RecordQueryError || result != ResultIncomplete || len(h.recv.Requests()) != 0 ||
			res.Error != incompletePrefix+"the result was truncated to 2 series; nothing was written" {
			t.Fatalf("truncated: result = %+v, %s", res, result)
		}
		h = newRecordHarness(t, 0)
		h.prom.Set(recordExpr, promtest.S(1, "instance", "a"))
		h.prom.SetPartial(true)
		res, result = h.run()
		if res.Status != monitorcontract.RecordQueryError || result != ResultIncomplete || len(h.recv.Requests()) != 0 ||
			!strings.HasPrefix(res.Error, incompletePrefix) {
			t.Fatalf("partial: result = %+v, %s", res, result)
		}
	})
	t.Run("write failures", func(t *testing.T) {
		h := newRecordHarness(t, 0)
		h.prom.Set(recordExpr, promtest.S(1, "instance", "a"))
		h.recv.SetStatus(http.StatusInternalServerError)
		res, result := h.run()
		if res.Status != monitorcontract.RecordWriteError || result != ResultWriteError || res.Series != 0 ||
			!strings.Contains(res.Error, "wrote 0 of 1 series") || !strings.Contains(res.Error, "500") {
			t.Fatalf("result = %+v", res)
		}
		// 没有配置写入地址。
		h = newRecordHarness(t, 0, func(d *Datasource) { d.RemoteWriteURL = "" })
		h.prom.Set(recordExpr, promtest.S(1, "instance", "a"))
		if res, _ := h.run(); res.Status != monitorcontract.RecordWriteError || !strings.Contains(res.Error, "remote write url is not configured") {
			t.Fatalf("result = %+v", res)
		}
		// 写入地址非法：查询照常可用，写回以固定文案报错（不回显地址）。
		h = newRecordHarness(t, 0, func(d *Datasource) { d.RemoteWriteURL = "ftp://user:pw@example.com/x" })
		h.prom.Set(recordExpr, promtest.S(1, "instance", "a"))
		if res, _ := h.run(); res.Status != monitorcontract.RecordWriteError || !strings.HasPrefix(res.Error, "datasource remote write url is invalid") ||
			strings.Contains(res.Error, "pw") {
			t.Fatalf("result = %+v", res)
		}
	})
	t.Run("series whose labels cannot form a valid metric are rejected and reported", func(t *testing.T) {
		h := newRecordHarness(t, 0)
		h.prom.Set(recordExpr, promtest.S(1, "instance", "a"), promtest.S(2, "instance", strings.Repeat("x", maxLabelValueBytes+1)),
			promtest.S(3, "bad-name", "v"), promtest.S(4, "__reserved", "v"))
		res, _ := h.run()
		if res.Status != monitorcontract.RecordOK || res.Series != 1 || len(h.recv.Series()) != 1 ||
			res.Error != "3 series were not written: their labels do not form a valid metric" {
			t.Fatalf("result = %+v", res)
		}
		// 一条都写不出去：写回失败，而不是「查询无结果」。
		h.prom.Set(recordExpr, promtest.S(3, "bad-name", "v"))
		if res, result := h.run(); res.Status != monitorcontract.RecordWriteError || result != ResultWriteError ||
			res.Error != "1 series were not written: their labels do not form a valid metric" {
			t.Fatalf("result = %+v, %s", res, result)
		}
	})
	t.Run("series that collapse onto the same labels fail the round", func(t *testing.T) {
		h := newRecordHarness(t, 0)
		h.prom.Set(recordExpr, promtest.S(1, "instance", "a", "job", "x"), promtest.S(2, "instance", "a", "job", "y"))
		res, result := h.run()
		if res.Status != monitorcontract.RecordQueryError || result != ResultError || res.Error != errDuplicateSeries.Error() || len(h.recv.Requests()) != 0 {
			t.Fatalf("result = %+v", res)
		}
	})
	t.Run("large results are written in bounded chunks", func(t *testing.T) {
		h := newRecordHarness(t, 5000)
		samples := make([]promtest.Sample, 1200)
		for i := range samples {
			samples[i] = promtest.S(float64(i), "instance", fmt.Sprintf("host-%04d", i))
		}
		h.prom.Set(recordExpr, samples...)
		res, _ := h.run()
		reqs := h.recv.Requests()
		if res.Status != monitorcontract.RecordOK || res.Series != 1200 || len(reqs) != 3 || len(reqs[0]) != recordWriteSeries || len(reqs[2]) != 200 {
			t.Fatalf("result = %+v, requests = %d", res, len(reqs))
		}
	})
	t.Run("rules that cannot run are released for the planner to clean up", func(t *testing.T) {
		for name, mod := range map[string]func(*recordHarness){
			"rule deleted":  func(h *recordHarness) { h.repo.rule = nil },
			"rule disabled": func(h *recordHarness) { h.repo.rule.Active = false },
			"datasource gone": func(h *recordHarness) {
				h.repo.rule.DatasourceID = "018f3e1a-0000-7000-8000-0000000000ff"
			},
		} {
			h := newRecordHarness(t, 0)
			mod(h)
			res, result := h.run()
			if res != nil || result != ResultSkipped || len(h.repo.released) != 1 || h.repo.released[0].delay != retryDelay || h.prom.TotalHits() != 0 {
				t.Fatalf("%s: result = %+v, %s, released = %d", name, res, result, len(h.repo.released))
			}
		}
	})
	t.Run("fenced write-back and repository failures", func(t *testing.T) {
		h := newRecordHarness(t, 0)
		h.prom.Set(recordExpr, promtest.S(1, "instance", "a"))
		h.repo.finishErr = ErrLeaseLost
		if _, result := h.run(); result != ResultLeaseLost || len(h.repo.released) != 0 {
			t.Fatalf("result = %s", result)
		}
		h.repo.finishErr = errors.New("db down")
		if _, result := h.run(); result != ResultFailed || len(h.repo.released) != 1 {
			t.Fatalf("result = %s, released = %d", result, len(h.repo.released))
		}
	})
	t.Run("a name that is not a metric name is refused", func(t *testing.T) {
		h := newRecordHarness(t, 0)
		h.repo.rule.Name = "not a metric"
		h.prom.Set(recordExpr, promtest.S(1, "instance", "a"))
		if res, _ := h.run(); res.Status != monitorcontract.RecordQueryError || h.prom.TotalHits() != 0 {
			t.Fatalf("result = %+v", res)
		}
	})
}

func TestChunkSeries(t *testing.T) {
	mk := func(n, valueBytes int) []promclient.Series {
		out := make([]promclient.Series, n)
		for i := range out {
			out[i] = promclient.Series{Labels: map[string]string{"__name__": "m", "v": strings.Repeat("x", valueBytes)}}
		}
		return out
	}
	if got := chunkSeries(mk(1, 10)); len(got) != 1 || len(got[0]) != 1 {
		t.Fatalf("chunks = %d", len(got))
	}
	if got := chunkSeries(mk(recordWriteSeries*2+1, 10)); len(got) != 3 || len(got[2]) != 1 {
		t.Fatalf("chunks = %d", len(got))
	}
	// 按字节切：每条约 1 KiB，一个分片装不下 recordWriteSeries 条时提前切。
	big := chunkSeries(mk(recordWriteSeries, 16<<10))
	total := 0
	for _, c := range big {
		total += len(c)
		if len(c)*(16<<10) > recordWriteBytes+(16<<10) {
			t.Fatalf("a chunk carries %d series of 16KiB", len(c))
		}
	}
	if len(big) < 2 || total != recordWriteSeries {
		t.Fatalf("chunks = %d, total = %d", len(big), total)
	}
}

// —— 自监控 ——

type fakeWatchRepo struct {
	outages    []DatasourceOutage
	failures   []RuleFailure
	events     []*EngineEventInsert
	sinces     []time.Time
	refuse     map[string]bool // 去重键 → 写入被拒（状态已翻转 / 别的实例写过了）
	err        error
	pruned     int
	pruneStale time.Duration
	args       []any
}

func (f *fakeWatchRepo) DatasourceOutages(_ context.Context, after time.Duration, limit int) ([]DatasourceOutage, error) {
	f.args = append(f.args, after, limit)
	return f.outages, f.err
}

func (f *fakeWatchRepo) report(since time.Time, ev *EngineEventInsert) (bool, error) {
	if f.refuse[ev.DedupKey] {
		return false, nil
	}
	f.events, f.sinces = append(f.events, ev), append(f.sinces, since)
	return true, nil
}

func (f *fakeWatchRepo) ReportDatasourceDown(_ context.Context, since time.Time, ev *EngineEventInsert) (bool, error) {
	return f.report(since, ev)
}

func (f *fakeWatchRepo) RuleFailures(_ context.Context, after time.Duration, minFailures, limit int) ([]RuleFailure, error) {
	f.args = append(f.args, after, minFailures, limit)
	return f.failures, nil
}

func (f *fakeWatchRepo) ReportRuleFailing(_ context.Context, _ string, since time.Time, ev *EngineEventInsert) (bool, error) {
	return f.report(since, ev)
}

func (f *fakeWatchRepo) PruneInstances(_ context.Context, stale time.Duration) (int, error) {
	f.pruneStale = stale
	return f.pruned, nil
}

func TestWatchUsecase(t *testing.T) {
	since := time.Date(2026, 11, 17, 7, 0, 0, 123456000, time.UTC)
	now := since.Add(3*time.Hour + 20*time.Minute)
	repo := &fakeWatchRepo{
		outages: []DatasourceOutage{{DatasourceID: "ds-1", Name: "vm-prod", Error: "connection refused", Since: since, Now: now}},
		failures: []RuleFailure{
			{RuleID: "r-1", RuleName: "cpu", DatasourceID: "ds-2", DatasourceName: "vm-test", Error: "parse error", Since: since, Now: now, Failures: 40},
			{RuleID: "r-2", RuleName: "mem", DatasourceID: "ds-2", DatasourceName: "vm-test", Since: since, Now: now, Failures: 3},
		},
		pruned: 2,
	}
	// 第二条规则的事件已被别的实例写过（或状态刚翻转）：不计数。
	repo.refuse = map[string]bool{engineEventKey(monitorcontract.EventEngineRuleFailing, since, "r-2", "ds-2"): true}
	obs := &fakeObserver{}
	uc := NewWatchUsecase(repo, obs, quietLogger())
	n, err := uc.RunOnce(context.Background())
	if err != nil || n != 2 || len(repo.events) != 2 {
		t.Fatalf("RunOnce() = %d, %v; events = %d", n, err, len(repo.events))
	}
	// 缺省阈值：数据源 5 分钟；规则 10 分钟且至少 3 次；心跳 1 小时。
	if fmt.Sprint(repo.args) != fmt.Sprint([]any{5 * time.Minute, watchBatch, 10 * time.Minute, 3, watchBatch}) || repo.pruneStale != time.Hour {
		t.Fatalf("thresholds = %v, stale = %v", repo.args, repo.pruneStale)
	}
	micros := fmt.Sprint(since.UnixMicro())
	ds, rule := repo.events[0], repo.events[1]
	if ds.Type != monitorcontract.EventEngineDatasourceDown || ds.ObjectID != "ds-1" || ds.Requires != "" ||
		ds.DedupKey != "monitor.engine.datasource_down:ds-1:"+micros || !repo.sinces[0].Equal(since) {
		t.Fatalf("datasource event = %+v", ds)
	}
	want := monitorcontract.EngineEventPayload{
		ObjectType: "datasource", ObjectID: "ds-1", ObjectName: "vm-prod", Error: "connection refused",
		Since: "2026-11-17 07:00:00 +00:00", Duration: "3 小时 20 分钟",
	}
	if ds.Detail != want {
		t.Fatalf("detail = %+v", ds.Detail)
	}
	if rule.Type != monitorcontract.EventEngineRuleFailing || rule.ObjectID != "r-1" || rule.DedupKey != "monitor.engine.rule_failing:r-1:ds-2:"+micros ||
		rule.Detail.ObjectType != "rule" || rule.Detail.DatasourceName != "vm-test" || rule.Detail.Failures != 40 || rule.Detail.Error != "parse error" {
		t.Fatalf("rule event = %+v", rule)
	}
	if len(ds.DedupKey) > 160 || len(rule.DedupKey) > 160 {
		t.Fatal("dedup keys must fit varchar(160)")
	}
	if obs.engineEvs[monitorcontract.EventEngineDatasourceDown] != 1 || obs.engineEvs[monitorcontract.EventEngineRuleFailing] != 1 {
		t.Fatalf("metrics = %v", obs.engineEvs)
	}
	// 配置的阈值；一项失败不影响其余项。
	repo.err, repo.args, repo.events = errors.New("db down"), nil, nil
	uc.WithOptions(WatchOptions{DatasourceDownAfter: time.Minute, RuleFailingAfter: 2 * time.Minute, RuleFailingMinFailures: 5, InstanceStaleAfter: time.Minute})
	n, err = uc.RunOnce(context.Background())
	if err == nil || n != 1 || len(repo.events) != 1 || repo.pruneStale != time.Minute ||
		fmt.Sprint(repo.args) != fmt.Sprint([]any{time.Minute, watchBatch, 2 * time.Minute, 5, watchBatch}) {
		t.Fatalf("RunOnce() = %d, %v; args = %v", n, err, repo.args)
	}
}

func TestHumanDuration(t *testing.T) {
	for d, want := range map[time.Duration]string{
		0: "0 秒", 45 * time.Second: "45 秒", 90 * time.Second: "1 分钟 30 秒", time.Hour: "1 小时",
		25*time.Hour + 30*time.Minute: "1 天 1 小时", 2*time.Hour + 5*time.Second: "2 小时", -time.Minute: "0 秒",
	} {
		if got := humanDuration(d); got != want {
			t.Errorf("humanDuration(%v) = %q, want %q", d, got, want)
		}
	}
}

func TestPlannerProbeQueuesRecoveryEvents(t *testing.T) {
	up := promtest.New(t)
	store := newFakeStore(t, baseRule(), testDatasource(testDSID, up.URL))
	obs := &fakeObserver{}
	cache, cleanup := NewConfigCache(store, fakeOpener{}, obs, quietLogger())
	defer cleanup()
	since := at(-3600)
	repo := &fakePlannerRepo{recoveries: []HealthRecovery{{DatasourceID: testDSID, Name: "prom-c", Since: since, Now: at(0)}}}
	if _, err := NewPlannerUsecase(repo, cache, obs, quietLogger()).Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(repo.events) != 1 || obs.engineEvs[monitorcontract.EventEngineDatasourceUp] != 1 {
		t.Fatalf("events = %d, metrics = %v", len(repo.events), obs.engineEvs)
	}
	ev := repo.events[0]
	micros := fmt.Sprint(since.UnixMicro())
	if ev.Type != monitorcontract.EventEngineDatasourceUp || ev.DedupKey != "monitor.engine.datasource_recovered:"+testDSID+":"+micros ||
		ev.Requires != "monitor.engine.datasource_down:"+testDSID+":"+micros || ev.Detail.Duration != "1 小时" || ev.Detail.Error != "" {
		t.Fatalf("event = %+v", ev)
	}
}

func TestPlannerMaterializesRecordTasks(t *testing.T) {
	repo := &fakePlannerRepo{
		results: []MaterializeResult{{}, {}},
		records: []MaterializeResult{{Added: 1, Removed: 2}, {Removed: 1}},
	}
	uc := NewPlannerUsecase(repo, nil, &fakeObserver{}, quietLogger()).WithOptions(PlannerOptions{MaterializeBatch: 2})
	if n, err := uc.Materialize(context.Background()); err != nil || n != 2 {
		t.Fatalf("a full batch of removed record tasks must ask for another round: %d, %v", n, err)
	}
	if n, err := uc.Materialize(context.Background()); err != nil || n != 0 {
		t.Fatalf("n = %d, err = %v", n, err)
	}
}

type fakeInstanceRepo struct {
	beats   []InstanceBeat
	err     error
	removed []string
}

func (f *fakeInstanceRepo) Beat(_ context.Context, b *InstanceBeat) error {
	if f.err != nil {
		return f.err
	}
	f.beats = append(f.beats, *b)
	return nil
}

func (f *fakeInstanceRepo) Remove(_ context.Context, instance string) error {
	f.removed = append(f.removed, instance)
	return nil
}

func TestHeartbeat(t *testing.T) {
	repo, stats := &fakeInstanceRepo{}, NewInstanceStats()
	info := InstanceInfo{ID: "host-abc123", Roles: "planner,evaluator", Version: "v1.2.3"}
	uc, cleanup := NewHeartbeatUsecase(repo, info, stats, quietLogger())
	ctx := context.Background()
	for _, r := range []struct {
		result string
		lag    time.Duration
	}{
		{ResultOK, 100 * time.Millisecond},
		{ResultFiring, 2 * time.Second},
		{ResultError, time.Second},
		{ResultWriteError, 0},
		// 没有完成评估的不计。
		{ResultBusy, time.Minute},
		{ResultSkipped, time.Minute},
		{ResultLeaseLost, time.Minute},
	} {
		stats.observe(r.result, r.lag)
	}
	if err := uc.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	want := InstanceBeat{
		Instance: "host-abc123", Roles: "planner,evaluator", Version: "v1.2.3",
		TaskCount: 4, EvalLag: 2 * time.Second, EvalCount: 4, FailCount: 2,
	}
	if repo.beats[0] != want {
		t.Fatalf("beat = %+v", repo.beats[0])
	}
	// 窗口计数在写成功后清零，累计值保留；失败的心跳不丢窗口里的数。
	stats.observe(ResultOK, 300*time.Millisecond)
	repo.err = errors.New("db down")
	if err := uc.RunOnce(ctx); err == nil {
		t.Fatal("want an error")
	}
	repo.err = nil
	stats.MarkLeader()
	if err := uc.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if b := repo.beats[1]; b.TaskCount != 1 || b.EvalLag != 300*time.Millisecond || b.EvalCount != 5 || b.FailCount != 2 || !b.Leader {
		t.Fatalf("beat = %+v", b)
	}
	// 领导权过期（很久没有以主身份执行）。
	uc.WithOptions(HeartbeatOptions{LeaderTTL: time.Nanosecond})
	time.Sleep(time.Millisecond)
	if err := uc.RunOnce(ctx); err != nil || repo.beats[2].Leader || repo.beats[2].TaskCount != 0 {
		t.Fatalf("beat = %+v, err = %v", repo.beats[2], err)
	}
	cleanup()
	if !slices.Equal(repo.removed, []string{"host-abc123"}) {
		t.Fatalf("removed = %v", repo.removed)
	}
}

// —— 事件指令 ——

type fakeCommandRepo struct {
	pending []AlertCommand
	outcome map[string]error // 指令 ID → CloseAlert 的错误
	open    map[string]bool  // 指令 ID → 事件仍在告警中
	closed  []string
	listErr error
	limit   int
}

func (f *fakeCommandRepo) PendingCommands(_ context.Context, limit int) ([]AlertCommand, error) {
	f.limit = limit
	return f.pending[:min(len(f.pending), limit)], f.listErr
}

func (f *fakeCommandRepo) CloseAlert(_ context.Context, cmd AlertCommand, newID func() (string, error)) (bool, error) {
	if err := f.outcome[cmd.ID]; err != nil {
		return false, err
	}
	if _, err := newID(); err != nil {
		return false, err
	}
	if f.open[cmd.ID] {
		f.closed = append(f.closed, cmd.AlertID)
	}
	return f.open[cmd.ID], nil
}

func TestCommandUsecase(t *testing.T) {
	cmd := func(id string, kind int32) AlertCommand {
		return AlertCommand{ID: id, AlertID: "alert-" + id, Kind: kind}
	}
	repo := &fakeCommandRepo{
		pending: []AlertCommand{
			cmd("1", monitorcontract.AlertCmdClose), cmd("2", monitorcontract.AlertCmdClose), cmd("3", 99),
			cmd("4", monitorcontract.AlertCmdClose), cmd("5", monitorcontract.AlertCmdClose),
		},
		open:    map[string]bool{"1": true, "5": true},
		outcome: map[string]error{"4": ErrCommandGone, "5": errors.New("deadlock")},
	}
	obs := &fakeObserver{}
	uc := NewCommandUsecase(repo, obs, quietLogger())
	n, err := uc.RunOnce(context.Background())
	// 1 关闭；2 事件已不在告警中；3 不认识的指令留给认识它的版本；4 被别的实例处理了；5 失败、仍待处理。
	if err != nil || n != 0 || !slices.Equal(repo.closed, []string{"alert-1"}) || repo.limit != 100 {
		t.Fatalf("RunOnce() = %d, %v; closed = %v", n, err, repo.closed)
	}
	if obs.commands[CommandDone] != 1 || obs.commands[CommandRejected] != 1 || obs.commands[CommandFailed] != 1 {
		t.Fatalf("metrics = %v", obs.commands)
	}
	// 处理满一批：循环立即再跑一轮。
	uc.WithOptions(CommandOptions{Batch: 2})
	if n, err := uc.RunOnce(context.Background()); err != nil || n != 2 {
		t.Fatalf("RunOnce() = %d, %v", n, err)
	}
	// 全部失败才向循环返回错误（退避）。
	repo.pending, repo.outcome = []AlertCommand{cmd("9", monitorcontract.AlertCmdClose)}, map[string]error{"9": errors.New("db down")}
	if _, err := uc.RunOnce(context.Background()); err == nil {
		t.Fatal("want an error")
	}
	repo.listErr = errors.New("db down")
	if _, err := uc.RunOnce(context.Background()); err == nil {
		t.Fatal("want an error")
	}
}

func TestMaintenancePurgesP2Tables(t *testing.T) {
	repo := &fakeMaintenanceRepo{engineEvents: 1500, commands: 10}
	uc := NewMaintenanceUsecase(repo, repo, quietLogger())
	if err := uc.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if repo.calls["engine_events"] != 2 || repo.calls["commands"] != 1 || repo.engineEvents != 0 || repo.lastRetained["commands"] != 180*24*time.Hour {
		t.Fatalf("calls = %v", repo.calls)
	}
	repo = &fakeMaintenanceRepo{engineEvents: 5}
	uc = NewMaintenanceUsecase(repo, repo, quietLogger()).WithOptions(MaintenanceOptions{})
	if err := uc.RunOnce(context.Background()); err != nil || repo.calls["engine_events"] != 0 || repo.calls["commands"] != 0 {
		t.Fatalf("alert retention 0 must disable the purges: %v, %v", repo.calls, err)
	}
}
