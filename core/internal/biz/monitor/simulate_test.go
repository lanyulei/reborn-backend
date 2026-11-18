package biz_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// fakeInhibits biz.InhibitReader 的内存桩。
type fakeInhibits []*biz.Inhibit

func (f fakeInhibits) Get(context.Context, string) (*biz.Inhibit, error) {
	return nil, biz.ErrInhibitNotFound
}

func (f fakeInhibits) List(context.Context, biz.InhibitFilter, int64, int64) ([]*biz.Inhibit, int64, error) {
	return nil, 0, nil
}

func (f fakeInhibits) ListEnabled(_ context.Context, limit int) ([]*biz.Inhibit, error) {
	return f[:min(len(f), limit)], nil
}

// fakeCIs biz.CIIndexReader + biz.CfgVersionReader：记录 ListValid 的调用次数（缓存断言）。
type fakeCIs struct {
	entries []*biz.CIEntry
	version int64
	synced  bool
	loads   int
}

func (f *fakeCIs) Briefs(context.Context, []string) (map[string]*biz.CIEntry, error) { return nil, nil }

func (f *fakeCIs) ListValid(_ context.Context, limit int) ([]*biz.CIEntry, error) {
	f.loads++
	return f.entries[:min(len(f.entries), limit)], nil
}

func (f *fakeCIs) Summary(context.Context) (int32, *time.Time, error) {
	if !f.synced {
		return 0, nil, nil
	}
	now := time.Now()
	return int32(len(f.entries)), &now, nil // #nosec G115 -- 测试数据，几条
}

func (f *fakeCIs) CfgVersion(context.Context, string) (int64, error) { return f.version, nil }

type fakeCalendarD struct{ snap mc.CalendarSnapshot }

func (f *fakeCalendarD) Snapshot(context.Context) (mc.CalendarSnapshot, error) { return f.snap, nil }

type fakeRoutes struct{ matched bool }

func (f *fakeRoutes) RouteMatched(context.Context, string, int32, map[string]string) (bool, error) {
	return f.matched, nil
}

// fakeRosterD biz.DutyRoster：值班表 → [主班, 备班]。
type fakeRosterD map[string][2][]string

func (f fakeRosterD) OnDutyByRole(_ context.Context, id string) ([]string, []string, error) {
	return f[id][0], f[id][1], nil
}

type fakeOncall map[string][]string

func (f fakeOncall) Groups(context.Context) ([]biz.OncallGroup, error) {
	return []biz.OncallGroup{{ID: oncallOps, Name: "运维值班组"}}, nil
}

func (f fakeOncall) AdminIDs(_ context.Context, id string) ([]string, error) { return f[id], nil }

// simEnv 模拟触发的测试装置。
type simEnv struct {
	*p2Env
	sim      *biz.SimulateUsecase
	inhibits *fakeInhibits
	cis      *fakeCIs
	calendar *fakeCalendarD
	routes   *fakeRoutes
}

func newSimEnv(t *testing.T) *simEnv {
	t.Helper()
	p := newP2EnvD(t)
	s := &simEnv{
		p2Env: p, inhibits: &fakeInhibits{}, calendar: &fakeCalendarD{}, routes: &fakeRoutes{matched: true},
		cis: &fakeCIs{synced: true, version: 1, entries: []*biz.CIEntry{
			{ID: ciWebD, ModelCode: "vm", Name: "web-01", Env: "prod", Status: "active", Owner: agentOK, Tags: []string{"core"}, Attrs: map[string][]string{"ip": {"10.0.0.1"}}, UpstreamIDs: []string{ciHostD}, Valid: true},
			{ID: ciHostD, ModelCode: "host", Name: "host-01", Env: "prod", Status: "active", Owner: "外包团队", Attrs: map[string][]string{"ip": {"10.0.0.9"}}, Valid: true},
		}},
	}
	sim, err := biz.NewSimulateUsecase(biz.SimulateDeps{
		Scopes: p.scopes, Rules: p.rules, Sources: p.sources, Mutes: p.store.Mutes(), Alerts: p.store.Alerts(), Inhibits: s.inhibits,
		CIs: s.cis, Cfg: s.cis, Calendar: s.calendar, Escalations: p.escalations, Subs: p.store.Subscriptions(),
		Dir: p.org, Leaders: p.org, Duty: p.org, Roster: fakeRosterD{schedOps: {{uBob}, {uCarol}}}, Groups: fakeOncall{oncallOps: {uDave}},
		Schedules: p.org, Routes: s.routes, Events: biznotify.NewNotificationUsecase(p.events, nil), Txm: testfake.NewTxManager(),
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), &conf.Monitor{SiteUrl: "https://ops.example.com/"})
	if err != nil {
		t.Fatalf("NewSimulateUsecase: %v", err)
	}
	s.sim = sim
	return s
}

func series(value float64, kv ...string) biz.QuerySeries {
	labels := map[string]string{"__name__": "up"}
	for i := 0; i+1 < len(kv); i += 2 {
		labels[kv[i]] = kv[i+1]
	}
	return biz.QuerySeries{Labels: labels, Points: []biz.QueryPoint{{Time: time.Now(), Value: value}}}
}

func (s *simEnv) setSeries(items ...biz.QuerySeries) {
	s.query.Result = &biz.QueryResult{Type: "vector", Series: items}
}

func (s *simEnv) run(t *testing.T, user string, in biz.SimulateInput) *biz.SimulateResult {
	t.Helper()
	if in.DatasourceID == "" {
		in.DatasourceID = dsVM
	}
	res, err := s.sim.Simulate(context.Background(), user, in)
	if err != nil {
		t.Fatalf("Simulate: %v", err)
	}
	return res
}

func stageOf(t *testing.T, res *biz.SimulateResult, name string) biz.SimulateStage {
	t.Helper()
	for _, st := range res.Stages {
		if st.Stage == name {
			return st
		}
	}
	t.Fatalf("stage %s missing in %+v", name, res.Stages)
	return biz.SimulateStage{}
}

func wantStages(t *testing.T, res *biz.SimulateResult, want map[string]string) {
	t.Helper()
	order := []string{"query", "effective", "relabel", "ci", "drop", "scope", "level_inhibit", "inhibit", "mute", "notify", "send"}
	got := make([]string, 0, len(res.Stages))
	for _, st := range res.Stages {
		got = append(got, st.Stage)
		if st.Message == "" {
			t.Errorf("stage %s has no message", st.Stage)
		}
	}
	if !slices.Equal(got, order) {
		t.Fatalf("stages = %v, want the fixed order %v", got, order)
	}
	for name, status := range want {
		if st := stageOf(t, res, name); st.Status != status {
			t.Errorf("stage %s = %s (%s), want %s", name, st.Status, st.Message, status)
		}
	}
}

func (s *simEnv) eventCount(t *testing.T) int64 {
	t.Helper()
	_, n, err := s.events.ListEvents(context.Background(), biznotify.EventFilter{}, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestSimulate_Authorization(t *testing.T) {
	t.Parallel()
	s := newSimEnv(t)
	ctx := context.Background()
	in := biz.SimulateInput{Spec: spec(gHost, "sim"), DatasourceID: dsVM}
	// 只读、范围外、无任何范围的用户一律 403；可管理的可以。
	for _, user := range []string{uBob, uDave, uCarol, uGone} {
		if _, err := s.sim.Simulate(ctx, user, in); !isReason(err, biz.ErrForbidden) {
			t.Errorf("%s: err = %v, want MONITOR_FORBIDDEN", user, err)
		}
	}
	if _, err := s.sim.Simulate(ctx, uAlice, in); err != nil {
		t.Errorf("manager: %v", err)
	}
	// 规则内容与新建同一套校验。
	bad := in
	bad.Spec.Queries = []mc.Query{{Expr: "up ==", Severity: 3}}
	if _, err := s.sim.Simulate(ctx, uAlice, bad); !isReason(err, biz.ErrRuleInvalid) {
		t.Errorf("invalid expr: err = %v", err)
	}
	bad = in
	bad.Spec.EscalationID = escApp
	if _, err := s.sim.Simulate(ctx, uAlice, bad); !isReason(err, biz.ErrRuleInvalid) {
		t.Errorf("unusable escalation: err = %v", err)
	}
	// 数据源：不存在、已停用、不在规则的数据源范围内。
	for name, mutate := range map[string]func(in *biz.SimulateInput){
		"unknown":  func(in *biz.SimulateInput) { in.DatasourceID = "ds-none" },
		"disabled": func(in *biz.SimulateInput) { in.DatasourceID = dsOff },
		"not in scope": func(in *biz.SimulateInput) {
			in.Spec.DatasourceScope, in.Spec.DatasourceIDs = mc.DatasourceScopeSpecified, []string{dsVM}
			in.DatasourceID = "ds-other"
		},
		"bad sample label": func(in *biz.SimulateInput) { in.SampleLabels = map[string]string{"1x": "v"} },
	} {
		c := in
		mutate(&c)
		if _, err := s.sim.Simulate(ctx, uAlice, c); !isReason(err, biz.ErrSimulateInvalid) {
			t.Errorf("datasource %s: err = %v, want MONITOR_SIMULATE_INVALID", name, err)
		}
	}
	if n := s.eventCount(t); n != 0 {
		t.Errorf("rejected / dry-run simulations wrote %d notify events", n)
	}
}

func TestSimulate_SyntheticDryRun(t *testing.T) {
	t.Parallel()
	s := newSimEnv(t)
	sp := spec(gHost, "synthetic")
	sp.Labels = map[string]string{"team": "ops"}
	sp.Annotations = map[string]string{"summary": `{{ $labels.instance }} = {{ $value }}`, "bad": `{{ humanize $labels.instance }}`}
	sp.Notify = mc.NotifyTargets{UserIDs: []string{uAlice}, ScheduleIDs: []string{schedOps}, DeptIDs: []string{deptOps}}
	res := s.run(t, uAlice, biz.SimulateInput{Spec: sp, SampleLabels: map[string]string{"instance": "10.0.0.1:9100", "team": "x"}, SampleValue: 42})
	wantStages(t, res, map[string]string{
		"query": "warn", "effective": "pass", "relabel": "skip", "ci": "pass", "drop": "skip", "scope": "skip",
		"level_inhibit": "skip", "inhibit": "pass", "mute": "pass", "notify": "warn", "send": "skip",
	})
	if !res.Synthetic || !res.InEffect || res.EventTotal != 1 || len(res.Events) != 1 || res.Sent || len(res.NotifyEventIDs) != 0 || !res.RouteMatched {
		t.Fatalf("result = %+v", res)
	}
	ev := res.Events[0]
	// 规则附加标签覆盖样例标签；关联到资源并写入富化标签（富化标签不进指纹）。
	if ev.Labels["team"] != "ops" || ev.Labels["ci_env"] != "prod" || ev.Labels["ci_model"] != "vm" || ev.SourceLabels["ci_env"] != "" {
		t.Errorf("labels = %v source = %v", ev.Labels, ev.SourceLabels)
	}
	want := mc.Fingerprint("00000000-0000-0000-0000-000000000000", dsVM, map[string]string{"instance": "10.0.0.1:9100", "team": "ops"}, 0)
	if ev.Fingerprint != want || ev.Outcome != "notify" || ev.CIStatus != "matched" || ev.CI == nil || ev.CI.ID != ciWebD || ev.Severity != 3 || ev.Value != 42 {
		t.Errorf("event = %+v, want fingerprint %s", ev, want)
	}
	if ev.Annotations["summary"] != "10.0.0.1:9100 = 42" || ev.Annotations["bad"] != "<template error>" {
		t.Errorf("annotations = %v", ev.Annotations)
	}
	// 用户 + 值班表（uGone 不是可用用户被略过）+ 部门负责人，去重。
	ids := refIDs(res.Recipients)
	if !slices.Equal(ids, []string{uAlice, uBob, uCarol}) || res.Recipients[0].Name != "Alice" {
		t.Errorf("recipients = %+v", res.Recipients)
	}
	if len(res.RecipientNotes) != 1 || !strings.Contains(res.RecipientNotes[0], "已停用") {
		t.Errorf("notes = %v", res.RecipientNotes)
	}
	if n := s.eventCount(t); n != 0 {
		t.Errorf("dry run wrote %d notify events", n)
	}
}

func refIDs(refs []biz.NamedRef) []string {
	out := make([]string, 0, len(refs))
	for _, r := range refs {
		out = append(out, r.ID)
	}
	return out
}

func TestSimulate_QueryStage(t *testing.T) {
	t.Parallel()
	s := newSimEnv(t)
	// 查询全部失败：query 为 error，其余未执行，没有事件。
	s.query.Err = biz.QueryFailed(true, "parse error: unexpected end")
	res := s.run(t, uAlice, biz.SimulateInput{Spec: spec(gHost, "q")})
	wantStages(t, res, map[string]string{"query": "error", "effective": "skip", "notify": "skip", "send": "skip"})
	if len(res.Events) != 0 || len(res.Queries) != 1 || !strings.Contains(res.Queries[0].Error, "parse error") {
		t.Fatalf("result = %+v", res)
	}
	// 数据源不可达同样记在查询上（不是接口错误）。
	s.query.Err = biz.QueryFailed(false, "http: Bad Gateway")
	if res = s.run(t, uAlice, biz.SimulateInput{Spec: spec(gHost, "q")}); stageOf(t, res, "query").Status != "error" {
		t.Errorf("unreachable: %+v", res.Stages[0])
	}
	// 内部错误不吞。
	s.query.Err = errors.New("boom")
	if _, err := s.sim.Simulate(context.Background(), uAlice, biz.SimulateInput{Spec: spec(gHost, "q"), DatasourceID: dsVM}); err == nil {
		t.Error("internal query error must be returned")
	}
	// 有结果：每条序列一个事件，至多返回 20 个，event_total 为总数；截断给出提示。
	s.query.Err = nil
	items := make([]biz.QuerySeries, 0, 25)
	for i := range 25 {
		items = append(items, series(float64(i), "instance", "h"+string(rune('a'+i))))
	}
	s.query.Result = &biz.QueryResult{Type: "vector", Series: items, Truncated: true}
	res = s.run(t, uAlice, biz.SimulateInput{Spec: spec(gHost, "q")})
	if res.Synthetic || res.EventTotal != 25 || len(res.Events) != 20 || stageOf(t, res, "query").Status != "warn" || !res.Queries[0].Truncated || res.Queries[0].Series != 25 {
		t.Fatalf("events = %d total = %d stage = %+v", len(res.Events), res.EventTotal, res.Stages[0])
	}
	if _, has := res.Events[0].Labels["__name__"]; has {
		t.Error("__name__ must be removed from event labels")
	}
	// 模拟不改动查询结果里的标签（结果归查询方所有）。
	if items[0].Labels["__name__"] != "up" {
		t.Error("simulation mutated the query result")
	}
}

func TestSimulate_Effective(t *testing.T) {
	t.Parallel()
	s := newSimEnv(t)
	// 时间窗只含「后天」（UTC）整天：此刻不在生效时间内（取后天而不是明天：测试跨零点也成立），后续阶段照常模拟。
	today := (int(time.Now().UTC().Weekday())+6)%7 + 1 // days：1 = 周一 … 7 = 周日
	other := []int32{1, 2, 3, 4, 5, 6, 7}[(today+1)%7]
	sp := spec(gHost, "eff")
	sp.Effective = mc.Effective{Timezone: "UTC", Windows: []mc.TimeWindow{{Days: []int32{other}, Start: "00:00", End: "00:00"}}}
	res := s.run(t, uAlice, biz.SimulateInput{Spec: sp})
	wantStages(t, res, map[string]string{"effective": "warn", "notify": "warn"})
	if res.InEffect || len(res.Events) != 1 {
		t.Fatalf("in_effect = %v events = %d", res.InEffect, len(res.Events))
	}
	// 按工作日历而日历尚未物化：按生效处理并提示。
	sp.Effective = mc.Effective{Mode: mc.EffectiveModeWorkTime}
	res = s.run(t, uAlice, biz.SimulateInput{Spec: sp})
	if st := stageOf(t, res, "effective"); !res.InEffect || st.Status != "warn" || !strings.Contains(st.Message, "工作日历") {
		t.Errorf("no calendar: in_effect %v stage %+v", res.InEffect, st)
	}
	// 有日历：全周全天上班 → work_time 生效、off_time 不生效。
	s.calendar.snap = mc.CalendarSnapshot{Timezone: "UTC", Weekdays: []int32{0, 1, 2, 3, 4, 5, 6}, Segments: []mc.CalendarSegment{{Start: "00:00", End: "24:00"}}}
	if res = s.run(t, uAlice, biz.SimulateInput{Spec: sp}); !res.InEffect || stageOf(t, res, "effective").Status != "pass" {
		t.Errorf("work_time with an always-working calendar: %+v", res.Stages[1])
	}
	sp.Effective.Mode = mc.EffectiveModeOffTime
	if res = s.run(t, uAlice, biz.SimulateInput{Spec: sp}); res.InEffect {
		t.Error("off_time with an always-working calendar must not be in effect")
	}
}

func TestSimulate_PipelineStages(t *testing.T) {
	t.Parallel()
	s := newSimEnv(t)
	base := func(name string) biz.RuleSpec {
		sp := spec(gHost, name)
		sp.Notify.UserIDs = []string{uAlice}
		return sp
	}
	// 改写在指纹之前：改写后的标签进指纹，源标签保留原样。
	s.setSeries(series(1, "instance", "10.0.0.1:9100", "tmp_x", "1"))
	sp := base("relabel")
	sp.Relabel = []mc.RelabelStep{
		{Action: mc.RelabelReplace, SourceLabels: []string{"instance"}, Regex: `(.+):\d+`, TargetLabel: "host"},
		{Action: mc.RelabelLabelDrop, Regex: "tmp_.*"},
	}
	res := s.run(t, uAlice, biz.SimulateInput{Spec: sp})
	ev := res.Events[0]
	wantStages(t, res, map[string]string{"relabel": "pass", "ci": "pass"})
	if ev.Labels["host"] != "10.0.0.1" || ev.Labels["tmp_x"] != "" || ev.SourceLabels["tmp_x"] != "1" || ev.SourceLabels["host"] != "" {
		t.Errorf("labels = %v source = %v", ev.Labels, ev.SourceLabels)
	}
	if want := mc.Fingerprint("00000000-0000-0000-0000-000000000000", dsVM, map[string]string{"instance": "10.0.0.1:9100", "host": "10.0.0.1"}, 0); ev.Fingerprint != want {
		t.Errorf("fingerprint = %s, want %s (relabelled labels, no ci_ labels)", ev.Fingerprint, want)
	}

	// 关联的各种结论。
	for _, c := range []struct {
		name   string
		labels []string
		match  mc.CIMatch
		status string
		stage  string
	}{
		{"disabled", []string{"instance", "10.0.0.1"}, mc.CIMatch{Disabled: true}, "disabled", "skip"},
		{"no label", []string{"job", "x"}, mc.CIMatch{}, "no_label", "warn"},
		{"not found", []string{"instance", "10.9.9.9"}, mc.CIMatch{}, "not_found", "warn"},
		{"by name", []string{"ident", "WEB-01"}, mc.CIMatch{}, "matched", "pass"},
	} {
		s.setSeries(series(1, c.labels...))
		sp := base("ci " + c.name)
		sp.CIMatch = c.match
		res := s.run(t, uAlice, biz.SimulateInput{Spec: sp})
		if got := res.Events[0].CIStatus; got != c.status || stageOf(t, res, "ci").Status != c.stage {
			t.Errorf("ci %s: status %s stage %+v", c.name, got, stageOf(t, res, "ci"))
		}
	}

	// 条件丢弃：读得到富化标签；命中即拦下，其后的阶段未执行。
	s.setSeries(series(1, "instance", "10.0.0.1"))
	sp = base("drop")
	sp.DropExpr = `{{ if eq $labels.ci_env "prod" }}true{{ end }}`
	res = s.run(t, uAlice, biz.SimulateInput{Spec: sp, Send: true})
	wantStages(t, res, map[string]string{"drop": "block", "scope": "skip", "inhibit": "skip", "mute": "skip", "notify": "skip", "send": "skip"})
	if res.Events[0].Outcome != "dropped" || res.Sent {
		t.Errorf("event = %+v sent = %v", res.Events[0], res.Sent)
	}
	sp.DropExpr = `{{ if eq $labels.ci_env "dev" }}true{{ end }}`
	if res = s.run(t, uAlice, biz.SimulateInput{Spec: sp}); stageOf(t, res, "drop").Status != "pass" || res.Events[0].Outcome != "notify" {
		t.Errorf("drop not matching: %+v", res.Events[0])
	}

	// 资源范围：没有关联到资源、或资源不满足条件 → 不产生事件。
	sp = base("scope")
	sp.ResourceScope = mc.ResourceScope{Enabled: true, Envs: []string{"prod"}, ModelCodes: []string{"vm"}}
	if res = s.run(t, uAlice, biz.SimulateInput{Spec: sp}); stageOf(t, res, "scope").Status != "pass" {
		t.Errorf("in scope: %+v", stageOf(t, res, "scope"))
	}
	sp.ResourceScope.ModelCodes = []string{"host"}
	res = s.run(t, uAlice, biz.SimulateInput{Spec: sp})
	wantStages(t, res, map[string]string{"scope": "block", "level_inhibit": "skip", "inhibit": "skip", "mute": "skip", "notify": "skip"})
	if res.Events[0].Outcome != "out_of_scope" {
		t.Errorf("out of scope event = %+v", res.Events[0])
	}

	// 资源索引从未同步：关联不可用，启用了资源范围的规则不产生事件。
	s.cis.entries, s.cis.synced, s.cis.version = nil, false, 2
	res = s.run(t, uAlice, biz.SimulateInput{Spec: sp})
	if res.Events[0].CIStatus != "unavailable" || res.Events[0].Outcome != "out_of_scope" || stageOf(t, res, "ci").Status != "warn" {
		t.Errorf("index unavailable: %+v", res.Events[0])
	}
}

func TestSimulate_CIIndexCachedByVersion(t *testing.T) {
	t.Parallel()
	s := newSimEnv(t)
	in := biz.SimulateInput{Spec: spec(gHost, "cache")}
	s.run(t, uAlice, in)
	s.run(t, uAlice, in)
	if s.cis.loads != 1 {
		t.Fatalf("index loaded %d times for one version, want 1", s.cis.loads)
	}
	s.cis.version++
	s.run(t, uAlice, in)
	if s.cis.loads != 2 {
		t.Fatalf("index loaded %d times after a version bump, want 2", s.cis.loads)
	}
}

func TestSimulate_LevelInhibit(t *testing.T) {
	t.Parallel()
	s := newSimEnv(t)
	s.setSeries(series(96, "instance", "a"))
	sp := spec(gHost, "levels")
	sp.Queries = []mc.Query{{Expr: "cpu > 85", Severity: 2}, {Expr: "cpu > 95", Severity: 3}}
	res := s.run(t, uAlice, biz.SimulateInput{Spec: sp})
	// 同一条序列两档都命中：低档被抑制（stages 针对第一个事件 = 低档）。
	if len(res.Events) != 2 || res.Events[0].Outcome != "level_inhibited" || res.Events[1].Outcome != "notify" || res.Events[0].Fingerprint != res.Events[1].Fingerprint {
		t.Fatalf("events = %+v / %+v", res.Events[0], res.Events[1])
	}
	wantStages(t, res, map[string]string{"level_inhibit": "block", "inhibit": "skip", "mute": "skip", "notify": "skip"})
	// 关闭级别抑制：各级别各自成事件，指纹不同。
	sp.LevelInhibit = false
	res = s.run(t, uAlice, biz.SimulateInput{Spec: sp})
	if res.Events[0].Outcome != "notify" || res.Events[1].Outcome != "notify" || res.Events[0].Fingerprint == res.Events[1].Fingerprint {
		t.Fatalf("without level inhibit: %+v / %+v", res.Events[0], res.Events[1])
	}
	if stageOf(t, res, "level_inhibit").Status != "skip" {
		t.Errorf("stage = %+v", stageOf(t, res, "level_inhibit"))
	}
}

func (s *simEnv) seedFiring(t *testing.T, ruleID, groupID string, severity int32, ciID string, inhibited bool, kv ...string) string {
	t.Helper()
	id, err := idgen.NewID()
	if err != nil {
		t.Fatal(err)
	}
	labels := map[string]string{}
	for i := 0; i+1 < len(kv); i += 2 {
		labels[kv[i]] = kv[i+1]
	}
	now := time.Now()
	a := &biz.Alert{
		ID: id, Fingerprint: strings.Repeat("a", 32), RuleID: ruleID, RuleName: "seed", GroupID: groupID, DatasourceID: dsVM,
		Severity: severity, Status: mc.AlertFiring, Labels: labels, FirstFireTime: now, LastFireTime: now, CIID: ciID, Inhibited: inhibited,
	}
	if inhibited {
		a.InhibitKind = mc.InhibitRule
	}
	s.store.SeedAlert(a)
	return id
}

// seedSub 直接落一条订阅（绕过用例的范围校验：测的是匹配时对所有者范围的判定）。
func (s *simEnv) seedSub(t *testing.T, owner string, spec biz.SubscriptionSpec) {
	t.Helper()
	id, err := idgen.NewID()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.store.Subscriptions().Create(context.Background(), &biz.Subscription{ID: id, OwnerID: owner, Spec: spec}); err != nil {
		t.Fatalf("seed subscription %s: %v", spec.Name, err)
	}
}

func TestSimulate_Inhibit(t *testing.T) {
	t.Parallel()
	s := newSimEnv(t)
	const inhibitID = "018f3e1a-0e05-7000-8000-000000000001"
	*s.inhibits = fakeInhibits{{ID: inhibitID, Spec: biz.InhibitSpec{
		GroupID: gInfra, Name: "主机失联抑制", Enabled: true, EqualLabels: []string{"instance"},
		Source: mc.InhibitSelector{Matchers: []mc.Matcher{{Label: "alertname", Op: mc.OpEqual, Value: "HostDown"}}},
	}}}
	s.setSeries(series(1, "instance", "10.0.0.1"))
	sp := spec(gHost, "inhibit")
	sp.Notify.UserIDs = []string{uAlice}
	in := biz.SimulateInput{Spec: sp, Send: true}
	// 没有源告警：不抑制。
	res := s.run(t, uAlice, in)
	if res.Events[0].Outcome != "notify" || !res.Sent {
		t.Fatalf("no source: %+v", res.Events[0])
	}
	// 源告警已被抑制 / 相等标签不同 / 不在作用分组内：都不算。
	s.seedFiring(t, "r-src", gDB, 4, "", true, "alertname", "HostDown", "instance", "10.0.0.1")
	s.seedFiring(t, "r-src", gDB, 4, "", false, "alertname", "HostDown", "instance", "10.0.0.2")
	s.seedFiring(t, "r-src", gApp, 4, "", false, "alertname", "HostDown", "instance", "10.0.0.1")
	if res = s.run(t, uAlice, in); res.Events[0].Outcome != "notify" {
		t.Fatalf("non-matching sources must not inhibit: %+v", res.Events[0])
	}
	src := s.seedFiring(t, "r-src", gDB, 4, "", false, "alertname", "HostDown", "instance", "10.0.0.1")
	before := s.eventCount(t)
	res = s.run(t, uAlice, in)
	ev := res.Events[0]
	wantStages(t, res, map[string]string{"inhibit": "block", "mute": "pass", "notify": "skip", "send": "skip"})
	if ev.Outcome != "inhibited" || ev.InhibitID != inhibitID || ev.InhibitAlertID != src || !strings.Contains(ev.Reason, "主机失联抑制") || res.Sent {
		t.Fatalf("inhibited event = %+v", ev)
	}
	if s.eventCount(t) != before {
		t.Error("an inhibited event must not send the test notification")
	}
	// 作用分组已删除的抑制规则不生效（引擎不加载它）。
	(*s.inhibits)[0].Spec.GroupID = "g-deleted"
	if res = s.run(t, uAlice, in); res.Events[0].Outcome != "notify" {
		t.Errorf("inhibit rule of a deleted group: %+v", res.Events[0])
	}
}

func TestSimulate_InhibitExcludesOwnAlertsAndTopology(t *testing.T) {
	t.Parallel()
	s := newSimEnv(t)
	s.setSeries(series(1, "instance", "10.0.0.1"))
	sp := spec(gHost, "topo")
	saved := s.mustRule(t, uAlice, sp)
	// 上游资源（host-01）上有活跃告警：开启拓扑抑制的规则被抑制，未开启的不受影响。
	up := s.seedFiring(t, "r-up", gDB, 3, ciHostD, false, "instance", "10.0.0.9")
	res := s.run(t, uAlice, biz.SimulateInput{Spec: sp})
	if res.Events[0].Outcome != "notify" {
		t.Fatalf("topology inhibit off: %+v", res.Events[0])
	}
	sp.TopologyInhibit = true
	res = s.run(t, uAlice, biz.SimulateInput{Spec: sp})
	if ev := res.Events[0]; ev.Outcome != "inhibited" || ev.InhibitAlertID != up || ev.InhibitID != "" || !strings.Contains(ev.Reason, "拓扑") {
		t.Fatalf("topology inhibited event = %+v", ev)
	}
	// 上游的那条告警若是正在编辑的规则自己的：排除在外。
	s.store.SetAlert(up, func(a *biz.Alert) { a.RuleID = saved.ID })
	res = s.run(t, uAlice, biz.SimulateInput{Spec: sp, RuleID: saved.ID})
	if res.Events[0].Outcome != "notify" {
		t.Fatalf("own alerts must be excluded: %+v", res.Events[0])
	}
	if want := mc.Fingerprint(saved.ID, dsVM, map[string]string{"instance": "10.0.0.1"}, 0); res.Events[0].Fingerprint != want {
		t.Errorf("fingerprint must use the saved rule id: %s != %s", res.Events[0].Fingerprint, want)
	}
}

func TestSimulate_Mute(t *testing.T) {
	t.Parallel()
	s := newSimEnv(t)
	ctx := context.Background()
	s.setSeries(series(1, "instance", "10.0.0.1", "env", "prod"))
	sp := spec(gHost, "mute")
	sp.Notify.UserIDs = []string{uAlice}
	in := biz.SimulateInput{Spec: sp}
	start, end := time.Now().Add(-time.Hour), time.Now().Add(time.Hour)
	mute := func(group, name string, mode int32, mutate func(m *biz.MuteSpec)) *biz.MuteView {
		t.Helper()
		m := biz.MuteSpec{
			GroupID: group, Name: name, Reason: "变更", Mode: mode, TimeType: mc.MuteTimeOnce, StartTime: &start, EndTime: &end, Enabled: true,
			Matchers: []mc.Matcher{{Label: "env", Op: mc.OpEqual, Value: "prod"}},
		}
		if mutate != nil {
			mutate(&m)
		}
		v, err := s.mutes.Create(ctx, uAdmin, m)
		if err != nil {
			t.Fatalf("create mute %s: %v", name, err)
		}
		return v
	}
	// 不生效的屏蔽规则：别的分组、未开始、已停用、标签不匹配、级别不匹配、限定了别的规则 / 数据源。
	future := time.Now().Add(30 * time.Minute)
	mute(gApp, "别的分组", mc.MuteModeEvent, nil)
	mute(gInfra, "未开始", mc.MuteModeEvent, func(m *biz.MuteSpec) { m.StartTime = &future })
	mute(gInfra, "停用", mc.MuteModeEvent, func(m *biz.MuteSpec) { m.Enabled = false })
	mute(gInfra, "标签不匹配", mc.MuteModeEvent, func(m *biz.MuteSpec) { m.Matchers[0].Value = "dev" })
	mute(gInfra, "级别不匹配", mc.MuteModeEvent, func(m *biz.MuteSpec) { m.Severities = []int32{1} })
	mute(gInfra, "别的数据源", mc.MuteModeEvent, func(m *biz.MuteSpec) { m.DatasourceIDs = []string{dsOff} })
	res := s.run(t, uAlice, in)
	if res.Events[0].Outcome != "notify" || stageOf(t, res, "mute").Status != "pass" {
		t.Fatalf("inactive mutes must not match: %+v", res.Events[0])
	}
	// 上级分组的「只屏蔽通知」命中。
	notify := mute(gInfra, "变更窗口", mc.MuteModeNotify, nil)
	res = s.run(t, uAlice, biz.SimulateInput{Spec: sp, Send: true})
	wantStages(t, res, map[string]string{"inhibit": "pass", "mute": "block", "notify": "skip", "send": "skip"})
	if ev := res.Events[0]; ev.Outcome != "muted_notify" || ev.MuteID != notify.ID || !strings.Contains(ev.Reason, "变更窗口") || res.Sent {
		t.Fatalf("muted_notify event = %+v", ev)
	}
	// 全局的「屏蔽事件」优先于「只屏蔽通知」。
	event := mute("", "全局噪声", mc.MuteModeEvent, nil)
	res = s.run(t, uAlice, in)
	if ev := res.Events[0]; ev.Outcome != "muted_event" || ev.MuteID != event.ID {
		t.Fatalf("muted_event event = %+v", ev)
	}
	if n := s.eventCount(t); n != 0 {
		t.Errorf("muted simulations wrote %d notify events", n)
	}
}

func TestSimulate_Maintenance(t *testing.T) {
	t.Parallel()
	s := newSimEnv(t)
	s.cis.entries[0].Status = mc.CIStatusMaintenance
	s.setSeries(series(1, "instance", "10.0.0.1"))
	res := s.run(t, uAlice, biz.SimulateInput{Spec: spec(gHost, "maint"), Send: true})
	wantStages(t, res, map[string]string{"mute": "block", "notify": "skip", "send": "skip"})
	if ev := res.Events[0]; ev.Outcome != "maintenance" || ev.MuteID != "" || res.Sent {
		t.Fatalf("maintenance event = %+v", ev)
	}
}

func TestSimulate_NotifyResolution(t *testing.T) {
	t.Parallel()
	s := newSimEnv(t)
	s.setSeries(series(1, "instance", "10.0.0.1"))
	s.org.OnDutyUsers["sched-empty"] = nil
	s.org.Schedules["sched-empty"] = "夜班值班"
	s.org.Depts["dept-empty"] = "空部门"
	s.escalations[escGlobal].Spec.Levels = []mc.EscalationLevel{
		{Trigger: 1, AfterM: 10, Targets: mc.EscalationTargets{Schedules: []mc.EscalationSchedule{{ID: schedOps, BackupOnly: true}}}},
		{Trigger: 2, AfterM: 30, Targets: mc.EscalationTargets{OncallGroupIDs: []string{oncallOps, "og-none"}, Depts: []mc.EscalationDept{{ID: deptOps, Up: 2}}}},
	}
	sp := spec(gHost, "notify")
	sp.Notify = mc.NotifyTargets{ScheduleIDs: []string{"sched-empty"}, DeptIDs: []string{"dept-empty"}, CIOwner: true}
	sp.EscalationID = escGlobal
	// 订阅：bob 订阅 infra（接收自己 + 一个群）；carol 对 infra 没有范围，她的订阅不生效；
	// 资源负责人已经是规则的通知对象，订阅里不再重复。
	for _, c := range []struct {
		owner string
		spec  biz.SubscriptionSpec
	}{
		{uBob, biz.SubscriptionSpec{Name: "bob 关注", GroupIDs: []string{gInfra}, Severities: []int32{3}, OnFiring: true, Enabled: true, Receivers: mc.Receivers{Self: true, UserIDs: []string{agentOK}, Groups: []string{"$group:ops"}}}},
		{uCarol, biz.SubscriptionSpec{Name: "carol 越权", GroupIDs: []string{gApp}, Severities: []int32{3}, OnFiring: true, Enabled: true, Receivers: mc.Receivers{Self: true}}},
		{uAlice, biz.SubscriptionSpec{Name: "只要恢复", GroupIDs: []string{gInfra}, Severities: []int32{3}, OnResolved: true, Enabled: true, Receivers: mc.Receivers{Self: true}}},
	} {
		s.seedSub(t, c.owner, c.spec)
	}
	res := s.run(t, uAlice, biz.SimulateInput{Spec: sp})
	// 规则：值班表无人、部门无负责人，只剩资源负责人。
	if ids := refIDs(res.Recipients); !slices.Equal(ids, []string{agentOK}) || res.Recipients[0].Name != "机器人" {
		t.Fatalf("recipients = %+v", res.Recipients)
	}
	notes := strings.Join(res.RecipientNotes, "\n")
	for _, want := range []string{"夜班值班", "空部门", "og-none"} {
		if !strings.Contains(notes, want) {
			t.Errorf("notes missing %q: %v", want, res.RecipientNotes)
		}
	}
	if stageOf(t, res, "notify").Status != "warn" {
		t.Errorf("notify stage = %+v", stageOf(t, res, "notify"))
	}
	if len(res.Subscriptions) != 1 || res.Subscriptions[0].Name != "bob 关注" || !slices.Equal(refIDs(res.Subscriptions[0].Recipients), []string{uBob, "$group:ops"}) ||
		res.Subscriptions[0].Recipients[1].Name != "$group:ops" {
		t.Fatalf("subscriptions = %+v", res.Subscriptions)
	}
	// 升级：第 1 级仅备班；第 2 级值班分组管理员 + 部门负责人（上级链不够长时退回能找到的最高一级）。
	if len(res.Escalation) != 2 || res.Escalation[0].Level != 1 || res.Escalation[0].AfterM != 10 || !slices.Equal(refIDs(res.Escalation[0].Recipients), []string{uCarol}) ||
		res.Escalation[1].Trigger != 2 || !slices.Equal(refIDs(res.Escalation[1].Recipients), []string{uDave, uCarol}) {
		t.Fatalf("escalation = %+v", res.Escalation)
	}
	// 资源负责人不是系统用户；没有任何通知对象 → 提示交由路由。
	s.setSeries(series(1, "instance", "10.0.0.9"))
	sp.Notify = mc.NotifyTargets{CIOwner: true}
	res = s.run(t, uAlice, biz.SimulateInput{Spec: sp})
	if len(res.Recipients) != 0 || !strings.Contains(strings.Join(res.RecipientNotes, "\n"), "资源「host-01」的负责人不是") || stageOf(t, res, "notify").Status != "warn" {
		t.Fatalf("non-user owner: recipients %+v notes %v", res.Recipients, res.RecipientNotes)
	}
	// 事件没有关联到资源。
	s.setSeries(series(1, "instance", "10.9.9.9"))
	if res = s.run(t, uAlice, biz.SimulateInput{Spec: sp}); !strings.Contains(strings.Join(res.RecipientNotes, "\n"), "没有关联到资源") {
		t.Errorf("not linked: %v", res.RecipientNotes)
	}
	// 级别低于升级策略的最低级别、策略停用：不列升级。
	s.escalations[escGlobal].Spec.MinSeverity = 4
	if res = s.run(t, uAlice, biz.SimulateInput{Spec: sp}); len(res.Escalation) != 0 {
		t.Errorf("below min severity: %+v", res.Escalation)
	}
	// 没有路由能匹配。
	s.routes.matched = false
	sp.Notify = mc.NotifyTargets{UserIDs: []string{uAlice}}
	res = s.run(t, uAlice, biz.SimulateInput{Spec: sp})
	if st := stageOf(t, res, "notify"); res.RouteMatched || st.Status != "warn" || !strings.Contains(st.Message, "路由") {
		t.Errorf("no route: %+v", st)
	}
}

func TestSimulate_SendWritesExactlyOneTestEvent(t *testing.T) {
	t.Parallel()
	s := newSimEnv(t)
	ctx := context.Background()
	s.setSeries(series(97.5, "instance", "10.0.0.1"))
	sp := spec(gHost, "CPU 过高")
	sp.Notify = mc.NotifyTargets{UserIDs: []string{uAlice, uBob}}
	sp.Annotations = map[string]string{"summary": "cpu {{ $value }}"}
	sp.EscalationID = escGlobal
	s.seedSub(t, uBob, biz.SubscriptionSpec{
		Name: "订阅", GroupIDs: []string{gInfra}, Severities: []int32{3}, OnFiring: true, Enabled: true, Receivers: mc.Receivers{UserIDs: []string{uCarol}},
	})
	alertsBefore, _, _ := s.store.Alerts().List(ctx, biz.AlertFilter{Groups: biz.GroupFilter{All: true}}, 1, 10)
	res := s.run(t, uAlice, biz.SimulateInput{Spec: sp, Send: true})
	wantStages(t, res, map[string]string{"notify": "pass", "send": "pass"})
	if !res.Sent || len(res.NotifyEventIDs) != 1 {
		t.Fatalf("sent = %v ids = %v", res.Sent, res.NotifyEventIDs)
	}
	events, n, err := s.events.ListEvents(ctx, biznotify.EventFilter{}, 1, 10)
	if err != nil || n != 1 {
		t.Fatalf("notify events = %d err %v", n, err)
	}
	ev := events[0]
	// 只发给规则的通知对象（不含订阅者与升级对象）。
	if ev.ID != res.NotifyEventIDs[0] || ev.EventType != mc.EventAlertFiring || ev.BizID != mc.NotifyBizID || ev.Severity != 3 ||
		!strings.HasPrefix(ev.Title, "[TEST] CPU 过高") || !slices.Equal(ev.Recipients, []string{uAlice, uBob}) || !strings.HasPrefix(ev.DedupKey, "monitor:test:") {
		t.Fatalf("event = %+v", ev)
	}
	payload := string(ev.Payload)
	for _, want := range []string{`"test":true`, `"rule_name":"CPU 过高"`, `"value":"97.5"`, `"alert_url":"https://ops.example.com/monitor/rule"`, `"ci_name":"web-01"`, `"summary":"cpu 97.5"`, `"alert_id":""`} {
		if !strings.Contains(payload, want) {
			t.Errorf("payload missing %s: %s", want, payload)
		}
	}
	// 再点一次：新的去重键，又是恰好一条。
	res2 := s.run(t, uAlice, biz.SimulateInput{Spec: sp, Send: true})
	events, n, _ = s.events.ListEvents(ctx, biznotify.EventFilter{}, 1, 10)
	if n != 2 || !res2.Sent || events[0].DedupKey == events[1].DedupKey {
		t.Fatalf("second send: events = %d", n)
	}
	// 不写告警表、通知记录、发件箱。
	alertsAfter, _, _ := s.store.Alerts().List(ctx, biz.AlertFilter{Groups: biz.GroupFilter{All: true}}, 1, 10)
	if len(alertsAfter) != len(alertsBefore) || len(s.store.NotifyLogs()) != 0 {
		t.Errorf("simulation wrote alert tables: alerts %d → %d, logs %d", len(alertsBefore), len(alertsAfter), len(s.store.NotifyLogs()))
	}
	// 没有通知对象时不带收件人写一条。
	sp.Notify = mc.NotifyTargets{}
	res = s.run(t, uAlice, biz.SimulateInput{Spec: sp, Send: true})
	events, n, _ = s.events.ListEvents(ctx, biznotify.EventFilter{}, 1, 10)
	if n != 3 || !res.Sent || len(res.NotifyEventIDs) != 1 {
		t.Fatalf("send without recipients: events = %d sent = %v", n, res.Sent)
	}
	for _, e := range events {
		if e.ID == res.NotifyEventIDs[0] && len(e.Recipients) != 0 {
			t.Errorf("event without recipients = %+v", e.Recipients)
		}
	}
}

func TestSimulate_BadSiteURL(t *testing.T) {
	t.Parallel()
	if _, err := biz.NewSimulateUsecase(biz.SimulateDeps{}, slog.New(slog.NewTextHandler(io.Discard, nil)), &conf.Monitor{SiteUrl: "ftp://x"}); err == nil {
		t.Error("an invalid site_url must fail at construction")
	}
}
