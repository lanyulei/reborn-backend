package biz_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

type escUC struct {
	*env
	uc       *biz.EscalationUsecase
	policies *testfake.MonitorEscalations
}

func newEscUC(t *testing.T) *escUC {
	t.Helper()
	e := newEnv(t)
	p := testfake.NewMonitorEscalations()
	groups := &fakeOncallGroups{groups: []biz.OncallGroup{{ID: "og-z", Name: "网络组"}, {ID: ogOps, Name: "运维组"}}}
	return &escUC{env: e, policies: p, uc: biz.NewEscalationUsecase(e.scopes, p, p, p, groups, e.org, e.org)}
}

func escSpec(groupID, name string) biz.EscalationSpec {
	return biz.EscalationSpec{
		GroupID: groupID, Name: name, Mode: mc.EscalationModeStack, MinSeverity: 2, Enabled: true,
		Levels: []mc.EscalationLevel{
			{Trigger: mc.EscalationTriggerUnacked, AfterM: 10, RepeatIntervalM: 60, MaxNotify: 3, Targets: mc.EscalationTargets{
				UserIDs: []string{uBob}, Schedules: []mc.EscalationSchedule{{ID: schedOps, BackupOnly: true}}, OncallGroupIDs: []string{ogOps},
			}},
			{Trigger: mc.EscalationTriggerUnresolved, AfterM: 30, Targets: mc.EscalationTargets{Depts: []mc.EscalationDept{{ID: deptOps, Up: 1}}}},
		},
	}
}

func TestEscalation_CreateScopeRules(t *testing.T) {
	t.Parallel()
	e := newEscUC(t)
	ctx := context.Background()
	// 只读、范围外不能建；可管理可以；全局仅超管。
	_, err := e.uc.Create(ctx, uBob, escSpec(gHost, "p"))
	wantErr(t, "read-only create", err, biz.ErrForbidden)
	_, err = e.uc.Create(ctx, uCarol, escSpec(gHost, "p"))
	wantErr(t, "out-of-scope create", err, biz.ErrForbidden)
	_, err = e.uc.Create(ctx, uAlice, escSpec("", "g"))
	wantErr(t, "non-admin global create", err, biz.ErrForbidden)
	_, err = e.uc.Create(ctx, uAdmin, escSpec("g-none", "p"))
	wantErr(t, "missing group", err, biz.ErrGroupNotFound)

	v, err := e.uc.Create(ctx, uAlice, escSpec(gHost, " 主机升级 "))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if v.Spec.Name != "主机升级" || v.GroupName != "基础设施 / 主机" || !v.CanManage || v.Version != 1 || len(v.LevelRefs) != 2 ||
		v.LevelRefs[0].Users[0].Name != "Bob" || v.LevelRefs[0].Schedules[0].Name != "运维值班" ||
		v.LevelRefs[0].OncallGroups[0].Name != "运维组" || v.LevelRefs[1].Depts[0].Name != "运维部" {
		t.Fatalf("view = %+v refs = %+v", v, v.LevelRefs)
	}
	// 估算：规则首次 1 + 第 1 级 3 次（上限）+ 第 2 级 1 次。
	if v.Estimate.Rule != 1 || !slices.Equal(v.Estimate.Levels, []int32{3, 1}) || v.Estimate.Total != 5 {
		t.Fatalf("estimate = %+v", v.Estimate)
	}
	if e.policies.Writes() != 1 {
		t.Fatalf("writes = %d", e.policies.Writes())
	}
	_, err = e.uc.Create(ctx, uAlice, escSpec(gHost, "主机升级"))
	wantErr(t, "duplicate name", err, biz.ErrEscalationNameExists)
	// 另一个归属下同名可以。
	if _, err := e.uc.Create(ctx, uAdmin, escSpec("", "主机升级")); err != nil {
		t.Fatalf("global create: %v", err)
	}
}

func TestEscalation_Validation(t *testing.T) {
	t.Parallel()
	e := newEscUC(t)
	ctx := context.Background()
	for name, mutate := range map[string]func(s *biz.EscalationSpec){
		"blank name":           func(s *biz.EscalationSpec) { s.Name = "  " },
		"bad mode":             func(s *biz.EscalationSpec) { s.Mode = 3 },
		"bad severity":         func(s *biz.EscalationSpec) { s.MinSeverity = 0 },
		"no levels":            func(s *biz.EscalationSpec) { s.Levels = nil },
		"after_m not rising":   func(s *biz.EscalationSpec) { s.Levels[1].AfterM = 10 },
		"empty targets":        func(s *biz.EscalationSpec) { s.Levels[1].Targets = mc.EscalationTargets{} },
		"inactive user":        func(s *biz.EscalationSpec) { s.Levels[0].Targets.UserIDs = []string{uGone} },
		"unknown schedule":     func(s *biz.EscalationSpec) { s.Levels[0].Targets.Schedules[0].ID = "sched-x" },
		"unknown oncall group": func(s *biz.EscalationSpec) { s.Levels[0].Targets.OncallGroupIDs = []string{"og-x"} },
		"unknown dept":         func(s *biz.EscalationSpec) { s.Levels[1].Targets.Depts[0].ID = "dept-x" },
	} {
		s := escSpec(gHost, "p")
		mutate(&s)
		_, err := e.uc.Create(ctx, uAlice, s)
		wantErr(t, name, err, biz.ErrEscalationInvalid)
	}
	if e.policies.Writes() != 0 {
		t.Fatal("invalid specs must not be written")
	}
}

func TestEscalation_UpdateAndDelete(t *testing.T) {
	t.Parallel()
	e := newEscUC(t)
	ctx := context.Background()
	v, err := e.uc.Create(ctx, uAlice, escSpec(gHost, "p"))
	if err != nil {
		t.Fatal(err)
	}
	s := escSpec(gHost, "p2")
	_, err = e.uc.Update(ctx, uBob, v.ID, s, v.Version)
	wantErr(t, "read-only update", err, biz.ErrForbidden)
	moved := escSpec(gDB, "p2")
	_, err = e.uc.Update(ctx, uAlice, v.ID, moved, v.Version)
	wantErr(t, "group change", err, biz.ErrEscalationInvalid)
	_, err = e.uc.Update(ctx, uAlice, v.ID, s, v.Version+1)
	wantErr(t, "stale version", err, biz.ErrVersionConflict)
	// 保存前就在策略上的用户后来被停用：不阻止保存；新加入的停用用户仍被拒绝。
	delete(e.org.Users, uBob)
	e.org.Disabled[uBob] = "Bob"
	got, err := e.uc.Update(ctx, uAlice, v.ID, s, v.Version)
	if err != nil || got.Version != 2 || got.Spec.Name != "p2" || got.Spec.GroupID != gHost {
		t.Fatalf("update = %+v err = %v", got, err)
	}
	s.Levels[0].Targets.UserIDs = []string{uBob, uGone}
	_, err = e.uc.Update(ctx, uAlice, v.ID, s, got.Version)
	wantErr(t, "newly added inactive user", err, biz.ErrEscalationInvalid)

	e.policies.RuleRefs[v.ID] = 2
	if got, _ = e.uc.Get(ctx, uAlice, v.ID); got.RuleCount != 2 {
		t.Fatalf("rule_count = %d", got.RuleCount)
	}
	err = e.uc.Delete(ctx, uAlice, v.ID)
	wantErr(t, "in use", err, biz.ErrEscalationInUse)
	if md := kerrors.FromError(err).Metadata; md["rule_count"] != "2" {
		t.Fatalf("metadata = %v", md)
	}
	e.policies.RuleRefs[v.ID] = 0
	wantErr(t, "read-only delete", e.uc.Delete(ctx, uBob, v.ID), biz.ErrForbidden)
	if err := e.uc.Delete(ctx, uAlice, v.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	_, err = e.uc.Get(ctx, uAlice, v.ID)
	wantErr(t, "deleted", err, biz.ErrEscalationNotFound)

	// 全局策略：非超管不能改、不能删。
	e.org.Users[uBob] = "Bob"
	g, err := e.uc.Create(ctx, uAdmin, escSpec("", "g"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.uc.Update(ctx, uAlice, g.ID, escSpec("", "g2"), g.Version)
	wantErr(t, "global update by non-admin", err, biz.ErrForbidden)
	wantErr(t, "global delete by non-admin", e.uc.Delete(ctx, uAlice, g.ID), biz.ErrForbidden)
}

func TestEscalation_ListGetVisibility(t *testing.T) {
	t.Parallel()
	e := newEscUC(t)
	ctx := context.Background()
	must := func(user string, s biz.EscalationSpec) *biz.EscalationView {
		v, err := e.uc.Create(ctx, user, s)
		if err != nil {
			t.Fatalf("create %s: %v", s.Name, err)
		}
		return v
	}
	global := must(uAdmin, escSpec("", "全局"))
	host := must(uAlice, escSpec(gHost, "b-host"))
	db := must(uAlice, escSpec(gDB, "a-db"))
	app := must(uCarol, escSpec(gApp, "app"))
	off := escSpec(gInfra, "停用的")
	off.Enabled = false
	infraOff := must(uAlice, off)

	names := func(user string, q biz.EscalationListQuery) []string {
		items, total, err := e.uc.List(ctx, user, q, 1, 50)
		if err != nil {
			t.Fatalf("list as %s: %v", user, err)
		}
		out := make([]string, 0, len(items))
		for _, it := range items {
			out = append(out, it.Spec.Name)
		}
		if int(total) != len(out) {
			t.Fatalf("total = %d items = %d", total, len(out))
		}
		return out
	}
	// 全局在前，其余按名称。
	if got := names(uAlice, biz.EscalationListQuery{}); !slices.Equal(got, []string{"全局", "a-db", "b-host", "停用的"}) {
		t.Fatalf("alice = %v", got)
	}
	if got := names(uDave, biz.EscalationListQuery{}); !slices.Equal(got, []string{"全局", "b-host"}) {
		t.Fatalf("dave = %v", got)
	}
	if got := names(uCarol, biz.EscalationListQuery{}); !slices.Equal(got, []string{"全局", "app"}) {
		t.Fatalf("carol = %v", got)
	}
	if got := names(uGone, biz.EscalationListQuery{}); len(got) != 0 {
		t.Fatalf("no scope = %v", got)
	}
	yes, no := true, false
	if got := names(uAlice, biz.EscalationListQuery{Global: &yes}); !slices.Equal(got, []string{"全局"}) {
		t.Fatalf("global only = %v", got)
	}
	if got := names(uAlice, biz.EscalationListQuery{Global: &no, Enabled: &yes}); !slices.Equal(got, []string{"a-db", "b-host"}) {
		t.Fatalf("group enabled = %v", got)
	}
	if got := names(uAlice, biz.EscalationListQuery{GroupID: gDB}); !slices.Equal(got, []string{"a-db"}) {
		t.Fatalf("by group = %v", got)
	}
	if got := names(uCarol, biz.EscalationListQuery{GroupID: gHost}); len(got) != 0 {
		t.Fatalf("out-of-scope group filter = %v", got)
	}
	if got := names(uAlice, biz.EscalationListQuery{Keyword: "host"}); !slices.Equal(got, []string{"b-host"}) {
		t.Fatalf("keyword = %v", got)
	}

	_, err := e.uc.Get(ctx, uCarol, host.ID)
	wantErr(t, "out-of-scope get", err, biz.ErrForbidden)
	_, err = e.uc.Get(ctx, uGone, global.ID)
	wantErr(t, "global get without any scope", err, biz.ErrForbidden)
	if v, err := e.uc.Get(ctx, uBob, host.ID); err != nil || v.CanManage {
		t.Fatalf("read-only get = %+v, %v", v, err)
	}
	if v, err := e.uc.Get(ctx, uDave, global.ID); err != nil || v.CanManage || v.GroupName != "" {
		t.Fatalf("global get = %+v, %v", v, err)
	}

	// 候选项：全局 + 归属于该分组或其上级的、已启用的策略；兄弟分组与下级分组的不算。
	opts, err := e.uc.Options(ctx, uBob, gHost)
	if err != nil || len(opts) != 2 || !opts[0].Global || opts[1].ID != host.ID || opts[1].LevelCount != 2 || opts[1].GroupName != "基础设施 / 主机" {
		t.Fatalf("options(host) = %+v, %v", opts, err)
	}
	if opts, _ = e.uc.Options(ctx, uAlice, gInfra); len(opts) != 1 || opts[0].ID != global.ID {
		t.Fatalf("options(infra) = %+v (disabled %s, child %s must be excluded)", opts, infraOff.ID, db.ID)
	}
	_, err = e.uc.Options(ctx, uCarol, gHost)
	wantErr(t, "options out of scope", err, biz.ErrForbidden)
	_ = app

	groups, err := e.uc.OncallGroupOptions(ctx)
	if err != nil || len(groups) != 2 || groups[0].Name != "网络组" && groups[0].Name != "运维组" {
		t.Fatalf("oncall groups = %+v, %v", groups, err)
	}
	if !slices.IsSortedFunc(groups, func(a, b biz.OncallGroup) int { return strings.Compare(a.Name, b.Name) }) {
		t.Fatalf("oncall groups not sorted: %+v", groups)
	}
}

func TestEscalation_Estimate(t *testing.T) {
	t.Parallel()
	e := newEscUC(t)
	s := escSpec("", "")
	est, err := e.uc.Estimate(s, 0, 0)
	if err != nil || est.Rule != 1 || est.Total != 5 {
		t.Fatalf("estimate = %+v, %v", est, err)
	}
	// 规则每 60 分钟重复、至多 5 次：一并计入。
	if est, err = e.uc.Estimate(s, 60, 5); err != nil || est.Rule != 5 || est.Total != 9 {
		t.Fatalf("estimate with rule repeat = %+v, %v", est, err)
	}
	s.Levels[1].AfterM = 5
	_, err = e.uc.Estimate(s, 0, 0)
	wantErr(t, "bad levels", err, biz.ErrEscalationInvalid)
	_, err = e.uc.Estimate(escSpec("", ""), 3, 0)
	wantErr(t, "bad repeat", err, biz.ErrEscalationInvalid)
	if e.policies.Writes() != 0 {
		t.Fatal("estimate must not persist")
	}
}

// ---- 自监控事件派发 ----

type fakeAudience struct {
	users []string
	err   error
	calls int
}

func (f *fakeAudience) UserIDs(context.Context) ([]string, error) {
	f.calls++
	return f.users, f.err
}

func newEngineUC(t *testing.T, siteURL string, aud *fakeAudience) (*biz.EngineEventUsecase, *testfake.MonitorEngineEvents, *env) {
	t.Helper()
	e := newEnv(t)
	box := testfake.NewMonitorEngineEvents()
	uc, err := biz.NewEngineEventUsecase(testfake.NewTxManager(), box, aud, biznotify.NewNotificationUsecase(e.events, nil),
		testLogger(e), &conf.Monitor{SiteUrl: siteURL})
	if err != nil {
		t.Fatal(err)
	}
	return uc, box, e
}

func engineDetail(t *testing.T, p mc.EngineEventPayload) []byte {
	t.Helper()
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestEngineEvent_Dispatch(t *testing.T) {
	t.Parallel()
	users := make([]string, 0, 25)
	for i := range 25 {
		users = append(users, "u-"+string(rune('a'+i)))
	}
	aud := &fakeAudience{users: users}
	uc, box, e := newEngineUC(t, "https://ops.example.com/", aud)
	dsID, ruleID := "018f3e1a-0000-7000-8000-0000000000d1", "018f3e1a-0000-7000-8000-0000000000a1"
	longKey := mc.EventEngineDatasourceDown + ":" + dsID + ":" + strings.Repeat("x", 100)
	box.Seed(biz.EngineEvent{
		ID: "ev-1", Type: mc.EventEngineDatasourceDown, ObjectID: dsID, DedupKey: longKey,
		Detail: engineDetail(t, mc.EngineEventPayload{ObjectType: mc.EngineObjectDatasource, ObjectID: dsID, ObjectName: "vm-prod", Error: "refused", Since: "s", Duration: "12m"}),
	})
	box.Seed(biz.EngineEvent{
		ID: "ev-2", Type: mc.EventEngineRuleRecovered, ObjectID: ruleID, DedupKey: "k2",
		Detail: engineDetail(t, mc.EngineEventPayload{ObjectName: "cpu-high", DatasourceName: "vm-prod"}),
	})
	n, err := uc.Run(context.Background())
	if err != nil || n != 2 {
		t.Fatalf("Run = %d, %v", n, err)
	}
	if aud.calls != 1 {
		t.Fatalf("audience resolved %d times, want once per run", aud.calls)
	}
	events, _, _ := e.events.ListEvents(context.Background(), biznotify.EventFilter{}, 1, 100)
	if len(events) != 4 {
		t.Fatalf("events = %d, want 2 chunks per row", len(events))
	}
	byType := map[string][]*biznotify.Event{}
	for _, ev := range events {
		byType[ev.EventType] = append(byType[ev.EventType], ev)
		if len(ev.DedupKey) > 128 || len(ev.Recipients) > 20 || ev.BizID != mc.NotifyBizID {
			t.Fatalf("event = %+v", ev)
		}
	}
	down, up := byType[mc.EventEngineDatasourceDown], byType[mc.EventEngineRuleRecovered]
	if len(down) != 2 || len(up) != 2 || down[0].DedupKey == down[1].DedupKey || len(down[0].Recipients)+len(down[1].Recipients) != 25 {
		t.Fatalf("chunks: down = %d up = %d", len(down), len(up))
	}
	if down[0].Severity != mc.SeverityCritical || up[0].Severity != mc.SeverityInfo || down[0].Title != "数据源持续不可用：vm-prod" ||
		up[0].Title != "告警规则评估已恢复：cpu-high" {
		t.Fatalf("severity / title: %+v / %+v", down[0], up[0])
	}
	var p mc.EngineEventPayload
	_ = json.Unmarshal(down[0].Payload, &p)
	if p.URL != "https://ops.example.com/monitor/engine" || p.ObjectName != "vm-prod" || p.Error != "refused" || p.Duration != "12m" {
		t.Fatalf("datasource payload = %+v", p)
	}
	p = mc.EngineEventPayload{}
	_ = json.Unmarshal(up[0].Payload, &p)
	// 引擎没有填对象类型 / ID 时按事件类型与行上的对象补齐。
	if p.URL != "https://ops.example.com/monitor/rule/"+ruleID || p.ObjectType != mc.EngineObjectRule || p.ObjectID != ruleID {
		t.Fatalf("rule payload = %+v", p)
	}
	if box.Row("ev-1").Status != mc.OutboxDone || box.Row("ev-2").Status != mc.OutboxDone {
		t.Fatal("rows must be marked done")
	}
	// 同一行重放得到相同的去重键（稳定）。
	box.Seed(biz.EngineEvent{ID: "ev-3", Type: mc.EventEngineDatasourceDown, ObjectID: dsID, DedupKey: longKey, Detail: []byte(`{}`)})
	if _, err := uc.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	all, _, _ := e.events.ListEvents(context.Background(), biznotify.EventFilter{}, 1, 100)
	if len(all) != 4 {
		t.Fatalf("events after replay = %d: the dedup key must swallow the duplicate", len(all))
	}
}

func TestEngineEvent_NoAudienceDropsAndFailures(t *testing.T) {
	t.Parallel()
	aud := &fakeAudience{}
	uc, box, e := newEngineUC(t, "", aud)
	obj := "018f3e1a-0000-7000-8000-0000000000d2"
	box.Seed(biz.EngineEvent{ID: "ok", Type: mc.EventEngineRuleFailing, ObjectID: obj, DedupKey: "a", Detail: engineDetail(t, mc.EngineEventPayload{ObjectName: "r", Failures: 7})})
	box.Seed(biz.EngineEvent{ID: "bad-type", Type: "monitor.engine.nope", ObjectID: "o2", DedupKey: "b", Detail: []byte(`{}`)})
	box.Seed(biz.EngineEvent{ID: "bad-json", Type: mc.EventEngineDatasourceUp, ObjectID: "o3", DedupKey: "c", Detail: []byte(`[`)})
	if n, err := uc.Run(context.Background()); err != nil || n != 3 {
		t.Fatalf("Run = %d, %v", n, err)
	}
	events, _, _ := e.events.ListEvents(context.Background(), biznotify.EventFilter{}, 1, 10)
	if len(events) != 1 || len(events[0].Recipients) != 0 {
		t.Fatalf("events = %+v", events)
	}
	var p mc.EngineEventPayload
	_ = json.Unmarshal(events[0].Payload, &p)
	if p.URL != "/monitor/rule/"+obj || p.Failures != 7 {
		t.Fatalf("payload = %+v", p)
	}
	if box.Row("bad-type").Status != mc.OutboxDropped || box.Row("bad-json").Status != mc.OutboxDropped {
		t.Fatal("unparseable rows must be dropped")
	}

	// 收件人解析失败：按失败重试，不丢弃、不发；恢复后发出。
	aud.err = errors.New("directory down")
	box.Seed(biz.EngineEvent{ID: "retry", Type: mc.EventEngineDatasourceDown, ObjectID: "o4", DedupKey: "d", Detail: []byte(`{"object_name":"x"}`)})
	if n, err := uc.Run(context.Background()); err != nil || n != 0 {
		t.Fatalf("Run with failing audience = %d, %v", n, err)
	}
	if r := box.Row("retry"); r.Status != mc.OutboxPending || r.Attempts != 1 || r.LastError == "" {
		t.Fatalf("row = %+v", r)
	}
	aud.err = nil
	if n, _ := uc.Run(context.Background()); n != 1 || box.Row("retry").Status != mc.OutboxDone {
		t.Fatal("row must be dispatched after recovery")
	}
}

func TestEngineEvent_BadSiteURL(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	if _, err := biz.NewEngineEventUsecase(testfake.NewTxManager(), testfake.NewMonitorEngineEvents(), &fakeAudience{},
		biznotify.NewNotificationUsecase(e.events, nil), testLogger(e), &conf.Monitor{SiteUrl: "ftp://x"}); err == nil {
		t.Fatal("want an error for a bad site_url")
	}
}
