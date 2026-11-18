package biz_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// seedAlerts 预置四条告警：host 两条（一条活跃、一条已恢复）、db 一条活跃且被屏蔽、app 一条活跃。
func seedAlerts(e *env) map[string]string {
	now := time.Now()
	ids := map[string]string{"host": "al-host", "host-old": "al-host-old", "db": "al-db", "app": "al-app"}
	mk := func(id, group, rule string, sev, status int32, labels map[string]string, age time.Duration, muted bool) {
		a := &biz.Alert{
			ID: id, Fingerprint: "fp-" + id, RuleID: "rule-" + rule, RuleName: rule, GroupID: group, DatasourceID: dsVM, DatasourceName: dsVM,
			Severity: sev, Status: status, Labels: labels, FirstFireTime: now.Add(-age), LastFireTime: now.Add(-age), Muted: muted,
		}
		if status != mc.AlertFiring {
			end := now.Add(-age + time.Minute)
			a.ResolveTime = &end
		}
		if muted {
			a.MuteID = "mute-x"
		}
		e.store.SeedAlert(a)
	}
	mk(ids["host"], gHost, "cpu", mc.SeverityCritical, mc.AlertFiring, map[string]string{"instance": "h1", "env": "prod"}, time.Minute, false)
	mk(ids["host-old"], gHost, "cpu", mc.SeverityWarning, mc.AlertResolved, map[string]string{"instance": "h2", "env": "prod"}, 2*time.Hour, false)
	mk(ids["db"], gDB, "slow", mc.SeverityFatal, mc.AlertFiring, map[string]string{"instance": "d1", "env": "test"}, time.Minute, true)
	mk(ids["app"], gApp, "5xx", mc.SeverityCritical, mc.AlertFiring, map[string]string{"instance": "a1", "env": "prod"}, time.Minute, false)
	return ids
}

func alertIDs(views []*biz.AlertView) []string {
	out := make([]string, 0, len(views))
	for _, v := range views {
		out = append(out, v.ID)
	}
	slices.Sort(out)
	return out
}

func TestAlert_ListScopedAndFiltered(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	ids := seedAlerts(e)
	list := func(user string, q biz.AlertListQuery) []string {
		t.Helper()
		views, total, err := e.alerts.List(ctx, user, q, 1, 50)
		if err != nil {
			t.Fatalf("List(%s): %v", user, err)
		}
		if int(total) != len(views) {
			t.Fatalf("total %d != items %d", total, len(views))
		}
		return alertIDs(views)
	}
	firing := biz.AlertFilter{Statuses: []int32{mc.AlertFiring}}
	for _, c := range []struct {
		name string
		user string
		q    biz.AlertListQuery
		want []string
	}{
		{"admin sees all", uAdmin, biz.AlertListQuery{}, []string{ids["app"], ids["db"], ids["host"], ids["host-old"]}},
		{"alice sees infra tree", uAlice, biz.AlertListQuery{}, []string{ids["db"], ids["host"], ids["host-old"]}},
		{"carol sees app only", uCarol, biz.AlertListQuery{}, []string{ids["app"]}},
		{"dave sees host only", uDave, biz.AlertListQuery{}, []string{ids["host"], ids["host-old"]}},
		{"no grants sees nothing", uGone, biz.AlertListQuery{}, []string{}},
		{"group filter includes descendants", uAlice, biz.AlertListQuery{GroupID: gInfra, AlertFilter: firing}, []string{ids["db"], ids["host"]}},
		{"group filter out of scope", uCarol, biz.AlertListQuery{GroupID: gInfra}, []string{}},
		{"group filter clipped to scope", uDave, biz.AlertListQuery{GroupID: gInfra}, []string{ids["host"], ids["host-old"]}},
		{"status", uAlice, biz.AlertListQuery{AlertFilter: biz.AlertFilter{Statuses: []int32{mc.AlertResolved}}}, []string{ids["host-old"]}},
		{"severity", uAdmin, biz.AlertListQuery{AlertFilter: biz.AlertFilter{Severities: []int32{mc.SeverityFatal}}}, []string{ids["db"]}},
		{"label", uAdmin, biz.AlertListQuery{AlertFilter: biz.AlertFilter{Labels: map[string]string{"env": "prod", "instance": "h1"}}}, []string{ids["host"]}},
		{"keyword", uAdmin, biz.AlertListQuery{AlertFilter: biz.AlertFilter{Keyword: " slow "}}, []string{ids["db"]}},
	} {
		if got := list(c.user, c.q); !slices.Equal(got, c.want) {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
	muted := true
	if got := list(uAlice, biz.AlertListQuery{AlertFilter: biz.AlertFilter{Muted: &muted}}); !slices.Equal(got, []string{ids["db"]}) {
		t.Errorf("muted filter: %v", got)
	}
	// 未给时间范围：活跃告警不受限，历史告警只回看 7 天。
	old := time.Now().Add(-30 * 24 * time.Hour)
	e.store.SeedAlert(&biz.Alert{ID: "al-ancient", GroupID: gHost, Severity: 1, Status: mc.AlertResolved, FirstFireTime: old, LastFireTime: old, ResolveTime: &old})
	e.store.SeedAlert(&biz.Alert{ID: "al-long-firing", GroupID: gHost, Severity: 1, Status: mc.AlertFiring, FirstFireTime: old, LastFireTime: old})
	got := list(uDave, biz.AlertListQuery{})
	if slices.Contains(got, "al-ancient") || !slices.Contains(got, "al-long-firing") {
		t.Errorf("default history window: %v", got)
	}
	// 给了时间范围就按范围查；跨度超过 90 天拒绝。
	from, to := old.Add(-time.Hour), old.Add(time.Hour)
	if got := list(uDave, biz.AlertListQuery{AlertFilter: biz.AlertFilter{FireStart: &from, FireEnd: &to}}); !slices.Equal(got, []string{"al-ancient", "al-long-firing"}) {
		t.Errorf("explicit range: %v", got)
	}
	wide := time.Now().Add(-200 * 24 * time.Hour)
	now := time.Now()
	_, _, err := e.alerts.List(ctx, uAdmin, biz.AlertListQuery{AlertFilter: biz.AlertFilter{FireStart: &wide, FireEnd: &now}}, 1, 10)
	wantErr(t, "range over 90 days", err, biz.ErrTimeRangeInvalid)
}

func TestAlert_GetAndMine(t *testing.T) {
	t.Parallel()
	d := newDispatchEnv(t)
	ctx := context.Background()
	// 规则在 host 分组；通知对象里有 carol（部门负责人），她对 host 没有范围。
	d.subscribe(t, uAlice, func(s *biz.SubscriptionSpec) {
		s.Name, s.Receivers = "给群", mc.Receivers{Groups: []string{"$group:ops"}}
	})
	// 通知前：范围外的人看不到，且与事件不存在是同一个错误。
	_, err := d.alerts.Get(ctx, uCarol, d.alert)
	wantErr(t, "out of scope before being notified", err, biz.ErrAlertNotFound)
	_, err = d.alerts.Get(ctx, uCarol, "no-such-alert")
	wantErr(t, "missing alert", err, biz.ErrAlertNotFound)
	d.store.SeedOutbox("ob-1", d.alert, mc.NotifyKindFiring, 1)
	d.run(t)

	// 在范围内：完整详情与通知时间线。
	detail, err := d.alerts.Get(ctx, uDave, d.alert)
	if err != nil || !detail.CanViewRule || !detail.Alert.RuleExists || detail.Alert.GroupName != "基础设施 / 主机" || detail.Alert.DurationS < 290 {
		t.Fatalf("in-scope detail = %+v err=%v", detail, err)
	}
	if len(detail.Logs) != 2 || detail.Logs[0].Source != mc.NotifySourceRule || detail.Logs[1].SubscriptionName != "给群" {
		t.Fatalf("timeline = %+v", detail.Logs)
	}
	var names []string
	for _, r := range detail.Logs[0].RecipientRefs {
		names = append(names, r.Type+":"+r.Name)
	}
	if !slices.Equal(names, []string{"user:Alice", "user:Bob", "user:Carol"}) {
		t.Fatalf("rule recipients = %v", names)
	}
	if g := detail.Logs[1].RecipientRefs; len(g) != 1 || g[0].Type != biz.RecipientGroup || g[0].ID != "$group:ops" {
		t.Fatalf("group recipient = %+v", g)
	}
	// 被通知到的人即使不在范围内也能看这一条事件，但不能看规则。
	detail, err = d.alerts.Get(ctx, uCarol, d.alert)
	if err != nil || detail.CanViewRule {
		t.Fatalf("recipient out of scope: %+v err=%v", detail, err)
	}
	// 没被通知、也不在范围内的人仍然看不到。
	_, err = d.alerts.Get(ctx, uGone, d.alert)
	wantErr(t, "neither in scope nor notified", err, biz.ErrAlertNotFound)

	// mine：只看通知过我的，以「我是接收方」为准——carol 对 host 没有范围，但她被通知了，
	// 所以能在「通知过我的」里看到这一条；不带 mine 的普通列表仍然按范围，她什么都看不到。
	other := "018f3e1a-aaaa-7000-8000-00000000a009"
	d.seedAlert(other, mc.SeverityCritical, map[string]string{"instance": "x"})
	list := func(user string, q biz.AlertListQuery) []string {
		t.Helper()
		views, _, err := d.alerts.List(ctx, user, q, 1, 10)
		if err != nil {
			t.Fatal(err)
		}
		return alertIDs(views)
	}
	mine := biz.AlertListQuery{Mine: true}
	if got := list(uBob, mine); !slices.Equal(got, []string{d.alert}) {
		t.Fatalf("bob's mine = %v", got)
	}
	if got := list(uDave, mine); len(got) != 0 {
		t.Fatalf("dave is in scope but was not notified: %v", got)
	}
	if got := list(uCarol, mine); !slices.Equal(got, []string{d.alert}) {
		t.Fatalf("a notified recipient outside the scope must see the alert under mine: %v", got)
	}
	if got := list(uCarol, biz.AlertListQuery{}); len(got) != 0 {
		t.Fatalf("the plain list stays within scope: %v", got)
	}
	if got := list(uGone, mine); len(got) != 0 {
		t.Fatalf("mine never shows alerts the user was not notified about: %v", got)
	}
	// mine + 分组筛选：按分组子树过滤，不要求对该分组有范围。
	if got := list(uCarol, biz.AlertListQuery{Mine: true, GroupID: gInfra}); !slices.Equal(got, []string{d.alert}) {
		t.Fatalf("mine within a group subtree = %v", got)
	}
	if got := list(uCarol, biz.AlertListQuery{Mine: true, GroupID: gApp}); len(got) != 0 {
		t.Fatalf("mine within another group = %v", got)
	}
}

func TestAlert_Aggregate(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	seedAlerts(e)
	agg := func(user string, q biz.AggregateQuery) (map[string]int32, int64) {
		t.Helper()
		items, total, err := e.alerts.Aggregate(ctx, user, q)
		if err != nil {
			t.Fatalf("Aggregate(%s, %s): %v", user, q.By, err)
		}
		out := map[string]int32{}
		for _, it := range items {
			out[it.Key+"|"+it.Name] = it.Count
		}
		return out, total
	}
	// 只统计活跃的、范围内的。
	got, total := agg(uAlice, biz.AggregateQuery{By: "severity"})
	if total != 2 || got["3|严重"] != 1 || got["4|致命"] != 1 || len(got) != 2 {
		t.Fatalf("by severity = %v total=%d", got, total)
	}
	got, total = agg(uAdmin, biz.AggregateQuery{By: "group"})
	if total != 3 || got[gHost+"|基础设施 / 主机"] != 1 || got[gApp+"|应用"] != 1 {
		t.Fatalf("by group = %v total=%d", got, total)
	}
	got, _ = agg(uAdmin, biz.AggregateQuery{By: "label:env"})
	if got["prod|prod"] != 2 || got["test|test"] != 1 {
		t.Fatalf("by label = %v", got)
	}
	got, _ = agg(uAdmin, biz.AggregateQuery{By: "rule"})
	if got["rule-cpu|cpu"] != 1 || len(got) != 3 {
		t.Fatalf("by rule = %v", got)
	}
	notMuted := false
	if _, total := agg(uAlice, biz.AggregateQuery{By: "severity", Muted: &notMuted}); total != 1 {
		t.Fatalf("muted filter total = %d", total)
	}
	if got, total := agg(uCarol, biz.AggregateQuery{By: "severity", GroupID: gInfra}); total != 0 || len(got) != 0 {
		t.Fatalf("out-of-scope group must aggregate to nothing: %v", got)
	}
	_, _, err := e.alerts.Aggregate(ctx, uAdmin, biz.AggregateQuery{By: "nonsense"})
	wantErr(t, "unknown dimension", err, biz.ErrQueryInvalid)
}

func onceMute(group string) biz.MuteSpec {
	start, end := time.Now().Add(-time.Hour), time.Now().Add(time.Hour)
	return biz.MuteSpec{
		GroupID: group, Name: "变更窗口", Reason: "发布", Mode: mc.MuteModeNotify, TimeType: mc.MuteTimeOnce,
		StartTime: &start, EndTime: &end, Enabled: true,
	}
}

func TestMute_Authorization(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	// 新建：作用分组须可管理；全局仅超管。
	_, err := e.mutes.Create(ctx, uBob, onceMute(gHost))
	wantErr(t, "read-only creates", err, biz.ErrForbidden)
	_, err = e.mutes.Create(ctx, uAlice, onceMute(gApp))
	wantErr(t, "create out of scope", err, biz.ErrForbidden)
	_, err = e.mutes.Create(ctx, uAlice, onceMute(""))
	wantErr(t, "non-admin creates global mute", err, biz.ErrForbidden)
	_, err = e.mutes.Create(ctx, uAdmin, onceMute("no-such-group"))
	wantErr(t, "unknown group", err, biz.ErrGroupNotFound)
	hostMute, err := e.mutes.Create(ctx, uAlice, onceMute(gHost))
	if err != nil || !hostMute.CanManage || hostMute.State != biz.MuteStateActive || hostMute.GroupName != "基础设施 / 主机" || hostMute.CreateUserName != "Alice" {
		t.Fatalf("create = %+v err=%v", hostMute, err)
	}
	global, err := e.mutes.Create(ctx, uAdmin, onceMute(""))
	if err != nil || global.Spec.GroupID != "" {
		t.Fatalf("global mute: %v", err)
	}
	appMute, err := e.mutes.Create(ctx, uCarol, onceMute(gApp))
	if err != nil {
		t.Fatal(err)
	}
	if e.store.CfgVersion(mc.CfgMute) != 3 {
		t.Fatalf("each write must bump the mute cfg version: %d", e.store.CfgVersion(mc.CfgMute))
	}

	// 读：范围内可见；全局的有任一范围即可见（只读）；没有任何范围的人什么都看不到。
	visible := func(user string) map[string]bool {
		t.Helper()
		views, _, err := e.mutes.List(ctx, user, biz.MuteListQuery{}, 1, 50)
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]bool{}
		for _, v := range views {
			out[v.ID] = v.CanManage
		}
		return out
	}
	if got := visible(uBob); len(got) != 2 || got[hostMute.ID] || got[global.ID] {
		t.Fatalf("bob sees %v (read-only on both)", got)
	}
	if got := visible(uCarol); len(got) != 2 || !got[appMute.ID] || got[global.ID] {
		t.Fatalf("carol sees %v", got)
	}
	if got := visible(uAdmin); len(got) != 3 || !got[global.ID] {
		t.Fatalf("admin sees %v", got)
	}
	if got := visible(uGone); len(got) != 0 {
		t.Fatalf("no grants sees %v", got)
	}
	_, err = e.mutes.Get(ctx, uCarol, hostMute.ID)
	wantErr(t, "get out of scope", err, biz.ErrForbidden)
	if v, err := e.mutes.Get(ctx, uBob, global.ID); err != nil || v.CanManage {
		t.Fatalf("global mute is readable but not manageable: %+v err=%v", v, err)
	}
	_, err = e.mutes.Get(ctx, uGone, global.ID)
	wantErr(t, "global mute for a user without any scope", err, biz.ErrForbidden)
	// 按分组过滤：作用于它、它的上级或下级的，加全局的。
	views, _, _ := e.mutes.List(ctx, uAlice, biz.MuteListQuery{GroupID: gInfra}, 1, 50)
	if got := alertLikeIDs(views); !slices.Equal(got, sorted(hostMute.ID, global.ID)) {
		t.Fatalf("filter by ancestor group = %v", got)
	}

	// 改 / 删：原分组与新分组都要可管理；乐观锁。
	_, err = e.mutes.Update(ctx, uBob, hostMute.ID, onceMute(gHost), 1)
	wantErr(t, "read-only updates", err, biz.ErrForbidden)
	_, err = e.mutes.Update(ctx, uCarol, hostMute.ID, onceMute(gApp), 1)
	wantErr(t, "steal a mute into own group", err, biz.ErrForbidden)
	_, err = e.mutes.Update(ctx, uAlice, hostMute.ID, onceMute(gApp), 1)
	wantErr(t, "move a mute to an unmanaged group", err, biz.ErrForbidden)
	_, err = e.mutes.Update(ctx, uAlice, hostMute.ID, onceMute(""), 1)
	wantErr(t, "widen a mute to global", err, biz.ErrForbidden)
	_, err = e.mutes.Update(ctx, uAlice, global.ID, onceMute(gHost), 1)
	wantErr(t, "non-admin edits global mute", err, biz.ErrForbidden)
	_, err = e.mutes.Update(ctx, uAlice, hostMute.ID, onceMute(gDB), 7)
	wantErr(t, "stale version", err, biz.ErrVersionConflict)
	if v, err := e.mutes.Update(ctx, uAlice, hostMute.ID, onceMute(gDB), 1); err != nil || v.Version != 2 || v.Spec.GroupID != gDB {
		t.Fatalf("update = %+v err=%v", v, err)
	}
	wantErr(t, "delete out of scope", e.mutes.Delete(ctx, uCarol, hostMute.ID), biz.ErrForbidden)
	wantErr(t, "non-admin deletes global", e.mutes.Delete(ctx, uAlice, global.ID), biz.ErrForbidden)
	if err := e.mutes.Delete(ctx, uAlice, hostMute.ID); err != nil {
		t.Fatal(err)
	}
	_, err = e.mutes.Get(ctx, uAlice, hostMute.ID)
	wantErr(t, "deleted", err, biz.ErrMuteNotFound)
}

func alertLikeIDs(views []*biz.MuteView) []string {
	out := make([]string, 0, len(views))
	for _, v := range views {
		out = append(out, v.ID)
	}
	slices.Sort(out)
	return out
}

func sorted(ids ...string) []string {
	slices.Sort(ids)
	return ids
}

func TestMute_Validation(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	hostRule := e.mustRule(t, uAlice, spec(gHost, "host-rule"))
	appRule := e.mustRule(t, uCarol, spec(gApp, "app-rule"))
	now := time.Now()
	cases := []struct {
		name   string
		mutate func(s *biz.MuteSpec)
	}{
		{"blank name", func(s *biz.MuteSpec) { s.Name = " " }},
		{"blank reason", func(s *biz.MuteSpec) { s.Reason = "" }},
		{"bad mode", func(s *biz.MuteSpec) { s.Mode = 3 }},
		{"bad time type", func(s *biz.MuteSpec) { s.TimeType = 0 }},
		{"once without end", func(s *biz.MuteSpec) { s.EndTime = nil }},
		{"once with periodic", func(s *biz.MuteSpec) { s.Periodic = &mc.Effective{} }},
		{"end before start", func(s *biz.MuteSpec) { end := now.Add(-2 * time.Hour); s.EndTime = &end }},
		{"periodic without windows", func(s *biz.MuteSpec) {
			s.TimeType, s.StartTime, s.EndTime, s.Periodic = mc.MuteTimePeriodic, nil, nil, &mc.Effective{}
		}},
		{"periodic bad window", func(s *biz.MuteSpec) {
			s.TimeType, s.StartTime, s.EndTime = mc.MuteTimePeriodic, nil, nil
			s.Periodic = &mc.Effective{Windows: []mc.TimeWindow{{Days: []int32{1}, Start: "25:00", End: "01:00"}}}
		}},
		{"bad severity", func(s *biz.MuteSpec) { s.Severities = []int32{9} }},
		{"bad regex", func(s *biz.MuteSpec) { s.Matchers = []mc.Matcher{{Label: "a", Op: mc.OpRegex, Value: "("}} }},
		{"in without values", func(s *biz.MuteSpec) { s.Matchers = []mc.Matcher{{Label: "a", Op: mc.OpIn}} }},
		{"in with value", func(s *biz.MuteSpec) {
			s.Matchers = []mc.Matcher{{Label: "a", Op: mc.OpIn, Value: "x", Values: []string{"x"}}}
		}},
		{"unknown op", func(s *biz.MuteSpec) { s.Matchers = []mc.Matcher{{Label: "a", Op: ">", Value: "1"}} }},
		{"unknown rule", func(s *biz.MuteSpec) { s.RuleIDs = []string{"nope"} }},
		{"rule outside the mute's group", func(s *biz.MuteSpec) { s.RuleIDs = []string{appRule.ID} }},
		{"unknown datasource", func(s *biz.MuteSpec) { s.DatasourceIDs = []string{"nope"} }},
		{"duplicate rules", func(s *biz.MuteSpec) { s.RuleIDs = []string{hostRule.ID, hostRule.ID} }},
	}
	for _, c := range cases {
		s := onceMute(gInfra)
		c.mutate(&s)
		_, err := e.mutes.Create(ctx, uAlice, s)
		wantErr(t, c.name, err, biz.ErrMuteInvalid)
	}
	// 合法：限定下级分组里的规则、周期时间窗、各类匹配条件。
	ok := onceMute(gInfra)
	ok.RuleIDs, ok.DatasourceIDs, ok.Severities = []string{hostRule.ID}, []string{dsVM}, []int32{3, 4}
	ok.TimeType, ok.StartTime, ok.EndTime = mc.MuteTimePeriodic, nil, nil
	ok.Periodic = &mc.Effective{Timezone: "Asia/Shanghai", Windows: []mc.TimeWindow{{Days: []int32{1, 2, 3, 4, 5, 6, 7}, Start: "00:00", End: "00:00"}}}
	ok.Matchers = []mc.Matcher{{Label: "env", Op: mc.OpNotIn, Values: []string{"test"}}, {Label: "instance", Op: mc.OpRegex, Value: "h.*"}}
	v, err := e.mutes.Create(ctx, uAlice, ok)
	if err != nil || v.State != biz.MuteStateActive || v.Rules[0].Name != "host-rule" || v.Datasources[0].Name != dsVM {
		t.Fatalf("valid periodic mute = %+v err=%v", v, err)
	}
	// 状态过滤。
	expired := onceMute(gHost)
	start, end := now.Add(-2*time.Hour), now.Add(-time.Hour)
	expired.StartTime, expired.EndTime = &start, &end
	disabled := onceMute(gHost)
	disabled.Enabled = false
	pending := onceMute(gHost)
	ps, pe := now.Add(time.Hour), now.Add(2*time.Hour)
	pending.StartTime, pending.EndTime = &ps, &pe
	for _, s := range []biz.MuteSpec{expired, disabled, pending} {
		if _, err := e.mutes.Create(ctx, uAlice, s); err != nil {
			t.Fatal(err)
		}
	}
	for state, want := range map[string]int64{biz.MuteStateActive: 1, biz.MuteStateExpired: 1, biz.MuteStateDisabled: 1, biz.MuteStatePending: 1, "": 4} {
		views, total, err := e.mutes.List(ctx, uAlice, biz.MuteListQuery{State: state}, 1, 50)
		if err != nil || total != want {
			t.Errorf("state %q: total = %d, want %d (err=%v)", state, total, want, err)
		}
		for _, v := range views {
			if state != "" && v.State != state {
				t.Errorf("state %q returned a %s mute", state, v.State)
			}
		}
	}
}

func TestMute_Preview(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	ids := seedAlerts(e)
	preview := func(user string, s biz.MuteSpec) []string {
		t.Helper()
		views, total, err := e.mutes.Preview(ctx, user, s)
		if err != nil {
			t.Fatalf("Preview(%s): %v", user, err)
		}
		if total != len(views) {
			t.Fatalf("total %d != %d", total, len(views))
		}
		return alertIDs(views)
	}
	// 只看活跃告警；作用分组含下级。
	if got := preview(uAlice, onceMute(gInfra)); !slices.Equal(got, []string{ids["db"], ids["host"]}) {
		t.Fatalf("infra mute hits %v", got)
	}
	// 只读用户也能预览；对作用分组没有范围（哪怕能读它的某个下级）不行。
	if got := preview(uBob, onceMute(gInfra)); !slices.Equal(got, []string{ids["db"], ids["host"]}) {
		t.Fatalf("read-only preview: %v", got)
	}
	_, _, err := e.mutes.Preview(ctx, uDave, onceMute(gInfra))
	wantErr(t, "preview on a group the caller cannot read", err, biz.ErrForbidden)
	_, _, err = e.mutes.Preview(ctx, uCarol, onceMute(gInfra))
	wantErr(t, "preview out of scope", err, biz.ErrForbidden)
	// 全局屏蔽的预览：各自只看到自己范围内的。
	if got := preview(uCarol, onceMute("")); !slices.Equal(got, []string{ids["app"]}) {
		t.Fatalf("global preview for carol = %v", got)
	}
	if got := preview(uDave, onceMute("")); !slices.Equal(got, []string{ids["host"]}) {
		t.Fatalf("global preview must be clipped to the caller's scope: %v", got)
	}
	if got := preview(uAdmin, onceMute("")); len(got) != 3 {
		t.Fatalf("global preview for admin = %v", got)
	}
	// 条件：级别、标签（与引擎同一套匹配器：缺失的标签按空串，正则整体锚定）。
	s := onceMute(gInfra)
	s.Severities = []int32{mc.SeverityFatal}
	if got := preview(uAlice, s); !slices.Equal(got, []string{ids["db"]}) {
		t.Fatalf("severity condition: %v", got)
	}
	s = onceMute(gInfra)
	s.Matchers = []mc.Matcher{{Label: "env", Op: mc.OpEqual, Value: "prod"}, {Label: "instance", Op: mc.OpRegex, Value: "h"}}
	if got := preview(uAlice, s); len(got) != 0 {
		t.Fatalf("regex must be anchored (h does not match h1): %v", got)
	}
	s.Matchers[1].Value = "h.*"
	if got := preview(uAlice, s); !slices.Equal(got, []string{ids["host"]}) {
		t.Fatalf("label conditions: %v", got)
	}
	s.Matchers = []mc.Matcher{{Label: "zone", Op: mc.OpNotEqual, Value: "cn"}}
	if got := preview(uAlice, s); len(got) != 2 {
		t.Fatalf("a missing label compares as empty: %v", got)
	}
	s.Matchers = []mc.Matcher{{Label: "a", Op: mc.OpRegex, Value: "("}}
	_, _, err = e.mutes.Preview(ctx, uAlice, s)
	wantErr(t, "invalid matcher", err, biz.ErrMuteInvalid)
	// 命中很多时只返回前 50 条，总数照实。
	for i := range 70 {
		e.store.SeedAlert(&biz.Alert{
			ID: fmt.Sprintf("al-many-%02d", i), GroupID: gHost, Severity: 2, Status: mc.AlertFiring,
			FirstFireTime: time.Now(), LastFireTime: time.Now(), Labels: map[string]string{"bulk": "y"},
		})
	}
	s = onceMute(gHost)
	s.Matchers = []mc.Matcher{{Label: "bulk", Op: mc.OpEqual, Value: "y"}}
	views, total, err := e.mutes.Preview(ctx, uAlice, s)
	if err != nil || total != 70 || len(views) != 50 {
		t.Fatalf("capped preview = %d items / total %d err=%v", len(views), total, err)
	}
}

func subSpec(groups ...string) biz.SubscriptionSpec {
	return biz.SubscriptionSpec{
		Name: "订阅", GroupIDs: groups, Severities: []int32{2, 3, 4}, OnFiring: true, Receivers: mc.Receivers{Self: true}, Enabled: true,
	}
}

func TestSubscription_Authorization(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	// 给自己订阅：只读即可；范围外不行。
	bobSub, err := e.subs.Create(ctx, uBob, subSpec(gInfra))
	if err != nil || bobSub.OwnerID != uBob || bobSub.OwnerName != "Bob" || !bobSub.CanManage || bobSub.Groups[0].Name != "基础设施" {
		t.Fatalf("self subscription with read scope = %+v err=%v", bobSub, err)
	}
	_, err = e.subs.Create(ctx, uBob, subSpec(gInfra, gApp))
	wantErr(t, "one of the groups is out of scope", err, biz.ErrForbidden)
	_, err = e.subs.Create(ctx, uGone, subSpec(gHost))
	wantErr(t, "no scope at all", err, biz.ErrForbidden)
	// 自己既是 self 又列在 user_ids 里：仍算给自己。
	selfListed := subSpec(gHost)
	selfListed.Receivers = mc.Receivers{UserIDs: []string{uBob}}
	if _, err := e.subs.Create(ctx, uBob, selfListed); err != nil {
		t.Fatalf("listing oneself is still a self subscription: %v", err)
	}
	// 接收方超出本人（他人 / 值班表 / 群）：要求对所选分组全部可管理。
	for name, r := range map[string]mc.Receivers{
		"other user": {UserIDs: []string{uAlice}},
		"schedule":   {Self: true, ScheduleIDs: []string{schedOps}},
		"group":      {Self: true, Groups: []string{"$group:ops"}},
	} {
		s := subSpec(gHost)
		s.Receivers = r
		_, err := e.subs.Create(ctx, uBob, s)
		wantErr(t, "read-only subscribes "+name, err, biz.ErrForbidden)
		if _, err := e.subs.Create(ctx, uAlice, s); err != nil {
			t.Fatalf("manager subscribes %s: %v", name, err)
		}
	}
	// 可管理须覆盖全部所选分组。
	e.org.UserRoles[uAlice] = []string{rInfraManage, rAppManage}
	e.org.UserRoles[uBob] = []string{rInfraRead, rAppManage}
	mixed := subSpec(gInfra, gApp)
	mixed.Receivers = mc.Receivers{Groups: []string{"$group:ops"}}
	_, err = e.subs.Create(ctx, uBob, mixed)
	wantErr(t, "manage on only some of the groups", err, biz.ErrForbidden)
	if _, err := e.subs.Create(ctx, uAlice, mixed); err != nil {
		t.Fatalf("manage on all groups: %v", err)
	}
	e.org.UserRoles[uAlice] = []string{rInfraManage}
	e.org.UserRoles[uBob] = []string{rInfraRead}

	// 校验。
	for name, mutate := range map[string]func(s *biz.SubscriptionSpec){
		"no groups":          func(s *biz.SubscriptionSpec) { s.GroupIDs = nil },
		"no severities":      func(s *biz.SubscriptionSpec) { s.Severities = nil },
		"no event":           func(s *biz.SubscriptionSpec) { s.OnFiring = false },
		"no receivers":       func(s *biz.SubscriptionSpec) { s.Receivers = mc.Receivers{} },
		"bad group marker":   func(s *biz.SubscriptionSpec) { s.Receivers.Groups = []string{"ops"} },
		"bad matcher":        func(s *biz.SubscriptionSpec) { s.Matchers = []mc.Matcher{{Label: "1x", Op: mc.OpEqual}} },
		"unknown rule":       func(s *biz.SubscriptionSpec) { s.RuleIDs = []string{"nope"} },
		"unknown receiver":   func(s *biz.SubscriptionSpec) { s.Receivers.UserIDs = []string{uGone} },
		"unknown schedule":   func(s *biz.SubscriptionSpec) { s.Receivers.ScheduleIDs = []string{"nope"} },
		"unknown datasource": func(s *biz.SubscriptionSpec) { s.DatasourceIDs = []string{"nope"} },
	} {
		s := subSpec(gHost)
		mutate(&s)
		_, err := e.subs.Create(ctx, uAlice, s)
		wantErr(t, name, err, biz.ErrSubscriptionInvalid)
	}

	// 读：mine 只有自己的；all = 订阅分组在我的可读范围内的（自己的恒可见）。
	count := func(user, scope string) int64 {
		t.Helper()
		_, total, err := e.subs.List(ctx, user, biz.SubscriptionListQuery{Scope: scope}, 1, 50)
		if err != nil {
			t.Fatal(err)
		}
		return total
	}
	if n := count(uBob, biz.SubscriptionScopeMine); n != 2 {
		t.Fatalf("bob mine = %d", n)
	}
	if n := count(uBob, biz.SubscriptionScopeAll); n != 6 {
		t.Fatalf("bob all = %d", n)
	}
	if n := count(uCarol, biz.SubscriptionScopeAll); n != 1 { // 只有那条同时订阅了 app 的
		t.Fatalf("carol all = %d", n)
	}
	if n := count(uGone, biz.SubscriptionScopeAll); n != 0 {
		t.Fatalf("no scope all = %d", n)
	}
	if n := count(uAdmin, biz.SubscriptionScopeAll); n != 6 {
		t.Fatalf("admin all = %d", n)
	}
	_, err = e.subs.Get(ctx, uCarol, bobSub.ID)
	wantErr(t, "get out of scope", err, biz.ErrForbidden)
	if v, err := e.subs.Get(ctx, uDave, bobSub.ID); err == nil || v != nil {
		t.Fatalf("dave only reads host, bob's subscription is on infra: %v", err)
	}

	// 改 / 删：所有者本人，或对全部订阅分组可管理的人；别人不行。
	renamed := subSpec(gInfra)
	renamed.Name = "改名"
	_, err = e.subs.Update(ctx, uCarol, bobSub.ID, renamed, 1)
	wantErr(t, "stranger updates", err, biz.ErrForbidden)
	_, err = e.subs.Update(ctx, uBob, bobSub.ID, renamed, 5)
	wantErr(t, "stale version", err, biz.ErrVersionConflict)
	if v, err := e.subs.Update(ctx, uBob, bobSub.ID, renamed, 1); err != nil || v.Spec.Name != "改名" || v.OwnerID != uBob || v.Version != 2 {
		t.Fatalf("owner updates = %+v err=%v", v, err)
	}
	// 所有者自己把接收方扩到他人：仍需可管理。
	widened := renamed
	widened.Receivers = mc.Receivers{Self: true, UserIDs: []string{uAlice}}
	_, err = e.subs.Update(ctx, uBob, bobSub.ID, widened, 2)
	wantErr(t, "owner widens receivers without manage", err, biz.ErrForbidden)
	// 分组管理员可代为维护，所有者不变。
	if v, err := e.subs.Update(ctx, uAlice, bobSub.ID, renamed, 2); err != nil || v.OwnerID != uBob || v.Version != 3 {
		t.Fatalf("group manager updates = %+v err=%v", v, err)
	}
	wantErr(t, "stranger deletes", e.subs.Delete(ctx, uCarol, bobSub.ID), biz.ErrForbidden)
	if err := e.subs.Delete(ctx, uBob, bobSub.ID); err != nil {
		t.Fatal(err)
	}

	// 所有者失去范围后：列表里标出已无权访问的分组。
	daveSub, err := e.subs.Create(ctx, uDave, subSpec(gHost))
	if err != nil {
		t.Fatal(err)
	}
	e.org.UserRoles[uDave] = nil
	v, err := e.subs.Get(ctx, uAlice, daveSub.ID)
	if err != nil || !slices.Equal(v.OutOfScopeGroups, []string{gHost}) || !v.CanManage {
		t.Fatalf("out-of-scope marker = %+v err=%v", v, err)
	}
	// 所有者本人仍能看到并删除自己的订阅。
	if v, err := e.subs.Get(ctx, uDave, daveSub.ID); err != nil || !v.CanManage {
		t.Fatalf("owner still sees own subscription: %v", err)
	}
}

func TestSubscription_PreviewAndHits(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	ids := seedAlerts(e)
	old := time.Now().Add(-48 * time.Hour)
	e.store.SeedAlert(&biz.Alert{ID: "al-2d", GroupID: gHost, Severity: 3, Status: mc.AlertResolved, FirstFireTime: old, LastFireTime: old, ResolveTime: &old})
	preview := func(user string, s biz.SubscriptionSpec) []string {
		t.Helper()
		views, total, err := e.subs.Preview(ctx, user, s)
		if err != nil {
			t.Fatalf("Preview: %v", err)
		}
		if total != len(views) {
			t.Fatalf("total %d != %d", total, len(views))
		}
		return alertIDs(views)
	}
	closedAt := time.Now().Add(-time.Hour)
	e.store.SeedAlert(&biz.Alert{ID: "al-closed", GroupID: gHost, Severity: 3, Status: mc.AlertClosed, FirstFireTime: closedAt, LastFireTime: closedAt, ResolveTime: &closedAt})
	// 活跃 + 近 24 小时的历史（触发过即算，含之后被关闭的）；被屏蔽的不发给订阅者，不计入；48 小时前的不计入。
	if got := preview(uAlice, subSpec(gInfra)); !slices.Equal(got, []string{"al-closed", ids["host"], ids["host-old"]}) {
		t.Fatalf("infra subscription preview = %v", got)
	}
	s := subSpec(gInfra)
	s.Severities = []int32{mc.SeverityCritical}
	if got := preview(uAlice, s); !slices.Equal(got, []string{"al-closed", ids["host"]}) {
		t.Fatalf("severity = %v", got)
	}
	s = subSpec(gInfra)
	s.OnFiring, s.OnResolved = false, true
	if got := preview(uAlice, s); !slices.Equal(got, []string{ids["host-old"]}) {
		t.Fatalf("resolved-only = %v", got)
	}
	s = subSpec(gInfra)
	s.Matchers = []mc.Matcher{{Label: "instance", Op: mc.OpIn, Values: []string{"h2", "zz"}}}
	if got := preview(uAlice, s); !slices.Equal(got, []string{ids["host-old"]}) {
		t.Fatalf("matcher = %v", got)
	}
	_, _, err := e.subs.Preview(ctx, uCarol, subSpec(gInfra))
	wantErr(t, "preview out of scope", err, biz.ErrForbidden)

	// 命中统计来自通知记录。
	d := newDispatchEnv(t)
	sub := d.subscribe(t, uDave, nil)
	if v, _ := d.subs.Get(ctx, uDave, sub.ID); v.LastHitTime != nil || v.HitCount7d != 0 {
		t.Fatalf("fresh subscription must have no hits: %+v", v)
	}
	d.store.SeedOutbox("ob-1", d.alert, mc.NotifyKindFiring, 1)
	d.run(t)
	if v, _ := d.subs.Get(ctx, uDave, sub.ID); v.LastHitTime == nil || v.HitCount7d != 1 {
		t.Fatalf("hits after dispatch = %v / %d", v.LastHitTime, v.HitCount7d)
	}
}
