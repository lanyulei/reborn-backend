package biz_test

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/kit/monitorpipeline"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// p2envF 在 P1 的 env 之上装配抑制规则与记录规则用例。
type p2envF struct {
	*env
	st       *testfake.MonitorInhibitStore
	inhibits *biz.InhibitUsecase
	records  *biz.RecordRuleUsecase
}

type countingDecorator struct{ calls, views int }

func (d *countingDecorator) Decorate(_ context.Context, views []*biz.AlertView) error {
	d.calls++
	d.views += len(views)
	return nil
}

func newP2EnvF(t *testing.T, dec biz.AlertViewDecorator) *p2envF {
	t.Helper()
	e := newEnv(t)
	st := testfake.NewMonitorInhibitStore()
	st.Groups = map[string]bool{gInfra: true, gHost: true, gDB: true, gApp: true}
	st.Targets = map[string]biz.DatasourceWriteTarget{
		"ds-w":    {Name: "可写", Enabled: true, HasWriteURL: true},
		"ds-w2":   {Name: "可写2", Enabled: true, HasWriteURL: true},
		"ds-ro":   {Name: "只读", Enabled: true},
		"ds-down": {Name: "停用", HasWriteURL: true},
	}
	ms := e.store
	return &p2envF{
		env: e, st: st,
		inhibits: biz.ProvideInhibitUsecase(e.scopes, st, st, st, ms.Alerts(), ms.Rules(), ms.Rules(), ms.Mutes(), e.org, dec),
		records:  biz.NewRecordRuleUsecase(e.scopes, st.RecordRules(), st.RecordRules(), st, e.org),
	}
}

func inhibitSpec(groupID string) biz.InhibitSpec {
	return biz.InhibitSpec{
		GroupID: groupID, Name: "主机失联", Enabled: true, EqualLabels: []string{"ident"},
		Source: mc.InhibitSelector{Matchers: []mc.Matcher{{Label: "alertname", Op: mc.OpEqual, Value: "HostDown"}}},
	}
}

func TestInhibit_Authorization(t *testing.T) {
	t.Parallel()
	e := newP2EnvF(t, nil)
	ctx := context.Background()

	// 新建：可管理才行；全局仅超级管理员；分组须存在。
	_, err := e.inhibits.Create(ctx, uBob, inhibitSpec(gInfra))
	wantErr(t, "read-only create", err, biz.ErrForbidden)
	_, err = e.inhibits.Create(ctx, uCarol, inhibitSpec(gInfra))
	wantErr(t, "out-of-scope create", err, biz.ErrForbidden)
	_, err = e.inhibits.Create(ctx, uAlice, inhibitSpec(""))
	wantErr(t, "global create by non-admin", err, biz.ErrForbidden)
	_, err = e.inhibits.Create(ctx, uAdmin, inhibitSpec("no-such-group"))
	wantErr(t, "missing group", err, biz.ErrGroupNotFound)

	in, err := e.inhibits.Create(ctx, uAlice, inhibitSpec(gInfra))
	if err != nil || in.Version != 1 || !in.CanManage || in.GroupName != "基础设施" || in.CreateUserName != "Alice" || e.st.InhibitCfgBumps() != 1 {
		t.Fatalf("create = %+v err=%v bumps=%d", in, err, e.st.InhibitCfgBumps())
	}
	global, err := e.inhibits.Create(ctx, uAdmin, inhibitSpec(""))
	if err != nil {
		t.Fatal(err)
	}

	// 查看：只读可见、范围外 403；全局规则有任一范围即可见，没有任何范围的人看不到。
	if v, err := e.inhibits.Get(ctx, uBob, in.ID); err != nil || v.CanManage {
		t.Fatalf("read-only get = %+v err=%v", v, err)
	}
	_, err = e.inhibits.Get(ctx, uCarol, in.ID)
	wantErr(t, "out-of-scope get", err, biz.ErrForbidden)
	if v, err := e.inhibits.Get(ctx, uCarol, global.ID); err != nil || v.CanManage {
		t.Fatalf("global get by scoped user = %+v err=%v", v, err)
	}
	_, err = e.inhibits.Get(ctx, uGone, global.ID)
	wantErr(t, "global get without any scope", err, biz.ErrForbidden)
	_, err = e.inhibits.Get(ctx, uAdmin, "missing")
	wantErr(t, "missing", err, biz.ErrInhibitNotFound)

	// 列表：范围过滤 + 全局；按分组过滤含上级与下级（全局规则对任何分组都生效，照常列出）。
	e.st.Counts[in.ID] = 3
	for _, c := range []struct {
		user, group string
		want        int
	}{{uAdmin, "", 2}, {uBob, "", 2}, {uCarol, "", 1}, {uGone, "", 0}, {uDave, "", 1}, {uBob, gHost, 2}, {uCarol, gApp, 1}} {
		items, total, err := e.inhibits.List(ctx, c.user, biz.InhibitListQuery{GroupID: c.group}, 1, 20)
		if err != nil || int(total) != c.want || len(items) != c.want {
			t.Fatalf("list as %s group=%q = %d/%d err=%v, want %d", c.user, c.group, len(items), total, err, c.want)
		}
	}
	items, _, _ := e.inhibits.List(ctx, uBob, biz.InhibitListQuery{GroupID: gHost}, 1, 20)
	if i := slices.IndexFunc(items, func(v *biz.InhibitView) bool { return v.ID == in.ID }); i < 0 || items[i].InhibitedCount != 3 {
		t.Fatalf("inhibited count missing: %+v", items)
	}
	off := false
	if _, total, _ := e.inhibits.List(ctx, uAdmin, biz.InhibitListQuery{Enabled: &off, Keyword: "主机"}, 1, 20); total != 0 {
		t.Fatalf("enabled filter total = %d", total)
	}

	// 编辑 / 删除：只读与范围外 403；移到无权管理的分组 403；全局仅超级管理员；乐观锁。
	_, err = e.inhibits.Update(ctx, uBob, in.ID, inhibitSpec(gInfra), 1)
	wantErr(t, "read-only update", err, biz.ErrForbidden)
	_, err = e.inhibits.Update(ctx, uAlice, in.ID, inhibitSpec(gApp), 1)
	wantErr(t, "move to unmanaged group", err, biz.ErrForbidden)
	_, err = e.inhibits.Update(ctx, uAlice, in.ID, inhibitSpec(""), 1)
	wantErr(t, "make global by non-admin", err, biz.ErrForbidden)
	_, err = e.inhibits.Update(ctx, uAlice, global.ID, inhibitSpec(gInfra), 1)
	wantErr(t, "edit global by non-admin", err, biz.ErrForbidden)
	_, err = e.inhibits.Update(ctx, uAlice, in.ID, inhibitSpec(gHost), 9)
	wantErr(t, "stale version", err, biz.ErrVersionConflict)
	upd, err := e.inhibits.Update(ctx, uAlice, in.ID, inhibitSpec(gHost), 1)
	if err != nil || upd.Version != 2 || upd.Spec.GroupID != gHost || e.st.InhibitCfgBumps() != 3 {
		t.Fatalf("update = %+v err=%v", upd, err)
	}
	wantErr(t, "read-only delete", e.inhibits.Delete(ctx, uBob, in.ID), biz.ErrForbidden)
	wantErr(t, "global delete by non-admin", e.inhibits.Delete(ctx, uAlice, global.ID), biz.ErrForbidden)
	if err := e.inhibits.Delete(ctx, uAlice, in.ID); err != nil || e.st.InhibitCfgBumps() != 4 {
		t.Fatalf("delete err=%v", err)
	}
	if names, _ := e.st.InhibitNames(ctx, []string{in.ID}); names[in.ID] != "主机失联" {
		t.Fatalf("deleted inhibit must keep its name: %v", names)
	}
}

func TestInhibit_Validation(t *testing.T) {
	t.Parallel()
	e := newP2EnvF(t, nil)
	ctx := context.Background()
	hostRule := e.mustRule(t, uAlice, spec(gHost, "host-down"))
	appRule := e.mustRule(t, uCarol, spec(gApp, "app-rule"))
	for name, mutate := range map[string]func(*biz.InhibitSpec){
		"blank name":          func(s *biz.InhibitSpec) { s.Name = "  " },
		"long name":           func(s *biz.InhibitSpec) { s.Name = strings.Repeat("长", 129) },
		"long description":    func(s *biz.InhibitSpec) { s.Description = strings.Repeat("x", 256) },
		"empty source":        func(s *biz.InhibitSpec) { s.Source = mc.InhibitSelector{} },
		"no target no equal":  func(s *biz.InhibitSpec) { s.EqualLabels = nil },
		"bad equal label":     func(s *biz.InhibitSpec) { s.EqualLabels = []string{"1bad"} },
		"bad regex":           func(s *biz.InhibitSpec) { s.Target.Matchers = []mc.Matcher{{Label: "a", Op: mc.OpRegex, Value: "("}} },
		"bad severity":        func(s *biz.InhibitSpec) { s.Target.Severities = []int32{9} },
		"duplicate rule":      func(s *biz.InhibitSpec) { s.Source.RuleIDs = []string{hostRule.ID, hostRule.ID} },
		"missing rule":        func(s *biz.InhibitSpec) { s.Target.RuleIDs = []string{"no-such-rule"} },
		"rule outside group":  func(s *biz.InhibitSpec) { s.Source.RuleIDs = []string{appRule.ID} },
		"target rule outside": func(s *biz.InhibitSpec) { s.Target.RuleIDs = []string{hostRule.ID, appRule.ID} },
	} {
		s := inhibitSpec(gInfra)
		mutate(&s)
		_, err := e.inhibits.Create(ctx, uAdmin, s)
		wantErr(t, name, err, biz.ErrInhibitInvalid)
	}
	// 不存在与不在作用分组内是同一句话。
	a := inhibitSpec(gInfra)
	a.Source.RuleIDs = []string{"no-such-rule"}
	_, errMissing := e.inhibits.Create(ctx, uAdmin, a)
	a.Source.RuleIDs = []string{appRule.ID}
	_, errOutside := e.inhibits.Create(ctx, uAdmin, a)
	if kerrors.FromError(errMissing).Message != kerrors.FromError(errOutside).Message {
		t.Fatalf("messages differ: %v / %v", errMissing, errOutside)
	}
	// 下级分组里的规则可以；全局规则可以限定任何分组的规则。
	ok := inhibitSpec(gInfra)
	ok.Source.RuleIDs, ok.Target.Severities = []string{hostRule.ID}, []int32{mc.SeverityWarning}
	v, err := e.inhibits.Create(ctx, uAlice, ok)
	if err != nil || len(v.SourceRules) != 1 || v.SourceRules[0].Name != "host-down" || v.SourceRules[0].Missing {
		t.Fatalf("create with descendant rule = %+v err=%v", v, err)
	}
	g := inhibitSpec("")
	g.Source.RuleIDs = []string{hostRule.ID, appRule.ID}
	if _, err := e.inhibits.Create(ctx, uAdmin, g); err != nil {
		t.Fatalf("global with rules from any group: %v", err)
	}
	// 预览：形状不合法 400；范围外 403；全局预览需要任一范围。
	bad := inhibitSpec(gInfra)
	bad.Source = mc.InhibitSelector{}
	_, err = e.inhibits.Preview(ctx, uBob, bad)
	wantErr(t, "preview invalid", err, biz.ErrInhibitInvalid)
	_, err = e.inhibits.Preview(ctx, uCarol, inhibitSpec(gInfra))
	wantErr(t, "preview out of scope", err, biz.ErrForbidden)
	_, err = e.inhibits.Preview(ctx, uGone, inhibitSpec(""))
	wantErr(t, "preview global without scope", err, biz.ErrForbidden)
}

func viewIDs(views []*biz.AlertView) []string {
	out := make([]string, 0, len(views))
	for _, v := range views {
		out = append(out, v.ID)
	}
	slices.Sort(out)
	return out
}

// 预览与 kit 的抑制判定逐例对照：同一批告警、同一条规则，预览给出的目标必须恰好是直接调用
// monitorpipeline 得到的命中集合。
func TestInhibit_PreviewMatchesPipeline(t *testing.T) {
	t.Parallel()
	now := time.Now()
	type al struct {
		id, group, rule string
		sev             int32
		labels          map[string]string
		inhibited       bool
	}
	alerts := []al{
		{"a01", gHost, "r-down", 4, map[string]string{"alertname": "HostDown", "ident": "h1"}, false},
		{"a02", gHost, "r-cpu", 2, map[string]string{"alertname": "CPU", "ident": "h1"}, false},
		{"a03", gHost, "r-cpu", 2, map[string]string{"alertname": "CPU", "ident": "h2"}, false},
		{"a04", gDB, "r-db", 3, map[string]string{"alertname": "Slow", "ident": "h1"}, false},
		{"a05", gApp, "r-app", 2, map[string]string{"alertname": "CPU", "ident": "h1"}, false},
		{"a06", gHost, "r-down", 4, map[string]string{"alertname": "HostDown", "ident": "h3"}, true}, // 被抑制的告警不能做源
		{"a07", gHost, "r-cpu", 2, map[string]string{"alertname": "CPU", "ident": "h3"}, false},
		{"a08", gHost, "r-cpu", 2, map[string]string{"alertname": "CPU"}, false},                 // 没有 ident
		{"a09", gDB, "r-down", 4, map[string]string{"alertname": "HostDown"}, false},             // 没有 ident 的源
		{"a10", gHost, "r-disk", 2, map[string]string{"alertname": "Disk", "ident": "h1"}, true}, // 已被抑制的目标仍在预览里
		{"a11", gHost, "r-down", 4, map[string]string{"alertname": "HostDown", "ident": "h1"}, false},
	}
	specs := map[string]biz.InhibitSpec{
		"equal only": inhibitSpec(gInfra),
		"target severities": func() biz.InhibitSpec {
			s := inhibitSpec(gInfra)
			s.Target.Severities = []int32{2}
			return s
		}(),
		"both sides match (no mutual inhibition)": {
			GroupID: gInfra, Name: "x", EqualLabels: []string{"ident"},
			Source: mc.InhibitSelector{Severities: []int32{2, 4}}, Target: mc.InhibitSelector{Severities: []int32{2, 4}},
		},
		"rule ids, no equal": {
			GroupID: gHost, Name: "x",
			Source: mc.InhibitSelector{RuleIDs: []string{"r-down"}}, Target: mc.InhibitSelector{RuleIDs: []string{"r-cpu"}},
		},
		"sub group only": inhibitSpec(gDB),
		"global": func() biz.InhibitSpec {
			s := inhibitSpec("")
			s.Target.Matchers = []mc.Matcher{{Label: "alertname", Op: mc.OpNotEqual, Value: "HostDown"}}
			return s
		}(),
	}
	for name, sp := range specs {
		dec := &countingDecorator{}
		e := newP2EnvF(t, dec)
		ctx := context.Background()
		scope, err := e.scopes.Resolve(ctx, uAdmin)
		if err != nil {
			t.Fatal(err)
		}
		var active, all []monitorpipeline.AlertRef
		var wantSources []string
		for _, a := range alerts {
			e.store.SeedAlert(&biz.Alert{
				ID: a.id, GroupID: a.group, RuleID: a.rule, RuleName: a.rule, Severity: a.sev, Status: mc.AlertFiring, Labels: a.labels,
				Inhibited: a.inhibited, FirstFireTime: now, LastFireTime: now,
			})
			g, _ := scope.Group(a.group)
			ref := monitorpipeline.AlertRef{ID: a.id, RuleID: a.rule, GroupPath: g.Path, Severity: a.sev, Labels: a.labels}
			all = append(all, ref)
			if !a.inhibited {
				active = append(active, ref)
			}
		}
		// 一条已恢复的告警：不参与。
		e.store.SeedAlert(&biz.Alert{
			ID: "a99", GroupID: gHost, RuleID: "r-down", Severity: 4, Status: mc.AlertResolved, ResolveTime: &now,
			Labels: map[string]string{"alertname": "HostDown", "ident": "h2"}, FirstFireTime: now, LastFireTime: now,
		})
		path := ""
		if g, ok := scope.Group(sp.GroupID); ok {
			path = g.Path
		}
		rule := monitorpipeline.InhibitRule{ID: "x", GroupPath: path, Source: sp.Source, Target: sp.Target, Equal: sp.EqualLabels}
		snap := monitorpipeline.CompileInhibits([]monitorpipeline.InhibitRule{rule}).Snapshot(active)
		var want []string
		for i := range all {
			if _, hit := snap.Match(&all[i]); hit {
				want = append(want, all[i].ID)
			}
		}
		// 源：未被抑制、在作用分组内、满足源条件（独立地用 MatchAll 算一遍）。
		for _, a := range alerts {
			g, _ := scope.Group(a.group)
			inScope := path == "" || strings.HasPrefix(g.Path, path)
			if a.inhibited || !inScope || (len(sp.Source.RuleIDs) > 0 && !slices.Contains(sp.Source.RuleIDs, a.rule)) ||
				(len(sp.Source.Severities) > 0 && !slices.Contains(sp.Source.Severities, a.sev)) || !mc.MatchAll(sp.Source.Matchers, a.labels) {
				continue
			}
			wantSources = append(wantSources, a.id)
		}
		slices.Sort(want)
		slices.Sort(wantSources)
		p, err := e.inhibits.Preview(ctx, uAdmin, sp)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got := viewIDs(p.Targets); !slices.Equal(got, want) || p.TargetTotal != len(want) {
			t.Errorf("%s: targets = %v (total %d), kit says %v", name, got, p.TargetTotal, want)
		}
		if got := viewIDs(p.Sources); !slices.Equal(got, wantSources) || p.SourceTotal != len(wantSources) {
			t.Errorf("%s: sources = %v (total %d), want %v", name, got, p.SourceTotal, wantSources)
		}
		if dec.calls != 1 || dec.views != len(p.Sources)+len(p.Targets) {
			t.Errorf("%s: decorator calls=%d views=%d", name, dec.calls, dec.views)
		}
	}
	// 固定预期（防止两边一起错）：equal only 的规则下 h1 的非源告警被抑制，h3 的源已被抑制所以 a07 不受影响。
	e := newP2EnvF(t, nil)
	for _, a := range alerts {
		e.store.SeedAlert(&biz.Alert{
			ID: a.id, GroupID: a.group, RuleID: a.rule, Severity: a.sev, Status: mc.AlertFiring, Labels: a.labels,
			Inhibited: a.inhibited, FirstFireTime: now, LastFireTime: now,
		})
	}
	p, err := e.inhibits.Preview(context.Background(), uBob, inhibitSpec(gInfra))
	if err != nil || !slices.Equal(viewIDs(p.Targets), []string{"a02", "a04", "a08", "a10"}) || !slices.Equal(viewIDs(p.Sources), []string{"a01", "a09", "a11"}) {
		t.Fatalf("fixed expectation: targets=%v sources=%v err=%v", viewIDs(p.Targets), viewIDs(p.Sources), err)
	}
	if !p.Targets[0].CanAck || p.Targets[0].CanManage {
		t.Fatalf("read-only caller flags = %+v", p.Targets[0])
	}
	// 只读 host 的人预览 infra：403；预览 host 只看得到 host 的告警（a08 的源 a09 在 db 分组，作用范围外）。
	_, err = e.inhibits.Preview(context.Background(), uDave, inhibitSpec(gInfra))
	wantErr(t, "preview parent group", err, biz.ErrForbidden)
	p, err = e.inhibits.Preview(context.Background(), uDave, inhibitSpec(gHost))
	if err != nil || slices.Contains(viewIDs(p.Targets), "a04") || !slices.Equal(viewIDs(p.Targets), []string{"a02", "a10"}) {
		t.Fatalf("scoped preview = %v err=%v", viewIDs(p.Targets), err)
	}
}

func TestInhibit_PreviewCapsItems(t *testing.T) {
	t.Parallel()
	e := newP2EnvF(t, nil)
	now := time.Now()
	e.store.SeedAlert(&biz.Alert{ID: "src", GroupID: gHost, Severity: 4, Status: mc.AlertFiring, Labels: map[string]string{"alertname": "HostDown"}, FirstFireTime: now, LastFireTime: now})
	for i := range 70 {
		e.store.SeedAlert(&biz.Alert{
			ID: "t" + string(rune('A'+i/26)) + string(rune('a'+i%26)), GroupID: gHost, Severity: 2, Status: mc.AlertFiring,
			Labels: map[string]string{"alertname": "CPU"}, FirstFireTime: now, LastFireTime: now,
		})
	}
	p, err := e.inhibits.Preview(context.Background(), uAdmin, inhibitSpec(gInfra))
	if err != nil || len(p.Targets) != 50 || p.TargetTotal != 70 || len(p.Sources) != 1 || p.SourceTotal != 1 {
		t.Fatalf("preview = %d/%d sources %d/%d err=%v", len(p.Targets), p.TargetTotal, len(p.Sources), p.SourceTotal, err)
	}
}

func recordSpec(groupID, name string) biz.RecordRuleSpec {
	return biz.RecordRuleSpec{GroupID: groupID, Name: name, DatasourceID: "ds-w", Expr: "sum(rate(http_requests_total[5m])) by (job)", IntervalS: 60, Enabled: true}
}

func TestRecordRule_Authorization(t *testing.T) {
	t.Parallel()
	e := newP2EnvF(t, nil)
	ctx := context.Background()
	_, err := e.records.Create(ctx, uBob, recordSpec(gHost, "job:req:rate5m"))
	wantErr(t, "read-only create", err, biz.ErrForbidden)
	_, err = e.records.Create(ctx, uCarol, recordSpec(gHost, "job:req:rate5m"))
	wantErr(t, "out-of-scope create", err, biz.ErrForbidden)
	_, err = e.records.Create(ctx, uAdmin, recordSpec("no-such-group", "job:req:rate5m"))
	wantErr(t, "missing group", err, biz.ErrGroupNotFound)
	_, err = e.records.Create(ctx, uAlice, recordSpec("no-such-group", "job:req:rate5m"))
	wantErr(t, "missing group as non-admin is forbidden, not 404", err, biz.ErrForbidden)

	r, err := e.records.Create(ctx, uAlice, recordSpec(gHost, "job:req:rate5m"))
	if err != nil || r.Version != 1 || !r.CanManage || !r.Writable || r.DatasourceName != "可写" || r.GroupName != "基础设施 / 主机" || r.UpdateUserName != "Alice" || r.Task != nil {
		t.Fatalf("create = %+v err=%v", r, err)
	}
	if v, err := e.records.Get(ctx, uDave, r.ID); err != nil || v.CanManage {
		t.Fatalf("read-only get = %+v err=%v", v, err)
	}
	_, err = e.records.Get(ctx, uCarol, r.ID)
	wantErr(t, "out-of-scope get", err, biz.ErrForbidden)
	_, err = e.records.Get(ctx, uAdmin, "missing")
	wantErr(t, "missing", err, biz.ErrRecordRuleNotFound)

	app, err := e.records.Create(ctx, uCarol, recordSpec(gApp, "app:req:rate5m"))
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		user, group string
		want        int
	}{{uAdmin, "", 2}, {uBob, "", 1}, {uCarol, "", 1}, {uGone, "", 0}, {uBob, gApp, 0}, {uAlice, gInfra, 1}} {
		items, total, err := e.records.List(ctx, c.user, biz.RecordRuleListQuery{GroupID: c.group}, 1, 20)
		if err != nil || int(total) != c.want || len(items) != c.want {
			t.Fatalf("list as %s group=%q = %d/%d err=%v, want %d", c.user, c.group, len(items), total, err, c.want)
		}
	}

	// 编辑：只读 403；换分组要求两边都可管理；乐观锁。
	_, err = e.records.Update(ctx, uBob, r.ID, recordSpec(gHost, "job:req:rate5m"), 1)
	wantErr(t, "read-only update", err, biz.ErrForbidden)
	_, err = e.records.Update(ctx, uAlice, r.ID, recordSpec(gApp, "job:req:rate5m"), 1)
	wantErr(t, "move to unmanaged group", err, biz.ErrForbidden)
	_, err = e.records.Update(ctx, uCarol, r.ID, recordSpec(gApp, "job:req:rate5m"), 1)
	wantErr(t, "move from unmanaged group", err, biz.ErrForbidden)
	_, err = e.records.Update(ctx, uAlice, r.ID, recordSpec(gDB, "job:req:rate5m"), 7)
	wantErr(t, "stale version", err, biz.ErrVersionConflict)
	moved, err := e.records.Update(ctx, uAlice, r.ID, recordSpec(gDB, "job:req:rate5m"), 1)
	if err != nil || moved.Version != 2 || moved.Spec.GroupID != gDB {
		t.Fatalf("move = %+v err=%v", moved, err)
	}

	// 启停：只读 403；已是目标状态不写入；停用后不展示执行状态。
	_, err = e.records.Toggle(ctx, uBob, r.ID, false)
	wantErr(t, "read-only toggle", err, biz.ErrForbidden)
	if v, err := e.records.Toggle(ctx, uAlice, r.ID, true); err != nil || v.Version != 2 {
		t.Fatalf("no-op toggle = %+v err=%v", v, err)
	}
	at := time.Now()
	e.st.SeedRecordTask(r.ID, biz.RecordTask{LastStatus: mc.RecordWriteError, LastError: "http: Bad Gateway", LastEvalTime: &at, LastSeries: 3})
	if v, _ := e.records.Get(ctx, uAlice, r.ID); v.Task == nil || v.Task.LastStatus != mc.RecordWriteError {
		t.Fatalf("task = %+v", v.Task)
	}
	status := mc.RecordWriteError
	if _, total, _ := e.records.List(ctx, uAdmin, biz.RecordRuleListQuery{RecordRuleFilter: biz.RecordRuleFilter{LastStatus: &status}}, 1, 20); total != 1 {
		t.Fatalf("last_status filter total = %d", total)
	}
	offV, err := e.records.Toggle(ctx, uAlice, r.ID, false)
	if err != nil || offV.Version != 3 || offV.Spec.Enabled || offV.Task != nil {
		t.Fatalf("disable = %+v err=%v", offV, err)
	}
	if _, total, _ := e.records.List(ctx, uAdmin, biz.RecordRuleListQuery{RecordRuleFilter: biz.RecordRuleFilter{LastStatus: &status}}, 1, 20); total != 0 {
		t.Fatalf("disabled rule must count as never-run, total = %d", total)
	}
	// 重新启用时数据源须可用。
	e.st.Targets["ds-w"] = biz.DatasourceWriteTarget{Name: "可写", HasWriteURL: true}
	_, err = e.records.Toggle(ctx, uAlice, r.ID, true)
	wantErr(t, "enable on disabled datasource", err, biz.ErrRecordRuleInvalid)
	e.st.Targets["ds-w"] = biz.DatasourceWriteTarget{Name: "可写", Enabled: true}
	_, err = e.records.Toggle(ctx, uAlice, r.ID, true)
	wantErr(t, "enable without write url", err, biz.ErrRecordRuleNoWriteURL)
	if v, _ := e.records.Get(ctx, uAlice, r.ID); v.Writable {
		t.Fatal("writable must follow the datasource")
	}
	e.st.Targets["ds-w"] = biz.DatasourceWriteTarget{Name: "可写", Enabled: true, HasWriteURL: true}

	wantErr(t, "read-only delete", e.records.Delete(ctx, uBob, r.ID), biz.ErrForbidden)
	wantErr(t, "out-of-scope delete", e.records.Delete(ctx, uAlice, app.ID), biz.ErrForbidden)
	if err := e.records.Delete(ctx, uAlice, r.ID); err != nil {
		t.Fatal(err)
	}
}

func TestRecordRule_Validation(t *testing.T) {
	t.Parallel()
	e := newP2EnvF(t, nil)
	ctx := context.Background()
	first, err := e.records.Create(ctx, uAlice, recordSpec(gHost, "job:req:rate5m"))
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		mutate func(*biz.RecordRuleSpec)
		field  string
		want   *kerrors.Error
	}{
		{"digit first", func(s *biz.RecordRuleSpec) { s.Name = "1abc" }, biz.RecordFieldName, biz.ErrRecordRuleInvalid},
		{"dash", func(s *biz.RecordRuleSpec) { s.Name = "a-b" }, biz.RecordFieldName, biz.ErrRecordRuleInvalid},
		{"empty name", func(s *biz.RecordRuleSpec) { s.Name = " " }, biz.RecordFieldName, biz.ErrRecordRuleInvalid},
		{"long name", func(s *biz.RecordRuleSpec) { s.Name = strings.Repeat("a", 129) }, biz.RecordFieldName, biz.ErrRecordRuleInvalid},
		{"long description", func(s *biz.RecordRuleSpec) { s.Description = strings.Repeat("d", 513) }, biz.RecordFieldDesc, biz.ErrRecordRuleInvalid},
		{"empty expr", func(s *biz.RecordRuleSpec) { s.Expr = "" }, biz.RecordFieldExpr, biz.ErrRecordRuleInvalid},
		{"bad expr", func(s *biz.RecordRuleSpec) { s.Expr = "sum(rate(" }, biz.RecordFieldExpr, biz.ErrRecordRuleInvalid},
		{"with template", func(s *biz.RecordRuleSpec) { s.Expr = "WITH (x = 1) x" }, biz.RecordFieldExpr, biz.ErrRecordRuleInvalid},
		{"long expr", func(s *biz.RecordRuleSpec) { s.Expr = "up + " + strings.Repeat("1", 8200) }, biz.RecordFieldExpr, biz.ErrRecordRuleInvalid},
		{"reserved label", func(s *biz.RecordRuleSpec) { s.Labels = map[string]string{"__name__": "x"} }, biz.RecordFieldLabels, biz.ErrRecordRuleInvalid},
		{"empty label value", func(s *biz.RecordRuleSpec) { s.Labels = map[string]string{"a": ""} }, biz.RecordFieldLabels, biz.ErrRecordRuleInvalid},
		{"too many labels", func(s *biz.RecordRuleSpec) {
			s.Labels = map[string]string{}
			for i := range 21 {
				s.Labels["l"+string(rune('a'+i))] = "v"
			}
		}, biz.RecordFieldLabels, biz.ErrRecordRuleInvalid},
		{"interval low", func(s *biz.RecordRuleSpec) { s.IntervalS = 14 }, biz.RecordFieldInterval, biz.ErrRecordRuleInvalid},
		{"interval high", func(s *biz.RecordRuleSpec) { s.IntervalS = 3601 }, biz.RecordFieldInterval, biz.ErrRecordRuleInvalid},
		{"missing datasource", func(s *biz.RecordRuleSpec) { s.DatasourceID = "nope" }, biz.RecordFieldDatasource, biz.ErrRecordRuleInvalid},
		{"disabled datasource", func(s *biz.RecordRuleSpec) { s.DatasourceID = "ds-down" }, biz.RecordFieldDatasource, biz.ErrRecordRuleInvalid},
		{"no write url", func(s *biz.RecordRuleSpec) { s.DatasourceID = "ds-ro" }, biz.RecordFieldDatasource, biz.ErrRecordRuleNoWriteURL},
		{"duplicate", func(s *biz.RecordRuleSpec) { s.Name = "job:req:rate5m" }, biz.RecordFieldName, biz.ErrRecordRuleNameExists},
	}
	for _, c := range cases {
		s := recordSpec(gHost, "ok:metric")
		c.mutate(&s)
		_, err := e.records.Create(ctx, uAlice, s)
		wantErr(t, "create "+c.name, err, c.want)
		issues, err := e.records.Validate(ctx, uBob, s, "")
		if err != nil || len(issues) != 1 || issues[0].Field != c.field || issues[0].Reason != c.want.Reason || issues[0].Message == "" {
			t.Fatalf("validate %s = %+v err=%v", c.name, issues, err)
		}
	}
	// 同名在别的分组 / 别的数据源下不算重复。
	if _, err := e.records.Create(ctx, uAlice, recordSpec(gDB, "job:req:rate5m")); err != nil {
		t.Fatalf("same name in another group: %v", err)
	}
	other := recordSpec(gHost, "job:req:rate5m")
	other.DatasourceID = "ds-w2"
	if _, err := e.records.Create(ctx, uAlice, other); err != nil {
		t.Fatalf("same name on another datasource: %v", err)
	}
	// 多个问题一并返回；通过时为空。
	multi := recordSpec(gHost, "9bad")
	multi.Expr, multi.IntervalS = "(", 1
	if issues, err := e.records.Validate(ctx, uBob, multi, ""); err != nil || len(issues) != 3 {
		t.Fatalf("multi issues = %+v err=%v", issues, err)
	}
	if issues, err := e.records.Validate(ctx, uDave, recordSpec(gHost, "fine:metric"), ""); err != nil || len(issues) != 0 {
		t.Fatalf("valid spec = %+v err=%v", issues, err)
	}
	// 校验接口的范围：所选分组不可读 403；带上自己的 ID 不算重名；范围外规则的 ID 不起作用。
	_, err = e.records.Validate(ctx, uCarol, recordSpec(gHost, "x:y"), "")
	wantErr(t, "validate out of scope", err, biz.ErrForbidden)
	if issues, err := e.records.Validate(ctx, uBob, recordSpec(gHost, "job:req:rate5m"), first.ID); err != nil || len(issues) != 0 {
		t.Fatalf("validate self = %+v err=%v", issues, err)
	}
	app, err := e.records.Create(ctx, uCarol, recordSpec(gApp, "job:req:rate5m"))
	if err != nil {
		t.Fatal(err)
	}
	if issues, err := e.records.Validate(ctx, uBob, recordSpec(gHost, "job:req:rate5m"), app.ID); err != nil || len(issues) != 1 {
		t.Fatalf("an unreadable rule id must not exempt the duplicate: %+v err=%v", issues, err)
	}
	if issues, err := e.records.Validate(ctx, uBob, recordSpec(gHost, "job:req:rate5m"), "missing-id"); err != nil || len(issues) != 1 {
		t.Fatalf("a missing rule id must be ignored: %+v err=%v", issues, err)
	}
	// 原本就用着的数据源被停用不阻止保存；换到停用的数据源不行。
	e.st.Targets["ds-w"] = biz.DatasourceWriteTarget{Name: "可写", HasWriteURL: true}
	edited := recordSpec(gHost, "job:req:rate5m")
	edited.Description = "改个说明"
	if v, err := e.records.Update(ctx, uAlice, first.ID, edited, 1); err != nil || v.Writable || v.Spec.Description != "改个说明" {
		t.Fatalf("update on a since-disabled datasource = %+v err=%v", v, err)
	}
	if issues, err := e.records.Validate(ctx, uBob, edited, first.ID); err != nil || len(issues) != 0 {
		t.Fatalf("validate kept datasource = %+v err=%v", issues, err)
	}
	e.st.Targets["ds-w2"] = biz.DatasourceWriteTarget{Name: "可写2", HasWriteURL: true}
	edited.DatasourceID, edited.Name = "ds-w2", "other:metric"
	_, err = e.records.Update(ctx, uAlice, first.ID, edited, 2)
	wantErr(t, "switch to a disabled datasource", err, biz.ErrRecordRuleInvalid)
	// 写入地址没了：即便是原来的数据源也不能保存。
	e.st.Targets["ds-w"] = biz.DatasourceWriteTarget{Name: "可写", Enabled: true}
	edited = recordSpec(gHost, "job:req:rate5m")
	_, err = e.records.Update(ctx, uAlice, first.ID, edited, 2)
	wantErr(t, "kept datasource lost its write url", err, biz.ErrRecordRuleNoWriteURL)
}

type writeURLs struct{ blocked string }

func (w writeURLs) ValidateWriteURL(raw string) error {
	if w.blocked != "" && strings.Contains(raw, w.blocked) {
		return biz.DatasourceAddressBlocked("link-local address is not allowed")
	}
	return nil
}

// Remote Write 地址与「沿用已保存凭据」的组合：凭据会随写入请求发往写入地址的主机。
func TestDatasource_RemoteWriteOrigin(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	ms := e.store
	uc := biz.ProvideDatasourceUsecase(ms.Datasources(), ms.Datasources(), ms.Datasources(), e.cipher, e.query, e.org, e.org, writeURLs{blocked: "169.254."})
	token := "tok-secret"
	const query, insert = "http://vmselect:8481/select/0/prometheus", "http://vminsert:8480/insert/0/prometheus/api/v1/write"
	in := func(write string, secret *string, headers map[string]string) biz.DatasourceInput {
		return biz.DatasourceInput{
			Name: "vm", Conn: biz.Connection{URL: query, AuthType: mc.AuthBearer, Headers: headers}, Secret: secret, RemoteWriteURL: write, Enabled: true,
		}
	}
	// 新建：凭据是当场填写的，写入地址可以在别的主机；被策略拒绝的地址保存即拒绝；格式照旧。
	_, err := uc.Create(ctx, uAdmin, in("http://169.254.169.254/write", &token, nil))
	wantErr(t, "blocked write address", err, biz.ErrDatasourceAddressBlocked)
	_, err = uc.Create(ctx, uAdmin, in("http://vminsert/write?token=x", &token, nil))
	wantErr(t, "query in write url", err, biz.ErrDatasourceInvalid)
	ds, err := uc.Create(ctx, uAdmin, in(insert, &token, map[string]string{"X-Scope": "a"}))
	if err != nil || ds.RemoteWriteURL != insert {
		t.Fatalf("create = %+v err=%v", ds, err)
	}
	version := ds.Version
	masked := map[string]string{"X-Scope": biz.HeaderMask}
	plain := map[string]string{"X-Scope": "a"}
	step := func(name string, input biz.DatasourceInput, want *kerrors.Error) {
		t.Helper()
		got, err := uc.Update(ctx, uAdmin, ds.ID, input, version)
		if want != nil {
			wantErr(t, name, err, want)
			return
		}
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		version = got.Version
	}
	step("keep secret, write origin unchanged (path differs)", in("http://vminsert:8480/other/path", nil, plain), nil)
	step("keep secret, write url same origin as query", in("http://vmselect:8481/api/v1/write", nil, plain), nil)
	// 此刻已保存的写入地址与查询地址同源。
	step("keep secret, write url moved to a third host", in("http://evil:9000/write", nil, plain), biz.ErrDatasourceSecretRequired)
	step("keep masked header only, write url moved", in("http://evil:9000/write", &token, masked), biz.ErrDatasourceSecretRequired)
	step("keep secret, write url scheme changed", in("https://vmselect:8481/api/v1/write", nil, plain), biz.ErrDatasourceSecretRequired)
	step("keep secret, write url port changed", in("http://vmselect:8482/api/v1/write", nil, plain), biz.ErrDatasourceSecretRequired)
	step("re-enter everything, write url moved", in("http://other:9000/write", &token, plain), nil)
	step("keep secret, clear write url", in("", nil, masked), nil)
	// 之前没有写入地址：沿用凭据时新增一个不同源的写入地址同样要重填。
	step("keep secret, add a cross-origin write url", in(insert, nil, plain), biz.ErrDatasourceSecretRequired)
	step("keep secret, add a same-origin write url", in("http://vmselect:8481/write", nil, masked), nil)
	// 无认证、没有掩码请求头：没有可外带的东西，任意写入地址都可以。
	none := biz.DatasourceInput{Name: "vm", Conn: biz.Connection{URL: query, AuthType: mc.AuthNone}, RemoteWriteURL: "http://anywhere:1/write", Enabled: true}
	step("auth none, any write url", none, nil)
	step("blocked address on update", in("http://169.254.1.1/write", &token, nil), biz.ErrDatasourceAddressBlocked)
}
