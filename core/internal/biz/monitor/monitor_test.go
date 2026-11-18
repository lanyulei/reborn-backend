package biz_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// 固定的测试对象。分组树：
//
//	root（内置）
//	infra ── host
//	      └─ db
//	app
const (
	gInfra, gHost, gDB, gApp = "g-infra", "g-host", "g-db", "g-app"

	uAdmin, uAlice, uBob, uCarol, uDave, uGone = "u-admin", "u-alice", "u-bob", "u-carol", "u-dave", "u-gone"

	rInfraManage, rInfraRead, rAppManage, rHostRead = "r-infra-manage", "r-infra-read", "r-app-manage", "r-host-read"

	dsVM, dsOff = "ds-vm", "ds-off"
	schedOps    = "sched-ops"
	deptOps     = "dept-ops"
)

// env 一套装配好的监控域用例（内存仓储 + 跨域桩）。
type env struct {
	store    *testfake.MonitorStore
	org      *testfake.MonitorOrg
	events   *testfake.Store
	query    *testfake.MonitorQueryClient
	cipher   *testfake.MonitorCipher
	scopes   *biz.ScopeUsecase
	groups   *biz.GroupUsecase
	sources  *biz.DatasourceUsecase
	rules    *biz.RuleUsecase
	alerts   *biz.AlertUsecase
	mutes    *biz.MuteUsecase
	subs     *biz.SubscriptionUsecase
	dispatch *biz.DispatchUsecase
	targets  *biz.TargetUsecase
	// logs 派发用例写出的日志（断言告警日志）。
	logs *bytes.Buffer
}

// syncWriter 并发安全的日志缓冲。
type syncWriter struct {
	mu  sync.Mutex
	buf *bytes.Buffer
}

func (w *syncWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

// newDispatch 以给定配置装配派发用例。
func (e *env) newDispatch(t *testing.T, c *conf.Monitor) *biz.DispatchUsecase {
	t.Helper()
	uc, err := biz.NewDispatchUsecase(testfake.NewTxManager(), e.store.Outbox(), e.store.Outbox(), e.store.Alerts(), e.store.Rules(),
		e.store.Subscriptions(), e.scopes, e.org, e.org, e.org, biznotify.NewNotificationUsecase(e.events, nil),
		slog.New(slog.NewTextHandler(&syncWriter{buf: e.logs}, nil)), c)
	if err != nil {
		t.Fatalf("NewDispatchUsecase() error = %v", err)
	}
	return uc
}

// newEnv alice 对 infra 可管理（含 host、db），bob 对 infra 只读，carol 对 app 可管理，
// dave 只对 host 只读；uGone 不是可用用户。
func newEnv(t *testing.T) *env {
	t.Helper()
	ms := testfake.NewMonitorStore()
	ms.SeedGroup(gInfra, "", "基础设施")
	ms.SeedGroup(gHost, gInfra, "主机")
	ms.SeedGroup(gDB, gInfra, "数据库")
	ms.SeedGroup(gApp, "", "应用")
	ms.SeedRoleScope(rInfraManage, gInfra, mc.ScopeManage)
	ms.SeedRoleScope(rInfraRead, gInfra, mc.ScopeRead)
	ms.SeedRoleScope(rAppManage, gApp, mc.ScopeManage)
	ms.SeedRoleScope(rHostRead, gHost, mc.ScopeRead)
	org := &testfake.MonitorOrg{
		Admins: []string{uAdmin},
		UserRoles: map[string][]string{
			uAlice: {rInfraManage}, uBob: {rInfraRead}, uCarol: {rAppManage}, uDave: {rHostRead},
		},
		Roles: []biz.RoleRef{
			{ID: rInfraManage, Name: "infra 管理", Enabled: true},
			{ID: rInfraRead, Name: "infra 只读", Enabled: true},
			{ID: rAppManage, Name: "app 管理", Enabled: true},
			{ID: rHostRead, Name: "host 只读", Enabled: false},
		},
		Users:    map[string]string{uAdmin: "管理员", uAlice: "Alice", uBob: "Bob", uCarol: "Carol", uDave: "Dave"},
		Disabled: map[string]string{uGone: "已离职"},
		// 缺省人人都有两项接口权限：数据范围相关的用例不受它影响；需要「没有权限」的测试自行改写。
		AlertReaders:      []string{uAlice, uBob, uCarol, uDave},
		DatasourceEditors: []string{uAlice, uBob, uCarol, uDave},
		Depts:             map[string]string{deptOps: "运维部"},
		Leaders:           map[string][]string{deptOps: {uCarol}},
		Schedules:         map[string]string{schedOps: "运维值班"}, OnDutyUsers: map[string][]string{schedOps: {uBob, uGone}},
	}
	events := testfake.NewStore()
	query := &testfake.MonitorQueryClient{}
	cipher := &testfake.MonitorCipher{Enabled: true}
	scopes := biz.NewScopeUsecase(org, ms.RoleScopes(), ms.Groups())
	e := &env{
		store: ms, org: org, events: events, query: query, cipher: cipher, scopes: scopes, logs: &bytes.Buffer{},
		groups:  biz.NewGroupUsecase(scopes, ms.Groups(), ms.Groups(), ms.RoleScopes(), org, org),
		sources: biz.NewDatasourceUsecase(ms.Datasources(), ms.Datasources(), ms.Datasources(), cipher, query, org, org),
		rules:   biz.NewRuleUsecase(scopes, ms.Rules(), ms.Rules(), ms.Rules(), ms.Versions(), ms.Evals(), ms.Datasources(), org, org),
		alerts:  biz.NewAlertUsecase(scopes, ms.Alerts(), ms.Alerts(), ms.Logs(), ms.Rules(), ms.Mutes(), org, org),
		mutes:   biz.NewMuteUsecase(scopes, ms.Mutes(), ms.Mutes(), ms.Alerts(), ms.Alerts(), ms.Rules(), ms.Rules(), ms.Datasources(), org, org),
		subs: biz.NewSubscriptionUsecase(scopes, ms.Subscriptions(), ms.Subscriptions(), ms.Logs(), ms.Alerts(), ms.Mutes(),
			ms.Rules(), ms.Rules(), ms.Datasources(), org, org),
		targets: biz.NewTargetUsecase(&testfake.MonitorTargets{
			ScheduleItems: []biz.TargetSchedule{{ID: schedOps, Name: "运维值班", GroupName: "运维组"}},
			DeptItems:     []biz.TargetDept{{ID: "dept-tech", Name: "技术部"}, {ID: deptOps, Name: "运维部", ParentID: "dept-tech"}},
			UserItems:     []biz.TargetUser{{ID: uAlice, Username: "alice", Nickname: "Alice"}, {ID: uBob, Username: "bob", Nickname: "Bob"}},
		}),
	}
	e.dispatch = e.newDispatch(t, nil)
	ctx := context.Background()
	for _, d := range []struct {
		id      string
		enabled bool
	}{{dsVM, true}, {dsOff, false}} {
		if err := ms.Datasources().Create(ctx, &biz.Datasource{ID: d.id, Name: d.id, URL: "http://" + d.id, AuthType: mc.AuthNone, TimeoutMS: 5000, Enabled: d.enabled}, nil, uAdmin); err != nil {
			t.Fatalf("seed datasource: %v", err)
		}
	}
	return e
}

func spec(groupID, name string) biz.RuleSpec {
	return biz.RuleSpec{
		GroupID: groupID, Name: name, Enabled: true, DatasourceScope: mc.DatasourceScopeAll, EvalIntervalS: 60, ForS: 60,
		LevelInhibit: true, NotifyRecovery: true, Queries: []mc.Query{{Expr: "up == 0", Severity: mc.SeverityCritical}},
	}
}

func (e *env) mustRule(t *testing.T, userID string, s biz.RuleSpec) *biz.RuleView {
	t.Helper()
	r, err := e.rules.Create(context.Background(), userID, s)
	if err != nil {
		t.Fatalf("create rule %s: %v", s.Name, err)
	}
	return r
}

func wantErr(t *testing.T, what string, err error, want *kerrors.Error) {
	t.Helper()
	if !kerrors.Is(err, want) {
		t.Fatalf("%s: err = %v, want %s", what, err, want.Reason)
	}
}

func TestScope_Resolve(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	cases := []struct {
		user     string
		admin    bool
		levels   map[string]int32
		readable []string
	}{
		// 超级管理员：全部分组可管理（含内置根分组）。
		{uAdmin, true, map[string]int32{gInfra: 2, gHost: 2, gDB: 2, gApp: 2, mc.RootGroupID: 2}, []string{mc.RootGroupID, gApp, gDB, gHost, gInfra}},
		// 授权上级即包含全部下级。
		{uAlice, false, map[string]int32{gInfra: 2, gHost: 2, gDB: 2, gApp: 0, mc.RootGroupID: 0}, []string{gDB, gHost, gInfra}},
		{uBob, false, map[string]int32{gInfra: 1, gHost: 1, gDB: 1, gApp: 0}, []string{gDB, gHost, gInfra}},
		{uCarol, false, map[string]int32{gApp: 2, gInfra: 0, gHost: 0}, []string{gApp}},
		// 只授权了下级：上级与兄弟分组都不可见。
		{uDave, false, map[string]int32{gHost: 1, gInfra: 0, gDB: 0}, []string{gHost}},
		// 没有任何角色 / 非可用用户：什么都看不到。
		{uGone, false, map[string]int32{gInfra: 0, gHost: 0, gApp: 0, mc.RootGroupID: 0}, []string{}},
	}
	for _, c := range cases {
		s, err := e.scopes.Resolve(ctx, c.user)
		if err != nil {
			t.Fatalf("Resolve(%s): %v", c.user, err)
		}
		if s.IsAdmin() != c.admin {
			t.Errorf("%s: IsAdmin = %v", c.user, s.IsAdmin())
		}
		for g, want := range c.levels {
			if got := s.Level(g); got != want {
				t.Errorf("%s: Level(%s) = %d, want %d", c.user, g, got, want)
			}
		}
		got := s.ReadableGroupIDs()
		want := slices.Clone(c.readable)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Errorf("%s: readable = %v, want %v", c.user, got, want)
		}
		if s.HasAny() != (len(c.readable) > 0) {
			t.Errorf("%s: HasAny = %v", c.user, s.HasAny())
		}
	}
}

func TestScope_UnionAndFilter(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	// 多个角色取并集，同一分组取最高级别。
	e.org.UserRoles[uBob] = []string{rInfraRead, rAppManage, rInfraManage}
	s, err := e.scopes.Resolve(ctx, uBob)
	if err != nil {
		t.Fatal(err)
	}
	if s.Level(gHost) != mc.ScopeManage || s.Level(gApp) != mc.ScopeManage {
		t.Fatalf("union of roles: host = %d app = %d", s.Level(gHost), s.Level(gApp))
	}
	// Filter：不指定分组 = 全部可读；指定分组 = 该分组及其下级中可读的部分；范围外 = 空。
	dave, _ := e.scopes.Resolve(ctx, uDave)
	if f := dave.Filter(""); f.All || !slices.Equal(f.IDs, []string{gHost}) {
		t.Fatalf("dave Filter(\"\") = %+v", f)
	}
	if f := dave.Filter(gInfra); f.All || !slices.Equal(f.IDs, []string{gHost}) {
		t.Fatalf("dave Filter(infra) must keep only the readable descendant: %+v", f)
	}
	if f := dave.Filter(gApp); !f.Empty() {
		t.Fatalf("dave Filter(app) must be empty: %+v", f)
	}
	if f := dave.Filter("no-such-group"); !f.Empty() {
		t.Fatalf("Filter(unknown) must be empty: %+v", f)
	}
	admin, _ := e.scopes.Resolve(ctx, uAdmin)
	if f := admin.Filter(""); !f.All {
		t.Fatalf("admin Filter(\"\") must be unrestricted: %+v", f)
	}
	if f := admin.Filter(gInfra); f.All || len(f.IDs) != 3 {
		t.Fatalf("admin Filter(infra) = %+v", f)
	}
	// 分组被删除后，其上的授权随之失效。
	if err := e.store.Groups().Delete(ctx, gApp, uAdmin); err != nil {
		t.Fatal(err)
	}
	carol, _ := e.scopes.Resolve(ctx, uCarol)
	if carol.HasAny() || carol.CanRead(gApp) {
		t.Fatalf("grant on a deleted group must not count: %v", carol.ReadableGroupIDs())
	}
	// ResolveMany 与逐个解析一致。
	many, err := e.scopes.ResolveMany(ctx, []string{uAlice, uDave, uAlice, uAdmin})
	if err != nil || len(many) != 3 || !many[uAlice].CanManage(gDB) || many[uDave].CanRead(gDB) || !many[uAdmin].IsAdmin() {
		t.Fatalf("ResolveMany = %v, %v", many, err)
	}
}

func TestGroup_Usecase(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	// 列表：范围外的上级以 level 0 返回并隐去说明与计数。
	views, isAdmin, err := e.groups.List(ctx, uDave)
	if err != nil || isAdmin || len(views) != 2 {
		t.Fatalf("dave groups = %d admin=%v err=%v", len(views), isAdmin, err)
	}
	if views[0].ID != gInfra || views[0].Level != 0 || views[0].ChildCount != 0 || views[1].ID != gHost || views[1].Level != 1 || views[1].Depth != 2 || views[1].FullName != "基础设施 / 主机" {
		t.Fatalf("dave tree = %+v / %+v", views[0], views[1])
	}
	if views, _, _ := e.groups.List(ctx, uGone); len(views) != 0 {
		t.Fatalf("user without grants must see nothing: %d", len(views))
	}
	// 新建：顶级仅超管；下级要求对上级可管理。
	_, err = e.groups.Create(ctx, uAlice, "", "top", "", 0)
	wantErr(t, "non-admin top group", err, biz.ErrForbidden)
	_, err = e.groups.Create(ctx, uBob, gInfra, "x", "", 0)
	wantErr(t, "read-only creates child", err, biz.ErrForbidden)
	_, err = e.groups.Create(ctx, uAlice, gApp, "x", "", 0)
	wantErr(t, "create child out of scope", err, biz.ErrForbidden)
	_, err = e.groups.Create(ctx, uAlice, gInfra, "主机", "", 0)
	wantErr(t, "duplicate name", err, biz.ErrGroupNameExists)
	_, err = e.groups.Create(ctx, uAlice, gInfra, "   ", "", 0)
	wantErr(t, "blank name", err, biz.ErrGroupInvalid)
	child, err := e.groups.Create(ctx, uAlice, gHost, "机房 A", "", 1)
	if err != nil || child.Level != mc.ScopeManage || child.Depth != 3 || e.store.CfgVersion(mc.CfgRuleGroup) != 1 {
		t.Fatalf("create child: %+v err=%v cfg=%d", child, err, e.store.CfgVersion(mc.CfgRuleGroup))
	}
	// 层级上限 5。
	parent := child.ID
	for depth := 4; depth <= biz.MaxGroupDepth; depth++ {
		g, err := e.groups.Create(ctx, uAdmin, parent, "lvl", "", 0)
		if err != nil {
			t.Fatalf("create depth %d: %v", depth, err)
		}
		parent = g.ID
	}
	_, err = e.groups.Create(ctx, uAdmin, parent, "too-deep", "", 0)
	wantErr(t, "depth limit", err, biz.ErrGroupInvalid)
	// 删除：有下级或规则时拒绝；顶级分组仅超管。
	wantErr(t, "delete with children", e.groups.Delete(ctx, uAlice, gHost), biz.ErrGroupInUse)
	e.mustRule(t, uAlice, spec(gDB, "db-down"))
	wantErr(t, "delete with rules", e.groups.Delete(ctx, uAlice, gDB), biz.ErrGroupInUse)
	wantErr(t, "non-admin deletes top group", e.groups.Delete(ctx, uCarol, gApp), biz.ErrForbidden)
	wantErr(t, "delete out of scope", e.groups.Delete(ctx, uCarol, gDB), biz.ErrForbidden)
	if err := e.groups.Delete(ctx, uAdmin, gApp); err != nil {
		t.Fatalf("admin deletes top group: %v", err)
	}
	// 角色授权：仅超管可写；角色须存在且不重复；列表区分直接授权与继承。
	_, err = e.groups.SetRoleScopes(ctx, uAlice, gHost, nil)
	wantErr(t, "non-admin sets scopes", err, biz.ErrForbidden)
	_, err = e.groups.SetRoleScopes(ctx, uAdmin, gHost, []*biz.RoleScope{{RoleID: "nope", Level: 1}})
	wantErr(t, "unknown role", err, biz.ErrRoleScopeInvalid)
	_, err = e.groups.SetRoleScopes(ctx, uAdmin, gHost, []*biz.RoleScope{{RoleID: rAppManage, Level: 1}, {RoleID: rAppManage, Level: 2}})
	wantErr(t, "duplicate role", err, biz.ErrRoleScopeInvalid)
	direct, err := e.groups.SetRoleScopes(ctx, uAdmin, gHost, []*biz.RoleScope{{RoleID: rAppManage, Level: 1}})
	if err != nil || len(direct) != 1 || direct[0].RoleName != "app 管理" || direct[0].UpdateUserName != "管理员" {
		t.Fatalf("set scopes = %+v, %v", direct, err)
	}
	// 整组替换：原先 host 上的只读授权被取消，dave 失去范围。
	if s, _ := e.scopes.Resolve(ctx, uDave); s.HasAny() {
		t.Fatalf("replaced grant must be gone: %v", s.ReadableGroupIDs())
	}
	if s, _ := e.scopes.Resolve(ctx, uCarol); !s.CanRead(gHost) || s.CanManage(gHost) {
		t.Fatalf("carol must now read host")
	}
	_, inherited, err := e.groups.ListRoleScopes(ctx, uAlice, gHost)
	if err != nil || len(inherited) != 2 || inherited[0].GroupID != gInfra || inherited[0].GroupName != "基础设施" {
		t.Fatalf("inherited = %+v, %v", inherited, err)
	}
	_, _, err = e.groups.ListRoleScopes(ctx, uBob, gHost)
	wantErr(t, "read-only lists scopes", err, biz.ErrForbidden)
}

func TestRule_ScopeAndLifecycle(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	// 新建：只读不够，范围外不行。
	_, err := e.rules.Create(ctx, uBob, spec(gHost, "x"))
	wantErr(t, "read-only creates", err, biz.ErrForbidden)
	_, err = e.rules.Create(ctx, uAlice, spec(gApp, "x"))
	wantErr(t, "create out of scope", err, biz.ErrForbidden)
	_, err = e.rules.Create(ctx, uAdmin, spec("no-such-group", "x"))
	wantErr(t, "unknown group", err, biz.ErrGroupNotFound)
	// 引用校验：数据源须存在且启用，通知对象须存在。
	bad := spec(gHost, "x")
	bad.DatasourceScope, bad.DatasourceIDs = mc.DatasourceScopeSpecified, []string{"ds-none"}
	_, err = e.rules.Create(ctx, uAlice, bad)
	wantErr(t, "unknown datasource", err, biz.ErrRuleInvalid)
	bad.DatasourceIDs = []string{dsOff}
	_, err = e.rules.Create(ctx, uAlice, bad)
	wantErr(t, "disabled datasource", err, biz.ErrRuleInvalid)
	for _, mutate := range []func(s *biz.RuleSpec){
		func(s *biz.RuleSpec) { s.Notify.UserIDs = []string{uGone} },
		func(s *biz.RuleSpec) { s.Notify.ScheduleIDs = []string{"sched-none"} },
		func(s *biz.RuleSpec) { s.Notify.DeptIDs = []string{"dept-none"} },
	} {
		s := spec(gHost, "x")
		mutate(&s)
		_, err = e.rules.Create(ctx, uAlice, s)
		wantErr(t, "unknown notify target", err, biz.ErrRuleInvalid)
	}

	full := spec(gHost, "cpu")
	full.DatasourceScope, full.DatasourceIDs = mc.DatasourceScopeSpecified, []string{dsVM}
	full.Notify = mc.NotifyTargets{UserIDs: []string{uAlice}, ScheduleIDs: []string{schedOps}, DeptIDs: []string{deptOps}}
	rule := e.mustRule(t, uAlice, full)
	if rule.Version != 1 || !rule.CanManage || rule.GroupName != "基础设施 / 主机" || rule.Datasources[0].Name != dsVM ||
		rule.NotifyUsers[0].Name != "Alice" || rule.NotifySchedules[0].Name != "运维值班" || rule.NotifyDepts[0].Name != "运维部" {
		t.Fatalf("created rule view = %+v", rule)
	}
	_, err = e.rules.Create(ctx, uAlice, spec(gHost, "cpu"))
	wantErr(t, "duplicate name", err, biz.ErrRuleNameExists)

	// 读：只读可见且 CanManage=false；范围外 403。
	if v, err := e.rules.Get(ctx, uBob, rule.ID); err != nil || v.CanManage {
		t.Fatalf("read-only get = %+v, %v", v, err)
	}
	_, err = e.rules.Get(ctx, uCarol, rule.ID)
	wantErr(t, "get out of scope", err, biz.ErrForbidden)
	// 评估状态汇总：告警中优先于查询错误；停用时恒为未评估。
	at := time.Now()
	e.store.SeedEvalTask(&biz.EvalTask{RuleID: rule.ID, DatasourceID: dsVM, LastStatus: mc.EvalError, LastError: "boom", LastEvalTime: &at})
	e.store.SeedEvalTask(&biz.EvalTask{RuleID: rule.ID, DatasourceID: dsOff, LastStatus: mc.EvalFiring, FiringCount: 3})
	if v, _ := e.rules.Get(ctx, uAlice, rule.ID); v.Status != mc.EvalFiring || v.FiringCount != 3 || v.LastError != "boom" || len(v.Tasks) != 2 || v.Tasks[0].DatasourceName == "" {
		t.Fatalf("status summary = %d/%d/%q tasks=%d", v.Status, v.FiringCount, v.LastError, len(v.Tasks))
	}

	// 更新：乐观锁；只改名不推进 spec_time；改查询推进。
	wantSpecTime := rule.SpecTime
	renamed := rule.Spec
	renamed.Name = "cpu-high"
	_, err = e.rules.Update(ctx, uAlice, rule.ID, renamed, 9, "")
	wantErr(t, "stale version", err, biz.ErrVersionConflict)
	_, err = e.rules.Update(ctx, uBob, rule.ID, renamed, 1, "")
	wantErr(t, "read-only updates", err, biz.ErrForbidden)
	v2, err := e.rules.Update(ctx, uAlice, rule.ID, renamed, 1, "改名")
	if err != nil || v2.Version != 2 || !v2.SpecTime.Equal(wantSpecTime) {
		t.Fatalf("rename: %+v err=%v", v2, err)
	}
	time.Sleep(2 * time.Millisecond)
	changed := v2.Spec
	changed.Queries = []mc.Query{{Expr: "up == 0", Severity: mc.SeverityFatal}}
	v3, err := e.rules.Update(ctx, uAlice, rule.ID, changed, 2, "")
	if err != nil || v3.Version != 3 || !v3.SpecTime.After(wantSpecTime) {
		t.Fatalf("query change must bump spec_time: %v -> %v err=%v", wantSpecTime, v3.SpecTime, err)
	}
	// 数据源被停用后，原本就在规则里的仍可保存；新加入停用的不行。
	if err := e.store.Datasources().Update(ctx, &biz.Datasource{ID: dsVM, Name: dsVM, URL: "http://x", AuthType: mc.AuthNone, TimeoutMS: 5000}, biz.SecretChange{}, 1, uAdmin); err != nil {
		t.Fatal(err)
	}
	if _, err := e.rules.Update(ctx, uAlice, rule.ID, v3.Spec, 3, ""); err != nil {
		t.Fatalf("rule keeping a now-disabled datasource must still save: %v", err)
	}
	// 移动分组：对目标分组要可管理。
	moved := v3.Spec
	moved.GroupID = gApp
	_, err = e.rules.Update(ctx, uAlice, rule.ID, moved, 4, "")
	wantErr(t, "move out of scope", err, biz.ErrForbidden)
	moved.GroupID = gDB
	v5, err := e.rules.Update(ctx, uAlice, rule.ID, moved, 4, "")
	if err != nil || v5.Spec.GroupID != gDB {
		t.Fatalf("move within scope: %v", err)
	}

	// 启停：已是目标状态不产生新版本。
	if v, err := e.rules.Toggle(ctx, uAlice, rule.ID, true); err != nil || v.Version != 5 {
		t.Fatalf("no-op toggle: version %d err=%v", v.Version, err)
	}
	if v, err := e.rules.Toggle(ctx, uAlice, rule.ID, false); err != nil || v.Version != 6 || v.Spec.Enabled || v.Status != mc.EvalNever {
		t.Fatalf("disable: %+v err=%v", v, err)
	}

	// 版本历史：每次变更一条，动作与摘要正确；只读可看。
	versions, total, err := e.rules.ListVersions(ctx, uBob, rule.ID, 1, 20)
	if err != nil || total != 6 {
		t.Fatalf("versions = %d, %v", total, err)
	}
	var actions []string
	for _, v := range versions {
		actions = append(actions, v.Action)
	}
	if got := strings.Join(actions, ","); got != "disable,move,update,update,update,create" {
		t.Fatalf("version actions = %s", got)
	}
	if versions[4].Summary != "改名" || versions[4].OperatorName != "Alice" || versions[0].Spec != nil {
		t.Fatalf("version meta = %+v", versions[4])
	}
	// 回滚到版本 1：生成新版本，内容回到当时（含分组），乐观锁生效。
	_, err = e.rules.Rollback(ctx, uAlice, rule.ID, 1, 5)
	wantErr(t, "rollback stale", err, biz.ErrVersionConflict)
	_, err = e.rules.Rollback(ctx, uAlice, rule.ID, 99, 6)
	wantErr(t, "rollback unknown version", err, biz.ErrRuleVersionNotFound)
	back, err := e.rules.Rollback(ctx, uAlice, rule.ID, 1, 6)
	if err != nil || back.Version != 7 || back.Spec.Name != "cpu" || back.Spec.GroupID != gHost || !back.Spec.Enabled || back.Spec.Queries[0].Severity != mc.SeverityCritical {
		t.Fatalf("rollback = %+v err=%v", back, err)
	}
	if v, err := e.rules.GetVersion(ctx, uAlice, rule.ID, 7); err != nil || v.Action != mc.RuleActionRollback || v.Spec.Name != "cpu" || v.GroupName != "基础设施 / 主机" {
		t.Fatalf("rollback version = %+v err=%v", v, err)
	}

	// 克隆：停用状态；默认名自动加序号；目标分组要可管理；只读用户可克隆到自己可管理的分组。
	c1, err := e.rules.Clone(ctx, uAlice, rule.ID, "", "")
	if err != nil || c1.Spec.Name != "cpu - 副本" || c1.Spec.Enabled {
		t.Fatalf("clone = %+v err=%v", c1, err)
	}
	c2, err := e.rules.Clone(ctx, uAlice, rule.ID, "", "")
	if err != nil || c2.Spec.Name != "cpu - 副本 (2)" {
		t.Fatalf("second clone name = %q err=%v", c2.Spec.Name, err)
	}
	_, err = e.rules.Clone(ctx, uAlice, rule.ID, "cpu", "")
	wantErr(t, "clone explicit duplicate name", err, biz.ErrRuleNameExists)
	_, err = e.rules.Clone(ctx, uBob, rule.ID, "", "")
	wantErr(t, "read-only clones into same group", err, biz.ErrForbidden)
	_, err = e.rules.Clone(ctx, uCarol, rule.ID, "", gApp)
	wantErr(t, "clone a rule out of scope", err, biz.ErrForbidden)

	// 删除：留一条 delete 版本；删除后历史仅超管可查。
	wantErr(t, "read-only deletes", e.rules.Delete(ctx, uBob, rule.ID), biz.ErrForbidden)
	if err := e.rules.Delete(ctx, uAlice, rule.ID); err != nil {
		t.Fatal(err)
	}
	_, _, err = e.rules.ListVersions(ctx, uAlice, rule.ID, 1, 20)
	wantErr(t, "history of deleted rule for non-admin", err, biz.ErrRuleNotFound)
	if versions, total, err := e.rules.ListVersions(ctx, uAdmin, rule.ID, 1, 20); err != nil || total != 8 || versions[0].Action != mc.RuleActionDelete {
		t.Fatalf("history of deleted rule for admin = %d, %v", total, err)
	}
}

func TestRule_ListScoped(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	e.mustRule(t, uAdmin, spec(gInfra, "infra-rule"))
	e.mustRule(t, uAdmin, spec(gHost, "host-rule"))
	e.mustRule(t, uAdmin, spec(gDB, "db-rule"))
	e.mustRule(t, uAdmin, spec(gApp, "app-rule"))
	count := func(user, group string) int64 {
		t.Helper()
		_, total, err := e.rules.List(ctx, user, biz.RuleListQuery{GroupID: group}, 1, 50)
		if err != nil {
			t.Fatalf("List(%s, %s): %v", user, group, err)
		}
		return total
	}
	for _, c := range []struct {
		user, group string
		want        int64
	}{
		{uAdmin, "", 4},
		{uAlice, "", 3},
		{uBob, "", 3},
		{uCarol, "", 1},
		{uDave, "", 1},
		{uGone, "", 0},
		{uAlice, gHost, 1},
		{uAlice, gInfra, 3},
		{uAlice, gApp, 0},
		{uDave, gInfra, 1},
		{uCarol, gInfra, 0},
	} {
		if got := count(c.user, c.group); got != c.want {
			t.Errorf("rules visible to %s in %q = %d, want %d", c.user, c.group, got, c.want)
		}
	}
}

func TestRule_Batch(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	mine := e.mustRule(t, uAlice, spec(gHost, "mine"))
	multi := spec(gHost, "multi")
	multi.Queries = append(multi.Queries, mc.Query{Expr: "up == 0", Severity: mc.SeverityWarning})
	two := e.mustRule(t, uAlice, multi)
	theirs := e.mustRule(t, uCarol, spec(gApp, "theirs"))

	sev := mc.SeverityFatal
	res, err := e.rules.Batch(ctx, uAlice, biz.BatchInput{IDs: []string{mine.ID, two.ID, theirs.ID, "missing"}, Action: biz.BatchSetSeverity, Severity: &sev})
	if err != nil || len(res) != 4 {
		t.Fatalf("batch = %v, %v", res, err)
	}
	if !res[0].OK || res[0].Name != "mine" {
		t.Errorf("own rule: %+v", res[0])
	}
	if res[1].OK || res[1].Reason != biz.ErrRuleInvalid.Reason {
		t.Errorf("multi-query rule must be rejected: %+v", res[1])
	}
	if res[2].OK || res[2].Reason != biz.ErrForbidden.Reason {
		t.Errorf("rule out of scope must be forbidden: %+v", res[2])
	}
	if res[3].OK || res[3].Reason != biz.ErrRuleNotFound.Reason || res[3].Name != "" {
		t.Errorf("missing rule: %+v", res[3])
	}
	if v, _ := e.rules.Get(ctx, uAlice, mine.ID); v.Spec.Queries[0].Severity != mc.SeverityFatal || v.Version != 2 {
		t.Fatalf("batch did not apply: %+v", v.Spec.Queries)
	}
	if v, _ := e.rules.Get(ctx, uCarol, theirs.ID); v.Version != 1 {
		t.Fatalf("rule out of scope was modified")
	}
	if vs, _, _ := e.rules.ListVersions(ctx, uAlice, mine.ID, 1, 10); vs[0].Action != mc.RuleActionBatch {
		t.Fatalf("batch must leave a batch version: %s", vs[0].Action)
	}

	// 入参层面的错误整体拒绝。
	_, err = e.rules.Batch(ctx, uAlice, biz.BatchInput{IDs: []string{mine.ID}, Action: biz.BatchMove})
	wantErr(t, "missing params", err, biz.ErrRuleBatchInvalid)
	_, err = e.rules.Batch(ctx, uAlice, biz.BatchInput{IDs: []string{mine.ID, mine.ID}, Action: biz.BatchEnable})
	wantErr(t, "duplicate ids", err, biz.ErrRuleBatchInvalid)
	ids := make([]string, biz.MaxBatchRules+1)
	for i := range ids {
		ids[i] = "id-" + strings.Repeat("x", i%7) + string(rune('a'+i%26)) + strings.Repeat("y", i/26)
	}
	_, err = e.rules.Batch(ctx, uAlice, biz.BatchInput{IDs: ids, Action: biz.BatchEnable})
	wantErr(t, "too many ids", err, biz.ErrRuleBatchInvalid)
	app := gApp
	_, err = e.rules.Batch(ctx, uAlice, biz.BatchInput{IDs: []string{mine.ID}, Action: biz.BatchMove, GroupID: &app})
	wantErr(t, "move into unmanaged group", err, biz.ErrForbidden)
	_, err = e.rules.Batch(ctx, uAlice, biz.BatchInput{IDs: []string{mine.ID}, Action: biz.BatchSetNotify, Notify: &mc.NotifyTargets{UserIDs: []string{uGone}}})
	wantErr(t, "unknown notify target", err, biz.ErrRuleInvalid)

	// 其余动作。
	db := gDB
	interval := int32(30)
	labels := map[string]string{}
	for i := range mc.MaxRuleLabels {
		labels["l"+string(rune('a'+i))] = "v"
	}
	steps := []struct {
		in    biz.BatchInput
		check func(s *biz.RuleSpec) bool
	}{
		{biz.BatchInput{Action: biz.BatchMove, GroupID: &db}, func(s *biz.RuleSpec) bool { return s.GroupID == gDB }},
		{biz.BatchInput{Action: biz.BatchSetTiming, EvalIntervalS: &interval}, func(s *biz.RuleSpec) bool { return s.EvalIntervalS == 30 && s.ForS == 60 }},
		{biz.BatchInput{Action: biz.BatchAddLabels, Labels: map[string]string{"team": "infra", "env": "prod"}}, func(s *biz.RuleSpec) bool { return s.Labels["team"] == "infra" }},
		{biz.BatchInput{Action: biz.BatchRemoveLabels, LabelNames: []string{"env", "absent"}}, func(s *biz.RuleSpec) bool { _, ok := s.Labels["env"]; return !ok && len(s.Labels) == 1 }},
		{biz.BatchInput{Action: biz.BatchSetNotify, Notify: &mc.NotifyTargets{UserIDs: []string{uBob}}, MaxNotify: &interval}, func(s *biz.RuleSpec) bool {
			return slices.Equal(s.Notify.UserIDs, []string{uBob}) && s.MaxNotify == 30 && s.NotifyRecovery
		}},
		{biz.BatchInput{Action: biz.BatchDisable}, func(s *biz.RuleSpec) bool { return !s.Enabled }},
	}
	for _, st := range steps {
		st.in.IDs = []string{mine.ID}
		res, err := e.rules.Batch(ctx, uAlice, st.in)
		if err != nil || !res[0].OK {
			t.Fatalf("batch %s: %+v err=%v", st.in.Action, res, err)
		}
		if v, _ := e.rules.Get(ctx, uAlice, mine.ID); !st.check(&v.Spec) {
			t.Fatalf("batch %s did not take effect: %+v", st.in.Action, v.Spec)
		}
	}
	// 合并后超过标签上限：该条失败。
	res, _ = e.rules.Batch(ctx, uAlice, biz.BatchInput{IDs: []string{mine.ID}, Action: biz.BatchAddLabels, Labels: labels})
	if res[0].OK || res[0].Reason != biz.ErrRuleInvalid.Reason {
		t.Fatalf("label limit after merge: %+v", res[0])
	}
	res, _ = e.rules.Batch(ctx, uAlice, biz.BatchInput{IDs: []string{mine.ID, two.ID}, Action: biz.BatchDelete})
	if !res[0].OK || !res[1].OK {
		t.Fatalf("batch delete: %+v", res)
	}
	_, err = e.rules.Get(ctx, uAlice, mine.ID)
	wantErr(t, "deleted", err, biz.ErrRuleNotFound)
}

const importYAML = `
groups:
- name: node
  interval: 1m
  rules:
  - alert: existing
    expr: up == 0
    labels: {severity: critical}
  - alert: fresh
    expr: up == 0
    for: 5m
    labels: {severity: warning, team: infra}
  - alert: fresh
    expr: up == 1
    labels: {severity: critical, team: infra}
  - record: r
    expr: sum(up)
  - alert: broken
    expr: "up =="
`

func TestRule_ImportExport(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	existing := e.mustRule(t, uAlice, spec(gHost, "existing"))
	opts := biz.ImportOptions{Content: importYAML, GroupID: gHost, DatasourceScope: mc.DatasourceScopeAll, DefaultSeverity: mc.SeverityInfo}

	// 范围与选项校验。
	_, err := e.rules.PreviewImport(ctx, uBob, opts)
	wantErr(t, "read-only previews", err, biz.ErrForbidden)
	_, err = e.rules.Import(ctx, uCarol, opts, biz.ConflictSkip, false)
	wantErr(t, "import out of scope", err, biz.ErrForbidden)
	badDS := opts
	badDS.DatasourceScope, badDS.DatasourceIDs = mc.DatasourceScopeSpecified, []string{dsOff}
	_, err = e.rules.PreviewImport(ctx, uAlice, badDS)
	wantErr(t, "disabled datasource", err, biz.ErrRuleInvalid)
	garbage := opts
	garbage.Content = "not: a rule file"
	_, err = e.rules.PreviewImport(ctx, uAlice, garbage)
	wantErr(t, "garbage content", err, biz.ErrRuleImportInvalid)

	// 预览：不落库，逐条给出结论。
	items, err := e.rules.PreviewImport(ctx, uAlice, opts)
	if err != nil || len(items) != 5 {
		t.Fatalf("preview = %d, %v", len(items), err)
	}
	statuses := []string{biz.ImportConflict, biz.ImportNew, biz.ImportConflict, biz.ImportSkipped, biz.ImportInvalid}
	for i, want := range statuses {
		if items[i].Status != want {
			t.Errorf("preview item %d (%s) = %s, want %s (%s)", i, items[i].Name, items[i].Status, want, items[i].Error)
		}
	}
	if items[0].ExistingRuleID != existing.ID || items[2].ExistingRuleID != "" || !strings.Contains(strings.Join(items[2].Warnings, "|"), "合并") {
		t.Fatalf("conflict details: %+v / %+v", items[0], items[2])
	}
	if _, total, _ := e.rules.List(ctx, uAlice, biz.RuleListQuery{}, 1, 50); total != 1 {
		t.Fatalf("preview must not write: %d rules", total)
	}

	// skip：重名跳过（含文件内重名的第二条）。
	results, err := e.rules.Import(ctx, uAlice, opts, biz.ConflictSkip, false)
	if err != nil {
		t.Fatal(err)
	}
	actions := func(rs []biz.ImportResult) string {
		out := make([]string, 0, len(rs))
		for _, r := range rs {
			out = append(out, r.Action)
		}
		return strings.Join(out, ",")
	}
	if got := actions(results); got != "skipped,created,skipped,skipped,failed" {
		t.Fatalf("skip policy actions = %s", got)
	}
	fresh, err := e.rules.Get(ctx, uAlice, results[1].RuleID)
	if err != nil || fresh.Spec.Enabled || fresh.Spec.ForS != 300 || fresh.Spec.Labels["team"] != "infra" || fresh.Spec.Queries[0].Severity != mc.SeverityWarning {
		t.Fatalf("imported rule = %+v err=%v", fresh, err)
	}
	if vs, _, _ := e.rules.ListVersions(ctx, uAlice, fresh.ID, 1, 5); vs[0].Action != mc.RuleActionImport {
		t.Fatalf("import version action = %s", vs[0].Action)
	}

	// overwrite：现有规则被覆盖并生成新版本；文件内重名的第二条改名导入而不是覆盖第一条。
	results, err = e.rules.Import(ctx, uAlice, opts, biz.ConflictOverwrite, true)
	if err != nil || actions(results) != "updated,updated,created,skipped,failed" {
		t.Fatalf("overwrite policy actions = %s err=%v", actions(results), err)
	}
	if results[0].RuleID != existing.ID || results[2].Name != "fresh (2)" {
		t.Fatalf("overwrite results = %+v", results)
	}
	if v, _ := e.rules.Get(ctx, uAlice, existing.ID); v.Version != 2 || !v.Spec.Enabled {
		t.Fatalf("overwritten rule = v%d enabled=%v", v.Version, v.Spec.Enabled)
	}
	if v, _ := e.rules.Get(ctx, uAlice, fresh.ID); v.Spec.Queries[0].Expr != "up == 0" {
		t.Fatalf("in-file duplicate must not overwrite the first: %s", v.Spec.Queries[0].Expr)
	}

	// rename：全部改名新建。
	results, err = e.rules.Import(ctx, uAlice, opts, biz.ConflictRename, false)
	if err != nil || actions(results) != "created,created,created,skipped,failed" || results[0].Name != "existing (2)" || results[1].Name != "fresh (3)" {
		t.Fatalf("rename policy = %+v err=%v", results, err)
	}

	// 导出：只导出范围内的；范围外的分组 403；按 ID 导出时范围外的规则被过滤。
	e.mustRule(t, uCarol, spec(gApp, "app-rule"))
	content, n, err := e.rules.Export(ctx, uBob, nil, gInfra, "")
	if err != nil || n != 6 || !strings.Contains(content, "alert: existing") || strings.Contains(content, "app-rule") {
		t.Fatalf("export group = %d err=%v\n%s", n, err, content)
	}
	if !strings.Contains(content, "severity: warning") || !strings.Contains(content, "for: 5m") || !strings.Contains(content, "基础设施 / 主机") {
		t.Fatalf("exported content lacks fields:\n%s", content)
	}
	_, _, err = e.rules.Export(ctx, uBob, nil, gApp, "")
	wantErr(t, "export group out of scope", err, biz.ErrForbidden)
	all, _, _ := e.rules.List(ctx, uAdmin, biz.RuleListQuery{}, 1, 50)
	ids := make([]string, 0, len(all))
	for _, r := range all {
		ids = append(ids, r.ID)
	}
	if _, n, err := e.rules.Export(ctx, uCarol, ids, "", ""); err != nil || n != 1 {
		t.Fatalf("export by ids must keep only readable rules: %d err=%v", n, err)
	}
	if _, n, err := e.rules.Export(ctx, uAlice, []string{fresh.ID}, gInfra, ""); err != nil || n != 1 {
		t.Fatalf("export ids ∩ group: %d err=%v", n, err)
	}
	_, _, err = e.rules.Export(ctx, uAlice, nil, "", "")
	wantErr(t, "export nothing", err, biz.ErrRuleInvalid)
}

func TestDatasource_Usecase(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	token := "tok-secret"
	in := func(name string, auth int32, secret *string) biz.DatasourceInput {
		return biz.DatasourceInput{Name: name, Conn: biz.Connection{URL: "http://vm:8428", AuthType: auth, Username: "u"}, Secret: secret, Enabled: true}
	}
	// 校验。
	_, err := e.sources.Create(ctx, uAdmin, in("  ", mc.AuthNone, nil))
	wantErr(t, "blank name", err, biz.ErrDatasourceInvalid)
	_, err = e.sources.Create(ctx, uAdmin, in("x", mc.AuthBearer, nil))
	wantErr(t, "bearer without token", err, biz.ErrDatasourceInvalid)
	bad := in("x", mc.AuthNone, nil)
	bad.Conn.URL = "ftp://vm"
	_, err = e.sources.Create(ctx, uAdmin, bad)
	wantErr(t, "bad url", err, biz.ErrDatasourceInvalid)
	bad = in("x", mc.AuthNone, nil)
	bad.Conn.TimeoutMS = 60000
	_, err = e.sources.Create(ctx, uAdmin, bad)
	wantErr(t, "timeout out of range", err, biz.ErrDatasourceInvalid)

	// 远程写入地址只是记录，但不能夹带凭据。
	for _, raw := range []string{"https://user:pass@vm:8480/insert", "ftp://vm", "http://vm/write?token=x", "http:///nohost"} {
		bad = in("x", mc.AuthNone, nil)
		bad.RemoteWriteURL = raw
		_, err = e.sources.Create(ctx, uAdmin, bad)
		wantErr(t, "remote write url "+raw, err, biz.ErrDatasourceInvalid)
	}

	// 主密钥未配置：只有带凭据的写入失败。
	e.cipher.Enabled = false
	_, err = e.sources.Create(ctx, uAdmin, in("sealed", mc.AuthBearer, &token))
	wantErr(t, "seal without master key", err, biz.ErrSecretUnavailable)
	plain, err := e.sources.Create(ctx, uAdmin, in("plain", mc.AuthNone, &token))
	if err != nil || plain.HasSecret || plain.Username != "" || plain.TimeoutMS != 10000 {
		t.Fatalf("auth=none must drop credentials and still save without a master key: %+v err=%v", plain, err)
	}
	e.cipher.Enabled = true

	withRemote := in("vm", mc.AuthBearer, &token)
	withRemote.RemoteWriteURL = "http://vminsert:8480/insert/0/prometheus"
	ds, err := e.sources.Create(ctx, uAdmin, withRemote)
	if err != nil || !ds.HasSecret || ds.UpdateUserName != "管理员" || ds.Version != 1 {
		t.Fatalf("create = %+v err=%v", ds, err)
	}
	_, err = e.sources.Create(ctx, uAdmin, in("vm", mc.AuthNone, nil))
	wantErr(t, "duplicate name", err, biz.ErrDatasourceNameExists)

	// 更新：不传凭据沿用；改为无认证则清除。
	upd, err := e.sources.Update(ctx, uAdmin, ds.ID, in("vm", mc.AuthBearer, nil), 1)
	if err != nil || !upd.HasSecret || upd.Version != 2 {
		t.Fatalf("update keeping secret = %+v err=%v", upd, err)
	}
	_, err = e.sources.Update(ctx, uAdmin, ds.ID, in("vm", mc.AuthBearer, nil), 1)
	wantErr(t, "stale version", err, biz.ErrVersionConflict)
	_, err = e.sources.Update(ctx, uAdmin, "nope", in("vm", mc.AuthNone, nil), 1)
	wantErr(t, "unknown id", err, biz.ErrDatasourceNotFound)

	// 测试连接：已保存的用已保存的凭据；未保存的配置给了 id 时沿用其凭据，给了新凭据则用新的。
	if res, err := e.sources.Test(ctx, ds.ID); err != nil || !res.OK {
		t.Fatalf("test saved = %+v err=%v", res, err)
	}
	other := "other-token"
	if _, err := e.sources.TestConfig(ctx, uAdmin, biz.Connection{URL: "http://vm:8428/select/0", AuthType: mc.AuthBearer}, nil, ds.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := e.sources.TestConfig(ctx, uAdmin, biz.Connection{URL: "http://new:8428", AuthType: mc.AuthBearer}, &other, ds.ID); err != nil {
		t.Fatal(err)
	}
	conns := e.query.Conns()
	if len(conns) != 3 || conns[0].Secret != token || conns[1].Secret != token || conns[1].URL != "http://vm:8428/select/0" || conns[2].Secret != other {
		t.Fatalf("secrets used for test connection are wrong (%d calls)", len(conns))
	}
	// 失败不作为接口错误返回，错误原文进结果。
	e.query.Err = biz.QueryFailed(false, "timeout: context deadline exceeded")
	if res, err := e.sources.Test(ctx, ds.ID); err != nil || res.OK || !strings.Contains(res.Error, "timeout") {
		t.Fatalf("failed probe = %+v err=%v", res, err)
	}
	// 查询：错误透传；停用的数据源不能查；区间步数有上限。
	_, err = e.sources.Query(ctx, ds.ID, "up", time.Time{})
	wantErr(t, "query failure", err, biz.ErrDatasourceUnreachable)
	e.query.Err = biz.QueryFailed(true, "bad_data: parse error")
	_, err = e.sources.Query(ctx, ds.ID, "up ==", time.Time{})
	wantErr(t, "bad query", err, biz.ErrQueryInvalid)
	e.query.Err = nil
	_, err = e.sources.Query(ctx, dsOff, "up", time.Time{})
	wantErr(t, "disabled datasource", err, biz.ErrDatasourceDisabled)
	now := time.Now()
	_, err = e.sources.QueryRange(ctx, ds.ID, "up", now.Add(-30*24*time.Hour), now, time.Second)
	wantErr(t, "too many steps", err, biz.ErrQueryInvalid)
	_, err = e.sources.QueryRange(ctx, ds.ID, "up", now, now.Add(-time.Hour), time.Minute)
	wantErr(t, "inverted range", err, biz.ErrQueryInvalid)
	// 点数上限：超出的序列被丢弃并标记截断。
	big := make([]biz.QueryPoint, 150000)
	e.query.Result = &biz.QueryResult{Type: "matrix", Series: []biz.QuerySeries{{Points: big}, {Points: big}, {Points: big}}}
	res, err := e.sources.QueryRange(ctx, ds.ID, "up", now.Add(-time.Hour), now, time.Minute)
	if err != nil || len(res.Series) != 1 || !res.Truncated {
		t.Fatalf("point cap: series=%d truncated=%v err=%v", len(res.Series), res.Truncated, err)
	}
	e.query.Result = nil

	// 改为无认证：清除凭据。
	cleared, err := e.sources.Update(ctx, uAdmin, ds.ID, in("vm", mc.AuthNone, nil), 2)
	if err != nil || cleared.HasSecret {
		t.Fatalf("auth=none must clear the secret: %+v err=%v", cleared, err)
	}
	// 缺省数据源唯一。
	def := in("default-1", mc.AuthNone, nil)
	def.IsDefault = true
	first, err := e.sources.Create(ctx, uAdmin, def)
	if err != nil {
		t.Fatal(err)
	}
	def.Name = "default-2"
	if _, err := e.sources.Create(ctx, uAdmin, def); err != nil {
		t.Fatal(err)
	}
	opts, _ := e.sources.ListOptions(ctx, false)
	defaults := 0
	for _, o := range opts {
		if o.IsDefault {
			defaults++
		}
	}
	if got, _ := e.sources.Get(ctx, first.ID); defaults != 1 || got.IsDefault || got.Version != 2 {
		t.Fatalf("defaults = %d, first = %+v", defaults, got)
	}
	// 删除：被规则指定时拒绝并列出规则名。
	s := spec(gHost, "uses-vm")
	s.DatasourceScope, s.DatasourceIDs = mc.DatasourceScopeSpecified, []string{ds.ID}
	rule := e.mustRule(t, uAlice, s)
	err = e.sources.Delete(ctx, uAdmin, ds.ID)
	wantErr(t, "delete referenced", err, biz.ErrDatasourceInUse)
	if !strings.Contains(err.Error(), "uses-vm") {
		t.Fatalf("in-use error must name the rule: %v", err)
	}
	if err := e.rules.Delete(ctx, uAlice, rule.ID); err != nil {
		t.Fatal(err)
	}
	if err := e.sources.Delete(ctx, uAdmin, ds.ID); err != nil {
		t.Fatalf("delete after the rule is gone: %v", err)
	}
}

func TestDatasource_ConcurrencyLimit(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	block := make(chan struct{})
	e.query.Block = block
	const limit = 8
	done := make(chan error, limit)
	for range limit {
		go func() {
			_, err := e.sources.Query(ctx, dsVM, "up", time.Time{})
			done <- err
		}()
	}
	// 等到额度占满。
	deadline := time.Now().Add(5 * time.Second)
	for len(e.query.Conns()) < limit {
		if time.Now().After(deadline) {
			t.Fatalf("queries did not start: %d", len(e.query.Conns()))
		}
		time.Sleep(time.Millisecond)
	}
	_, err := e.sources.Query(ctx, dsVM, "up", time.Time{})
	wantErr(t, "over the concurrency limit", err, biz.ErrQueryBusy)
	if _, err := e.sources.Test(ctx, dsVM); !kerrors.Is(err, biz.ErrQueryBusy) {
		t.Fatalf("test connection shares the limit: %v", err)
	}
	close(block)
	for range limit {
		if err := <-done; err != nil {
			t.Fatalf("in-flight query failed: %v", err)
		}
	}
	if _, err := e.sources.Query(ctx, dsVM, "up", time.Time{}); err != nil {
		t.Fatalf("slots must be released: %v", err)
	}
}

var errBoom = errors.New("boom")
