package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	topbiz "github.com/lanyulei/reborn/backend/core/internal/biz"
	bizmonitor "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	monitorservice "github.com/lanyulei/reborn/backend/core/internal/service/monitor"
	service "github.com/lanyulei/reborn/backend/core/internal/service/system"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// monitorServices 监控告警域的 e2e 装配产物：六个 service 与可预置运行数据的内存仓储。
type monitorServices struct {
	datasource   *monitorservice.DatasourceService
	group        *monitorservice.RuleGroupService
	rule         *monitorservice.RuleService
	alert        *monitorservice.AlertService
	mute         *monitorservice.MuteService
	subscription *monitorservice.SubscriptionService
	// p2 第二、三期新增的六个服务（装配见 monitor_p2_e2e_test.go）。
	p2       monitorservice.P2Services
	store    *testfake.MonitorStore
	query    *testfake.MonitorQueryClient
	dispatch *bizmonitor.DispatchUsecase
}

// monitorUsers 以 system 域的内存用户仓储回答监控域的组织目录查询（e2e 的用户经接口动态创建）。
type monitorUsers struct {
	store *testfake.Store
}

func (m monitorUsers) ActiveUserIDs(ctx context.Context, ids []string) ([]string, error) {
	names, err := m.UserNames(ctx, ids)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if _, ok := names[id]; ok && !slices.Contains(out, id) {
			out = append(out, id)
		}
	}
	return out, nil
}

func (m monitorUsers) UserNames(ctx context.Context, ids []string) (map[string]string, error) {
	out := make(map[string]string, len(ids))
	for _, id := range ids {
		if u, err := m.store.GetUser(ctx, id); err == nil && u.Status == biz.StatusEnabled {
			out[id] = u.Nickname
		}
	}
	return out, nil
}

func (monitorUsers) DeptNames(context.Context, []string) (map[string]string, error) {
	return map[string]string{}, nil
}

// newMonitorServices 组装监控域：内存仓储 + system 域真实的角色解析（数据范围随角色绑定即时变化）。
func newMonitorServices(store *testfake.Store, authz *biz.AuthzUsecase, roles *biz.RoleUsecase) monitorServices {
	ms := testfake.NewMonitorStore()
	org := &testfake.MonitorOrg{}
	users := monitorUsers{store: store}
	query := &testfake.MonitorQueryClient{}
	// 接口权限判定用组合根里真实的适配器：与授权中间件同一口径。
	perms := topbiz.NewMonitorPermissions(authz)
	targets := &testfake.MonitorTargets{
		ScheduleItems: []bizmonitor.TargetSchedule{{ID: "018f3e1a-0e00-7000-8000-000000000001", Name: "运维值班", GroupName: "运维组"}},
		DeptItems: []bizmonitor.TargetDept{
			{ID: "018f3e1a-0e01-7000-8000-000000000001", Name: "技术部"},
			{ID: "018f3e1a-0e01-7000-8000-000000000002", Name: "运维部", ParentID: "018f3e1a-0e01-7000-8000-000000000001"},
		},
		UserItems: []bizmonitor.TargetUser{
			{ID: "018f3e1a-0e02-7000-8000-000000000001", Username: "alice", Nickname: "Alice"},
			{ID: "018f3e1a-0e02-7000-8000-000000000002", Username: "bob"},
		},
	}
	scopes := bizmonitor.NewScopeUsecase(authz, ms.RoleScopes(), ms.Groups())
	// 规则 / 告警 / 数据源 / 派发经第二、三期的装配（monitor_p2_e2e_test.go）接上新的协作者。
	p2 := newMonitorP2(monitorP2Input{
		ms: ms, store: store, scopes: scopes, users: users, org: org, perms: perms, authz: authz, query: query,
		targets: bizmonitor.NewTargetUsecase(targets),
	})
	return monitorServices{
		datasource: p2.datasource,
		group:      monitorservice.NewRuleGroupService(bizmonitor.NewGroupUsecase(scopes, ms.Groups(), ms.Groups(), ms.RoleScopes(), topbiz.NewMonitorRoles(roles), users)),
		rule:       p2.rule,
		alert:      p2.alert,
		mute: monitorservice.NewMuteService(bizmonitor.NewMuteUsecase(
			scopes, ms.Mutes(), ms.Mutes(), ms.Alerts(), ms.Alerts(), ms.Rules(), ms.Rules(), ms.Datasources(), users, org)),
		subscription: monitorservice.NewSubscriptionService(bizmonitor.NewSubscriptionUsecase(
			scopes, ms.Subscriptions(), ms.Subscriptions(), ms.Logs(), ms.Alerts(), ms.Mutes(), ms.Rules(), ms.Rules(), ms.Datasources(), users, org)),
		p2: p2.p2, store: ms, query: query, dispatch: p2.dispatch,
	}
}

// monitorOps 监控域全部可授权接口（proto full method）。
func monitorOps() []string {
	var ops []string
	for _, m := range service.AuthorizableMethods() {
		if strings.HasPrefix(m.Name, "/monitor.v1.") {
			ops = append(ops, m.Name)
		}
	}
	return ops
}

// monitorUser 新建用户并绑定一个持有给定接口权限的角色，返回令牌与角色 ID。
func monitorUser(t *testing.T, ts *httptest.Server, svc testServices, admin, name string, ops []string) (bearer, userID, roleID string) {
	t.Helper()
	user := createUser(t, ts, admin, name)
	role := createRole(t, ts, admin, "监控-"+name, "monitor_"+name)
	codes := make([]string, 0, len(ops))
	for _, op := range ops {
		codes = append(codes, `"`+biz.OperationCode(op)+`"`)
	}
	if code, body := doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/system/role/"+role.Id+"/permission", admin,
		`{"apis":[`+strings.Join(codes, ",")+`]}`); code != http.StatusOK {
		t.Fatalf("set role permissions: %d %s", code, body)
	}
	if code, body := doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/system/user/"+user.Id+"/role", admin,
		`{"role_ids":["`+role.Id+`"]}`); code != http.StatusOK {
		t.Fatalf("bind role: %d %s", code, body)
	}
	return testBearer(t, svc.signer, user.Id), user.Id, role.Id
}

// monitorID 发起请求并取出响应里 <key>.id；状态码不符直接失败。
func monitorID(t *testing.T, ts *httptest.Server, method, path, bearer, body, key string) string {
	t.Helper()
	code, resp := doRequestWithToken(t, method, ts.URL+path, bearer, body)
	if code != http.StatusOK {
		t.Fatalf("%s %s: %d %s", method, path, code, resp)
	}
	var out map[string]struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal([]byte(resp), &out); err != nil || out[key].ID == "" {
		t.Fatalf("%s %s: no %s.id in %s", method, path, key, resp)
	}
	return out[key].ID
}

func monitorRuleBody(groupID, name string) string {
	return `{"spec":{"groupId":"` + groupID + `","name":"` + name + `","enabled":true,"datasourceScope":2,` +
		`"queries":[{"expr":"up == 0","severity":3}],"levelInhibit":true,"evalIntervalS":60,"forS":60,"notifyRecovery":true}}`
}

// expectStatus 断言状态码与响应里包含的片段。
func expectStatus(t *testing.T, what string, ts *httptest.Server, method, path, bearer, body string, want int, contains ...string) string {
	t.Helper()
	code, resp := doRequestWithToken(t, method, ts.URL+path, bearer, body)
	if code != want {
		t.Fatalf("%s: status = %d, want %d; body = %s", what, code, want, resp)
	}
	for _, c := range contains {
		if !strings.Contains(resp, c) {
			t.Fatalf("%s: body lacks %q: %s", what, c, resp)
		}
	}
	return resp
}

// 认证、接口授权与数据范围：两个非管理员用户分属两个分组，互相看不到、改不了对方的规则、
// 告警事件与屏蔽规则；没有接口权限的用户被授权中间件拦下；没有分组授权的用户什么都看不到。
func TestMonitor_ScopeIsolation(t *testing.T) {
	t.Parallel()
	ts, svc := newTestHTTPServerWithServices(t)
	admin := testBearer(t, svc.signer, e2eActorID)
	const base = "/api/v1/monitor"

	// 未登录 401；登录但没有任何监控接口权限 403（授权中间件，不是数据范围）。
	expectStatus(t, "anonymous", ts, http.MethodGet, base+"/rule?page=1&pageSize=10", "", "", http.StatusUnauthorized)
	plain := createUser(t, ts, admin, "mon_plain")
	expectStatus(t, "no api permission", ts, http.MethodGet, base+"/rule?page=1&pageSize=10", testBearer(t, svc.signer, plain.Id), "", http.StatusForbidden)

	groupA := monitorID(t, ts, http.MethodPost, base+"/rule-group", admin, `{"name":"team-a"}`, "group")
	groupB := monitorID(t, ts, http.MethodPost, base+"/rule-group", admin, `{"name":"team-b"}`, "group")
	ops := monitorOps()
	if len(ops) < 40 {
		t.Fatalf("monitor operations not registered: %d", len(ops))
	}
	alice, aliceID, roleA := monitorUser(t, ts, svc, admin, "mon_alice", ops)
	bob, bobID, roleB := monitorUser(t, ts, svc, admin, "mon_bob", ops)
	nobody, _, _ := monitorUser(t, ts, svc, admin, "mon_nobody", ops)

	// 分组授权仅超级管理员；顶级分组仅超级管理员。
	scopeBody := func(groupID, roleID string) string {
		return `{"groupId":"` + groupID + `","items":[{"roleId":"` + roleID + `","level":2}]}`
	}
	expectStatus(t, "non-admin sets role scope", ts, http.MethodPut, base+"/role-scope", alice, scopeBody(groupB, roleA), http.StatusForbidden, "MONITOR_FORBIDDEN")
	expectStatus(t, "non-admin creates top group", ts, http.MethodPost, base+"/rule-group", alice, `{"name":"mine"}`, http.StatusForbidden, "MONITOR_FORBIDDEN")
	expectStatus(t, "grant A", ts, http.MethodPut, base+"/role-scope", admin, scopeBody(groupA, roleA), http.StatusOK, `"level":2`)
	expectStatus(t, "grant B", ts, http.MethodPut, base+"/role-scope", admin, scopeBody(groupB, roleB), http.StatusOK)
	expectStatus(t, "unknown role", ts, http.MethodPut, base+"/role-scope", admin, scopeBody(groupB, groupA), http.StatusBadRequest, "MONITOR_ROLE_SCOPE_INVALID")

	// 分组树：各自只看到自己的分组；没有授权的用户看不到任何分组。
	resp := expectStatus(t, "alice groups", ts, http.MethodGet, base+"/rule-group", alice, "", http.StatusOK, "team-a", `"level":2`)
	if strings.Contains(resp, "team-b") {
		t.Fatalf("alice must not see team-b: %s", resp)
	}
	if resp := expectStatus(t, "nobody groups", ts, http.MethodGet, base+"/rule-group", nobody, "", http.StatusOK); strings.Contains(resp, "team-") {
		t.Fatalf("user without grants must see no group: %s", resp)
	}
	// 可管理的用户可在自己的分组下建子分组，不能在别人的分组下建。
	subA := monitorID(t, ts, http.MethodPost, base+"/rule-group", alice, `{"parentId":"`+groupA+`","name":"db"}`, "group")
	expectStatus(t, "alice creates child under B", ts, http.MethodPost, base+"/rule-group", alice, `{"parentId":"`+groupB+`","name":"x"}`, http.StatusForbidden)

	// 规则：只能建在可管理的分组里（授权分组的下级同样可管理）。
	ruleA := monitorID(t, ts, http.MethodPost, base+"/rule", alice, monitorRuleBody(groupA, "cpu-high"), "rule")
	monitorID(t, ts, http.MethodPost, base+"/rule", alice, monitorRuleBody(subA, "db-down"), "rule")
	ruleB := monitorID(t, ts, http.MethodPost, base+"/rule", bob, monitorRuleBody(groupB, "disk-full"), "rule")
	expectStatus(t, "alice creates rule in B", ts, http.MethodPost, base+"/rule", alice, monitorRuleBody(groupB, "x"), http.StatusForbidden, "MONITOR_FORBIDDEN")
	expectStatus(t, "invalid promql", ts, http.MethodPost, base+"/rule", alice,
		strings.Replace(monitorRuleBody(groupA, "bad"), "up == 0", "up ==", 1), http.StatusBadRequest, "MONITOR_RULE_INVALID")
	expectStatus(t, "duplicate name", ts, http.MethodPost, base+"/rule", alice, monitorRuleBody(groupA, "cpu-high"), http.StatusConflict, "MONITOR_RULE_NAME_EXISTS")

	// 列表按范围过滤；指定范围外的分组得到空列表而不是报错。
	resp = expectStatus(t, "alice rules", ts, http.MethodGet, base+"/rule?page=1&pageSize=10", alice, "", http.StatusOK, `"total":"2"`, "cpu-high", "db-down")
	if strings.Contains(resp, "disk-full") {
		t.Fatalf("alice must not see team-b rules: %s", resp)
	}
	expectStatus(t, "bob rules", ts, http.MethodGet, base+"/rule?page=1&pageSize=10", bob, "", http.StatusOK, `"total":"1"`, "disk-full")
	expectStatus(t, "bob filters by group A", ts, http.MethodGet, base+"/rule?page=1&pageSize=10&groupId="+groupA, bob, "", http.StatusOK, `"total":"0"`)
	expectStatus(t, "nobody rules", ts, http.MethodGet, base+"/rule?page=1&pageSize=10", nobody, "", http.StatusOK, `"total":"0"`)
	expectStatus(t, "admin rules", ts, http.MethodGet, base+"/rule?page=1&pageSize=10", admin, "", http.StatusOK, `"total":"3"`)

	// 单条规则：范围外一律 403，且不能改、不能启停、不能克隆、不能回滚、不能删，也看不到历史与判定记录。
	for _, c := range []struct{ what, method, path, body string }{
		{"get", http.MethodGet, "/rule/" + ruleA, ""},
		{"update", http.MethodPut, "/rule/" + ruleA, strings.Replace(monitorRuleBody(groupA, "cpu-high"), `{"spec"`, `{"version":1,"spec"`, 1)},
		{"toggle", http.MethodPost, "/rule/" + ruleA + ":toggle", `{"enabled":false}`},
		{"clone", http.MethodPost, "/rule/" + ruleA + ":clone", `{}`},
		{"rollback", http.MethodPost, "/rule/" + ruleA + ":rollback", `{"version":1,"currentVersion":1}`},
		{"versions", http.MethodGet, "/rule/" + ruleA + "/version?page=1&pageSize=10", ""},
		{"version", http.MethodGet, "/rule/" + ruleA + "/version/1", ""},
		{"eval records", http.MethodGet, "/rule/" + ruleA + "/eval-record?page=1&pageSize=10", ""},
		{"delete", http.MethodDelete, "/rule/" + ruleA, ""},
	} {
		expectStatus(t, "bob "+c.what+" rule A", ts, c.method, base+c.path, bob, c.body, http.StatusForbidden, "MONITOR_FORBIDDEN")
	}
	// 把自己的规则移到别人的分组：对目标分组没有管理权。
	expectStatus(t, "bob moves rule into A", ts, http.MethodPut, base+"/rule/"+ruleB, bob,
		strings.Replace(monitorRuleBody(groupA, "disk-full"), `{"spec"`, `{"version":1,"spec"`, 1), http.StatusForbidden)
	// 批量：逐条判定，范围外的那条失败、自己的那条成功。
	expectStatus(t, "bob batch", ts, http.MethodPost, base+"/rule:batch", bob, `{"ids":["`+ruleA+`","`+ruleB+`"],"action":"disable"}`,
		http.StatusOK, `"succeeded":1`, `"failed":1`, "MONITOR_FORBIDDEN")
	expectStatus(t, "rule A untouched", ts, http.MethodGet, base+"/rule/"+ruleA, alice, "", http.StatusOK, `"enabled":true`, `"version":1`)
	expectStatus(t, "batch params mismatch", ts, http.MethodPost, base+"/rule:batch", bob, `{"ids":["`+ruleB+`"],"action":"move"}`,
		http.StatusBadRequest, "MONITOR_RULE_BATCH_INVALID")
	// 导出 / 导入同样受范围约束。
	expectStatus(t, "bob exports group A", ts, http.MethodPost, base+"/rule:export", bob, `{"groupId":"`+groupA+`"}`, http.StatusForbidden)
	expectStatus(t, "bob exports rule A by id", ts, http.MethodPost, base+"/rule:export", bob, `{"ids":["`+ruleA+`"]}`, http.StatusOK, `"count":0`)
	importBody := `{"options":{"content":"groups:\n- name: g\n  rules:\n  - alert: imported\n    expr: up == 0\n","groupId":"` + groupA + `","datasourceScope":2,"defaultSeverity":2},"onConflict":"skip"}`
	expectStatus(t, "bob imports into A", ts, http.MethodPost, base+"/rule:import", bob, importBody, http.StatusForbidden)
	expectStatus(t, "alice imports into A", ts, http.MethodPost, base+"/rule:import", alice, importBody, http.StatusOK, `"created":1`)

	// 告警事件（评估引擎的产物，这里直接预置）。
	now := time.Now()
	alertA, alertB := "018f3e1a-aaaa-7000-8000-000000000001", "018f3e1a-bbbb-7000-8000-000000000001"
	svc.monitor.store.SeedAlert(&bizmonitor.Alert{
		ID: alertA, Fingerprint: strings.Repeat("a", 32), RuleID: ruleA, RuleName: "cpu-high", GroupID: groupA, Severity: 3,
		Status: mc.AlertFiring, Labels: map[string]string{"instance": "a-1"}, FirstFireTime: now, LastFireTime: now,
	})
	svc.monitor.store.SeedAlert(&bizmonitor.Alert{
		ID: alertB, Fingerprint: strings.Repeat("b", 32), RuleID: ruleB, RuleName: "disk-full", GroupID: groupB, Severity: 4,
		Status: mc.AlertFiring, Labels: map[string]string{"instance": "b-1"}, FirstFireTime: now, LastFireTime: now,
	})
	resp = expectStatus(t, "alice alerts", ts, http.MethodGet, base+"/alert?page=1&pageSize=10", alice, "", http.StatusOK, `"total":"1"`, "a-1")
	// 按完整的键值匹配：响应里的随机 UUID 可能恰好含有 "b-1" 这样的片段。
	if strings.Contains(resp, `"instance":"b-1"`) {
		t.Fatalf("alice must not see team-b alerts: %s", resp)
	}
	expectStatus(t, "bob alerts", ts, http.MethodGet, base+"/alert?page=1&pageSize=10", bob, "", http.StatusOK, `"total":"1"`, "b-1")
	expectStatus(t, "nobody alerts", ts, http.MethodGet, base+"/alert?page=1&pageSize=10", nobody, "", http.StatusOK, `"total":"0"`)
	// 范围外的事件与不存在的事件同样是 404（不泄露是否存在）。
	expectStatus(t, "bob gets alert A", ts, http.MethodGet, base+"/alert/"+alertA, bob, "", http.StatusNotFound, "MONITOR_ALERT_NOT_FOUND")
	expectStatus(t, "alice gets alert A", ts, http.MethodGet, base+"/alert/"+alertA, alice, "", http.StatusOK, `"canViewRule":true`)
	expectStatus(t, "alice aggregate", ts, http.MethodGet, base+"/alert-aggregate?by=severity", alice, "", http.StatusOK, `"total":1`, `"key":"3"`)
	expectStatus(t, "bob aggregate by label", ts, http.MethodGet, base+"/alert-aggregate?by=label:instance", bob, "", http.StatusOK, `"total":1`, `"key":"b-1"`)

	// 屏蔽规则：必须落在可管理的分组；全局屏蔽仅超级管理员；别人的看不到也改不了。
	muteBody := func(groupID string) string {
		group := ""
		if groupID != "" {
			group = `"groupId":"` + groupID + `",`
		}
		return `{"spec":{` + group + `"name":"维护","reason":"变更窗口","mode":1,"timeType":1,"enabled":true,` +
			`"startTime":"` + now.Add(-time.Hour).UTC().Format(time.RFC3339) + `","endTime":"` + now.Add(time.Hour).UTC().Format(time.RFC3339) + `"}}`
	}
	muteA := monitorID(t, ts, http.MethodPost, base+"/mute", alice, muteBody(groupA), "mute")
	expectStatus(t, "alice mutes B", ts, http.MethodPost, base+"/mute", alice, muteBody(groupB), http.StatusForbidden, "MONITOR_FORBIDDEN")
	expectStatus(t, "alice creates global mute", ts, http.MethodPost, base+"/mute", alice, muteBody(""), http.StatusForbidden, "MONITOR_FORBIDDEN")
	monitorID(t, ts, http.MethodPost, base+"/mute", admin, muteBody(""), "mute")
	expectStatus(t, "bob gets mute A", ts, http.MethodGet, base+"/mute/"+muteA, bob, "", http.StatusForbidden)
	expectStatus(t, "bob updates mute A", ts, http.MethodPut, base+"/mute/"+muteA, bob,
		strings.Replace(muteBody(groupB), `{"spec"`, `{"version":1,"spec"`, 1), http.StatusForbidden)
	expectStatus(t, "bob deletes mute A", ts, http.MethodDelete, base+"/mute/"+muteA, bob, "", http.StatusForbidden)
	// 别人看到的只有全局屏蔽（只读）；自己看到自己的加全局的。
	resp = expectStatus(t, "bob mutes", ts, http.MethodGet, base+"/mute?page=1&pageSize=10", bob, "", http.StatusOK, `"total":"1"`, `"canManage":false`)
	if strings.Contains(resp, muteA) {
		t.Fatalf("bob must not see team-a mute: %s", resp)
	}
	expectStatus(t, "alice mutes", ts, http.MethodGet, base+"/mute?page=1&pageSize=10", alice, "", http.StatusOK, `"total":"2"`, muteA, `"state":"active"`)
	// 预览只在可读范围内找事件。
	expectStatus(t, "alice previews mute A", ts, http.MethodPost, base+"/mute:preview", alice, muteBody(groupA), http.StatusOK, `"total":1`, "a-1")
	expectStatus(t, "bob previews mute A", ts, http.MethodPost, base+"/mute:preview", bob, muteBody(groupA), http.StatusForbidden)

	// 订阅：只能订阅可读的分组；为他人订阅需要对分组可管理（这里 alice 对 A 可管理）。
	subBody := func(groupID, receivers string) string {
		return `{"spec":{"name":"关注","groupIds":["` + groupID + `"],"severities":[3,4],"onFiring":true,"enabled":true,"receivers":` + receivers + `}}`
	}
	expectStatus(t, "bob subscribes to A", ts, http.MethodPost, base+"/subscription", bob, subBody(groupA, `{"self":true}`), http.StatusForbidden, "MONITOR_FORBIDDEN")
	subscriptionA := monitorID(t, ts, http.MethodPost, base+"/subscription", alice, subBody(groupA, `{"self":true}`), "subscription")
	expectStatus(t, "bob gets subscription A", ts, http.MethodGet, base+"/subscription/"+subscriptionA, bob, "", http.StatusForbidden)
	expectStatus(t, "bob deletes subscription A", ts, http.MethodDelete, base+"/subscription/"+subscriptionA, bob, "", http.StatusForbidden)
	expectStatus(t, "bob lists all subscriptions", ts, http.MethodGet, base+"/subscription?page=1&pageSize=10&scope=all", bob, "", http.StatusOK, `"total":"0"`)
	expectStatus(t, "alice lists mine", ts, http.MethodGet, base+"/subscription?page=1&pageSize=10&scope=mine", alice, "", http.StatusOK, `"total":"1"`, `"canManage":true`)
	expectStatus(t, "alice previews subscription", ts, http.MethodPost, base+"/subscription:preview", alice, subBody(groupA, `{"self":true}`), http.StatusOK, `"total":1`, "a-1")

	// 通知对象不受分组限制：A 组的规则把 bob 列为通知对象，派发之后 bob 能看这一条事件（只读、
	// 看不了规则），也能在「通知过我的」里找到它；其余仍然隔离。
	notifyRule := strings.Replace(monitorRuleBody(groupA, "notify-bob"), `"notifyRecovery":true`,
		`"notifyRecovery":true,"notify":{"userIds":["`+bobID+`"]}`, 1)
	ruleN := monitorID(t, ts, http.MethodPost, base+"/rule", alice, notifyRule, "rule")
	alertN := "018f3e1a-aaaa-7000-8000-000000000002"
	svc.monitor.store.SeedAlert(&bizmonitor.Alert{
		ID: alertN, Fingerprint: strings.Repeat("c", 32), RuleID: ruleN, RuleName: "notify-bob", GroupID: groupA, Severity: 3,
		Status: mc.AlertFiring, Labels: map[string]string{"instance": "a-2"}, FirstFireTime: now, LastFireTime: now,
	})
	expectStatus(t, "bob before being notified", ts, http.MethodGet, base+"/alert/"+alertN, bob, "", http.StatusNotFound)
	svc.monitor.store.SeedOutbox("ob-e2e", alertN, mc.NotifyKindFiring, 1)
	if n, err := svc.monitor.dispatch.Run(context.Background()); err != nil || n != 1 {
		t.Fatalf("dispatch = %d, %v", n, err)
	}
	expectStatus(t, "notified recipient reads the alert", ts, http.MethodGet, base+"/alert/"+alertN, bob, "", http.StatusOK,
		`"canViewRule":false`, `"notifyCount":1`, `"type":"user"`, bobID)
	expectStatus(t, "notified recipient cannot read the rule", ts, http.MethodGet, base+"/rule/"+ruleN, bob, "", http.StatusForbidden)
	expectStatus(t, "bob's mine", ts, http.MethodGet, base+"/alert?page=1&pageSize=10&mine=true", bob, "", http.StatusOK, `"total":"1"`, "a-2")
	expectStatus(t, "bob's plain list is unchanged", ts, http.MethodGet, base+"/alert?page=1&pageSize=10", bob, "", http.StatusOK, `"total":"1"`, "b-1")
	expectStatus(t, "the other alert of team A stays hidden", ts, http.MethodGet, base+"/alert/"+alertA, bob, "", http.StatusNotFound)
	expectStatus(t, "alice sees the timeline", ts, http.MethodGet, base+"/alert/"+alertN, alice, "", http.StatusOK, `"canViewRule":true`, `"source":1`)

	// 只读级别：能看不能改。把 bob 的角色加到 A 上（只读）。
	expectStatus(t, "grant bob read on A", ts, http.MethodPut, base+"/role-scope", admin,
		`{"groupId":"`+groupA+`","items":[{"roleId":"`+roleA+`","level":2},{"roleId":"`+roleB+`","level":1}]}`, http.StatusOK)
	expectStatus(t, "bob reads rule A", ts, http.MethodGet, base+"/rule/"+ruleA, bob, "", http.StatusOK, `"canManage":false`)
	expectStatus(t, "bob toggles rule A", ts, http.MethodPost, base+"/rule/"+ruleA+":toggle", bob, `{"enabled":false}`, http.StatusForbidden)
	expectStatus(t, "bob subscribes self to A", ts, http.MethodPost, base+"/subscription", bob, subBody(groupA, `{"self":true}`), http.StatusOK)
	// 为他人订阅需要可管理：只读不够。
	expectStatus(t, "bob subscribes alice to A", ts, http.MethodPost, base+"/subscription", bob,
		subBody(groupA, `{"userIds":["`+aliceID+`"]}`), http.StatusForbidden, "MONITOR_FORBIDDEN")
}

// 数据源：凭据只写不读；测试连接沿用已保存的凭据；被规则指定时不可删除；缺省数据源唯一。
func TestMonitor_Datasource(t *testing.T) {
	t.Parallel()
	ts, svc := newTestHTTPServerWithServices(t)
	admin := testBearer(t, svc.signer, e2eActorID)
	const base = "/api/v1/monitor"
	dsBody := func(name, extra string) string {
		return `{"spec":{"name":"` + name + `","connection":{"url":"http://vm:8428","authType":3,"timeoutMs":5000` + extra + `},"isDefault":true,"enabled":true}}`
	}
	expectStatus(t, "bearer without token", ts, http.MethodPost, base+"/datasource", admin, dsBody("vm", ""), http.StatusBadRequest, "MONITOR_DATASOURCE_INVALID")
	resp := expectStatus(t, "create", ts, http.MethodPost, base+"/datasource", admin, dsBody("vm", `,"secret":"tok-123"`), http.StatusOK, `"hasSecret":true`, `"isDefault":true`)
	if strings.Contains(resp, "tok-123") {
		t.Fatalf("secret leaked in response: %s", resp)
	}
	var created struct {
		Datasource struct {
			ID string `json:"id"`
		} `json:"datasource"`
	}
	if err := json.Unmarshal([]byte(resp), &created); err != nil {
		t.Fatal(err)
	}
	id := created.Datasource.ID
	expectStatus(t, "duplicate name", ts, http.MethodPost, base+"/datasource", admin, dsBody("vm", `,"secret":"x"`), http.StatusConflict, "MONITOR_DATASOURCE_NAME_EXISTS")

	// 不带凭据更新 = 沿用；测试连接用到的是已保存的凭据。
	expectStatus(t, "update keeps secret", ts, http.MethodPut, base+"/datasource/"+id, admin,
		strings.Replace(dsBody("vm", ""), `{"spec"`, `{"version":1,"spec"`, 1), http.StatusOK, `"hasSecret":true`, `"version":2`)
	expectStatus(t, "stale version", ts, http.MethodPut, base+"/datasource/"+id, admin,
		strings.Replace(dsBody("vm", ""), `{"spec"`, `{"version":1,"spec"`, 1), http.StatusConflict, "MONITOR_VERSION_CONFLICT")
	expectStatus(t, "test saved", ts, http.MethodPost, base+"/datasource/"+id+":test", admin, `{}`, http.StatusOK, `"ok":true`)
	// 沿用已保存的凭据只限同一个地址（协议、主机、端口）：换了主机必须重新输入，否则凭据会被发到别处。
	expectStatus(t, "test unsaved config reusing secret", ts, http.MethodPost, base+"/datasource:test", admin,
		`{"id":"`+id+`","connection":{"url":"http://vm:8428/select/0/prometheus","authType":3,"timeoutMs":5000}}`, http.StatusOK, `"ok":true`)
	expectStatus(t, "test another host reusing secret", ts, http.MethodPost, base+"/datasource:test", admin,
		`{"id":"`+id+`","connection":{"url":"http://other:8428","authType":3,"timeoutMs":5000}}`, http.StatusBadRequest, "MONITOR_DATASOURCE_SECRET_REQUIRED")
	expectStatus(t, "update to another host keeping secret", ts, http.MethodPut, base+"/datasource/"+id, admin,
		strings.Replace(strings.Replace(dsBody("vm", ""), `{"spec"`, `{"version":2,"spec"`, 1), "http://vm:8428", "https://vm:8428", 1),
		http.StatusBadRequest, "MONITOR_DATASOURCE_SECRET_REQUIRED")
	conns := svc.monitor.query.Conns()
	if len(conns) != 2 || conns[0].Secret != "tok-123" || conns[1].Secret != "tok-123" || conns[1].URL != "http://vm:8428/select/0/prometheus" {
		t.Fatalf("test connection must reuse the saved secret on the same origin only: %d calls", len(conns))
	}

	// 第二个缺省数据源：前一个的缺省标记被取消。
	second := monitorID(t, ts, http.MethodPost, base+"/datasource", admin, dsBody("vm-2", `,"secret":"y"`), "datasource")
	expectStatus(t, "first no longer default", ts, http.MethodGet, base+"/datasource/"+id, admin, "", http.StatusOK, `"version":3`)
	resp = expectStatus(t, "options", ts, http.MethodGet, base+"/datasource-option", admin, "", http.StatusOK)
	if strings.Count(resp, `"isDefault":true`) != 1 {
		t.Fatalf("exactly one default datasource expected: %s", resp)
	}

	// 被规则明确指定时不可删除；范围为全部数据源的规则不算引用。
	rule := `{"spec":{"groupId":"` + mc.RootGroupID + `","name":"r1","enabled":true,"datasourceScope":1,"datasourceIds":["` + id + `"],` +
		`"queries":[{"expr":"up == 0","severity":3}],"evalIntervalS":60}}`
	ruleID := monitorID(t, ts, http.MethodPost, base+"/rule", admin, rule, "rule")
	monitorID(t, ts, http.MethodPost, base+"/rule", admin, monitorRuleBody(mc.RootGroupID, "global"), "rule")
	expectStatus(t, "delete referenced", ts, http.MethodDelete, base+"/datasource/"+id, admin, "", http.StatusConflict, "MONITOR_DATASOURCE_IN_USE", "r1")
	expectStatus(t, "delete unreferenced", ts, http.MethodDelete, base+"/datasource/"+second, admin, "", http.StatusOK)
	expectStatus(t, "delete rule", ts, http.MethodDelete, base+"/rule/"+ruleID, admin, "", http.StatusOK)
	expectStatus(t, "delete after rule removed", ts, http.MethodDelete, base+"/datasource/"+id, admin, "", http.StatusOK)
	expectStatus(t, "query deleted datasource", ts, http.MethodPost, base+"/query", admin, `{"datasourceId":"`+id+`","expr":"up"}`, http.StatusNotFound)
}
