package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	bizmonitor "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// 评审第一轮之后的契约补充与访问控制，经完整的 HTTP 链路（鉴权 / 授权中间件、protojson、校验）验证。

const (
	opListRules   = "/monitor.v1.MonitorRuleService/ListRules"
	opListAlerts  = "/monitor.v1.MonitorAlertService/ListAlerts"
	opGetAlert    = "/monitor.v1.MonitorAlertService/GetAlert"
	opCreateRule  = "/monitor.v1.MonitorRuleService/CreateRule"
	opTargetOpt   = "/monitor.v1.MonitorRuleService/ListMonitorTargetOptions"
	opCreateSub   = "/monitor.v1.MonitorSubscriptionService/CreateAlertSubscription"
	opListGroups  = "/monitor.v1.MonitorRuleGroupService/ListRuleGroups"
	opDeleteGroup = "/monitor.v1.MonitorRuleGroupService/DeleteRuleGroup"
)

// X5：告警详情免接口授权，访问控制在监控域内。
//
//	接收方（哪怕没有任何监控权限）                 → 200，收件人视图
//	有「查询告警事件」接口权限 + 事件在数据范围内   → 200，完整视图
//	有范围、没有接口权限、不是接收方               → 404
//	有接口权限、不在范围内、不是接收方             → 404
//	账号已停用                                     → 403 ACCOUNT_DISABLED
//	未登录                                         → 401
func TestMonitor_AlertDetailAccess(t *testing.T) {
	t.Parallel()
	ts, svc := newTestHTTPServerWithServices(t)
	admin := testBearer(t, svc.signer, e2eActorID)
	const base = "/api/v1/monitor"
	if got := monitorOps(); strings.Contains(strings.Join(got, "\n"), opGetAlert) {
		t.Fatalf("GetAlert is exempt from interface authorization and must not be grantable")
	}

	group := monitorID(t, ts, http.MethodPost, base+"/rule-group", admin, `{"name":"team-x"}`, "group")
	other := monitorID(t, ts, http.MethodPost, base+"/rule-group", admin, `{"name":"team-y"}`, "group")
	// 接收方：只是一个普通用户，没有角色、没有任何接口权限、没有任何分组范围。
	rcpt := createUser(t, ts, admin, "mon_rcpt")
	rcptBearer := testBearer(t, svc.signer, rcpt.Id)
	bystander := createUser(t, ts, admin, "mon_bystander")
	// 有范围、没有告警查询权限（只能看规则）。
	scoped, _, roleScoped := monitorUser(t, ts, svc, admin, "mon_scoped", []string{opListRules})
	// 有告警查询权限、范围在别的分组。
	granted, _, roleGranted := monitorUser(t, ts, svc, admin, "mon_granted", []string{opListAlerts})
	// 两者都有。
	reader, _, roleReader := monitorUser(t, ts, svc, admin, "mon_reader", []string{opListAlerts})
	for groupID, roles := range map[string][]string{group: {roleScoped, roleReader}, other: {roleGranted}} {
		items := make([]string, 0, len(roles))
		for _, r := range roles {
			items = append(items, `{"roleId":"`+r+`","level":1}`)
		}
		expectStatus(t, "grant scope", ts, http.MethodPut, base+"/role-scope", admin,
			`{"groupId":"`+groupID+`","items":[`+strings.Join(items, ",")+`]}`, http.StatusOK)
	}

	rule := strings.Replace(monitorRuleBody(group, "notify-rcpt"), `"notifyRecovery":true`,
		`"notifyRecovery":true,"notify":{"userIds":["`+rcpt.Id+`","`+bystander.Id+`"]}`, 1)
	ruleID := monitorID(t, ts, http.MethodPost, base+"/rule", admin, rule, "rule")
	now := time.Now()
	alertID, hidden := "018f3e1a-cccc-7000-8000-000000000001", "018f3e1a-cccc-7000-8000-000000000002"
	for id, instance := range map[string]string{alertID: "x-1", hidden: "x-2"} {
		svc.monitor.store.SeedAlert(&bizmonitor.Alert{
			ID: id, Fingerprint: strings.Repeat(id[len(id)-1:], 32), RuleID: ruleID, RuleName: "notify-rcpt", GroupID: group, Severity: 3,
			Status: mc.AlertFiring, Labels: map[string]string{"instance": instance}, FirstFireTime: now, LastFireTime: now, MuteID: "018f3e1a-dddd-7000-8000-000000000001",
		})
	}
	get := func(id string) string { return base + "/alert/" + id }

	// 通知发出之前：接收方还不是接收方。
	expectStatus(t, "recipient before dispatch", ts, http.MethodGet, get(alertID), rcptBearer, "", http.StatusNotFound, "MONITOR_ALERT_NOT_FOUND")
	svc.monitor.store.SeedOutbox("ob-x5", alertID, mc.NotifyKindFiring, 1)
	if n, err := svc.monitor.dispatch.Run(context.Background()); err != nil || n != 1 {
		t.Fatalf("dispatch = %d, %v", n, err)
	}

	// 接收方：200，只读的收件人视图。
	resp := expectStatus(t, "recipient without any monitor grant", ts, http.MethodGet, get(alertID), rcptBearer, "", http.StatusOK,
		`"canViewRule":false`, `"ruleName":"notify-rcpt"`, `"instance":"x-1"`, rcpt.Id)
	for what, leak := range map[string]string{
		"another recipient":  bystander.Id,
		"the mute rule":      "018f3e1a-dddd-7000-8000-000000000001",
		"notify event ids":   `"notifyEventIds":["`,
		"another alert":      `"instance":"x-2"`,
		"a second log entry": `"source":2`,
	} {
		if strings.Contains(resp, leak) {
			t.Errorf("the recipient view leaks %s: %s", what, resp)
		}
	}
	// 只限这一条：同一规则的另一条事件、规则本身、告警列表都不行。
	expectStatus(t, "recipient reads another alert of the rule", ts, http.MethodGet, get(hidden), rcptBearer, "", http.StatusNotFound)
	expectStatus(t, "recipient reads the rule", ts, http.MethodGet, base+"/rule/"+ruleID, rcptBearer, "", http.StatusForbidden, "PERMISSION_DENIED")
	expectStatus(t, "recipient lists alerts", ts, http.MethodGet, base+"/alert?page=1&pageSize=10&mine=true", rcptBearer, "", http.StatusForbidden, "PERMISSION_DENIED")

	// 非接收方：接口权限与范围缺一不可，缺了就是 404，与事件不存在看不出区别。
	missing := expectStatus(t, "missing alert", ts, http.MethodGet, get("018f3e1a-cccc-7000-8000-00000000ffff"), scoped, "", http.StatusNotFound)
	for what, bearer := range map[string]string{"scope without grant": scoped, "grant without scope": granted} {
		if got := expectStatus(t, what, ts, http.MethodGet, get(alertID), bearer, "", http.StatusNotFound, "MONITOR_ALERT_NOT_FOUND"); got != missing {
			t.Errorf("%s: response differs from a missing alert:\n%s\n%s", what, got, missing)
		}
	}
	resp = expectStatus(t, "grant + scope", ts, http.MethodGet, get(alertID), reader, "", http.StatusOK,
		`"canViewRule":true`, bystander.Id, rcpt.Id, `"muteId":"018f3e1a-dddd-7000-8000-000000000001"`)
	if !strings.Contains(resp, `"notifyEventIds":["`) {
		t.Errorf("the full view carries the timeline details: %s", resp)
	}
	expectStatus(t, "admin", ts, http.MethodGet, get(alertID), admin, "", http.StatusOK, `"canViewRule":true`)

	// 未登录 401；接收方账号停用后 403。
	expectStatus(t, "anonymous", ts, http.MethodGet, get(alertID), "", "", http.StatusUnauthorized)
	expectStatus(t, "disable recipient", ts, http.MethodPost, "/api/v1/system/user/"+rcpt.Id+"/status", admin, `{"status":2}`, http.StatusOK)
	expectStatus(t, "disabled recipient", ts, http.MethodGet, get(alertID), rcptBearer, "", http.StatusForbidden, "ACCOUNT_DISABLED")
}

// X6：通知对象候选项随规则 / 订阅的新建、编辑权限开放；现有的用户候选接口对只有监控权限的人不可用。
func TestMonitor_TargetOptions(t *testing.T) {
	t.Parallel()
	ts, svc := newTestHTTPServerWithServices(t)
	admin := testBearer(t, svc.signer, e2eActorID)
	const path = "/api/v1/monitor/target-option"

	// 权限目录里「新建规则」元素绑定的接口：建规则的人拿到的就是这些。
	editor, _, _ := monitorUser(t, ts, svc, admin, "mon_editor", []string{opListRules, opCreateRule, opTargetOpt, opListGroups})
	viewer, _, _ := monitorUser(t, ts, svc, admin, "mon_viewer", []string{opListRules, opListAlerts})

	const want = `{"schedules":[{"id":"018f3e1a-0e00-7000-8000-000000000001","name":"运维值班","groupName":"运维组"}],` +
		`"depts":[{"id":"018f3e1a-0e01-7000-8000-000000000001","name":"技术部","parentId":""},` +
		`{"id":"018f3e1a-0e01-7000-8000-000000000002","name":"运维部","parentId":"018f3e1a-0e01-7000-8000-000000000001"}],` +
		`"users":[{"id":"018f3e1a-0e02-7000-8000-000000000001","username":"alice","nickname":"Alice"},` +
		`{"id":"018f3e1a-0e02-7000-8000-000000000002","username":"bob","nickname":""}]}`
	// 这就是文档里写给前端的 JSON 形态：键名、键序、取值逐一比对（零值字段照常输出：顶级部门的 parentId、
	// 没有昵称的用户的 nickname 都是空串）。protojson 的输出在分隔符后带不带空格随构建而变，先去掉空白。
	var got bytes.Buffer
	if err := json.Compact(&got, []byte(expectStatus(t, "all kinds", ts, http.MethodGet, path, editor, "", http.StatusOK))); err != nil {
		t.Fatal(err)
	}
	if got.String() != want {
		t.Fatalf("response shape changed:\n got %s\nwant %s", got.String(), want)
	}
	// 只要用户、带关键字。
	// 只要用户、带关键字：未请求的种类是空数组。
	users := expectStatus(t, "users by keyword", ts, http.MethodGet, path+"?kinds=user&keyword=ali", editor, "", http.StatusOK,
		`"schedules":[]`, `"depts":[]`, `"username":"alice"`)
	if strings.Contains(users, "bob") {
		t.Fatalf("kinds=user&keyword=ali must return matching users only: %s", users)
	}
	expectStatus(t, "schedules and depts", ts, http.MethodGet, path+"?kinds=schedule&kinds=dept", editor, "", http.StatusOK, "运维值班", "运维部", `"users":[]`)
	expectStatus(t, "unknown kind", ts, http.MethodGet, path+"?kinds=role", editor, "", http.StatusBadRequest)
	expectStatus(t, "keyword too long", ts, http.MethodGet, path+"?keyword="+strings.Repeat("x", 65), editor, "", http.StatusBadRequest)

	// 没有该接口权限的监控用户、未登录：拒绝。
	expectStatus(t, "viewer", ts, http.MethodGet, path, viewer, "", http.StatusForbidden, "PERMISSION_DENIED")
	expectStatus(t, "anonymous", ts, http.MethodGet, path, "", "", http.StatusUnauthorized)

	// 现有的候选 / 列表接口属于组织架构、值班、流程的页面，只有监控权限的人调不通——所以本接口
	// 自带用户候选，而不是让前端去调它们。
	for _, existing := range []string{
		"/api/v1/system/dept-user-candidate", "/api/v1/oncall/user-candidate", "/api/v1/workflow/user-candidate",
		"/api/v1/system/dept", "/api/v1/oncall/schedule",
	} {
		expectStatus(t, "existing endpoint "+existing, ts, http.MethodGet, existing, editor, "", http.StatusForbidden, "PERMISSION_DENIED")
	}
}

// C11 / C2 经 HTTP：请求头掩码原样往返；换地址必须重新输入。
func TestMonitor_DatasourceHeaderMask(t *testing.T) {
	t.Parallel()
	ts, svc := newTestHTTPServerWithServices(t)
	admin := testBearer(t, svc.signer, e2eActorID)
	const base = "/api/v1/monitor"
	body := func(version int, url, headers string) string {
		v := ""
		if version > 0 {
			v = `"version":` + strconv.Itoa(version) + `,`
		}
		return `{` + v + `"spec":{"name":"vm","connection":{"url":"` + url + `","authType":1,"timeoutMs":5000,"headers":` + headers + `},"enabled":true}}`
	}
	resp := expectStatus(t, "create", ts, http.MethodPost, base+"/datasource", admin, body(0, "http://vm:8428", `{"X-API-Key":"k-secret-1","X-Scope":"team-a"}`),
		http.StatusOK, `"X-API-Key":"******"`, `"X-Scope":"******"`)
	if strings.Contains(resp, "k-secret-1") || strings.Contains(resp, "team-a") {
		t.Fatalf("header values leaked: %s", resp)
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
	for _, path := range []string{"/datasource/" + id, "/datasource?page=1&pageSize=10"} {
		if got := expectStatus(t, "read "+path, ts, http.MethodGet, base+path, admin, "", http.StatusOK, `"X-API-Key":"******"`); strings.Contains(got, "k-secret-1") {
			t.Fatalf("header value leaked by %s: %s", path, got)
		}
	}
	// 前端把读到的内容原样提交回来：值不变。
	expectStatus(t, "round trip", ts, http.MethodPut, base+"/datasource/"+id, admin, body(1, "http://vm:8428", `{"X-API-Key":"******","X-Scope":"******"}`),
		http.StatusOK, `"version":2`, `"X-API-Key":"******"`)
	expectStatus(t, "test saved", ts, http.MethodPost, base+"/datasource/"+id+":test", admin, `{}`, http.StatusOK, `"ok":true`)
	expectStatus(t, "test edited config", ts, http.MethodPost, base+"/datasource:test", admin,
		`{"id":"`+id+`","connection":{"url":"http://vm:8428","authType":1,"timeoutMs":5000,"headers":{"X-API-Key":"******","X-Scope":"changed"}}}`, http.StatusOK, `"ok":true`)
	conns := svc.monitor.query.Conns()
	if len(conns) != 2 || conns[0].Headers["X-API-Key"] != "k-secret-1" || conns[0].Headers["X-Scope"] != "team-a" ||
		conns[1].Headers["X-API-Key"] != "k-secret-1" || conns[1].Headers["X-Scope"] != "changed" {
		t.Fatalf("masked headers must be replaced by the stored values before the request is sent (%d calls)", len(conns))
	}
	// 掩码 + 新地址：拒绝；掩码用在没保存过的键上：拒绝。
	expectStatus(t, "masked header to another host", ts, http.MethodPut, base+"/datasource/"+id, admin,
		body(2, "http://evil.example:8428", `{"X-API-Key":"******"}`), http.StatusBadRequest, "MONITOR_DATASOURCE_SECRET_REQUIRED")
	expectStatus(t, "test masked header against another host", ts, http.MethodPost, base+"/datasource:test", admin,
		`{"id":"`+id+`","connection":{"url":"http://evil.example:8428","authType":1,"timeoutMs":5000,"headers":{"X-API-Key":"******"}}}`,
		http.StatusBadRequest, "MONITOR_DATASOURCE_SECRET_REQUIRED")
	expectStatus(t, "mask for a new key", ts, http.MethodPut, base+"/datasource/"+id, admin,
		body(2, "http://vm:8428", `{"X-Other":"******"}`), http.StatusBadRequest, "MONITOR_DATASOURCE_SECRET_REQUIRED")
	expectStatus(t, "mask on create", ts, http.MethodPost, base+"/datasource", admin,
		strings.Replace(body(0, "http://vm:8428", `{"X-API-Key":"******"}`), `"name":"vm"`, `"name":"vm-2"`, 1), http.StatusBadRequest, "MONITOR_DATASOURCE_SECRET_REQUIRED")
	if len(svc.monitor.query.Conns()) != 2 {
		t.Fatalf("a refused request must not reach the datasource")
	}
	// 换地址时填了新值：可以。
	expectStatus(t, "move with re-entered header", ts, http.MethodPut, base+"/datasource/"+id, admin,
		body(2, "http://new-host:8428", `{"X-API-Key":"k-2"}`), http.StatusOK, `"url":"http://new-host:8428"`, `"X-API-Key":"******"`)
}

// X1 / X2 / C1 / C15 经 HTTP。
func TestMonitor_ImportExportContract(t *testing.T) {
	t.Parallel()
	ts, svc := newTestHTTPServerWithServices(t)
	admin := testBearer(t, svc.signer, e2eActorID)
	const base = "/api/v1/monitor"
	group := monitorID(t, ts, http.MethodPost, base+"/rule-group", admin, `{"name":"src"}`, "group")
	target := monitorID(t, ts, http.MethodPost, base+"/rule-group", admin, `{"name":"dst"}`, "group")
	ruleID := monitorID(t, ts, http.MethodPost, base+"/rule", admin, monitorRuleBody(group, "cpu-high"), "rule")

	// 导出：缺省 YAML；format=json 是带版本的完整格式，文件名随之变化。
	expectStatus(t, "export yaml", ts, http.MethodPost, base+"/rule:export", admin, `{"ids":["`+ruleID+`"]}`, http.StatusOK, `alert: cpu-high`, `.yaml"`)
	expectStatus(t, "export unknown format", ts, http.MethodPost, base+"/rule:export", admin, `{"ids":["`+ruleID+`"],"format":"xml"}`, http.StatusBadRequest)
	resp := expectStatus(t, "export json", ts, http.MethodPost, base+"/rule:export", admin, `{"ids":["`+ruleID+`"],"format":"json"}`, http.StatusOK, `"count":1`, `.json"`)
	var exported struct {
		Content string `json:"content"`
	}
	if err := json.Unmarshal([]byte(resp), &exported); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(exported.Content, `"format": "reborn.monitor.rules/v1"`) || !strings.Contains(exported.Content, `"name": "src"`) {
		t.Fatalf("exported content = %s", exported.Content)
	}
	// 导入：内容自动识别为 JSON，落到导入选项指定的分组。
	content, _ := json.Marshal(exported.Content)
	opts := `{"content":` + string(content) + `,"groupId":"` + target + `","datasourceScope":2,"defaultSeverity":2}`
	expectStatus(t, "preview json", ts, http.MethodPost, base+"/rule:import-preview", admin, `{"options":`+opts+`}`, http.StatusOK,
		`"newCount":1`, `"sourceGroup":"src"`, `"groupId":"`+target+`"`)
	expectStatus(t, "import json", ts, http.MethodPost, base+"/rule:import", admin, `{"options":`+opts+`,"onConflict":"skip","enabled":true}`, http.StatusOK, `"created":1`)
	expectStatus(t, "imported rule", ts, http.MethodGet, base+"/rule?page=1&pageSize=10&groupId="+target, admin, "", http.StatusOK, `"total":"1"`, "cpu-high", `"enabled":true`)
	wrong, _ := json.Marshal(strings.Replace(exported.Content, "rules/v1", "rules/v9", 1))
	expectStatus(t, "unsupported version", ts, http.MethodPost, base+"/rule:import-preview", admin,
		`{"options":{"content":`+string(wrong)+`,"groupId":"`+target+`","datasourceScope":2,"defaultSeverity":2}}`, http.StatusBadRequest, "MONITOR_RULE_IMPORT_INVALID")

	// 级别对照表（YAML）。
	yaml := `{"content":"groups:\n- name: g\n  rules:\n  - alert: mapped\n    expr: up == 0\n    labels: {severity: SEV-1}\n","groupId":"` + target +
		`","datasourceScope":2,"defaultSeverity":1,"severityMap":{"sev-1":4}}`
	expectStatus(t, "severity map", ts, http.MethodPost, base+"/rule:import-preview", admin, `{"options":`+yaml+`}`, http.StatusOK, `"severity":4`, `"newCount":1`)
	expectStatus(t, "severity map out of range", ts, http.MethodPost, base+"/rule:import-preview", admin,
		`{"options":`+strings.Replace(yaml, `{"sev-1":4}`, `{"sev-1":5}`, 1)+`}`, http.StatusBadRequest)

	// WITH 模板：校验接口立刻给出结论，保存接口拒绝。
	bomb := "WITH (a0 = x"
	for i := 1; i <= 20; i++ {
		prev, cur := "a"+strconv.Itoa(i-1), "a"+strconv.Itoa(i)
		bomb += ", " + cur + " = " + prev + " + " + prev
	}
	bomb += ") a20"
	start := time.Now()
	expectStatus(t, "validate WITH bomb", ts, http.MethodPost, base+"/rule:validate", admin, `{"exprs":["`+bomb+`","up == 0"]}`, http.StatusOK,
		`"valid":false`, "WITH templates are not supported", `"valid":true`)
	expectStatus(t, "save WITH bomb", ts, http.MethodPost, base+"/rule", admin,
		strings.Replace(monitorRuleBody(group, "bomb"), "up == 0", bomb, 1), http.StatusBadRequest, "MONITOR_RULE_INVALID")
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("WITH expressions must be rejected without expanding them (took %v)", took)
	}

	// 删除仍被屏蔽规则 / 订阅引用的分组：409，数量在 metadata 里。
	now := time.Now()
	mute := `{"spec":{"groupId":"` + target + `","name":"维护","reason":"变更","mode":1,"timeType":1,"enabled":true,"startTime":"` +
		now.Add(-time.Hour).UTC().Format(time.RFC3339) + `","endTime":"` + now.Add(time.Hour).UTC().Format(time.RFC3339) + `"}}`
	monitorID(t, ts, http.MethodPost, base+"/mute", admin, mute, "mute")
	monitorID(t, ts, http.MethodPost, base+"/subscription", admin,
		`{"spec":{"name":"关注","groupIds":["`+target+`"],"severities":[3],"onFiring":true,"enabled":true,"receivers":{"self":true}}}`, "subscription")
	// 先清掉分组里的规则，剩下的阻碍就只有引用。
	rules := expectStatus(t, "rules in target", ts, http.MethodGet, base+"/rule?page=1&pageSize=10&groupId="+target, admin, "", http.StatusOK)
	var listed struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
	}
	if err := json.Unmarshal([]byte(rules), &listed); err != nil {
		t.Fatal(err)
	}
	for _, r := range listed.Items {
		expectStatus(t, "delete rule", ts, http.MethodDelete, base+"/rule/"+r.ID, admin, "", http.StatusOK)
	}
	expectStatus(t, "delete referenced group", ts, http.MethodDelete, base+"/rule-group/"+target, admin, "", http.StatusConflict,
		"MONITOR_GROUP_IN_USE", `"mute_count":"1"`, `"subscription_count":"1"`)
}
