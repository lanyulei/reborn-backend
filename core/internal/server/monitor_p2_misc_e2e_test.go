package server

import (
	"context"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	bizmonitor "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	service "github.com/lanyulei/reborn/backend/core/internal/service/system"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

func rfc(t time.Time) string { return url.QueryEscape(t.UTC().Format(time.RFC3339)) }

// 统计与引擎状态只统计调用方数据范围内的数据：alice 永远数不到 B 组的事件，没有范围的人全是零；时间范围
// 超过 90 天 400；引擎状态里的失败规则按范围过滤，全局计数不含对象内容。
func TestMonitorP2_StatsAndEngine(t *testing.T) {
	t.Parallel()
	f := newP2Fixture(t)
	// A 组：一条告警中（夹具）+ 一条已认领已恢复；B 组：一条告警中（夹具）+ 两条被屏蔽的致命告警。
	ackAt, resolveAt := f.now.Add(-8*time.Minute), f.now.Add(-5*time.Minute)
	f.seedAlert("018f3e1a-bbbb-7000-8000-0000000000f1", f.ruleA, "cpu-high", f.groupA, 3, func(a *bizmonitor.Alert) {
		a.Status, a.ResolveTime, a.AckUser, a.AckTime, a.NotifyCount = mc.AlertResolved, &resolveAt, f.aliceID, &ackAt, 1
	})
	for _, id := range []string{"018f3e1a-bbbb-7000-8000-0000000000f2", "018f3e1a-bbbb-7000-8000-0000000000f3"} {
		f.seedAlert(id, f.ruleB, "disk-full", f.groupB, 4, func(a *bizmonitor.Alert) { a.Muted = true })
	}
	start, end := rfc(f.now.Add(-24*time.Hour)), rfc(f.now.Add(time.Hour))
	rng := "startTime=" + start + "&endTime=" + end

	// 概览。
	f.get(t, "alice overview", "/stats/overview", f.alice, http.StatusOK, `"firing":1`, `"firingBySeverity":[0,0,1,0]`, `"unacked":1`, `"muted":0`)
	f.get(t, "bob overview", "/stats/overview", f.bob, http.StatusOK, `"firing":3`, `"firingBySeverity":[0,1,0,2]`, `"unacked":1`, `"muted":2`)
	f.get(t, "admin overview", "/stats/overview", f.admin, http.StatusOK, `"firing":4`)
	f.get(t, "nobody overview", "/stats/overview", f.nobody, http.StatusOK, `"firing":0`, `"todayFired":0`)
	f.get(t, "alice overview filtered by B", "/stats/overview?groupId="+f.groupB, f.alice, http.StatusOK, `"firing":0`)
	f.get(t, "bad timezone", "/stats/overview?timezone=Mars%2FOlympus", f.alice, http.StatusBadRequest, "MONITOR_QUERY_INVALID")

	// 趋势 / 排行 / 效率 / 降噪：各自只有自己分组的数字。
	resp := f.get(t, "alice trend", "/stats/trend?"+rng+"&by=group", f.alice, http.StatusOK, `"name":"team-a"`)
	lacks(t, "alice trend", resp, "team-b", f.groupB)
	if !strings.Contains(resp, `"total":2`) {
		t.Fatalf("alice trend must count her 2 alerts: %s", resp)
	}
	resp = f.get(t, "bob trend", "/stats/trend?"+rng+"&by=group", f.bob, http.StatusOK, `"name":"team-b"`, `"total":3`)
	lacks(t, "bob trend", resp, "team-a", f.groupA)
	lacks(t, "nobody trend", f.get(t, "nobody trend", "/stats/trend?"+rng, f.nobody, http.StatusOK), `"total":1`, `"total":2`, `"total":3`)

	resp = f.get(t, "alice top", "/stats/top?"+rng+"&by=rule", f.alice, http.StatusOK, `"total":2`, f.ruleA, `"name":"cpu-high"`, `"groupName":"team-a"`)
	lacks(t, "alice top", resp, f.ruleB, "disk-full")
	resp = f.get(t, "bob top", "/stats/top?"+rng+"&by=rule", f.bob, http.StatusOK, `"total":3`, f.ruleB)
	lacks(t, "bob top", resp, f.ruleA, "cpu-high")
	f.get(t, "nobody top", "/stats/top?"+rng+"&by=rule", f.nobody, http.StatusOK, `"items":[]`, `"total":0`)
	lacks(t, "alice top by label", f.get(t, "alice top by label", "/stats/top?"+rng+"&by=label:instance", f.alice, http.StatusOK, "host-a"), "host-b")

	resp = f.get(t, "alice efficiency", "/stats/efficiency?"+rng+"&by=severity", f.alice, http.StatusOK, `"total":{"key":"total"`, `"alerts":2`, `"acked":1`, `"resolved":1`, `"mttaS":"120"`, `"mttrS":"300"`)
	lacks(t, "alice efficiency", resp, `"alerts":3`, `"alerts":5`)
	f.get(t, "bob efficiency", "/stats/efficiency?"+rng, f.bob, http.StatusOK, `"alerts":3`, `"acked":0`, `"mttaS":"0"`)

	f.get(t, "alice noise", "/stats/noise?"+rng, f.alice, http.StatusOK, `"alerts":2`, `"muted":0`, `"notified":1`, `"silent":1`)
	f.get(t, "bob noise", "/stats/noise?"+rng, f.bob, http.StatusOK, `"alerts":3`, `"muted":2`, `"notified":0`, `"silent":3`)
	f.get(t, "nobody noise", "/stats/noise?"+rng, f.nobody, http.StatusOK, `"alerts":0`, `"notifications":0`)

	// 时间范围：超过 90 天、起止颠倒。
	long := "startTime=" + rfc(f.now.Add(-91*24*time.Hour)) + "&endTime=" + end
	for _, path := range []string{"/stats/trend?", "/stats/top?by=rule&", "/stats/efficiency?", "/stats/noise?"} {
		f.get(t, "range over 90 days "+path, path+long, f.alice, http.StatusBadRequest, "MONITOR_TIME_RANGE_INVALID")
		f.get(t, "reversed range "+path, path+"startTime="+end+"&endTime="+start, f.alice, http.StatusBadRequest)
	}

	// 引擎状态：实例与全局计数人人可见（有该接口权限即可）；持续失败的规则按数据范围过滤。
	since := f.now.Add(-time.Hour)
	f.h.engine.InstanceItems = []*bizmonitor.EngineInstance{
		{Instance: "engine-1", Roles: []string{"planner", "evaluator"}, Leader: true, Alive: true, StartTime: since, LastSeen: f.now, TaskCount: 5},
	}
	f.h.engine.SummaryValue = bizmonitor.EngineSummary{TaskTotal: 10, TaskError: 2, NotifyPending: 3}
	f.h.engine.RuleItems = []*bizmonitor.EngineRuleIssue{
		{RuleID: f.ruleA, RuleName: "cpu-high", GroupID: f.groupA, Error: "timeout", Since: &since, Failures: 5},
		{RuleID: f.ruleB, RuleName: "disk-full", GroupID: f.groupB, Error: "bad gateway", Since: &since, Failures: 4},
	}
	resp = f.get(t, "alice engine", "/engine/status", f.alice, http.StatusOK, `"instance":"engine-1"`, `"healthy":true`, `"taskTotal":10`, `"notifyPending":3`, f.ruleA, `"groupName":"team-a"`)
	lacks(t, "alice engine", resp, f.ruleB, "disk-full", "bad gateway")
	resp = f.get(t, "bob engine", "/engine/status", f.bob, http.StatusOK, f.ruleB)
	lacks(t, "bob engine", resp, f.ruleA, "cpu-high")
	f.get(t, "nobody engine", "/engine/status", f.nobody, http.StatusOK, `"rules":[]`, `"taskTotal":10`)
	f.get(t, "admin engine", "/engine/status", f.admin, http.StatusOK, f.ruleA, f.ruleB)
}

// 值班班次告警数免接口授权（值班日历页也要调），访问控制在监控域：持有「查询告警事件」或「查询值班班次」
// 任一接口权限才返回；否则 403（域内判定，不是授权中间件）；未登录 401。
func TestMonitorP2_ShiftAlertCounts(t *testing.T) {
	t.Parallel()
	f := newP2Fixture(t)
	const opListAlerts, opListShifts = "/monitor.v1.MonitorAlertService/ListAlerts", "/oncall.v1.OncallScheduleService/ListShifts"
	if !service.IsAuthzExempt("/monitor.v1.MonitorAlertService/ListShiftAlertCounts") {
		t.Fatal("ListShiftAlertCounts must be authz-exempt")
	}
	monitorOnly, _, _ := monitorUser(t, f.ts, f.svc, f.admin, "p2_mon_only", []string{opListAlerts})
	oncallOnly, _, _ := monitorUser(t, f.ts, f.svc, f.admin, "p2_oncall_only", []string{opListShifts})
	// 有别的监控接口权限、但两项都没有。
	other, _, _ := monitorUser(t, f.ts, f.svc, f.admin, "p2_other", []string{"/monitor.v1.MonitorRuleService/ListRules"})
	plain := testBearer(t, f.svc.signer, createUser(t, f.ts, f.admin, "p2_plain").Id)

	shiftStart, shiftEnd := f.now.Add(-2*time.Hour), f.now.Add(2*time.Hour)
	f.h.cross.set(func(c *monitorCross) {
		c.shifts[e2eSchedule] = []bizmonitor.ShiftSlot{
			{UserID: f.bobID, UserName: "p2_bob", Role: 1, Start: shiftStart, End: shiftEnd},
			{UserID: f.aliceID, UserName: "p2_alice", Role: 2, Start: shiftStart, End: shiftEnd},
		}
	})
	// 班次内通知到 bob 的两条事件（同一事件两次通知只计一次），班次外的一条不计。
	f.notified("018f3e1a-cccc-7000-8000-0000000000a1", f.alertA, f.bobID, f.now.Add(-time.Hour))
	f.notified("018f3e1a-cccc-7000-8000-0000000000a2", f.alertA, f.bobID, f.now.Add(-30*time.Minute))
	f.notified("018f3e1a-cccc-7000-8000-0000000000a3", f.alertB, f.bobID, f.now.Add(-10*time.Minute))
	f.notified("018f3e1a-cccc-7000-8000-0000000000a4", f.alertB, f.bobID, f.now.Add(-5*time.Hour))
	query := "/shift-alert-count?scheduleId=" + e2eSchedule + "&startTime=" + rfc(f.now.Add(-24*time.Hour)) + "&endTime=" + rfc(f.now.Add(24*time.Hour))

	expectStatus(t, "anonymous", f.ts, http.MethodGet, monitorBase+query, "", "", http.StatusUnauthorized)
	f.get(t, "no grants at all", query, plain, http.StatusForbidden, "MONITOR_FORBIDDEN")
	f.get(t, "neither permission", query, other, http.StatusForbidden, "MONITOR_FORBIDDEN")
	want := []string{`"userId":"` + f.bobID + `"`, `"role":1`, `"alertCount":2`, `"userId":"` + f.aliceID + `"`, `"role":2`, `"alertCount":0`}
	f.get(t, "only the alert-list permission", query, monitorOnly, http.StatusOK, want...)
	f.get(t, "only the oncall calendar permission", query, oncallOnly, http.StatusOK, want...)
	f.get(t, "admin", query, f.admin, http.StatusOK, want...)
	// 不存在的值班表：空列表；范围超过 62 天：400。
	f.get(t, "unknown schedule", strings.Replace(query, e2eSchedule, "018f3e1a-dead-7000-8000-000000000008", 1), monitorOnly, http.StatusOK, `"items":[]`)
	f.get(t, "range over 62 days", "/shift-alert-count?scheduleId="+e2eSchedule+"&startTime="+rfc(f.now.Add(-70*24*time.Hour))+"&endTime="+rfc(f.now), monitorOnly, http.StatusBadRequest)
}

// monitorTestEvents 通知管道里模拟触发写入的测试事件（标题带 [TEST]）。
func monitorTestEvents(t *testing.T, f *p2Fixture) []*biznotify.Event {
	t.Helper()
	events, _, err := f.svc.store.ListEvents(context.Background(), biznotify.EventFilter{Keyword: "[TEST] "}, 1, 100)
	if err != nil {
		t.Fatalf("list notify events: %v", err)
	}
	return events
}

// 规则的第二、三期：内置模板包、候选项、新字段的保存与回显（含校验）、模拟触发（干跑不写任何通知事件，
// 真实发送恰好写一条标题带 [TEST] 的事件；要求可管理）。
func TestMonitorP2_Rule(t *testing.T) {
	t.Parallel()
	f := newP2Fixture(t)

	// 内置模板包：静态内容；编码不存在 404；内容可以原样走导入预览，逐条都合法。
	resp := f.get(t, "template packs", "/rule-template", f.alice, http.StatusOK, `"code":"node-exporter"`, `"code":"mysql"`, `"code":"redis"`, `"code":"kafka"`, `"code":"reborn-self"`, `"category":"host"`)
	if strings.Contains(resp, `"ruleCount":0`) {
		t.Fatalf("every pack must have rules: %s", resp)
	}
	f.get(t, "unknown pack", "/rule-template/nope", f.alice, http.StatusNotFound, "MONITOR_RULE_TEMPLATE_NOT_FOUND")
	pack := f.get(t, "pack detail", "/rule-template/node-exporter", f.alice, http.StatusOK, `"pack":{"code":"node-exporter"`, `"rules":[{"name":`, `"content":"`)
	content := p2RawField(t, pack, "content")
	preview := f.post(t, "import preview of a pack", "/rule:import-preview", f.alice,
		`{"options":{"content":`+content+`,"groupId":"`+f.groupA+`","datasourceScope":2,"defaultSeverity":2}}`, http.StatusOK, `"invalidCount":0`)
	if strings.Contains(preview, `"total":0`) {
		t.Fatalf("pack must yield rules: %s", preview)
	}

	// 候选项：最小披露，不要求工单 / 流程 / CMDB 的权限。
	f.h.envs.Set("prod", "test")
	f.get(t, "rule options", "/rule-option", f.nobody, http.StatusOK, `"ticketTemplates":[{"id":"`+e2eTemplateID+`","name":"故障处理","categoryName":"运维"}]`,
		`"workflows":[{"key":"incident","name":"故障应急"}]`, `"ciModels":[{"code":"host","name":"主机"}]`, `"ciEnvs":["prod","test"]`, `"ciMatchAttrs":["ip"]`, `"aiAvailable":false`)

	// 新字段的保存与回显。
	p2 := `"notifyRecovery":true,"effective":{"mode":"work_time"},"notify":{"ciOwner":true},` +
		`"ciMatch":{"label":"instance","by":"attr","attr":"ip","modelCodes":["host"],"enrichTags":["core"]},` +
		`"resourceScope":{"enabled":true,"modelCodes":["host"],"envs":["prod"]},` +
		`"relabel":[{"action":"replace","sourceLabels":["instance"],"regex":"(.+):\\d+","targetLabel":"host"},{"action":"labeldrop","regex":"tmp_.*"}],` +
		`"dropExpr":"{{ if eq $labels.ci_env \"dev\" }}true{{ end }}","topologyInhibit":true,` +
		`"actions":{"ticket":{"enabled":true,"templateId":"` + e2eTemplateID + `","agentUserId":"` + f.aliceID + `"},` +
		`"workflow":{"enabled":true,"definitionKey":"incident","agentUserId":"` + f.aliceID + `"},"aiSummary":true}`
	ruleBody := func(name, fields string) string {
		return strings.Replace(monitorRuleBody(f.groupA, name), `"notifyRecovery":true`, fields, 1)
	}
	resp = f.post(t, "create with P2 fields", "/rule", f.alice, ruleBody("p2-rule", p2), http.StatusOK,
		`"effective":{"timezone":"","windows":[],"mode":"work_time"}`, `"ciOwner":true`,
		`"ciMatch":{"disabled":false,"label":"instance","by":"attr","attr":"ip","modelCodes":["host"],"enrichTags":["core"]}`,
		`"resourceScope":{"enabled":true,"modelCodes":["host"],"envs":["prod"],"tags":[]}`,
		`{"action":"labeldrop","sourceLabels":[],"separator":"","regex":"tmp_.*","targetLabel":"","replacement":""}`,
		`"topologyInhibit":true`, `"aiSummary":true`,
		`"ticketTemplate":{"id":"`+e2eTemplateID+`","name":"故障处理","missing":false}`, `"workflowDefinition":{"id":"incident","name":"故障应急","missing":false}`,
		`"ticketAgent":{"id":"`+f.aliceID+`","name":"p2_alice","missing":false}`)
	ruleID := p2Field(t, resp, "rule", "id")
	f.get(t, "get echoes P2 fields", "/rule/"+ruleID, f.alice, http.StatusOK, `"dropExpr":"{{ if eq $labels.ci_env \"dev\" }}true{{ end }}"`, `"mode":"work_time"`)
	f.get(t, "version snapshot carries P2 fields", "/rule/"+ruleID+"/version/1", f.alice, http.StatusOK, `"topologyInhibit":true`, `"label":"instance"`)
	// 校验：每一项不合法的配置都在保存时拒绝。
	for what, fields := range map[string]string{
		"unregistered match attr":   strings.Replace(p2, `"attr":"ip"`, `"attr":"sn"`, 1),
		"bad relabel regex":         strings.Replace(p2, `tmp_.*`, `tmp_(`, 1),
		"replace without target":    strings.Replace(p2, `,"targetLabel":"host"`, ``, 1),
		"bad drop expr":             strings.Replace(p2, `{{ end }}`, ``, 1),
		"mode with windows":         strings.Replace(p2, `"effective":{"mode":"work_time"}`, `"effective":{"mode":"work_time","windows":[{"days":[1],"start":"09:00","end":"18:00"}]}`, 1),
		"unknown ticket template":   strings.Replace(p2, e2eTemplateID, "018f3e1a-dead-7000-8000-000000000009", 1),
		"unknown workflow":          strings.Replace(p2, `"definitionKey":"incident"`, `"definitionKey":"nope"`, 1),
		"inactive agent":            strings.Replace(p2, f.aliceID, "018f3e1a-dead-7000-8000-00000000000a", 2),
		"unknown escalation":        p2 + `,"escalationId":"018f3e1a-dead-7000-8000-00000000000b"`,
		"scope values but disabled": strings.Replace(p2, `"resourceScope":{"enabled":true`, `"resourceScope":{"enabled":false`, 1),
	} {
		f.post(t, what, "/rule", f.alice, ruleBody("bad", fields), http.StatusBadRequest)
	}
	lacks(t, "nothing invalid was saved", f.get(t, "list", "/rule?page=1&pageSize=50", f.alice, http.StatusOK), `"name":"bad"`)

	// 模拟触发：要求对规则所在分组可管理。
	ds := p2Field(t, f.post(t, "datasource", "/datasource", f.admin,
		`{"spec":{"name":"vm","connection":{"url":"http://vm:8428","authType":1,"timeoutMs":5000},"enabled":true}}`, http.StatusOK), "datasource", "id")
	simBody := func(groupID string, send bool, notify string) string {
		spec := strings.Replace(monitorRuleBody(groupID, "sim"), `"notifyRecovery":true`, `"notifyRecovery":true`+notify, 1)
		s := "false"
		if send {
			s = "true"
		}
		return strings.TrimSuffix(spec, "}") + `,"datasourceId":"` + ds + `","sampleLabels":{"instance":"web-01"},"sampleValue":97.5,"send":` + s + `}`
	}
	f.post(t, "read-only simulates", "/rule:simulate", f.bob, simBody(f.groupB, false, ""), http.StatusForbidden, "MONITOR_FORBIDDEN")
	f.post(t, "out of scope simulates", "/rule:simulate", f.alice, simBody(f.groupB, false, ""), http.StatusForbidden, "MONITOR_FORBIDDEN")
	f.post(t, "nobody simulates", "/rule:simulate", f.nobody, simBody(f.groupA, false, ""), http.StatusForbidden, "MONITOR_FORBIDDEN")
	f.post(t, "unknown datasource", "/rule:simulate", f.alice, strings.Replace(simBody(f.groupA, false, ""), ds, "018f3e1a-dead-7000-8000-00000000000c", 1),
		http.StatusBadRequest, "MONITOR_SIMULATE_INVALID")
	f.post(t, "invalid spec", "/rule:simulate", f.alice, strings.Replace(simBody(f.groupA, false, ""), "up == 0", "up ==", 1), http.StatusBadRequest, "MONITOR_RULE_INVALID")

	// 干跑：查询没有结果 → 用样例标签合成事件；逐阶段给结论；不写任何通知事件、不产生告警。
	notify := `,"notify":{"userIds":["` + f.bobID + `"]}`
	resp = f.post(t, "dry run", "/rule:simulate", f.alice, simBody(f.groupA, false, notify), http.StatusOK,
		`"synthetic":true`, `"inEffect":true`, `"eventTotal":1`, `"outcome":"notify"`, `"sent":false`, `"notifyEventIds":[]`, `"routeMatched":true`,
		`"recipients":[{"id":"`+f.bobID+`","name":"p2_bob","missing":false}]`, `"labels":{"instance":"web-01"}`)
	stages := []string{"query", "effective", "relabel", "ci", "drop", "scope", "level_inhibit", "inhibit", "mute", "notify", "send"}
	last := -1
	for _, st := range stages {
		i := strings.Index(resp, `"stage":"`+st+`"`)
		if i <= last {
			t.Fatalf("stage %q missing or out of order: %s", st, resp)
		}
		last = i
	}
	if n := len(monitorTestEvents(t, f)); n != 0 {
		t.Fatalf("dry run wrote %d notify events", n)
	}
	// 真实发送：恰好一条，标题带 [TEST]，只发给解析出的通知对象。
	resp = f.post(t, "real send", "/rule:simulate", f.alice, simBody(f.groupA, true, notify), http.StatusOK, `"sent":true`, `"outcome":"notify"`)
	events := monitorTestEvents(t, f)
	if len(events) != 1 || !strings.HasPrefix(events[0].Title, "[TEST] sim") || events[0].EventType != mc.EventAlertFiring ||
		!slices.Equal(events[0].Recipients, []string{f.bobID}) || !strings.Contains(string(events[0].Payload), `"test":true`) {
		t.Fatalf("test notification = %+v", events)
	}
	if !strings.Contains(resp, `"notifyEventIds":["`+events[0].ID+`"]`) {
		t.Fatalf("response must carry the notify event id %s: %s", events[0].ID, resp)
	}
	// 被屏蔽的事件即使 send=true 也不发。
	muteBody := `{"spec":{"groupId":"` + f.groupA + `","name":"维护","reason":"变更","mode":1,"timeType":1,"enabled":true,` +
		`"startTime":"` + f.now.Add(-time.Hour).UTC().Format(time.RFC3339) + `","endTime":"` + f.now.Add(time.Hour).UTC().Format(time.RFC3339) + `"}}`
	f.post(t, "mute group A", "/mute", f.alice, muteBody, http.StatusOK)
	f.post(t, "muted event is not sent", "/rule:simulate", f.alice, simBody(f.groupA, true, notify), http.StatusOK, `"outcome":"muted_notify"`, `"sent":false`)
	if n := len(monitorTestEvents(t, f)); n != 1 {
		t.Fatalf("notify events = %d, want still 1", n)
	}
	// 模拟触发不产生告警事件、不写时间线。
	f.get(t, "no alert was created", "/alert?page=1&pageSize=20&keyword=sim", f.alice, http.StatusOK, `"total":"0"`)
	if n := len(f.h.ops.Events()); n != 0 {
		t.Fatalf("simulate wrote %d timeline entries", n)
	}
}

// p2NewOps 第二、三期新增的全部接口：操作名 → 一次合法形状的请求。
func p2NewOps() map[string][3]string {
	const id = "018f3e1a-dead-7000-8000-0000000000ff"
	rng := "startTime=" + rfc(time.Now().Add(-time.Hour)) + "&endTime=" + rfc(time.Now())
	esc := `{"spec":{"name":"x","mode":1,"minSeverity":1,"levels":[{"trigger":1,"afterM":5,"targets":{"userIds":["` + id + `"]}}]}}`
	inh := `{"spec":{"name":"x","source":{"severities":[4]},"equalLabels":["dc"]}}`
	rec := `{"spec":{"groupId":"` + id + `","name":"a:b","datasourceId":"` + id + `","expr":"up","intervalS":60}}`
	view := `{"spec":{"name":"x","by":"rule"}}`
	alert, esvc, isvc, rsvc, vsvc, ssvc := "/monitor.v1.MonitorAlertService/", "/monitor.v1.MonitorEscalationService/", "/monitor.v1.MonitorInhibitService/",
		"/monitor.v1.MonitorRecordRuleService/", "/monitor.v1.MonitorAlertViewService/", "/monitor.v1.MonitorStatsService/"
	return map[string][3]string{
		alert + "AckAlert":          {http.MethodPost, "/alert/" + id + ":ack", `{}`},
		alert + "UnackAlert":        {http.MethodPost, "/alert/" + id + ":unack", `{}`},
		alert + "TransferAlert":     {http.MethodPost, "/alert/" + id + ":transfer", `{"userId":"` + id + `"}`},
		alert + "CommentAlert":      {http.MethodPost, "/alert/" + id + ":comment", `{"text":"x"}`},
		alert + "CloseAlert":        {http.MethodPost, "/alert/" + id + ":close", `{}`},
		alert + "BatchAckAlerts":    {http.MethodPost, "/alert:batch-ack", `{"ids":["` + id + `"]}`},
		alert + "BatchCloseAlerts":  {http.MethodPost, "/alert:batch-close", `{"ids":["` + id + `"]}`},
		alert + "CreateAlertTicket": {http.MethodPost, "/alert/" + id + ":ticket", `{"templateId":"` + id + `"}`},

		"/monitor.v1.MonitorRuleService/ListRuleTemplatePacks":  {http.MethodGet, "/rule-template", ""},
		"/monitor.v1.MonitorRuleService/GetRuleTemplatePack":    {http.MethodGet, "/rule-template/mysql", ""},
		"/monitor.v1.MonitorRuleService/SimulateRule":           {http.MethodPost, "/rule:simulate", strings.TrimSuffix(monitorRuleBody(id, "x"), "}") + `,"datasourceId":"` + id + `"}`},
		"/monitor.v1.MonitorRuleService/ListMonitorRuleOptions": {http.MethodGet, "/rule-option", ""},

		esvc + "ListEscalations":        {http.MethodGet, "/escalation?page=1&pageSize=10", ""},
		esvc + "GetEscalation":          {http.MethodGet, "/escalation/" + id, ""},
		esvc + "CreateEscalation":       {http.MethodPost, "/escalation", esc},
		esvc + "UpdateEscalation":       {http.MethodPut, "/escalation/" + id, withVersion(esc, "1")},
		esvc + "DeleteEscalation":       {http.MethodDelete, "/escalation/" + id, ""},
		esvc + "EstimateEscalation":     {http.MethodPost, "/escalation:estimate", esc},
		esvc + "ListEscalationOptions":  {http.MethodGet, "/escalation-option?groupId=" + id, ""},
		esvc + "ListOncallGroupOptions": {http.MethodGet, "/oncall-group-option", ""},

		isvc + "ListInhibits":   {http.MethodGet, "/inhibit?page=1&pageSize=10", ""},
		isvc + "GetInhibit":     {http.MethodGet, "/inhibit/" + id, ""},
		isvc + "CreateInhibit":  {http.MethodPost, "/inhibit", inh},
		isvc + "UpdateInhibit":  {http.MethodPut, "/inhibit/" + id, withVersion(inh, "1")},
		isvc + "DeleteInhibit":  {http.MethodDelete, "/inhibit/" + id, ""},
		isvc + "PreviewInhibit": {http.MethodPost, "/inhibit:preview", inh},

		rsvc + "ListRecordRules":    {http.MethodGet, "/record-rule?page=1&pageSize=10", ""},
		rsvc + "GetRecordRule":      {http.MethodGet, "/record-rule/" + id, ""},
		rsvc + "CreateRecordRule":   {http.MethodPost, "/record-rule", rec},
		rsvc + "UpdateRecordRule":   {http.MethodPut, "/record-rule/" + id, withVersion(rec, "1")},
		rsvc + "DeleteRecordRule":   {http.MethodDelete, "/record-rule/" + id, ""},
		rsvc + "ToggleRecordRule":   {http.MethodPost, "/record-rule/" + id + ":toggle", `{"enabled":true}`},
		rsvc + "ValidateRecordRule": {http.MethodPost, "/record-rule:validate", rec},

		vsvc + "ListAlertViews":  {http.MethodGet, "/alert-view", ""},
		vsvc + "CreateAlertView": {http.MethodPost, "/alert-view", view},
		vsvc + "UpdateAlertView": {http.MethodPut, "/alert-view/" + id, view},
		vsvc + "DeleteAlertView": {http.MethodDelete, "/alert-view/" + id, ""},

		ssvc + "GetAlertOverview":   {http.MethodGet, "/stats/overview", ""},
		ssvc + "GetAlertTrend":      {http.MethodGet, "/stats/trend?" + rng, ""},
		ssvc + "GetAlertTop":        {http.MethodGet, "/stats/top?by=rule&" + rng, ""},
		ssvc + "GetAlertEfficiency": {http.MethodGet, "/stats/efficiency?" + rng, ""},
		ssvc + "GetAlertNoise":      {http.MethodGet, "/stats/noise?" + rng, ""},

		"/monitor.v1.MonitorEngineService/GetEngineStatus": {http.MethodGet, "/engine/status", ""},
	}
}

// 第二、三期的每个新接口：都在可授权操作清单里、都登记在权限目录的某个菜单 / 元素下；没有该接口授权的用户
// 一律被授权中间件拦下（403，不是监控域的错误码），只授了这一个接口的用户能通过中间件。
func TestMonitorP2_NewOperationsAuthz(t *testing.T) {
	t.Parallel()
	ts, svc := newTestHTTPServerWithServices(t)
	admin := testBearer(t, svc.signer, e2eActorID)
	ops := p2NewOps()

	// 表覆盖了全部新接口：六个新服务的全部方法 + 两个已有服务的新方法（免授权的班次告警数除外）。
	authorizable := monitorOps()
	newServices := []string{"MonitorEscalationService", "MonitorInhibitService", "MonitorRecordRuleService", "MonitorAlertViewService", "MonitorStatsService", "MonitorEngineService"}
	for _, op := range authorizable {
		svcName := strings.Split(strings.TrimPrefix(op, "/monitor.v1."), "/")[0]
		if _, ok := ops[op]; !ok && slices.Contains(newServices, svcName) {
			t.Errorf("operation %s of a new service is not covered by this test", op)
		}
	}
	catalog := map[string]bool{}
	for _, m := range biz.Catalog() {
		for _, op := range m.DefaultOps {
			catalog[op] = true
		}
		for _, e := range m.Elements {
			for _, op := range e.Ops {
				catalog[op] = true
			}
		}
	}
	for op := range ops {
		if !slices.Contains(authorizable, op) {
			t.Errorf("%s is not an authorizable operation (service not registered in operationFiles?)", op)
		}
		if !catalog[op] {
			t.Errorf("%s is missing from the permission catalog", op)
		}
	}
	// 免授权的两个接口不在目录里也不在可授权清单里。
	for _, op := range []string{"/monitor.v1.MonitorAlertService/ListShiftAlertCounts", "/monitor.v1.MonitorAlertService/GetAlert"} {
		if !service.IsAuthzExempt(op) || slices.Contains(authorizable, op) || catalog[op] {
			t.Errorf("%s must be authz-exempt and absent from the authorizable list and the catalog", op)
		}
	}
	// 目录里登记的监控接口都真实存在（防止笔误）。
	for op := range catalog {
		if strings.HasPrefix(op, "/monitor.v1.") && !slices.Contains(authorizable, op) {
			t.Errorf("catalog lists %s, which is not an authorizable operation", op)
		}
	}
	if t.Failed() {
		return
	}

	// 有别的监控接口权限、唯独没有被测的这些：中间件 403。
	outsider, _, _ := monitorUser(t, ts, svc, admin, "p2_outsider", []string{"/monitor.v1.MonitorRuleService/ListRules"})
	// 只授了全部新接口：能通过中间件（之后是什么结果由域决定，但不会是中间件的 403）。
	names := make([]string, 0, len(ops))
	for op := range ops {
		names = append(names, op)
	}
	slices.Sort(names)
	insider, _, _ := monitorUser(t, ts, svc, admin, "p2_insider", names)
	const middlewareReason = `"reason":"PERMISSION_DENIED"`
	for _, op := range names {
		req := ops[op]
		code, body := doRequestWithToken(t, req[0], ts.URL+monitorBase+req[1], outsider, req[2])
		if code != http.StatusForbidden || !strings.Contains(body, middlewareReason) {
			t.Errorf("%s without the grant: %d %s, want 403 from the authz middleware", op, code, body)
		}
		if code, body := doRequestWithToken(t, req[0], ts.URL+monitorBase+req[1], "", req[2]); code != http.StatusUnauthorized {
			t.Errorf("%s anonymous: %d %s, want 401", op, code, body)
		}
		code, body = doRequestWithToken(t, req[0], ts.URL+monitorBase+req[1], insider, req[2])
		if code == http.StatusUnauthorized || code == http.StatusNotImplemented || code >= 500 ||
			(code == http.StatusForbidden && !strings.Contains(body, "MONITOR_FORBIDDEN")) {
			t.Errorf("%s with the grant: %d %s, want it to reach the domain", op, code, body)
		}
	}
}
