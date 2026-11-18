package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	bizmonitor "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

const monitorBase = "/api/v1/monitor"

// p2Fixture 第二、三期 e2e 的公共场景：两个分组，alice 对 A 可管理、bob 对 B 只读、nobody 有全部接口权限
// 但没有任何分组授权；A、B 各有一条规则与一条告警中的事件。
type p2Fixture struct {
	ts     *httptest.Server
	svc    testServices
	h      *monitorP2
	admin  string
	alice  string
	bob    string
	nobody string
	// 用户 ID 与角色 ID。
	adminID, aliceID, bobID, nobodyID string
	roleA, roleB                      string
	groupA, groupB                    string
	ruleA, ruleB                      string
	alertA, alertB                    string
	now                               time.Time
}

const (
	p2AlertA = "018f3e1a-bbbb-7000-8000-00000000000a"
	p2AlertB = "018f3e1a-bbbb-7000-8000-00000000000b"
)

func newP2Fixture(t *testing.T) *p2Fixture {
	t.Helper()
	ts, svc := newTestHTTPServerWithServices(t)
	f := &p2Fixture{ts: ts, svc: svc, h: monitorP2Of(svc.monitor.store), adminID: e2eActorID, now: time.Now()}
	f.admin = testBearer(t, svc.signer, e2eActorID)
	f.groupA = monitorID(t, ts, http.MethodPost, monitorBase+"/rule-group", f.admin, `{"name":"team-a"}`, "group")
	f.groupB = monitorID(t, ts, http.MethodPost, monitorBase+"/rule-group", f.admin, `{"name":"team-b"}`, "group")
	// 另加值班日历的接口权限不在此列：班次告警数的测试单独授权。
	ops := monitorOps()
	f.alice, f.aliceID, f.roleA = monitorUser(t, ts, svc, f.admin, "p2_alice", ops)
	f.bob, f.bobID, f.roleB = monitorUser(t, ts, svc, f.admin, "p2_bob", ops)
	f.nobody, f.nobodyID, _ = monitorUser(t, ts, svc, f.admin, "p2_nobody", ops)
	f.grant(t, f.groupA, f.roleA, mc.ScopeManage)
	f.grant(t, f.groupB, f.roleB, mc.ScopeRead)
	f.ruleA = monitorID(t, ts, http.MethodPost, monitorBase+"/rule", f.alice, monitorRuleBody(f.groupA, "cpu-high"), "rule")
	f.ruleB = monitorID(t, ts, http.MethodPost, monitorBase+"/rule", f.admin, monitorRuleBody(f.groupB, "disk-full"), "rule")
	f.alertA, f.alertB = p2AlertA, p2AlertB
	f.seedAlert(f.alertA, f.ruleA, "cpu-high", f.groupA, 3, nil)
	f.seedAlert(f.alertB, f.ruleB, "disk-full", f.groupB, 2, nil)
	return f
}

func (f *p2Fixture) grant(t *testing.T, groupID, roleID string, level int32) {
	t.Helper()
	body := `{"groupId":"` + groupID + `","items":[{"roleId":"` + roleID + `","level":` + strconv.Itoa(int(level)) + `}]}`
	expectStatus(t, "grant scope", f.ts, http.MethodPut, monitorBase+"/role-scope", f.admin, body, http.StatusOK)
}

// seedAlert 预置一条告警中的事件；mutate 可改写其余字段。
func (f *p2Fixture) seedAlert(id, ruleID, ruleName, groupID string, severity int32, mutate func(a *bizmonitor.Alert)) {
	a := &bizmonitor.Alert{
		ID: id, Fingerprint: strings.Repeat(id[len(id)-1:], 32), RuleID: ruleID, RuleName: ruleName, GroupID: groupID, Severity: severity,
		Status: mc.AlertFiring, Labels: map[string]string{"instance": "host-" + id[len(id)-1:]},
		FirstFireTime: f.now.Add(-10 * time.Minute), LastFireTime: f.now,
	}
	if mutate != nil {
		mutate(a)
	}
	f.svc.monitor.store.SeedAlert(a)
}

// notified 预置一条通知记录：userID 是事件的通知接收方。
func (f *p2Fixture) notified(logID, alertID, userID string, at time.Time) {
	f.svc.monitor.store.SeedNotifyLog(&bizmonitor.NotifyLog{
		ID: logID, AlertID: alertID, Kind: mc.NotifyKindFiring, Seq: 1, Source: mc.NotifySourceRule, Recipients: []string{userID}, CreateTime: at,
	})
}

// p2Do 发起请求并断言状态码与片段。响应是 protojson（键与值之间的空白不固定），先压紧再比对，片段可以
// 跨多个字段书写。返回压紧后的响应体。
func p2Do(t *testing.T, what string, ts *httptest.Server, method, path, bearer, body string, want int, contains ...string) string {
	t.Helper()
	code, resp := doRequestWithToken(t, method, ts.URL+path, bearer, body)
	var buf bytes.Buffer
	if err := json.Compact(&buf, []byte(resp)); err == nil {
		resp = buf.String()
	}
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

func (f *p2Fixture) post(t *testing.T, what, path, bearer, body string, want int, contains ...string) string {
	t.Helper()
	return p2Do(t, what, f.ts, http.MethodPost, monitorBase+path, bearer, body, want, contains...)
}

func (f *p2Fixture) get(t *testing.T, what, path, bearer string, want int, contains ...string) string {
	t.Helper()
	return p2Do(t, what, f.ts, http.MethodGet, monitorBase+path, bearer, "", want, contains...)
}

func (f *p2Fixture) put(t *testing.T, what, path, bearer, body string, want int, contains ...string) string {
	t.Helper()
	return p2Do(t, what, f.ts, http.MethodPut, monitorBase+path, bearer, body, want, contains...)
}

func (f *p2Fixture) del(t *testing.T, what, path, bearer string, want int, contains ...string) string {
	t.Helper()
	return p2Do(t, what, f.ts, http.MethodDelete, monitorBase+path, bearer, "", want, contains...)
}

func lacks(t *testing.T, what, body string, fragments ...string) {
	t.Helper()
	for _, frag := range fragments {
		if strings.Contains(body, frag) {
			t.Fatalf("%s: body must not contain %q: %s", what, frag, body)
		}
	}
}

// 事件操作的数据范围：认领 / 取消认领 / 转交 / 备注要求只读，关闭 / 转工单要求可管理；范围外且不是通知
// 接收方 → 404（不泄露事件是否存在）；只是通知接收方 → 403；状态冲突 → 409。
func TestMonitorP2_AlertActions(t *testing.T) {
	t.Parallel()
	f := newP2Fixture(t)
	a, b := "/alert/"+f.alertA, "/alert/"+f.alertB

	// 范围外：nobody 与 bob 对 A 组的事件一律 404。
	for _, c := range []struct{ what, path, body string }{
		{"ack", a + ":ack", `{}`},
		{"unack", a + ":unack", `{}`},
		{"transfer", a + ":transfer", `{"userId":"` + f.aliceID + `"}`},
		{"comment", a + ":comment", `{"text":"x"}`},
		{"close", a + ":close", `{"reason":"x"}`},
		{"ticket", a + ":ticket", `{"templateId":"` + e2eTemplateID + `"}`},
	} {
		f.post(t, "nobody "+c.what, c.path, f.nobody, c.body, http.StatusNotFound, "MONITOR_ALERT_NOT_FOUND")
		f.post(t, "bob (out of scope) "+c.what, c.path, f.bob, c.body, http.StatusNotFound, "MONITOR_ALERT_NOT_FOUND")
	}
	// 只是通知接收方：能看这一条事件，但不能做任何操作（403）。
	f.notified("018f3e1a-cccc-7000-8000-000000000001", f.alertA, f.bobID, f.now)
	f.get(t, "recipient views alert", a, f.bob, http.StatusOK, `"canAck":false`, `"canManage":false`)
	for _, c := range []struct{ what, path, body string }{
		{"ack", a + ":ack", `{}`},
		{"unack", a + ":unack", `{}`},
		{"transfer", a + ":transfer", `{"userId":"` + f.aliceID + `"}`},
		{"comment", a + ":comment", `{"text":"x"}`},
		{"close", a + ":close", `{}`},
		{"ticket", a + ":ticket", `{"templateId":"` + e2eTemplateID + `"}`},
	} {
		f.post(t, "recipient "+c.what, c.path, f.bob, c.body, http.StatusForbidden, "MONITOR_FORBIDDEN")
	}
	if n := len(f.h.ops.Events()) + len(f.h.ops.Cmds()); n != 0 {
		t.Fatalf("refused actions must not write anything, got %d rows", n)
	}

	// 只读（bob 对 B）：可以认领、备注、取消认领；不能关闭、不能转工单。
	f.post(t, "read-only ack", b+":ack", f.bob, `{}`, http.StatusOK, `"ackUserId":"`+f.bobID+`"`, `"handleState":2`, `"canAck":true`, `"canManage":false`)
	f.post(t, "ack again is a no-op", b+":ack", f.bob, `{}`, http.StatusOK, `"ackUserId":"`+f.bobID+`"`)
	f.post(t, "read-only comment", b+":comment", f.bob, `{"text":"  正在看  "}`, http.StatusOK, `"type":"comment"`, `"text":"正在看"`)
	f.post(t, "blank comment", b+":comment", f.bob, `{"text":"   "}`, http.StatusBadRequest, "MONITOR_ALERT_ACTION_INVALID")
	f.post(t, "read-only close", b+":close", f.bob, `{"reason":"x"}`, http.StatusForbidden, "MONITOR_FORBIDDEN")
	f.post(t, "read-only ticket", b+":ticket", f.bob, `{"templateId":"`+e2eTemplateID+`"}`, http.StatusForbidden, "MONITOR_FORBIDDEN")
	// 已被他人认领：409，带认领人。
	resp := f.post(t, "admin acks an acked alert", b+":ack", f.admin, `{}`, http.StatusConflict, "MONITOR_ALERT_ALREADY_ACKED")
	if !strings.Contains(resp, `"ack_user_id":"`+f.bobID+`"`) {
		t.Fatalf("ALREADY_ACKED must carry ack_user_id metadata: %s", resp)
	}
	// 转交：对象须是可用用户、不能是当前认领人；本人可以把自己认领的转出去。
	f.post(t, "transfer to self (current acker)", b+":transfer", f.bob, `{"userId":"`+f.bobID+`"}`, http.StatusBadRequest, "MONITOR_ALERT_ACTION_INVALID")
	f.post(t, "transfer to unknown user", b+":transfer", f.bob, `{"userId":"018f3e1a-dead-7000-8000-000000000001"}`, http.StatusBadRequest, "MONITOR_ALERT_ACTION_INVALID")
	f.post(t, "acker transfers", b+":transfer", f.bob, `{"userId":"`+f.aliceID+`","note":"交接"}`, http.StatusOK, `"ackUserId":"`+f.aliceID+`"`)
	// 现在认领人是 alice（对 B 没有范围）：bob 只读，取消别人的认领需要可管理。
	f.post(t, "read-only unacks someone else's ack", b+":unack", f.bob, `{}`, http.StatusForbidden, "MONITOR_FORBIDDEN")
	f.post(t, "read-only transfers someone else's ack", b+":transfer", f.bob, `{"userId":"`+f.bobID+`"}`, http.StatusForbidden, "MONITOR_FORBIDDEN")
	f.post(t, "admin unacks", b+":unack", f.admin, `{}`, http.StatusOK, `"ackUserId":""`, `"handleState":1`)
	f.post(t, "unack again is a no-op", b+":unack", f.admin, `{}`, http.StatusOK, `"ackUserId":""`)

	// 可管理（alice 对 A）：关闭是异步的——写一条指令，事件仍是告警中、closing=true；重复下达是空操作。
	f.post(t, "manager closes", a+":close", f.alice, `{"reason":"机器下线"}`, http.StatusOK, `"pending":true`, `"closing":true`, `"status":1`)
	f.post(t, "close again", a+":close", f.alice, `{"reason":"机器下线"}`, http.StatusOK, `"pending":true`)
	if cmds := f.h.ops.Cmds(); len(cmds) != 1 || cmds[0].AlertID != f.alertA || cmds[0].Operator != f.aliceID || cmds[0].Reason != "机器下线" {
		t.Fatalf("close must queue exactly one command, got %+v", cmds)
	}
	// 已不在告警中：认领 / 关闭 409，备注仍然可以。
	resolved := "018f3e1a-bbbb-7000-8000-00000000000c"
	f.seedAlert(resolved, f.ruleA, "cpu-high", f.groupA, 3, func(al *bizmonitor.Alert) {
		at := f.now
		al.Status, al.ResolveTime = mc.AlertResolved, &at
	})
	f.post(t, "ack resolved", "/alert/"+resolved+":ack", f.alice, `{}`, http.StatusConflict, "MONITOR_ALERT_NOT_FIRING")
	f.post(t, "close resolved", "/alert/"+resolved+":close", f.alice, `{}`, http.StatusConflict, "MONITOR_ALERT_NOT_FIRING")
	f.post(t, "comment resolved", "/alert/"+resolved+":comment", f.alice, `{"text":"复盘"}`, http.StatusOK, `"type":"comment"`)

	// 批量：逐条判定。alice 对 A 可管理、对 B 没有范围；范围外的那条按不存在报告，不影响其余。
	resp = f.post(t, "batch ack", "/alert:batch-ack", f.alice, `{"ids":["`+f.alertA+`","`+f.alertB+`","`+resolved+`"]}`,
		http.StatusOK, `"succeeded":1`, `"failed":2`, "MONITOR_ALERT_NOT_FOUND", "MONITOR_ALERT_NOT_FIRING")
	var batch struct {
		Results []struct {
			ID     string `json:"id"`
			OK     bool   `json:"ok"`
			Reason string `json:"reason"`
		} `json:"results"`
	}
	if err := json.Unmarshal([]byte(resp), &batch); err != nil || len(batch.Results) != 3 ||
		batch.Results[0].ID != f.alertA || !batch.Results[0].OK || batch.Results[1].ID != f.alertB || batch.Results[1].OK {
		t.Fatalf("batch results must follow the request order: %s", resp)
	}
	f.post(t, "read-only batch close", "/alert:batch-close", f.bob, `{"ids":["`+f.alertB+`","`+f.alertA+`"],"reason":"x"}`,
		http.StatusOK, `"succeeded":0`, `"failed":2`, "MONITOR_FORBIDDEN")
	f.post(t, "admin batch close", "/alert:batch-close", f.admin, `{"ids":["`+f.alertB+`"],"reason":"清理"}`, http.StatusOK, `"succeeded":1`, `"failed":0`)
	if cmds := f.h.ops.Cmds(); len(cmds) != 2 {
		t.Fatalf("commands = %+v, want one per closed alert", cmds)
	}

	// 转工单：可管理；提单人 = 调用方；事件已关联工单时 409 带工单 ID；同一幂等键重放返回同一张。
	ticketBody := `{"templateId":"` + e2eTemplateID + `","requestId":"018f3e1a-eeee-7000-8000-000000000001"}`
	f.post(t, "manager creates ticket", a+":ticket", f.alice, ticketBody, http.StatusOK, `"number":"INC-`, `"auto":false`, `"canView":true`, `"ticketId":"018f3e1a-0e12-`)
	tickets := f.h.cross.ticketList()
	if len(tickets) != 1 || tickets[0].info.Creator != f.aliceID || !strings.Contains(tickets[0].info.Title, "cpu-high") || len(tickets[0].resources) != 0 {
		t.Fatalf("ticket = %+v, want one ticket created by the caller without resources", tickets)
	}
	// 同一幂等键重放（提单人本人）：返回首次创建的工单，不再建一张。
	if code, body := doRequestWithToken(t, http.MethodPost, f.ts.URL+monitorBase+a+":ticket", f.alice, ticketBody); code != http.StatusOK ||
		!strings.Contains(body, tickets[0].info.ID) {
		t.Fatalf("idempotent replay: %d %s", code, body)
	}
	resp = f.post(t, "second ticket", a+":ticket", f.admin, `{"templateId":"`+e2eTemplateID+`"}`, http.StatusConflict, "MONITOR_ALERT_TICKET_EXISTS")
	if !strings.Contains(resp, `"ticket_id":"`+tickets[0].info.ID+`"`) {
		t.Fatalf("TICKET_EXISTS must carry ticket_id metadata: %s", resp)
	}
	if n := len(f.h.cross.ticketList()); n != 1 {
		t.Fatalf("tickets = %d, want 1 (replay and conflict must not create more)", n)
	}
	var ticketEvents int
	for _, ev := range f.h.ops.Events() {
		if ev.Type == mc.AlertEventTicket {
			ticketEvents++
			if ev.Operator != f.aliceID || ev.Detail.TicketID != tickets[0].info.ID || ev.Detail.Auto {
				t.Fatalf("ticket timeline entry = %+v", ev)
			}
		}
	}
	if ticketEvents != 1 {
		t.Fatalf("ticket timeline entries = %d, want 1", ticketEvents)
	}
}

// 事件详情的第二、三期内容：时间线（时间线条目与通知记录合并）、资源卡片、影响面、工单卡片——完整视图与
// 收件人视图各自看到什么；调用方在 CMDB 里看不到的资源只给 ID 与名称。以及列表 / 聚合的新过滤条件。
func TestMonitorP2_AlertDetailAndFilters(t *testing.T) {
	t.Parallel()
	f := newP2Fixture(t)
	const ciHost, ciVM = "018f3e1a-0c1a-7000-8000-000000000001", "018f3e1a-0c1a-7000-8000-000000000002"
	f.h.sync.SeedCI(&bizmonitor.CIEntry{ID: ciHost, ModelCode: "host", Name: "web-01", Env: "prod", Status: "active", Valid: true})
	f.h.sync.SeedCI(&bizmonitor.CIEntry{ID: ciVM, ModelCode: "vm", Name: "vm-01", Env: "prod", Status: "active", Valid: true})
	f.h.cross.set(func(c *monitorCross) {
		c.cards[ciHost] = &bizmonitor.CICard{ID: ciHost, Name: "web-01", ModelCode: "host", ModelName: "主机", Env: "prod", Status: "active", Owner: "ops-team", Tags: []string{"core"}}
		c.impact[ciHost] = []bizmonitor.CIImpact{{ID: ciVM, Name: "vm-01", ModelCode: "vm", ModelName: "虚拟机", Depth: 1}}
	})
	// A 组的事件关联到主机；另一条 A 组事件关联到下游虚拟机（影响面的活跃告警数）。
	f.svc.monitor.store.SetAlert(f.alertA, func(a *bizmonitor.Alert) { a.CIID = ciHost })
	vmAlert := "018f3e1a-bbbb-7000-8000-00000000000d"
	f.seedAlert(vmAlert, f.ruleA, "cpu-high", f.groupA, 4, func(a *bizmonitor.Alert) { a.CIID, a.Inhibited, a.InhibitKind = ciVM, true, mc.InhibitTopology })
	f.h.ops.SeedEvent(&bizmonitor.AlertEvent{ID: "018f3e1a-dddd-7000-8000-000000000001", AlertID: f.alertA, Type: mc.AlertEventFire, Detail: mc.AlertEventDetail{Severity: 3}, CreateTime: f.now.Add(-10 * time.Minute)})
	f.notified("018f3e1a-cccc-7000-8000-000000000002", f.alertA, f.bobID, f.now.Add(-9*time.Minute))
	a := "/alert/" + f.alertA
	f.post(t, "alice acks", a+":ack", f.alice, `{}`, http.StatusOK)
	f.post(t, "alice comments", a+":comment", f.alice, `{"text":"内部备注"}`, http.StatusOK)
	f.post(t, "alice creates ticket", a+":ticket", f.alice, `{"templateId":"`+e2eTemplateID+`"}`, http.StatusOK)

	// 完整视图，但调用方在 CMDB 里看不到这台主机：资源卡片只有 ID 与名称；影响面的节点同样只有 ID / 名称 / 层级。
	resp := f.get(t, "alice detail (no cmdb access)", a, f.alice, http.StatusOK,
		`"type":"fire"`, `"type":"notification"`, `"type":"ack"`, `"type":"comment"`, `"type":"ticket"`, `"text":"内部备注"`,
		`"ci":{"id":"`+ciHost+`","name":"web-01","modelCode":"","modelName":"","env":"","status":"","owner":"","ownerId":"","tags":[],"exists":true,"canView":false`,
		`"ciName":"web-01"`, `"impact":[{"id":"`+ciVM+`","name":"vm-01","modelCode":"","modelName":"","depth":1,"firingCount":1,"canView":false}]`,
		`"ticket":{"id":"018f3e1a-0e12-`, `"canView":true`, `"ackUserName":"`)
	lacks(t, "hidden ci card", resp, `"env":"prod"`, "ops-team")
	// 时间线按时间升序：触发 → 通知 → 认领 …
	if i, j, k := strings.Index(resp, `"type":"fire"`), strings.Index(resp, `"type":"notification"`), strings.Index(resp, `"type":"ack"`); i > j || j > k {
		t.Fatalf("timeline must be ordered by time: %s", resp)
	}
	// 调用方能在 CMDB 里看到主机之后：完整卡片；下游虚拟机仍然看不到。
	f.h.cross.set(func(c *monitorCross) { c.ciViewers[ciHost] = []string{f.aliceID} })
	f.get(t, "alice detail (cmdb access)", a, f.alice, http.StatusOK, `"modelName":"主机"`, `"env":"prod"`, `"owner":"ops-team"`, `"tags":["core"]`, `"canView":true`,
		`"impact":[{"id":"`+ciVM+`","name":"vm-01","modelCode":"","modelName":"","depth":1,"firingCount":1,"canView":false}]`)

	// 收件人视图（bob 对 A 没有范围，只是通知接收方）：只有触发与发给本人的通知；没有备注、认领人操作、
	// 影响面；资源只有 ID 与名称；工单不可查看时没有标题。
	resp = f.get(t, "recipient detail", a, f.bob, http.StatusOK, `"type":"fire"`, `"type":"notification"`, `"canViewRule":false`,
		`"ci":{"id":"`+ciHost+`","name":"web-01","modelCode":""`, `"impact":[]`)
	lacks(t, "recipient detail", resp, "内部备注", `"type":"comment"`, `"type":"ack"`, `"type":"ticket"`, `"env":"prod"`, "ops-team", ciVM)
	if !strings.Contains(resp, `"ticket":{`) || !strings.Contains(resp, `"title":"","status":0`) || !strings.Contains(resp, `"canView":false}`) {
		t.Fatalf("recipient must not see the ticket's title: %s", resp)
	}
	// 范围外且不是接收方：404。
	f.get(t, "nobody detail", a, f.nobody, http.StatusNotFound, "MONITOR_ALERT_NOT_FOUND")

	// 列表的新过滤条件（都叠加在数据范围之上）。
	list := func(what, bearer, query string, want int, contains ...string) string {
		t.Helper()
		return f.get(t, what, "/alert?page=1&pageSize=20&"+query, bearer, want, contains...)
	}
	list("by ci", f.alice, "ciId="+ciHost, http.StatusOK, `"total":"1"`, f.alertA)
	list("by ci, out of scope", f.bob, "ciId="+ciHost, http.StatusOK, `"total":"0"`)
	list("acked", f.alice, "acked=true", http.StatusOK, `"total":"1"`, f.alertA)
	list("ack user", f.admin, "ackUserId="+f.aliceID, http.StatusOK, `"total":"1"`, f.alertA)
	list("inhibited", f.alice, "inhibited=true", http.StatusOK, `"total":"1"`, vmAlert, `"handleState":4`)
	list("handle state: acked", f.alice, "handleState=2", http.StatusOK, `"total":"1"`, f.alertA)
	list("handle state: inhibited", f.alice, "handleState=4", http.StatusOK, `"total":"1"`, vmAlert)
	list("handle state: unacked", f.admin, "handleState=1", http.StatusOK, `"total":"1"`, f.alertB)
	list("handle state out of range", f.alice, "handleState=9", http.StatusBadRequest)
	// 「我负责的」= 通知过我的，或由我认领的。
	list("mine: acked by me", f.alice, "mine=true", http.StatusOK, `"total":"1"`, f.alertA)
	resp = list("mine: notified me (recipient view)", f.bob, "mine=true", http.StatusOK, `"total":"1"`, f.alertA, `"canAck":false`)
	lacks(t, "recipient list item", resp, `"inhibitId":"0`)
	list("mine: nothing", f.nobody, "mine=true", http.StatusOK, `"total":"0"`)

	// 聚合的新过滤条件。
	agg := func(what, bearer, query string, contains ...string) {
		t.Helper()
		f.get(t, what, "/alert-aggregate?by=severity&"+query, bearer, http.StatusOK, contains...)
	}
	agg("aggregate acked", f.alice, "acked=true", `"total":1`)
	agg("aggregate inhibited", f.alice, "inhibited=true", `"total":1`, `"key":"4"`)
	agg("aggregate labels", f.alice, "labels=instance%3Dhost-a", `"total":1`)
	agg("aggregate out of scope", f.bob, "inhibited=true", `"total":0`)
}

// p2Field 取响应里 <key>.<field> 的字符串值。
func p2Field(t *testing.T, resp, key, field string) string {
	t.Helper()
	var out map[string]map[string]any
	if err := json.Unmarshal([]byte(resp), &out); err != nil {
		t.Fatalf("decode %s: %v", resp, err)
	}
	v, _ := out[key][field].(string)
	if v == "" {
		t.Fatalf("no %s.%s in %s", key, field, resp)
	}
	return v
}

// p2RawField 取响应顶层字段的原始 JSON（字符串字段即带引号、已转义的字面量，可原样拼进请求体）。
func p2RawField(t *testing.T, resp, field string) string {
	t.Helper()
	var out map[string]json.RawMessage
	if err := json.Unmarshal([]byte(resp), &out); err != nil || len(out[field]) == 0 {
		t.Fatalf("no %s in %s", field, resp)
	}
	return string(out[field])
}
