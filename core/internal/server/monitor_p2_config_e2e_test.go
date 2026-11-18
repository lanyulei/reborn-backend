package server

import (
	"net/http"
	"strconv"
	"strings"
	"testing"

	bizmonitor "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// withVersion 把新建请求体改成编辑请求体（带乐观锁版本号）。
func withVersion(body, version string) string {
	return strings.Replace(body, `{"spec"`, `{"version":`+version+`,"spec"`, 1)
}

// 升级策略：跟随归属分组（查看只读、维护可管理），全局策略所有有范围的人可见、仅超级管理员可维护；归属
// 分组不可更改；仍被规则引用时不可删除；估算与候选项。
func TestMonitorP2_Escalation(t *testing.T) {
	t.Parallel()
	f := newP2Fixture(t)
	body := func(groupID, name string) string {
		group := ""
		if groupID != "" {
			group = `"groupId":"` + groupID + `",`
		}
		return `{"spec":{` + group + `"name":"` + name + `","mode":1,"minSeverity":2,"enabled":true,"levels":[` +
			`{"trigger":1,"afterM":15,"repeatIntervalM":30,"maxNotify":3,"targets":{"userIds":["` + f.aliceID + `"]}},` +
			`{"trigger":2,"afterM":60,"repeatIntervalM":0,"maxNotify":1,"targets":{"oncallGroupIds":["` + e2eOncallGroup + `"]}}]}}`
	}
	// 新建：可管理才行；全局仅超级管理员。
	f.post(t, "read-only creates", "/escalation", f.bob, body(f.groupB, "x"), http.StatusForbidden, "MONITOR_FORBIDDEN")
	f.post(t, "alice creates in B", "/escalation", f.alice, body(f.groupB, "x"), http.StatusForbidden, "MONITOR_FORBIDDEN")
	f.post(t, "alice creates global", "/escalation", f.alice, body("", "x"), http.StatusForbidden, "MONITOR_FORBIDDEN")
	f.post(t, "nobody creates", "/escalation", f.nobody, body(f.groupA, "x"), http.StatusForbidden, "MONITOR_FORBIDDEN")
	resp := f.post(t, "alice creates in A", "/escalation", f.alice, body(f.groupA, "A 组升级"), http.StatusOK,
		`"groupName":"team-a"`, `"canManage":true`, `"estimate":{"rule":`, `"version":1`, `"oncallGroups":[{"id":"`+e2eOncallGroup+`","name":"运维组"`)
	escA := p2Field(t, resp, "escalation", "id")
	escB := p2Field(t, f.post(t, "admin creates in B", "/escalation", f.admin, body(f.groupB, "B 组升级"), http.StatusOK), "escalation", "id")
	escG := p2Field(t, f.post(t, "admin creates global", "/escalation", f.admin, body("", "全局升级"), http.StatusOK, `"groupName":""`), "escalation", "id")
	f.post(t, "duplicate name", "/escalation", f.alice, body(f.groupA, "A 组升级"), http.StatusConflict, "MONITOR_ESCALATION_NAME_EXISTS")
	// 校验：after_m 须严格递增；引用的对象须存在。
	f.post(t, "after_m not increasing", "/escalation", f.alice,
		strings.Replace(body(f.groupA, "bad"), `"afterM":60`, `"afterM":10`, 1), http.StatusBadRequest, "MONITOR_ESCALATION_INVALID")
	f.post(t, "unknown oncall group", "/escalation", f.alice,
		strings.Replace(body(f.groupA, "bad"), e2eOncallGroup, "018f3e1a-dead-7000-8000-000000000002", 1), http.StatusBadRequest, "MONITOR_ESCALATION_INVALID")
	f.post(t, "inactive user", "/escalation", f.alice,
		strings.Replace(body(f.groupA, "bad"), f.aliceID, "018f3e1a-dead-7000-8000-000000000003", 1), http.StatusBadRequest, "MONITOR_ESCALATION_INVALID")

	// 列表与详情：各自看到自己分组的 + 全局的；没有任何范围的人什么都看不到。
	resp = f.get(t, "alice lists", "/escalation?page=1&pageSize=20", f.alice, http.StatusOK, `"total":"2"`, escA, escG)
	lacks(t, "alice lists", resp, escB, "B 组升级")
	resp = f.get(t, "bob lists", "/escalation?page=1&pageSize=20", f.bob, http.StatusOK, `"total":"2"`, escB, escG, `"canManage":false`)
	lacks(t, "bob lists", resp, escA, `"canManage":true`)
	f.get(t, "nobody lists", "/escalation?page=1&pageSize=20", f.nobody, http.StatusOK, `"total":"0"`)
	f.get(t, "alice lists only global", "/escalation?page=1&pageSize=20&global=true", f.alice, http.StatusOK, `"total":"1"`, escG)
	f.get(t, "alice filters by B", "/escalation?page=1&pageSize=20&groupId="+f.groupB, f.alice, http.StatusOK, `"total":"0"`)
	f.get(t, "bob gets A", "/escalation/"+escA, f.bob, http.StatusForbidden, "MONITOR_FORBIDDEN")
	f.get(t, "bob gets B", "/escalation/"+escB, f.bob, http.StatusOK, `"canManage":false`)
	f.get(t, "bob gets global", "/escalation/"+escG, f.bob, http.StatusOK, `"canManage":false`)
	f.get(t, "nobody gets global", "/escalation/"+escG, f.nobody, http.StatusForbidden, "MONITOR_FORBIDDEN")

	// 编辑 / 删除：范围外与只读都不行；全局仅超级管理员；归属分组不可更改；版本不一致 409。
	f.put(t, "bob updates A", "/escalation/"+escA, f.bob, withVersion(body(f.groupA, "x"), "1"), http.StatusForbidden)
	f.put(t, "bob updates B (read-only)", "/escalation/"+escB, f.bob, withVersion(body(f.groupB, "x"), "1"), http.StatusForbidden)
	f.put(t, "alice updates global", "/escalation/"+escG, f.alice, withVersion(body("", "x"), "1"), http.StatusForbidden)
	f.put(t, "alice moves A to global", "/escalation/"+escA, f.alice, withVersion(body("", "A 组升级"), "1"), http.StatusBadRequest, "MONITOR_ESCALATION_INVALID")
	f.put(t, "admin moves A to B", "/escalation/"+escA, f.admin, withVersion(body(f.groupB, "A 组升级"), "1"), http.StatusBadRequest, "MONITOR_ESCALATION_INVALID")
	f.put(t, "stale version", "/escalation/"+escA, f.alice, withVersion(body(f.groupA, "A 组升级"), "7"), http.StatusConflict, "MONITOR_VERSION_CONFLICT")
	f.put(t, "alice updates A", "/escalation/"+escA, f.alice, withVersion(body(f.groupA, "A 组升级 v2"), "1"), http.StatusOK, `"version":2`, "A 组升级 v2")
	f.del(t, "bob deletes A", "/escalation/"+escA, f.bob, http.StatusForbidden)
	f.del(t, "bob deletes B (read-only)", "/escalation/"+escB, f.bob, http.StatusForbidden)
	f.del(t, "alice deletes global", "/escalation/"+escG, f.alice, http.StatusForbidden)

	// 估算不落库；候选项要求对所给分组只读以上，返回全局的 + 该分组（或其上级）的已启用策略。
	f.post(t, "estimate", "/escalation:estimate", f.alice,
		strings.Replace(body(f.groupA, "e"), `{"spec"`, `{"ruleRepeatIntervalM":60,"ruleMaxNotify":5,"spec"`, 1), http.StatusOK, `"estimate":{"rule":5,"levels":[3,1],"total":9}`)
	resp = f.get(t, "alice options for A", "/escalation-option?groupId="+f.groupA, f.alice, http.StatusOK, escA, escG, `"levelCount":2`)
	lacks(t, "alice options", resp, escB)
	f.get(t, "alice options for B", "/escalation-option?groupId="+f.groupB, f.alice, http.StatusForbidden, "MONITOR_FORBIDDEN")
	f.get(t, "nobody options", "/escalation-option?groupId="+f.groupA, f.nobody, http.StatusForbidden)
	f.get(t, "oncall group options", "/oncall-group-option", f.alice, http.StatusOK, e2eOncallGroup, "运维组")

	// 规则引用：只能引用全局的或本分组（及上级）的策略——别的分组的、不存在的一律同一句话。
	ruleWith := func(groupID, name, escalationID string) string {
		return strings.Replace(monitorRuleBody(groupID, name), `"notifyRecovery":true`, `"notifyRecovery":true,"escalationId":"`+escalationID+`"`, 1)
	}
	f.post(t, "rule references B's policy", "/rule", f.alice, ruleWith(f.groupA, "r1", escB), http.StatusBadRequest, "MONITOR_RULE_INVALID")
	f.post(t, "rule references unknown policy", "/rule", f.alice, ruleWith(f.groupA, "r1", "018f3e1a-dead-7000-8000-000000000004"), http.StatusBadRequest, "MONITOR_RULE_INVALID")
	r1 := p2Field(t, f.post(t, "rule references own policy", "/rule", f.alice, ruleWith(f.groupA, "r1", escA), http.StatusOK,
		`"escalationId":"`+escA+`"`, `"escalation":{"id":"`+escA+`","name":"A 组升级 v2","missing":false}`), "rule", "id")
	f.post(t, "rule references global policy", "/rule", f.alice, ruleWith(f.groupA, "r2", escG), http.StatusOK, `"escalationId":"`+escG+`"`)
	// 列表按升级策略过滤：参数被接受，结果仍受数据范围约束。过滤本身由 data 层的 SQL 实现（集成测试覆盖）；
	// 内存仓储 testfake.monitorRuleView.List 不认 RuleFilter.EscalationID，这里无法断言过滤后的条数。
	f.get(t, "rules by escalation", "/rule?page=1&pageSize=20&escalationId="+escA, f.alice, http.StatusOK, r1)
	lacks(t, "rules by escalation, out of scope", f.get(t, "bob filters by A's policy", "/rule?page=1&pageSize=20&escalationId="+escA, f.bob, http.StatusOK), r1, "cpu-high")
	f.get(t, "rules by escalation, bad id", "/rule?page=1&pageSize=20&escalationId=nope", f.alice, http.StatusBadRequest)
	// 批量设置升级策略：逐条判定——策略须能被每条规则所在的分组引用。
	f.post(t, "batch set_escalation", "/rule:batch", f.admin, `{"ids":["`+f.ruleA+`","`+f.ruleB+`"],"action":"set_escalation","escalationId":"`+escA+`"}`,
		http.StatusOK, `"succeeded":1`, `"failed":1`)
	f.get(t, "rule A now escalates", "/rule/"+f.ruleA, f.alice, http.StatusOK, `"escalationId":"`+escA+`"`)
	f.get(t, "rule B unchanged", "/rule/"+f.ruleB, f.admin, http.StatusOK, `"escalationId":""`)
	f.post(t, "batch clears escalation", "/rule:batch", f.alice, `{"ids":["`+f.ruleA+`"],"action":"set_escalation"}`, http.StatusOK, `"succeeded":1`)
	f.get(t, "rule A cleared", "/rule/"+f.ruleA, f.alice, http.StatusOK, `"escalationId":""`)

	// 仍被规则引用：不可删除，带引用数。
	resp = f.del(t, "delete in-use policy", "/escalation/"+escA, f.alice, http.StatusConflict, "MONITOR_ESCALATION_IN_USE")
	if !strings.Contains(resp, `"rule_count":"1"`) {
		t.Fatalf("ESCALATION_IN_USE must carry rule_count metadata: %s", resp)
	}
	f.del(t, "delete rule", "/rule/"+r1, f.alice, http.StatusOK)
	f.del(t, "delete policy", "/escalation/"+escA, f.alice, http.StatusOK)
	f.get(t, "deleted policy", "/escalation/"+escA, f.alice, http.StatusNotFound, "MONITOR_ESCALATION_NOT_FOUND")
}

// 抑制规则：范围口径同屏蔽规则；限定的规则须在作用分组内；预览只在调用方可读的范围内找事件。
func TestMonitorP2_Inhibit(t *testing.T) {
	t.Parallel()
	f := newP2Fixture(t)
	body := func(groupID, sourceRule string) string {
		group := ""
		if groupID != "" {
			group = `"groupId":"` + groupID + `",`
		}
		source := `"severities":[4]`
		if sourceRule != "" {
			source = `"ruleIds":["` + sourceRule + `"]`
		}
		return `{"spec":{` + group + `"name":"主机失联抑制","enabled":true,"source":{` + source + `},"target":{"severities":[2,3]},"equalLabels":["dc"]}}`
	}
	f.post(t, "read-only creates", "/inhibit", f.bob, body(f.groupB, ""), http.StatusForbidden, "MONITOR_FORBIDDEN")
	f.post(t, "alice creates in B", "/inhibit", f.alice, body(f.groupB, ""), http.StatusForbidden, "MONITOR_FORBIDDEN")
	f.post(t, "alice creates global", "/inhibit", f.alice, body("", ""), http.StatusForbidden, "MONITOR_FORBIDDEN")
	// 限定别的分组的规则：与规则不存在同一句话（不泄露它是否存在）。
	outside := f.post(t, "rule outside the group", "/inhibit", f.alice, body(f.groupA, f.ruleB), http.StatusBadRequest, "MONITOR_INHIBIT_INVALID")
	missing := f.post(t, "unknown rule", "/inhibit", f.alice, body(f.groupA, "018f3e1a-dead-7000-8000-000000000005"), http.StatusBadRequest, "MONITOR_INHIBIT_INVALID")
	if outside != missing {
		t.Fatalf("a rule outside the group and a missing rule must be indistinguishable:\n%s\n%s", outside, missing)
	}
	// 目标条件与相等标签不能同时为空。
	f.post(t, "empty target and equal", "/inhibit", f.alice,
		`{"spec":{"groupId":"`+f.groupA+`","name":"x","enabled":true,"source":{"severities":[4]}}}`, http.StatusBadRequest)
	inhA := p2Field(t, f.post(t, "alice creates in A", "/inhibit", f.alice, body(f.groupA, f.ruleA), http.StatusOK,
		`"groupName":"team-a"`, `"canManage":true`, `"sourceRules":[{"id":"`+f.ruleA+`","name":"cpu-high","missing":false}]`), "inhibit", "id")
	inhG := p2Field(t, f.post(t, "admin creates global", "/inhibit", f.admin, body("", ""), http.StatusOK), "inhibit", "id")

	resp := f.get(t, "bob lists", "/inhibit?page=1&pageSize=20", f.bob, http.StatusOK, `"total":"1"`, inhG, `"canManage":false`)
	lacks(t, "bob lists", resp, inhA)
	f.get(t, "alice lists", "/inhibit?page=1&pageSize=20", f.alice, http.StatusOK, `"total":"2"`, inhA, inhG)
	f.get(t, "nobody lists", "/inhibit?page=1&pageSize=20", f.nobody, http.StatusOK, `"total":"0"`)
	f.get(t, "bob gets A", "/inhibit/"+inhA, f.bob, http.StatusForbidden, "MONITOR_FORBIDDEN")
	f.get(t, "bob gets global", "/inhibit/"+inhG, f.bob, http.StatusOK)
	f.put(t, "bob updates A", "/inhibit/"+inhA, f.bob, withVersion(body(f.groupA, ""), "1"), http.StatusForbidden)
	f.put(t, "alice updates global", "/inhibit/"+inhG, f.alice, withVersion(body("", ""), "1"), http.StatusForbidden)
	f.put(t, "alice moves A to B", "/inhibit/"+inhA, f.alice, withVersion(body(f.groupB, ""), "1"), http.StatusForbidden)
	f.put(t, "stale version", "/inhibit/"+inhA, f.alice, withVersion(body(f.groupA, ""), "9"), http.StatusConflict, "MONITOR_VERSION_CONFLICT")
	f.put(t, "alice updates A", "/inhibit/"+inhA, f.alice, withVersion(body(f.groupA, ""), "1"), http.StatusOK, `"version":2`)
	f.del(t, "bob deletes A", "/inhibit/"+inhA, f.bob, http.StatusForbidden)
	f.del(t, "alice deletes global", "/inhibit/"+inhG, f.alice, http.StatusForbidden)

	// 预览：A 组里一条致命（源）与一条严重（目标），dc 相同；B 组里同样有一条致命——alice 的预览里不出现。
	src, dst := "018f3e1a-bbbb-7000-8000-0000000000e1", "018f3e1a-bbbb-7000-8000-0000000000e2"
	f.seedAlert(src, f.ruleA, "host-down", f.groupA, 4, func(a *bizmonitor.Alert) { a.Labels = map[string]string{"dc": "sh"} })
	f.seedAlert(dst, f.ruleA, "cpu-high", f.groupA, 3, func(a *bizmonitor.Alert) { a.Labels = map[string]string{"dc": "sh"} })
	f.seedAlert("018f3e1a-bbbb-7000-8000-0000000000e3", f.ruleB, "host-down", f.groupB, 4, func(a *bizmonitor.Alert) { a.Labels = map[string]string{"dc": "sh"} })
	f.seedAlert("018f3e1a-bbbb-7000-8000-0000000000e4", f.ruleB, "disk-full", f.groupB, 3, func(a *bizmonitor.Alert) { a.Labels = map[string]string{"dc": "sh"} })
	resp = f.post(t, "alice previews", "/inhibit:preview", f.alice, body(f.groupA, ""), http.StatusOK, `"sourceTotal":1`, `"targetTotal":1`, src, dst)
	lacks(t, "preview stays in scope", resp, "018f3e1a-bbbb-7000-8000-0000000000e3", "018f3e1a-bbbb-7000-8000-0000000000e4", f.alertB)
	f.post(t, "bob previews A", "/inhibit:preview", f.bob, body(f.groupA, ""), http.StatusForbidden, "MONITOR_FORBIDDEN")
	// 全局预览：非管理员只看到自己范围内的事件。
	resp = f.post(t, "bob previews global", "/inhibit:preview", f.bob, body("", ""), http.StatusOK, `"sourceTotal":1`, `"targetTotal":1`,
		"018f3e1a-bbbb-7000-8000-0000000000e3", "018f3e1a-bbbb-7000-8000-0000000000e4")
	lacks(t, "bob previews global", resp, src, dst, f.alertA)

	f.del(t, "alice deletes A", "/inhibit/"+inhA, f.alice, http.StatusOK)
	f.get(t, "deleted", "/inhibit/"+inhA, f.alice, http.StatusNotFound, "MONITOR_INHIBIT_NOT_FOUND")
}

// 记录规则：范围口径同告警规则；数据源须配置了 Remote Write 地址。数据源侧：写入地址与查询地址不同源时，
// 沿用已保存的凭据须重新填写。
func TestMonitorP2_RecordRuleAndWriteURL(t *testing.T) {
	t.Parallel()
	f := newP2Fixture(t)
	dsBody := func(name, writeURL, secret string) string {
		return `{"spec":{"name":"` + name + `","connection":{"url":"http://vm:8428","authType":3,"timeoutMs":5000` + secret + `},"remoteWriteUrl":"` + writeURL + `","enabled":true}}`
	}
	dsRW := p2Field(t, f.post(t, "datasource with write url", "/datasource", f.admin, dsBody("vm-rw", "http://vm:8428/api/v1/write", `,"secret":"tok-1"`), http.StatusOK), "datasource", "id")
	dsRO := p2Field(t, f.post(t, "datasource without write url", "/datasource", f.admin, dsBody("vm-ro", "", `,"secret":"tok-2"`), http.StatusOK), "datasource", "id")

	// 写入地址换到另一个源、又想沿用已保存的凭据：拒绝；重新填写凭据后允许；同源的改动不要求。
	f.put(t, "cross-origin write url keeps saved secret", "/datasource/"+dsRO, f.admin, withVersion(dsBody("vm-ro", "http://vminsert:8480/insert/0/prometheus/api/v1/write", ""), "1"),
		http.StatusBadRequest, "MONITOR_DATASOURCE_SECRET_REQUIRED")
	f.put(t, "same-origin write url keeps saved secret", "/datasource/"+dsRW, f.admin, withVersion(dsBody("vm-rw", "http://vm:8428/insert/api/v1/write", ""), "1"),
		http.StatusOK, `"version":2`)
	dsX := p2Field(t, f.post(t, "cross-origin write url on create", "/datasource", f.admin,
		dsBody("vm-x", "http://vminsert:8480/insert/0/prometheus/api/v1/write", `,"secret":"tok-3"`), http.StatusOK), "datasource", "id")
	f.put(t, "cross-origin write url with re-entered secret", "/datasource/"+dsRO, f.admin,
		withVersion(dsBody("vm-ro", "http://vminsert:8480/insert/0/prometheus/api/v1/write", `,"secret":"tok-2"`), "1"), http.StatusOK)
	f.put(t, "clear write url again", "/datasource/"+dsRO, f.admin, withVersion(dsBody("vm-ro", "", ""), "2"), http.StatusOK)

	// 记录规则的内存仓储在写入时自己再核对一次数据源（对应 data 层写事务里的行锁复核），数据来自它的 Targets。
	for id, target := range map[string]bizmonitor.DatasourceWriteTarget{
		dsRW: {Name: "vm-rw", Enabled: true, HasWriteURL: true}, dsRO: {Name: "vm-ro", Enabled: true}, dsX: {Name: "vm-x", Enabled: true, HasWriteURL: true},
	} {
		f.h.inhibits.Targets[id] = target
	}

	body := func(groupID, name, datasourceID string) string {
		return `{"spec":{"groupId":"` + groupID + `","name":"` + name + `","datasourceId":"` + datasourceID + `","expr":"sum(rate(http_requests_total[5m])) by (job)","intervalS":60,"enabled":true}}`
	}
	f.post(t, "read-only creates", "/record-rule", f.bob, body(f.groupB, "job:req:rate5m", dsRW), http.StatusForbidden, "MONITOR_FORBIDDEN")
	f.post(t, "alice creates in B", "/record-rule", f.alice, body(f.groupB, "job:req:rate5m", dsRW), http.StatusForbidden, "MONITOR_FORBIDDEN")
	f.post(t, "nobody creates", "/record-rule", f.nobody, body(f.groupA, "job:req:rate5m", dsRW), http.StatusForbidden, "MONITOR_FORBIDDEN")
	f.post(t, "no write url", "/record-rule", f.alice, body(f.groupA, "job:req:rate5m", dsRO), http.StatusBadRequest, "MONITOR_RECORD_RULE_NO_WRITE_URL")
	f.post(t, "bad expr", "/record-rule", f.alice, strings.Replace(body(f.groupA, "job:req:rate5m", dsRW), "by (job)", "by (", 1), http.StatusBadRequest, "MONITOR_RECORD_RULE_INVALID")
	recA := p2Field(t, f.post(t, "alice creates in A", "/record-rule", f.alice, body(f.groupA, "job:req:rate5m", dsRW), http.StatusOK,
		`"groupName":"team-a"`, `"datasourceName":"vm-rw"`, `"writable":true`, `"canManage":true`, `"version":1`), "recordRule", "id")
	recB := p2Field(t, f.post(t, "admin creates in B", "/record-rule", f.admin, body(f.groupB, "job:req:rate5m", dsX), http.StatusOK), "recordRule", "id")
	f.post(t, "duplicate metric name", "/record-rule", f.alice, body(f.groupA, "job:req:rate5m", dsRW), http.StatusConflict, "MONITOR_RECORD_RULE_NAME_EXISTS")

	// 校验接口：逐项返回问题，不落库；对所给分组没有范围 403。
	f.post(t, "validate ok", "/record-rule:validate", f.alice, body(f.groupA, "job:req:rate1h", dsRW), http.StatusOK, `"valid":true`, `"issues":[]`)
	f.post(t, "validate: no write url", "/record-rule:validate", f.alice, body(f.groupA, "job:req:rate5m", dsRO), http.StatusOK,
		`"valid":false`, `"field":"datasource_id"`, "MONITOR_RECORD_RULE_NO_WRITE_URL")
	f.post(t, "validate: duplicate + bad expr", "/record-rule:validate", f.alice, strings.Replace(body(f.groupA, "job:req:rate5m", dsRW), "by (job)", "by (", 1), http.StatusOK,
		`"valid":false`, "MONITOR_RECORD_RULE_NAME_EXISTS", `"field":"expr"`)
	f.post(t, "validate excludes itself", "/record-rule:validate", f.alice,
		strings.Replace(body(f.groupA, "job:req:rate5m", dsRW), `{"spec"`, `{"id":"`+recA+`","spec"`, 1), http.StatusOK, `"valid":true`)
	f.post(t, "validate out of scope", "/record-rule:validate", f.alice, body(f.groupB, "x", dsRW), http.StatusForbidden, "MONITOR_FORBIDDEN")

	resp := f.get(t, "alice lists", "/record-rule?page=1&pageSize=20", f.alice, http.StatusOK, `"total":"1"`, recA)
	lacks(t, "alice lists", resp, recB)
	f.get(t, "bob lists", "/record-rule?page=1&pageSize=20", f.bob, http.StatusOK, `"total":"1"`, recB, `"canManage":false`)
	f.get(t, "nobody lists", "/record-rule?page=1&pageSize=20", f.nobody, http.StatusOK, `"total":"0"`)
	f.get(t, "bob filters by A", "/record-rule?page=1&pageSize=20&groupId="+f.groupA, f.bob, http.StatusOK, `"total":"0"`)
	f.get(t, "bob gets A", "/record-rule/"+recA, f.bob, http.StatusForbidden, "MONITOR_FORBIDDEN")
	f.get(t, "bob gets B", "/record-rule/"+recB, f.bob, http.StatusOK)
	f.put(t, "bob updates A", "/record-rule/"+recA, f.bob, withVersion(body(f.groupA, "job:req:rate5m", dsRW), "1"), http.StatusForbidden)
	f.put(t, "bob updates B (read-only)", "/record-rule/"+recB, f.bob, withVersion(body(f.groupB, "job:req:rate5m", dsX), "1"), http.StatusForbidden)
	f.post(t, "bob toggles B (read-only)", "/record-rule/"+recB+":toggle", f.bob, `{"enabled":false}`, http.StatusForbidden)
	f.post(t, "bob toggles A", "/record-rule/"+recA+":toggle", f.bob, `{"enabled":false}`, http.StatusForbidden)
	f.del(t, "bob deletes A", "/record-rule/"+recA, f.bob, http.StatusForbidden)
	f.put(t, "alice moves A into B", "/record-rule/"+recA, f.alice, withVersion(body(f.groupB, "job:req:rate5m", dsRW), "1"), http.StatusForbidden)
	f.put(t, "stale version", "/record-rule/"+recA, f.alice, withVersion(body(f.groupA, "job:req:rate5m", dsRW), "9"), http.StatusConflict, "MONITOR_VERSION_CONFLICT")
	f.put(t, "alice updates A", "/record-rule/"+recA, f.alice, withVersion(strings.Replace(body(f.groupA, "job:req:rate5m", dsRW), `"intervalS":60`, `"intervalS":120`, 1), "1"),
		http.StatusOK, `"intervalS":120`, `"version":2`)
	f.post(t, "alice toggles A", "/record-rule/"+recA+":toggle", f.alice, `{"enabled":false}`, http.StatusOK, `"enabled":false`, `"lastStatus":`+strconv.Itoa(int(mc.RecordNever)))
	f.del(t, "alice deletes A", "/record-rule/"+recA, f.alice, http.StatusOK)
	f.get(t, "deleted", "/record-rule/"+recA, f.alice, http.StatusNotFound, "MONITOR_RECORD_RULE_NOT_FOUND")
}

// 聚合视图：个人视图只有本人可见、可改；公共视图所有人可见、仅超级管理员可建 / 改 / 删。无权操作与不存在
// 同样是 404；想把视图设为公共的非管理员是 403。
func TestMonitorP2_AlertViews(t *testing.T) {
	t.Parallel()
	f := newP2Fixture(t)
	body := func(name string, public bool, groupID string) string {
		filters := `"severities":[3,4]`
		if groupID != "" {
			filters += `,"groupId":"` + groupID + `"`
		}
		p := "false"
		if public {
			p = "true"
		}
		return `{"spec":{"name":"` + name + `","by":"label:instance","isPublic":` + p + `,"filters":{` + filters + `}}}`
	}
	mine := p2Field(t, f.post(t, "alice saves", "/alert-view", f.alice, body("我的视图", false, f.groupA), http.StatusOK, `"mine":true`, `"canEdit":true`, `"ownerId":"`+f.aliceID+`"`), "view", "id")
	f.post(t, "duplicate name", "/alert-view", f.alice, body("我的视图", false, ""), http.StatusConflict, "MONITOR_ALERT_VIEW_NAME_EXISTS")
	f.post(t, "same name, another owner", "/alert-view", f.bob, body("我的视图", false, ""), http.StatusOK)
	f.post(t, "blank name", "/alert-view", f.alice, body("   ", false, ""), http.StatusBadRequest, "MONITOR_ALERT_VIEW_INVALID")
	// 过滤条件里的分组须是自己可读的：范围外的与不存在的同一句话。
	outside := f.post(t, "filter on a group out of scope", "/alert-view", f.alice, body("v2", false, f.groupB), http.StatusBadRequest, "MONITOR_ALERT_VIEW_INVALID")
	unknown := f.post(t, "filter on an unknown group", "/alert-view", f.alice, body("v2", false, "018f3e1a-dead-7000-8000-000000000006"), http.StatusBadRequest, "MONITOR_ALERT_VIEW_INVALID")
	if outside != unknown {
		t.Fatalf("a group out of scope and a missing group must be indistinguishable:\n%s\n%s", outside, unknown)
	}
	f.post(t, "non-admin saves public view", "/alert-view", f.alice, body("公共", true, ""), http.StatusForbidden, "MONITOR_FORBIDDEN")
	public := p2Field(t, f.post(t, "admin saves public view", "/alert-view", f.admin, body("公共", true, ""), http.StatusOK, `"isPublic":true`), "view", "id")

	// 列表：公共的 + 自己的；别人的个人视图不可见。
	resp := f.get(t, "alice lists", "/alert-view", f.alice, http.StatusOK, mine, public)
	if strings.Count(resp, `"id":`) != 2 {
		t.Fatalf("alice must see her view and the public one only: %s", resp)
	}
	resp = f.get(t, "bob lists", "/alert-view", f.bob, http.StatusOK, public, `"canEdit":false`)
	lacks(t, "bob lists", resp, mine)

	// 改 / 删别人的个人视图、非管理员改 / 删公共视图：一律 404；把自己的视图设为公共：403。
	f.put(t, "bob updates alice's view", "/alert-view/"+mine, f.bob, body("x", false, ""), http.StatusNotFound, "MONITOR_ALERT_VIEW_NOT_FOUND")
	f.del(t, "bob deletes alice's view", "/alert-view/"+mine, f.bob, http.StatusNotFound, "MONITOR_ALERT_VIEW_NOT_FOUND")
	f.put(t, "admin updates alice's personal view", "/alert-view/"+mine, f.admin, body("x", false, ""), http.StatusNotFound, "MONITOR_ALERT_VIEW_NOT_FOUND")
	f.put(t, "alice updates public view", "/alert-view/"+public, f.alice, body("公共", true, ""), http.StatusNotFound, "MONITOR_ALERT_VIEW_NOT_FOUND")
	f.del(t, "alice deletes public view", "/alert-view/"+public, f.alice, http.StatusNotFound, "MONITOR_ALERT_VIEW_NOT_FOUND")
	f.put(t, "alice makes her view public", "/alert-view/"+mine, f.alice, body("我的视图", true, ""), http.StatusForbidden, "MONITOR_FORBIDDEN")
	f.put(t, "unknown view", "/alert-view/018f3e1a-dead-7000-8000-000000000007", f.alice, body("x", false, ""), http.StatusNotFound, "MONITOR_ALERT_VIEW_NOT_FOUND")
	f.put(t, "alice updates her view", "/alert-view/"+mine, f.alice, body("我的视图 2", false, ""), http.StatusOK, "我的视图 2", `"by":"label:instance"`)
	f.put(t, "admin updates public view", "/alert-view/"+public, f.admin, body("公共 2", true, ""), http.StatusOK, "公共 2")
	f.del(t, "alice deletes her view", "/alert-view/"+mine, f.alice, http.StatusOK)
	f.del(t, "admin deletes public view", "/alert-view/"+public, f.admin, http.StatusOK)
	f.get(t, "alice lists again", "/alert-view", f.alice, http.StatusOK, `"items":[]`)
}
