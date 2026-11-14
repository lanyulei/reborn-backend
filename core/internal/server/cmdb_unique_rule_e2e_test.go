package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// 唯一规则全链路（HTTP）：规则 CRUD、随模型详情返回、字段 unique 只读派生、预检、
// 存量重复拒绝、多规则独立约束、字段占用保护、审计与授权。
func TestCmdb_UniqueRules(t *testing.T) {
	t.Parallel()
	ts, svc := newTestHTTPServerWithServices(t)
	admin := testBearer(t, svc.signer, e2eActorID)
	cmdbSetup(t, ts, admin) // host：ip（规则 IP）、cores
	do := func(method, url, body string) (int, string) {
		return doRequestWithToken(t, method, ts.URL+url, admin, body)
	}
	mustOK := func(method, url, body string) string {
		t.Helper()
		code, resp := do(method, url, body)
		if code != http.StatusOK {
			t.Fatalf("%s %s: status = %d body = %s", method, url, code, resp)
		}
		return resp
	}
	type ruleJSON struct {
		ID         string   `json:"id"`
		Name       string   `json:"name"`
		FieldCodes []string `json:"fieldCodes"`
	}
	type modelJSON struct {
		Fields []struct {
			ID     string `json:"id"`
			Code   string `json:"code"`
			Unique bool   `json:"unique"`
		} `json:"fields"`
		UniqueRules []ruleJSON `json:"uniqueRules"`
	}
	getModel := func() modelJSON {
		t.Helper()
		var m modelJSON
		if err := json.Unmarshal([]byte(mustOK(http.MethodGet, "/api/v1/cmdb/model/host", "")), &m); err != nil {
			t.Fatal(err)
		}
		return m
	}
	fieldID := func(m modelJSON, code string) string {
		for _, f := range m.Fields {
			if f.Code == code {
				return f.ID
			}
		}
		t.Fatalf("field %s not found", code)
		return ""
	}
	unique := func(m modelJSON, code string) bool {
		for _, f := range m.Fields {
			if f.Code == code {
				return f.Unique
			}
		}
		return false
	}

	// 规则随模型详情返回；字段的 unique 是派生值。
	m := getModel()
	if len(m.UniqueRules) != 1 || m.UniqueRules[0].Name != "IP" || !unique(m, "ip") || unique(m, "cores") {
		t.Fatalf("model detail = %+v", m)
	}
	ipRuleID := m.UniqueRules[0].ID

	// 字段上的 unique 开关已停用：新建传 true、更新改变派生值都拒绝；回传当前值兼容。
	if code, body := do(http.MethodPost, "/api/v1/cmdb/model/host/field", `{"code":"sn2","name":"x","type":"string","unique":true}`); code != http.StatusBadRequest || !strings.Contains(body, "unique rules") {
		t.Fatalf("create field with unique: status = %d body = %s", code, body)
	}
	if code, body := do(http.MethodPut, "/api/v1/cmdb/model/host/field/"+fieldID(m, "cores"), `{"unique":true}`); code != http.StatusBadRequest || !strings.Contains(body, "unique rules") {
		t.Fatalf("update field unique: status = %d body = %s", code, body)
	}
	mustOK(http.MethodPut, "/api/v1/cmdb/model/host/field/"+fieldID(m, "ip"), `{"name":"管理 IP","unique":true}`)

	// 第二组字段：serial + 机位（dc、rack）。
	for _, f := range []string{
		`{"code":"serial","name":"序列号","type":"string"}`, `{"code":"dc","name":"机房","type":"string"}`, `{"code":"rack","name":"机柜","type":"string"}`,
		`{"code":"load","name":"负载","type":"float"}`, `{"code":"token","name":"令牌","type":"string","sensitive":true}`,
	} {
		mustOK(http.MethodPost, "/api/v1/cmdb/model/host/field", f)
	}
	createCI(t, ts, admin, `{"modelCode":"host","name":"h1","attrs":{"ip":"10.0.0.1","serial":"SN1","dc":"bj","rack":"A1"}}`)
	createCI(t, ts, admin, `{"modelCode":"host","name":"h2","attrs":{"ip":"10.0.0.2","serial":"SN1","dc":"bj","rack":"A2"}}`)

	// 定义校验：未知 / 类型不支持 / 敏感字段、重复字段、超过 5 个字段。
	for name, body := range map[string]string{
		"unknown field":   `{"name":"r","fieldCodes":["nope"]}`,
		"float field":     `{"name":"r","fieldCodes":["load"]}`,
		"sensitive field": `{"name":"r","fieldCodes":["token"]}`,
		"duplicate field": `{"name":"r","fieldCodes":["dc","dc"]}`,
		"too many fields": `{"name":"r","fieldCodes":["ip","serial","dc","rack","cores","load"]}`,
		"empty fields":    `{"name":"r","fieldCodes":[]}`,
		"blank name":      `{"name":"   ","fieldCodes":["dc"]}`,
	} {
		if code, resp := do(http.MethodPost, "/api/v1/cmdb/model/host/unique-rule", body); code != http.StatusBadRequest {
			t.Fatalf("%s: status = %d body = %s", name, code, resp)
		}
	}

	// 预检与存量重复：serial 有 1 组重复（h1、h2）→ 建规则 409，且不留痕。
	check := mustOK(http.MethodPost, "/api/v1/cmdb/model/host/unique-rule:check", `{"fieldCodes":["serial"]}`)
	if !strings.Contains(check, `"conflictGroups":"1"`) || !strings.Contains(check, `"h1"`) || !strings.Contains(check, `"h2"`) {
		t.Fatalf("check(serial) = %s", check)
	}
	if code, body := do(http.MethodPost, "/api/v1/cmdb/model/host/unique-rule", `{"name":"序列号","fieldCodes":["serial"]}`); code != http.StatusConflict || !strings.Contains(body, "CMDB_UNIQUE_RULE_CONFLICT") {
		t.Fatalf("rule over duplicates: status = %d body = %s", code, body)
	}
	if m := getModel(); len(m.UniqueRules) != 1 || unique(m, "serial") {
		t.Fatalf("failed rule must not persist: %+v", m.UniqueRules)
	}

	// 联合规则 dc+rack 无重复：可建；与 IP 规则各自独立生效。
	if check := mustOK(http.MethodPost, "/api/v1/cmdb/model/host/unique-rule:check", `{"fieldCodes":["rack","dc"]}`); !strings.Contains(check, `"conflictGroups":"0"`) {
		t.Fatalf("check(dc, rack) = %s", check)
	}
	var created struct {
		Rule ruleJSON `json:"rule"`
	}
	if err := json.Unmarshal([]byte(mustOK(http.MethodPost, "/api/v1/cmdb/model/host/unique-rule", `{"name":" 机位 ","fieldCodes":["rack","dc"]}`)), &created); err != nil {
		t.Fatal(err)
	}
	if created.Rule.Name != "机位" || strings.Join(created.Rule.FieldCodes, ",") != "dc,rack" {
		t.Fatalf("created rule must be normalized: %+v", created.Rule)
	}
	if m := getModel(); len(m.UniqueRules) != 2 || !unique(m, "dc") || !unique(m, "rack") {
		t.Fatalf("after create: %+v", m)
	}
	if code, body := do(http.MethodPost, "/api/v1/cmdb/ci", `{"modelCode":"host","name":"h3","attrs":{"ip":"10.0.0.3","dc":"bj","rack":"A1"}}`); code != http.StatusConflict ||
		!strings.Contains(body, "CMDB_CI_DUPLICATE") || !strings.Contains(body, "机位") || strings.Contains(body, "h1") {
		t.Fatalf("composite rule violation must name the rule and not the other resource: status = %d body = %s", code, body)
	}
	if code, body := do(http.MethodPost, "/api/v1/cmdb/ci", `{"modelCode":"host","name":"h3","attrs":{"ip":"10.0.0.1","dc":"sh","rack":"A1"}}`); code != http.StatusConflict || !strings.Contains(body, `IP`) {
		t.Fatalf("ip rule violation: status = %d body = %s", code, body)
	}
	// 同机房不同机柜、字段不全的资源不受联合规则约束。
	createCI(t, ts, admin, `{"modelCode":"host","name":"h3","attrs":{"ip":"10.0.0.3","dc":"bj","rack":"A3"}}`)
	createCI(t, ts, admin, `{"modelCode":"host","name":"h4","attrs":{"ip":"10.0.0.4","dc":"bj"}}`)
	createCI(t, ts, admin, `{"modelCode":"host","name":"h5","attrs":{"ip":"10.0.0.5","dc":"bj"}}`)

	// 查重：同名 / 同字段组。
	if code, body := do(http.MethodPost, "/api/v1/cmdb/model/host/unique-rule", `{"name":"机位","fieldCodes":["serial"]}`); code != http.StatusConflict || !strings.Contains(body, "CMDB_UNIQUE_RULE_EXISTS") {
		t.Fatalf("duplicate rule name: status = %d body = %s", code, body)
	}
	if code, body := do(http.MethodPost, "/api/v1/cmdb/model/host/unique-rule", `{"name":"机位2","fieldCodes":["dc","rack"]}`); code != http.StatusConflict || !strings.Contains(body, "CMDB_UNIQUE_RULE_EXISTS") {
		t.Fatalf("duplicate rule fields: status = %d body = %s", code, body)
	}

	// 更新：改名；字段组改成有重复的被拒；不带变化的请求原样返回。
	mustOK(http.MethodPut, "/api/v1/cmdb/model/host/unique-rule/"+created.Rule.ID, `{"name":"机房机位"}`)
	if code, body := do(http.MethodPut, "/api/v1/cmdb/model/host/unique-rule/"+created.Rule.ID, `{"fieldCodes":["dc"]}`); code != http.StatusConflict || !strings.Contains(body, "CMDB_UNIQUE_RULE_CONFLICT") {
		t.Fatalf("rekey into duplicates: status = %d body = %s", code, body)
	}
	var noop struct {
		Rule ruleJSON `json:"rule"`
	}
	if err := json.Unmarshal([]byte(mustOK(http.MethodPut, "/api/v1/cmdb/model/host/unique-rule/"+created.Rule.ID, `{}`)), &noop); err != nil {
		t.Fatal(err)
	}
	if noop.Rule.Name != "机房机位" || strings.Join(noop.Rule.FieldCodes, ",") != "dc,rack" {
		t.Fatalf("no-op update = %+v", noop.Rule)
	}
	// 回传相同的字段组（顺序不同）等同于不变。
	mustOK(http.MethodPut, "/api/v1/cmdb/model/host/unique-rule/"+created.Rule.ID, `{"fieldCodes":["rack","dc"]}`)

	// 字段占用保护：被规则引用的字段不可废弃 / 设为敏感。
	m = getModel()
	if code, body := do(http.MethodDelete, "/api/v1/cmdb/model/host/field/"+fieldID(m, "rack"), ""); code != http.StatusConflict || !strings.Contains(body, "CMDB_FIELD_IN_USE") || !strings.Contains(body, "机房机位") {
		t.Fatalf("deprecate field in rule: status = %d body = %s", code, body)
	}
	if code, body := do(http.MethodPut, "/api/v1/cmdb/model/host/field/"+fieldID(m, "rack"), `{"sensitive":true}`); code != http.StatusConflict || !strings.Contains(body, "CMDB_FIELD_IN_USE") {
		t.Fatalf("sensitive field in rule: status = %d body = %s", code, body)
	}

	// 删除规则：约束解除，字段可废弃。
	mustOK(http.MethodDelete, "/api/v1/cmdb/model/host/unique-rule/"+created.Rule.ID, "")
	createCI(t, ts, admin, `{"modelCode":"host","name":"h6","attrs":{"ip":"10.0.0.6","dc":"bj","rack":"A1"}}`)
	mustOK(http.MethodDelete, "/api/v1/cmdb/model/host/field/"+fieldID(m, "rack"), "")
	if code, _ := do(http.MethodDelete, "/api/v1/cmdb/model/host/unique-rule/"+created.Rule.ID, ""); code != http.StatusNotFound {
		t.Fatalf("delete missing rule: status = %d", code)
	}
	if code, _ := do(http.MethodPut, "/api/v1/cmdb/model/part/unique-rule/"+ipRuleID, `{"name":"x"}`); code != http.StatusNotFound {
		t.Fatalf("rule of another model must be 404, got %d", code)
	}
	if code, _ := do(http.MethodPost, "/api/v1/cmdb/model/nope/unique-rule", `{"name":"x","fieldCodes":["ip"]}`); code != http.StatusNotFound {
		t.Fatalf("rule on missing model: status = %d", code)
	}

	// 每类型上限 8 条。
	for i := 0; i < 8; i++ {
		code := "u" + string(rune('a'+i))
		mustOK(http.MethodPost, "/api/v1/cmdb/model/part/field", `{"code":"`+code+`","name":"`+code+`","type":"string"}`)
		mustOK(http.MethodPost, "/api/v1/cmdb/model/part/unique-rule", `{"name":"`+code+`","fieldCodes":["`+code+`"]}`)
	}
	mustOK(http.MethodPost, "/api/v1/cmdb/model/part/field", `{"code":"uz","name":"uz","type":"string"}`)
	if code, body := do(http.MethodPost, "/api/v1/cmdb/model/part/unique-rule", `{"name":"uz","fieldCodes":["uz"]}`); code != http.StatusBadRequest || !strings.Contains(body, "at most 8") {
		t.Fatalf("rule limit: status = %d body = %s", code, body)
	}

	// 审计：规则的增 / 改 / 删都有记录。
	audit := mustOK(http.MethodGet, "/api/v1/cmdb/schema-audit?page=1&pageSize=50&targetType=unique_rule&modelCode=host", "")
	audit = strings.ReplaceAll(audit, ", ", ",") // protojson 的空白不稳定，归一后再做子串断言
	for _, want := range []string{`"action":"create"`, `"action":"update"`, `"action":"delete"`, `"targetName":"机房机位"`, `"fields":["dc","rack"]`} {
		if !strings.Contains(audit, want) {
			t.Fatalf("unique rule audit missing %s: %s", want, audit)
		}
	}

	// 授权：未授写接口的角色 403；规则读取随模型详情（GetModel）可见。
	viewer := createUser(t, ts, admin, "uq_viewer")
	viewerBearer := testBearer(t, svc.signer, viewer.Id)
	role := createRole(t, ts, admin, "模型只读", "uq_viewer")
	if code, body := do(http.MethodPut, "/api/v1/system/role/"+role.Id+"/permission",
		`{"menus":["cmdb.model"],"apis":["`+opCode("/cmdb.v1.CmdbModelService/GetModel")+`"]}`); code != http.StatusOK {
		t.Fatalf("grant: status = %d body = %s", code, body)
	}
	mustOK(http.MethodPut, "/api/v1/system/user/"+viewer.Id+"/role", `{"role_ids":["`+role.Id+`"]}`)
	if code, body := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/model/host", viewerBearer, ""); code != http.StatusOK || !strings.Contains(body, `"uniqueRules"`) {
		t.Fatalf("viewer get model: status = %d body = %s", code, body)
	}
	for _, req := range [][3]string{
		{http.MethodPost, "/api/v1/cmdb/model/host/unique-rule", `{"name":"x","fieldCodes":["serial"]}`},
		{http.MethodPut, "/api/v1/cmdb/model/host/unique-rule/" + ipRuleID, `{"name":"x"}`},
		{http.MethodDelete, "/api/v1/cmdb/model/host/unique-rule/" + ipRuleID, ""},
		{http.MethodPost, "/api/v1/cmdb/model/host/unique-rule:check", `{"fieldCodes":["serial"]}`},
	} {
		if code, _ := doRequestWithToken(t, req[0], ts.URL+req[1], viewerBearer, req[2]); code != http.StatusForbidden {
			t.Fatalf("viewer %s %s: status = %d, want 403", req[0], req[1], code)
		}
	}
}
