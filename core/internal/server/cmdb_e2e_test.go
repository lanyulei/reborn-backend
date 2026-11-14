package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
)

func opCode(name string) string { return biz.OperationCode(name) }

// cmdbSetup 经 HTTP 搭建一套最小模型：host（ip 必填唯一，cores int）与 part（kind 枚举），
// 关系 host_has_part（host → part，一对多，级联退役）、host_uplink（host → switch，多对多，restrict）。
func cmdbSetup(t *testing.T, ts *httptest.Server, bearer string) {
	t.Helper()
	mustOK := func(method, url, body string) string {
		t.Helper()
		code, resp := doRequestWithToken(t, method, ts.URL+url, bearer, body)
		if code != http.StatusOK {
			t.Fatalf("%s %s: status = %d body = %s", method, url, code, resp)
		}
		return resp
	}
	mustOK(http.MethodPost, "/api/v1/cmdb/model", `{"code":"host","name":"主机","category":"compute"}`)
	mustOK(http.MethodPost, "/api/v1/cmdb/model", `{"code":"part","name":"配件","category":"component"}`)
	mustOK(http.MethodPost, "/api/v1/cmdb/model", `{"code":"switch","name":"交换机","category":"network"}`)
	mustOK(http.MethodPost, "/api/v1/cmdb/model/host/field", `{"code":"ip","name":"IP","type":"ip","required":true,"listVisible":true}`)
	mustOK(http.MethodPost, "/api/v1/cmdb/model/host/unique-rule", `{"name":"IP","fieldCodes":["ip"]}`)
	mustOK(http.MethodPost, "/api/v1/cmdb/model/host/field", `{"code":"cores","name":"核数","type":"int","rule":{"min":1,"max":128}}`)
	mustOK(http.MethodPost, "/api/v1/cmdb/model/part/field", `{"code":"kind","name":"类型","type":"enum","required":true,"enumValues":["memory","disk"]}`)
	mustOK(http.MethodPost, "/api/v1/cmdb/relation-type", `{"code":"host_has_part","name":"安装配件","forwardName":"安装的配件","backwardName":"安装于","srcModel":"host","dstModel":"part","cardinality":"one_to_many","onDelete":"cascade_retire"}`)
	mustOK(http.MethodPost, "/api/v1/cmdb/relation-type", `{"code":"host_uplink","name":"上联","forwardName":"上联交换机","backwardName":"下联主机","srcModel":"host","dstModel":"switch","cardinality":"many_to_many"}`)
}

type ciJSON struct {
	ID      string         `json:"id"`
	Name    string         `json:"name"`
	Status  string         `json:"status"`
	Version string         `json:"version"`
	Attrs   map[string]any `json:"attrs"`
}

func createCI(t *testing.T, ts *httptest.Server, bearer, body string) ciJSON {
	t.Helper()
	code, resp := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/cmdb/ci", bearer, body)
	if code != http.StatusOK {
		t.Fatalf("create ci: status = %d body = %s", code, resp)
	}
	var out struct{ Ci ciJSON }
	if err := json.Unmarshal([]byte(resp), &out); err != nil {
		t.Fatalf("create ci body = %s (%v)", resp, err)
	}
	return out.Ci
}

type relationSummaryJSON struct {
	RelationType string `json:"relationType"`
	Direction    string `json:"direction"`
	Name         string `json:"name"`
	TargetModel  string `json:"targetModel"`
	Count        string `json:"count"`
}

// summaryCount 从详情响应取某关系类型某方向的统计（不存在返回 -1）。
func summaryCount(t *testing.T, body, relationType, direction string) (relationSummaryJSON, bool) {
	t.Helper()
	var out struct {
		Relations []relationSummaryJSON `json:"relations"`
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatalf("parse detail: %v", err)
	}
	for _, r := range out.Relations {
		if r.RelationType == relationType && r.Direction == direction {
			return r, true
		}
	}
	return relationSummaryJSON{}, false
}

func getCI(t *testing.T, ts *httptest.Server, bearer, id string) (ciJSON, string) {
	t.Helper()
	code, resp := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci/"+id, bearer, "")
	if code != http.StatusOK {
		t.Fatalf("get ci: status = %d body = %s", code, resp)
	}
	var out struct{ Ci ciJSON }
	if err := json.Unmarshal([]byte(resp), &out); err != nil {
		t.Fatal(err)
	}
	return out.Ci, resp
}

// 模型元数据：详情含字段与适用关系；引用中的类型不可删；关系类型被引用不可删。
func TestCmdb_ModelMetadata(t *testing.T) {
	t.Parallel()
	ts, svc := newTestHTTPServerWithServices(t)
	bearer := testBearer(t, svc.signer, e2eActorID)
	cmdbSetup(t, ts, bearer)

	code, body := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/model/host", bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"code":"ip"`) || !strings.Contains(body, `"code":"cores"`) ||
		!strings.Contains(body, `"code":"host_has_part"`) || !strings.Contains(body, `"code":"host_uplink"`) {
		t.Fatalf("get model: status = %d body = %s", code, body)
	}
	// part 的适用关系只有 host_has_part。
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/relation-type?modelCode=part", bearer, "")
	if code != http.StatusOK || !strings.Contains(body, "host_has_part") || strings.Contains(body, "host_uplink") {
		t.Fatalf("list relation types for part: %s", body)
	}
	// 非法字段定义：enum 无值、ref 指向不存在类型、pattern 用于数值。
	for _, bad := range []string{
		`{"code":"e","name":"e","type":"enum"}`,
		`{"code":"r","name":"r","type":"ref","refModel":"nope"}`,
		`{"code":"n","name":"n","type":"int","rule":{"pattern":"^a$"}}`,
	} {
		if code, body := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/cmdb/model/host/field", bearer, bad); code != http.StatusBadRequest {
			t.Fatalf("invalid field %s: status = %d body = %s", bad, code, body)
		}
	}
	// 重复字段编码 409；重复类型编码 409。
	if code, _ := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/cmdb/model/host/field", bearer, `{"code":"ip","name":"x","type":"string"}`); code != http.StatusConflict {
		t.Fatalf("duplicate field: status = %d", code)
	}
	if code, _ := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/cmdb/model", bearer, `{"code":"host","name":"x","category":"compute"}`); code != http.StatusConflict {
		t.Fatalf("duplicate model: status = %d", code)
	}
	// host 被关系类型引用，不可删除；删除关系类型后仍有资源也不可删除。
	if code, body := doRequestWithToken(t, http.MethodDelete, ts.URL+"/api/v1/cmdb/model/switch", bearer, ""); code != http.StatusConflict || !strings.Contains(body, "CMDB_MODEL_IN_USE") {
		t.Fatalf("delete referenced model: status = %d body = %s", code, body)
	}
	createCI(t, ts, bearer, `{"modelCode":"switch","name":"sw-1"}`)
	if code, _ := doRequestWithToken(t, http.MethodDelete, ts.URL+"/api/v1/cmdb/relation-type/host_uplink", bearer, ""); code != http.StatusOK {
		t.Fatalf("delete unused relation type: status = %d", code)
	}
	if code, body := doRequestWithToken(t, http.MethodDelete, ts.URL+"/api/v1/cmdb/model/switch", bearer, ""); code != http.StatusConflict {
		t.Fatalf("delete model with resources: status = %d body = %s", code, body)
	}
}

// 属性校验与唯一性：必填、类型、枚举、范围、未知字段、名称与唯一键重复。
func TestCmdb_CIValidation(t *testing.T) {
	t.Parallel()
	ts, svc := newTestHTTPServerWithServices(t)
	bearer := testBearer(t, svc.signer, e2eActorID)
	cmdbSetup(t, ts, bearer)

	cases := map[string]string{
		`{"modelCode":"host","name":"h1"}`:                                       "required",
		`{"modelCode":"host","name":"h1","attrs":{"ip":"not-an-ip"}}`:            "IP",
		`{"modelCode":"host","name":"h1","attrs":{"ip":"10.0.0.1","cores":999}}`: "128",
		`{"modelCode":"host","name":"h1","attrs":{"ip":"10.0.0.1","cores":1.5}}`: "integer",
		`{"modelCode":"host","name":"h1","attrs":{"ip":"10.0.0.1","unknown":1}}`: "unknown field",
		`{"modelCode":"part","name":"p1","attrs":{"kind":"gpu"}}`:                "one of",
		`{"modelCode":"nope","name":"x"}`:                                        "CMDB_MODEL_NOT_FOUND",
	}
	for body, want := range cases {
		code, resp := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/cmdb/ci", bearer, body)
		if code == http.StatusOK || !strings.Contains(resp, want) {
			t.Fatalf("create %s: status = %d body = %s, want %q", body, code, resp, want)
		}
	}
	h1 := createCI(t, ts, bearer, `{"modelCode":"host","name":"h1","env":"prod","tags":["web"],"attrs":{"ip":"10.0.0.1","cores":16}}`)
	if h1.Status != "active" || h1.Version != "1" || h1.Attrs["cores"] != float64(16) {
		t.Fatalf("created ci = %+v", h1)
	}
	// 同名 409；同 IP（唯一字段）409；名称不同且 IP 不同则成功。
	if code, body := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/cmdb/ci", bearer, `{"modelCode":"host","name":"h1","attrs":{"ip":"10.0.0.2"}}`); code != http.StatusConflict || !strings.Contains(body, "CMDB_CI_DUPLICATE") {
		t.Fatalf("duplicate name: status = %d body = %s", code, body)
	}
	if code, body := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/cmdb/ci", bearer, `{"modelCode":"host","name":"h2","attrs":{"ip":"10.0.0.1"}}`); code != http.StatusConflict || !strings.Contains(body, "CMDB_CI_DUPLICATE") {
		t.Fatalf("duplicate unique key: status = %d body = %s", code, body)
	}
	createCI(t, ts, bearer, `{"modelCode":"host","name":"h2","attrs":{"ip":"10.0.0.2"}}`)
	// 列表：按类型 + 属性等值 + 标签过滤。
	code, body := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci?page=1&pageSize=10&modelCode=host&tag=web", bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"total":"1"`) || !strings.Contains(body, `"name":"h1"`) {
		t.Fatalf("list by tag: status = %d body = %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci?page=1&pageSize=10&modelCode=host&attrs.ip=10.0.0.2", bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"total":"1"`) || !strings.Contains(body, `"name":"h2"`) {
		t.Fatalf("list by attr: status = %d body = %s", code, body)
	}
}

// 乐观锁、属性合并、状态机、历史。
func TestCmdb_UpdateTransitionHistory(t *testing.T) {
	t.Parallel()
	ts, svc := newTestHTTPServerWithServices(t)
	bearer := testBearer(t, svc.signer, e2eActorID)
	cmdbSetup(t, ts, bearer)
	h1 := createCI(t, ts, bearer, `{"modelCode":"host","name":"h1","attrs":{"ip":"10.0.0.1","cores":16}}`)

	// 过期版本 409。
	if code, body := doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/cmdb/ci/"+h1.ID, bearer, `{"version":99,"name":"h1b"}`); code != http.StatusConflict || !strings.Contains(body, "CMDB_CI_VERSION_CONFLICT") {
		t.Fatalf("stale version: status = %d body = %s", code, body)
	}
	// 合并更新：改 cores，清空不存在的键无影响；版本 +1。
	code, body := doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/cmdb/ci/"+h1.ID, bearer, `{"version":1,"owner":"ops","attrs":{"cores":32}}`)
	if code != http.StatusOK || !strings.Contains(body, `"version":"2"`) || !strings.Contains(body, `"cores":32`) || !strings.Contains(body, `"ip":"10.0.0.1"`) {
		t.Fatalf("update: status = %d body = %s", code, body)
	}
	// 清空必填字段被拒绝。
	if code, body := doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/cmdb/ci/"+h1.ID, bearer, `{"version":2,"attrs":{"ip":null}}`); code != http.StatusBadRequest {
		t.Fatalf("clear required: status = %d body = %s", code, body)
	}
	// 无变化的更新不升版本、不写历史。
	code, body = doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/cmdb/ci/"+h1.ID, bearer, `{"version":2,"owner":"ops"}`)
	if code != http.StatusOK || !strings.Contains(body, `"version":"2"`) {
		t.Fatalf("no-op update: status = %d body = %s", code, body)
	}
	// 状态机：active → maintenance ok；maintenance → planning 400；→ retired ok；retired → active 400。
	transition := func(version, status string) (int, string) {
		return doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/cmdb/ci/"+h1.ID+":transition", bearer, `{"version":`+version+`,"status":"`+status+`"}`)
	}
	if code, body := transition("2", "maintenance"); code != http.StatusOK || !strings.Contains(body, `"status":"maintenance"`) {
		t.Fatalf("to maintenance: status = %d body = %s", code, body)
	}
	if code, body := transition("3", "planning"); code != http.StatusBadRequest || !strings.Contains(body, "CMDB_CI_TRANSITION_INVALID") {
		t.Fatalf("invalid transition: status = %d body = %s", code, body)
	}
	if code, _ := transition("3", "retired"); code != http.StatusOK {
		t.Fatalf("retire: status = %d", code)
	}
	if code, _ := transition("4", "active"); code != http.StatusBadRequest {
		t.Fatalf("revive retired: status = %d", code)
	}
	// 历史：create / update / transition×2，倒序。
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci/"+h1.ID+"/history?page=1&pageSize=10", bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"total":"4"`) {
		t.Fatalf("history: status = %d body = %s", code, body)
	}
	var hist struct {
		Items []struct {
			Action   string         `json:"action"`
			Operator string         `json:"operator"`
			Diff     map[string]any `json:"diff"`
		} `json:"items"`
	}
	if err := json.Unmarshal([]byte(body), &hist); err != nil {
		t.Fatal(err)
	}
	if hist.Items[0].Action != "transition" || hist.Items[3].Action != "create" || hist.Items[2].Action != "update" || hist.Items[2].Operator != e2eActorID {
		t.Fatalf("history order/operator wrong: %+v", hist.Items)
	}
	after := hist.Items[2].Diff["after"].(map[string]any)
	if after["owner"] != "ops" || after["attrs"].(map[string]any)["cores"] != float64(32) {
		t.Fatalf("update diff wrong: %v", hist.Items[2].Diff)
	}
}

// 关联：类型/基数校验、双向展示、统计、解除、迁移、级联退役、删除保护。
func TestCmdb_Relations(t *testing.T) {
	t.Parallel()
	ts, svc := newTestHTTPServerWithServices(t)
	bearer := testBearer(t, svc.signer, e2eActorID)
	cmdbSetup(t, ts, bearer)
	h1 := createCI(t, ts, bearer, `{"modelCode":"host","name":"h1","attrs":{"ip":"10.0.0.1"}}`)
	h2 := createCI(t, ts, bearer, `{"modelCode":"host","name":"h2","attrs":{"ip":"10.0.0.2"}}`)
	p1 := createCI(t, ts, bearer, `{"modelCode":"part","name":"mem-1","attrs":{"kind":"memory"}}`)
	p2 := createCI(t, ts, bearer, `{"modelCode":"part","name":"disk-1","attrs":{"kind":"disk"}}`)
	sw := createCI(t, ts, bearer, `{"modelCode":"switch","name":"sw-1"}`)
	link := func(ci, typ, dir string, targets ...string) (int, string) {
		return doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/cmdb/ci/"+ci+"/relation", bearer,
			`{"relationType":"`+typ+`","direction":"`+dir+`","targetIds":["`+strings.Join(targets, `","`)+`"]}`)
	}

	// 类型不匹配：把交换机当配件装到主机上 400。
	if code, body := link(h1.ID, "host_has_part", "outgoing", sw.ID); code != http.StatusBadRequest || !strings.Contains(body, "CMDB_RELATION_NOT_ALLOWED") {
		t.Fatalf("wrong model: status = %d body = %s", code, body)
	}
	// 主机视角批量安装两个配件。
	if code, body := link(h1.ID, "host_has_part", "outgoing", p1.ID, p2.ID); code != http.StatusOK || !strings.Contains(body, `"linked":2`) {
		t.Fatalf("link parts: status = %d body = %s", code, body)
	}
	// 一对多：配件已装在 h1，再装到 h2 → 409。
	if code, body := link(h2.ID, "host_has_part", "outgoing", p1.ID); code != http.StatusConflict || !strings.Contains(body, "CMDB_RELATION_CARDINALITY") {
		t.Fatalf("cardinality: status = %d body = %s", code, body)
	}
	// 重复关联跳过（linked 0）。
	if code, body := link(h1.ID, "host_has_part", "outgoing", p1.ID); code != http.StatusOK || !strings.Contains(body, `"linked":0`) {
		t.Fatalf("relink: status = %d body = %s", code, body)
	}
	// 多对多：两台主机上联同一交换机，从交换机视角 incoming 关联。
	if code, body := link(sw.ID, "host_uplink", "incoming", h1.ID, h2.ID); code != http.StatusOK || !strings.Contains(body, `"linked":2`) {
		t.Fatalf("uplink: status = %d body = %s", code, body)
	}
	// 详情统计：h1 安装的配件 2、上联交换机 1；p1 反向"安装于" 1。
	_, body := getCI(t, ts, bearer, h1.ID)
	parts, ok := summaryCount(t, body, "host_has_part", "outgoing")
	if !ok || parts.Count != "2" || parts.Name != "安装的配件" || parts.TargetModel != "part" {
		t.Fatalf("host parts summary wrong: %+v (%s)", parts, body)
	}
	if up, ok := summaryCount(t, body, "host_uplink", "outgoing"); !ok || up.Count != "1" || up.Name != "上联交换机" {
		t.Fatalf("host uplink summary wrong: %+v", up)
	}
	_, body = getCI(t, ts, bearer, p1.ID)
	if in, ok := summaryCount(t, body, "host_has_part", "incoming"); !ok || in.Count != "1" || in.Name != "安装于" || in.TargetModel != "host" {
		t.Fatalf("part summary wrong: %+v", in)
	}
	// 关联列表：主机详情的配件面板含对端属性；配件详情反向面板是主机。
	code, body := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci/"+h1.ID+"/related?relationType=host_has_part&direction=outgoing&page=1&pageSize=10", bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"total":"2"`) || !strings.Contains(body, `"kind":"memory"`) {
		t.Fatalf("related parts: status = %d body = %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci/"+p1.ID+"/related?relationType=host_has_part&direction=incoming&page=1&pageSize=10&keyword=h1", bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"total":"1"`) || !strings.Contains(body, `"name":"h1"`) {
		t.Fatalf("related host: status = %d body = %s", code, body)
	}
	// 迁移：p1 从 h1 迁到 h2（配件视角 incoming）。
	code, body = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/cmdb/ci/"+p1.ID+"/relation:move", bearer,
		`{"relationType":"host_has_part","direction":"incoming","fromId":"`+h1.ID+`","toId":"`+h2.ID+`"}`)
	if code != http.StatusOK {
		t.Fatalf("move: status = %d body = %s", code, body)
	}
	_, body = getCI(t, ts, bearer, h2.ID)
	if moved, ok := summaryCount(t, body, "host_has_part", "outgoing"); !ok || moved.Count != "1" {
		t.Fatalf("after move h2 summary wrong: %+v", moved)
	}
	_, body = getCI(t, ts, bearer, h1.ID)
	if left, ok := summaryCount(t, body, "host_has_part", "outgoing"); !ok || left.Count != "1" {
		t.Fatalf("after move h1 summary wrong: %+v", left)
	}
	// 删除保护：仍有关系的资源不能删。
	if code, body := doRequestWithToken(t, http.MethodDelete, ts.URL+"/api/v1/cmdb/ci/"+p2.ID, bearer, ""); code != http.StatusConflict || !strings.Contains(body, "CMDB_CI_IN_USE") {
		t.Fatalf("delete linked ci: status = %d body = %s", code, body)
	}
	// 解除后可删；解除不存在的关系 404。
	code, body = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/cmdb/ci/"+p2.ID+"/relation:unlink", bearer,
		`{"relationType":"host_has_part","direction":"incoming","targetId":"`+h1.ID+`"}`)
	if code != http.StatusOK {
		t.Fatalf("unlink: status = %d body = %s", code, body)
	}
	if code, _ := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/cmdb/ci/"+p2.ID+"/relation:unlink", bearer,
		`{"relationType":"host_has_part","direction":"incoming","targetId":"`+h1.ID+`"}`); code != http.StatusNotFound {
		t.Fatalf("unlink twice: status = %d", code)
	}
	if code, _ := doRequestWithToken(t, http.MethodDelete, ts.URL+"/api/v1/cmdb/ci/"+p2.ID, bearer, ""); code != http.StatusOK {
		t.Fatalf("delete unlinked ci: status = %d", code)
	}
	// 级联退役：h2 退役 → p1 随之退役；restrict 关系（上联交换机）不阻止（它是 h2 的 outgoing 且 restrict）→ 应 409。
	h2cur, _ := getCI(t, ts, bearer, h2.ID)
	code, body = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/cmdb/ci/"+h2.ID+":transition", bearer, `{"version":`+h2cur.Version+`,"status":"retired"}`)
	if code != http.StatusConflict || !strings.Contains(body, "CMDB_CI_IN_USE") {
		t.Fatalf("retire with restrict relation: status = %d body = %s", code, body)
	}
	// 解除上联后退役成功且配件级联退役。
	if code, _ := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/cmdb/ci/"+h2.ID+"/relation:unlink", bearer,
		`{"relationType":"host_uplink","direction":"outgoing","targetId":"`+sw.ID+`"}`); code != http.StatusOK {
		t.Fatalf("unlink uplink: status = %d", code)
	}
	h2cur, _ = getCI(t, ts, bearer, h2.ID)
	if code, body := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/cmdb/ci/"+h2.ID+":transition", bearer, `{"version":`+h2cur.Version+`,"status":"retired"}`); code != http.StatusOK {
		t.Fatalf("retire cascade: status = %d body = %s", code, body)
	}
	if p1cur, _ := getCI(t, ts, bearer, p1.ID); p1cur.Status != "retired" {
		t.Fatalf("part must be cascade-retired, status = %s", p1cur.Status)
	}
	// 关系历史在两端都有记录。
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci/"+p1.ID+"/history?page=1&pageSize=20", bearer, "")
	if code != http.StatusOK || strings.Count(body, `"action":"link"`) < 2 || strings.Count(body, `"action":"unlink"`) < 1 {
		t.Fatalf("relation history on part: %s", body)
	}
}

// 入口授权：未授 cmdb 接口的用户 403；仅授查询的用户不能写。
func TestCmdb_Permissions(t *testing.T) {
	t.Parallel()
	ts, svc := newTestHTTPServerWithServices(t)
	admin := testBearer(t, svc.signer, e2eActorID)
	cmdbSetup(t, ts, admin)
	viewer := createUser(t, ts, admin, "cmdb_viewer")
	viewerBearer := testBearer(t, svc.signer, viewer.Id)
	if code, _ := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/model?page=1&pageSize=10", viewerBearer, ""); code != http.StatusForbidden {
		t.Fatalf("ungranted list models: status = %d", code)
	}
	role := createRole(t, ts, admin, "CMDB 只读", "cmdb_viewer")
	code, body := doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/system/role/"+role.Id+"/permission", admin,
		`{"menus":["cmdb.ci"],"apis":["`+opCode("/cmdb.v1.CmdbCIService/ListCIs")+`","`+opCode("/cmdb.v1.CmdbModelService/ListModels")+`"]}`)
	if code != http.StatusOK {
		t.Fatalf("grant: status = %d body = %s", code, body)
	}
	if code, body := doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/system/user/"+viewer.Id+"/role", admin, `{"role_ids":["`+role.Id+`"]}`); code != http.StatusOK {
		t.Fatalf("bind: status = %d body = %s", code, body)
	}
	if code, _ := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci?page=1&pageSize=10", viewerBearer, ""); code != http.StatusOK {
		t.Fatalf("granted list cis: status = %d", code)
	}
	if code, _ := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/cmdb/ci", viewerBearer, `{"modelCode":"switch","name":"x"}`); code != http.StatusForbidden {
		t.Fatalf("viewer create ci: status = %d", code)
	}
}
