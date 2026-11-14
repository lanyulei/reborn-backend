package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

type batchJSON struct {
	Succeeded int `json:"succeeded"`
	Failed    int `json:"failed"`
	Results   []struct {
		ID      string `json:"id"`
		Name    string `json:"name"`
		Ok      bool   `json:"ok"`
		Message string `json:"message"`
	} `json:"results"`
}

// 回收站 / 恢复、批量操作、负责人（HTTP 全链路）。
func TestCmdb_RecycleBatchOwner(t *testing.T) {
	t.Parallel()
	ts, svc := newTestHTTPServerWithServices(t)
	admin := testBearer(t, svc.signer, e2eActorID)
	cmdbSetup(t, ts, admin) // host：ip（规则 IP）、cores；part：kind
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
	batch := func(url, body string) batchJSON {
		t.Helper()
		var b batchJSON
		if err := json.Unmarshal([]byte(mustOK(http.MethodPost, url, body)), &b); err != nil {
			t.Fatal(err)
		}
		return b
	}
	ids := func(items ...ciJSON) string {
		out := make([]string, 0, len(items))
		for _, c := range items {
			out = append(out, `"`+c.ID+`"`)
		}
		return "[" + strings.Join(out, ",") + "]"
	}
	const missing = "01a1b987-129c-7186-9a25-000000000000"

	h1 := createCI(t, ts, admin, `{"modelCode":"host","name":"h1","env":"prod","tags":["web"],"attrs":{"ip":"10.0.0.1"}}`)
	h2 := createCI(t, ts, admin, `{"modelCode":"host","name":"h2","env":"prod","tags":["web","old"],"attrs":{"ip":"10.0.0.2"}}`)
	h3 := createCI(t, ts, admin, `{"modelCode":"host","name":"h3","env":"test","attrs":{"ip":"10.0.0.3"}}`)

	// ---- 负责人：候选只含 ID 与显示名；按负责人筛选。
	owners := mustOK(http.MethodGet, "/api/v1/cmdb/owner-candidate", "")
	if !strings.Contains(owners, `"name"`) || strings.Contains(owners, "email") || strings.Contains(owners, "phone") {
		t.Fatalf("owner candidates must only expose id and name: %s", owners)
	}

	// ---- 批量编辑：负责人 + 标签增删 + 属性；不存在的 ID 单独失败、不影响其它条。
	res := batch("/api/v1/cmdb/ci:batchUpdate", `{"ids":["`+h1.ID+`","`+h2.ID+`","`+missing+`"],"owner":"`+e2eActorID+`","addTags":["batch"],"removeTags":["old"],"attrs":{"cores":8}}`)
	if res.Succeeded != 2 || res.Failed != 1 || len(res.Results) != 3 || res.Results[2].Ok || res.Results[2].Message == "" || res.Results[0].Name != "h1" {
		t.Fatalf("batch update = %+v", res)
	}
	g2, _ := getCI(t, ts, admin, h2.ID)
	if g2.Attrs["cores"] != float64(8) || g2.Attrs["ip"] != "10.0.0.2" {
		t.Fatalf("h2 after batch = %+v", g2)
	}
	body := mustOK(http.MethodGet, "/api/v1/cmdb/ci/"+h2.ID, "")
	norm := strings.ReplaceAll(body, ", ", ",")
	if !strings.Contains(norm, `"tags":["web","batch"]`) || !strings.Contains(norm, `"owner":"`+e2eActorID+`"`) {
		t.Fatalf("h2 tags/owner after batch: %s", body)
	}
	list := mustOK(http.MethodGet, "/api/v1/cmdb/ci?page=1&pageSize=20&owner="+e2eActorID, "")
	if !strings.Contains(list, `"total":"2"`) || strings.Contains(list, `"name":"h3"`) {
		t.Fatalf("filter by owner: %s", list)
	}
	// 属性不合法（未知字段）逐条报错；撞唯一规则逐条报错并指明规则。
	res = batch("/api/v1/cmdb/ci:batchUpdate", `{"ids":`+ids(h1, h2)+`,"attrs":{"nope":1}}`)
	if res.Failed != 2 || !strings.Contains(res.Results[0].Message, "nope") {
		t.Fatalf("batch invalid attrs = %+v", res)
	}
	res = batch("/api/v1/cmdb/ci:batchUpdate", `{"ids":`+ids(h1, h2)+`,"attrs":{"ip":"10.0.0.9"}}`)
	if res.Succeeded != 1 || res.Failed != 1 || !strings.Contains(res.Results[1].Message, "IP") {
		t.Fatalf("batch unique conflict = %+v", res)
	}
	// 请求校验：空 / 重复 / 超过 100 个 ID。
	for name, b := range map[string]string{"empty": `{"ids":[]}`, "dup": `{"ids":["` + h1.ID + `","` + h1.ID + `"]}`, "bad id": `{"ids":["x"]}`} {
		if code, resp := do(http.MethodPost, "/api/v1/cmdb/ci:batchDelete", b); code != http.StatusBadRequest {
			t.Fatalf("%s: status = %d body = %s", name, code, resp)
		}
	}

	// ---- 批量状态流转：非法流转逐条失败；已在目标状态视为成功。
	res = batch("/api/v1/cmdb/ci:batchTransition", `{"ids":`+ids(h1, h2, h3)+`,"status":"maintenance"}`)
	if res.Succeeded != 3 {
		t.Fatalf("batch transition = %+v", res)
	}
	res = batch("/api/v1/cmdb/ci:batchTransition", `{"ids":`+ids(h1)+`,"status":"maintenance"}`)
	if res.Succeeded != 1 {
		t.Fatalf("batch transition to same status = %+v", res)
	}
	res = batch("/api/v1/cmdb/ci:batchTransition", `{"ids":`+ids(h1)+`,"status":"planning"}`)
	if res.Failed != 1 || res.Results[0].Message == "" {
		t.Fatalf("batch invalid transition = %+v", res)
	}

	// ---- 批量删除：有关联的资源拒绝，其余进回收站。
	p1 := createCI(t, ts, admin, `{"modelCode":"part","name":"p1","attrs":{"kind":"memory"}}`)
	mustOK(http.MethodPost, "/api/v1/cmdb/ci/"+h1.ID+"/relation", `{"relationType":"host_has_part","direction":"outgoing","targetIds":["`+p1.ID+`"]}`)
	res = batch("/api/v1/cmdb/ci:batchDelete", `{"ids":`+ids(h1, h2, h3)+`}`)
	if res.Succeeded != 2 || res.Failed != 1 || res.Results[0].Ok || !strings.Contains(res.Results[0].Message, "relation") {
		t.Fatalf("batch delete = %+v", res)
	}
	if code, _ := do(http.MethodGet, "/api/v1/cmdb/ci/"+h2.ID, ""); code != http.StatusNotFound {
		t.Fatalf("deleted ci get: status = %d", code)
	}

	// ---- 回收站：只列已删除；缺省列表不含已删除。
	bin := mustOK(http.MethodGet, "/api/v1/cmdb/ci?page=1&pageSize=20&deleted=true&modelCode=host", "")
	if !strings.Contains(bin, `"total":"2"`) || !strings.Contains(bin, `"name":"h2"`) || !strings.Contains(bin, `"name":"h3"`) || strings.Contains(bin, `"name":"h1"`) {
		t.Fatalf("recycle bin: %s", bin)
	}
	if live := mustOK(http.MethodGet, "/api/v1/cmdb/ci?page=1&pageSize=20&modelCode=host", ""); !strings.Contains(live, `"total":"1"`) {
		t.Fatalf("live list: %s", live)
	}

	// ---- 恢复：名称 / 唯一规则被占用时拒绝；否则恢复并记历史；未删除的资源不能"恢复"。
	taker := createCI(t, ts, admin, `{"modelCode":"host","name":"h2","attrs":{"ip":"10.0.9.9"}}`)
	if code, resp := do(http.MethodPost, "/api/v1/cmdb/ci/"+h2.ID+":restore", `{}`); code != http.StatusConflict || !strings.Contains(resp, "CMDB_CI_DUPLICATE") || !strings.Contains(resp, "name") {
		t.Fatalf("restore with taken name: status = %d body = %s", code, resp)
	}
	mustOK(http.MethodPut, "/api/v1/cmdb/ci/"+taker.ID, `{"version":1,"name":"h2-new","attrs":{"ip":"10.0.0.2"}}`)
	if code, resp := do(http.MethodPost, "/api/v1/cmdb/ci/"+h2.ID+":restore", `{}`); code != http.StatusConflict || !strings.Contains(resp, "IP") {
		t.Fatalf("restore with taken unique value: status = %d body = %s", code, resp)
	}
	mustOK(http.MethodPost, "/api/v1/cmdb/ci/"+h3.ID+":restore", `{}`)
	if code, _ := do(http.MethodGet, "/api/v1/cmdb/ci/"+h3.ID, ""); code != http.StatusOK {
		t.Fatalf("restored ci get: status = %d", code)
	}
	if hist := mustOK(http.MethodGet, "/api/v1/cmdb/ci/"+h3.ID+"/history?page=1&pageSize=20", ""); !strings.Contains(hist, `"action":"restore"`) || !strings.Contains(hist, `"action":"delete"`) {
		t.Fatalf("restore history: %s", hist)
	}
	if code, _ := do(http.MethodPost, "/api/v1/cmdb/ci/"+h3.ID+":restore", `{}`); code != http.StatusNotFound {
		t.Fatalf("restore a live ci: status = %d", code)
	}
	// 恢复后的值重新受唯一规则保护。
	if code, _ := do(http.MethodPost, "/api/v1/cmdb/ci", `{"modelCode":"host","name":"h3b","attrs":{"ip":"10.0.0.3"}}`); code != http.StatusConflict {
		t.Fatalf("restored value must be protected again: status = %d", code)
	}

	// ---- 导出与列表同一组筛选（含负责人）。
	if csv := mustOK(http.MethodGet, "/api/v1/cmdb/ci:export?modelCode=host&owner="+e2eActorID, ""); !strings.Contains(csv, "h1") || strings.Contains(csv, "h3") {
		t.Fatalf("export must honour the owner filter: %s", csv)
	}

	// ---- 数据范围：只能看 part 的用户，对 host 的回收站 / 恢复 / 批量操作一律不可见、不可改。
	scoped := createUser(t, ts, admin, "batch_scoped")
	sb := testBearer(t, svc.signer, scoped.Id)
	srole := createRole(t, ts, admin, "配件管理", "batch_scoped")
	apis := make([]string, 0, 8)
	for _, op := range []string{"ListCIs", "RestoreCI", "BatchUpdateCIs", "BatchTransitionCIs", "BatchDeleteCIs"} {
		apis = append(apis, `"`+opCode("/cmdb.v1.CmdbCIService/"+op)+`"`)
	}
	mustOK(http.MethodPut, "/api/v1/system/role/"+srole.Id+"/permission", `{"menus":["cmdb.ci"],"apis":[`+strings.Join(apis, ",")+`]}`)
	mustOK(http.MethodPut, "/api/v1/system/user/"+scoped.Id+"/role", `{"role_ids":["`+srole.Id+`"]}`)
	mustOK(http.MethodPut, "/api/v1/cmdb/scope/role/"+srole.Id, `{"modelCodes":["part"]}`)
	if code, resp := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci?page=1&pageSize=20&deleted=true&modelCode=host", sb, ""); code != http.StatusOK || strings.Contains(resp, "h2") {
		t.Fatalf("scoped recycle bin must hide out-of-scope resources: %d %s", code, resp)
	}
	if code, _ := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/cmdb/ci/"+h2.ID+":restore", sb, `{}`); code != http.StatusForbidden {
		t.Fatalf("scoped restore: status = %d", code)
	}
	for _, req := range [][2]string{
		{"/api/v1/cmdb/ci:batchUpdate", `{"ids":["` + h1.ID + `"],"env":"x"}`},
		{"/api/v1/cmdb/ci:batchTransition", `{"ids":["` + h1.ID + `"],"status":"active"}`},
		{"/api/v1/cmdb/ci:batchDelete", `{"ids":["` + h1.ID + `"]}`},
	} {
		code, resp := doRequestWithToken(t, http.MethodPost, ts.URL+req[0], sb, req[1])
		var b batchJSON
		_ = json.Unmarshal([]byte(resp), &b)
		if code != http.StatusOK || b.Failed != 1 || b.Results[0].Ok || b.Results[0].Name != "" {
			t.Fatalf("scoped %s must fail per item without leaking the name: %d %s", req[0], code, resp)
		}
	}
	if g, _ := getCI(t, ts, admin, h1.ID); g.Status != "maintenance" {
		t.Fatalf("out-of-scope batch must not change the resource: %+v", g)
	}

	// ---- 授权：只读角色不能批量写 / 恢复，但能取负责人候选。
	viewer := createUser(t, ts, admin, "batch_viewer")
	vb := testBearer(t, svc.signer, viewer.Id)
	role := createRole(t, ts, admin, "资源只读", "batch_viewer")
	mustOK(http.MethodPut, "/api/v1/system/role/"+role.Id+"/permission",
		`{"menus":["cmdb.ci"],"apis":["`+opCode("/cmdb.v1.CmdbCIService/ListCIs")+`","`+opCode("/cmdb.v1.CmdbCIService/ListOwnerCandidates")+`"]}`)
	mustOK(http.MethodPut, "/api/v1/system/user/"+viewer.Id+"/role", `{"role_ids":["`+role.Id+`"]}`)
	if code, _ := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/owner-candidate", vb, ""); code != http.StatusOK {
		t.Fatalf("viewer owner candidates: status = %d", code)
	}
	for _, req := range [][2]string{
		{"/api/v1/cmdb/ci:batchUpdate", `{"ids":["` + h1.ID + `"],"owner":"x"}`},
		{"/api/v1/cmdb/ci:batchTransition", `{"ids":["` + h1.ID + `"],"status":"active"}`},
		{"/api/v1/cmdb/ci:batchDelete", `{"ids":["` + h1.ID + `"]}`},
		{"/api/v1/cmdb/ci/" + h2.ID + ":restore", `{}`},
	} {
		if code, _ := doRequestWithToken(t, http.MethodPost, ts.URL+req[0], vb, req[1]); code != http.StatusForbidden {
			t.Fatalf("viewer POST %s: status = %d, want 403", req[0], code)
		}
	}
}

// 数据质量：看板计数与列表的 issue 筛选同口径；退役资源不计；受数据范围约束。
func TestCmdb_Quality(t *testing.T) {
	t.Parallel()
	ts, svc := newTestHTTPServerWithServices(t)
	admin := testBearer(t, svc.signer, e2eActorID)
	cmdbSetup(t, ts, admin) // host：ip 必填；part：kind 必填
	mustOK := func(method, url, body string) string {
		t.Helper()
		code, resp := doRequestWithToken(t, method, ts.URL+url, admin, body)
		if code != http.StatusOK {
			t.Fatalf("%s %s: status = %d body = %s", method, url, code, resp)
		}
		return resp
	}
	h1 := createCI(t, ts, admin, `{"modelCode":"host","name":"q1","owner":"someone","attrs":{"ip":"10.1.0.1"}}`)
	createCI(t, ts, admin, `{"modelCode":"host","name":"q2","attrs":{"ip":"10.1.0.2"}}`)
	h3 := createCI(t, ts, admin, `{"modelCode":"host","name":"q3","attrs":{"ip":"10.1.0.3"}}`)
	p1 := createCI(t, ts, admin, `{"modelCode":"part","name":"qp","owner":"someone","attrs":{"kind":"disk"}}`)
	mustOK(http.MethodPost, "/api/v1/cmdb/ci/"+h1.ID+"/relation", `{"relationType":"host_has_part","direction":"outgoing","targetIds":["`+p1.ID+`"]}`)
	// q3 退役：不再计入任何问题。
	mustOK(http.MethodPost, "/api/v1/cmdb/ci/"+h3.ID+":transition", `{"version":1,"status":"retired"}`)

	type row struct {
		ModelCode       string `json:"modelCode"`
		Total           string `json:"total"`
		MissingRequired string `json:"missingRequired"`
		NoOwner         string `json:"noOwner"`
		Orphan          string `json:"orphan"`
		Stale           string `json:"stale"`
		Outdated        string `json:"outdated"`
	}
	var q struct {
		Items        []row `json:"items"`
		OutdatedDays int   `json:"outdatedDays"`
	}
	if err := json.Unmarshal([]byte(mustOK(http.MethodGet, "/api/v1/cmdb/quality", "")), &q); err != nil {
		t.Fatal(err)
	}
	byModel := map[string]row{}
	for _, r := range q.Items {
		byModel[r.ModelCode] = r
	}
	// host 在册 2（q1、q2）：q2 无负责人且无关联；q1 有负责人、有关联。
	if h := byModel["host"]; h.Total != "2" || h.NoOwner != "1" || h.Orphan != "1" || (h.MissingRequired != "" && h.MissingRequired != "0") {
		t.Fatalf("host quality = %+v (all: %+v)", h, q)
	}
	if p := byModel["part"]; p.Total != "1" || (p.Orphan != "" && p.Orphan != "0") || (p.NoOwner != "" && p.NoOwner != "0") {
		t.Fatalf("part quality = %+v", p)
	}
	if q.OutdatedDays != 180 || q.Items[0].ModelCode != "host" {
		t.Fatalf("quality meta / order = %+v", q)
	}
	// 列表筛选与看板同口径。
	for issue, want := range map[string]string{"no_owner": "q2", "orphan": "q2"} {
		list := mustOK(http.MethodGet, "/api/v1/cmdb/ci?page=1&pageSize=20&modelCode=host&issue="+issue, "")
		if !strings.Contains(list, `"total":"1"`) || !strings.Contains(list, `"name":"`+want+`"`) {
			t.Fatalf("issue=%s list: %s", issue, list)
		}
	}
	if code, _ := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci?page=1&pageSize=20&issue=bogus", admin, ""); code != http.StatusBadRequest {
		t.Fatalf("unknown issue: status = %d", code)
	}

	// 数据范围：只能看 part 的用户看不到 host 的质量数据。
	user := createUser(t, ts, admin, "quality_scoped")
	role := createRole(t, ts, admin, "配件质量", "quality_scoped")
	mustOK(http.MethodPut, "/api/v1/system/role/"+role.Id+"/permission", `{"menus":["cmdb.ci"],"apis":["`+opCode("/cmdb.v1.CmdbInsightService/GetQuality")+`"]}`)
	mustOK(http.MethodPut, "/api/v1/system/user/"+user.Id+"/role", `{"role_ids":["`+role.Id+`"]}`)
	mustOK(http.MethodPut, "/api/v1/cmdb/scope/role/"+role.Id, `{"modelCodes":["part"]}`)
	code, resp := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/quality", testBearer(t, svc.signer, user.Id), "")
	if code != http.StatusOK || strings.Contains(resp, `"host"`) || !strings.Contains(resp, `"part"`) {
		t.Fatalf("scoped quality: %d %s", code, resp)
	}
}
