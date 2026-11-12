package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// 详情布局：缺省生成、校验引用、保存后原样返回。
func TestCmdb_ModelView(t *testing.T) {
	t.Parallel()
	ts, svc := newTestHTTPServerWithServices(t)
	bearer := testBearer(t, svc.signer, e2eActorID)
	cmdbSetup(t, ts, bearer)

	code, body := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/model/host/view", bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"configured":false`) || !strings.Contains(body, `"host_has_part"`) || !strings.Contains(body, `"cores"`) {
		t.Fatalf("default view: status = %d body = %s", code, body)
	}
	bad := []string{
		`{"sections":[{"type":"attrs","fields":["nope"]}]}`,
		`{"sections":[{"type":"attrs","fields":["ip"]},{"type":"attrs","fields":["ip"]}]}`,
		`{"sections":[{"type":"relation","relationType":"host_has_part","direction":"incoming"}]}`,
		`{"sections":[{"type":"relation","relationType":"host_has_part","direction":"outgoing","columns":["nope"]}]}`,
	}
	for _, b := range bad {
		if code, body := doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/cmdb/model/host/view", bearer, b); code != http.StatusBadRequest || !strings.Contains(body, "CMDB_VIEW_INVALID") {
			t.Fatalf("invalid view %s: status = %d body = %s", b, code, body)
		}
	}
	good := `{"sections":[{"type":"attrs","title":"网络","fields":["ip"]},{"type":"attrs","title":"规格","fields":["cores"]},{"type":"relation","relationType":"host_has_part","direction":"outgoing","title":"配件","columns":["kind"],"collapsed":true}]}`
	if code, body := doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/cmdb/model/host/view", bearer, good); code != http.StatusOK {
		t.Fatalf("save view: status = %d body = %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/model/host/view", bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"configured":true`) || !strings.Contains(body, `"title":"规格"`) || !strings.Contains(body, `"collapsed":true`) {
		t.Fatalf("saved view: status = %d body = %s", code, body)
	}
}

// 数据范围：受限角色只能看到/操作范围内的类型与环境；不限角色行为与引入前一致。
func TestCmdb_DataScope(t *testing.T) {
	t.Parallel()
	ts, svc := newTestHTTPServerWithServices(t)
	admin := testBearer(t, svc.signer, e2eActorID)
	cmdbSetup(t, ts, admin)
	hostProd := createCI(t, ts, admin, `{"modelCode":"host","name":"h-prod","env":"prod","attrs":{"ip":"10.0.0.1"}}`)
	hostTest := createCI(t, ts, admin, `{"modelCode":"host","name":"h-test","env":"test","attrs":{"ip":"10.0.0.2"}}`)
	part := createCI(t, ts, admin, `{"modelCode":"part","name":"p1","env":"prod","attrs":{"kind":"memory"}}`)
	sw := createCI(t, ts, admin, `{"modelCode":"switch","name":"sw-1","env":"prod"}`)
	if code, _ := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/cmdb/ci/"+hostProd.ID+"/relation", admin,
		`{"relationType":"host_has_part","direction":"outgoing","targetIds":["`+part.ID+`"]}`); code != http.StatusOK {
		t.Fatal("link part")
	}
	if code, _ := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/cmdb/ci/"+hostProd.ID+"/relation", admin,
		`{"relationType":"host_uplink","direction":"outgoing","targetIds":["`+sw.ID+`"]}`); code != http.StatusOK {
		t.Fatal("link uplink")
	}

	// 受限用户：角色可见 host + part，环境仅 prod。
	user := createUser(t, ts, admin, "scoped_user")
	grantAssistantRole(t, ts, admin, user.Id, "cmdb_scoped",
		"/cmdb.v1.CmdbCIService/ListCIs", "/cmdb.v1.CmdbCIService/GetCI", "/cmdb.v1.CmdbCIService/CreateCI",
		"/cmdb.v1.CmdbCIService/UpdateCI", "/cmdb.v1.CmdbRelationService/ListRelated", "/cmdb.v1.CmdbRelationService/LinkCIs",
		"/cmdb.v1.CmdbScopeService/GetMyScope", "/cmdb.v1.CmdbCIService/ExportCIs", "/cmdb.v1.CmdbCIService/ImportCIs")
	userBearer := testBearer(t, svc.signer, user.Id)
	// 范围未配置 = 不限：能看到 test 环境主机。
	if code, body := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci/"+hostTest.ID, userBearer, ""); code != http.StatusOK {
		t.Fatalf("unconfigured scope must be unrestricted: %d %s", code, body)
	}
	// 配置范围：取角色 ID。
	code, body := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/scope/role", admin, "")
	if code != http.StatusOK {
		t.Fatalf("list role scopes: %d %s", code, body)
	}
	var roles struct {
		Items []struct {
			RoleID   string `json:"roleId"`
			RoleCode string `json:"roleCode"`
		} `json:"items"`
	}
	if err := json.Unmarshal([]byte(body), &roles); err != nil {
		t.Fatal(err)
	}
	var roleID string
	for _, r := range roles.Items {
		if r.RoleCode == "cmdb_scoped" {
			roleID = r.RoleID
		}
	}
	if roleID == "" {
		t.Fatalf("role not listed: %s", body)
	}
	if code, body := doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/cmdb/scope/role/"+roleID, admin, `{"modelCodes":["host","part"],"envs":["prod"]}`); code != http.StatusOK {
		t.Fatalf("set scope: %d %s", code, body)
	}
	if code, body := doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/cmdb/scope/role/"+roleID, admin, `{"modelCodes":["nope"]}`); code != http.StatusNotFound {
		t.Fatalf("set scope unknown model: %d %s", code, body)
	}
	// 我的范围。
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/scope/me", userBearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"unrestricted":false`) || !strings.Contains(body, `"host"`) || strings.Contains(body, `"switch"`) {
		t.Fatalf("my scope: %d %s", code, body)
	}
	// 列表：不带类型 → 只有 host/part 且 prod；带范围外类型 → 空。
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci?page=1&pageSize=10", userBearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"total":"2"`) || strings.Contains(body, "h-test") || strings.Contains(body, "sw-1") {
		t.Fatalf("scoped list: %d %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci?page=1&pageSize=10&modelCode=switch", userBearer, "")
	if code != http.StatusOK || strings.Contains(body, "sw-1") {
		t.Fatalf("out-of-scope model list must be empty: %d %s", code, body)
	}
	// 单资源：范围外 403；范围内 200。
	if code, body := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci/"+hostTest.ID, userBearer, ""); code != http.StatusForbidden || !strings.Contains(body, "CMDB_SCOPE_FORBIDDEN") {
		t.Fatalf("out-of-scope env get: %d %s", code, body)
	}
	if code, _ := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci/"+sw.ID, userBearer, ""); code != http.StatusForbidden {
		t.Fatalf("out-of-scope model get: %d", code)
	}
	if code, _ := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci/"+hostProd.ID, userBearer, ""); code != http.StatusOK {
		t.Fatalf("in-scope get: %d", code)
	}
	// 创建：范围外环境 403；范围内 200；更新把环境改出范围 403。
	if code, _ := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/cmdb/ci", userBearer, `{"modelCode":"host","name":"h-new","env":"test","attrs":{"ip":"10.0.0.9"}}`); code != http.StatusForbidden {
		t.Fatalf("create out-of-scope env: %d", code)
	}
	created := createCI(t, ts, userBearer, `{"modelCode":"host","name":"h-new","env":"prod","attrs":{"ip":"10.0.0.9"}}`)
	if code, _ := doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/cmdb/ci/"+created.ID, userBearer, `{"version":1,"env":"test"}`); code != http.StatusForbidden {
		t.Fatalf("move env out of scope: %d", code)
	}
	// 关联：目标在范围外（交换机）403；关联列表隐藏范围外对端。
	if code, _ := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/cmdb/ci/"+created.ID+"/relation", userBearer,
		`{"relationType":"host_uplink","direction":"outgoing","targetIds":["`+sw.ID+`"]}`); code != http.StatusForbidden {
		t.Fatalf("link out-of-scope target: %d", code)
	}
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci/"+hostProd.ID+"/related?relationType=host_uplink&direction=outgoing&page=1&pageSize=10", userBearer, "")
	if code != http.StatusOK || strings.Contains(body, "sw-1") || !strings.Contains(body, `"total":"0"`) {
		t.Fatalf("related must hide out-of-scope: %d %s", code, body)
	}
	// 导出：范围外类型 403；范围内只含 prod。
	if code, _ := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci:export?modelCode=switch", userBearer, ""); code != http.StatusForbidden {
		t.Fatalf("export out-of-scope: %d", code)
	}
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci:export?modelCode=host", userBearer, "")
	if code != http.StatusOK || strings.Contains(body, "h-test") || !strings.Contains(body, "h-prod") {
		t.Fatalf("scoped export: %d %s", code, body)
	}
	// 导入：含范围外环境的行整批拒绝。
	if code, _ := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/cmdb/ci:import", userBearer, `{"modelCode":"host","dryRun":true,"rows":[{"name":"x","env":"test","attrs":{"ip":"10.0.1.1"}}]}`); code != http.StatusForbidden {
		t.Fatalf("import out-of-scope env: %d", code)
	}
	// 超管不受影响。
	if code, body := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci?page=1&pageSize=10", admin, ""); code != http.StatusOK || !strings.Contains(body, `"total":"5"`) {
		t.Fatalf("admin unrestricted: %d %s", code, body)
	}
}
