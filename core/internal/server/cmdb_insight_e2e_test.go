package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// 模型分组：内置分组预置且不可删；自定义分组可建/改/删；模型的分组须存在；
// 分组下仍有模型时不可删。
func TestCmdb_ModelGroups(t *testing.T) {
	t.Parallel()
	ts, svc := newTestHTTPServerWithServices(t)
	bearer := testBearer(t, svc.signer, e2eActorID)
	do := func(method, url, body string) (int, string) {
		t.Helper()
		return doRequestWithToken(t, method, ts.URL+url, bearer, body)
	}

	code, body := do(http.MethodGet, "/api/v1/cmdb/model-group", "")
	if code != http.StatusOK || !strings.Contains(body, `"code":"compute"`) || !strings.Contains(body, `"code":"other"`) {
		t.Fatalf("list groups: status = %d body = %s", code, body)
	}
	if code, body := do(http.MethodDelete, "/api/v1/cmdb/model-group/compute", ""); code != http.StatusConflict {
		t.Fatalf("delete builtin group: status = %d body = %s", code, body)
	}
	if code, body := do(http.MethodPost, "/api/v1/cmdb/model-group", `{"code":"database","name":"数据库"}`); code != http.StatusOK {
		t.Fatalf("create group: status = %d body = %s", code, body)
	}
	if code, _ := do(http.MethodPost, "/api/v1/cmdb/model-group", `{"code":"database","name":"重复"}`); code != http.StatusConflict {
		t.Fatalf("duplicate group: status = %d", code)
	}
	// 模型的分组须存在。
	if code, body := do(http.MethodPost, "/api/v1/cmdb/model", `{"code":"mysql","name":"MySQL","category":"nope"}`); code != http.StatusNotFound {
		t.Fatalf("create model in missing group: status = %d body = %s", code, body)
	}
	if code, body := do(http.MethodPost, "/api/v1/cmdb/model", `{"code":"mysql","name":"MySQL","category":"database","icon":"database"}`); code != http.StatusOK {
		t.Fatalf("create model in custom group: status = %d body = %s", code, body)
	}
	if code, body := do(http.MethodPut, "/api/v1/cmdb/model/mysql", `{"category":"nope"}`); code != http.StatusNotFound {
		t.Fatalf("move model to missing group: status = %d body = %s", code, body)
	}
	// 分组下有模型：不可删；模型数随列表返回。
	if code, _ := do(http.MethodDelete, "/api/v1/cmdb/model-group/database", ""); code != http.StatusConflict {
		t.Fatalf("delete non-empty group: status = %d", code)
	}
	code, body = do(http.MethodGet, "/api/v1/cmdb/model-group", "")
	var groups struct {
		Items []struct {
			Code       string `json:"code"`
			Name       string `json:"name"`
			ModelCount string `json:"modelCount"`
		} `json:"items"`
	}
	if err := json.Unmarshal([]byte(body), &groups); err != nil || code != http.StatusOK {
		t.Fatalf("list groups: status = %d body = %s (%v)", code, body, err)
	}
	found := false
	for _, g := range groups.Items {
		if g.Code == "database" {
			found = g.ModelCount == "1"
		}
	}
	if !found {
		t.Fatalf("database group model count: %s", body)
	}
	if code, body := do(http.MethodPut, "/api/v1/cmdb/model-group/database", `{"name":"数据库服务","sort":60}`); code != http.StatusOK || !strings.Contains(body, "数据库服务") {
		t.Fatalf("update group: status = %d body = %s", code, body)
	}
	// 模型移走后分组可删。
	if code, body := do(http.MethodPut, "/api/v1/cmdb/model/mysql", `{"category":"other"}`); code != http.StatusOK {
		t.Fatalf("move model: status = %d body = %s", code, body)
	}
	if code, body := do(http.MethodDelete, "/api/v1/cmdb/model-group/database", ""); code != http.StatusOK {
		t.Fatalf("delete empty group: status = %d body = %s", code, body)
	}

	// 模型变更审计：分组 / 模型的增删改均有记录，含操作者与差异。
	code, body = do(http.MethodGet, "/api/v1/cmdb/schema-audit?page=1&pageSize=50", "")
	if code != http.StatusOK {
		t.Fatalf("schema audit: status = %d body = %s", code, body)
	}
	var audit struct {
		Total string `json:"total"`
		Items []struct {
			TargetType string         `json:"targetType"`
			TargetCode string         `json:"targetCode"`
			Action     string         `json:"action"`
			Operator   string         `json:"operator"`
			Diff       map[string]any `json:"diff"`
		} `json:"items"`
	}
	if err := json.Unmarshal([]byte(body), &audit); err != nil {
		t.Fatalf("schema audit body = %s (%v)", body, err)
	}
	seen := map[string]bool{}
	for _, a := range audit.Items {
		seen[a.TargetType+":"+a.TargetCode+":"+a.Action] = true
		if a.Operator != e2eActorID {
			t.Fatalf("audit operator = %q, want actor", a.Operator)
		}
	}
	for _, key := range []string{"group:database:create", "group:database:update", "group:database:delete", "model:mysql:create", "model:mysql:update"} {
		if !seen[key] {
			t.Fatalf("schema audit missing %s: %s", key, body)
		}
	}
	// 过滤：仅模型的 update；差异只含变化的键。
	code, body = do(http.MethodGet, "/api/v1/cmdb/schema-audit?page=1&pageSize=10&targetType=model&action=update&modelCode=mysql", "")
	if code != http.StatusOK || !strings.Contains(body, `"category":"other"`) || strings.Contains(body, `"icon"`) {
		t.Fatalf("filtered schema audit: status = %d body = %s", code, body)
	}
}

// 字段与关系类型变更也进模型变更审计。
func TestCmdb_SchemaAuditFieldsAndRelationTypes(t *testing.T) {
	t.Parallel()
	ts, svc := newTestHTTPServerWithServices(t)
	bearer := testBearer(t, svc.signer, e2eActorID)
	cmdbSetup(t, ts, bearer)
	code, body := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/schema-audit?page=1&pageSize=50&targetType=field&modelCode=host", bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"targetCode":"ip"`) || !strings.Contains(body, `"targetCode":"cores"`) {
		t.Fatalf("field audit: status = %d body = %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/schema-audit?page=1&pageSize=50&targetType=relation_type", bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"targetCode":"host_has_part"`) || !strings.Contains(body, `"targetCode":"host_uplink"`) {
		t.Fatalf("relation type audit: status = %d body = %s", code, body)
	}
}

// 收藏：按用户隔离、幂等；模型须存在。运营概览：总数与分布随资源变化。
func TestCmdb_FavoritesAndOverview(t *testing.T) {
	t.Parallel()
	ts, svc := newTestHTTPServerWithServices(t)
	bearer := testBearer(t, svc.signer, e2eActorID)
	cmdbSetup(t, ts, bearer)
	do := func(method, url, body string) (int, string) {
		t.Helper()
		return doRequestWithToken(t, method, ts.URL+url, bearer, body)
	}

	if code, body := do(http.MethodPut, "/api/v1/cmdb/favorite/nope", `{"favorite":true}`); code != http.StatusNotFound {
		t.Fatalf("favorite missing model: status = %d body = %s", code, body)
	}
	for range 2 { // 幂等
		if code, body := do(http.MethodPut, "/api/v1/cmdb/favorite/host", `{"favorite":true}`); code != http.StatusOK {
			t.Fatalf("favorite: status = %d body = %s", code, body)
		}
	}
	code, body := do(http.MethodGet, "/api/v1/cmdb/favorite", "")
	if code != http.StatusOK || strings.Count(body, `"host"`) != 1 {
		t.Fatalf("list favorites: status = %d body = %s", code, body)
	}
	if code, _ := do(http.MethodPut, "/api/v1/cmdb/favorite/host", `{"favorite":false}`); code != http.StatusOK {
		t.Fatalf("unfavorite: status = %d", code)
	}
	if code, body := do(http.MethodGet, "/api/v1/cmdb/favorite", ""); code != http.StatusOK || strings.Contains(body, `"host"`) {
		t.Fatalf("favorites after remove: %s", body)
	}

	h1 := createCI(t, ts, bearer, `{"modelCode":"host","name":"h1","env":"prod","attrs":{"ip":"10.0.0.1"}}`)
	createCI(t, ts, bearer, `{"modelCode":"host","name":"h2","env":"test","attrs":{"ip":"10.0.0.2"}}`)
	p1 := createCI(t, ts, bearer, `{"modelCode":"part","name":"p1","attrs":{"kind":"disk"}}`)
	if code, body := do(http.MethodPost, "/api/v1/cmdb/ci/"+h1.ID+"/relation", `{"relationType":"host_has_part","direction":"outgoing","targetIds":["`+p1.ID+`"]}`); code != http.StatusOK {
		t.Fatalf("link: status = %d body = %s", code, body)
	}
	code, body = do(http.MethodGet, "/api/v1/cmdb/overview?days=7", "")
	if code != http.StatusOK {
		t.Fatalf("overview: status = %d body = %s", code, body)
	}
	var ov struct {
		ModelCount    string `json:"modelCount"`
		CiCount       string `json:"ciCount"`
		RelationCount string `json:"relationCount"`
		ChangeCount   string `json:"changeCount"`
		Days          int    `json:"days"`
		ByModel       []struct {
			Key, Name, Count string
		} `json:"byModel"`
		ByEnv []struct{ Key, Count string } `json:"byEnv"`
		Trend []struct{ Date, Action, Count string }
	}
	if err := json.Unmarshal([]byte(body), &ov); err != nil {
		t.Fatalf("overview body = %s (%v)", body, err)
	}
	if ov.ModelCount != "3" || ov.CiCount != "3" || ov.RelationCount != "1" || ov.Days != 7 {
		t.Fatalf("overview totals = %+v", ov)
	}
	if len(ov.ByModel) != 2 || ov.ByModel[0].Key != "host" || ov.ByModel[0].Count != "2" || ov.ByModel[0].Name != "主机" {
		t.Fatalf("overview byModel = %+v", ov.ByModel)
	}
	if len(ov.ByEnv) != 3 || len(ov.Trend) == 0 || ov.ChangeCount == "0" {
		t.Fatalf("overview env/trend = %s", body)
	}
	if code, _ := do(http.MethodGet, "/api/v1/cmdb/overview?days=500", ""); code != http.StatusBadRequest {
		t.Fatalf("overview days out of range: status = %d", code)
	}
}
