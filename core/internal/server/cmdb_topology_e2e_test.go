package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// 全局搜索：名称 / 外部标识 / 标签 / 属性值命中，名称优先，limit 生效，数据范围叠加。
func TestCmdb_Search(t *testing.T) {
	t.Parallel()
	ts, svc := newTestHTTPServerWithServices(t)
	admin := testBearer(t, svc.signer, e2eActorID)
	cmdbSetup(t, ts, admin)
	createCI(t, ts, admin, `{"modelCode":"host","name":"web-alpha","env":"prod","tags":["alpha-team"],"attrs":{"ip":"10.0.0.1"}}`)
	createCI(t, ts, admin, `{"modelCode":"host","name":"db-1","env":"prod","externalId":"i-alpha2","attrs":{"ip":"10.0.0.2"}}`)
	createCI(t, ts, admin, `{"modelCode":"host","name":"cache-1","env":"test","attrs":{"ip":"10.0.0.3"}}`)
	createCI(t, ts, admin, `{"modelCode":"part","name":"mem-1","env":"prod","attrs":{"kind":"memory"}}`)
	search := func(bearer, q string) []map[string]any {
		code, body := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci:search?q="+q, bearer, "")
		if code != http.StatusOK {
			t.Fatalf("search %q: %d %s", q, code, body)
		}
		var out struct {
			Items []map[string]any `json:"items"`
		}
		if err := json.Unmarshal([]byte(body), &out); err != nil {
			t.Fatal(err)
		}
		return out.Items
	}
	hits := search(admin, "alpha")
	if len(hits) != 2 || hits[0]["matchedField"] != "name" || hits[1]["matchedField"] != "externalId" {
		t.Fatalf("alpha hits = %+v", hits)
	}
	if hits := search(admin, "10.0.0.3"); len(hits) != 1 || hits[0]["matchedField"] != "ip" || hits[0]["matchedValue"] != "10.0.0.3" {
		t.Fatalf("attr hits = %+v", hits)
	}
	if hits := search(admin, "alpha-team"); len(hits) != 1 || hits[0]["matchedField"] != "tags" {
		t.Fatalf("tag hits = %+v", hits)
	}
	if hits := search(admin, "memory"); len(hits) != 1 || hits[0]["matchedField"] != "kind" {
		t.Fatalf("enum hits = %+v", hits)
	}
	if hits := search(admin, "1&limit=2"); len(hits) != 2 {
		t.Fatalf("limit must cap results: %d", len(hits))
	}
	if code, _ := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci:search?q=", admin, ""); code != http.StatusBadRequest {
		t.Fatalf("empty query must be 400: %d", code)
	}
	// 受限用户：只看 host + prod。
	user := createUser(t, ts, admin, "search_user")
	grantAssistantRole(t, ts, admin, user.Id, "cmdb_search", "/cmdb.v1.CmdbCIService/SearchCIs")
	code, body := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/scope/role", admin, "")
	if code != http.StatusOK {
		t.Fatal(body)
	}
	var roles struct {
		Items []struct {
			RoleID string `json:"roleId"`
			Code   string `json:"roleCode"`
		} `json:"items"`
	}
	_ = json.Unmarshal([]byte(body), &roles)
	for _, r := range roles.Items {
		if r.Code == "cmdb_search" {
			if code, body := doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/cmdb/scope/role/"+r.RoleID, admin, `{"modelCodes":["host"],"envs":["prod"]}`); code != http.StatusOK {
				t.Fatal(body)
			}
		}
	}
	scoped := testBearer(t, svc.signer, user.Id)
	hits = search(scoped, "1")
	for _, h := range hits {
		ci := h["ci"].(map[string]any)
		if ci["modelCode"] != "host" || ci["env"] != "prod" {
			t.Fatalf("scoped search leaked %+v", ci)
		}
	}
	if len(hits) != 2 {
		t.Fatalf("scoped hits = %d", len(hits))
	}
}

// 拓扑：按层展开、方向无关、边去重、层数与节点上限、范围外节点隐藏。
func TestCmdb_Neighbors(t *testing.T) {
	t.Parallel()
	ts, svc := newTestHTTPServerWithServices(t)
	admin := testBearer(t, svc.signer, e2eActorID)
	cmdbSetup(t, ts, admin)
	host := createCI(t, ts, admin, `{"modelCode":"host","name":"h-1","env":"prod","attrs":{"ip":"10.0.0.1"}}`)
	host2 := createCI(t, ts, admin, `{"modelCode":"host","name":"h-2","env":"prod","attrs":{"ip":"10.0.0.2"}}`)
	part := createCI(t, ts, admin, `{"modelCode":"part","name":"p-1","env":"prod","attrs":{"kind":"memory"}}`)
	sw := createCI(t, ts, admin, `{"modelCode":"switch","name":"sw-1","env":"prod"}`)
	link := func(ci, rel, dir, target string) {
		if code, body := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/cmdb/ci/"+ci+"/relation", admin,
			`{"relationType":"`+rel+`","direction":"`+dir+`","targetIds":["`+target+`"]}`); code != http.StatusOK {
			t.Fatalf("link: %d %s", code, body)
		}
	}
	link(host.ID, "host_has_part", "outgoing", part.ID)
	link(host.ID, "host_uplink", "outgoing", sw.ID)
	link(host2.ID, "host_uplink", "outgoing", sw.ID)
	type topo struct {
		Nodes []struct {
			ID    string `json:"id"`
			Name  string `json:"name"`
			Depth int    `json:"depth"`
		} `json:"nodes"`
		Edges     []map[string]string `json:"edges"`
		Truncated bool                `json:"truncated"`
	}
	fetch := func(bearer, id, query string) topo {
		code, body := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci/"+id+"/neighbors"+query, bearer, "")
		if code != http.StatusOK {
			t.Fatalf("neighbors: %d %s", code, body)
		}
		var out topo
		if err := json.Unmarshal([]byte(body), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	// 深度 1：中心 + 配件 + 交换机，2 条边。
	d1 := fetch(admin, host.ID, "?depth=1")
	if len(d1.Nodes) != 3 || len(d1.Edges) != 2 || d1.Nodes[0].ID != host.ID || d1.Nodes[0].Depth != 0 || d1.Truncated {
		t.Fatalf("depth1 = %+v", d1)
	}
	// 深度 2：经交换机到达 h-2（3 条边，无重复）。
	d2 := fetch(admin, host.ID, "?depth=2")
	if len(d2.Nodes) != 4 || len(d2.Edges) != 3 {
		t.Fatalf("depth2 = %+v", d2)
	}
	var h2Depth int
	for _, n := range d2.Nodes {
		if n.ID == host2.ID {
			h2Depth = n.Depth
		}
	}
	if h2Depth != 2 {
		t.Fatalf("h-2 depth = %d", h2Depth)
	}
	// 节点上限：max_nodes=2 → 截断，只保留两端都在集合内的边。
	tr := fetch(admin, host.ID, "?depth=3&maxNodes=2")
	if len(tr.Nodes) != 2 || !tr.Truncated || len(tr.Edges) != 1 {
		t.Fatalf("truncated = %+v", tr)
	}
	if code, _ := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci/"+host.ID+"/neighbors?depth=9", admin, ""); code != http.StatusBadRequest {
		t.Fatalf("depth>3 must be 400: %d", code)
	}
	if code, _ := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci/00000000-0000-0000-0000-000000000001/neighbors", admin, ""); code != http.StatusNotFound {
		t.Fatalf("unknown ci must be 404: %d", code)
	}
	// 受限用户（仅 host 可见）：交换机与配件及其边被隐藏，但经交换机的 h-2 仍不可达 → 只剩中心。
	user := createUser(t, ts, admin, "topo_user")
	grantAssistantRole(t, ts, admin, user.Id, "cmdb_topo", "/cmdb.v1.CmdbRelationService/Neighbors", "/cmdb.v1.CmdbCIService/GetCI")
	code, body := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/scope/role", admin, "")
	if code != http.StatusOK {
		t.Fatal(body)
	}
	if !strings.Contains(body, "cmdb_topo") {
		t.Fatal("role missing")
	}
	var roles struct {
		Items []struct {
			RoleID string `json:"roleId"`
			Code   string `json:"roleCode"`
		} `json:"items"`
	}
	_ = json.Unmarshal([]byte(body), &roles)
	for _, r := range roles.Items {
		if r.Code == "cmdb_topo" {
			if code, body := doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/cmdb/scope/role/"+r.RoleID, admin, `{"modelCodes":["host"]}`); code != http.StatusOK {
				t.Fatal(body)
			}
		}
	}
	scoped := fetch(testBearer(t, svc.signer, user.Id), host.ID, "?depth=2")
	for _, n := range scoped.Nodes {
		if n.Name == "sw-1" || n.Name == "p-1" {
			t.Fatalf("scoped topology leaked %s", n.Name)
		}
	}
	if len(scoped.Edges) != 0 || len(scoped.Nodes) != 2 {
		t.Fatalf("scoped = %+v", scoped)
	}
}
