package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// 第三阶段优化：字段索引/敏感标记与脱敏、关联条件筛选、游标分页、全局审计、关系导入、JSON 导出、回调令牌。
func TestCmdb_Phase3(t *testing.T) {
	t.Parallel()
	ts, svc := newTestHTTPServerWithServices(t)
	admin := testBearer(t, svc.signer, e2eActorID)
	cmdbSetup(t, ts, admin)

	// 字段：sensitive + indexed → 索引桩被调用；读取脱敏；带占位值回写不覆盖真实值。
	code, body := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/cmdb/model/host/field", admin,
		`{"code":"root_pwd","name":"root 密码","type":"string","sensitive":true,"indexed":true}`)
	if code != http.StatusOK || !strings.Contains(body, `"sensitive":true`) || !strings.Contains(body, `"indexed":true`) {
		t.Fatalf("create sensitive field: %d %s", code, body)
	}
	host := createCI(t, ts, admin, `{"modelCode":"host","name":"h-1","env":"prod","attrs":{"ip":"10.0.0.1","root_pwd":"S3cret!"}}`)
	got, _ := getCI(t, ts, admin, host.ID)
	if got.Attrs["root_pwd"] != "******" || got.Attrs["ip"] != "10.0.0.1" {
		t.Fatalf("sensitive attr must be masked: %+v", got.Attrs)
	}
	if code, body := doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/cmdb/ci/"+host.ID, admin, `{"version":1,"attrs":{"root_pwd":"******","ip":"10.0.0.2"}}`); code != http.StatusOK || !strings.Contains(body, `"version":"2"`) {
		t.Fatalf("update with masked value: %d %s", code, body)
	}
	if raw := svc.cmdbData.CIAttr(host.ID, "root_pwd"); raw != "S3cret!" {
		t.Fatalf("masked placeholder must not overwrite the stored secret: %v", raw)
	}
	if code, body := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci/"+host.ID+"/history?page=1&pageSize=10", admin, ""); code != http.StatusOK || strings.Contains(body, "S3cret") {
		t.Fatalf("history must be masked: %d %s", code, body)
	}
	if code, body := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci:search?q=S3cret", admin, ""); code != http.StatusOK || strings.Contains(body, "S3cret") {
		t.Fatalf("search must not leak sensitive values: %d %s", code, body)
	}
	if code, body := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci:export?modelCode=host&format=json", admin, ""); code != http.StatusOK || strings.Contains(body, "S3cret") || !strings.Contains(body, `"attrs":{`) {
		t.Fatalf("json export: %d %s", code, body)
	}
	if code, body := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci:export?modelCode=host&format=xml", admin, ""); code != http.StatusBadRequest {
		t.Fatalf("unknown format must be 400: %d %s", code, body)
	}

	// 关联条件筛选：host-1 挂 part；host-2 无配件。
	host2 := createCI(t, ts, admin, `{"modelCode":"host","name":"h-2","env":"prod","attrs":{"ip":"10.0.0.3"}}`)
	part := createCI(t, ts, admin, `{"modelCode":"part","name":"p-1","env":"prod","attrs":{"kind":"memory"}}`)
	if code, body := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/cmdb/ci/"+host.ID+"/relation", admin,
		`{"relationType":"host_has_part","direction":"outgoing","targetIds":["`+part.ID+`"]}`); code != http.StatusOK {
		t.Fatalf("link: %d %s", code, body)
	}
	list := func(query string) []map[string]any {
		code, body := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci?page=1&pageSize=10&modelCode=host"+query, admin, "")
		if code != http.StatusOK {
			t.Fatalf("list %s: %d %s", query, code, body)
		}
		var out struct {
			Items      []map[string]any `json:"items"`
			NextCursor string           `json:"nextCursor"`
		}
		_ = json.Unmarshal([]byte(body), &out)
		return out.Items
	}
	if items := list("&relType=host_has_part&relNone=true"); len(items) != 1 || items[0]["name"] != "h-2" {
		t.Fatalf("relNone = %+v", items)
	}
	if items := list("&relType=host_has_part"); len(items) != 1 || items[0]["name"] != "h-1" {
		t.Fatalf("has relation = %+v", items)
	}
	if items := list("&relType=host_has_part&relTargetId=" + part.ID); len(items) != 1 || items[0]["name"] != "h-1" {
		t.Fatalf("rel target = %+v", items)
	}
	if items := list("&relType=host_has_part&relTargetId=" + host2.ID); len(items) != 0 {
		t.Fatalf("rel target miss = %+v", items)
	}
	// 游标分页：pageSize=1 → 两页，游标不重叠，末页无 next。
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci?page=1&pageSize=1&modelCode=host", admin, "")
	var p1 struct {
		Items      []map[string]any `json:"items"`
		NextCursor string           `json:"nextCursor"`
	}
	_ = json.Unmarshal([]byte(body), &p1)
	if code != http.StatusOK || len(p1.Items) != 1 || p1.NextCursor == "" {
		t.Fatalf("cursor page1: %d %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci?page=1&pageSize=1&modelCode=host&cursor="+p1.NextCursor, admin, "")
	var p2 struct {
		Items      []map[string]any `json:"items"`
		NextCursor string           `json:"nextCursor"`
	}
	_ = json.Unmarshal([]byte(body), &p2)
	if code != http.StatusOK || len(p2.Items) != 1 || p2.Items[0]["id"] == p1.Items[0]["id"] {
		t.Fatalf("cursor page2: %d %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci?page=1&pageSize=1&modelCode=host&cursor="+p2.NextCursor, admin, "")
	if code != http.StatusOK || !strings.Contains(body, `"items":[]`) {
		t.Fatalf("cursor page3 must be empty: %d %s", code, body)
	}
	if code, _ := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci?page=1&pageSize=1&cursor=!!!", admin, ""); code != http.StatusBadRequest {
		t.Fatalf("bad cursor must be 400: %d", code)
	}

	// 全局审计：按动作 / 类型 / 关键字过滤，带资源名与类型；受限用户只见范围内。
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/history?page=1&pageSize=50&action=link", admin, "")
	if code != http.StatusOK || !strings.Contains(body, `"ciName":"h-1"`) || !strings.Contains(body, `"ciName":"p-1"`) || strings.Contains(body, `"action":"create"`) {
		t.Fatalf("audit link: %d %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/history?page=1&pageSize=50&modelCode=part&keyword=p-", admin, "")
	if code != http.StatusOK || strings.Contains(body, `"ciName":"h-1"`) || !strings.Contains(body, `"modelCode":"part"`) {
		t.Fatalf("audit filter: %d %s", code, body)
	}

	// 关系导入：新配件 p-2 挂到 host-2 成功；p-3 声明的对端不存在 → 资源已建、关系失败。
	code, body = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/cmdb/ci:import", admin,
		`{"modelCode":"part","dryRun":false,"rows":[{"name":"p-2","attrs":{"kind":"disk"},"relations":[{"relationType":"host_has_part","direction":"incoming","targetName":"h-2"}]},
		  {"name":"p-3","attrs":{"kind":"disk"},"relations":[{"relationType":"host_has_part","direction":"incoming","targetName":"nope"}]}]}`)
	// p-3 的资源已创建、仅关系失败：按资源动作计数（created），action 保持 create 并带 ciId，
	// 关系失败经 "relation failed: " 前缀的 message 带出（不计 failed）。
	if code != http.StatusOK || !strings.Contains(body, `"created":2`) || strings.Contains(body, `"failed":1`) || strings.Contains(body, `"action":"error"`) ||
		!strings.Contains(body, "relations linked: 1") || !strings.Contains(body, `"message":"relation failed: `) || !strings.Contains(body, `"nope\" not found`) {
		t.Fatalf("import relations: %d %s", code, body)
	}
	if items := list("&relType=host_has_part&relTargetId=" + host2.ID); len(items) != 0 {
		t.Fatalf("host-2 lookup by target is from host side; expected none: %+v", items)
	}
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci?page=1&pageSize=10&modelCode=part&relType=host_has_part&relDirection=incoming&relTargetId="+host2.ID, admin, "")
	if code != http.StatusOK || !strings.Contains(body, `"name":"p-2"`) {
		t.Fatalf("p-2 must be linked to h-2: %d %s", code, body)
	}

	// 回调令牌：轮换返回明文一次、库中只存哈希；停用后 webhookEnabled=false；源详情含回调路径。
	code, body = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/cmdb/sync/source", admin,
		`{"name":"ali","provider":"aliyun","config":{"region_id":"r","access_key_id":"k"},"credential":"s"}`)
	if code != http.StatusOK {
		t.Fatalf("create source: %d %s", code, body)
	}
	var created struct {
		Source struct {
			ID string `json:"id"`
		} `json:"source"`
	}
	_ = json.Unmarshal([]byte(body), &created)
	code, body = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/cmdb/sync/source/"+created.Source.ID+"/webhook", admin, `{}`)
	var rot struct {
		Token, Path, Header string
	}
	_ = json.Unmarshal([]byte(body), &rot)
	if code != http.StatusOK || len(rot.Token) != 64 || rot.Header != "X-Cmdb-Webhook-Token" || !strings.HasSuffix(rot.Path, created.Source.ID) {
		t.Fatalf("rotate webhook: %d %s", code, body)
	}
	if h := svc.syncData.WebhookHash(created.Source.ID); len(h) != 32 || strings.Contains(string(h), rot.Token[:8]) {
		t.Fatalf("only the hash must be stored: %x", h)
	}
	if code, body := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/sync/source/"+created.Source.ID, admin, ""); code != http.StatusOK || !strings.Contains(body, `"webhookEnabled":true`) || !strings.Contains(body, `"webhookPath":"/webhook/v1/cmdb/sync/`) {
		t.Fatalf("source webhook flags: %d %s", code, body)
	}
	if code, _ := doRequestWithToken(t, http.MethodDelete, ts.URL+"/api/v1/cmdb/sync/source/"+created.Source.ID+"/webhook", admin, ""); code != http.StatusOK {
		t.Fatalf("disable webhook: %d", code)
	}
	if code, body := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/sync/source/"+created.Source.ID, admin, ""); code != http.StatusOK || !strings.Contains(body, `"webhookEnabled":false`) {
		t.Fatalf("webhook disabled flag: %d %s", code, body)
	}
}
