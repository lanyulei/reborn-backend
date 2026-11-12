package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	bizcmdb "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	"github.com/lanyulei/reborn/backend/kit/cmdbcontract"
)

// 同步控制面：采集源 CRUD（凭据只写不读）、触发任务幂等、运行记录、权限。
func TestCmdb_SyncControlPlane(t *testing.T) {
	t.Parallel()
	ts, svc := newTestHTTPServerWithServices(t)
	admin := testBearer(t, svc.signer, e2eActorID)
	base := ts.URL + "/api/v1/cmdb/sync/source"

	// 创建：响应不含凭据；config 含未知键 → 400。
	code, body := doRequestWithToken(t, http.MethodPost, base, admin,
		`{"name":"ali-prod","provider":"aliyun","config":{"region_id":"cn-hangzhou","access_key_id":"LTAI"},"credential":"TOPSECRET","env":"prod","intervalSeconds":600}`)
	if code != http.StatusOK || strings.Contains(body, "TOPSECRET") || !strings.Contains(body, `"hasCredential":true`) {
		t.Fatalf("create source: %d %s", code, body)
	}
	var created struct {
		Source struct {
			ID      string `json:"id"`
			Version string `json:"version"`
		} `json:"source"`
	}
	if err := json.Unmarshal([]byte(body), &created); err != nil {
		t.Fatal(err)
	}
	id := created.Source.ID
	if code, body := doRequestWithToken(t, http.MethodPost, base, admin,
		`{"name":"bad","provider":"aliyun","config":{"region_id":"r","access_key_id":"k","access_key_secret":"s"},"credential":"x"}`); code != http.StatusBadRequest || !strings.Contains(body, "CMDB_SOURCE_INVALID") {
		t.Fatalf("config with secret key must be rejected: %d %s", code, body)
	}
	if code, _ := doRequestWithToken(t, http.MethodPost, base, admin, `{"name":"x","provider":"gcp","config":{},"credential":"x"}`); code != http.StatusBadRequest {
		t.Fatalf("unknown provider must be rejected: %d", code)
	}
	// 列表 / 详情。
	if code, body := doRequestWithToken(t, http.MethodGet, base+"?page=1&pageSize=10&provider=aliyun", admin, ""); code != http.StatusOK || !strings.Contains(body, `"total":"1"`) || strings.Contains(body, "TOPSECRET") {
		t.Fatalf("list sources: %d %s", code, body)
	}
	if code, body := doRequestWithToken(t, http.MethodGet, base+"/"+id, admin, ""); code != http.StatusOK || !strings.Contains(body, `"lastStatus":"never"`) {
		t.Fatalf("get source: %d %s", code, body)
	}
	// 更新：不传凭据保留；版本冲突 409。
	upd := `{"version":1,"name":"ali-prod-2","config":{"region_id":"cn-beijing","access_key_id":"LTAI2"},"env":"prod","intervalSeconds":900,"gracePeriodSeconds":0,"enabled":true}`
	if code, body := doRequestWithToken(t, http.MethodPut, base+"/"+id, admin, upd); code != http.StatusOK || !strings.Contains(body, `"version":"2"`) || !strings.Contains(body, "cn-beijing") {
		t.Fatalf("update source: %d %s", code, body)
	}
	if string(svc.syncData.Credential(id)) != "sealed:TOPSECRET" {
		t.Fatalf("credential must be kept when omitted: %q", svc.syncData.Credential(id))
	}
	if code, body := doRequestWithToken(t, http.MethodPut, base+"/"+id, admin, upd); code != http.StatusConflict || !strings.Contains(body, "CMDB_SOURCE_VERSION_CONFLICT") {
		t.Fatalf("stale version must conflict: %d %s", code, body)
	}
	// 触发：成功 → 再触发 409 忙；运行完成后可见记录。
	code, body = doRequestWithToken(t, http.MethodPost, base+"/"+id+":trigger", admin, `{}`)
	if code != http.StatusOK || !strings.Contains(body, `"taskId"`) {
		t.Fatalf("trigger: %d %s", code, body)
	}
	var trig struct {
		TaskID string `json:"taskId"`
	}
	_ = json.Unmarshal([]byte(body), &trig)
	if code, body := doRequestWithToken(t, http.MethodPost, base+"/"+id+":trigger", admin, `{}`); code != http.StatusConflict || !strings.Contains(body, "CMDB_SYNC_BUSY") {
		t.Fatalf("second trigger must be busy: %d %s", code, body)
	}
	if code, body := doRequestWithToken(t, http.MethodGet, base+"/"+id, admin, ""); code != http.StatusOK || !strings.Contains(body, `"activeTask":true`) {
		t.Fatalf("active task flag: %d %s", code, body)
	}
	svc.syncData.FinishTask(trig.TaskID, &bizcmdb.SyncRun{
		ID: "11111111-1111-1111-1111-111111111111", SourceID: id, TaskID: trig.TaskID, Status: cmdbcontract.RunSucceeded,
		Discovered: 5, Created: 2, Updated: 1, Unchanged: 2, Errors: []cmdbcontract.RunError{{ModelCode: "cloud_host", ExternalID: "i-1", Name: "dup", Reason: "name conflict"}},
	})
	code, body = doRequestWithToken(t, http.MethodGet, base+"/"+id+"/run?page=1&pageSize=10", admin, "")
	if code != http.StatusOK || !strings.Contains(body, `"discovered":5`) || !strings.Contains(body, `"name conflict"`) {
		t.Fatalf("list runs: %d %s", code, body)
	}
	// 权限：只有菜单默认权限的用户可读不可写。
	user := createUser(t, ts, admin, "sync_reader")
	grantAssistantRole(t, ts, admin, user.Id, "cmdb_sync_reader",
		"/cmdb.v1.CmdbSyncService/ListSources", "/cmdb.v1.CmdbSyncService/GetSource", "/cmdb.v1.CmdbSyncService/ListRuns")
	reader := testBearer(t, svc.signer, user.Id)
	if code, _ := doRequestWithToken(t, http.MethodGet, base+"?page=1&pageSize=10", reader, ""); code != http.StatusOK {
		t.Fatalf("reader list: %d", code)
	}
	if code, _ := doRequestWithToken(t, http.MethodPost, base+"/"+id+":trigger", reader, `{}`); code != http.StatusForbidden {
		t.Fatalf("reader trigger must be forbidden: %d", code)
	}
	if code, _ := doRequestWithToken(t, http.MethodDelete, base+"/"+id, reader, ""); code != http.StatusForbidden {
		t.Fatalf("reader delete must be forbidden: %d", code)
	}
	// 删除 → 404。
	if code, _ := doRequestWithToken(t, http.MethodDelete, base+"/"+id, admin, ""); code != http.StatusOK {
		t.Fatalf("delete: %d", code)
	}
	if code, _ := doRequestWithToken(t, http.MethodGet, base+"/"+id, admin, ""); code != http.StatusNotFound {
		t.Fatalf("deleted source must be 404: %d", code)
	}
}
