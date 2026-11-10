package server

// NotificationWriter e2e：管理端点（SetRolePermissions）在业务写事务内直插
// permission.change 事件（设计 §11），事件经 NotifyEventService 只读链路
// 立即可查——「业务提交 = 事件必然落库」的全链路（HTTP → service → biz →
// 跨域事务编排 → 直插原语）验证。

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
)

func TestHTTPServer_PermissionChangeNotifies(t *testing.T) {
	t.Parallel()
	ts, signer, store := newTestHTTPServerWithStore(t)
	bearer := testBearer(t, signer, e2eActorID)

	// 预置角色（经 HTTP 创建）。
	code, body := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/system/role", bearer,
		`{"name":"运维","code":"ops_e2e"}`)
	if code != http.StatusOK {
		t.Fatalf("create role: status = %d body = %s", code, body)
	}
	var roleResp struct {
		Role struct {
			ID string `json:"id"`
		} `json:"role"`
	}
	if err := json.Unmarshal([]byte(body), &roleResp); err != nil || roleResp.Role.ID == "" {
		t.Fatalf("create role body = %s (%v)", body, err)
	}

	// 授权变更：业务写 + 事件写同事务（操作者 e2eActorID 进事件载荷）。
	code, body = doRequestWithToken(t, http.MethodPut,
		ts.URL+"/api/v1/system/role/"+roleResp.Role.ID+"/permission", bearer,
		`{"menus":["system.user"]}`)
	if code != http.StatusOK {
		t.Fatalf("set role permissions: status = %d body = %s", code, body)
	}

	// 事件立即可查（同事务提交）：permission.change / internal / received。
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/event?page=1&page_size=10&eventType=permission.change", bearer, "")
	if code != http.StatusOK {
		t.Fatalf("list events: status = %d body = %s", code, body)
	}
	for _, want := range []string{
		`"total":"1"`, `"source":"internal"`, `"status":1`, `"publishState":0`,
		`"bizId":"core"`, `"title":"角色权限变更"`,
		`"role_id":"` + roleResp.Role.ID + `"`, `"operator":"` + e2eActorID + `"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("list events body %q missing %s", body, want)
		}
	}

	// 落库即原始数据（testfake 与查询链路同一存储）：恰一条。
	_, total, err := store.ListEvents(context.Background(),
		biznotify.EventFilter{EventType: "permission.change"}, 1, 10)
	if err != nil || total != 1 {
		t.Fatalf("store events total = %d (%v), want 1", total, err)
	}
}
