package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	v1 "github.com/lanyulei/monitor/backend/core/api/system/v1"
	"github.com/lanyulei/monitor/backend/core/internal/biz"
)

// permissionJSON 是权限接口响应的断言子集（protojson camelCase）。
type permissionJSON struct {
	Menus    []string `json:"menus"`
	Elements []string `json:"elements"`
	Apis     []string `json:"apis"`
}

// TestHTTPServer_PermissionAuthz 授权门禁 e2e：deny-by-default、is_admin 直通、
// 角色授权生效、多角色并集、停用角色失效、白名单接口可达、授权即刻生效。
func TestHTTPServer_PermissionAuthz(t *testing.T) {
	t.Parallel()
	ts, signer := newTestHTTPServer(t)
	adminBearer := testBearer(t, signer, e2eActorID)
	listUsersURL := "/api/v1/system/user?page=1&page_size=10"

	// 普通用户（is_admin=false）：未授权一律 403 PERMISSION_DENIED。
	zoe := createUser(t, ts, adminBearer, "zoe")
	zoeBearer := testBearer(t, signer, zoe.Id)
	code, body := doRequestWithToken(t, http.MethodGet, ts.URL+listUsersURL, zoeBearer, "")
	if code != http.StatusForbidden || !strings.Contains(body, "PERMISSION_DENIED") {
		t.Fatalf("ungranted user: status = %d body = %q, want 403 PERMISSION_DENIED", code, body)
	}

	// is_admin 超管直通（无任何角色）。
	if code, body := doRequestWithToken(t, http.MethodGet, ts.URL+listUsersURL, adminBearer, ""); code != http.StatusOK {
		t.Fatalf("admin bypass: status = %d body = %q", code, body)
	}

	// 白名单接口对普通用户可达：/me 自助身份、匿名 login 仍走认证口径。
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/me", zoeBearer, "")
	if code != http.StatusOK {
		t.Fatalf("me for plain user: status = %d body = %q", code, body)
	}
	code, _ = doRequest(t, http.MethodGet, ts.URL+listUsersURL, "")
	if code != http.StatusUnauthorized {
		t.Fatalf("anonymous request: status = %d, want 401 (authz 不得削弱 auth)", code)
	}

	// admin 建两个角色：user_viewer 授 ListUsers，role_viewer 授 ListRoles
	//（接口直接授权口径，走新三字段契约）。
	uv := createRole(t, ts, adminBearer, "用户查看", "user_viewer")
	rv := createRole(t, ts, adminBearer, "角色查看", "role_viewer")
	grant := func(roleID, opCode string) {
		t.Helper()
		code, body := doRequestWithToken(t, http.MethodPut,
			ts.URL+"/api/v1/system/role/"+roleID+"/permission", adminBearer,
			`{"apis":["`+opCode+`"]}`)
		if code != http.StatusOK {
			t.Fatalf("grant %s to %s: status = %d body = %q", opCode, roleID, code, body)
		}
	}
	grant(uv.Id, biz.OperationCode(v1.OperationUserServiceListUsers))
	grant(rv.Id, biz.OperationCode(v1.OperationRoleServiceListRoles))

	// 绑定单角色：授权接口 200，未授权接口仍 403；Set 后即刻生效（同实例）。
	bind := func(roleIDs string) {
		t.Helper()
		code, body := doRequestWithToken(t, http.MethodPut,
			ts.URL+"/api/v1/system/user/"+zoe.Id+"/role", adminBearer,
			`{"role_ids":[`+roleIDs+`]}`)
		if code != http.StatusOK {
			t.Fatalf("bind roles %s: status = %d body = %q", roleIDs, code, body)
		}
	}
	bind(`"` + uv.Id + `"`)
	if code, body := doRequestWithToken(t, http.MethodGet, ts.URL+listUsersURL, zoeBearer, ""); code != http.StatusOK {
		t.Fatalf("granted ListUsers: status = %d body = %q", code, body)
	}
	if code, _ := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/system/role?page=1&page_size=10", zoeBearer, ""); code != http.StatusForbidden {
		t.Fatalf("ungranted ListRoles: status = %d, want 403", code)
	}

	// 多角色并集：叠加第二个角色后两个接口都可用（用户显式需求）。
	bind(`"` + uv.Id + `", "` + rv.Id + `"`)
	if code, _ := doRequestWithToken(t, http.MethodGet, ts.URL+listUsersURL, zoeBearer, ""); code != http.StatusOK {
		t.Fatalf("union ListUsers: status = %d", code)
	}
	if code, _ := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/system/role?page=1&page_size=10", zoeBearer, ""); code != http.StatusOK {
		t.Fatalf("union ListRoles: status = %d", code)
	}

	// 停用角色：status=0 后其授权立即失效（实时读取，无需重载）。
	code, _ = doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/system/role/"+uv.Id,
		adminBearer, `{"status":0}`)
	if code != http.StatusOK {
		t.Fatalf("disable role: status = %d", code)
	}
	if code, _ := doRequestWithToken(t, http.MethodGet, ts.URL+listUsersURL, zoeBearer, ""); code != http.StatusForbidden {
		t.Fatalf("disabled role still grants: status = %d, want 403", code)
	}
	if code, _ := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/system/role?page=1&page_size=10", zoeBearer, ""); code != http.StatusOK {
		t.Fatalf("enabled role revoked by sibling disable: status = %d", code)
	}

	// 撤销授权（清空）：回读一致且立即失去权限。
	code, _ = doRequestWithToken(t, http.MethodPut,
		ts.URL+"/api/v1/system/role/"+rv.Id+"/permission", adminBearer, `{}`)
	if code != http.StatusOK {
		t.Fatalf("revoke all: status = %d", code)
	}
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/system/role/"+rv.Id+"/permission", adminBearer, "")
	if code != http.StatusOK {
		t.Fatalf("get role permissions: status = %d body = %q", code, body)
	}
	var perm permissionJSON
	if err := json.Unmarshal([]byte(body), &perm); err != nil {
		t.Fatalf("get role permissions unmarshal %q: %v", body, err)
	}
	if len(perm.Menus) != 0 || len(perm.Elements) != 0 || len(perm.Apis) != 0 {
		t.Fatalf("after revoke grants = %+v, want empty", perm)
	}
	if code, _ := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/system/role?page=1&page_size=10", zoeBearer, ""); code != http.StatusForbidden {
		t.Fatalf("revoked role still grants: status = %d, want 403", code)
	}

	// 授权面校验：格式合法但未枚举的 code 400 OPERATION_INVALID；旧口径的
	// full method 被 protovalidate 先行拒绝（同样是 400）。
	code, body = doRequestWithToken(t, http.MethodPut,
		ts.URL+"/api/v1/system/role/"+rv.Id+"/permission", adminBearer,
		`{"apis":["`+biz.OperationCode(v1.OperationUserServiceListUsers)+`","`+biz.OperationCode("/system.v1.UserService/Nope")+`"]}`)
	if code != http.StatusBadRequest || !strings.Contains(body, "OPERATION_INVALID") {
		t.Fatalf("unknown operation: status = %d body = %q, want 400 OPERATION_INVALID", code, body)
	}
	code, _ = doRequestWithToken(t, http.MethodPut,
		ts.URL+"/api/v1/system/role/"+rv.Id+"/permission", adminBearer,
		`{"apis":["`+v1.OperationUserServiceListUsers+`"]}`)
	if code != http.StatusBadRequest {
		t.Fatalf("full method in operations: status = %d, want 400 (protovalidate)", code)
	}
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/system/operations", adminBearer, "")
	if code != http.StatusOK {
		t.Fatalf("list operations: status = %d body = %q", code, body)
	}
	if !strings.Contains(body, "/system.v1.UserService/CreateUser") {
		t.Fatalf("list operations body missing CreateUser: %q", body)
	}

	// 非超管用户不能配置授权（SetRolePermissions 本身受 authz 保护）。
	code, _ = doRequestWithToken(t, http.MethodPut,
		ts.URL+"/api/v1/system/role/"+rv.Id+"/permission", zoeBearer,
		`{"apis":["`+biz.OperationCode(v1.OperationUserServiceListUsers)+`"]}`)
	if code != http.StatusForbidden {
		t.Fatalf("plain user configures permissions: status = %d, want 403", code)
	}
}
