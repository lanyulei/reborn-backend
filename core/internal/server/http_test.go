package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lanyulei/monitor/backend/core/internal/biz"
	"github.com/lanyulei/monitor/backend/core/internal/conf"
)

// e2eActorID 是既有 e2e 用例的固定操作者 ID；鉴权只校验令牌签名与有效期，
// 非 user 作用域的接口不检查操作者存在性。
const e2eActorID = "018f3e1a-7b1c-7cc1-9d2a-3f5e7a9b0001"

// newTestHTTPServer 组装完整的中间件链 + 编解码 + service（真实 biz 与内存仓储）。
func newTestHTTPServer(t *testing.T) (*httptest.Server, *biz.TokenSigner) {
	t.Helper()
	svc := newTestServices(t)
	srv := NewHTTPServer(
		&conf.Server{Http: &conf.Server_HTTP{}},
		testLogger(),
		svc.user,
		svc.role,
		svc.auth,
		svc.syscfg,
		svc.signer,
		svc.authz,
	)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts, svc.signer
}

// testBearer 为任意用户 ID 直接签发测试访问令牌：既有 e2e 用例不经登录闭环，
// 避免「建用户需 token」的循环依赖；登录链路在 auth_test.go 单独覆盖。
func testBearer(t *testing.T, signer *biz.TokenSigner, userID string) string {
	t.Helper()
	token, _, err := signer.SignAccess(userID, "e2e-actor", time.Now())
	if err != nil {
		t.Fatalf("sign test token: %v", err)
	}
	return token
}

func doRequest(t *testing.T, method, url, body string) (int, string) {
	t.Helper()
	return doRequestWithToken(t, method, url, "", body)
}

// doRequestWithToken 携带 Bearer 令牌发起请求；token 为空表示匿名请求。
func doRequestWithToken(t *testing.T, method, url, token, body string) (int, string) {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, string(b)
}

// userJSON 是 e2e 断言用的用户响应子集。
type userJSON struct {
	Id       string `json:"id"`
	Username string `json:"username"`
	Nickname string `json:"nickname"`
	Email    string `json:"email"`
	Gender   string `json:"gender"`
	Status   int32  `json:"status"`
}

type listUsersJSON struct {
	Items []userJSON `json:"items"`
	Total string     `json:"total"` // protojson 将 int64 编码为字符串
}

type roleJSON struct {
	Id   string `json:"id"`
	Name string `json:"name"`
	Code string `json:"code"`
}

type listRolesJSON struct {
	Items []roleJSON `json:"items"`
	Total string     `json:"total"`
}

// createUser 是多个用例共用的建用户辅助（需携带 Bearer）。
func createUser(t *testing.T, ts *httptest.Server, token, username string) userJSON {
	t.Helper()
	code, body := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/system/user", token,
		`{"username":"`+username+`","password":"plain-secret","nickname":"`+username+`","gender":"male"}`)
	if code != http.StatusOK {
		t.Fatalf("create user %s: status = %d body = %s", username, code, body)
	}
	var resp struct{ User userJSON }
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		t.Fatalf("create user %s: unmarshal %q: %v", username, body, err)
	}
	return resp.User
}

func createRole(t *testing.T, ts *httptest.Server, token, name, code string) roleJSON {
	t.Helper()
	status, body := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/system/role", token,
		`{"name":"`+name+`","code":"`+code+`"}`)
	if status != http.StatusOK {
		t.Fatalf("create role %s: status = %d body = %s", code, status, body)
	}
	var resp struct{ Role roleJSON }
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		t.Fatalf("create role %s: unmarshal %q: %v", code, body, err)
	}
	return resp.Role
}

// 端到端冒烟：中间件链、protojson 编解码、校验钩子、错误映射全链路验证。
func TestHTTPServer_UserEndToEnd(t *testing.T) {
	t.Parallel()
	ts, signer := newTestHTTPServer(t)
	bearer := testBearer(t, signer, e2eActorID)

	// 创建：JSON 请求体经 protojson 解码；响应不含密码。
	code, body := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/system/user", bearer,
		`{"username":"alice","password":"plain-secret","nickname":"Alice","gender":"female","email":"alice@example.com"}`)
	if code != http.StatusOK {
		t.Fatalf("create user: status = %d body = %s", code, body)
	}
	if strings.Contains(body, "password") {
		t.Fatalf("create user body %q must not contain password", body)
	}
	var created struct{ User userJSON }
	if err := json.Unmarshal([]byte(body), &created); err != nil {
		t.Fatalf("create user unmarshal %q: %v", body, err)
	}
	if created.User.Id == "" || created.User.Username != "alice" || created.User.Email != "alice@example.com" {
		t.Fatalf("create user = %+v", created.User)
	}

	// 详情：响应必须是 protojson 编码（标准库 json 会输出空对象）。
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/system/user/"+created.User.Id, bearer, "")
	if code != http.StatusOK {
		t.Fatalf("get user: status = %d body = %s", code, body)
	}
	if !strings.Contains(body, `"username":"alice"`) || strings.Contains(body, "password") {
		t.Fatalf("get user body = %q", body)
	}

	// 列表：分页参数经校验后返回（含装配期预置的 e2e-actor 超管种子）。
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/system/user?page=1&page_size=10", bearer, "")
	if code != http.StatusOK {
		t.Fatalf("list users: status = %d body = %s", code, body)
	}
	var list listUsersJSON
	if err := json.Unmarshal([]byte(body), &list); err != nil {
		t.Fatalf("list users unmarshal %q: %v", body, err)
	}
	if list.Total != "2" || len(list.Items) != 2 {
		t.Fatalf("list users = %+v", list)
	}
	names := []string{list.Items[0].Username, list.Items[1].Username}
	if !slices.Contains(names, "alice") || !slices.Contains(names, "e2e-actor") {
		t.Fatalf("list users = %+v", list)
	}

	// 重复创建：409 + USER_EXISTS。
	code, body = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/system/user", bearer,
		`{"username":"alice","password":"plain-secret","nickname":"Dup","gender":"male"}`)
	if code != http.StatusConflict || !strings.Contains(body, "USER_EXISTS") {
		t.Fatalf("duplicate create: status = %d body = %q", code, body)
	}

	// 不存在：404 + USER_NOT_FOUND（biz 哨兵错误透传）。
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/system/user/018f3e1a-7b1c-7cc1-9d2a-3f5e7a9b1c3d", bearer, "")
	if code != http.StatusNotFound || !strings.Contains(body, "USER_NOT_FOUND") {
		t.Fatalf("get missing: status = %d body = %q", code, body)
	}

	// 更新（可选字段）+ 状态 + 密码重置。
	code, body = doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/system/user/"+created.User.Id, bearer,
		`{"nickname":"Alice Chen"}`)
	if code != http.StatusOK {
		t.Fatalf("update user: status = %d body = %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/system/user/"+created.User.Id+"/status", bearer, `{"status":2}`)
	if code != http.StatusOK {
		t.Fatalf("update status: status = %d body = %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/system/user/"+created.User.Id+"/password", bearer,
		`{"password":"new-secret-123"}`)
	if code != http.StatusOK {
		t.Fatalf("reset password: status = %d body = %s", code, body)
	}

	// 删除后再查：404。
	code, body = doRequestWithToken(t, http.MethodDelete, ts.URL+"/api/v1/system/user/"+created.User.Id, bearer, "")
	if code != http.StatusOK {
		t.Fatalf("delete user: status = %d body = %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/system/user/"+created.User.Id, bearer, "")
	if code != http.StatusNotFound {
		t.Fatalf("get deleted user: status = %d body = %s", code, body)
	}
}

func TestHTTPServer_UserRoleBinding(t *testing.T) {
	t.Parallel()
	ts, signer := newTestHTTPServer(t)
	bearer := testBearer(t, signer, e2eActorID)
	user := createUser(t, ts, bearer, "alice")
	admin := createRole(t, ts, bearer, "管理员", "admin")
	viewer := createRole(t, ts, bearer, "观察者", "viewer")

	// 绑定不存在角色：404 + ROLE_NOT_FOUND。
	code, body := doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/system/user/"+user.Id+"/role", bearer,
		`{"role_ids":["018f3e1a-7b1c-7cc1-9d2a-3f5e7a9b1c3d"]}`)
	if code != http.StatusNotFound || !strings.Contains(body, "ROLE_NOT_FOUND") {
		t.Fatalf("bind missing role: status = %d body = %q", code, body)
	}

	// 全量覆盖绑定两个角色后查询，再覆盖为一个。
	code, body = doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/system/user/"+user.Id+"/role", bearer,
		`{"role_ids":["`+admin.Id+`","`+viewer.Id+`"]}`)
	if code != http.StatusOK {
		t.Fatalf("bind roles: status = %d body = %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/system/user/"+user.Id+"/role", bearer, "")
	if code != http.StatusOK {
		t.Fatalf("list user roles: status = %d body = %s", code, body)
	}
	var bound struct{ Items []roleJSON }
	if err := json.Unmarshal([]byte(body), &bound); err != nil {
		t.Fatalf("list user roles unmarshal %q: %v", body, err)
	}
	if len(bound.Items) != 2 {
		t.Fatalf("bound roles = %+v, want 2", bound.Items)
	}

	code, body = doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/system/user/"+user.Id+"/role", bearer,
		`{"role_ids":["`+admin.Id+`"]}`)
	if code != http.StatusOK {
		t.Fatalf("rebind roles: status = %d body = %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/system/user/"+user.Id+"/role", bearer, "")
	if code != http.StatusOK {
		t.Fatalf("list user roles: status = %d body = %s", code, body)
	}
	if err := json.Unmarshal([]byte(body), &bound); err != nil {
		t.Fatalf("list user roles unmarshal %q: %v", body, err)
	}
	if len(bound.Items) != 1 || bound.Items[0].Code != "admin" {
		t.Fatalf("rebound roles = %+v, want only admin", bound.Items)
	}

	// 空数组清空绑定。
	code, body = doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/system/user/"+user.Id+"/role", bearer, `{"role_ids":[]}`)
	if code != http.StatusOK {
		t.Fatalf("clear roles: status = %d body = %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/system/user/"+user.Id+"/role", bearer, "")
	if code != http.StatusOK {
		t.Fatalf("list cleared roles: status = %d body = %s", code, body)
	}
	if err := json.Unmarshal([]byte(body), &bound); err != nil {
		t.Fatalf("list cleared roles unmarshal %q: %v", body, err)
	}
	if len(bound.Items) != 0 {
		t.Fatalf("cleared roles = %+v, want empty", bound.Items)
	}
}

func TestHTTPServer_RoleEndToEnd(t *testing.T) {
	t.Parallel()
	ts, signer := newTestHTTPServer(t)
	bearer := testBearer(t, signer, e2eActorID)
	role := createRole(t, ts, bearer, "管理员", "admin")

	// 重复 code：409 + ROLE_CODE_EXISTS。
	code, body := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/system/role", bearer, `{"name":"管理员2","code":"admin"}`)
	if code != http.StatusConflict || !strings.Contains(body, "ROLE_CODE_EXISTS") {
		t.Fatalf("duplicate role code: status = %d body = %q", code, body)
	}

	// 更新（code 不可改，只允许 name/status）。
	code, body = doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/system/role/"+role.Id, bearer, `{"name":"超级管理员"}`)
	if code != http.StatusOK {
		t.Fatalf("update role: status = %d body = %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/system/role/"+role.Id, bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"code":"admin"`) {
		t.Fatalf("get role: status = %d body = %s", code, body)
	}

	// 列表。
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/system/role?page=1&page_size=10", bearer, "")
	if code != http.StatusOK {
		t.Fatalf("list roles: status = %d body = %s", code, body)
	}
	var list listRolesJSON
	if err := json.Unmarshal([]byte(body), &list); err != nil {
		t.Fatalf("list roles unmarshal %q: %v", body, err)
	}
	if list.Total != "1" || len(list.Items) != 1 {
		t.Fatalf("list roles = %+v", list)
	}

	// 删除后再查：404。
	code, body = doRequestWithToken(t, http.MethodDelete, ts.URL+"/api/v1/system/role/"+role.Id, bearer, "")
	if code != http.StatusOK {
		t.Fatalf("delete role: status = %d body = %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/system/role/"+role.Id, bearer, "")
	if code != http.StatusNotFound {
		t.Fatalf("get deleted role: status = %d body = %s", code, body)
	}
}

// protovalidate 规则在边界生效：非法输入被 400 拒绝（携带有效令牌，排除鉴权干扰）。
func TestHTTPServer_ValidationRejected(t *testing.T) {
	t.Parallel()
	ts, signer := newTestHTTPServer(t)
	bearer := testBearer(t, signer, e2eActorID)

	cases := []struct {
		name, method, path, body string
	}{
		{
			"short password", http.MethodPost, "/api/v1/system/user",
			`{"username":"alice","password":"short","nickname":"Alice","gender":"male"}`,
		},
		{
			"bad username", http.MethodPost, "/api/v1/system/user",
			`{"username":"a!","password":"plain-secret","nickname":"Alice","gender":"male"}`,
		},
		{
			"bad gender", http.MethodPost, "/api/v1/system/user",
			`{"username":"alice","password":"plain-secret","nickname":"Alice","gender":"other"}`,
		},
		{
			"bad email", http.MethodPost, "/api/v1/system/user",
			`{"username":"alice","password":"plain-secret","nickname":"Alice","gender":"male","email":"not-an-email"}`,
		},
		{"invalid user id", http.MethodGet, "/api/v1/system/user/not-a-uuid", ""},
		{"oversize page_size", http.MethodGet, "/api/v1/system/user?page=1&page_size=101", ""},
		{"zero page", http.MethodGet, "/api/v1/system/user?page=0&page_size=10", ""},
		{"empty role update", http.MethodPut, "/api/v1/system/role/018f3e1a-7b1c-7cc1-9d2a-3f5e7a9b1c3d", `{}`},
		{"bad role code", http.MethodPost, "/api/v1/system/role", `{"name":"管理员","code":"Bad-Code"}`},
		{"zero user status", http.MethodPost, "/api/v1/system/user/018f3e1a-7b1c-7cc1-9d2a-3f5e7a9b1c3d/status", `{"status":0}`},
		{"unknown user status", http.MethodPost, "/api/v1/system/user/018f3e1a-7b1c-7cc1-9d2a-3f5e7a9b1c3d/status", `{"status":5}`},
		{
			"unknown create status", http.MethodPost, "/api/v1/system/user",
			`{"username":"dave","password":"plain-secret","nickname":"Dave","gender":"male","status":9}`,
		},
		{"unknown status filter", http.MethodGet, "/api/v1/system/user?page=1&page_size=10&status=7", ""},
		{
			"duplicate role_ids", http.MethodPut, "/api/v1/system/user/018f3e1a-7b1c-7cc1-9d2a-3f5e7a9b1c3d/role",
			`{"role_ids":["018f3e1a-7b1c-7cc1-9d2a-3f5e7a9b1c3d","018f3e1a-7b1c-7cc1-9d2a-3f5e7a9b1c3d"]}`,
		},
		{"short login password", http.MethodPost, "/api/v1/login", `{"username":"alice","password":"short"}`},
		{"bad login username", http.MethodPost, "/api/v1/login", `{"username":"a!","password":"plain-secret"}`},
		{"empty refresh token", http.MethodPost, "/api/v1/token/refresh", `{"refresh_token":""}`},
	}
	for _, tc := range cases {
		code, body := doRequestWithToken(t, tc.method, ts.URL+tc.path, bearer, tc.body)
		if code != http.StatusBadRequest {
			t.Errorf("%s: status = %d body = %q, want 400", tc.name, code, body)
		}
	}
}
