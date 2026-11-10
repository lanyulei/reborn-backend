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

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"
)

// e2eActorID 是既有 e2e 用例的固定操作者 ID；鉴权只校验令牌签名与有效期，
// 非 user 作用域的接口不检查操作者存在性。
const e2eActorID = "018f3e1a-7b1c-7cc1-9d2a-3f5e7a9b0001"

// newTestHTTPServer 组装完整的中间件链 + 编解码 + service（真实 biz 与内存仓储）。
func newTestHTTPServer(t *testing.T) (*httptest.Server, *biz.TokenSigner) {
	t.Helper()
	ts, signer, _ := newTestHTTPServerWithStore(t)
	return ts, signer
}

// newTestHTTPServerWithStore 同 newTestHTTPServer，并暴露 testfake.Store 供
// 只读用例（事件/投递查询）预置运行数据。
func newTestHTTPServerWithStore(t *testing.T) (*httptest.Server, *biz.TokenSigner, *testfake.Store) {
	t.Helper()
	svc := newTestServices(t)
	srv := NewHTTPServer(
		&conf.Server{Http: &conf.Server_HTTP{}},
		testLogger(),
		svc.user,
		svc.role,
		svc.auth,
		svc.syscfg,
		svc.loginLog,
		svc.notify,
		svc.template,
		svc.route,
		svc.silence,
		svc.subscription,
		svc.producer,
		svc.event,
		svc.dispatch,
		svc.deliveryLog,
		svc.deadLetter,
		svc.signer,
		svc.authz,
	)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts, svc.signer, svc.store
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

// TestHTTPServer_NotifyChannelEndToEnd 端到端：notify 域路由注册、auth/authz
// 中间件、protojson oneof 编解码与 secret 只写不读契约全链路验证。
func TestHTTPServer_NotifyChannelEndToEnd(t *testing.T) {
	t.Parallel()
	ts, signer := newTestHTTPServer(t)
	bearer := testBearer(t, signer, e2eActorID)

	// 匿名请求被 auth 中间件拦截。
	code, _ := doRequest(t, http.MethodGet, ts.URL+"/api/v1/notify/channel?page=1&page_size=10", "")
	if code != http.StatusUnauthorized {
		t.Fatalf("anonymous list: status = %d, want 401", code)
	}

	// 创建：oneof detail 经 protojson 解码；响应不含 secret。
	code, body := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/notify/channel", bearer,
		`{"type":"NOTIFY_CHANNEL_TYPE_EMAIL","name":"email-corp","secret":"smtp-password",`+
			`"email":{"host":"smtp.example.com","port":465,"username":"ops","fromAddr":"ops@example.com","starttls":true},`+
			`"rateLimit":{"qps":5,"burst":10}}`)
	if code != http.StatusOK {
		t.Fatalf("create channel: status = %d body = %s", code, body)
	}
	if strings.Contains(body, "smtp-password") || strings.Contains(body, "secret") {
		t.Fatalf("create channel body %q must not contain secret", body)
	}
	var created struct {
		Channel struct {
			Id   string `json:"id"`
			Name string `json:"name"`
		} `json:"channel"`
	}
	if err := json.Unmarshal([]byte(body), &created); err != nil {
		t.Fatalf("create channel unmarshal %q: %v", body, err)
	}
	if created.Channel.Id == "" || created.Channel.Name != "email-corp" {
		t.Fatalf("create channel = %+v", created.Channel)
	}

	// 详情：类型化配置经 oneof 正确回读。
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/notify/channel/"+created.Channel.Id, bearer, "")
	if code != http.StatusOK {
		t.Fatalf("get channel: status = %d body = %s", code, body)
	}
	if !strings.Contains(body, `"email":{`) || !strings.Contains(body, `"host":"smtp.example.com"`) {
		t.Fatalf("get channel body = %q, want email detail", body)
	}

	// 列表：分页 + protojson int64 字符串编码。
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/notify/channel?page=1&page_size=10", bearer, "")
	if code != http.StatusOK {
		t.Fatalf("list channels: status = %d body = %s", code, body)
	}
	if !strings.Contains(body, `"total":"1"`) || !strings.Contains(body, `"name":"email-corp"`) {
		t.Fatalf("list channels body = %q", body)
	}

	// 删除后读不可见。
	code, _ = doRequestWithToken(t, http.MethodDelete, ts.URL+"/api/v1/notify/channel/"+created.Channel.Id, bearer, "")
	if code != http.StatusOK {
		t.Fatalf("delete channel: status = %d", code)
	}
	code, _ = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/notify/channel/"+created.Channel.Id, bearer, "")
	if code != http.StatusNotFound {
		t.Fatalf("get deleted channel: status = %d, want 404", code)
	}
}

// TestHTTPServer_NotifyTemplateEndToEnd 端到端：模板路由注册、鉴权中间件、
// spec 校验（409 组合冲突）与模板文本白名单校验（400 NOTIFY_TEMPLATE_INVALID）。
func TestHTTPServer_NotifyTemplateEndToEnd(t *testing.T) {
	t.Parallel()
	ts, signer := newTestHTTPServer(t)
	bearer := testBearer(t, signer, e2eActorID)

	// 匿名请求被 auth 中间件拦截。
	code, _ := doRequest(t, http.MethodGet, ts.URL+"/api/v1/notify/template?page=1&page_size=10", "")
	if code != http.StatusUnauthorized {
		t.Fatalf("anonymous list: status = %d, want 401", code)
	}

	// 创建：模板文本合法，version 起始 1。
	code, body := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/notify/template", bearer,
		`{"eventType":"monitor.alert.firing","channelType":"dingtalk",`+
			`"subject":"[{{.BizID}}] {{.Title}}","body":"级别 {{.Severity}}：{{.Title}}（{{.Count}} 条）"}`)
	if code != http.StatusOK {
		t.Fatalf("create template: status = %d body = %s", code, body)
	}
	var created struct {
		Template struct {
			Id      string `json:"id"`
			Version int32  `json:"version"`
		} `json:"template"`
	}
	if err := json.Unmarshal([]byte(body), &created); err != nil {
		t.Fatalf("create template unmarshal %q: %v", body, err)
	}
	if created.Template.Id == "" || created.Template.Version != 1 {
		t.Fatalf("create template = %+v", created.Template)
	}

	// 组合冲突：409 + NOTIFY_TEMPLATE_SPEC_EXISTS。
	code, body = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/notify/template", bearer,
		`{"eventType":"monitor.alert.firing","channelType":"dingtalk","subject":"s","body":"b {{.Title}}"}`)
	if code != http.StatusConflict || !strings.Contains(body, "NOTIFY_TEMPLATE_SPEC_EXISTS") {
		t.Fatalf("duplicate spec: status = %d body = %q, want 409 NOTIFY_TEMPLATE_SPEC_EXISTS", code, body)
	}

	// 白名单外变量：400 + NOTIFY_TEMPLATE_INVALID（biz 校验，非 protovalidate）。
	code, body = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/notify/template", bearer,
		`{"eventType":"monitor.alert.resolved","channelType":"dingtalk","subject":"s","body":"{{.Titel}}"}`)
	if code != http.StatusBadRequest || !strings.Contains(body, "NOTIFY_TEMPLATE_INVALID") {
		t.Fatalf("unknown variable: status = %d body = %q, want 400 NOTIFY_TEMPLATE_INVALID", code, body)
	}

	// 更新：正文换合法文本，version 自增对读路径可见。
	code, body = doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/notify/template/"+created.Template.Id, bearer,
		`{"body":"恢复：{{.Title}}"}`)
	if code != http.StatusOK {
		t.Fatalf("update template: status = %d body = %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/notify/template/"+created.Template.Id, bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"version":2`) || !strings.Contains(body, `恢复：{{.Title}}`) {
		t.Fatalf("get template: status = %d body = %s", code, body)
	}

	// 列表：事件过滤 + 分页。
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/template?page=1&page_size=10&eventType=monitor.alert.firing", bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"total":"1"`) {
		t.Fatalf("list templates: status = %d body = %s", code, body)
	}

	// 删除后读不可见。
	code, _ = doRequestWithToken(t, http.MethodDelete, ts.URL+"/api/v1/notify/template/"+created.Template.Id, bearer, "")
	if code != http.StatusOK {
		t.Fatalf("delete template: status = %d", code)
	}
	code, _ = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/notify/template/"+created.Template.Id, bearer, "")
	if code != http.StatusNotFound {
		t.Fatalf("get deleted template: status = %d, want 404", code)
	}
}

// TestHTTPServer_NotifyRouteEndToEnd 端到端：路由注册与鉴权、调优项默认值
// 解析、protovalidate 模式拒绝（400）与渠道绑定校验（400 NOTIFY_ROUTE_INVALID）、
// spec 整组替换与独立启停。
func TestHTTPServer_NotifyRouteEndToEnd(t *testing.T) {
	t.Parallel()
	ts, signer := newTestHTTPServer(t)
	bearer := testBearer(t, signer, e2eActorID)

	// 匿名请求被 auth 中间件拦截。
	code, _ := doRequest(t, http.MethodGet, ts.URL+"/api/v1/notify/route?page=1&page_size=10", "")
	if code != http.StatusUnauthorized {
		t.Fatalf("anonymous list: status = %d, want 401", code)
	}

	// 预置一个渠道实例供路由绑定。
	code, body := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/notify/channel", bearer,
		`{"type":"NOTIFY_CHANNEL_TYPE_DINGTALK","name":"dingtalk-ops","secret":"robot-token",`+
			`"robot":{"atMobiles":["13800000000"]}}`)
	if code != http.StatusOK {
		t.Fatalf("create channel: status = %d body = %s", code, body)
	}
	var ch struct {
		Channel struct {
			Id string `json:"id"`
		} `json:"channel"`
	}
	if err := json.Unmarshal([]byte(body), &ch); err != nil {
		t.Fatalf("create channel unmarshal %q: %v", body, err)
	}

	// 创建：缺省调优项解析为 severity 1/priority 5/窗口 0/阈值 500/时长 30。
	code, body = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/notify/route", bearer,
		`{"spec":{"eventPattern":"monitor.alert.*","channelIds":["`+ch.Channel.Id+
			`"],"recipient":{"fixed":["ops@example.com"]}}}`)
	if code != http.StatusOK {
		t.Fatalf("create route: status = %d body = %s", code, body)
	}
	var created struct {
		Route struct {
			Id string `json:"id"`
		} `json:"route"`
	}
	if err := json.Unmarshal([]byte(body), &created); err != nil {
		t.Fatalf("create route unmarshal %q: %v", body, err)
	}
	if created.Route.Id == "" {
		t.Fatalf("create route = %q", body)
	}
	for _, want := range []string{
		`"severityMin":1`, `"priority":5`, `"aggregateWindowS":0`,
		`"suppressThreshold":500`, `"suppressDurationM":30`, `"enabled":true`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("create route body %q missing %s", body, want)
		}
	}

	// 非法模式：protovalidate 400。
	code, body = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/notify/route", bearer,
		`{"spec":{"eventPattern":"monitor.*.firing","channelIds":["`+ch.Channel.Id+
			`"],"recipient":{"fixed":["ops@example.com"]}}}`)
	if code != http.StatusBadRequest {
		t.Fatalf("bad pattern: status = %d body = %q, want 400", code, body)
	}

	// 引用不存在渠道：biz 校验 400 + NOTIFY_ROUTE_INVALID。
	code, body = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/notify/route", bearer,
		`{"spec":{"eventPattern":"resource.change","channelIds":["018f3e1a-0301-7000-8000-0000000000ff"],`+
			`"recipient":{"subscription":true}}}`)
	if code != http.StatusBadRequest || !strings.Contains(body, "NOTIFY_ROUTE_INVALID") {
		t.Fatalf("missing channel: status = %d body = %q, want 400 NOTIFY_ROUTE_INVALID", code, body)
	}

	// spec 整组替换：换模式 + 订阅收件 + 聚合窗口。
	code, body = doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/notify/route/"+created.Route.Id, bearer,
		`{"spec":{"eventPattern":"permission.change","channelIds":["`+ch.Channel.Id+
			`"],"recipient":{"subscription":true},"aggregateWindowS":120}}`)
	if code != http.StatusOK {
		t.Fatalf("update route: status = %d body = %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/notify/route/"+created.Route.Id, bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"eventPattern":"permission.change"`) ||
		!strings.Contains(body, `"subscription":true`) || !strings.Contains(body, `"aggregateWindowS":120`) {
		t.Fatalf("get route: status = %d body = %s", code, body)
	}

	// 独立启停不动规则。
	code, body = doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/notify/route/"+created.Route.Id, bearer,
		`{"enabled":false}`)
	if code != http.StatusOK {
		t.Fatalf("toggle route: status = %d body = %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/route?page=1&page_size=10&enabled=false", bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"total":"1"`) {
		t.Fatalf("list disabled routes: status = %d body = %s", code, body)
	}

	// 删除后读不可见。
	code, _ = doRequestWithToken(t, http.MethodDelete, ts.URL+"/api/v1/notify/route/"+created.Route.Id, bearer, "")
	if code != http.StatusOK {
		t.Fatalf("delete route: status = %d", code)
	}
	code, _ = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/notify/route/"+created.Route.Id, bearer, "")
	if code != http.StatusNotFound {
		t.Fatalf("get deleted route: status = %d, want 404", code)
	}
}

// TestHTTPServer_NotifySilenceEndToEnd 端到端：路由注册与鉴权、调优项默认值
// 解析（severityMax=2、无窗口=全程）、时间窗往返与非法校验（400
// NOTIFY_SILENCE_INVALID）、spec 整组替换清除窗口与独立启停。
func TestHTTPServer_NotifySilenceEndToEnd(t *testing.T) {
	t.Parallel()
	ts, signer := newTestHTTPServer(t)
	bearer := testBearer(t, signer, e2eActorID)

	// 匿名请求被 auth 中间件拦截。
	code, _ := doRequest(t, http.MethodGet, ts.URL+"/api/v1/notify/silence?page=1&page_size=10", "")
	if code != http.StatusUnauthorized {
		t.Fatalf("anonymous list: status = %d, want 401", code)
	}

	// 创建：缺省 severityMax=2 + 跨午夜时间窗（含时区）往返。
	code, body := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/notify/silence", bearer,
		`{"spec":{"eventPattern":"monitor.alert.*",`+
			`"timeWindow":{"start":"22:00","end":"08:00","tz":"Asia/Shanghai"}}}`)
	if code != http.StatusOK {
		t.Fatalf("create silence: status = %d body = %s", code, body)
	}
	var created struct {
		Silence struct {
			Id string `json:"id"`
		} `json:"silence"`
	}
	if err := json.Unmarshal([]byte(body), &created); err != nil {
		t.Fatalf("create silence unmarshal %q: %v", body, err)
	}
	if created.Silence.Id == "" {
		t.Fatalf("create silence = %q", body)
	}
	for _, want := range []string{
		`"severityMax":2`, `"enabled":true`, `"bizId":"default"`,
		`"start":"22:00"`, `"end":"08:00"`, `"tz":"Asia/Shanghai"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("create silence body %q missing %s", body, want)
		}
	}

	// 非法模式：protovalidate 400。
	code, body = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/notify/silence", bearer,
		`{"spec":{"eventPattern":"monitor.*.firing"}}`)
	if code != http.StatusBadRequest {
		t.Fatalf("bad pattern: status = %d body = %q, want 400", code, body)
	}

	// 非法时间窗：零长（起点等于终点）→ biz 校验 400 NOTIFY_SILENCE_INVALID。
	code, body = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/notify/silence", bearer,
		`{"spec":{"eventPattern":"monitor.alert.*","timeWindow":{"start":"09:00","end":"09:00"}}}`)
	if code != http.StatusBadRequest || !strings.Contains(body, "NOTIFY_SILENCE_INVALID") {
		t.Fatalf("zero-length window: status = %d body = %q, want 400 NOTIFY_SILENCE_INVALID", code, body)
	}

	// 未知时区：biz 校验 400 NOTIFY_SILENCE_INVALID。
	code, body = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/notify/silence", bearer,
		`{"spec":{"eventPattern":"monitor.alert.*",`+
			`"timeWindow":{"start":"22:00","end":"08:00","tz":"Mars/Olympus"}}}`)
	if code != http.StatusBadRequest || !strings.Contains(body, "NOTIFY_SILENCE_INVALID") {
		t.Fatalf("bad tz: status = %d body = %q, want 400 NOTIFY_SILENCE_INVALID", code, body)
	}

	// spec 整组替换：换模式 + 不传 time_window 即清除窗口（响应省略 timeWindow）。
	code, body = doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/notify/silence/"+created.Silence.Id, bearer,
		`{"spec":{"eventPattern":"permission.change","severityMax":4}}`)
	if code != http.StatusOK {
		t.Fatalf("update silence: status = %d body = %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/notify/silence/"+created.Silence.Id, bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"eventPattern":"permission.change"`) ||
		!strings.Contains(body, `"severityMax":4`) || strings.Contains(body, "timeWindow") {
		t.Fatalf("get silence: status = %d body = %s", code, body)
	}

	// 独立启停不动规则。
	code, body = doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/notify/silence/"+created.Silence.Id, bearer,
		`{"enabled":false}`)
	if code != http.StatusOK {
		t.Fatalf("toggle silence: status = %d body = %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/silence?page=1&page_size=10&enabled=false", bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"total":"1"`) {
		t.Fatalf("list disabled silences: status = %d body = %s", code, body)
	}
	// keyword 模糊匹配 event_pattern。
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/silence?page=1&page_size=10&keyword=permission", bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"total":"1"`) {
		t.Fatalf("list silences by keyword: status = %d body = %s", code, body)
	}

	// 删除后读不可见。
	code, _ = doRequestWithToken(t, http.MethodDelete, ts.URL+"/api/v1/notify/silence/"+created.Silence.Id, bearer, "")
	if code != http.StatusOK {
		t.Fatalf("delete silence: status = %d", code)
	}
	code, _ = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/notify/silence/"+created.Silence.Id, bearer, "")
	if code != http.StatusNotFound {
		t.Fatalf("get deleted silence: status = %d, want 404", code)
	}
}

// TestHTTPServer_NotifySubscriptionEndToEnd 端到端：路由注册与鉴权、偏好
// jsonb 往返（protojson camelCase）、user_id 缺省=本人（claims）、admin 代管
// 他人、同自然键 Upsert 替换与软删重建。
func TestHTTPServer_NotifySubscriptionEndToEnd(t *testing.T) {
	t.Parallel()
	ts, signer := newTestHTTPServer(t)
	bearer := testBearer(t, signer, e2eActorID) // is_admin 操作者

	// 匿名请求被 auth 中间件拦截。
	code, _ := doRequest(t, http.MethodGet, ts.URL+"/api/v1/notify/subscription?page=1&page_size=10", "")
	if code != http.StatusUnauthorized {
		t.Fatalf("anonymous list: status = %d, want 401", code)
	}

	// 保存本人订阅：user_id 缺省=操作者；偏好开关/目标群两形态往返。
	code, body := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/notify/subscription", bearer,
		`{"eventPattern":"monitor.alert.*","channelPrefs":{`+
			`"email":{"subscribed":true},"dingtalk":{"target":"$group:ops"}}}`)
	if code != http.StatusOK {
		t.Fatalf("upsert subscription: status = %d body = %s", code, body)
	}
	var created struct {
		Subscription struct {
			Id string `json:"id"`
		} `json:"subscription"`
	}
	if err := json.Unmarshal([]byte(body), &created); err != nil {
		t.Fatalf("upsert subscription unmarshal %q: %v", body, err)
	}
	if created.Subscription.Id == "" {
		t.Fatalf("upsert subscription = %q", body)
	}
	for _, want := range []string{
		`"userId":"` + e2eActorID + `"`, `"bizId":"default"`, `"enabled":true`,
		`"subscribed":true`, `"target":"$group:ops"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("upsert subscription body %q missing %s", body, want)
		}
	}

	// admin 代管他人：合法且归属目标用户。
	other := "018f3e1a-0304-7000-8000-0000000000b1"
	code, body = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/notify/subscription", bearer,
		`{"userId":"`+other+`","eventPattern":"permission.change",`+
			`"channelPrefs":{"wecom":{"subscribed":true}}}`)
	if code != http.StatusOK {
		t.Fatalf("upsert for other: status = %d body = %s", code, body)
	}
	if !strings.Contains(body, `"userId":"`+other+`"`) {
		t.Fatalf("upsert for other body %q missing target userId", body)
	}

	// 非法模式：protovalidate 400。
	code, body = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/notify/subscription", bearer,
		`{"eventPattern":"monitor.*.firing","channelPrefs":{"email":{"subscribed":true}}}`)
	if code != http.StatusBadRequest {
		t.Fatalf("bad pattern: status = %d body = %q, want 400", code, body)
	}

	// 同自然键再次保存 = 替换（同 id、偏好换向、启停）。
	code, body = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/notify/subscription", bearer,
		`{"eventPattern":"monitor.alert.*","channelPrefs":{"feishu":{"subscribed":true}},"enabled":false}`)
	if code != http.StatusOK {
		t.Fatalf("upsert replace: status = %d body = %s", code, body)
	}
	var replaced struct {
		Subscription struct {
			Id string `json:"id"`
		} `json:"subscription"`
	}
	if err := json.Unmarshal([]byte(body), &replaced); err != nil {
		t.Fatalf("upsert replace unmarshal %q: %v", body, err)
	}
	if replaced.Subscription.Id != created.Subscription.Id {
		t.Fatalf("upsert replace id = %q, want %q（自然键替换）", replaced.Subscription.Id, created.Subscription.Id)
	}
	if !strings.Contains(body, `"enabled":false`) || !strings.Contains(body, `"feishu"`) {
		t.Fatalf("upsert replace body %q missing replaced fields", body)
	}

	// 列表：缺省=本人（不含代管行）；keyword 过滤。
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/notify/subscription?page=1&page_size=10", bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"total":"1"`) || strings.Contains(body, "permission.change") {
		t.Fatalf("list own: status = %d body = %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/subscription?page=1&page_size=10&user_id="+other, bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"total":"1"`) {
		t.Fatalf("list other (admin): status = %d body = %s", code, body)
	}

	// 删除本人订阅，删除后本人列表清空（代管行不泄漏进本人列表）。
	code, _ = doRequestWithToken(t, http.MethodDelete, ts.URL+"/api/v1/notify/subscription/"+replaced.Subscription.Id, bearer, "")
	if code != http.StatusOK {
		t.Fatalf("delete own: status = %d", code)
	}
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/notify/subscription?page=1&page_size=10", bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"total":"0"`) {
		t.Fatalf("list after delete own: status = %d body = %s", code, body)
	}
}
