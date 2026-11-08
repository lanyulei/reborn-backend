package server

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

// loginJSON 是登录/刷新响应的断言子集（protojson 默认 camelCase 字段名，
// int64 编码为字符串）。
type loginJSON struct {
	AccessToken  string   `json:"accessToken"`
	ExpiresIn    string   `json:"expiresIn"`
	RefreshToken string   `json:"refreshToken"`
	User         userJSON `json:"user"`
}

// 鉴权门禁：白名单外的接口一律要求 Bearer 访问令牌；令牌错误形态翻译为
// proto 错误码（TOKEN_INVALID/TOKEN_EXPIRED），有效令牌放行。
func TestHTTPServer_AuthGate(t *testing.T) {
	t.Parallel()
	ts, signer := newTestHTTPServer(t)
	const listURL = "/api/v1/system/user?page=1&page_size=10"

	expired, _, err := signer.SignAccess(e2eActorID, "e2e-actor", time.Now().Add(-16*time.Minute))
	if err != nil {
		t.Fatalf("sign expired token: %v", err)
	}

	cases := []struct {
		name     string
		token    string
		wantCode int
		wantBody string
	}{
		{"no token", "", http.StatusUnauthorized, "TOKEN_INVALID"},
		{"garbage token", "not-a-jwt", http.StatusUnauthorized, "TOKEN_INVALID"},
		{"expired token", expired, http.StatusUnauthorized, "TOKEN_EXPIRED"},
	}
	for _, tc := range cases {
		code, body := doRequestWithToken(t, http.MethodGet, ts.URL+listURL, tc.token, "")
		if code != tc.wantCode || !strings.Contains(body, tc.wantBody) {
			t.Errorf("%s: status = %d body = %q, want %d containing %q", tc.name, code, body, tc.wantCode, tc.wantBody)
		}
	}

	// 有效令牌放行。
	code, body := doRequestWithToken(t, http.MethodGet, ts.URL+listURL, testBearer(t, signer, e2eActorID), "")
	if code != http.StatusOK {
		t.Fatalf("valid token: status = %d body = %q", code, body)
	}
}

// 登录全链路：登录（免鉴权）→ me → 刷新（免鉴权、轮换）→ 旧令牌重放拒绝 →
// 登出吊销 → 登出后刷新被拒（access 无状态的固有窗口由短 TTL 缓解）。
func TestHTTPServer_AuthLoginFlow(t *testing.T) {
	t.Parallel()
	ts, signer := newTestHTTPServer(t)
	bearer := testBearer(t, signer, e2eActorID)
	createUser(t, ts, bearer, "bob")

	// 错误密码与不存在的用户：同一错误口径（401 INVALID_CREDENTIALS，防枚举）。
	code, body := doRequest(t, http.MethodPost, ts.URL+"/api/v1/login",
		`{"username":"bob","password":"wrong-password"}`)
	if code != http.StatusUnauthorized || !strings.Contains(body, "INVALID_CREDENTIALS") {
		t.Fatalf("login wrong password: status = %d body = %q", code, body)
	}
	code, body = doRequest(t, http.MethodPost, ts.URL+"/api/v1/login",
		`{"username":"nobody","password":"wrong-password"}`)
	if code != http.StatusUnauthorized || !strings.Contains(body, "INVALID_CREDENTIALS") {
		t.Fatalf("login unknown user: status = %d body = %q", code, body)
	}

	// 正确登录：双令牌 + 用户信息，响应不含密码。
	code, body = doRequest(t, http.MethodPost, ts.URL+"/api/v1/login",
		`{"username":"bob","password":"plain-secret"}`)
	if code != http.StatusOK {
		t.Fatalf("login: status = %d body = %q", code, body)
	}
	var login loginJSON
	if err := json.Unmarshal([]byte(body), &login); err != nil {
		t.Fatalf("login unmarshal %q: %v", body, err)
	}
	if login.AccessToken == "" || login.RefreshToken == "" || login.ExpiresIn != "900" {
		t.Fatalf("login tokens = %+v", login)
	}
	if login.User.Username != "bob" || strings.Contains(body, "password") {
		t.Fatalf("login user = %+v body = %q", login.User, body)
	}

	// me：携带 access token 返回当前登录者。
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/me", login.AccessToken, "")
	if code != http.StatusOK || !strings.Contains(body, `"username":"bob"`) {
		t.Fatalf("me: status = %d body = %q", code, body)
	}

	// 刷新免鉴权：换发新双令牌。
	code, body = doRequest(t, http.MethodPost, ts.URL+"/api/v1/token/refresh",
		`{"refresh_token":"`+login.RefreshToken+`"}`)
	if code != http.StatusOK {
		t.Fatalf("refresh: status = %d body = %q", code, body)
	}
	var refreshed loginJSON
	if err := json.Unmarshal([]byte(body), &refreshed); err != nil {
		t.Fatalf("refresh unmarshal %q: %v", body, err)
	}
	if refreshed.AccessToken == "" || refreshed.RefreshToken == "" || refreshed.RefreshToken == login.RefreshToken {
		t.Fatalf("refresh tokens = %+v", refreshed)
	}

	// 旧刷新令牌已被轮换：重放 → 401。
	code, body = doRequest(t, http.MethodPost, ts.URL+"/api/v1/token/refresh",
		`{"refresh_token":"`+login.RefreshToken+`"}`)
	if code != http.StatusUnauthorized {
		t.Fatalf("replay rotated refresh token: status = %d body = %q", code, body)
	}

	// 登出：吊销当前用户全部刷新令牌。
	code, body = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/logout", refreshed.AccessToken, `{}`)
	if code != http.StatusOK {
		t.Fatalf("logout: status = %d body = %q", code, body)
	}

	// 登出后刷新被拒；access token 无状态仍有效（短 TTL 固有窗口，行为文档化）。
	code, body = doRequest(t, http.MethodPost, ts.URL+"/api/v1/token/refresh",
		`{"refresh_token":"`+refreshed.RefreshToken+`"}`)
	if code != http.StatusUnauthorized {
		t.Fatalf("refresh after logout: status = %d body = %q", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/me", refreshed.AccessToken, "")
	if code != http.StatusOK {
		t.Fatalf("me after logout (access residual window): status = %d body = %q", code, body)
	}
}

// 账号非可用状态：凭据正确也拒绝登录（403 ACCOUNT_DISABLED）。
func TestHTTPServer_LoginDisabledAccount(t *testing.T) {
	t.Parallel()
	ts, signer := newTestHTTPServer(t)
	bearer := testBearer(t, signer, e2eActorID)
	user := createUser(t, ts, bearer, "carl")

	code, body := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/system/user/"+user.Id+"/status",
		bearer, `{"status":2}`)
	if code != http.StatusOK {
		t.Fatalf("disable user: status = %d body = %q", code, body)
	}

	code, body = doRequest(t, http.MethodPost, ts.URL+"/api/v1/login",
		`{"username":"carl","password":"plain-secret"}`)
	if code != http.StatusForbidden || !strings.Contains(body, "ACCOUNT_DISABLED") {
		t.Fatalf("login disabled account: status = %d body = %q", code, body)
	}
}

// 前端 b64$ 编码密码全链路：创建/登录/改密接受编码值，明文直传继续兼容；
// 非法编码与解码后长度越界均 400 FIELD_DECODE。
func TestHTTPServer_B64PasswordFlow(t *testing.T) {
	t.Parallel()
	ts, signer := newTestHTTPServer(t)
	bearer := testBearer(t, signer, e2eActorID)
	b64pw := "b64$" + base64.StdEncoding.EncodeToString([]byte("plain-secret"))

	// 用编码密码建用户。
	code, body := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/system/user", bearer,
		`{"username":"dave","password":"`+b64pw+`","nickname":"Dave","gender":"male"}`)
	if code != http.StatusOK {
		t.Fatalf("create user with b64 password: status = %d body = %q", code, body)
	}
	var created struct {
		User struct{ Id string } `json:"user"`
	}
	if err := json.Unmarshal([]byte(body), &created); err != nil {
		t.Fatalf("create user unmarshal %q: %v", body, err)
	}

	// 编码值与明文登录等价。
	code, body = doRequest(t, http.MethodPost, ts.URL+"/api/v1/login",
		`{"username":"dave","password":"`+b64pw+`"}`)
	if code != http.StatusOK {
		t.Fatalf("login with b64 password: status = %d body = %q", code, body)
	}
	code, body = doRequest(t, http.MethodPost, ts.URL+"/api/v1/login",
		`{"username":"dave","password":"plain-secret"}`)
	if code != http.StatusOK {
		t.Fatalf("login with plaintext: status = %d body = %q", code, body)
	}

	// 非法 Base64 → 400；编码合法但解码后过短 → 400（同一 reason）。
	code, body = doRequest(t, http.MethodPost, ts.URL+"/api/v1/login",
		`{"username":"dave","password":"b64$!!not-base64!!"}`)
	if code != http.StatusBadRequest || !strings.Contains(body, "FIELD_DECODE") {
		t.Fatalf("login invalid base64: status = %d body = %q", code, body)
	}
	code, body = doRequest(t, http.MethodPost, ts.URL+"/api/v1/login",
		`{"username":"dave","password":"b64$`+base64.StdEncoding.EncodeToString([]byte("1234567"))+`"}`)
	if code != http.StatusBadRequest || !strings.Contains(body, "FIELD_DECODE") {
		t.Fatalf("login decoded-short password: status = %d body = %q", code, body)
	}

	// 改密接口同样接受编码值：改完新编码值可登录、旧密码失效。
	newB64 := "b64$" + base64.StdEncoding.EncodeToString([]byte("new-secret-123"))
	code, body = doRequestWithToken(t, http.MethodPost,
		ts.URL+"/api/v1/system/user/"+created.User.Id+"/password", bearer,
		`{"password":"`+newB64+`"}`)
	if code != http.StatusOK {
		t.Fatalf("reset password with b64 value: status = %d body = %q", code, body)
	}
	code, body = doRequest(t, http.MethodPost, ts.URL+"/api/v1/login",
		`{"username":"dave","password":"`+newB64+`"}`)
	if code != http.StatusOK {
		t.Fatalf("login with new b64 password: status = %d body = %q", code, body)
	}
	code, body = doRequest(t, http.MethodPost, ts.URL+"/api/v1/login",
		`{"username":"dave","password":"plain-secret"}`)
	if code != http.StatusUnauthorized {
		t.Fatalf("login with old password after reset: status = %d body = %q", code, body)
	}
}
