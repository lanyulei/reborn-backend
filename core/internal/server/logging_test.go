package server

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lanyulei/reborn/backend/core/internal/conf"
)

// TestMaskSensitiveAttr 锁定打码契约：仅顶层 args 键的字符串值按内容打码，
// 其余属性（含嵌套组、非字符串值）原样透传。
func TestMaskSensitiveAttr(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		groups []string
		attr   slog.Attr
		want   string
	}{
		{
			name: "login password masked",
			attr: slog.String("args", `username:"admin" password:"Admin123!"`),
			want: `username:"admin" password:"***"`,
		},
		{
			name: "refresh token masked",
			attr: slog.String("args", `refresh_token:"a79a623d8d1e9cd6"`),
			want: `refresh_token:"***"`,
		},
		{
			name: "create user password masked, others kept",
			attr: slog.String("args", `username:"alice" password:"plain-secret" nickname:"Alice" gender:1`),
			want: `username:"alice" password:"***" nickname:"Alice" gender:1`,
		},
		{
			name: "case insensitive field name",
			attr: slog.String("args", `Password:"x"`),
			want: `Password:"***"`,
		},
		{
			name: "webhook credential headers masked, benign header kept",
			attr: slog.String("args", `name:"hook" config:{webhook:{url:"https://h" headers:{key:"Authorization" value:"Bearer abc"} headers:{key:"X-Trace" value:"t-1"}}}`),
			want: `name:"hook" config:{webhook:{url:"https://h" headers:{key:"Authorization" value:"***"} headers:{key:"X-Trace" value:"t-1"}}}`,
		},
		{
			name: "generic token field masked",
			attr: slog.String("args", `token:"npt_abc" id:"x"`),
			want: `token:"***" id:"x"`,
		},
		{
			name: "no sensitive field untouched",
			attr: slog.String("args", `page:1 page_size:10 keyword:"ali"`),
			want: `page:1 page_size:10 keyword:"ali"`,
		},
		{
			name:   "nested group untouched",
			groups: []string{"inner"},
			attr:   slog.String("args", `password:"leak"`),
			want:   `password:"leak"`,
		},
		{
			name: "other keys untouched",
			attr: slog.String("password", "by-key-filtered-elsewhere"),
			want: "by-key-filtered-elsewhere",
		},
	}
	for _, tc := range cases {
		got := MaskSensitiveAttr(tc.groups, tc.attr)
		if got.Value.String() != tc.want {
			t.Errorf("%s: = %q, want %q", tc.name, got.Value.String(), tc.want)
		}
	}
}

// TestHTTPServer_LoginLogNoPlaintextPassword 端到端：登录失败（ERROR 级请求日志）
// 落盘内容不得包含明文密码；args 中密码字段以 *** 呈现，用户名保留可运维。
func TestHTTPServer_LoginLogNoPlaintextPassword(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{
		ReplaceAttr: MaskSensitiveAttr,
	}))
	svc := newTestServices(t)
	signer := svc.signer
	srv := NewHTTPServer(&conf.Server{Http: &conf.Server_HTTP{}}, logger, svc.user, svc.role, svc.auth, svc.syscfg, svc.loginLog, svc.notify, svc.template, svc.route, svc.silence, svc.subscription, svc.producer, svc.event, svc.dispatch, svc.deliveryLog, svc.deadLetter, svc.signer, svc.authz)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	bearer := testBearer(t, signer, e2eActorID)
	createUser(t, ts, bearer, "logger")

	code, body := doRequest(t, http.MethodPost, ts.URL+"/api/v1/login",
		`{"username":"logger","password":"wrong-pass-9"}`)
	if code != http.StatusUnauthorized {
		t.Fatalf("login wrong password: status = %d body = %q", code, body)
	}

	logged := buf.String()
	if !strings.Contains(logged, "server request") {
		t.Fatalf("no request log captured: %q", logged)
	}
	if strings.Contains(logged, "wrong-pass-9") {
		t.Fatalf("plaintext password leaked into log: %q", logged)
	}
	if !strings.Contains(logged, `password:\"***\"`) {
		t.Fatalf("masked password not present in log: %q", logged)
	}
	if !strings.Contains(logged, `username:\"logger\"`) {
		t.Fatalf("username (non-sensitive) should stay for ops: %q", logged)
	}
}
