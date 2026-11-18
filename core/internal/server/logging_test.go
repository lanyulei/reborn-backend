package server

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	monitorservice "github.com/lanyulei/reborn/backend/core/internal/service/monitor"

	"github.com/lanyulei/reborn/backend/core/internal/conf"
	"github.com/lanyulei/reborn/backend/kit/httpx"
)

// TestHTTPServer_LoginLogNoPlaintextPassword 端到端：登录失败（ERROR 级请求日志）
// 落盘内容不得包含明文密码；args 中密码字段以 *** 呈现，用户名保留可运维。
func TestHTTPServer_LoginLogNoPlaintextPassword(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{
		ReplaceAttr: httpx.MaskSensitiveAttr,
	}))
	svc := newTestServices(t)
	signer := svc.signer
	srv := NewHTTPServer(&conf.Server{Http: &conf.Server_HTTP{}}, logger, svc.user, svc.role, svc.dept, svc.auth, svc.syscfg, svc.loginLog, svc.notify, svc.template, svc.route, svc.silence, svc.subscription, svc.producer, svc.event, svc.dispatch, svc.deliveryLog, svc.deadLetter, svc.assistant, svc.assistantChat, svc.cmdbModel, svc.cmdbCI, svc.cmdbRelation, svc.cmdbScope, svc.cmdbSync, svc.cmdbInsight, nil, nil, nil, svc.file, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, monitorservice.P2Services{}, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, svc.signer, svc.authz, AllRoles(), nil, nil)
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
