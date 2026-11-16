package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/lanyulei/reborn/backend/core/internal/conf"
)

func TestNewRoles(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cases := []struct {
		name string
		conf []string
		env  string
		want []string
		bad  bool
	}{
		{name: "default is every role", want: []string{RoleAPI, RoleWorker}},
		{name: "from config", conf: []string{"worker"}, want: []string{RoleWorker}},
		{name: "env wins and is normalised", conf: []string{"worker"}, env: " API , ", want: []string{RoleAPI}},
		{name: "both", env: "worker,api", want: []string{RoleAPI, RoleWorker}},
		{name: "blank env falls back to config", conf: []string{"api"}, env: "  ", want: []string{RoleAPI}},
		{name: "typo fails fast", env: "api,wroker", bad: true},
		{name: "typo in config fails fast", conf: []string{"scheduler"}, bad: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv(rolesEnv, c.env)
			roles, err := NewRoles(&conf.Server{Roles: c.conf}, logger)
			if c.bad {
				if err == nil {
					t.Fatalf("NewRoles() = %v, want an error for an unknown role", roles.List())
				}
				return
			}
			if err != nil || !slices.Equal(roles.List(), c.want) {
				t.Fatalf("NewRoles() = %v, %v; want %v", roles.List(), err, c.want)
			}
		})
	}
}

type pingStub struct{ err error }

func (p pingStub) Ping(context.Context) error { return p.err }

// 只承担 worker 角色的进程：探活端点可用，业务接口一个都不注册。
func TestWorkerOnlyServerExposesNoBusinessRoutes(t *testing.T) {
	svc := newTestServices(t)
	build := func(roles Roles, pinger Pinger) *httptest.Server {
		srv := NewHTTPServer(&conf.Server{Http: &conf.Server_HTTP{}}, slog.New(slog.NewTextHandler(io.Discard, nil)),
			svc.user, svc.role, svc.dept, svc.auth, svc.syscfg, svc.loginLog, svc.notify, svc.template, svc.route, svc.silence,
			svc.subscription, svc.producer, svc.event, svc.dispatch, svc.deliveryLog, svc.deadLetter, svc.assistant, svc.assistantChat,
			svc.cmdbModel, svc.cmdbCI, svc.cmdbRelation, svc.cmdbScope, svc.cmdbSync, svc.cmdbInsight, nil, nil, nil, svc.file,
			nil, nil, nil, nil, nil, nil, nil, svc.signer, svc.authz, roles, pinger, nil)
		ts := httptest.NewServer(srv)
		t.Cleanup(ts.Close)
		return ts
	}
	get := func(ts *httptest.Server, path string) (int, string) {
		t.Helper()
		resp, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer func() { _ = resp.Body.Close() }()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body)
	}

	worker := build(Roles{RoleWorker: {}}, pingStub{})
	if code, body := get(worker, "/healthz"); code != http.StatusOK || body != "ok" {
		t.Fatalf("worker /healthz = %d %q", code, body)
	}
	// 登录配置是匿名可读的业务接口：api 角色下 200，worker 角色下根本没有这条路由。
	if code, _ := get(worker, "/api/v1/system/config"); code != http.StatusNotFound {
		t.Fatalf("worker-only process serves a business route: status %d, want 404", code)
	}
	api := build(AllRoles(), pingStub{})
	if code, _ := get(api, "/api/v1/system/config"); code != http.StatusOK {
		t.Fatalf("api /api/v1/system/config = %d, want 200", code)
	}
	// 数据库不通：探活返回 503，且不回显底层错误。
	down := build(AllRoles(), pingStub{err: errors.New("dial tcp 10.0.0.9:5432: connection refused")})
	if code, body := get(down, "/healthz"); code != http.StatusServiceUnavailable || strings.Contains(body, "10.0.0.9") {
		t.Fatalf("/healthz with the database down = %d %q", code, body)
	}
}
