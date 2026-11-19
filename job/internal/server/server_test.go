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
	"time"

	"github.com/lanyulei/reborn/backend/job/internal/biz"
	"github.com/lanyulei/reborn/backend/job/internal/biztest"
	"github.com/lanyulei/reborn/backend/job/internal/conf"
	"github.com/lanyulei/reborn/backend/job/internal/service"
	"github.com/lanyulei/reborn/backend/job/internal/worker"
	"github.com/lanyulei/reborn/backend/kit/jobcontract"
	"github.com/lanyulei/reborn/backend/kit/kitconf"

	"github.com/go-kratos/kratos/v3/transport"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestNewRoles(t *testing.T) {
	t.Setenv("SERVER_ROLES", "")
	must := func(c *conf.Server) conf.Roles {
		t.Helper()
		roles, err := conf.NewRoles(c)
		if err != nil {
			t.Fatalf("NewRoles() error = %v", err)
		}
		return roles
	}
	all := must(&conf.Server{})
	if !all.Has(conf.RoleEngine) || !all.Has(conf.RoleRunner) || len(all.List()) != 2 {
		t.Fatalf("empty config must enable all roles: %v", all.List())
	}
	if nilConf := must(nil); len(nilConf.List()) != 2 {
		t.Fatalf("nil config must enable all roles: %v", nilConf.List())
	}
	only := must(&conf.Server{Roles: []string{" Runner "}})
	if only.Has("engine") || !only.Has("runner") {
		t.Fatalf("roles = %v", only.List())
	}
	if _, err := conf.NewRoles(&conf.Server{Roles: []string{"runner", "bogus"}}); err == nil {
		t.Fatal("unknown role must fail assembly instead of being silently dropped")
	}
	t.Setenv("SERVER_ROLES", "engine")
	env := must(&conf.Server{Roles: []string{"runner"}})
	if !env.Has("engine") || env.Has("runner") {
		t.Fatalf("env must take precedence: %v", env.List())
	}
	t.Setenv("SERVER_ROLES", "runner, engine")
	if got := must(nil).List(); !slices.Equal(got, []string{"engine", "runner"}) {
		t.Fatalf("list order = %v", got)
	}
}

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

type pinger struct{ err error }

func (p pinger) Ping() error { return p.err }

func do(t *testing.T, method, url, token string) (int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, url, strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func TestHTTPServerHealthAndMetrics(t *testing.T) {
	metrics, cleanup, err := NewMetrics()
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	ctx := context.Background()
	metrics.Record(ctx, "lease-sweep", 3, nil)
	metrics.Record(ctx, "log-purge", 0, errors.New("boom"))
	metrics.ObserveCall(ctx, biz.OpClaim, biz.ResultEmpty)
	metrics.AddExecutions(ctx, biz.EventLeaseExpired, 2)

	p := &pinger{}
	ts := httptest.NewServer(NewHTTPServer(&conf.Server{Http: &kitconf.HTTP{Timeout: durationpb.New(time.Second)}}, testLogger(), p))
	defer ts.Close()
	if code, body := do(t, http.MethodGet, ts.URL+"/healthz", ""); code != http.StatusOK || body != "ok" {
		t.Fatalf("healthz = %d %q", code, body)
	}
	// 数据库不可达：503，应答里没有驱动的错误原文（该接口不鉴权）。
	p.err = errors.New(`failed to connect to host=db.internal user=job_engine database=reborn: password authentication failed`)
	code, body := do(t, http.MethodGet, ts.URL+"/healthz", "")
	if code != http.StatusServiceUnavailable || strings.TrimSpace(body) != "database unreachable" {
		t.Fatalf("healthz with a dead database = %d %q", code, body)
	}
	code, body = do(t, http.MethodGet, ts.URL+"/metrics", "")
	if code != http.StatusOK {
		t.Fatalf("metrics = %d", code)
	}
	for _, want := range []string{
		`job_loop_runs_total{loop="lease-sweep",`, `job_loop_items_total{loop="lease-sweep",`, `result="error"`,
		`job_runner_api_requests_total{`, `op="claim"`, `result="empty"`, `job_executions_total{event="lease_expired",`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics lack %q", want)
		}
	}
	// 协议接口不在健康检查 / 指标监听上。
	if code, _ := do(t, http.MethodPost, ts.URL+jobcontract.RunnerPathClaim, ""); code != http.StatusNotFound {
		t.Fatalf("runner api on the health listener = %d, want 404", code)
	}
}

func newService(t *testing.T) (*service.RunnerService, *biz.RunnerUsecase, *biz.HousekeepUsecase, string) {
	t.Helper()
	store, obs := biztest.NewStore(), biztest.NewObserver()
	const id = "018f0000-0000-7000-8000-0000000000aa"
	token, hash, err := jobcontract.NewRunnerToken(id)
	if err != nil {
		t.Fatal(err)
	}
	store.AddRunner(biz.Runner{ID: id, Enabled: true, TokenHash: hash})
	uc := biz.NewRunnerUsecase(store, store, store, store, store, &biztest.Opener{}, obs, testLogger())
	return service.NewRunnerService(uc, testLogger()), uc, biz.NewHousekeepUsecase(store, obs, testLogger()), token
}

func TestRunnerAPIServer(t *testing.T) {
	svc, _, _, token := newService(t)
	engine := conf.Roles{conf.RoleEngine: {}}
	srv, err := NewRunnerAPIServer(engine, &conf.Server{Runner: &conf.Server_RunnerAPI{Addr: "127.0.0.1:0"}}, svc, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	if srv.ReadHeaderTimeout <= 0 || srv.ReadTimeout <= 0 || srv.WriteTimeout <= 0 || srv.IdleTimeout <= 0 {
		t.Fatalf("server timeouts not hardened: %+v", srv.Server.Server)
	}
	ts := httptest.NewServer(srv)
	defer ts.Close()
	if code, body := do(t, http.MethodPost, ts.URL+jobcontract.RunnerPathClaim, token); code != http.StatusOK || strings.TrimSpace(body) != "{}" {
		t.Fatalf("claim = %d %q", code, body)
	}
	if code, body := do(t, http.MethodPost, ts.URL+jobcontract.RunnerPathHeartbeat, token); code != http.StatusOK || !strings.Contains(body, `"enabled":true`) {
		t.Fatalf("heartbeat = %d %q", code, body)
	}
	if code, _ := do(t, http.MethodPost, ts.URL+jobcontract.RunnerPathClaim, "bad"); code != http.StatusUnauthorized {
		t.Fatalf("bad token = %d", code)
	}
	// 协议监听上没有健康检查与指标。
	for _, path := range []string{"/healthz", "/metrics"} {
		if code, _ := do(t, http.MethodGet, ts.URL+path, ""); code != http.StatusNotFound {
			t.Fatalf("%s on the runner api listener = %d, want 404", path, code)
		}
	}

	// 证书与私钥必须成对；文件不存在即装配失败。
	if _, err := NewRunnerAPIServer(engine, &conf.Server{Runner: &conf.Server_RunnerAPI{CertFile: "/x/cert.pem"}}, svc, testLogger()); err == nil {
		t.Fatal("cert without key must be rejected")
	}
	if _, err := NewRunnerAPIServer(engine, &conf.Server{Runner: &conf.Server_RunnerAPI{CertFile: "/x/cert.pem", KeyFile: "/x/key.pem"}}, svc, testLogger()); err == nil {
		t.Fatal("missing certificate files must be rejected")
	}
	// 未启用 engine 角色：不构造监听（也不读证书）。
	none, err := NewRunnerAPIServer(conf.Roles{conf.RoleRunner: {}}, &conf.Server{Runner: &conf.Server_RunnerAPI{CertFile: "/x/cert.pem"}}, svc, testLogger())
	if err != nil || none != nil {
		t.Fatalf("runner-only role: %v %v", none, err)
	}
}

func TestNewServersAndRunnerByRole(t *testing.T) {
	svc, uc, house, token := newService(t)
	engine, runnerOnly := conf.Roles{conf.RoleEngine: {}}, conf.Roles{conf.RoleRunner: {}}
	both := conf.Roles{conf.RoleEngine: {}, conf.RoleRunner: {}}
	hs := NewHTTPServer(&conf.Server{}, testLogger(), pinger{})
	api, err := NewRunnerAPIServer(engine, &conf.Server{}, svc, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	set := worker.NewSet(&conf.Engine{}, uc, house, nil, nil, testLogger())
	if len(set.Engine) != 2 {
		t.Fatalf("engine loops = %d", len(set.Engine))
	}

	// 纯引擎：不需要任何 runner 配置。
	rn, err := NewRunner(engine, &conf.Runner{}, "v", testLogger())
	if err != nil || rn != nil {
		t.Fatalf("engine-only must not build a runner: %v %v", rn, err)
	}
	if got := NewServers(engine, testLogger(), hs, api, set, nil); len(got) != 4 {
		t.Fatalf("engine servers = %d, want http + runner api + 2 loops", len(got))
	}

	// 纯执行节点：只凭 runner 配置即可装配；配置不全则装配失败，且错误里没有令牌。
	if _, err := NewRunner(runnerOnly, &conf.Runner{Token: token}, "v", testLogger()); err == nil || strings.Contains(err.Error(), token) {
		t.Fatalf("incomplete runner config: %v", err)
	}
	rn, err = NewRunner(runnerOnly, &conf.Runner{EngineUrl: "http://127.0.0.1:8051", Token: token, WorkDir: "/tmp/reborn-job-test"}, "v", testLogger())
	if err != nil || rn == nil {
		t.Fatalf("runner: %v %v", rn, err)
	}
	got := NewServers(runnerOnly, testLogger(), hs, nil, set, rn)
	if len(got) != 2 || got[1] != transport.Server(rn) {
		t.Fatalf("runner servers = %d, want http + runner", len(got))
	}

	// 两种角色同进程：协议监听排在执行节点之后关闭。
	got = NewServers(both, testLogger(), hs, api, set, rn)
	if len(got) != 5 {
		t.Fatalf("both servers = %d", len(got))
	}
	if _, ok := got[1].(*afterRunner); !ok {
		t.Fatalf("runner api must be wrapped to stop after the runner, got %T", got[1])
	}
}
