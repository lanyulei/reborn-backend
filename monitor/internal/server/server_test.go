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

	"github.com/lanyulei/reborn/backend/kit/kitconf"
	"github.com/lanyulei/reborn/backend/kit/loop"
	"github.com/lanyulei/reborn/backend/monitor/internal/conf"
	"github.com/lanyulei/reborn/backend/monitor/internal/worker"
)

func TestNewRoles(t *testing.T) {
	t.Setenv("SERVER_ROLES", "")
	must := func(c *conf.Engine) Roles {
		t.Helper()
		roles, err := NewRoles(c)
		if err != nil {
			t.Fatalf("NewRoles() error = %v", err)
		}
		return roles
	}
	all := must(&conf.Engine{})
	if !all.Has(RolePlanner) || !all.Has(RoleEvaluator) || len(all.List()) != 2 {
		t.Fatalf("empty config must enable all roles: %v", all.List())
	}
	if nilConf := must(nil); len(nilConf.List()) != 2 {
		t.Fatalf("nil config must enable all roles: %v", nilConf.List())
	}
	only := must(&conf.Engine{Roles: []string{" Evaluator "}})
	if only.Has("planner") || !only.Has("evaluator") {
		t.Fatalf("roles = %v", only.List())
	}
	if _, err := NewRoles(&conf.Engine{Roles: []string{"evaluator", "bogus"}}); err == nil {
		t.Fatal("unknown role must fail assembly instead of being silently dropped")
	}
	t.Setenv("SERVER_ROLES", "planner")
	env := must(&conf.Engine{Roles: []string{"evaluator"}})
	if !env.Has("planner") || env.Has("evaluator") {
		t.Fatalf("env must take precedence: %v", env.List())
	}
	t.Setenv("SERVER_ROLES", "evaluator, planner")
	if got := must(nil).List(); !slices.Equal(got, []string{"planner", "evaluator"}) {
		t.Fatalf("list order = %v", got)
	}
}

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

type pinger struct{ err error }

func (p pinger) Ping() error { return p.err }

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(body)
}

func TestHTTPServer_HealthzAndMetrics(t *testing.T) {
	t.Parallel()
	m, cleanup, err := NewMetrics()
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	m.Record(context.Background(), "probe", 3, nil)
	m.Record(context.Background(), "probe", 0, errors.New("boom"))
	m.ObserveEval(context.Background(), "firing", 120*time.Millisecond, -time.Second)
	m.AddOutbox(context.Background(), 1, 2)
	m.AddOutbox(context.Background(), 99, 1)
	m.AddOutbox(context.Background(), 4, 1)
	m.AddTruncated(context.Background(), "ci")
	m.AddInhibitCycles(context.Background(), 2)
	m.ObserveRecord(context.Background(), "ok", 30*time.Millisecond, 12)
	m.ObserveRecord(context.Background(), "write_error", time.Millisecond, 0)
	m.AddCommand(context.Background(), "done")
	m.AddEngineEvents(context.Background(), "monitor.engine.datasource_down", 1)

	ts := httptest.NewServer(NewHTTPServer(&conf.Server{Http: &kitconf.HTTP{}}, testLogger(), pinger{}))
	defer ts.Close()
	if code, body := get(t, ts.URL+"/healthz"); code != http.StatusOK || body != "ok" {
		t.Fatalf("healthz = %d %q", code, body)
	}
	code, body := get(t, ts.URL+"/metrics")
	if code != http.StatusOK {
		t.Fatalf("metrics = %d", code)
	}
	for _, want := range []string{
		"monitor_loop_runs_total", "monitor_loop_items_total", `result="error"`,
		`monitor_evaluations_total{`, `result="firing"`, "monitor_evaluation_duration_seconds_bucket", "monitor_evaluation_lag_seconds_bucket",
		"monitor_outbox_rows_total", `kind="firing"`, `kind="unknown"`, `kind="escalation"`,
		`monitor_cache_truncated_total{cache="ci"`, "monitor_inhibit_cycle_alerts_total", `monitor_record_runs_total{`, `result="write_error"`,
		"monitor_record_duration_seconds_bucket", "monitor_record_series_total", `monitor_alert_commands_total{`, `result="done"`,
		`monitor_engine_events_total{`, `type="monitor.engine.datasource_down"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics output missing %q", want)
		}
	}
	if code, _ := get(t, ts.URL+"/api/v1/monitor/rule"); code != http.StatusNotFound {
		t.Fatalf("engine must not expose business routes, got %d", code)
	}

	bad := httptest.NewServer(NewHTTPServer(&conf.Server{Http: &kitconf.HTTP{}}, testLogger(), pinger{err: http.ErrServerClosed}))
	defer bad.Close()
	if code, _ := get(t, bad.URL+"/healthz"); code != http.StatusServiceUnavailable {
		t.Fatalf("unhealthy = %d", code)
	}
}

func TestNewServers_RoleGating(t *testing.T) {
	t.Parallel()
	hs := NewHTTPServer(&conf.Server{}, testLogger(), pinger{})
	noop := func(context.Context) (int, error) { return 0, nil }
	set := &worker.Set{
		Planner:    []*loop.Loop{loop.New("materialize", time.Second, testLogger(), nil, noop), loop.New("probe", time.Second, testLogger(), nil, noop)},
		Evaluators: []*loop.Loop{loop.New("evaluator-0", time.Second, testLogger(), nil, noop)},
		Always:     []*loop.Loop{loop.New("heartbeat", time.Second, testLogger(), nil, noop)},
	}
	// 心跳循环不分角色，恒启动。
	cases := []struct {
		roles Roles
		want  int
	}{
		{Roles{RolePlanner: {}, RoleEvaluator: {}}, 5},
		{Roles{RolePlanner: {}}, 4},
		{Roles{RoleEvaluator: {}}, 3},
	}
	for _, c := range cases {
		if got := len(NewServers(c.roles, testLogger(), hs, set)); got != c.want {
			t.Fatalf("roles %v: servers = %d, want %d", c.roles.List(), got, c.want)
		}
	}
	if got := len(NewServers(Roles{RolePlanner: {}}, testLogger(), hs, &worker.Set{})); got != 1 {
		t.Fatalf("empty set: servers = %d", got)
	}
}

func TestNewInstanceInfo(t *testing.T) {
	t.Parallel()
	a, err := NewInstanceInfo(Roles{RoleEvaluator: {}}, "v1.2.3")
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewInstanceInfo(Roles{RolePlanner: {}, RoleEvaluator: {}}, "")
	if err != nil {
		t.Fatal(err)
	}
	// 每次启动一个新的 ID（任务属主令牌的前缀：重启后旧进程的写回被围栏拒绝）；长度留出循环序号后仍放得进 varchar(64)。
	if a.ID == b.ID || a.ID == "" || len(a.ID) > maxInstanceHost+13 {
		t.Fatalf("ids = %q / %q", a.ID, b.ID)
	}
	if a.Roles != "evaluator" || b.Roles != "planner,evaluator" || a.Version != "v1.2.3" {
		t.Fatalf("info = %+v / %+v", a, b)
	}
}
