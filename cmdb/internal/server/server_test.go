package server

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/lanyulei/reborn/backend/cmdb/internal/conf"
	"github.com/lanyulei/reborn/backend/kit/kitconf"
)

func TestNewRoles(t *testing.T) {
	t.Setenv("SERVER_ROLES", "")
	must := func(c *conf.Sync) Roles {
		t.Helper()
		roles, err := NewRoles(c)
		if err != nil {
			t.Fatalf("NewRoles() error = %v", err)
		}
		return roles
	}
	all := must(&conf.Sync{})
	if !all.Has(RoleScheduler) || !all.Has(RoleWorker) || !all.Has(RoleWebhook) || len(all.List()) != 3 {
		t.Fatalf("empty config must enable all roles: %v", all.List())
	}
	if nilConf := must(nil); len(nilConf.List()) != 3 {
		t.Fatalf("nil config must enable all roles: %v", nilConf.List())
	}
	only := must(&conf.Sync{Roles: []string{" Worker "}})
	if only.Has("scheduler") || !only.Has("worker") {
		t.Fatalf("roles = %v", only.List())
	}
	if _, err := NewRoles(&conf.Sync{Roles: []string{"worker", "bogus"}}); err == nil {
		t.Fatal("unknown role must fail assembly instead of being silently dropped")
	}
	t.Setenv("SERVER_ROLES", "scheduler")
	env := must(&conf.Sync{Roles: []string{"worker"}})
	if !env.Has("scheduler") || env.Has("worker") {
		t.Fatalf("env must take precedence: %v", env.List())
	}
	t.Setenv("SERVER_ROLES", "worker, scheduler")
	if got := must(nil).List(); !slices.Equal(got, []string{"scheduler", "worker"}) {
		t.Fatalf("list order = %v", got)
	}
}

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

type pinger struct{ err error }

func (p pinger) Ping() error { return p.err }

func TestHTTPServer_Healthz(t *testing.T) {
	t.Parallel()
	srv := NewHTTPServer(&conf.Server{Http: &kitconf.HTTP{}}, Roles{}, testLogger(), pinger{}, nil)
	ts := httptest.NewServer(srv)
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz = %d", resp.StatusCode)
	}
	resp, err = http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("metrics = %d", resp.StatusCode)
	}
	bad := NewHTTPServer(&conf.Server{Http: &kitconf.HTTP{}}, Roles{}, testLogger(), pinger{err: http.ErrServerClosed}, nil)
	ts2 := httptest.NewServer(bad)
	defer ts2.Close()
	resp, err = http.Get(ts2.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("unhealthy = %d", resp.StatusCode)
	}
}
