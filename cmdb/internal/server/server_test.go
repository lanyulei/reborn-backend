package server

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lanyulei/reborn/backend/cmdb/internal/conf"
)

func TestNewRoles(t *testing.T) {
	t.Setenv("SERVER_ROLES", "")
	all := NewRoles(&conf.Sync{})
	if !all.Has(RoleScheduler) || !all.Has(RoleWorker) || !all.Has(RoleWebhook) || len(all.List()) != 3 {
		t.Fatalf("empty config must enable all roles: %v", all.List())
	}
	only := NewRoles(&conf.Sync{Roles: []string{" Worker ", "bogus"}})
	if only.Has(RoleScheduler) || !only.Has(RoleWorker) {
		t.Fatalf("roles = %v", only.List())
	}
	t.Setenv("SERVER_ROLES", "scheduler")
	env := NewRoles(&conf.Sync{Roles: []string{"worker"}})
	if !env.Has(RoleScheduler) || env.Has(RoleWorker) {
		t.Fatalf("env must take precedence: %v", env.List())
	}
}

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

type pinger struct{ err error }

func (p pinger) Ping() error { return p.err }

func TestHTTPServer_Healthz(t *testing.T) {
	t.Parallel()
	srv := NewHTTPServer(&conf.Server{Http: &conf.Server_HTTP{}}, Roles{}, testLogger(), pinger{}, nil)
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
	bad := NewHTTPServer(&conf.Server{Http: &conf.Server_HTTP{}}, Roles{}, testLogger(), pinger{err: http.ErrServerClosed}, nil)
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
