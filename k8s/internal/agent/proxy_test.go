package agent

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/client-go/rest"
)

type seen struct {
	auth, host, path, impUser string
	impGroups                 []string
	cookie                    string
}

// newTarget 假的 API Server：记录收到的请求；/redirect 跳转到 evil。
func newTarget(t *testing.T, evilURL string) (*httptest.Server, *atomic.Pointer[seen]) {
	t.Helper()
	var last atomic.Pointer[seen]
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		last.Store(&seen{
			auth: r.Header.Get("Authorization"), host: r.Host, path: r.URL.RequestURI(),
			impUser: r.Header.Get("Impersonate-User"), impGroups: r.Header.Values("Impersonate-Group"), cookie: r.Header.Get("Cookie"),
		})
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, evilURL+"/stolen", http.StatusFound)
			return
		}
		_, _ = io.WriteString(w, "target")
	}))
	t.Cleanup(srv.Close)
	return srv, &last
}

// rawRequest 在一条连接上发原始 HTTP 请求（可以构造绝对 URI、CONNECT、任意 Host）。
func rawRequest(t *testing.T, h http.Handler, raw string) *http.Response {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if _, err := io.WriteString(c, raw); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestProxyOnlyReachesConfiguredAPIServer(t *testing.T) {
	t.Parallel()
	var evilHits atomic.Int32
	evil := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { evilHits.Add(1) }))
	defer evil.Close()
	target, last := newTarget(t, evil.URL)
	h, err := NewProxy(&rest.Config{Host: target.URL, BearerToken: "agent-own-token"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	evilHost := strings.TrimPrefix(evil.URL, "http://")
	targetHost := strings.TrimPrefix(target.URL, "http://")

	cases := map[string]string{
		"absolute URI":   fmt.Sprintf("GET %s/api/v1/pods?x=1 HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer stolen\r\nCookie: a=b\r\nImpersonate-User: reborn:u1\r\nImpersonate-Group: reborn:role:r1\r\nImpersonate-Group: system:authenticated\r\n\r\n", evil.URL, evilHost),
		"host header":    fmt.Sprintf("GET /api/v1/pods?x=1 HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer stolen\r\nImpersonate-User: reborn:u1\r\nImpersonate-Group: reborn:role:r1\r\nImpersonate-Group: system:authenticated\r\n\r\n", evilHost),
		"forwarded host": fmt.Sprintf("GET /api/v1/pods?x=1 HTTP/1.1\r\nHost: x\r\nX-Forwarded-Host: %s\r\nAuthorization: Basic c3RvbGVu\r\nImpersonate-User: reborn:u1\r\nImpersonate-Group: reborn:role:r1\r\nImpersonate-Group: system:authenticated\r\n\r\n", evilHost),
	}
	for name, raw := range cases {
		resp := rawRequest(t, h, raw)
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		got := last.Load()
		if resp.StatusCode != 200 || string(body) != "target" {
			t.Fatalf("%s: status %d body %q", name, resp.StatusCode, body)
		}
		if got.host != targetHost || got.path != "/api/v1/pods?x=1" {
			t.Errorf("%s: reached host %q path %q", name, got.host, got.path)
		}
		if got.auth != "Bearer agent-own-token" {
			t.Errorf("%s: inbound Authorization leaked or own credential missing: %q", name, got.auth)
		}
		if got.cookie != "" {
			t.Errorf("%s: cookie forwarded", name)
		}
		if got.impUser != "reborn:u1" || len(got.impGroups) != 2 {
			t.Errorf("%s: impersonation headers not preserved: %q %v", name, got.impUser, got.impGroups)
		}
	}

	// CONNECT 一律拒绝。
	resp := rawRequest(t, h, fmt.Sprintf("CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", evilHost, evilHost))
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("CONNECT: status %d", resp.StatusCode)
	}
	// 跳转不跟随：原样返回 302。
	resp = rawRequest(t, h, "GET /redirect HTTP/1.1\r\nHost: x\r\n\r\n")
	if resp.StatusCode != http.StatusFound {
		t.Errorf("redirect: status %d, want 302 passed through", resp.StatusCode)
	}
	if n := evilHits.Load(); n != 0 {
		t.Fatalf("evil host was reached %d times", n)
	}
}

func TestProxyRejectsBadTarget(t *testing.T) {
	t.Parallel()
	for _, host := range []string{"", "ftp://x", "://"} {
		if _, err := NewProxy(&rest.Config{Host: host}, slog.New(slog.DiscardHandler)); err == nil {
			t.Errorf("NewProxy(%q) must fail", host)
		}
	}
}

func TestProxyDropsConfiguredImpersonation(t *testing.T) {
	t.Parallel()
	target, last := newTarget(t, "http://127.0.0.1:1")
	// kubeconfig 里若配置了 as / as-groups，不得覆盖隧道带来的身份。
	h, err := NewProxy(&rest.Config{Host: target.URL, BearerToken: "t", Impersonate: rest.ImpersonationConfig{UserName: "system:admin"}}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	resp := rawRequest(t, h, "GET /version HTTP/1.1\r\nHost: x\r\nImpersonate-User: reborn:u2\r\n\r\n")
	_ = resp.Body.Close()
	if got := last.Load(); got.impUser != "reborn:u2" {
		t.Fatalf("impersonate user = %q", got.impUser)
	}
}
