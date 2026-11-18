package service

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

type denyTokens struct{ calls int }

func (d *denyTokens) Exchange(context.Context, string, string) (biz.AgentGrant, error) {
	d.calls++
	return biz.AgentGrant{}, biz.ErrAgentToken
}

func (d *denyTokens) Verify(context.Context, string, string) (biz.AgentGrant, error) {
	d.calls++
	return biz.AgentGrant{}, biz.ErrAgentToken
}

func (d *denyTokens) Alive(context.Context, []string) (map[string]struct{}, error) { return nil, nil }

type noTunnels struct{}

func (noTunnels) Serve(context.Context, string, string, string, net.Conn) error { return nil }

func do(t *testing.T, h http.Handler, method, path, auth, body string) (int, string) {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	out, _ := io.ReadAll(rec.Body)
	return rec.Code, string(out)
}

// 格式不对的令牌、不存在的令牌：应答完全相同；入口只有两个路径。
func TestAgentEntryUniformRejection(t *testing.T) {
	t.Parallel()
	tokens := &denyTokens{}
	h := NewAgentService(tokens, noTunnels{}, slog.New(slog.DiscardHandler)).Handler()
	wellFormed := "018f3e1a-0cd1-7000-8000-000000000001." + strings.Repeat("a", 43)

	c1, b1 := do(t, h, http.MethodGet, k8scontract.AgentConnectPath, "Bearer garbage", "")
	c2, b2 := do(t, h, http.MethodGet, k8scontract.AgentConnectPath, "Bearer "+wellFormed, "")
	c3, b3 := do(t, h, http.MethodGet, k8scontract.AgentConnectPath, "", "")
	if c1 != 401 || c2 != 401 || c3 != 401 || b1 != b2 || b2 != b3 {
		t.Fatalf("connect rejections differ: %d %q / %d %q / %d %q", c1, b1, c2, b2, c3, b3)
	}
	r1, rb1 := do(t, h, http.MethodPost, k8scontract.AgentRegisterPath, "", `{"token":"garbage"}`)
	r2, rb2 := do(t, h, http.MethodPost, k8scontract.AgentRegisterPath, "", `{"token":"`+wellFormed+`"}`)
	if r1 != 401 || r2 != 401 || rb1 != rb2 {
		t.Fatalf("register rejections differ: %d %q / %d %q", r1, rb1, r2, rb2)
	}
	if tokens.calls != 2 {
		t.Fatalf("malformed tokens must not reach the database: calls = %d", tokens.calls)
	}
	if c, _ := do(t, h, http.MethodPost, k8scontract.AgentRegisterPath, "", strings.Repeat("x", agentRegisterBodyLimit+10)); c != 400 {
		t.Fatalf("oversized register body: %d", c)
	}
	for _, p := range []string{"/", "/internal/v1/op/list", "/metrics", "/healthz", "/agent/v1/other"} {
		if c, _ := do(t, h, http.MethodGet, p, "", ""); c != 404 {
			t.Errorf("%s: status %d, want 404", p, c)
		}
	}
}

func TestIPLimiter(t *testing.T) {
	t.Parallel()
	l := newIPLimiter(3, time.Minute)
	now := time.Unix(1_800_000_000, 0)
	for i := range 3 {
		if !l.allow("1.1.1.1", now, 0) {
			t.Fatalf("request %d must pass", i)
		}
	}
	if l.allow("1.1.1.1", now, 0) {
		t.Fatal("4th request in the window must be limited")
	}
	if !l.allow("2.2.2.2", now, 0) {
		t.Fatal("other IPs are not affected")
	}
	if !l.allow("1.1.1.1", now.Add(time.Minute), 0) {
		t.Fatal("limit resets with the window")
	}
	// 通过认证的请求归还限额。
	l2 := newIPLimiter(1, time.Minute)
	if !l2.allow("a", now, 0) {
		t.Fatal("first request must pass")
	}
	l2.refund("a")
	if !l2.allow("a", now, 0) {
		t.Fatal("a refunded request must not count against the limit")
	}
	// 在途请求数上限。
	l3 := newIPLimiter(100, time.Minute)
	for i := range 2 {
		if !l3.allow("a", now, 2) {
			t.Fatalf("concurrent request %d must pass", i)
		}
	}
	if l3.allow("a", now, 2) {
		t.Fatal("third concurrent request must be refused")
	}
	l3.done("a")
	if !l3.allow("a", now, 2) {
		t.Fatal("a slot freed by done must be reusable")
	}
	// 键数到上限：淘汰一部分，不整体清空——已经被限住的来源不能因此解封。
	l4 := newIPLimiter(1, time.Minute)
	l4.maxKeys = 10
	l4.allow("victim", now, 1) // 在途：不会被淘汰
	for i := range 50 {
		l4.allow(string(rune('A'+i)), now, 0)
	}
	if len(l4.counts) > 11 {
		t.Fatalf("limiter table grew to %d keys", len(l4.counts))
	}
	if l4.allow("victim", now, 0) {
		t.Fatal("a limited source must stay limited when the table is trimmed")
	}
}

func TestLimitKeyAndClientIP(t *testing.T) {
	t.Parallel()
	if limitKey("2001:db8:1:2:aaaa::1") != limitKey("2001:db8:1:2:bbbb::2") {
		t.Fatal("addresses in one IPv6 /64 must share a key")
	}
	if limitKey("2001:db8:1:2::1") == limitKey("2001:db8:1:3::1") {
		t.Fatal("different /64s must not share a key")
	}
	if limitKey("10.0.0.1") == limitKey("10.0.0.2") {
		t.Fatal("IPv4 addresses are keyed individually")
	}
	s := NewAgentService(&denyTokens{}, noTunnels{}, slog.New(slog.DiscardHandler))
	req := httptest.NewRequest(http.MethodGet, k8scontract.AgentConnectPath, nil)
	req.RemoteAddr = "10.1.1.1:5555"
	req.Header.Set("X-Forwarded-For", "6.6.6.6, 7.7.7.7")
	if got := s.clientIP(req); got != "10.1.1.1" {
		t.Fatalf("without trusted proxies X-Forwarded-For must be ignored, got %s", got)
	}
	if err := s.TrustProxies([]string{"10.0.0.0/8"}); err != nil {
		t.Fatal(err)
	}
	if got := s.clientIP(req); got != "7.7.7.7" {
		t.Fatalf("behind a trusted proxy the last hop is the client, got %s", got)
	}
	req.RemoteAddr = "8.8.8.8:1"
	if got := s.clientIP(req); got != "8.8.8.8" {
		t.Fatalf("an untrusted peer cannot supply its own address, got %s", got)
	}
	if err := s.TrustProxies([]string{"not-a-cidr"}); err == nil {
		t.Fatal("invalid CIDR must be rejected")
	}
}

// 令牌 ID 不是 UUID：与其他无效令牌应答相同，且不查库。集群被停用：403，与令牌无效区分。
func TestAgentEntryTokenShapes(t *testing.T) {
	t.Parallel()
	tokens := &denyTokens{}
	h := NewAgentService(tokens, noTunnels{}, slog.New(slog.DiscardHandler)).Handler()
	notUUID := strings.Repeat("z", 36) + "." + strings.Repeat("a", 43)
	upper := "018F3E1A-0CD1-7000-8000-000000000001." + strings.Repeat("a", 43)
	for _, tok := range []string{notUUID, upper} {
		if c, _ := do(t, h, http.MethodGet, k8scontract.AgentConnectPath, "Bearer "+tok, ""); c != 401 {
			t.Fatalf("token %q: status %d, want 401", tok[:8], c)
		}
	}
	if tokens.calls != 0 {
		t.Fatalf("malformed token ids must not reach the database: calls = %d", tokens.calls)
	}
	disabled := NewAgentService(&disabledTokens{}, noTunnels{}, slog.New(slog.DiscardHandler)).Handler()
	wellFormed := "018f3e1a-0cd1-7000-8000-000000000001." + strings.Repeat("a", 43)
	if c, _ := do(t, disabled, http.MethodGet, k8scontract.AgentConnectPath, "Bearer "+wellFormed, ""); c != http.StatusForbidden {
		t.Fatalf("disabled cluster: status %d, want 403", c)
	}
}

type disabledTokens struct{ denyTokens }

func (*disabledTokens) Verify(context.Context, string, string) (biz.AgentGrant, error) {
	return biz.AgentGrant{}, biz.ErrAgentClusterDisabled
}

func TestAgentEntryRateLimited(t *testing.T) {
	t.Parallel()
	h := NewAgentService(&denyTokens{}, noTunnels{}, slog.New(slog.DiscardHandler)).Handler()
	last := 0
	for range agentRateLimit + 1 {
		last, _ = do(t, h, http.MethodGet, k8scontract.AgentConnectPath, "Bearer x", "")
	}
	if last != http.StatusTooManyRequests {
		t.Fatalf("status after exceeding the per-IP limit = %d", last)
	}
}
