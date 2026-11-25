package service

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"unicode/utf8"

	bizsystem "github.com/lanyulei/reborn/backend/core/internal/biz/system"

	jwtx "github.com/go-kratos/kratos/contrib/middleware/jwt/v3"
	"github.com/go-kratos/kratos/v3/transport"
	"github.com/golang-jwt/jwt/v5"
)

// fakeHeader 以 net/http.Header 承载请求头，补上 transport.Header 需要而 http.Header 没有的 Keys。
type fakeHeader http.Header

func (h fakeHeader) Get(key string) string      { return http.Header(h).Get(key) }
func (h fakeHeader) Set(key, value string)      { http.Header(h).Set(key, value) }
func (h fakeHeader) Add(key, value string)      { http.Header(h).Add(key, value) }
func (h fakeHeader) Values(key string) []string { return http.Header(h).Values(key) }
func (h fakeHeader) Keys() []string {
	out := make([]string, 0, len(h))
	for key := range h {
		out = append(out, key)
	}
	return out
}

// fakeTransport 只带请求头的传输层上下文（actorFromContext 只读 X-Request-Id）。
type fakeTransport struct {
	transport.Transporter
	header fakeHeader
}

func (t fakeTransport) RequestHeader() transport.Header { return t.header }

func withRequestID(ctx context.Context, requestID string) context.Context {
	header := fakeHeader{}
	if requestID != "" {
		header.Set(requestIDHeader, requestID)
	}
	return transport.NewServerContext(ctx, fakeTransport{header: header})
}

// 请求 ID 由客户端 / 网关任意给出：超长（列宽 varchar(64)）或含非法 UTF-8 的值此前原样落库，
// 让每个写接口在写变更历史时整个事务回滚成 500。
func TestRequestID_FitsColumnWidth(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, in, want string
	}{
		{"empty", "", ""},
		{"ascii at limit", strings.Repeat("a", 64), strings.Repeat("a", 64)},
		{"ascii over limit", strings.Repeat("a", 100), strings.Repeat("a", 64)},
		{"invalid utf8 only", "\xff\xfe", ""},
		{"invalid utf8 kept around", "a\xffb\xfe", "ab"},
		// 64 字节会切在「中」的中间：回退到 21 个完整字符（63 字节）。
		{"multibyte cut on rune boundary", strings.Repeat("中", 40), strings.Repeat("中", 21)},
		{"multibyte within limit", strings.Repeat("中", 21) + "x", strings.Repeat("中", 21) + "x"},
	}
	for _, c := range cases {
		got := requestID(c.in)
		if got != c.want {
			t.Errorf("%s: requestID() = %q (%d bytes), want %q", c.name, got, len(got), c.want)
		}
		if len(got) > maxRequestIDBytes || !utf8.ValidString(got) {
			t.Errorf("%s: result must be valid UTF-8 within %d bytes, got %q", c.name, maxRequestIDBytes, got)
		}
	}
}

func TestActorFromContext_RequestID(t *testing.T) {
	t.Parallel()
	claims := jwtx.NewContext(context.Background(), &bizsystem.AccessClaims{RegisteredClaims: jwt.RegisteredClaims{Subject: "u1"}})
	long := strings.Repeat("r", 80)
	actor, err := actorFromContext(withRequestID(claims, long))
	if err != nil {
		t.Fatalf("actorFromContext() error = %v", err)
	}
	if actor.RequestID != strings.Repeat("r", 64) {
		t.Fatalf("request id must be truncated to the column width, got %q (%d)", actor.RequestID, len(actor.RequestID))
	}
	// 没有传输层上下文（后台调用）：请求 ID 为空，不算错误。
	actor, err = actorFromContext(claims)
	if err != nil || actor.RequestID != "" {
		t.Fatalf("without transport: actor = %+v err = %v", actor, err)
	}
	// 没有鉴权身份：沿用既有拒绝。
	if _, err := actorFromContext(withRequestID(context.Background(), "req-1")); err == nil {
		t.Fatal("missing identity must be rejected")
	}
}
