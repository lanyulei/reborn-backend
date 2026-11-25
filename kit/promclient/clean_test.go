package promclient

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// 凭据很短时 clean 不再把错误说明改得没法读（monitor 模块评审 data-contracts P3-1）：Basic 认证的密码可以是
// 任意短字符串，此前密码为 "e" 时 "parse error at char 3" 会被逐处替换成 "pars*** ***rror at char 3"。
func TestClean_ShortSecretKeepsMessagesReadable(t *testing.T) {
	t.Parallel()
	// 短凭据：说明原样保留，只遮蔽请求实际带出的 Authorization 头的值。
	basic := &Client{secret: "e", header: http.Header{"Authorization": {"Basic b3BzOmU="}}}
	const upstream = "parse error at char 3: unexpected identifier"
	if got := basic.clean(upstream); got != upstream {
		t.Fatalf("clean(%q) = %q, want the message intact", upstream, got)
	}
	if got := basic.clean("Authorization: Basic b3BzOmU= rejected"); got != "Authorization: *** rejected" {
		t.Fatalf("the Authorization header value must still be masked: %q", got)
	}
	bearer := &Client{secret: "ab12", header: http.Header{"Authorization": {"Bearer ab12"}}}
	if got := bearer.clean("denied for Authorization: Bearer ab12"); got != "denied for Authorization: ***" {
		t.Fatalf("a short bearer token echoed as a header must be masked: %q", got)
	}
	// 恰好达到全文替换长度的凭据照旧全文替换；更短的不替换。
	edge := &Client{secret: "12345678", header: http.Header{}}
	if got := edge.clean("bad token 12345678"); got != "bad token ***" {
		t.Fatalf("clean = %q, want a secret of the threshold length replaced everywhere", got)
	}
	if got := (&Client{secret: "1234567", header: http.Header{}}).clean("id 1234567 rejected"); got != "id 1234567 rejected" {
		t.Fatalf("clean = %q, a secret below the threshold must not be replaced in prose", got)
	}
	// 长凭据：此前的行为不变（错误与告警里的任何位置都不出现凭据）。
	long := &Client{secret: "s3cr3t-token-value", header: http.Header{}}
	if got := long.clean("bad token s3cr3t-token-value"); got != "bad token ***" {
		t.Fatalf("clean = %q", got)
	}
	// 没有凭据：原样。
	if got := (&Client{}).clean(upstream); got != upstream {
		t.Fatalf("clean without credentials = %q", got)
	}
}

// 端到端：密码只有一个字符的数据源，上游的语法错误说明原样进入错误信息。
func TestQuery_ShortPasswordKeepsTheUpstreamErrorReadable(t *testing.T) {
	t.Parallel()
	srv := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = io.WriteString(w, `{"status":"error","errorType":"bad_data","error":"parse error at char 3: unexpected identifier"}`)
	})
	c := newTestClient(t, srv.URL, func(cfg *Config) { cfg.AuthType, cfg.Username, cfg.Secret = AuthBasic, "ops", "e" })
	_, err := c.Query(context.Background(), "1 +", time.Time{})
	if err == nil || !strings.Contains(err.Error(), "parse error at char 3: unexpected identifier") || strings.Contains(err.Error(), "***") {
		t.Fatalf("err = %v, want the upstream message intact", err)
	}
}
