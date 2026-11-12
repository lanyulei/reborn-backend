package channel

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/notify/internal/biz"
)

func TestWebhookSender_SendSigned(t *testing.T) {
	t.Parallel()
	var got struct {
		body    []byte
		headers http.Header
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.body, _ = io.ReadAll(r.Body)
		got.headers = r.Header.Clone()
		w.Header().Set("X-Request-Id", "req-1")
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	ch := &biz.Channel{
		ID: "c", Type: biz.ChannelTypeWebhook, Secret: "s3cret",
		Webhook: &biz.WebhookConfig{URL: srv.URL + "/hook", Method: "post", TimeoutS: 2, Headers: map[string]string{"X-Source": "core"}},
	}
	msg := biz.RenderedMessage{
		EventID: "e1", BizID: "default", EventType: "a.b", Severity: 2, Title: "T", Subject: "S", Body: "B",
		Payload: []byte(`{"k":1}`), SendKey: "key-1", Agg: &biz.AggContext{Count: 2},
	}
	res, err := NewWebhookSender().WithInsecure().Send(context.Background(), ch, msg)
	if err != nil || res.ProviderMsgID != "req-1" {
		t.Fatalf("Send() = %+v, %v", res, err)
	}
	if got.headers.Get(HeaderSendKey) != "key-1" || got.headers.Get(HeaderEventID) != "e1" || got.headers.Get("X-Source") != "core" ||
		got.headers.Get("Content-Type") != "application/json" {
		t.Fatalf("headers = %v", got.headers)
	}
	if !VerifySignature("s3cret", got.headers.Get(HeaderTimestamp), got.body, got.headers.Get(HeaderSignature)) {
		t.Fatal("signature does not verify")
	}
	var payload map[string]any
	if err := json.Unmarshal(got.body, &payload); err != nil || payload["send_key"] != "key-1" || payload["subject"] != "S" ||
		payload["payload"].(map[string]any)["k"] != float64(1) || payload["aggregate"].(map[string]any)["count"] != float64(2) {
		t.Fatalf("body = %s (%v)", got.body, err)
	}
}

func TestWebhookSender_Outcomes(t *testing.T) {
	t.Parallel()
	var status atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(int(status.Load()))
	}))
	defer srv.Close()
	s := NewWebhookSender().WithInsecure()
	ch := &biz.Channel{Type: biz.ChannelTypeWebhook, Webhook: &biz.WebhookConfig{URL: srv.URL, TimeoutS: 1}}
	cases := []struct {
		code      int
		retryable bool
	}{{500, true}, {503, true}, {429, true}, {408, true}, {400, false}, {404, false}, {401, false}}
	for _, c := range cases {
		status.Store(int32(c.code)) //nolint:gosec // HTTP 状态码 <1000
		res, err := s.Send(context.Background(), ch, biz.RenderedMessage{SendKey: "k"})
		if err == nil || res.Retryable != c.retryable {
			t.Errorf("status %d: retryable=%v err=%v, want retryable=%v", c.code, res.Retryable, err, c.retryable)
		}
	}
	// 连接拒绝：可重试；http 非安全 URL 默认拒绝（不可重试）；无签名密钥时不带签名头。
	res, err := s.Send(context.Background(), &biz.Channel{Type: biz.ChannelTypeWebhook, Webhook: &biz.WebhookConfig{URL: "http://127.0.0.1:1", TimeoutS: 1}}, biz.RenderedMessage{})
	if err == nil || !res.Retryable {
		t.Fatalf("connection refused: %+v %v", res, err)
	}
	res, err = NewWebhookSender().Send(context.Background(), &biz.Channel{Type: biz.ChannelTypeWebhook, Webhook: &biz.WebhookConfig{URL: srv.URL}}, biz.RenderedMessage{})
	if err == nil || res.Retryable || !strings.Contains(err.Error(), "https") {
		t.Fatalf("plain http must be rejected: %+v %v", res, err)
	}
	// 超时：可重试。
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { time.Sleep(300 * time.Millisecond); w.WriteHeader(200) }))
	defer slow.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	res, err = s.Send(ctx, &biz.Channel{Type: biz.ChannelTypeWebhook, Webhook: &biz.WebhookConfig{URL: slow.URL}}, biz.RenderedMessage{})
	if err == nil || !res.Retryable {
		t.Fatalf("timeout: %+v %v", res, err)
	}
}

// TestWebhookSender_SSRFBlockedAndRetryAfter 未放行 insecure 时，回环/私网目标在拨号前
// 被拒（确定性失败）；429 的 Retry-After 透传；幂等键同时放 Idempotency-Key。
func TestWebhookSender_SSRFBlockedAndRetryAfter(t *testing.T) {
	t.Parallel()
	s := NewWebhookSender()
	for _, u := range []string{"https://127.0.0.1:1/h", "https://localhost/h", "https://10.1.2.3/h"} {
		res, err := s.Send(context.Background(), &biz.Channel{Type: biz.ChannelTypeWebhook, Webhook: &biz.WebhookConfig{URL: u}}, biz.RenderedMessage{})
		if err == nil || res.Retryable || !strings.Contains(err.Error(), "private or local") {
			t.Fatalf("%s: must be blocked non-retryably, got %+v %v", u, res, err)
		}
	}
	// 解析后命中私网同样拒绝（DNS 重绑定场景）：用自定义解析器把公网域名指到回环。
	s2 := NewWebhookSender()
	s2.resolver = fakeResolver{"hooks.example.com": {net.ParseIP("127.0.0.1")}}
	res, err := s2.Send(context.Background(), &biz.Channel{Type: biz.ChannelTypeWebhook, Webhook: &biz.WebhookConfig{URL: "https://hooks.example.com/h"}}, biz.RenderedMessage{})
	if err == nil || res.Retryable || !errors.Is(err, errWebhookHostBlocked) {
		t.Fatalf("resolved private address must be blocked: %+v %v", res, err)
	}
	var idem string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		idem = r.Header.Get("Idempotency-Key")
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()
	res, err = NewWebhookSender().WithInsecure().Send(context.Background(), &biz.Channel{Type: biz.ChannelTypeWebhook, Webhook: &biz.WebhookConfig{URL: srv.URL}}, biz.RenderedMessage{SendKey: "k9"})
	if err == nil || !res.Retryable || res.RetryAfter != 30*time.Second || idem != "k9" {
		t.Fatalf("429: %+v %v idem=%q", res, err, idem)
	}
}

type fakeResolver map[string][]net.IP

func (f fakeResolver) LookupIPAddr(_ context.Context, host string) ([]net.IPAddr, error) {
	var out []net.IPAddr
	for _, ip := range f[host] {
		out = append(out, net.IPAddr{IP: ip})
	}
	return out, nil
}
