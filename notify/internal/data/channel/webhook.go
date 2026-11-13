package channel

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/kit/notifycontract"
	"github.com/lanyulei/reborn/backend/notify/internal/biz"
	"github.com/lanyulei/reborn/backend/notify/pkg/webhooksig"
)

// Webhook 出站契约头（定义在 pkg/webhooksig，供接收端 import 校验）。
const (
	HeaderSendKey   = webhooksig.HeaderSendKey   // 对外幂等键（接收端去重；同值亦放 Idempotency-Key）
	HeaderTimestamp = webhooksig.HeaderTimestamp // Unix 秒
	HeaderSignature = webhooksig.HeaderSignature // sha256=hex(HMAC-SHA256(secret, ts + "." + body))，有 secret 时携带
	HeaderEventID   = webhooksig.HeaderEventID
)

// errWebhookHostBlocked 出站目标解析到回环/私网/链路本地等地址（SSRF 防护）：确定性失败。
var errWebhookHostBlocked = errors.New("webhook host resolves to a private or local address")

// webhookPayload 出站 JSON 体。
type webhookPayload struct {
	EventID    string          `json:"event_id"`
	BizID      string          `json:"biz_id"`
	EventType  string          `json:"event_type"`
	Severity   int32           `json:"severity"`
	Title      string          `json:"title"`
	Subject    string          `json:"subject"`
	Body       string          `json:"body"`
	Format     string          `json:"format,omitempty"` // 模板输出格式（text/markdown/html/card）
	Payload    json.RawMessage `json:"payload,omitempty"`
	SendKey    string          `json:"send_key"`
	Aggregate  *biz.AggContext `json:"aggregate,omitempty"`
	OccurredAt time.Time       `json:"occurred_at"`
}

// WebhookSender 通用 Webhook 适配器：仅 HTTPS（设计 §9；测试可经 WithInsecure
// 放行 http 回环地址）；连接前对**解析后的真实地址**做私网/回环校验（免疫 DNS
// 重绑定），命中即确定性失败；5xx/408/429/超时/网络错误可重试，其余 4xx 不可重试；
// 429/503 的 Retry-After 透传给退避。
type WebhookSender struct {
	client   *http.Client
	dialer   *net.Dialer
	resolver interface {
		LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error)
	}
	allowInsecure bool
}

// NewWebhookSender new a WebhookSender（连接复用、禁跳转、安全拨号）。
func NewWebhookSender() *WebhookSender {
	s := &WebhookSender{dialer: &net.Dialer{Timeout: 3 * time.Second}, resolver: net.DefaultResolver}
	s.client = &http.Client{
		Transport: &http.Transport{
			MaxIdleConns: 64, MaxIdleConnsPerHost: 8, IdleConnTimeout: 90 * time.Second,
			DialContext: s.dialContext,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	return s
}

// dialContext 先解析再逐地址校验拨号：任一地址为私网/回环即整体拒绝（不能"挑一个
// 公网地址连"——对端可返回混合记录诱导）。allowInsecure 仅测试脚手架放行。
func (s *WebhookSender) dialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	if s.allowInsecure {
		return s.dialer.DialContext(ctx, network, addr)
	}
	ips, err := s.resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("webhook host %q resolves to no address", host)
	}
	for _, ip := range ips {
		if notifycontract.PrivateIP(ip.IP) {
			return nil, fmt.Errorf("%w: %s → %s", errWebhookHostBlocked, host, ip.IP)
		}
	}
	var last error
	for _, ip := range ips {
		conn, err := s.dialer.DialContext(ctx, network, net.JoinHostPort(ip.IP.String(), port))
		if err == nil {
			return conn, nil
		}
		last = err
	}
	return nil, last
}

// WithInsecure 允许 http://（仅测试脚手架使用）。
func (s *WebhookSender) WithInsecure() *WebhookSender {
	s.allowInsecure = true
	return s
}

// Supports implements biz.ChannelSender.
func (s *WebhookSender) Supports(channelType string) bool {
	return channelType == biz.ChannelTypeWebhook
}

// Send implements biz.ChannelSender.
func (s *WebhookSender) Send(ctx context.Context, ch *biz.Channel, msg biz.RenderedMessage) (biz.SendResult, error) {
	cfg := ch.Webhook
	if cfg == nil || cfg.URL == "" {
		return biz.SendResult{Retryable: false}, errors.New("webhook config missing url")
	}
	if !strings.HasPrefix(cfg.URL, "https://") && !s.allowInsecure {
		return biz.SendResult{Retryable: false}, errors.New("webhook url must be https")
	}
	if u, err := url.Parse(cfg.URL); err != nil || (!s.allowInsecure && notifycontract.UnsafeWebhookHost(u.Hostname())) {
		host := "unparseable url" // 只报主机：路径/查询串可能带密钥
		if err == nil {
			host = u.Host
		}
		return biz.SendResult{Retryable: false}, fmt.Errorf("%w: %s", errWebhookHostBlocked, host)
	}
	body, err := json.Marshal(webhookPayload{
		EventID: msg.EventID, BizID: msg.BizID, EventType: msg.EventType, Severity: msg.Severity, Title: msg.Title,
		Subject: msg.Subject, Body: msg.Body, Format: msg.Format, Payload: payloadRaw(msg.Payload), SendKey: msg.SendKey, Aggregate: msg.Agg,
		OccurredAt: time.Now().UTC(),
	})
	if err != nil {
		return biz.SendResult{Retryable: false}, fmt.Errorf("encode webhook body: %w", err)
	}
	method := strings.ToUpper(cfg.Method)
	if method == "" {
		method = http.MethodPost
	}
	// 超时由 sender 按渠道 timeout_s（受 max_send_timeout 上限）设置在 ctx 上；此处不再
	// 叠加——叠加等于 min(两者)，渠道值会被固定缺省吞掉。
	req, err := http.NewRequestWithContext(ctx, method, cfg.URL, bytes.NewReader(body))
	if err != nil {
		return biz.SendResult{Retryable: false}, fmt.Errorf("build webhook request: %w", scrubURLError(err))
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "reborn-notify/1.0")
	for k, v := range cfg.Headers {
		req.Header.Set(k, v)
	}
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	req.Header.Set(HeaderSendKey, msg.SendKey)
	req.Header.Set("Idempotency-Key", msg.SendKey)
	req.Header.Set(HeaderEventID, msg.EventID)
	req.Header.Set(HeaderTimestamp, ts)
	if ch.Secret != "" {
		req.Header.Set(HeaderSignature, Signature(ch.Secret, ts, body))
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return biz.SendResult{Retryable: !errors.Is(err, errWebhookHostBlocked)}, fmt.Errorf("webhook request: %w", scrubURLError(err))
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return biz.SendResult{ProviderMsgID: resp.Header.Get("X-Request-Id")}, nil
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode >= 500:
		return biz.SendResult{Retryable: true, RetryAfter: retryAfter(resp.Header)}, fmt.Errorf("webhook responded %d", resp.StatusCode)
	default:
		return biz.SendResult{Retryable: false}, fmt.Errorf("webhook responded %d", resp.StatusCode)
	}
}

// Signature 计算出站签名头值（pkg/webhooksig.Sign）。
func Signature(secret, ts string, body []byte) string { return webhooksig.Sign(secret, ts, body) }

// VerifySignature 接收端校验助手（pkg/webhooksig.Verify；下游请直接 import 该包）。
func VerifySignature(secret, ts string, body []byte, header string) bool {
	return webhooksig.Verify(secret, ts, body, header)
}

func payloadRaw(p []byte) json.RawMessage {
	if len(p) == 0 || !json.Valid(p) {
		return nil
	}
	return json.RawMessage(p)
}
