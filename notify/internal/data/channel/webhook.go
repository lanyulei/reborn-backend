package channel

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/notify/internal/biz"
)

// Webhook 出站契约头。
const (
	HeaderSendKey   = "X-Notify-Send-Key"  // 对外幂等键（接收端去重）
	HeaderTimestamp = "X-Notify-Timestamp" // Unix 秒
	HeaderSignature = "X-Notify-Signature" // sha256=hex(HMAC-SHA256(secret, ts + "." + body))，有 secret 时携带
	HeaderEventID   = "X-Notify-Event-Id"
)

// webhookPayload 出站 JSON 体。
type webhookPayload struct {
	EventID    string          `json:"event_id"`
	BizID      string          `json:"biz_id"`
	EventType  string          `json:"event_type"`
	Severity   int32           `json:"severity"`
	Title      string          `json:"title"`
	Subject    string          `json:"subject"`
	Body       string          `json:"body"`
	Payload    json.RawMessage `json:"payload,omitempty"`
	SendKey    string          `json:"send_key"`
	Aggregate  *biz.AggContext `json:"aggregate,omitempty"`
	OccurredAt time.Time       `json:"occurred_at"`
}

// WebhookSender 通用 Webhook 适配器：仅 HTTPS（设计 §9；测试可经 AllowInsecure
// 放行 http 回环地址）；5xx/超时/网络错误可重试，其余 4xx 不可重试。
type WebhookSender struct {
	client        *http.Client
	allowInsecure bool
}

// NewWebhookSender new a WebhookSender（连接复用、禁跳转）。
func NewWebhookSender() *WebhookSender {
	return &WebhookSender{client: &http.Client{
		Transport: &http.Transport{
			MaxIdleConns: 64, MaxIdleConnsPerHost: 8, IdleConnTimeout: 90 * time.Second,
			DialContext: (&net.Dialer{Timeout: 3 * time.Second}).DialContext,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
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
	body, err := json.Marshal(webhookPayload{
		EventID: msg.EventID, BizID: msg.BizID, EventType: msg.EventType, Severity: msg.Severity, Title: msg.Title,
		Subject: msg.Subject, Body: msg.Body, Payload: payloadRaw(msg.Payload), SendKey: msg.SendKey, Aggregate: msg.Agg,
		OccurredAt: time.Now().UTC(),
	})
	if err != nil {
		return biz.SendResult{Retryable: false}, fmt.Errorf("encode webhook body: %w", err)
	}
	method := strings.ToUpper(cfg.Method)
	if method == "" {
		method = http.MethodPost
	}
	if cfg.TimeoutS > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(cfg.TimeoutS)*time.Second)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(ctx, method, cfg.URL, bytes.NewReader(body))
	if err != nil {
		return biz.SendResult{Retryable: false}, fmt.Errorf("build webhook request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "reborn-notify/1.0")
	for k, v := range cfg.Headers {
		req.Header.Set(k, v)
	}
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	req.Header.Set(HeaderSendKey, msg.SendKey)
	req.Header.Set(HeaderEventID, msg.EventID)
	req.Header.Set(HeaderTimestamp, ts)
	if ch.Secret != "" {
		req.Header.Set(HeaderSignature, Signature(ch.Secret, ts, body))
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return biz.SendResult{Retryable: true}, fmt.Errorf("webhook request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return biz.SendResult{ProviderMsgID: resp.Header.Get("X-Request-Id")}, nil
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode >= 500:
		return biz.SendResult{Retryable: true}, fmt.Errorf("webhook responded %d", resp.StatusCode)
	default:
		return biz.SendResult{Retryable: false}, fmt.Errorf("webhook responded %d", resp.StatusCode)
	}
}

// Signature 计算出站签名头值：sha256=hex(HMAC-SHA256(secret, ts + "." + body))。
func Signature(secret, ts string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(ts))
	mac.Write([]byte("."))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// VerifySignature 接收端校验助手（测试与下游 SDK 复用）。
func VerifySignature(secret, ts string, body []byte, header string) bool {
	return hmac.Equal([]byte(Signature(secret, ts, body)), []byte(header))
}

func payloadRaw(p []byte) json.RawMessage {
	if len(p) == 0 || !json.Valid(p) {
		return nil
	}
	return json.RawMessage(p)
}
