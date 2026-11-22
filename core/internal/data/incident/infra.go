package data

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/incident"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	"github.com/lanyulei/reborn/backend/kit/netguard"
	"github.com/lanyulei/reborn/backend/kit/secretbox"
)

var _ biz.TokenCipher = (*TokenBox)(nil)

// TokenBox 分享链接令牌与 Webhook 密钥的加解密：kit/secretbox（AES-256-GCM），主密钥与通知渠道凭据相同
// （KRATOS_NOTIFY_MASTER_KEY）。
type TokenBox struct {
	box *secretbox.Box
}

// NewTokenBox 由启动配置构造。
func NewTokenBox(c *conf.Notify) (*TokenBox, error) {
	box, err := secretbox.New(c.GetMaster().GetKey())
	if err != nil {
		return nil, err
	}
	return &TokenBox{box: box}, nil
}

// Seal implements biz.TokenCipher.
func (b *TokenBox) Seal(plaintext string) ([]byte, error) {
	out, err := b.box.Seal(plaintext)
	if errors.Is(err, secretbox.ErrUnavailable) {
		return nil, biz.ErrSecretUnavailable
	}
	return out, err
}

// Open implements biz.TokenCipher.
func (b *TokenBox) Open(sealed []byte) (string, error) {
	out, err := b.box.Open(sealed)
	if errors.Is(err, secretbox.ErrUnavailable) {
		return "", biz.ErrSecretUnavailable
	}
	if err != nil {
		return "", fmt.Errorf("open sealed token: %w", err)
	}
	return out, nil
}

// Webhook 出站请求头。
const (
	headerEventID   = "X-Incident-Event-Id"
	headerTimestamp = "X-Incident-Timestamp"
	headerSignature = "X-Incident-Signature"
	webhookTimeout  = 10 * time.Second
	maxResponse     = 16 << 10
)

var _ biz.WebhookSender = (*WebhookSender)(nil)

// WebhookSender Webhook 投递器：仅 HTTPS、不跟随跳转；连接前对解析后的真实地址做内网 / 回环校验
// （kit/netguard，免疫 DNS 重绑定）；带 HMAC-SHA256 签名（sha256=hex(HMAC(secret, ts + "." + body))）。
type WebhookSender struct {
	client *http.Client
	dialer *netguard.Dialer
}

// NewWebhookSender new a WebhookSender.
func NewWebhookSender() *WebhookSender {
	s := &WebhookSender{dialer: netguard.NewDialer(3 * time.Second)}
	s.client = &http.Client{
		Timeout: webhookTimeout,
		Transport: &http.Transport{
			MaxIdleConns: 16, MaxIdleConnsPerHost: 4, IdleConnTimeout: 90 * time.Second,
			TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: webhookTimeout,
			DialContext: s.dialer.DialContext,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	return s
}

func sign(secret, ts string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(ts))
	mac.Write([]byte{'.'})
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// Send implements biz.WebhookSender。错误信息只含状态码，不回显响应体与密钥。
func (s *WebhookSender) Send(ctx context.Context, d biz.WebhookDelivery) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.URL, bytes.NewReader(d.Body))
	if err != nil {
		return false, errors.New("webhook url is invalid")
	}
	if req.URL.Scheme != "https" && !s.dialer.AllowInsecure {
		return false, errors.New("webhook url must be https")
	}
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(headerEventID, d.EventID)
	req.Header.Set("Idempotency-Key", d.EventID)
	req.Header.Set(headerTimestamp, ts)
	if d.Secret != "" {
		req.Header.Set(headerSignature, sign(d.Secret, ts, d.Body))
	}
	resp, err := s.client.Do(req)
	if err != nil {
		if errors.Is(err, netguard.ErrHostBlocked) {
			return false, errors.New("webhook host resolves to a private or local address")
		}
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return true, errors.New("webhook call timed out")
		}
		return true, errors.New("webhook call failed: network error")
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponse))
		_ = resp.Body.Close()
	}()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		retryable := resp.StatusCode >= 500 || resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusTooManyRequests
		return retryable, fmt.Errorf("webhook responded with status %d", resp.StatusCode)
	}
	return false, nil
}
