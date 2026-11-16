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

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"
)

// 自动任务出站请求头。
const (
	// HeaderJobID 作业 ID：同一条作业的多次尝试相同，接收端据此去重（至少一次投递）。
	HeaderJobID = "X-Workflow-Job-Id"
	// HeaderTimestamp Unix 秒。
	HeaderTimestamp = "X-Workflow-Timestamp"
	// HeaderSignature sha256=hex(HMAC-SHA256(secret, ts + "." + body))；端点配置了密钥时携带。
	HeaderSignature = "X-Workflow-Signature"
)

// webhookTimeout 单次调用的总超时（须明显小于作业租约）。
const webhookTimeout = 15 * time.Second

// maxWebhookResponse 响应体读取上限。
const maxWebhookResponse = 64 << 10

// errHostBlocked 目标解析到回环 / 内网 / 链路本地地址（SSRF 防护）：确定性失败，不重试。
var errHostBlocked = errors.New("webhook host resolves to a private or local address")

var _ biz.ServiceCaller = (*WebhookCaller)(nil)

// WebhookCaller 自动任务的 Webhook 调用器：仅 HTTPS、不跟随跳转；连接前对**解析后的真实
// 地址**做内网 / 回环校验（免疫 DNS 重绑定），任一解析地址命中即拒绝；响应体有上限。
type WebhookCaller struct {
	client   *http.Client
	dialer   *net.Dialer
	resolver interface {
		LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error)
	}
	allowInsecure bool
}

// NewWebhookCaller new a WebhookCaller.
func NewWebhookCaller() *WebhookCaller {
	c := &WebhookCaller{dialer: &net.Dialer{Timeout: 3 * time.Second}, resolver: net.DefaultResolver}
	c.client = &http.Client{
		Timeout: webhookTimeout,
		Transport: &http.Transport{
			MaxIdleConns: 32, MaxIdleConnsPerHost: 4, IdleConnTimeout: 90 * time.Second,
			TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: webhookTimeout,
			DialContext: c.dialContext,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	return c
}

// WithInsecure 允许 http:// 与回环地址（仅测试脚手架使用）。
func (c *WebhookCaller) WithInsecure() *WebhookCaller {
	c.allowInsecure = true
	return c
}

// dialContext 先解析再逐地址校验拨号：任一地址为内网 / 回环即整体拒绝（不能「挑一个公网
// 地址连」——对端可返回混合记录诱导）。
func (c *WebhookCaller) dialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if c.allowInsecure {
		return c.dialer.DialContext(ctx, network, addr)
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	ips, err := c.resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("webhook host %q resolves to no address", host)
	}
	for _, ip := range ips {
		if biz.BlockedIP(ip.IP) {
			return nil, fmt.Errorf("%w: %s", errHostBlocked, host)
		}
	}
	var last error
	for _, ip := range ips {
		conn, err := c.dialer.DialContext(ctx, network, net.JoinHostPort(ip.IP.String(), port))
		if err == nil {
			return conn, nil
		}
		last = err
	}
	return nil, last
}

// sign 计算请求签名。
func sign(secret, ts string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(ts))
	mac.Write([]byte{'.'})
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// Call implements biz.ServiceCaller：POST JSON。2xx 为成功并返回响应体；网络错误、超时、
// 5xx / 408 / 429 可重试；其余状态码与被安全策略拦截不可重试。错误信息只含状态码，
// 不回显响应体与密钥。
func (c *WebhookCaller) Call(ctx context.Context, hook *biz.Webhook, jobID string, body []byte) ([]byte, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, hook.URL, bytes.NewReader(body))
	if err != nil {
		return nil, false, errors.New("webhook url is invalid")
	}
	if req.URL.Scheme != "https" && !c.allowInsecure {
		return nil, false, errors.New("webhook url must be https")
	}
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(HeaderJobID, jobID)
	req.Header.Set("Idempotency-Key", jobID)
	req.Header.Set(HeaderTimestamp, ts)
	if hook.Secret != "" {
		req.Header.Set(HeaderSignature, sign(hook.Secret, ts, body))
	}
	resp, err := c.client.Do(req)
	if err != nil {
		if errors.Is(err, errHostBlocked) {
			return nil, false, errHostBlocked
		}
		// 不透出底层错误全文（可能含地址细节）：只区分超时与其它网络错误。
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return nil, true, errors.New("webhook call timed out")
		}
		return nil, true, errors.New("webhook call failed: network error")
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxWebhookResponse))
		_ = resp.Body.Close()
	}()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		retryable := resp.StatusCode >= 500 || resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusTooManyRequests
		return nil, retryable, fmt.Errorf("webhook responded with status %d", resp.StatusCode)
	}
	out, err := io.ReadAll(io.LimitReader(resp.Body, maxWebhookResponse+1))
	if err != nil {
		return nil, true, errors.New("webhook call failed: reading the response")
	}
	if len(out) > maxWebhookResponse {
		return nil, false, errors.New("webhook response is too large")
	}
	return bytes.TrimSpace(out), false, nil
}
