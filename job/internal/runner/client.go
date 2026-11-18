package runner

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/lanyulei/reborn/backend/kit/jobcontract"
)

const (
	// 引擎请求超时：心跳与领取是小请求；上报 / 结束可能带数百 KB 日志，放宽一些。
	controlTimeout = 5 * time.Second
	reportTimeout  = 20 * time.Second
	// claimTimeout 领取的超时：应答可能带着待分发的文件（4MiB，base64 后约 5.6MiB），慢链路上要给足时间——
	// 应答收不完，那条执行在引擎侧就成了没人跑的「执行中」。
	claimTimeout = 60 * time.Second
	// maxResponseBytes 应答体上限：领取应答最大约 6MiB（待分发的文件 + 至多 500 台主机 + 凭据）。
	maxResponseBytes = 16 << 20
)

// 引擎明确答复的协议错误（其余错误一律按可重试处理）。
var (
	// errLeaseLost 租约已失效（409）：终止并丢弃作业。
	errLeaseLost = errors.New("engine: execution lease lost")
	// errUnauthorized 接入令牌无效（401）。
	errUnauthorized = errors.New("engine: runner token rejected")
	// errDisabled 节点已停用（403）：不再领取。
	errDisabled = errors.New("engine: runner disabled")
)

// statusError 引擎返回的其它非 200 状态；错误信息只含状态码，不含应答体与请求内容。
type statusError struct {
	code int
}

func (e *statusError) Error() string { return fmt.Sprintf("engine: unexpected status %d", e.code) }

// permanent 报告错误是否不会因重试而好转：令牌被拒，或引擎认定请求本身不合法（4xx，
// 408 / 429 除外）。作业执行期间仍按可重试处理（不因此放弃在跑的作业）；只有收尾阶段持续遇到
// 这类错误才放弃，见 job.finalize。
func permanent(err error) bool {
	if errors.Is(err, errUnauthorized) || errors.Is(err, errDisabled) {
		return true
	}
	var se *statusError
	if errors.As(err, &se) {
		return se.code >= 400 && se.code < 500 && se.code != http.StatusRequestTimeout && se.code != http.StatusTooManyRequests
	}
	return false
}

// engine 引擎协议客户端的最小接口（作业与节点循环消费；测试用假实现替换）。
type engine interface {
	Heartbeat(ctx context.Context, req *jobcontract.HeartbeatRequest) (*jobcontract.HeartbeatResponse, error)
	Claim(ctx context.Context) (*jobcontract.Task, error)
	Report(ctx context.Context, req *jobcontract.ReportRequest) (*jobcontract.ReportResponse, error)
	Finish(ctx context.Context, req *jobcontract.FinishRequest) error
}

// client 引擎协议的 HTTP 客户端。
type client struct {
	base  string
	token string
	http  *http.Client
}

var _ engine = (*client)(nil)

// newClient 构造客户端：TLS 1.2+，可选自定义 CA；不跟随重定向（接入令牌不随重定向发往别处）。
func newClient(cfg *Config) (*client, error) {
	tlsConf := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: cfg.InsecureSkipVerify} //nolint:gosec // 仅开发环境经配置显式开启
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read runner.ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("runner.ca_file contains no PEM certificate")
		}
		tlsConf.RootCAs = pool
	}
	tr := &http.Transport{
		TLSClientConfig:       tlsConf,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: reportTimeout, // 只管等到应答头；应答体的时限由各请求的超时决定
		IdleConnTimeout:       90 * time.Second,
		MaxIdleConnsPerHost:   4,
		ForceAttemptHTTP2:     true,
	}
	return &client{
		base:  cfg.EngineURL,
		token: cfg.Token,
		http: &http.Client{
			Transport:     tr,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}, nil
}

// do 发送一次 POST 并解码应答；非 200 映射为协议错误。
func (c *client) do(ctx context.Context, path string, timeout time.Duration, in, out any) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	body, err := json.Marshal(in)
	if err != nil {
		return fmt.Errorf("encode request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("engine request %s: %w", path, err)
	}
	defer func() {
		// 读尽（有上限）再关闭，连接才能复用。
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		_ = resp.Body.Close()
	}()
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusUnauthorized:
		return errUnauthorized
	case http.StatusForbidden:
		return errDisabled
	case http.StatusConflict:
		return errLeaseLost
	default:
		return &statusError{code: resp.StatusCode}
	}
	if out == nil {
		return nil
	}
	dec := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes))
	dec.UseNumber() // 任务参数里的整数原样保留
	if err := dec.Decode(out); err != nil {
		return fmt.Errorf("decode engine response %s: %w", path, err)
	}
	return nil
}

// Heartbeat implements engine.
func (c *client) Heartbeat(ctx context.Context, req *jobcontract.HeartbeatRequest) (*jobcontract.HeartbeatResponse, error) {
	out := &jobcontract.HeartbeatResponse{}
	if err := c.do(ctx, jobcontract.RunnerPathHeartbeat, controlTimeout, req, out); err != nil {
		return nil, err
	}
	return out, nil
}

// Claim implements engine；没有任务返回 (nil, nil)。
func (c *client) Claim(ctx context.Context) (*jobcontract.Task, error) {
	out := &jobcontract.ClaimResponse{}
	if err := c.do(ctx, jobcontract.RunnerPathClaim, claimTimeout, struct{}{}, out); err != nil {
		return nil, err
	}
	return out.Task, nil
}

// Report implements engine.
func (c *client) Report(ctx context.Context, req *jobcontract.ReportRequest) (*jobcontract.ReportResponse, error) {
	out := &jobcontract.ReportResponse{}
	if err := c.do(ctx, jobcontract.RunnerPathReport, reportTimeout, req, out); err != nil {
		return nil, err
	}
	return out, nil
}

// Finish implements engine.
func (c *client) Finish(ctx context.Context, req *jobcontract.FinishRequest) error {
	return c.do(ctx, jobcontract.RunnerPathFinish, reportTimeout, req, nil)
}
