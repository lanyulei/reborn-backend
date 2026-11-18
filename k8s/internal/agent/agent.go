package agent

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/tunnelproto"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	"github.com/coder/websocket"
	"github.com/hashicorp/yamux"
)

// Config Agent 的运行参数。
type Config struct {
	// ServerURL 数据面隧道入口地址（https://host[:port]；http 仅限开发）。
	ServerURL string
	// RegisterToken 一次性的注册令牌；已有长期令牌时不使用。
	RegisterToken string
	// Version Agent 版本。
	Version string
	// CAFile 校验隧道入口证书的 CA（PEM 文件）；空 = 系统根证书。
	CAFile string
	// InsecureSkipVerify 跳过隧道入口证书校验（仅限开发）。
	InsecureSkipVerify bool
}

// ErrTokenRejected 隧道入口拒绝了令牌（已吊销、集群已移除或停用）：不应重试。
var ErrTokenRejected = errors.New("agent token rejected by the server")

// ErrClusterDisabled 令牌有效，但集群在平台上被停用（403）：保留令牌，稍后重连。
var ErrClusterDisabled = errors.New("cluster is disabled on the platform")

// rejectLimit / rejectWindow 长期令牌被判定为「已吊销」的条件：连续被拒至少 rejectLimit 次，且从第一次被拒
// 起已过 rejectWindow。可在测试里调小。
var (
	rejectLimit  = 5
	rejectWindow = 5 * time.Minute
	// rejectBackoff 被拒之后的重试间隔。
	rejectBackoff = backoffMax
)

const (
	backoffMin      = time.Second
	backoffMax      = 60 * time.Second
	stableSession   = 30 * time.Second
	handshakeLimit  = 20 * time.Second
	registerTimeout = 20 * time.Second
)

// Agent 隧道客户端。
type Agent struct {
	cfg     Config
	base    *url.URL
	store   TokenStore
	handler http.Handler
	client  *http.Client
	log     *slog.Logger
}

// New 校验配置并构造 Agent。handler 是到 API Server 的反向代理（NewProxy）。
func New(cfg Config, store TokenStore, handler http.Handler, log *slog.Logger) (*Agent, error) {
	u, err := url.Parse(strings.TrimRight(cfg.ServerURL, "/"))
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil {
		return nil, errors.New("server url must be an http(s) URL without credentials")
	}
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: cfg.InsecureSkipVerify} //nolint:gosec // 仅在显式配置（开发环境）时跳过校验
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read ca file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("ca file is not valid PEM")
		}
		tlsCfg.RootCAs = pool
	}
	if u.Scheme == "http" {
		log.Warn("server url is plain http: the agent token and cluster traffic travel unencrypted")
	}
	client := &http.Client{
		Transport: &http.Transport{
			Proxy:               http.ProxyFromEnvironment,
			TLSClientConfig:     tlsCfg,
			TLSHandshakeTimeout: 10 * time.Second,
			ForceAttemptHTTP2:   false,
		},
		// 入口不应跳转；跟随跳转会把令牌带到别处。
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	return &Agent{cfg: cfg, base: u, store: store, handler: handler, client: client, log: log}, nil
}

// Run 注册（如需要）并保持隧道，直到 ctx 结束（返回 nil）或令牌被拒绝（返回 ErrTokenRejected）。
func (a *Agent) Run(ctx context.Context) error {
	token, err := a.store.Load(ctx)
	if err != nil {
		return fmt.Errorf("load agent token: %w", err)
	}
	backoff := backoffMin
	for token == "" {
		if a.cfg.RegisterToken == "" {
			return errors.New("no stored agent token and no register token: generate a new install command")
		}
		token, err = a.register(ctx)
		if errors.Is(err, ErrTokenRejected) {
			return fmt.Errorf("register token is invalid, expired or already used: generate a new install command: %w", err)
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			a.log.Warn("register failed, retrying", "error", err, "in", backoff.String())
			if !sleep(ctx, &backoff) {
				return nil
			}
			continue
		}
		if serr := a.store.Save(ctx, token); serr != nil {
			// 注册令牌已用掉：本次运行继续用内存里的令牌，但重启后需要重新安装。
			a.log.Error("persist agent token failed: the agent will need reinstalling after a restart", "error", serr)
		}
	}
	backoff = backoffMin
	var (
		rejects     int
		firstReject time.Time
	)
	for {
		start := time.Now()
		err := a.session(ctx, token)
		if ctx.Err() != nil {
			return nil
		}
		switch {
		case errors.Is(err, ErrTokenRejected):
			// 一次 401 不足以断定令牌被吊销：前置代理、WAF 也可能返回它。只有持续被拒（至少 rejectLimit 次、
			// 跨越 rejectWindow）才删除令牌并退出；在那之前按最大间隔重试。
			if rejects == 0 {
				firstReject = time.Now()
			}
			rejects++
			if rejects >= rejectLimit && time.Since(firstReject) >= rejectWindow {
				if derr := a.store.Delete(context.WithoutCancel(ctx)); derr != nil {
					a.log.Warn("remove rejected agent token failed", "error", derr)
				}
				return err
			}
			backoff = rejectBackoff
		case errors.Is(err, ErrClusterDisabled):
			// 集群在平台上被停用：保留令牌，慢速重试，等它被重新启用。
			rejects, backoff = 0, rejectBackoff
		default:
			rejects = 0
		}
		if time.Since(start) >= stableSession {
			backoff = backoffMin
		}
		a.log.Warn("tunnel closed, reconnecting", "error", err, "in", backoff.String())
		if !sleep(ctx, &backoff) {
			return nil
		}
	}
}

// sleep 带抖动的指数退避；ctx 结束返回 false。
func sleep(ctx context.Context, backoff *time.Duration) bool {
	d := *backoff/2 + rand.N(*backoff/2+1) //nolint:gosec // 退避抖动，不需要密码学随机
	if *backoff *= 2; *backoff > backoffMax {
		*backoff = backoffMax
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func (a *Agent) register(ctx context.Context) (string, error) {
	body, err := json.Marshal(k8scontract.AgentRegisterRequest{Token: a.cfg.RegisterToken, Version: a.cfg.Version})
	if err != nil {
		return "", err
	}
	rctx, cancel := context.WithTimeout(ctx, registerTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(rctx, http.MethodPost, a.base.String()+k8scontract.AgentRegisterPath, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<10))
	switch {
	case resp.StatusCode == http.StatusUnauthorized:
		return "", ErrTokenRejected
	case resp.StatusCode != http.StatusOK:
		return "", fmt.Errorf("register: unexpected status %d", resp.StatusCode)
	}
	var out k8scontract.AgentRegisterResponse
	if err := json.Unmarshal(raw, &out); err != nil || out.Token == "" {
		return "", errors.New("register: malformed response")
	}
	a.log.Info("agent registered", "cluster", out.ClusterName)
	return out.Token, nil
}

// session 建立一条隧道并服务到它结束。
func (a *Agent) session(ctx context.Context, token string) error {
	hctx, cancel := context.WithTimeout(ctx, handshakeLimit)
	ws, resp, err := websocket.Dial(hctx, a.base.String()+k8scontract.AgentConnectPath, &websocket.DialOptions{
		HTTPClient:   a.client,
		Subprotocols: []string{k8scontract.AgentSubprotocol},
		HTTPHeader: http.Header{
			"Authorization":                {"Bearer " + token},
			k8scontract.AgentVersionHeader: {a.cfg.Version},
		},
		CompressionMode: websocket.CompressionDisabled,
	})
	cancel()
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusUnauthorized {
			return ErrTokenRejected
		}
		if resp != nil && resp.StatusCode == http.StatusForbidden {
			return ErrClusterDisabled
		}
		if resp != nil {
			return fmt.Errorf("connect: status %d", resp.StatusCode)
		}
		return fmt.Errorf("connect: %w", err)
	}
	if ws.Subprotocol() != k8scontract.AgentSubprotocol {
		_ = ws.Close(websocket.StatusPolicyViolation, "subprotocol mismatch")
		return errors.New("connect: server did not accept the tunnel subprotocol")
	}
	ws.SetReadLimit(-1) // yamux 自己做流控
	sctx, stop := context.WithCancel(ctx)
	defer stop()
	conn := websocket.NetConn(sctx, ws, websocket.MessageBinary)
	// Agent 是 yamux 服务端：只接受数据面开的流，自己从不开流。
	mux, err := yamux.Server(conn, tunnelproto.SessionConfig())
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("start tunnel session: %w", err)
	}
	a.log.Info("tunnel connected", "server", a.base.Host)
	srv := &http.Server{
		Handler:           a.handler,
		ReadHeaderTimeout: 30 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return sctx },
		ErrorLog:          slog.NewLogLogger(a.log.Handler(), slog.LevelDebug),
	}
	done := make(chan struct{})
	go func() { // 退出路径：Serve 返回（隧道断开）或 ctx 结束
		select {
		case <-ctx.Done():
			_ = mux.Close()
		case <-done:
		}
	}()
	err = srv.Serve(mux) // 隧道断开时 Accept 返回错误
	close(done)
	_ = mux.Close()
	_ = srv.Close()
	if ctx.Err() != nil {
		return nil
	}
	return err
}
