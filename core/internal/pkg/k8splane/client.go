// Package k8splane 是 core 访问容器平台数据面（backend/k8s）内部接口的客户端，实现 k8s 域 biz.Plane：
// 按集群归属选实例、给每个请求签名身份头、把数据面的错误体还原为业务错误。协议见 kit/k8scontract。
//
// 不含数据库访问（实例地址经 Resolver 取得，由 data 层实现），所以放在 internal/pkg：
// server 层的端到端测试可以用内存 Resolver 直接驱动它。
//
// 安全：目标地址只来自 Resolver（即数据面自己登记的 k8s_instance.addr），从不取自用户输入；
// 不跟随重定向；响应体有上限；身份头每次请求现签（60 秒有效）。
package k8splane

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/k8s"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"

	"github.com/coder/websocket"
)

const (
	defaultCallTimeout = 30 * time.Second
	// streamHeaderTimeout 流式请求等待数据面响应头的上限（建立日志流、订阅前的权限检查都在这之内）。
	streamHeaderTimeout = 30 * time.Second
	dialTimeout         = 3 * time.Second
	// maxResponseBytes 一元调用的响应体上限：单页列表与单个对象的 YAML 都远小于它。
	maxResponseBytes = 32 << 20
	// maxErrorBytes 错误响应体的读取上限。
	maxErrorBytes = 64 << 10
)

// Resolver 数据面实例定位（data 层实现，内部带短时缓存）。返回的地址形如 http://host:port。
type Resolver interface {
	// Owner 负责该集群的存活实例地址；集群不存在返回 biz.ErrClusterNotFound，没有存活的负责实例返回
	// biz.ErrPlaneUnavailable 的说明。
	Owner(ctx context.Context, clusterID string) (string, error)
	// Any 任一存活实例的地址（与集群无关的操作）。
	Any(ctx context.Context) (string, error)
	// Invalidate 丢弃该集群的归属缓存（归属已转移或实例连不上时调用）。
	Invalidate(clusterID string)
}

// Client implements biz.Plane.
type Client struct {
	resolver Resolver
	key      []byte
	call     *http.Client
	stream   *http.Client
	log      *slog.Logger
	now      func() time.Time
}

var _ biz.Plane = (*Client)(nil)

// New 构造客户端。内部密钥未配置或不足 32 字节时构造成功，但每次调用都返回 503 K8S_PLANE_UNAVAILABLE
// （不阻断 core 启动：集群登记等不经数据面的功能照常可用）。
func New(resolver Resolver, c *conf.K8S, logger *slog.Logger) *Client {
	timeout := defaultCallTimeout
	if d := c.GetPlaneTimeout().AsDuration(); d > 0 {
		timeout = d
	}
	key := []byte(c.GetInternalKey())
	if len(key) < kc.MinInternalKeyBytes {
		if len(key) > 0 {
			logger.Warn("k8s.internal_key is shorter than 32 bytes: k8s data plane calls are disabled")
		}
		key = nil
	}
	transport := func(headerTimeout time.Duration) *http.Transport {
		return &http.Transport{
			Proxy:                 nil, // 内网直连，不走环境变量代理
			DialContext:           (&net.Dialer{Timeout: dialTimeout, KeepAlive: 30 * time.Second}).DialContext,
			MaxIdleConnsPerHost:   32,
			IdleConnTimeout:       90 * time.Second,
			ResponseHeaderTimeout: headerTimeout,
			DisableCompression:    true, // 流式响应不得被压缩缓冲
		}
	}
	noRedirect := func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{
		resolver: resolver, key: key, log: logger, now: time.Now,
		call:   &http.Client{Transport: transport(timeout), Timeout: timeout, CheckRedirect: noRedirect},
		stream: &http.Client{Transport: transport(streamHeaderTimeout), CheckRedirect: noRedirect},
	}
}

// target 取目标实例地址并校验其形态。
func (c *Client) target(ctx context.Context, clusterID string) (*url.URL, error) {
	if c.key == nil {
		return nil, biz.PlaneUnavailable("k8s.internal_key is not configured (at least 32 bytes, same as the data plane's plane.internal_key)")
	}
	var addr string
	var err error
	if clusterID == "" {
		addr, err = c.resolver.Any(ctx)
	} else {
		addr, err = c.resolver.Owner(ctx, clusterID)
	}
	if err != nil {
		return nil, err
	}
	u, err := url.Parse(addr)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
		return nil, biz.PlaneUnavailable("data plane instance advertises an invalid address")
	}
	return &url.URL{Scheme: u.Scheme, Host: u.Host}, nil
}

// headers 身份头与集群头；每次请求现签。
func (c *Client) headers(id kc.Identity, clusterID string) (http.Header, error) {
	h := http.Header{}
	id.IssuedAt = c.now().Unix()
	if err := kc.SignIdentity(h, c.key, id); err != nil {
		return nil, biz.PlaneUnavailable("sign identity: %s", err)
	}
	if clusterID != "" {
		h.Set(kc.ClusterHeader, clusterID)
	}
	return h, nil
}

// post 发起一次请求；返回未读的响应。连接失败视为实例不可用。
func (c *Client) post(ctx context.Context, hc *http.Client, id kc.Identity, clusterID, path string, body []byte) (*http.Response, error) {
	base, err := c.target(ctx, clusterID)
	if err != nil {
		return nil, err
	}
	h, err := c.headers(id, clusterID)
	if err != nil {
		return nil, err
	}
	base.Path = path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header = h
	req.Header.Set("Content-Type", "application/json")
	resp, err := hc.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		c.resolver.Invalidate(clusterID)
		c.log.WarnContext(ctx, "k8s data plane request failed", "path", path, "error", err)
		return nil, biz.PlaneUnavailable("data plane instance is unreachable")
	}
	return resp, nil
}

// planeError 读取错误响应体并还原为业务错误。
func planeError(resp *http.Response) error {
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBytes))
	var body kc.ErrorBody
	if err := json.Unmarshal(raw, &body); err != nil || body.Reason == "" {
		return biz.PlaneError(http.StatusBadGateway, kc.ReasonUpstream, fmt.Sprintf("data plane returned HTTP %d", resp.StatusCode))
	}
	if body.Code == 0 {
		body.Code = resp.StatusCode
	}
	return biz.PlaneError(body.Code, body.Reason, body.Message)
}

// do 带「归属已转移」重试的请求：数据面返回 K8S_NOT_OWNER 时丢弃归属缓存并重试一次。
func (c *Client) do(ctx context.Context, hc *http.Client, id kc.Identity, clusterID, path string, req any) (*http.Response, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	for attempt := 0; ; attempt++ {
		resp, err := c.post(ctx, hc, id, clusterID, path, body)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode == http.StatusOK {
			return resp, nil
		}
		err = planeError(resp)
		_ = resp.Body.Close()
		if attempt == 0 && clusterID != "" && biz.IsNotOwner(err) {
			c.resolver.Invalidate(clusterID)
			continue
		}
		if biz.IsNotOwner(err) {
			return nil, biz.PlaneUnavailable("cluster ownership is moving between data plane instances, please retry")
		}
		return nil, err
	}
}

// Call implements biz.Plane.
func (c *Client) Call(ctx context.Context, id kc.Identity, clusterID, op string, req any) ([]byte, error) {
	resp, err := c.do(ctx, c.call, id, clusterID, kc.InternalOpPath+op, req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, biz.PlaneUnavailable("read data plane response failed")
	}
	if len(raw) > maxResponseBytes {
		return nil, biz.PlaneError(http.StatusBadGateway, kc.ReasonUpstream, "data plane response is too large")
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = []byte("{}")
	}
	return raw, nil
}

// Stream implements biz.Plane.
func (c *Client) Stream(ctx context.Context, id kc.Identity, clusterID, stream string, req any) (io.ReadCloser, error) {
	resp, err := c.do(ctx, c.stream, id, clusterID, kc.InternalStreamPath+stream, req)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// DialExec implements biz.Plane.
func (c *Client) DialExec(ctx context.Context, id kc.Identity, clusterID string, params kc.ExecParams) (biz.ExecConn, error) {
	raw, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	for attempt := 0; ; attempt++ {
		base, err := c.target(ctx, clusterID)
		if err != nil {
			return nil, err
		}
		h, err := c.headers(id, clusterID)
		if err != nil {
			return nil, err
		}
		base.Path = kc.InternalExecPath
		base.RawQuery = url.Values{"params": {base64.RawURLEncoding.EncodeToString(raw)}}.Encode()
		dialCtx, cancel := context.WithTimeout(ctx, streamHeaderTimeout)
		conn, resp, err := websocket.Dial(dialCtx, base.String(), &websocket.DialOptions{HTTPClient: c.stream, HTTPHeader: h})
		cancel()
		if err == nil {
			conn.SetReadLimit(kc.ExecMaxFrameSize + 16)
			return &execConn{conn: conn}, nil
		}
		if resp == nil || resp.Body == nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			c.resolver.Invalidate(clusterID)
			c.log.WarnContext(ctx, "k8s data plane exec dial failed", "error", err)
			return nil, biz.PlaneUnavailable("data plane instance is unreachable")
		}
		perr := planeError(resp)
		_ = resp.Body.Close()
		if attempt == 0 && biz.IsNotOwner(perr) {
			c.resolver.Invalidate(clusterID)
			continue
		}
		return nil, perr
	}
}

// execConn 把 WebSocket 适配为 biz.ExecConn；只收发二进制帧。
type execConn struct {
	conn *websocket.Conn
}

// Read implements biz.ExecConn.
func (e *execConn) Read(ctx context.Context) ([]byte, error) {
	for {
		typ, data, err := e.conn.Read(ctx)
		if err != nil {
			return nil, err
		}
		if typ == websocket.MessageBinary {
			return data, nil
		}
	}
}

// Write implements biz.ExecConn.
func (e *execConn) Write(ctx context.Context, frame []byte) error {
	return e.conn.Write(ctx, websocket.MessageBinary, frame)
}

// Close implements biz.ExecConn.
func (e *execConn) Close() error {
	err := e.conn.CloseNow()
	if err != nil && (errors.Is(err, net.ErrClosed) || strings.Contains(err.Error(), "already")) {
		return nil
	}
	return err
}

var _ biz.PlaneRouter = (*Client)(nil)

// Route implements biz.PlaneRouter：负责该集群的实例地址与已签名的身份头，供 server 层对数据面的原始通道
// （集群 API 透传、文件传输）做不缓冲的反向代理。地址只来自 k8s_instance 表（数据面自己登记），与请求无关。
func (c *Client) Route(ctx context.Context, id kc.Identity, clusterID string) (string, http.Header, error) {
	base, err := c.target(ctx, clusterID)
	if err != nil {
		return "", nil, err
	}
	h, err := c.headers(id, clusterID)
	if err != nil {
		return "", nil, err
	}
	return base.String(), h, nil
}

// Invalidate implements biz.PlaneRouter.
func (c *Client) Invalidate(clusterID string) { c.resolver.Invalidate(clusterID) }
