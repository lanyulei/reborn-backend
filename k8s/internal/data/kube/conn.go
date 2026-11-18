// Package kube 是数据面访问集群的唯一出口（client-go 只允许在 internal/data 之下 import）：
// 每个集群一条连接（共享的 HTTP 传输层 + 平台 / 用户两套客户端）、按需建立的资源缓存、
// 读写操作、日志流与终端。
//
// 身份是本包最重要的不变式：
//   - 用户身份的客户端只能经 conn.asUser(ctx, id) 取得；其传输层对每个出站请求加
//     Impersonate-User / Impersonate-Group，请求上下文里没有身份则拒绝发出；
//   - 平台身份的客户端（探活、缓存、授权同步、权限询问）是另一套对象，其传输层会去掉任何
//     Impersonate-* 头。两套客户端互不可达，不存在「忘了加头就以平台身份发出用户请求」的路径。
package kube

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
)

// TunnelDialer Agent 接入的集群的拨号方式：在该集群的隧道上开一条到其 API Server 的流
// （流上跑明文 HTTP/1.1，隧道本身是 TLS）。隧道服务端落地后实现本接口并替换 OfflineDialer。
type TunnelDialer interface {
	DialCluster(ctx context.Context, clusterID string) (net.Conn, error)
}

// OfflineDialer 隧道服务端尚未接入时的缺省实现：Agent 集群一律拨号失败（表现为不可达）。
type OfflineDialer struct{}

// ErrAgentOffline Agent 未连接。
var ErrAgentOffline = errors.New("agent is not connected")

// DialCluster implements TunnelDialer.
func (OfflineDialer) DialCluster(context.Context, string) (net.Conn, error) {
	return nil, ErrAgentOffline
}

// SecretOpener 解密集群凭据（data.SecretBox）。
type SecretOpener interface {
	Open(ciphertext []byte) (string, error)
}

// ScopeReader 读取集群的角色数据范围（全部命名空间请求时，用它枚举用户可能有权限的命名空间）。
type ScopeReader interface {
	Scopes(ctx context.Context, clusterID string) ([]biz.Scope, error)
}

// SavedEndpoint 集群已保存的连接地址。已保存的凭据只允许发往它：否则持有「测试连接」权限的人
// 可以把地址换成自己的服务器，骗出只写不读的集群凭据。
type SavedEndpoint struct {
	APIServer string
	ProxyURL  string
	CACert    string
}

// CredentialReader 读取已保存的凭据密文及其对应的连接地址（测试连接时沿用）。
type CredentialReader interface {
	Credential(ctx context.Context, clusterID string) ([]byte, SavedEndpoint, error)
}

// Options 连接与缓存参数。
type Options struct {
	QPS         float32
	Burst       int
	CacheIdle   time.Duration
	SyncTimeout time.Duration
	// MemoryLimit 进程堆内存上限（字节）；0 = 不限。
	MemoryLimit uint64
}

func (o Options) withDefaults() Options {
	if o.QPS <= 0 {
		o.QPS = 50
	}
	if o.Burst <= 0 {
		o.Burst = 100
	}
	if o.CacheIdle <= 0 {
		o.CacheIdle = 15 * time.Minute
	}
	if o.SyncTimeout <= 0 {
		o.SyncTimeout = 30 * time.Second
	}
	return o
}

// Manager 全部集群连接的注册表。
type Manager struct {
	opener SecretOpener
	scopes ScopeReader
	creds  CredentialReader
	tunnel TunnelDialer
	opts   Options
	log    *slog.Logger

	mu    sync.RWMutex
	conns map[string]*conn // 不变式：只含 biz.Registry 判定为本实例负责的集群

	// apps Helm 仓库读取、事后审计与索引缓存（EnableApps 注入；见 helm_repo.go）。
	apps *appsState

	// debugImages 调试容器的镜像允许清单（SetDebugImages 注入；见 debug.go）。
	debugImages []string

	// helm 在途的 Helm 写操作（见 helm_release.go）。
	helm *helmOps
}

var (
	_ biz.Connector       = (*Manager)(nil)
	_ biz.ClusterProbe    = (*Manager)(nil)
	_ biz.RBAC            = (*Manager)(nil)
	_ biz.NamespaceLister = (*Manager)(nil)
	_ biz.Executor        = (*Manager)(nil)
	_ biz.Streamer        = (*Manager)(nil)
	_ biz.ClusterTester   = (*Manager)(nil)
	_ biz.Shell           = (*Manager)(nil)
)

// NewManager new a Manager.
func NewManager(opener SecretOpener, scopes ScopeReader, creds CredentialReader, tunnel TunnelDialer, opts Options, log *slog.Logger) *Manager {
	return &Manager{opener: opener, scopes: scopes, creds: creds, tunnel: tunnel, opts: opts.withDefaults(), log: log, conns: map[string]*conn{}, helm: newHelmOps()}
}

// Connect implements biz.Connector：以新配置替换旧连接。构造失败时登记一条「坏连接」，
// 之后的每个操作都返回同一个原因（探活把它写进 last_error）。
func (m *Manager) Connect(_ context.Context, c *biz.Cluster) error {
	nc, err := m.build(c, nil)
	if err != nil {
		nc = &conn{m: m, cluster: c, buildErr: err}
		nc.ctx, nc.cancel = context.WithCancel(context.Background())
	}
	m.mu.Lock()
	old := m.conns[c.ID]
	m.conns[c.ID] = nc
	m.mu.Unlock()
	if old != nil {
		old.close()
	}
	return err
}

// Disconnect implements biz.Connector.
func (m *Manager) Disconnect(clusterID string) {
	m.mu.Lock()
	old := m.conns[clusterID]
	delete(m.conns, clusterID)
	m.mu.Unlock()
	m.helm.cancelCluster(clusterID)
	if old != nil {
		old.close()
	}
}

// Close 断开全部连接（进程退出）。先等在途的 Helm 写操作收尾（至多 helmShutdownWait）：
// 半途掐断的安装 / 升级会把 Release 留在 pending 状态。
func (m *Manager) Close() {
	m.helm.shutdown(helmShutdownWait)
	m.mu.Lock()
	all := m.conns
	m.conns = map[string]*conn{}
	m.mu.Unlock()
	for _, c := range all {
		c.close()
	}
}

func (m *Manager) conn(clusterID string) (*conn, error) {
	m.mu.RLock()
	c := m.conns[clusterID]
	m.mu.RUnlock()
	if c == nil {
		return nil, biz.ErrClusterOffline("cluster connection is not established")
	}
	if c.buildErr != nil {
		return nil, biz.ErrClusterOffline("%s", c.buildErr.Error())
	}
	return c, nil
}

// ClearAccess 清空全部集群的权限询问缓存与数据范围缓存（数据范围配置变化、授权同步落地之后）。
func (m *Manager) ClearAccess() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, c := range m.conns {
		if c.access != nil {
			c.access.clear()
		}
	}
}

// ClearClusterAccess 清空某个集群的权限询问缓存（该集群的绑定刚被对账修改）。
func (m *Manager) ClearClusterAccess(clusterID string) {
	m.mu.RLock()
	c := m.conns[clusterID]
	m.mu.RUnlock()
	if c != nil && c.access != nil {
		c.access.clear()
	}
}

// conn 一个集群的连接：创建后除缓存与各自带锁的子结构外只读。
type conn struct {
	m        *Manager
	cluster  *biz.Cluster
	buildErr error

	// host API Server 的基地址（Agent 集群为占位的 http 地址，实际走隧道）。
	host string
	// userRT / platRT 两种身份的传输层（HTTP/2 可用）；userH1 仅用于协议升级（终端）。
	userRT http.RoundTripper
	platRT http.RoundTripper
	userH1 http.RoundTripper
	idle   []*http.Transport

	userDyn  dynamic.Interface
	userKube kubernetes.Interface
	platDyn  dynamic.Interface
	platKube kubernetes.Interface
	// platMeta 只取元数据的客户端（ConfigMap / Secret 的缓存、CRD 目录）。
	platMeta metadata.Interface
	// platDisco 资源发现客户端：带整体超时（发现接口不接受 context）。
	platDisco discovery.DiscoveryInterface

	disco  *discoveryCache
	cache  *resourceCache
	access *accessCache

	// 探活用的节点数（节点缓存未建立时低频刷新，见 probe）。
	nodesMu                sync.Mutex
	nodesTotal, nodesReady int
	nodesAt                time.Time

	// ctx 连接的生命周期：断开时取消，缓存、日志流、终端随之结束。
	ctx    context.Context
	cancel context.CancelFunc
}

func (c *conn) close() {
	c.cancel()
	if c.cache != nil {
		c.cache.stopAll()
	}
	for _, t := range c.idle {
		t.CloseIdleConnections()
	}
}

// directSpec 直连参数（集群行或「测试连接」请求）。
type directSpec struct {
	apiServer string
	proxyURL  string
	caCert    string
	cred      k8scontract.Credential
}

// build 构造连接。spec 非 nil 时按它直连（测试连接），否则按集群行。
func (m *Manager) build(c *biz.Cluster, spec *directSpec) (*conn, error) {
	var (
		host   string
		h2, h1 *http.Transport
		token  string
		err    error
	)
	switch {
	case spec != nil:
		host, h2, h1, token, err = directTransports(*spec)
	case c.AccessMode == k8scontract.AccessAgent:
		host, h2, h1 = m.tunnelTransports(c.ID)
	default:
		var cred k8scontract.Credential
		if len(c.Credential) == 0 {
			return nil, errors.New("cluster credential is not configured")
		}
		plain, oerr := m.opener.Open(c.Credential)
		if oerr != nil {
			return nil, fmt.Errorf("decrypt cluster credential: %w", oerr)
		}
		if jerr := json.Unmarshal([]byte(plain), &cred); jerr != nil {
			return nil, errors.New("cluster credential is malformed")
		}
		host, h2, h1, token, err = directTransports(directSpec{apiServer: c.APIServer, proxyURL: c.ProxyURL, caCert: c.CACert, cred: cred})
	}
	if err != nil {
		return nil, err
	}
	nc := &conn{m: m, cluster: c, host: host, idle: []*http.Transport{h2, h1}}
	nc.ctx, nc.cancel = context.WithCancel(context.Background())
	var base2, base1 http.RoundTripper = h2, h1
	if spec == nil && c.AccessMode == k8scontract.AccessAgent {
		base2, base1 = cappedTransport{next: h2, limit: maxTunnelResponse}, cappedTransport{next: h1, limit: maxTunnelResponse}
	}
	nc.userRT = userRoundTripper{next: bearerRoundTripper{token: token, next: base2}}
	nc.userH1 = userRoundTripper{next: bearerRoundTripper{token: token, next: base1}}
	nc.platRT = platformRoundTripper{next: bearerRoundTripper{token: token, next: base2}}

	userCfg := &rest.Config{Host: host, Transport: nc.userRT, QPS: m.opts.QPS, Burst: m.opts.Burst, UserAgent: userAgent}
	platCfg := &rest.Config{Host: host, Transport: nc.platRT, QPS: m.opts.QPS, Burst: m.opts.Burst, UserAgent: userAgent}
	if nc.userDyn, err = dynamic.NewForConfig(userCfg); err == nil {
		if nc.userKube, err = kubernetes.NewForConfig(userCfg); err == nil {
			if nc.platDyn, err = dynamic.NewForConfig(platCfg); err == nil {
				if nc.platKube, err = kubernetes.NewForConfig(platCfg); err == nil {
					if nc.platMeta, err = metadata.NewForConfig(platCfg); err == nil {
						discoCfg := rest.CopyConfig(platCfg)
						discoCfg.Timeout = discoveryTimeout
						nc.platDisco, err = discovery.NewDiscoveryClientForConfig(discoCfg)
					}
				}
			}
		}
	}
	if err != nil {
		nc.cancel()
		return nil, fmt.Errorf("build cluster clients: %w", err)
	}
	nc.disco = &discoveryCache{}
	nc.access = newAccessCache()
	nc.cache = newResourceCache(nc)
	return nc, nil
}

const userAgent = "reborn-k8s"

// responseHeaderTimeout 等待响应头的上限。watch / 日志 / 终端的响应头都是立即返回的（之后才是长流），
// 所以它只约束「对端收了请求却迟迟不答」：一条已经断掉但还留在连接池里的连接、一个恶意的 Agent。
const responseHeaderTimeout = 60 * time.Second

func baseTransport() *http.Transport {
	return &http.Transport{
		MaxIdleConns:          64,
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
		ResponseHeaderTimeout: responseHeaderTimeout,
		// HTTP/2 连接保活：30 秒无帧即发 PING，15 秒无应答关闭连接。没有它，对端静默消失（NAT 超时、
		// 节点掉电）后复用的连接会一直挂着，其上不带截止时间的请求永远不返回。
		HTTP2: &http.HTTP2Config{SendPingTimeout: 30 * time.Second, PingTimeout: 15 * time.Second},
	}
}

// maxTunnelResponse 经隧道读取的非流式响应体上限。Agent 是不受信的一端：没有上限时，它用一个永不结束的
// 响应体就能撑爆数据面的内存，连带拖垮同一实例上的其他集群。列表都分页（≤500 条 / 页），资源目录与单个
// 对象远小于此。
const maxTunnelResponse = 256 << 20

// cappedTransport 给非流式响应体加字节上限（仅用于隧道）。
type cappedTransport struct {
	next  http.RoundTripper
	limit int64
}

// streamingRequest 报告请求的响应是否是长流（watch、日志跟随、协议升级）：这些不设字节上限。
func streamingRequest(r *http.Request) bool {
	if r.Header.Get("Upgrade") != "" {
		return true
	}
	q := r.URL.Query()
	truthy := func(k string) bool { v := q.Get(k); return v == "true" || v == "1" }
	return truthy("watch") || truthy("follow") || strings.Contains(r.URL.Path, "/watch/")
}

func (t cappedTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := t.next.RoundTrip(r)
	if err != nil || resp.Body == nil || streamingRequest(r) {
		return resp, err
	}
	resp.Body = &cappedBody{rc: resp.Body, left: t.limit}
	return resp, nil
}

type cappedBody struct {
	rc   io.ReadCloser
	left int64
}

var errResponseTooLarge = errors.New("kube: response body exceeds the size limit")

func (b *cappedBody) Read(p []byte) (int, error) {
	if b.left <= 0 {
		return 0, errResponseTooLarge
	}
	if int64(len(p)) > b.left {
		p = p[:b.left]
	}
	n, err := b.rc.Read(p)
	b.left -= int64(n)
	return n, err
}

func (b *cappedBody) Close() error { return b.rc.Close() }

// directTransports 直连集群的传输层：仅 https；CA 为空用系统根证书；凭据为令牌或客户端证书。
func directTransports(s directSpec) (host string, h2, h1 *http.Transport, token string, err error) {
	u, err := url.Parse(strings.TrimSpace(s.apiServer))
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil {
		return "", nil, nil, "", errors.New("api server address must be an https URL without credentials")
	}
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if strings.TrimSpace(s.caCert) != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(s.caCert)) {
			return "", nil, nil, "", errors.New("CA certificate is not valid PEM")
		}
		tlsCfg.RootCAs = pool
	}
	switch {
	case s.cred.ClientCert != "" || s.cred.ClientKey != "":
		pair, perr := tls.X509KeyPair([]byte(s.cred.ClientCert), []byte(s.cred.ClientKey))
		if perr != nil {
			return "", nil, nil, "", errors.New("client certificate and key do not form a valid pair")
		}
		tlsCfg.Certificates = []tls.Certificate{pair}
	case s.cred.Token != "":
		token = strings.TrimSpace(s.cred.Token)
	default:
		return "", nil, nil, "", errors.New("cluster credential is empty: a token or a client certificate is required")
	}
	var proxy func(*http.Request) (*url.URL, error)
	if strings.TrimSpace(s.proxyURL) != "" {
		pu, perr := url.Parse(strings.TrimSpace(s.proxyURL))
		if perr != nil || (pu.Scheme != "http" && pu.Scheme != "https") || pu.Host == "" {
			return "", nil, nil, "", errors.New("proxy address must be an http or https URL")
		}
		proxy = http.ProxyURL(pu)
	}
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	h2 = baseTransport()
	h2.Proxy, h2.DialContext, h2.TLSClientConfig, h2.ForceAttemptHTTP2 = proxy, dialer.DialContext, tlsCfg, true
	h1 = baseTransport()
	h1Cfg := tlsCfg.Clone()
	h1Cfg.NextProtos = []string{"http/1.1"}
	h1.Proxy, h1.DialContext, h1.TLSClientConfig = proxy, dialer.DialContext, h1Cfg
	h1.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{} // 禁用 HTTP/2：协议升级只能走 1.1
	return strings.TrimRight(u.Scheme+"://"+u.Host+u.Path, "/"), h2, h1, token, nil
}

// tunnelTransports Agent 集群的传输层：每条连接是隧道上的一条流，流上是明文 HTTP/1.1；
// 凭据由 Agent 自己加，平台不持有。
func (m *Manager) tunnelTransports(clusterID string) (host string, h2, h1 *http.Transport) {
	dial := func(ctx context.Context, _, _ string) (net.Conn, error) { return m.tunnel.DialCluster(ctx, clusterID) }
	h2 = baseTransport()
	h2.DialContext = dial
	h1 = baseTransport()
	h1.DialContext = dial
	return "http://agent.tunnel.invalid", h2, h1
}

// ---------------------------------------------------------------------------
// 身份
// ---------------------------------------------------------------------------

type identityKey struct{}

// errNoIdentity 用户身份的客户端被用在了没有身份的上下文里（编程错误）：请求不会发出。
var errNoIdentity = errors.New("kube: user-identity request without identity in context")

// identityIDPattern 用户 ID 与角色 ID 的形态（UUID）：它们会进请求头与集群内的主体名。
var identityIDPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// validIdentity 校验身份里会进入请求头的字段。
func validIdentity(id biz.Identity) error {
	if !identityIDPattern.MatchString(id.UserID) {
		return biz.ErrInvalid("invalid user id in identity")
	}
	for _, r := range id.RoleIDs {
		if !identityIDPattern.MatchString(r) {
			return biz.ErrInvalid("invalid role id in identity")
		}
	}
	return nil
}

// userClients 以某个用户身份访问集群的客户端集合。只能由 conn.asUser 构造；
// ctx 已带身份，调用集群时必须用它（或其派生 ctx）。
type userClients struct {
	ctx  context.Context
	id   biz.Identity
	dyn  dynamic.Interface
	kube kubernetes.Interface
	c    *conn

	// 以下只在执行已批准的变更单时设置（见 change.go）：
	// helmPin 审批时固定的 Chart 摘要与渲染结果；helmWait Helm 写操作的同步等待时长（0 = 缺省：超时转后台）；
	// started 开始向集群提交之前调用一次，把「已开始」落库（返回错误则不得提交）。
	helmPin  *k8scontract.HelmCheck
	helmWait time.Duration
	started  func() error
}

// asUser 取得以 id 身份访问集群的客户端；身份不合法则返回错误。
func (c *conn) asUser(ctx context.Context, id biz.Identity) (*userClients, error) {
	if err := validIdentity(id); err != nil {
		return nil, err
	}
	return &userClients{ctx: context.WithValue(ctx, identityKey{}, id), id: id, dyn: c.userDyn, kube: c.userKube, c: c}, nil
}

// stripImpersonation 去掉全部 Impersonate-* 头（含 Impersonate-Extra-* 与 Impersonate-Uid）。
func stripImpersonation(h http.Header) {
	for k := range h {
		if strings.HasPrefix(http.CanonicalHeaderKey(k), "Impersonate-") {
			delete(h, k)
		}
	}
}

// userRoundTripper 用户身份的传输层：从请求上下文取身份并加 Impersonate-* 头；没有身份则拒绝发出。
// 入站请求上已有的 Impersonate-* 头一律丢弃，身份只来自上下文。
type userRoundTripper struct{ next http.RoundTripper }

// RoundTrip implements http.RoundTripper.
func (u userRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	id, ok := req.Context().Value(identityKey{}).(biz.Identity)
	if !ok || validIdentity(id) != nil {
		if req.Body != nil {
			_ = req.Body.Close()
		}
		return nil, errNoIdentity
	}
	r := req.Clone(req.Context())
	stripImpersonation(r.Header)
	r.Header.Set("Impersonate-User", k8scontract.ImpersonateUser(id.UserID))
	for _, g := range k8scontract.ImpersonateGroups(id.RoleIDs, id.IsAdmin) {
		r.Header.Add("Impersonate-Group", g)
	}
	return u.next.RoundTrip(r)
}

// platformRoundTripper 平台身份的传输层：去掉任何 Impersonate-* 头，确保以接入凭据本身的身份发出。
type platformRoundTripper struct{ next http.RoundTripper }

// RoundTrip implements http.RoundTripper.
func (p platformRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	r := req.Clone(req.Context())
	stripImpersonation(r.Header)
	return p.next.RoundTrip(r)
}

// bearerRoundTripper 加接入凭据的 Bearer 令牌（客户端证书方式 token 为空，不加头）。
// 入站的 Authorization 一律覆盖：凭据只来自集群配置。
type bearerRoundTripper struct {
	token string
	next  http.RoundTripper
}

// RoundTrip implements http.RoundTripper.
func (b bearerRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	r := req.Clone(req.Context())
	r.Header.Del("Authorization")
	if b.token != "" {
		r.Header.Set("Authorization", "Bearer "+b.token)
	}
	return b.next.RoundTrip(r)
}
