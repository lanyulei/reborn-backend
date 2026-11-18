package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	"github.com/coder/websocket"
)

// TunnelServer 在一条已认证的连接上承载隧道（data/tunnel.Hub 实现）；阻塞到隧道结束。
type TunnelServer interface {
	Serve(ctx context.Context, clusterID, tokenID, version string, conn net.Conn) error
}

// Agent 隧道入口的限制。入口对所有集群开放，认证之前的每一步都要便宜且有上限。
const (
	// agentRateLimit / agentRateWindow 单个来源 IP 在窗口内的请求数上限（注册与连接合计）。
	agentRateLimit  = 120
	agentRateWindow = time.Minute
	// agentMaxPending 同时处于认证阶段（查库比对令牌）的请求数上限。
	agentMaxPending = 64
	// agentRegisterBodyLimit 注册请求体上限。
	agentRegisterBodyLimit = 4 << 10
	agentAuthTimeout       = 5 * time.Second
	agentVersionMax        = 32
)

// agentBodyDeadline 注册请求体的读取时限；agentMaxPerIP 单个来源同时处于认证之前阶段的请求数上限。
const (
	agentBodyDeadline = 3 * time.Second
	agentMaxPerIP     = 8
)

// AgentService Agent 隧道入口：只有注册与连接两个路径。不记录请求日志——头里有令牌。
type AgentService struct {
	tokens  biz.AgentTokenRepo
	tunnels TunnelServer
	log     *slog.Logger
	limiter *ipLimiter
	pending chan struct{}
	// trusted 可信的前置代理网段：对端地址落在其中时，来源 IP 取 X-Forwarded-For 的最后一跳。
	trusted []*net.IPNet
}

// NewAgentService new an AgentService.
func NewAgentService(tokens biz.AgentTokenRepo, tunnels TunnelServer, log *slog.Logger) *AgentService {
	return &AgentService{
		tokens: tokens, tunnels: tunnels, log: log,
		limiter: newIPLimiter(agentRateLimit, agentRateWindow), pending: make(chan struct{}, agentMaxPending),
	}
}

// TrustProxies 设置可信的前置代理网段（启动期调用）。隧道入口前面有负载均衡或 TLS 终结时必须设置：
// 否则所有 Agent 共用代理的地址，一个来源的限额就是全体的限额。
func (s *AgentService) TrustProxies(cidrs []string) error {
	for _, c := range cidrs {
		if c = strings.TrimSpace(c); c == "" {
			continue
		}
		_, n, err := net.ParseCIDR(c)
		if err != nil {
			return errors.New("server.tunnel.trusted_proxies: " + err.Error())
		}
		s.trusted = append(s.trusted, n)
	}
	return nil
}

// Handler 隧道入口的全部路由；其余路径一律 404。
func (s *AgentService) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(k8scontract.AgentRegisterPath, s.gate(s.serveRegister))
	mux.HandleFunc(k8scontract.AgentConnectPath, s.gate(s.serveConnect))
	return mux
}

// admission 一次请求在认证之前占用的名额；认证通过后 refund（不计入来源的限额）并释放并发名额。
type admission struct {
	s    *AgentService
	key  string
	once sync.Once
}

func (a *admission) release() { a.once.Do(func() { a.s.limiter.done(a.key) }) }

// authenticated 认证通过：归还本次请求占用的限额（限额只约束认证不过的请求）。
func (a *admission) authenticated() { a.s.limiter.refund(a.key) }

// slot 占一个「正在查库比对令牌」的名额；满了返回 false。只包住查库那一步：请求体读取等慢动作不占它，
// 否则几十条慢连接就能让所有 Agent 认证不了。
func (s *AgentService) slot() (func(), bool) {
	select {
	case s.pending <- struct{}{}:
		var once sync.Once
		return func() { once.Do(func() { <-s.pending }) }, true
	default:
		return nil, false
	}
}

// gate 认证之前的闸门：按来源限速、限并发。
func (s *AgentService) gate(next func(http.ResponseWriter, *http.Request, *admission)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := limitKey(s.clientIP(r))
		if !s.limiter.allow(key, time.Now(), agentMaxPerIP) {
			http.Error(w, "too many requests", http.StatusTooManyRequests)
			return
		}
		adm := &admission{s: s, key: key}
		defer adm.release()
		next(w, r, adm)
	}
}

// clientIP 来源地址。默认只认连接的对端地址：X-Forwarded-For 可被伪造，用它限速等于不限。
// 对端是配置的可信代理时，取它追加的那一跳（X-Forwarded-For 的最后一项）。
func (s *AgentService) clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	if len(s.trusted) == 0 {
		return host
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return host
	}
	for _, n := range s.trusted {
		if n.Contains(ip) {
			xff := r.Header.Values("X-Forwarded-For")
			if len(xff) == 0 {
				return host
			}
			parts := strings.Split(xff[len(xff)-1], ",")
			if last := strings.TrimSpace(parts[len(parts)-1]); net.ParseIP(last) != nil {
				return last
			}
			return host
		}
	}
	return host
}

// limitKey 限速的键：IPv4 按地址，IPv6 按 /64（一个用户通常拥有整个 /64，按地址限等于不限）。
func limitKey(host string) string {
	ip := net.ParseIP(host)
	if ip == nil {
		return host
	}
	if v4 := ip.To4(); v4 != nil {
		return v4.String()
	}
	return ip.Mask(net.CIDRMask(64, 128)).String() + "/64"
}

// unauthorized 令牌不存在、已过期、密钥不对、集群已停用：应答完全相同。
func unauthorized(w http.ResponseWriter) {
	http.Error(w, "invalid agent token", http.StatusUnauthorized)
}

func cleanVersion(v string) string {
	v = strings.TrimSpace(v)
	if len(v) > agentVersionMax {
		v = v[:agentVersionMax]
	}
	for _, c := range v {
		if c < 0x20 || c > 0x7e {
			return "unknown"
		}
	}
	return v
}

// tokenIDPattern 令牌 ID 的形态（小写 UUID）。不合形态的直接按无效应答，不进数据库
// （否则 uuid 转换失败会变成 500 与一条错误日志，既刷日志又与「无效」的应答不一致）。
var tokenIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func parseToken(raw string) (id, secret string, ok bool) {
	id, secret, ok = k8scontract.ParseToken(raw)
	if !ok || !tokenIDPattern.MatchString(id) {
		return "", "", false
	}
	return id, secret, true
}

func (s *AgentService) serveRegister(w http.ResponseWriter, r *http.Request, adm *admission) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	// 请求体很小：给读取单独设一个短时限（慢速发送的连接不该占着处理协程 15 秒）。
	_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(agentBodyDeadline))
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, agentRegisterBodyLimit))
	var req k8scontract.AgentRegisterRequest
	if err != nil || json.Unmarshal(body, &req) != nil {
		http.Error(w, "malformed request", http.StatusBadRequest)
		return
	}
	tokenID, secret, ok := parseToken(req.Token)
	if !ok {
		unauthorized(w)
		return
	}
	free, ok := s.slot()
	if !ok {
		http.Error(w, "server busy", http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), agentAuthTimeout)
	grant, err := s.tokens.Exchange(ctx, tokenID, secret)
	cancel()
	free()
	if errors.Is(err, biz.ErrAgentToken) {
		unauthorized(w)
		return
	}
	if err != nil {
		s.log.Error("agent register failed", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	adm.authenticated()
	s.log.Info("agent registered", "cluster", grant.ClusterName, "agent_version", cleanVersion(req.Version))
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, k8scontract.AgentRegisterResponse{Token: grant.Token, ClusterName: grant.ClusterName})
}

func (s *AgentService) serveConnect(w http.ResponseWriter, r *http.Request, adm *admission) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET required", http.StatusMethodNotAllowed)
		return
	}
	bearer, found := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	tokenID, secret, ok := parseToken(bearer)
	if !found || !ok {
		unauthorized(w)
		return
	}
	free, ok := s.slot()
	if !ok {
		http.Error(w, "server busy", http.StatusServiceUnavailable)
		return
	}
	actx, cancel := context.WithTimeout(r.Context(), agentAuthTimeout)
	grant, err := s.tokens.Verify(actx, tokenID, secret)
	cancel()
	free()
	if errors.Is(err, biz.ErrAgentClusterDisabled) {
		// 令牌是对的，只是集群被停用：与「令牌无效」区分开，Agent 据此保留令牌、稍后重连。
		adm.authenticated()
		http.Error(w, "cluster is disabled", http.StatusForbidden)
		return
	}
	if errors.Is(err, biz.ErrAgentToken) {
		unauthorized(w)
		return
	}
	if err != nil {
		s.log.Error("agent token check failed", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	// 认证通过：不计入来源的限额（一批 Agent 经同一个出口重连时不互相挤占），也不再占认证之前的并发名额。
	adm.authenticated()
	adm.release()
	// 来的是 Agent 而不是浏览器（凭 Bearer 令牌认证，浏览器发不出这个头）：不做 Origin 校验。
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols: []string{k8scontract.AgentSubprotocol}, InsecureSkipVerify: true,
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		return
	}
	if ws.Subprotocol() != k8scontract.AgentSubprotocol {
		_ = ws.Close(websocket.StatusPolicyViolation, "subprotocol required")
		return
	}
	// yamux 自己做流控与分帧，单条消息可达一个流窗口：解除 WebSocket 的消息大小上限。
	ws.SetReadLimit(-1)
	// 隧道的生命周期与本次请求的 ctx 脱钩（服务器关停时由 Hub.Close 结束）。
	ctx := context.WithoutCancel(r.Context())
	conn := websocket.NetConn(ctx, ws, websocket.MessageBinary)
	if err := s.tunnels.Serve(ctx, grant.ClusterID, grant.TokenID, cleanVersion(r.Header.Get(k8scontract.AgentVersionHeader)), conn); err != nil {
		s.log.Warn("agent tunnel ended with error", "cluster", grant.ClusterName, "error", err)
	}
}

// ipLimiter 按来源限速：固定窗口内的请求数上限，外加同时在途的请求数上限。键数有上限：超过后淘汰
// 一部分没有在途请求的键（map 的遍历顺序是随机的，相当于随机淘汰），不整体清空——整体清空会让
// 攻击者用大量来源地址替所有人重置限额。
type ipLimiter struct {
	limit  int
	window time.Duration

	mu      sync.Mutex
	start   time.Time
	counts  map[string]int
	active  map[string]int
	maxKeys int
}

func newIPLimiter(limit int, window time.Duration) *ipLimiter {
	return &ipLimiter{limit: limit, window: window, counts: map[string]int{}, active: map[string]int{}, maxKeys: 100_000}
}

// allow 记一次请求。maxActive > 0 时同时限制该来源在途的请求数；放行后调用方必须 done。
func (l *ipLimiter) allow(key string, now time.Time, maxActive int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if now.Sub(l.start) >= l.window {
		l.start, l.counts = now, map[string]int{}
	}
	if _, seen := l.counts[key]; !seen && len(l.counts) >= l.maxKeys {
		drop := l.maxKeys / 10
		for k := range l.counts {
			if drop == 0 {
				break
			}
			if l.active[k] == 0 {
				delete(l.counts, k)
				drop--
			}
		}
	}
	l.counts[key]++
	if l.counts[key] > l.limit {
		return false
	}
	if maxActive > 0 {
		if l.active[key] >= maxActive {
			return false
		}
		l.active[key]++
	}
	return true
}

// done 一次放行的请求结束了认证之前的阶段。
func (l *ipLimiter) done(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.active[key]--; l.active[key] <= 0 {
		delete(l.active, key)
	}
}

// refund 归还一次计数（该请求通过了认证）。
func (l *ipLimiter) refund(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.counts[key] > 0 {
		l.counts[key]--
	}
}
