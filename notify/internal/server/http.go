package server

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/lanyulei/reborn/backend/kit/httpx"
	ingressv1 "github.com/lanyulei/reborn/backend/notify/api/ingress/v1"
	"github.com/lanyulei/reborn/backend/notify/internal/biz"
	"github.com/lanyulei/reborn/backend/notify/internal/conf"
	"github.com/lanyulei/reborn/backend/notify/internal/service"

	"github.com/go-kratos/kratos/contrib/otel/v3/tracing"
	"github.com/go-kratos/kratos/v3/middleware/logging"
	"github.com/go-kratos/kratos/v3/middleware/ratelimit"
	"github.com/go-kratos/kratos/v3/middleware/recovery"
	"github.com/go-kratos/kratos/v3/middleware/selector"
	"github.com/go-kratos/kratos/v3/middleware/validate"
	khttp "github.com/go-kratos/kratos/v3/transport/http"
)

// ingressOperations 需 HMAC 鉴权的操作集（proto full method，HTTP/gRPC 同名）。
var ingressOperations = map[string]struct{}{
	ingressv1.OperationNotifyIngressServiceSendEvent: {},
	ingressv1.OperationNotifyIngressServiceBatchSend: {},
}

// NewHTTPServer HTTP 服务器：恒注册 /healthz 与 /metrics；ingress 接口仅 api 角色。
// 中间件：recovery → tracing → logging（ingress 操作走采样日志）→ validate →
// rate-limit → hmac（仅 ingress 操作）。
func NewHTTPServer(c *conf.Server, roles Roles, logger *slog.Logger, auth *biz.ProducerAuth,
	ingress *service.IngressService, metrics *Metrics, health *Health,
) *khttp.Server {
	opts := []khttp.ServerOption{
		khttp.Middleware(
			recovery.Recovery(),
			tracing.Server(),
			selector.Server(logging.Server(logger)).Match(func(_ context.Context, operation string) bool {
				_, ingress := ingressOperations[operation]
				return !ingress
			}).Build(),
			selector.Server(ingressLogging(logger, ingressLogSampleEvery, metrics.ObserveIngressRequest)).Match(func(_ context.Context, operation string) bool {
				_, ingress := ingressOperations[operation]
				return ingress
			}).Build(),
			validate.Validator(httpx.ProtoValidate),
			ratelimit.Server(),
			selector.Server(hmacMiddleware(auth)).Match(func(_ context.Context, operation string) bool {
				_, ok := ingressOperations[operation]
				return ok
			}).Build(),
		),
		khttp.Filter(httpx.LimitRequestBody),
		khttp.RequestDecoder(httpx.DecodeRequest),
		khttp.ResponseEncoder(httpx.EncodeResponse),
	}
	if c.GetHttp().GetNetwork() != "" {
		opts = append(opts, khttp.Network(c.GetHttp().GetNetwork()))
	}
	if c.GetHttp().GetAddr() != "" {
		opts = append(opts, khttp.Address(c.GetHttp().GetAddr()))
	}
	if c.GetHttp().GetTimeout() != nil {
		opts = append(opts, khttp.Timeout(c.GetHttp().GetTimeout().AsDuration()))
	}
	srv := khttp.NewServer(opts...)
	httpx.HardenServer(srv)
	srv.HandleFunc("/healthz", health.ServeHTTP)
	srv.Handle("/metrics", metrics.Handler())
	if roles.Has(RoleAPI) {
		ingressv1.RegisterNotifyIngressServiceHTTPServer(srv, ingress)
	}
	return srv
}

// Health /healthz：配置快照已加载、刷新未长期失败、数据库可达即 200。
type Health struct {
	cfg        *biz.ConfigCache
	ping       func() error
	backbone   biz.Backbone
	staleAfter time.Duration
	now        func() time.Time
}

// configStaleAfter 配置刷新连续失败超过此时长视为不健康（10s 轮询 × 12）：
// 实例还在用旧快照发送，但渠道停用/路由变更已无法到达它。
const configStaleAfter = 2 * time.Minute

// NewHealth new a Health.
func NewHealth(cfg *biz.ConfigCache, pinger biz.Pinger, backbone biz.Backbone) *Health {
	return &Health{cfg: cfg, ping: pinger.Ping, backbone: backbone, staleAfter: configStaleAfter, now: time.Now}
}

// ServeHTTP implements http.Handler.
func (h *Health) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.cfg.Current() == nil {
		http.Error(w, "config snapshot not loaded", http.StatusServiceUnavailable)
		return
	}
	if st := h.cfg.Status(); !st.LastOK.IsZero() && h.now().Sub(st.LastOK) > h.staleAfter {
		http.Error(w, "config refresh stale since "+st.LastOK.UTC().Format(time.RFC3339), http.StatusServiceUnavailable)
		return
	}
	if err := h.ping(); err != nil {
		http.Error(w, "database unreachable: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	if h.backbone != nil {
		if err := h.backbone.Ping(r.Context()); err != nil {
			http.Error(w, h.backbone.Name()+" backbone unreachable: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte("ok"))
}
