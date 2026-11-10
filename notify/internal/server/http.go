package server

import (
	"context"
	"log/slog"
	"net/http"
	"time"

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

const (
	httpReadHeaderTimeout = 10 * time.Second
	httpIdleTimeout       = 120 * time.Second
)

// ingressOperations 需 HMAC 鉴权的操作集（proto full method，HTTP/gRPC 同名）。
var ingressOperations = map[string]struct{}{
	ingressv1.OperationNotifyIngressServiceSendEvent: {},
	ingressv1.OperationNotifyIngressServiceBatchSend: {},
}

// NewHTTPServer HTTP 服务器：恒注册 /healthz 与 /metrics；ingress 接口仅 api 角色。
// 中间件：recovery → tracing → logging → validate → rate-limit → hmac（仅 ingress 操作）。
func NewHTTPServer(c *conf.Server, roles Roles, logger *slog.Logger, auth *biz.ProducerAuth,
	ingress *service.IngressService, metrics *Metrics, health *Health,
) *khttp.Server {
	opts := []khttp.ServerOption{
		khttp.Middleware(
			recovery.Recovery(),
			tracing.Server(),
			logging.Server(logger),
			validate.Validator(protoValidate),
			ratelimit.Server(),
			selector.Server(hmacMiddleware(auth)).Match(func(_ context.Context, operation string) bool {
				_, ok := ingressOperations[operation]
				return ok
			}).Build(),
		),
		khttp.Filter(limitRequestBody),
		khttp.RequestDecoder(decodeRequest),
		khttp.ResponseEncoder(encodeResponse),
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
	srv.ReadHeaderTimeout = httpReadHeaderTimeout
	srv.IdleTimeout = httpIdleTimeout
	srv.HandleFunc("/healthz", health.ServeHTTP)
	srv.Handle("/metrics", metrics.Handler())
	if roles.Has(RoleAPI) {
		ingressv1.RegisterNotifyIngressServiceHTTPServer(srv, ingress)
	}
	return srv
}

// Health /healthz：配置快照已加载且数据库可达即 200。
type Health struct {
	cfg  *biz.ConfigCache
	ping func() error
}

// NewHealth new a Health.
func NewHealth(cfg *biz.ConfigCache, pinger biz.Pinger) *Health {
	return &Health{cfg: cfg, ping: pinger.Ping}
}

// ServeHTTP implements http.Handler.
func (h *Health) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	if h.cfg.Current() == nil {
		http.Error(w, "config snapshot not loaded", http.StatusServiceUnavailable)
		return
	}
	if err := h.ping(); err != nil {
		http.Error(w, "database unreachable: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte("ok"))
}
