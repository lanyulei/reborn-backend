package server

import (
	"log/slog"
	"time"

	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	v1 "github.com/lanyulei/reborn/backend/core/api/system/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	notifyservice "github.com/lanyulei/reborn/backend/core/internal/service/notify"
	service "github.com/lanyulei/reborn/backend/core/internal/service/system"

	"github.com/go-kratos/kratos/contrib/otel/v3/tracing"
	"github.com/go-kratos/kratos/v3/middleware/logging"
	"github.com/go-kratos/kratos/v3/middleware/ratelimit"
	"github.com/go-kratos/kratos/v3/middleware/recovery"
	"github.com/go-kratos/kratos/v3/middleware/validate"
	"github.com/go-kratos/kratos/v3/transport/http"
)

// 连接级超时（Kratos 默认全不设，仅有请求 ctx 超时——而 ctx 超时覆盖不到
// 请求头/请求体的网络读取阶段）：ReadHeaderTimeout 防慢速头部攻击占满
// 连接（slowloris）；IdleTimeout 回收空闲 keep-alive 连接。请求体读取由
// limitRequestBody 的大小上限与反向代理层超时共同约束。
const (
	httpReadHeaderTimeout = 10 * time.Second
	httpIdleTimeout       = 120 * time.Second
)

// NewHTTPServer new an HTTP server.
// 中间件顺序固定（与 gRPC 一致）：recovery → tracing → logging → validate →
// rate-limit → auth → authz → 业务。auth 在 rate-limit 之后：登录含 Argon2id
// （单次几十毫秒的显著 CPU 成本），限流必须先于认证与密码校验执行以防 DoS；
// authz 在 auth 之后：授权判定依赖鉴权注入的 claims（deny-by-default）。
func NewHTTPServer(c *conf.Server, logger *slog.Logger, user *service.UserService,
	role *service.RoleService, auth *service.AuthService, syscfg *service.SystemConfigService,
	loginLog *service.LoginLogService, notify *notifyservice.NotifyChannelService,
	tmpl *notifyservice.NotifyTemplateService, route *notifyservice.NotifyRouteService,
	silence *notifyservice.NotifySilenceService,
	subscription *notifyservice.NotifySubscriptionService,
	producer *notifyservice.NotifyProducerService,
	event *notifyservice.NotifyEventService,
	dispatch *notifyservice.NotifyDispatchService,
	deliveryLog *notifyservice.NotifyDeliveryLogService,
	deadLetter *notifyservice.NotifyDeadLetterService,
	signer *biz.TokenSigner, authzUC *biz.AuthzUsecase,
) *http.Server {
	opts := []http.ServerOption{
		http.Middleware(
			recovery.Recovery(),
			tracing.Server(),
			logging.Server(logger),
			validate.Validator(protoValidate),
			ratelimit.Server(),
			authMiddleware(signer),
			authzMiddleware(authzUC, logger),
		),
		// body 大小上限在路由前包裹（见 codec.go limitRequestBody）。
		http.Filter(limitRequestBody),
		// proto.Message 的 JSON 请求/响应统一走 protojson（见 codec.go）。
		http.RequestDecoder(decodeRequest),
		http.ResponseEncoder(encodeResponse),
	}
	if c.Http.Network != "" {
		opts = append(opts, http.Network(c.Http.Network))
	}
	if c.Http.Addr != "" {
		opts = append(opts, http.Address(c.Http.Addr))
	}
	if c.Http.Timeout != nil {
		opts = append(opts, http.Timeout(c.Http.Timeout.AsDuration()))
	}
	srv := http.NewServer(opts...)
	srv.ReadHeaderTimeout = httpReadHeaderTimeout
	srv.IdleTimeout = httpIdleTimeout
	v1.RegisterUserServiceHTTPServer(srv, user)
	v1.RegisterRoleServiceHTTPServer(srv, role)
	v1.RegisterAuthServiceHTTPServer(srv, auth)
	v1.RegisterSystemConfigServiceHTTPServer(srv, syscfg)
	v1.RegisterLoginLogServiceHTTPServer(srv, loginLog)
	notifyv1.RegisterNotifyChannelServiceHTTPServer(srv, notify)
	notifyv1.RegisterNotifyTemplateServiceHTTPServer(srv, tmpl)
	notifyv1.RegisterNotifyRouteServiceHTTPServer(srv, route)
	notifyv1.RegisterNotifySilenceServiceHTTPServer(srv, silence)
	notifyv1.RegisterNotifySubscriptionServiceHTTPServer(srv, subscription)
	notifyv1.RegisterNotifyProducerServiceHTTPServer(srv, producer)
	notifyv1.RegisterNotifyEventServiceHTTPServer(srv, event)
	notifyv1.RegisterNotifyDispatchServiceHTTPServer(srv, dispatch)
	notifyv1.RegisterNotifyDeliveryLogServiceHTTPServer(srv, deliveryLog)
	notifyv1.RegisterNotifyDeadLetterServiceHTTPServer(srv, deadLetter)
	return srv
}
