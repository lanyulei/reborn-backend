package server

import (
	"log/slog"
	"time"

	assistantv1 "github.com/lanyulei/reborn/backend/core/api/assistant/v1"
	cmdbv1 "github.com/lanyulei/reborn/backend/core/api/cmdb/v1"
	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	v1 "github.com/lanyulei/reborn/backend/core/api/system/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	assistantservice "github.com/lanyulei/reborn/backend/core/internal/service/assistant"
	cmdbservice "github.com/lanyulei/reborn/backend/core/internal/service/cmdb"
	notifyservice "github.com/lanyulei/reborn/backend/core/internal/service/notify"
	service "github.com/lanyulei/reborn/backend/core/internal/service/system"
	"github.com/lanyulei/reborn/backend/kit/httpx"

	"github.com/go-kratos/kratos/contrib/otel/v3/tracing"
	"github.com/go-kratos/kratos/v3/middleware/logging"
	"github.com/go-kratos/kratos/v3/middleware/ratelimit"
	"github.com/go-kratos/kratos/v3/middleware/recovery"
	"github.com/go-kratos/kratos/v3/middleware/validate"
	"github.com/go-kratos/kratos/v3/transport/http"
)

// readTimeout 单个请求（头 + 体）的读取总时限。
const readTimeout = 60 * time.Second

// NewHTTPServer new an HTTP server.
// 中间件顺序固定：recovery → tracing → logging → validate →
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
	assistant *assistantservice.AssistantService, assistantChat *assistantservice.AssistantChatService,
	cmdbModel *cmdbservice.CmdbModelService, cmdbCI *cmdbservice.CmdbCIService, cmdbRelation *cmdbservice.CmdbRelationService,
	cmdbScope *cmdbservice.CmdbScopeService, cmdbSync *cmdbservice.CmdbSyncService, cmdbInsight *cmdbservice.CmdbInsightService,
	signer *biz.TokenSigner, authzUC *biz.AuthzUsecase,
) *http.Server {
	opts := []http.ServerOption{
		http.Middleware(
			recovery.Recovery(),
			tracing.Server(),
			logging.Server(logger),
			validate.Validator(httpx.ProtoValidate),
			ratelimit.Server(),
			authMiddleware(signer),
			operatorMiddleware(),
			authzMiddleware(authzUC, logger),
		),
		// body 8MiB 上限在路由前包裹；proto.Message 的 JSON 请求/响应统一走 protojson（kit/httpx）。
		http.Filter(httpx.LimitRequestBody),
		http.RequestDecoder(httpx.DecodeRequest),
		http.ResponseEncoder(httpx.EncodeResponse),
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
	httpx.HardenServer(srv) // ReadHeaderTimeout / IdleTimeout（Kratos 默认不设）
	// 请求头 + 请求体读取总时限（体上限 8MiB，头像/logo 上传足够）：防慢速
	// 请求体占住连接。不设 WriteTimeout——助手 SSE 为分钟级长响应（其读截止
	// 时间在进入流式阶段前清除，见 assistant.go）。
	srv.ReadTimeout = readTimeout
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
	assistantv1.RegisterAssistantServiceHTTPServer(srv, assistant)
	// AssistantChatService 的 Chat/Confirm 为 SSE 流式，不走生成注册（见 assistant.go）。
	registerAssistantStream(srv, assistantChat)
	cmdbv1.RegisterCmdbModelServiceHTTPServer(srv, cmdbModel)
	cmdbv1.RegisterCmdbCIServiceHTTPServer(srv, cmdbCI)
	cmdbv1.RegisterCmdbRelationServiceHTTPServer(srv, cmdbRelation)
	cmdbv1.RegisterCmdbScopeServiceHTTPServer(srv, cmdbScope)
	cmdbv1.RegisterCmdbSyncServiceHTTPServer(srv, cmdbSync)
	cmdbv1.RegisterCmdbInsightServiceHTTPServer(srv, cmdbInsight)
	return srv
}
