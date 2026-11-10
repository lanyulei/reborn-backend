package server

import (
	"log/slog"

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
	"github.com/go-kratos/kratos/v3/transport/grpc"
)

// NewGRPCServer new a gRPC server.
// 中间件顺序与 HTTP 一致：recovery → tracing → logging → validate →
// rate-limit → auth → authz → 业务（auth 在 rate-limit 之后、authz 依赖
// auth 注入的 claims，理由见 http.go）。
func NewGRPCServer(c *conf.Server, logger *slog.Logger, user *service.UserService,
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
) *grpc.Server {
	opts := []grpc.ServerOption{
		grpc.Middleware(
			recovery.Recovery(),
			tracing.Server(),
			logging.Server(logger),
			validate.Validator(protoValidate),
			ratelimit.Server(),
			authMiddleware(signer),
			authzMiddleware(authzUC, logger),
		),
	}
	if c.Grpc.Network != "" {
		opts = append(opts, grpc.Network(c.Grpc.Network))
	}
	if c.Grpc.Addr != "" {
		opts = append(opts, grpc.Address(c.Grpc.Addr))
	}
	if c.Grpc.Timeout != nil {
		opts = append(opts, grpc.Timeout(c.Grpc.Timeout.AsDuration()))
	}
	srv := grpc.NewServer(opts...)
	v1.RegisterUserServiceServer(srv, user)
	v1.RegisterRoleServiceServer(srv, role)
	v1.RegisterAuthServiceServer(srv, auth)
	v1.RegisterSystemConfigServiceServer(srv, syscfg)
	v1.RegisterLoginLogServiceServer(srv, loginLog)
	notifyv1.RegisterNotifyChannelServiceServer(srv, notify)
	notifyv1.RegisterNotifyTemplateServiceServer(srv, tmpl)
	notifyv1.RegisterNotifyRouteServiceServer(srv, route)
	notifyv1.RegisterNotifySilenceServiceServer(srv, silence)
	notifyv1.RegisterNotifySubscriptionServiceServer(srv, subscription)
	notifyv1.RegisterNotifyProducerServiceServer(srv, producer)
	notifyv1.RegisterNotifyEventServiceServer(srv, event)
	notifyv1.RegisterNotifyDispatchServiceServer(srv, dispatch)
	notifyv1.RegisterNotifyDeliveryLogServiceServer(srv, deliveryLog)
	notifyv1.RegisterNotifyDeadLetterServiceServer(srv, deadLetter)
	return srv
}
