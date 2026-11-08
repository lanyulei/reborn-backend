package server

import (
	"log/slog"

	v1 "github.com/lanyulei/monitor/backend/core/api/system/v1"
	"github.com/lanyulei/monitor/backend/core/internal/biz"
	"github.com/lanyulei/monitor/backend/core/internal/conf"
	"github.com/lanyulei/monitor/backend/core/internal/service"

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
	return srv
}
