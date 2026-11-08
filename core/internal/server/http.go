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
	"github.com/go-kratos/kratos/v3/transport/http"
)

// NewHTTPServer new an HTTP server.
// 中间件顺序固定（与 gRPC 一致）：recovery → tracing → logging → validate →
// rate-limit → auth → authz → 业务。auth 在 rate-limit 之后：登录含 Argon2id
// （单次几十毫秒的显著 CPU 成本），限流必须先于认证与密码校验执行以防 DoS；
// authz 在 auth 之后：授权判定依赖鉴权注入的 claims（deny-by-default）。
func NewHTTPServer(c *conf.Server, logger *slog.Logger, user *service.UserService,
	role *service.RoleService, auth *service.AuthService, syscfg *service.SystemConfigService,
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
	v1.RegisterUserServiceHTTPServer(srv, user)
	v1.RegisterRoleServiceHTTPServer(srv, role)
	v1.RegisterAuthServiceHTTPServer(srv, auth)
	v1.RegisterSystemConfigServiceHTTPServer(srv, syscfg)
	return srv
}
