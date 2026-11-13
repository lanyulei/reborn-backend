package server

import (
	"context"
	"log/slog"

	v1 "github.com/lanyulei/reborn/backend/core/api/system/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	service "github.com/lanyulei/reborn/backend/core/internal/service/system"

	"github.com/go-kratos/kratos/v3/errors"
	"github.com/go-kratos/kratos/v3/middleware"
	"github.com/go-kratos/kratos/v3/transport"
)

// authzMiddleware 组装授权中间件（挂 authMiddleware 之后）：deny-by-default，
// is_admin 超级管理员直通，多个启用角色的策略并集放行。存储或求值器故障
// 一律 500 fail closed，禁止放行；明确无权限返回 403 PERMISSION_DENIED。
// 主体（用户 ID）取自鉴权中间件注入的 claims；操作取自 transport 层
// （HTTP 生成代码 SetOperation 的 proto full method），经 biz.OperationCode
// 换算为稳定 code 后判定（与授权存储
// 同口径）。免授权方法集见 service.IsAuthzExempt（与操作枚举共享同一事实源）：
// 其中匿名接口直接放行，已登录的自助接口仍校验账号存在且可用。
func authzMiddleware(uc *biz.AuthzUsecase, logger *slog.Logger) middleware.Middleware {
	return func(handler middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req any) (any, error) {
			tr, ok := transport.FromServerContext(ctx)
			exempt := ok && service.IsAuthzExempt(tr.Operation())
			userID, err := service.UserIDFromContext(ctx)
			if err != nil {
				if exempt {
					// 免鉴权的匿名接口（登录/刷新/静态资源）：无 claims，直接放行。
					return handler(ctx, req)
				}
				// 鉴权中间件在前，正常不会走到；claims 异常按无效令牌透传哨兵。
				return nil, err
			}
			if !ok || tr.Operation() == "" {
				logger.ErrorContext(ctx, "authz: missing operation in transport context")
				return nil, v1.ErrorAuthUnspecified("authorization check failed")
			}
			// 请求级主体缓存：本次判定与 handler 内的后续授权查询共享一次读库。
			ctx = biz.WithRequestCache(ctx)
			if exempt {
				// 免授权但需登录的自助接口（/me、登出、头像上传）：不判接口
				// 权限，但已停用/已删除账号的未过期 access token 同样拒绝。
				active, err := uc.Active(ctx, userID)
				switch {
				case errors.Is(err, biz.ErrUserNotFound):
					return nil, biz.ErrTokenInvalid
				case err != nil:
					logger.ErrorContext(ctx, "authz check failed", "error", err)
					return nil, v1.ErrorAuthUnspecified("authorization check failed")
				case !active:
					return nil, biz.ErrAccountDisabled
				}
				return handler(ctx, req)
			}
			allowed, err := uc.Allowed(ctx, userID, biz.OperationCode(tr.Operation()))
			if err != nil {
				logger.ErrorContext(ctx, "authz check failed", "error", err)
				return nil, v1.ErrorAuthUnspecified("authorization check failed")
			}
			if !allowed {
				return nil, v1.ErrorPermissionDenied("permission denied")
			}
			return handler(ctx, req)
		}
	}
}
