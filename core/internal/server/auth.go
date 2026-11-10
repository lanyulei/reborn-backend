package server

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/system/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"

	jwtx "github.com/go-kratos/kratos/contrib/middleware/jwt/v3"
	"github.com/go-kratos/kratos/v3/errors"
	"github.com/go-kratos/kratos/v3/middleware"
	"github.com/go-kratos/kratos/v3/middleware/selector"
)

// authExempt 是免鉴权方法集（proto full method）：HTTP 侧生成代码在进入中间件
// 链前先 SetOperation，gRPC 侧拦截器以 info.FullMethod 为 operation，两协议的
// transport operation 同为 proto full method，故共用一份白名单。
// 引用生成常量而非手写字符串，签名漂移时在编译期暴露。
var authExempt = map[string]struct{}{
	v1.OperationAuthServiceLogin:        {},
	v1.OperationAuthServiceRefreshToken: {},
	// 查询系统配置：登录页登录前需拉取标题/logo，匿名可读（仅暴露
	// 展示字段）；更新接口不免检。
	v1.OperationSystemConfigServiceGetSystemConfig: {},
	// 查看 logo 文件：登录页 <img> 直载（无 Authorization 头），匿名
	// 可读；上传接口不免检。
	v1.OperationSystemConfigServiceGetSystemLogo: {},
}

// authMiddleware 组装鉴权中间件：白名单外的全部方法要求 Authorization: Bearer
// 访问令牌（JWT/HS256，校验集与签发侧完全一致）。
func authMiddleware(signer *biz.TokenSigner) middleware.Middleware {
	jwtMW := jwtx.Server(
		signer.KeyFunc(),
		jwtx.WithClaims(signer.ClaimsFactory()),
		jwtx.WithParserOptions(signer.ParserOptions()...),
	)
	return translateAuthError(selector.Server(jwtMW).
		Match(func(_ context.Context, operation string) bool {
			_, exempt := authExempt[operation]
			return !exempt
		}).
		Build())
}

// translateAuthError 将 jwt 中间件的内部哨兵错误翻译为 proto ErrorReason，
// 保证对外 401 错误契约仍以 proto 枚举为单一事实来源：过期 → TOKEN_EXPIRED
// （客户端可凭刷新令牌静默换发）；其余令牌问题 → TOKEN_INVALID（强制重新登录）；
// 非认证错误原样透传。
// 判别必须用指针同一性而非 errors.Is：jwt 哨兵错误的 (code, reason) 全部相同
// （401, "UNAUTHORIZED"），kratos errors.Is 按 (code, reason) 判等，任何两个
// 哨兵之间都会判为相等、无法区分形态；中间件返回哨兵本身且到本层之间无包装。
func translateAuthError(mw middleware.Middleware) middleware.Middleware {
	return func(handler middleware.Handler) middleware.Handler {
		inner := mw(handler)
		return func(ctx context.Context, req any) (any, error) {
			resp, err := inner(ctx, req)
			if err == nil {
				return resp, nil
			}
			switch err {
			case jwtx.ErrTokenExpired:
				return nil, errors.Unauthorized(v1.ErrorReason_TOKEN_EXPIRED.String(), "token has expired")
			case jwtx.ErrMissingJwtToken,
				jwtx.ErrTokenInvalid,
				jwtx.ErrTokenParseFail,
				jwtx.ErrUnSupportSigningMethod,
				jwtx.ErrWrongContext,
				jwtx.ErrMissingKeyFunc:
				return nil, errors.Unauthorized(v1.ErrorReason_TOKEN_INVALID.String(), "token is invalid")
			}
			return resp, err
		}
	}
}
