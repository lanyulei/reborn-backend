package service

import (
	"context"

	bizsystem "github.com/lanyulei/reborn/backend/core/internal/biz/system"

	jwtx "github.com/go-kratos/kratos/contrib/middleware/jwt/v3"
	"github.com/golang-jwt/jwt/v5"
)

// testOperatorID 是 service 单测的固定操作者（biz 范围由 testfake.BizScopeStub 决定）。
const testOperatorID = "018f3e1a-7b1c-7cc1-9d2a-3f5e7a9b9001"

// testCtx 模拟鉴权中间件注入 claims 后的请求 ctx（守卫经 UserIDFromContext 取身份）。
func testCtx() context.Context {
	return jwtx.NewContext(context.Background(), &bizsystem.AccessClaims{
		RegisteredClaims: jwt.RegisteredClaims{Subject: testOperatorID},
	})
}
