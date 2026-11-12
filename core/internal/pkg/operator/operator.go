// Package operator 在请求上下文里传递「当前操作者用户 ID」：由传输层鉴权后注入，
// data 层写审计列（create_user/update_user/delete_user）时读取。中性包：不依赖
// 传输层与 JWT，biz/data 均可 import（data 不得触碰 claims）。
package operator

import "context"

type ctxKey struct{}

// WithID 注入操作者 ID（空串等价于未注入）。
func WithID(ctx context.Context, userID string) context.Context {
	if userID == "" {
		return ctx
	}
	return context.WithValue(ctx, ctxKey{}, userID)
}

// FromContext 取操作者 ID；未注入（匿名接口、内部任务、测试）返回空串。
func FromContext(ctx context.Context) string {
	id, _ := ctx.Value(ctxKey{}).(string)
	return id
}
