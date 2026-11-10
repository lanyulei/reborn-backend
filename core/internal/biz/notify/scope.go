package biz

import (
	"context"

	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"

	"github.com/go-kratos/kratos/v3/errors"
)

// BizScopeResolver 跨域窄接口：解析操作者可管理的通知 biz 范围（system 域
// AuthzUsecase 实现，组合根 wire.Bind 绑定）。unrestricted=true 表示不限
// （超管或任一启用角色 biz_scope 为空——现有角色缺省），此时 scope 为空。
type BizScopeResolver interface {
	BizScope(ctx context.Context, userID string) (unrestricted bool, scope []string, err error)
}

// biz 范围哨兵错误。
var (
	// ErrBizForbidden 目标 biz 不在操作者可管理范围。
	ErrBizForbidden = errors.Forbidden(notifyv1.ErrorReason_NOTIFY_BIZ_FORBIDDEN.String(), "biz is outside the operator's manageable scope")
	// ErrBizRequired biz 受限用户的列表查询必须携带 biz（或 channel_id）过滤。
	ErrBizRequired = errors.BadRequest(notifyv1.ErrorReason_NOTIFY_BIZ_REQUIRED.String(), "biz filter is required for scoped operators")
)
