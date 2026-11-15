// Package service 值班域 proto 接口实现：只做身份提取、参数编排与模型转换，
// 业务规则（含分组管理员校验）全部收敛在 biz。
package service

import (
	"context"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/oncall"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"

	"github.com/google/wire"
)

// ProviderSet is 值班域 service providers。
// AdminResolver 由 system 域 UserUsecase 实现，在组合根 cmd/server/wire.go 绑定。
var ProviderSet = wire.NewSet(NewOncallGroupService, NewOncallScheduleService, NewOncallShareService)

// AdminResolver 判定操作者是否超级管理员（超管可管理任意分组）。
type AdminResolver interface {
	IsAdminUser(ctx context.Context, id string) (bool, error)
}

// actorFromContext 取操作者（JWT 用户 ID）并解析是否超级管理员；缺少身份按无效令牌拒绝，
// 操作者不存在按仓储错误透传（fail closed）。
func actorFromContext(ctx context.Context, admin AdminResolver) (biz.Actor, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return biz.Actor{}, err
	}
	isAdmin, err := admin.IsAdminUser(ctx, userID)
	if err != nil {
		return biz.Actor{}, err
	}
	return biz.Actor{UserID: userID, IsAdmin: isAdmin}, nil
}
