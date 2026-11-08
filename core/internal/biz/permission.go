package biz

import (
	"context"
	"fmt"
	"slices"
)

// PermissionPolicy 是一条 Casbin p 策略的领域表示：角色 code → 接口操作。
// 角色侧已由 data 层 JOIN 解析为 code（不可变），策略不感知角色 ID 变更。
type PermissionPolicy struct {
	RoleCode  string
	Operation string
}

// PermissionPolicySource 是策略加载仓储接口（全量），由 data 层实现（依赖倒置）。
type PermissionPolicySource interface {
	// ListPolicies 全量查询有效策略：JOIN 角色表解析 code，剔除已软删角色；
	// 不按角色 status 过滤——停用是否生效由判定链路按实时状态处理。
	ListPolicies(ctx context.Context) ([]PermissionPolicy, error)
}

// GrantSet 是角色直接授权集合：授权页显式勾选的菜单/页面元素/接口三类
// 资源。菜单/页面元素仅控制前端可见性；接口权限只来自 Apis（SetGrants
// 将其原样物化进策略表 system_permission），GrantSet 本身不参与 authz 判定。
type GrantSet struct {
	// Menus 菜单（页面）码列表。
	Menus []string
	// Elements 页面元素码列表。
	Elements []string
	// Apis 接口操作 code 直接授权列表。
	Apis []string
}

// normalized 返回三类各自去重、字典序排序后的副本（唯一索引兜底前先
// 应用侧归一，减少无效写）。
func (g GrantSet) normalized() GrantSet {
	return GrantSet{
		Menus:    sortedUnique(g.Menus),
		Elements: sortedUnique(g.Elements),
		Apis:     sortedUnique(g.Apis),
	}
}

// PermissionGrantReader 是角色直接授权读取仓储接口，由 data 层实现（依赖倒置）；
// 供授权管理与 /me 菜单元素汇总两个消费方共用。
type PermissionGrantReader interface {
	// ListGrantsByRole 查询角色三类直接授权（各自字典序）；角色存在性由
	// 用例层前置校验。
	ListGrantsByRole(ctx context.Context, roleID string) (GrantSet, error)
}

// PermissionAdminRepo 是角色权限管理仓储接口，由 data 层实现（依赖倒置）。
type PermissionAdminRepo interface {
	PermissionGrantReader
	// ReplaceForRole 事务内全量替换角色授权：直接绑定（system_grant）与物化
	// 策略（system_permission）同事务先物理删除再批量插入，两表原子一致。
	ReplaceForRole(ctx context.Context, roleID string, grants GrantSet, operations []string) error
}

// PermissionUsecase 是角色权限管理用例：授权 CRUD 与本实例快照刷新。
type PermissionUsecase struct {
	roles RoleReader
	admin PermissionAdminRepo
	authz *Authorizer
}

// NewPermissionUsecase new a PermissionUsecase.
func NewPermissionUsecase(roles RoleReader, admin PermissionAdminRepo, authz *Authorizer) *PermissionUsecase {
	return &PermissionUsecase{roles: roles, admin: admin, authz: authz}
}

// GetGrants 查询角色直接授权；角色不存在返回 ErrRoleNotFound。
func (uc *PermissionUsecase) GetGrants(ctx context.Context, roleID string) (GrantSet, error) {
	if _, err := uc.roles.GetRole(ctx, roleID); err != nil {
		return GrantSet{}, err
	}
	return uc.admin.ListGrantsByRole(ctx, roleID)
}

// SetGrants 全量覆盖角色直接授权并物化有效接口权限，随后刷新本实例策略
// 快照。物化口径：菜单/页面元素授权仅控制前端可见性（菜单显隐、按钮显隐），
// 不级联产生任何接口授权；接口权限只来自 apis 直接授权，system_permission
// 中的策略与 grants.Apis 一一对应。全量替换 + 重算使取消授权天然正确。
// 菜单/元素码经目录校验（ErrGrantInvalid）；接口码合法性（在可枚举集合内）
// 由 service 层前置校验。快照刷新用 WithoutCancel：授权已提交，刷新不得随
// 请求取消而跳过；刷新失败时其余实例仍在 policy_ttl 内收敛，状态不会永久陈旧。
func (uc *PermissionUsecase) SetGrants(ctx context.Context, roleID string, grants GrantSet) error {
	if _, err := uc.roles.GetRole(ctx, roleID); err != nil {
		return err
	}
	if err := ValidateGrantCodes(grants.Menus, grants.Elements); err != nil {
		return err
	}
	grants = grants.normalized()
	if err := uc.admin.ReplaceForRole(ctx, roleID, grants, grants.Apis); err != nil {
		return err
	}
	if err := uc.authz.Reload(context.WithoutCancel(ctx)); err != nil {
		return fmt.Errorf("reload permission policies: %w", err)
	}
	return nil
}

// sortedUnique 返回去重后的字典序副本；SetGrants 的入参归一化
// （(role_id, resource_type, resource_code) / (role_id, operation) 唯一
// 索引兜底，应用侧先行去重减少无效写）。
func sortedUnique(codes []string) []string {
	seen := make(map[string]struct{}, len(codes))
	uniq := make([]string, 0, len(codes))
	for _, code := range codes {
		if _, ok := seen[code]; ok {
			continue
		}
		seen[code] = struct{}{}
		uniq = append(uniq, code)
	}
	slices.Sort(uniq)
	return uniq
}
