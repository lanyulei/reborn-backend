package biz

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
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
		Menus:    SortedUnique(g.Menus),
		Elements: SortedUnique(g.Elements),
		Apis:     SortedUnique(g.Apis),
	}
}

// PermissionGrantReader 是角色直接授权读取仓储接口，由 data 层实现（依赖倒置）；
// 供授权管理与 /me 菜单元素汇总两个消费方共用。
type PermissionGrantReader interface {
	// ListGrantsByRole 查询角色三类直接授权（各自字典序）；角色存在性由
	// 用例层前置校验。
	ListGrantsByRole(ctx context.Context, roleID string) (GrantSet, error)
	// ListGrantsByRoles 单条查询多个角色直接授权的并集（三类各自去重、
	// 字典序）；空输入返回空集。供 /me 汇总避免按角色 N+1。
	ListGrantsByRoles(ctx context.Context, roleIDs []string) (GrantSet, error)
}

// PermissionAdminRepo 是角色权限管理仓储接口，由 data 层实现（依赖倒置）。
type PermissionAdminRepo interface {
	PermissionGrantReader
	// ReplaceForRole 在调用方事务内全量替换角色授权：直接绑定（system_grant）
	// 与物化策略（system_permission）先物理删除再插入，两表原子一致。事务
	// 句柄由用例层传入（txrun.Tx）——业务写与通知事件写同事务原子提交
	//（notify 设计 §11），仓储自身不再开事务。
	ReplaceForRole(ctx context.Context, tx txrun.Tx, roleID string, grants GrantSet, operations []string) error
}

// PermissionUsecase 是角色权限管理用例：授权 CRUD 与本实例快照刷新。
type PermissionUsecase struct {
	roles  RoleReader
	admin  PermissionAdminRepo
	authz  *Authorizer
	notify biznotify.NotificationWriter
	txs    txrun.Manager
}

// NewPermissionUsecase new a PermissionUsecase；notify/txs 用于权限变更
// 事件同事务直插（设计 §11，经顶层 wire.Bind 装配 notify 实现）。
func NewPermissionUsecase(roles RoleReader, admin PermissionAdminRepo, authz *Authorizer, notify biznotify.NotificationWriter, txs txrun.Manager) *PermissionUsecase {
	return &PermissionUsecase{roles: roles, admin: admin, authz: authz, notify: notify, txs: txs}
}

// GetGrants 查询角色直接授权；角色不存在返回 ErrRoleNotFound。
func (uc *PermissionUsecase) GetGrants(ctx context.Context, roleID string) (GrantSet, error) {
	if _, err := uc.roles.GetRole(ctx, roleID); err != nil {
		return GrantSet{}, err
	}
	return uc.admin.ListGrantsByRole(ctx, roleID)
}

// 权限变更通知事件常量（设计 §11 示例）：事件经 NotificationWriter 在
// ReplaceForRole 同事务直插（source=internal），管道侧按 notify 管理面的
// 路由/模板配置接管投递（路由须建在 biz "core" 下并覆盖该 event_type）。
const (
	// eventTypePermissionChange 权限变更事件类型。
	eventTypePermissionChange = "permission.change"
	// notifyBizCore core 平台自身事件的业务命名空间（与租户业务隔离）。
	notifyBizCore = "core"
	// permissionChangeGroupKeyPrefix 权限变更聚合前缀（按角色分组）。
	permissionChangeGroupKeyPrefix = "permission.change."
	// permissionChangeTitle 权限变更事件标题。
	permissionChangeTitle = "角色权限变更"
)

// SetGrants 全量覆盖角色直接授权并物化有效接口权限，并在同一事务内直插
// permission.change 通知事件（operator 为触发者用户 ID，可空），随后刷新
// 本实例策略快照。物化口径：菜单/页面元素授权仅控制前端可见性（菜单显隐、
// 按钮显隐），不级联产生任何接口授权；接口权限只来自 apis 直接授权，
// system_permission 中的策略与 grants.Apis 一一对应。全量替换 + 重算使
// 取消授权天然正确。菜单/元素码经目录校验（ErrGrantInvalid）；接口码
// 合法性（在可枚举集合内）由 service 层前置校验。事件写失败则整个事务
// 回滚——授权变更与通知永不脱节（设计 §11）。快照刷新用 WithoutCancel：
// 授权已提交，刷新不得随请求取消而跳过；刷新失败时其余实例仍在
// policy_ttl 内收敛，状态不会永久陈旧。
func (uc *PermissionUsecase) SetGrants(ctx context.Context, roleID string, grants GrantSet, operator string) error {
	if _, err := uc.roles.GetRole(ctx, roleID); err != nil {
		return err
	}
	if err := ValidateGrantCodes(grants.Menus, grants.Elements); err != nil {
		return err
	}
	grants = grants.normalized()

	payload := map[string]any{"role_id": roleID}
	if operator != "" {
		payload["operator"] = operator
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode permission.change payload: %w", err)
	}
	event := biznotify.Event{
		BizID:     notifyBizCore,
		EventType: eventTypePermissionChange,
		Severity:  2,
		Title:     permissionChangeTitle,
		Payload:   raw,
		GroupKey:  permissionChangeGroupKeyPrefix + roleID,
	}
	if err := uc.txs.WithinTx(ctx, func(tx txrun.Tx) error {
		if err := uc.admin.ReplaceForRole(ctx, tx, roleID, grants, grants.Apis); err != nil {
			return err
		}
		_, err := uc.notify.NotifyEvent(ctx, tx, event)
		return err
	}); err != nil {
		return err
	}
	// 授权已提交：刷新失败不得把成功的变更报成 500（客户端重试会重复
	// 写入/误判），记日志后由 policy_ttl 收敛。
	uc.authz.ReloadAfterCommit(context.WithoutCancel(ctx))
	return nil
}

// SortedUnique 返回去重后的字典序副本（testfake 亦复用）；SetGrants 的入参归一化
// （(role_id, resource_type, resource_code) / (role_id, operation) 唯一
// 索引兜底，应用侧先行去重减少无效写）。
func SortedUnique(codes []string) []string {
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
