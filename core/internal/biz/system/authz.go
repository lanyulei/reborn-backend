package biz

import (
	"context"
	"maps"
	"slices"

	"github.com/go-kratos/kratos/v3/errors"
)

// AuthzUsecase 是接口授权判定用例：组合实时用户/角色数据与内存策略快照。
type AuthzUsecase struct {
	users  UserReader
	roles  UserRoleRepo
	grants PermissionGrantReader
	authz  *Authorizer
}

// NewAuthzUsecase new a AuthzUsecase.
func NewAuthzUsecase(users UserReader, roles UserRoleRepo, grants PermissionGrantReader, authz *Authorizer) *AuthzUsecase {
	return &AuthzUsecase{users: users, roles: roles, grants: grants, authz: authz}
}

// Allowed 判定用户是否可执行操作，按序检查：
//  1. 用户不存在（含已软删）：(false, nil)——令牌虽有效但主体已消失，拒绝；
//  2. is_admin 超级管理员：true（跳过接口权限认证的既定需求）；
//  3. 多角色并集：任一启用角色（status=1）的策略命中即放行，停用角色不生效；
//  4. 全未命中：(false, nil)。
//
// operationCode 是操作的稳定标识（OperationCode 派生；策略存储同口径），
// biz 层不感知 proto full method 与派生规则。error 非 nil 表示仓储或求值器
// 故障，调用方应 500 fail closed，禁止放行。用户绑定与角色状态每请求实时
// 读库（多实例一致）；仅角色→操作策略走内存快照。
func (uc *AuthzUsecase) Allowed(ctx context.Context, userID, operationCode string) (bool, error) {
	isAdmin, roleCodes, err := uc.enabledRoleCodes(ctx, userID)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			return false, nil
		}
		return false, err
	}
	if isAdmin {
		return true, nil
	}
	for _, roleCode := range roleCodes {
		ok, err := uc.authz.Enforce(ctx, roleCode, operationCode)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

// EnabledRoleIDs 返回用户启用角色 ID（跨域窄接口 cmdb.RoleSource 的实现）：
// 超管 (true, nil)；非可用用户 (false, 空)。
func (uc *AuthzUsecase) EnabledRoleIDs(ctx context.Context, userID string) (bool, []string, error) {
	isAdmin, roles, err := uc.enabledRoles(ctx, userID)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			return false, nil, nil
		}
		return false, nil, err
	}
	if isAdmin {
		return true, nil, nil
	}
	ids := make([]string, 0, len(roles))
	for _, r := range roles {
		ids = append(ids, r.ID)
	}
	return false, ids, nil
}

// EffectiveOperations 返回用户当前有效授权操作 code 并集（字典序），供
// GetLoginProfile 暴露给前端做菜单/路由可见性，与 Allowed 同一判定口径：
// 用户不存在（含已软删）→ 空集；is_admin 超管 → 空集（直通，无需枚举，
// 前端按 is_admin 放行）；否则全部启用角色（status=1）的策略并集。
// error 非 nil 表示仓储或求值器故障，调用方应 500 fail closed。
func (uc *AuthzUsecase) EffectiveOperations(ctx context.Context, userID string) ([]string, error) {
	isAdmin, roleCodes, err := uc.enabledRoleCodes(ctx, userID)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			return []string{}, nil
		}
		return nil, err
	}
	if isAdmin {
		return []string{}, nil
	}
	return uc.authz.OperationsForRoles(ctx, roleCodes)
}

// EffectiveGrants 返回用户全部启用角色直接授权（菜单/元素码）的并集（各自
// 字典序去重），供 GetLoginProfile 暴露给前端做菜单与页面元素可见性；与
// Allowed 同口径：用户不存在（含已软删）→ 空集；is_admin 超管 → 空集
// （前端按 is_admin 放行）。停用角色的直接授权不参与并集。
func (uc *AuthzUsecase) EffectiveGrants(ctx context.Context, userID string) (menus []string, elements []string, err error) {
	isAdmin, roles, err := uc.enabledRoles(ctx, userID)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			return []string{}, []string{}, nil
		}
		return nil, nil, err
	}
	if isAdmin {
		return []string{}, []string{}, nil
	}
	if len(roles) == 0 {
		return []string{}, []string{}, nil
	}
	roleIDs := make([]string, 0, len(roles))
	for _, role := range roles {
		roleIDs = append(roleIDs, role.ID)
	}
	// 单条查询取全部角色授权并集（/me 每次登录态刷新都走，按角色逐条查询
	// 是 N+1）。
	grants, err := uc.grants.ListGrantsByRoles(ctx, roleIDs)
	if err != nil {
		return nil, nil, err
	}
	menuSet := make(map[string]struct{}, len(grants.Menus))
	elementSet := make(map[string]struct{}, len(grants.Elements))
	for _, m := range grants.Menus {
		menuSet[m] = struct{}{}
	}
	for _, e := range grants.Elements {
		elementSet[e] = struct{}{}
	}
	return slices.Sorted(maps.Keys(menuSet)), slices.Sorted(maps.Keys(elementSet)), nil
}

// BizScope 返回用户可管理的通知 biz 范围（通知控制面按 biz 隔离，跨域窄接口
// biznotify.BizScopeResolver 的实现）：超管、用户不存在/非可用之外任一启用角色
// biz_scope 为空 → 不限（unrestricted=true，现有角色缺省即此）；否则取启用角色
// scope 并集（字典序去重）。用户不存在/非可用 → 受限且范围为空（什么都不能管）。
func (uc *AuthzUsecase) BizScope(ctx context.Context, userID string) (unrestricted bool, scope []string, err error) {
	isAdmin, roles, err := uc.enabledRoles(ctx, userID)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			return false, []string{}, nil
		}
		return false, nil, err
	}
	if isAdmin {
		return true, nil, nil
	}
	set := make(map[string]struct{}, 8)
	for _, role := range roles {
		if len(role.BizScope) == 0 {
			return true, nil, nil
		}
		for _, b := range role.BizScope {
			set[b] = struct{}{}
		}
	}
	return false, slices.Sorted(maps.Keys(set)), nil
}

// enabledRoles 是 Allowed/EffectiveOperations/EffectiveGrants 的公共判定前段
// （AGENTS 4.5：同一逻辑第二份即提取，保证各处口径永不漂移）：
// 用户不存在（含已软删）→ ErrUserNotFound（语义由调用方收口）；
// 用户非可用状态（停用/暂停/离职）→ (false, 空)：尚未过期的 access token
// 不再获得任何授权（含超管直通），账号停用即刻生效而非等到令牌过期；
// is_admin 超管 → (true, nil)，不读角色绑定（直通短路）；
// 其余 → (false, 启用角色列表)。error 仅仓储故障。
func (uc *AuthzUsecase) enabledRoles(ctx context.Context, userID string) (bool, []*Role, error) {
	user, err := uc.users.GetUser(ctx, userID)
	if err != nil {
		return false, nil, err
	}
	if user.Status != StatusEnabled {
		return false, nil, nil
	}
	if user.IsAdmin {
		return true, nil, nil
	}
	roles, err := uc.roles.ListUserRoles(ctx, userID)
	if err != nil {
		return false, nil, err
	}
	enabled := make([]*Role, 0, len(roles))
	for _, role := range roles {
		if role.Status == RoleStatusEnabled {
			enabled = append(enabled, role)
		}
	}
	return false, enabled, nil
}

// enabledRoleCodes 由启用角色列表派生 code 列表（Allowed/EffectiveOperations
// 的 casbin subject 口径）。
func (uc *AuthzUsecase) enabledRoleCodes(ctx context.Context, userID string) (bool, []string, error) {
	isAdmin, roles, err := uc.enabledRoles(ctx, userID)
	if err != nil || isAdmin {
		return isAdmin, nil, err
	}
	codes := make([]string, 0, len(roles))
	for _, role := range roles {
		codes = append(codes, role.Code)
	}
	return false, codes, nil
}
