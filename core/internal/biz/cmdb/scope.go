package biz

import (
	"context"
	"slices"
	"time"
)

// RoleScope 角色数据范围；ModelCodes / Envs 空 = 该维度不限。
type RoleScope struct {
	RoleID     string
	ModelCodes []string
	Envs       []string
	UpdateUser string
	UpdateTime time.Time
}

// Scope 用户有效数据范围（启用角色范围的并集）。
type Scope struct {
	// Unrestricted 不限：超管，或任一启用角色未配置 / 类型范围为空。
	Unrestricted bool
	// Models 可见类型集合。
	Models map[string]struct{}
	// Envs 可见环境集合；EnvUnrestricted=true 时忽略。
	Envs            map[string]struct{}
	EnvUnrestricted bool
}

// AllowsModel 报告类型是否可见。
func (s Scope) AllowsModel(code string) bool {
	if s.Unrestricted {
		return true
	}
	_, ok := s.Models[code]
	return ok
}

// AllowsEnv 报告环境是否可见（空环境视为"未分环境"，仅环境不限时可见）。
func (s Scope) AllowsEnv(env string) bool {
	if s.Unrestricted || s.EnvUnrestricted {
		return true
	}
	_, ok := s.Envs[env]
	return ok
}

// Allows 报告资源是否在范围内。
func (s Scope) Allows(modelCode, env string) bool {
	return s.AllowsModel(modelCode) && s.AllowsEnv(env)
}

// ScopeRepo 角色范围仓储接口。
type ScopeRepo interface {
	// ListByRoles 按角色 ID 读取已配置的范围（未配置的角色不在结果里）。
	ListByRoles(ctx context.Context, roleIDs []string) ([]*RoleScope, error)
	ListAll(ctx context.Context) ([]*RoleScope, error)
	Upsert(ctx context.Context, scope *RoleScope) error
}

// RoleSource 跨域窄接口：用户启用角色（system 域 AuthzUsecase 实现）。
type RoleSource interface {
	// EnabledRoleIDs 超管返回 (true, nil)；非可用用户返回 (false, 空)。
	EnabledRoleIDs(ctx context.Context, userID string) (isAdmin bool, roleIDs []string, err error)
}

// RoleDirectory 跨域窄接口：全部未删除角色的 ID/名称/编码（system 域 RoleUsecase 实现）。
type RoleDirectory interface {
	ListRoleRefs(ctx context.Context) (ids, names, codes []string, err error)
}

// ScopeUsecase 数据范围用例：解析用户有效范围、维护角色范围。
type ScopeUsecase struct {
	scopes ScopeRepo
	roles  RoleSource
	dir    RoleDirectory
	models ModelRepo
}

// NewScopeUsecase new a ScopeUsecase.
func NewScopeUsecase(scopes ScopeRepo, roles RoleSource, dir RoleDirectory, models ModelRepo) *ScopeUsecase {
	return &ScopeUsecase{scopes: scopes, roles: roles, dir: dir, models: models}
}

// Resolve 解析用户有效范围：超管不限；任一启用角色未配置或类型范围为空 → 不限；
// 否则类型为各角色并集，环境同理（任一角色环境为空 → 环境不限）。
// 非可用用户 / 无启用角色 → 受限且为空（什么都看不到；接口权限层通常已拒绝）。
func (uc *ScopeUsecase) Resolve(ctx context.Context, userID string) (Scope, error) {
	isAdmin, roleIDs, err := uc.roles.EnabledRoleIDs(ctx, userID)
	if err != nil {
		return Scope{}, err
	}
	if isAdmin {
		return Scope{Unrestricted: true}, nil
	}
	scope := Scope{Models: map[string]struct{}{}, Envs: map[string]struct{}{}}
	if len(roleIDs) == 0 {
		return scope, nil
	}
	configured, err := uc.scopes.ListByRoles(ctx, roleIDs)
	if err != nil {
		return Scope{}, err
	}
	if len(configured) < len(roleIDs) {
		return Scope{Unrestricted: true}, nil // 存在未配置的启用角色
	}
	for _, rs := range configured {
		if len(rs.ModelCodes) == 0 {
			return Scope{Unrestricted: true}, nil
		}
		for _, code := range rs.ModelCodes {
			scope.Models[code] = struct{}{}
		}
		if len(rs.Envs) == 0 {
			scope.EnvUnrestricted = true
		}
		for _, env := range rs.Envs {
			scope.Envs[env] = struct{}{}
		}
	}
	return scope, nil
}

// ListRoleScopes 全部角色及其范围（未配置 = 空）。
func (uc *ScopeUsecase) ListRoleScopes(ctx context.Context) (ids, names, codes []string, scopes map[string]*RoleScope, err error) {
	ids, names, codes, err = uc.dir.ListRoleRefs(ctx)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	all, err := uc.scopes.ListAll(ctx)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	scopes = make(map[string]*RoleScope, len(all))
	for _, rs := range all {
		scopes[rs.RoleID] = rs
	}
	return ids, names, codes, scopes, nil
}

// SetRoleScope 设置角色范围：角色须存在，类型编码须存在。
func (uc *ScopeUsecase) SetRoleScope(ctx context.Context, actor Actor, roleID string, modelCodes, envs []string) (*RoleScope, error) {
	ids, _, _, err := uc.dir.ListRoleRefs(ctx)
	if err != nil {
		return nil, err
	}
	if !slices.Contains(ids, roleID) {
		return nil, ErrRoleNotFound
	}
	for _, code := range modelCodes {
		if _, err := uc.models.Get(ctx, code); err != nil {
			return nil, err
		}
	}
	rs := &RoleScope{RoleID: roleID, ModelCodes: slices.Clone(modelCodes), Envs: slices.Clone(envs), UpdateUser: actor.UserID, UpdateTime: time.Now()}
	if rs.ModelCodes == nil {
		rs.ModelCodes = []string{}
	}
	if rs.Envs == nil {
		rs.Envs = []string{}
	}
	if err := uc.scopes.Upsert(ctx, rs); err != nil {
		return nil, err
	}
	return rs, nil
}
