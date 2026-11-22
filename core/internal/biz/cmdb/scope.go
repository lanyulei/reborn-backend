package biz

import (
	"context"
	"slices"
	"strings"
	"time"
)

// RoleScope 角色数据范围。cmdb_role_scope 没有单独的「不限」列：已配置且 ModelCodes 为空的行
// 就是显式的「类型不限」标记（Envs 为空同理表示环境不限）。没有行的角色不贡献任何范围。
type RoleScope struct {
	RoleID     string
	ModelCodes []string
	Envs       []string
	UpdateUser string
	UpdateTime time.Time
}

// Scope 用户有效数据范围（启用角色范围的并集）。
type Scope struct {
	// Unrestricted 不限：超管，或任一启用角色显式配置为类型不限（范围行存在且 ModelCodes 为空）。
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
	// Allowed 用户是否持有某接口操作的权限（operationCode 为操作的稳定标识）。
	Allowed(ctx context.Context, userID, operationCode string) (bool, error)
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

// SelfRolePrefix 容器平台自助授权的用户专属角色标识前缀（k8s_self_<用户 ID>）：这类角色只承载
// 集群数据范围，不参与 CMDB 范围解析（即便有人给它配了范围行也忽略）。
const SelfRolePrefix = "k8s_self_"

// Resolve 解析用户有效范围（fail-closed）：超管不限；否则只看启用角色里配置了范围行的那些——
// 没有范围行的角色不贡献任何范围；范围行存在且类型为空 → 显式不限（见 RoleScope）；
// 否则类型为各角色并集，环境同理（任一已配置角色环境为空 → 环境不限）。
// k8s_self_ 前缀的专属角色一律跳过。非可用用户 / 无启用角色 / 全部角色未配置 → 受限且为空
// （什么都看不到；接口权限层通常已拒绝）。
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
	if len(configured) == 0 {
		return scope, nil
	}
	selfRoles, err := uc.selfRoleIDs(ctx)
	if err != nil {
		return Scope{}, err
	}
	for _, rs := range configured {
		if _, skip := selfRoles[rs.RoleID]; skip {
			continue
		}
		if len(rs.ModelCodes) == 0 {
			return Scope{Unrestricted: true}, nil // 显式配置的「类型不限」
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

// selfRoleIDs 当前全部 k8s_self_ 专属角色的 ID 集合；未装配角色目录（仅测试）时为空。
func (uc *ScopeUsecase) selfRoleIDs(ctx context.Context) (map[string]struct{}, error) {
	out := map[string]struct{}{}
	if uc.dir == nil {
		return out, nil
	}
	ids, _, codes, err := uc.dir.ListRoleRefs(ctx)
	if err != nil {
		return nil, err
	}
	for i, code := range codes {
		if i < len(ids) && strings.HasPrefix(code, SelfRolePrefix) {
			out[ids[i]] = struct{}{}
		}
	}
	return out, nil
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

// Allowed 用户是否持有某接口操作的权限（一个接口的行为需要以另一个接口的权限为前提时用）。
func (uc *ScopeUsecase) Allowed(ctx context.Context, userID, operationCode string) (bool, error) {
	return uc.roles.Allowed(ctx, userID, operationCode)
}
