package service

import (
	"context"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	bizsystem "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"
)

// scopeGuard 数据范围守卫（service 层：身份来自传输层 claims）：接口权限之外按
// 角色限制可见的资源类型与环境。不限用户零额外开销；受限用户：单资源操作须目标
// 在范围内（403 CMDB_SCOPE_FORBIDDEN），列表强制叠加范围过滤。
type scopeGuard struct {
	scopes *biz.ScopeUsecase
}

func newScopeGuard(scopes *biz.ScopeUsecase) scopeGuard { return scopeGuard{scopes: scopes} }

// resolve 当前用户的有效范围。
func (g scopeGuard) resolve(ctx context.Context) (biz.Scope, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return biz.Scope{}, err
	}
	return g.scopes.Resolve(ctx, userID)
}

// requireOperation 当前用户须同时持有另一个接口操作的权限（如认领同步冲突会改写资源，
// 须有"编辑资源"权限）；没有则 403。
func (g scopeGuard) requireOperation(ctx context.Context, operation string) error {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return err
	}
	ok, err := g.scopes.Allowed(ctx, userID, bizsystem.OperationCode(operation))
	if err != nil {
		return err
	}
	if !ok {
		return biz.ErrOperationForbidden
	}
	return nil
}

// requireCI 按资源 ID 读取后校验范围；返回资源供调用方复用。
func (g scopeGuard) requireCI(ctx context.Context, get func(ctx context.Context, id string) (*biz.CI, error), id string) (*biz.CI, error) {
	ci, _, err := g.requireCIScope(ctx, get, id)
	return ci, err
}

// requireCIScope 同 requireCI，并把这次解析出的范围一并返回：之后还要用范围的调用方（级联退役的
// 依赖资源、历史时刻的环境）直接复用，不必在同一请求里再解析一遍（每次解析是 1–4 次查询）。
// 先读资源、再解析范围，顺序与 requireCI 原来一致（资源不存在时返回的仍是 404 而不是范围错误）。
func (g scopeGuard) requireCIScope(ctx context.Context, get func(ctx context.Context, id string) (*biz.CI, error), id string) (*biz.CI, biz.Scope, error) {
	ci, err := get(ctx, id)
	if err != nil {
		return nil, biz.Scope{}, err
	}
	scope, err := g.resolve(ctx)
	if err != nil {
		return nil, biz.Scope{}, err
	}
	if !scope.Allows(ci.ModelCode, ci.Env) {
		return nil, biz.Scope{}, biz.ErrScopeForbidden
	}
	return ci, scope, nil
}

// narrowFilter 列表过滤叠加范围：指定了范围外类型直接判为空结果（返回 false）；
// 未指定类型时返回可见类型集合由仓储按 IN 过滤。
func (g scopeGuard) narrowFilter(ctx context.Context, filter *biz.CIFilter) (bool, error) {
	scope, err := g.resolve(ctx)
	if err != nil {
		return false, err
	}
	return narrowByScope(scope, filter), nil
}

// narrowByScope 把已解析的范围叠加到列表过滤上（规则见 narrowFilter）；返回 false 表示范围内没有
// 任何可见的类型 / 环境，调用方直接返回空结果。既要叠加过滤、又要 Scope 本身的调用方（如全局审计
// 还要按它裁剪 diff）解析一次后复用，免得同一请求重复解析。
func narrowByScope(scope biz.Scope, filter *biz.CIFilter) bool {
	if scope.Unrestricted {
		return true
	}
	if filter.ModelCode != "" {
		if !scope.AllowsModel(filter.ModelCode) {
			return false
		}
	} else if !scope.ModelUnrestricted { // 类型不限：不叠类型过滤
		filter.ModelCodes = keys(scope.Models)
		if len(filter.ModelCodes) == 0 {
			return false
		}
	}
	if !scope.EnvUnrestricted {
		if filter.Env != "" {
			if !scope.AllowsEnv(filter.Env) {
				return false
			}
		} else {
			filter.Envs = keys(scope.Envs)
			if len(filter.Envs) == 0 {
				return false
			}
		}
	}
	return true
}

func keys(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	return out
}
