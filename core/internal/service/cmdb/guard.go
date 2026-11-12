package service

import (
	"context"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
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

// require 目标类型与环境须在范围内。
func (g scopeGuard) require(ctx context.Context, modelCode, env string) error {
	scope, err := g.resolve(ctx)
	if err != nil {
		return err
	}
	if !scope.Allows(modelCode, env) {
		return biz.ErrScopeForbidden
	}
	return nil
}

// requireCI 按资源 ID 读取后校验范围；返回资源供调用方复用。
func (g scopeGuard) requireCI(ctx context.Context, get func(ctx context.Context, id string) (*biz.CI, error), id string) (*biz.CI, error) {
	ci, err := get(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := g.require(ctx, ci.ModelCode, ci.Env); err != nil {
		return nil, err
	}
	return ci, nil
}

// narrowFilter 列表过滤叠加范围：指定了范围外类型直接判为空结果（返回 false）；
// 未指定类型时返回可见类型集合由仓储按 IN 过滤。
func (g scopeGuard) narrowFilter(ctx context.Context, filter *biz.CIFilter) (bool, error) {
	scope, err := g.resolve(ctx)
	if err != nil {
		return false, err
	}
	if scope.Unrestricted {
		return true, nil
	}
	if filter.ModelCode != "" {
		if !scope.AllowsModel(filter.ModelCode) {
			return false, nil
		}
	} else {
		filter.ModelCodes = keys(scope.Models)
		if len(filter.ModelCodes) == 0 {
			return false, nil
		}
	}
	if !scope.EnvUnrestricted {
		if filter.Env != "" {
			if !scope.AllowsEnv(filter.Env) {
				return false, nil
			}
		} else {
			filter.Envs = keys(scope.Envs)
			if len(filter.Envs) == 0 {
				return false, nil
			}
		}
	}
	return true, nil
}

func keys(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	return out
}
