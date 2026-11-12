package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/cmdb/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"
)

// CmdbScopeService 实现 CmdbScopeServiceServer。
type CmdbScopeService struct {
	uc *biz.ScopeUsecase
}

// NewCmdbScopeService new a CmdbScopeService.
func NewCmdbScopeService(uc *biz.ScopeUsecase) *CmdbScopeService { return &CmdbScopeService{uc: uc} }

// ListRoleScopes implements proto CmdbScopeService/ListRoleScopes.
func (s *CmdbScopeService) ListRoleScopes(ctx context.Context, _ *v1.ListRoleScopesRequest) (*v1.ListRoleScopesResponse, error) {
	ids, names, codes, scopes, err := s.uc.ListRoleScopes(ctx)
	if err != nil {
		return nil, err
	}
	resp := &v1.ListRoleScopesResponse{Items: make([]*v1.RoleScope, 0, len(ids))}
	for i, id := range ids {
		item := &v1.RoleScope{RoleId: id, RoleName: names[i], RoleCode: codes[i], ModelCodes: []string{}, Envs: []string{}}
		if rs, ok := scopes[id]; ok {
			item.ModelCodes, item.Envs = rs.ModelCodes, rs.Envs
		}
		resp.Items = append(resp.Items, item)
	}
	return resp, nil
}

// SetRoleScope implements proto CmdbScopeService/SetRoleScope.
func (s *CmdbScopeService) SetRoleScope(ctx context.Context, req *v1.SetRoleScopeRequest) (*v1.SetRoleScopeResponse, error) {
	actor, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	rs, err := s.uc.SetRoleScope(ctx, actor, req.GetRoleId(), req.GetModelCodes(), req.GetEnvs())
	if err != nil {
		return nil, err
	}
	return &v1.SetRoleScopeResponse{Scope: &v1.RoleScope{RoleId: rs.RoleID, ModelCodes: rs.ModelCodes, Envs: rs.Envs}}, nil
}

// GetMyScope implements proto CmdbScopeService/GetMyScope.
func (s *CmdbScopeService) GetMyScope(ctx context.Context, _ *v1.GetMyScopeRequest) (*v1.GetMyScopeResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	scope, err := s.uc.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	resp := &v1.GetMyScopeResponse{Unrestricted: scope.Unrestricted, ModelCodes: []string{}, Envs: []string{}}
	if !scope.Unrestricted {
		resp.ModelCodes = keys(scope.Models)
		if !scope.EnvUnrestricted {
			resp.Envs = keys(scope.Envs)
		}
	}
	return resp, nil
}
