package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/monitor/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// RuleGroupService 实现 MonitorRuleGroupServiceHTTPServer。
type RuleGroupService struct {
	uc *biz.GroupUsecase
}

// NewRuleGroupService new a RuleGroupService.
func NewRuleGroupService(uc *biz.GroupUsecase) *RuleGroupService { return &RuleGroupService{uc: uc} }

func toProtoGroup(g *biz.GroupView) *v1.RuleGroup {
	return &v1.RuleGroup{
		Id: g.ID, ParentId: g.ParentID, Name: g.Name, FullName: g.FullName, Description: g.Description, Sort: g.Sort,
		Depth: g.Depth, Level: g.Level, RuleCount: g.RuleCount, ChildCount: g.ChildCount, Builtin: g.Builtin,
		CreateTime: timestamppb.New(g.CreateTime), UpdateTime: timestamppb.New(g.UpdateTime),
	}
}

func toProtoRoleScopes(items []*biz.RoleScopeView) []*v1.GroupRoleScope {
	out := make([]*v1.GroupRoleScope, 0, len(items))
	for _, it := range items {
		out = append(out, &v1.GroupRoleScope{
			RoleId: it.RoleID, RoleName: it.RoleName, RoleEnabled: it.RoleEnabled, Level: it.Level,
			UpdateUserName: it.UpdateUserName, UpdateTime: timestamppb.New(it.UpdateTime),
		})
	}
	return out
}

// ListRuleGroups implements proto MonitorRuleGroupService/ListRuleGroups.
func (s *RuleGroupService) ListRuleGroups(ctx context.Context, _ *v1.ListRuleGroupsRequest) (*v1.ListRuleGroupsResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	groups, isAdmin, err := s.uc.List(ctx, userID)
	if err != nil {
		return nil, err
	}
	resp := &v1.ListRuleGroupsResponse{Items: make([]*v1.RuleGroup, 0, len(groups)), IsAdmin: isAdmin}
	for _, g := range groups {
		resp.Items = append(resp.Items, toProtoGroup(g))
	}
	return resp, nil
}

// CreateRuleGroup implements proto MonitorRuleGroupService/CreateRuleGroup.
func (s *RuleGroupService) CreateRuleGroup(ctx context.Context, req *v1.CreateRuleGroupRequest) (*v1.CreateRuleGroupResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	g, err := s.uc.Create(ctx, userID, req.GetParentId(), req.GetName(), req.GetDescription(), req.GetSort())
	if err != nil {
		return nil, err
	}
	return &v1.CreateRuleGroupResponse{Group: toProtoGroup(g)}, nil
}

// UpdateRuleGroup implements proto MonitorRuleGroupService/UpdateRuleGroup.
func (s *RuleGroupService) UpdateRuleGroup(ctx context.Context, req *v1.UpdateRuleGroupRequest) (*v1.UpdateRuleGroupResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	g, err := s.uc.Update(ctx, userID, req.GetId(), req.GetName(), req.GetDescription(), req.GetSort())
	if err != nil {
		return nil, err
	}
	return &v1.UpdateRuleGroupResponse{Group: toProtoGroup(g)}, nil
}

// DeleteRuleGroup implements proto MonitorRuleGroupService/DeleteRuleGroup.
func (s *RuleGroupService) DeleteRuleGroup(ctx context.Context, req *v1.DeleteRuleGroupRequest) (*v1.DeleteRuleGroupResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.uc.Delete(ctx, userID, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.DeleteRuleGroupResponse{}, nil
}

// ListGroupRoleScopes implements proto MonitorRuleGroupService/ListGroupRoleScopes.
func (s *RuleGroupService) ListGroupRoleScopes(ctx context.Context, req *v1.ListGroupRoleScopesRequest) (*v1.ListGroupRoleScopesResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	direct, inherited, err := s.uc.ListRoleScopes(ctx, userID, req.GetGroupId())
	if err != nil {
		return nil, err
	}
	resp := &v1.ListGroupRoleScopesResponse{Items: toProtoRoleScopes(direct), Inherited: make([]*v1.InheritedGroupRoleScope, 0, len(inherited))}
	for _, it := range inherited {
		resp.Inherited = append(resp.Inherited, &v1.InheritedGroupRoleScope{
			RoleId: it.RoleID, RoleName: it.RoleName, Level: it.Level, GroupId: it.GroupID, GroupName: it.GroupName,
		})
	}
	return resp, nil
}

// SetGroupRoleScopes implements proto MonitorRuleGroupService/SetGroupRoleScopes.
func (s *RuleGroupService) SetGroupRoleScopes(ctx context.Context, req *v1.SetGroupRoleScopesRequest) (*v1.SetGroupRoleScopesResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	items := make([]*biz.RoleScope, 0, len(req.GetItems()))
	for _, it := range req.GetItems() {
		items = append(items, &biz.RoleScope{RoleID: it.GetRoleId(), GroupID: req.GetGroupId(), Level: it.GetLevel()})
	}
	direct, err := s.uc.SetRoleScopes(ctx, userID, req.GetGroupId(), items)
	if err != nil {
		return nil, err
	}
	return &v1.SetGroupRoleScopesResponse{Items: toProtoRoleScopes(direct)}, nil
}
