package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/oncall/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/oncall"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// OncallGroupService 实现 OncallGroupServiceServer。
type OncallGroupService struct {
	uc    *biz.GroupUsecase
	admin AdminResolver
}

// NewOncallGroupService new an OncallGroupService.
func NewOncallGroupService(uc *biz.GroupUsecase, admin AdminResolver) *OncallGroupService {
	return &OncallGroupService{uc: uc, admin: admin}
}

func toProtoGroup(g *biz.Group, actor biz.Actor) *v1.Group {
	return &v1.Group{
		Id: g.ID, Name: g.Name, Description: g.Description, MemberCount: g.MemberCount, ScheduleCount: g.ScheduleCount,
		MyRole: g.MyRole, CanManage: biz.CanManage(actor, g),
		CreateTime: timestamppb.New(g.CreateTime), UpdateTime: timestamppb.New(g.UpdateTime),
	}
}

// ListGroups implements proto OncallGroupService/ListGroups.
func (s *OncallGroupService) ListGroups(ctx context.Context, req *v1.ListGroupsRequest) (*v1.ListGroupsResponse, error) {
	actor, err := actorFromContext(ctx, s.admin)
	if err != nil {
		return nil, err
	}
	groups, total, err := s.uc.List(ctx, actor, req.GetKeyword(), req.GetMine(), req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	items := make([]*v1.Group, 0, len(groups))
	for _, g := range groups {
		items = append(items, toProtoGroup(g, actor))
	}
	return &v1.ListGroupsResponse{Items: items, Total: total}, nil
}

// CreateGroup implements proto OncallGroupService/CreateGroup.
func (s *OncallGroupService) CreateGroup(ctx context.Context, req *v1.CreateGroupRequest) (*v1.CreateGroupResponse, error) {
	actor, err := actorFromContext(ctx, s.admin)
	if err != nil {
		return nil, err
	}
	g, err := s.uc.Create(ctx, actor, req.GetName(), req.GetDescription())
	if err != nil {
		return nil, err
	}
	return &v1.CreateGroupResponse{Group: toProtoGroup(g, actor)}, nil
}

// GetGroup implements proto OncallGroupService/GetGroup.
func (s *OncallGroupService) GetGroup(ctx context.Context, req *v1.GetGroupRequest) (*v1.GetGroupResponse, error) {
	actor, err := actorFromContext(ctx, s.admin)
	if err != nil {
		return nil, err
	}
	g, err := s.uc.Get(ctx, actor, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.GetGroupResponse{Group: toProtoGroup(g, actor)}, nil
}

// UpdateGroup implements proto OncallGroupService/UpdateGroup.
func (s *OncallGroupService) UpdateGroup(ctx context.Context, req *v1.UpdateGroupRequest) (*v1.UpdateGroupResponse, error) {
	actor, err := actorFromContext(ctx, s.admin)
	if err != nil {
		return nil, err
	}
	if err := s.uc.Update(ctx, actor, req.GetId(), req.GetName(), req.GetDescription()); err != nil {
		return nil, err
	}
	return &v1.UpdateGroupResponse{}, nil
}

// DeleteGroup implements proto OncallGroupService/DeleteGroup.
func (s *OncallGroupService) DeleteGroup(ctx context.Context, req *v1.DeleteGroupRequest) (*v1.DeleteGroupResponse, error) {
	actor, err := actorFromContext(ctx, s.admin)
	if err != nil {
		return nil, err
	}
	if err := s.uc.Delete(ctx, actor, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.DeleteGroupResponse{}, nil
}

// ListGroupMembers implements proto OncallGroupService/ListGroupMembers.
func (s *OncallGroupService) ListGroupMembers(ctx context.Context, req *v1.ListGroupMembersRequest) (*v1.ListGroupMembersResponse, error) {
	actor, err := actorFromContext(ctx, s.admin)
	if err != nil {
		return nil, err
	}
	members, err := s.uc.ListMembers(ctx, actor, req.GetGroupId())
	if err != nil {
		return nil, err
	}
	items := make([]*v1.GroupMember, 0, len(members))
	for _, m := range members {
		items = append(items, &v1.GroupMember{
			UserId: m.UserID, Username: m.Username, Nickname: m.Nickname, Role: m.Role,
			Available: m.Available, InRotation: m.InRotation, CreateTime: timestamppb.New(m.CreateTime),
		})
	}
	return &v1.ListGroupMembersResponse{Items: items}, nil
}

// AddGroupMembers implements proto OncallGroupService/AddGroupMembers.
func (s *OncallGroupService) AddGroupMembers(ctx context.Context, req *v1.AddGroupMembersRequest) (*v1.AddGroupMembersResponse, error) {
	actor, err := actorFromContext(ctx, s.admin)
	if err != nil {
		return nil, err
	}
	added, err := s.uc.AddMembers(ctx, actor, req.GetGroupId(), req.GetUserIds(), req.GetRole())
	if err != nil {
		return nil, err
	}
	return &v1.AddGroupMembersResponse{Added: int32(added)}, nil //nolint:gosec // added ≤ 50（请求上限）
}

// UpdateGroupMember implements proto OncallGroupService/UpdateGroupMember.
func (s *OncallGroupService) UpdateGroupMember(ctx context.Context, req *v1.UpdateGroupMemberRequest) (*v1.UpdateGroupMemberResponse, error) {
	actor, err := actorFromContext(ctx, s.admin)
	if err != nil {
		return nil, err
	}
	if err := s.uc.UpdateMemberRole(ctx, actor, req.GetGroupId(), req.GetUserId(), req.GetRole()); err != nil {
		return nil, err
	}
	return &v1.UpdateGroupMemberResponse{}, nil
}

// RemoveGroupMember implements proto OncallGroupService/RemoveGroupMember.
func (s *OncallGroupService) RemoveGroupMember(ctx context.Context, req *v1.RemoveGroupMemberRequest) (*v1.RemoveGroupMemberResponse, error) {
	actor, err := actorFromContext(ctx, s.admin)
	if err != nil {
		return nil, err
	}
	if err := s.uc.RemoveMember(ctx, actor, req.GetGroupId(), req.GetUserId()); err != nil {
		return nil, err
	}
	return &v1.RemoveGroupMemberResponse{}, nil
}

// ListUserCandidates implements proto OncallGroupService/ListUserCandidates.
func (s *OncallGroupService) ListUserCandidates(ctx context.Context, req *v1.ListUserCandidatesRequest) (*v1.ListUserCandidatesResponse, error) {
	users, err := s.uc.Candidates(ctx, req.GetKeyword())
	if err != nil {
		return nil, err
	}
	items := make([]*v1.UserCandidate, 0, len(users))
	for _, u := range users {
		items = append(items, &v1.UserCandidate{Id: u.ID, Username: u.Username, Nickname: u.Nickname})
	}
	return &v1.ListUserCandidatesResponse{Items: items}, nil
}

// ListChangeLogs implements proto OncallGroupService/ListChangeLogs.
func (s *OncallGroupService) ListChangeLogs(ctx context.Context, req *v1.ListChangeLogsRequest) (*v1.ListChangeLogsResponse, error) {
	logs, total, err := s.uc.ListChangeLogs(ctx, req.GetGroupId(), req.GetScheduleId(), req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	items := make([]*v1.ChangeLog, 0, len(logs))
	for _, l := range logs {
		// detail 来自本域写入的 JSON 对象；个别值无法表达为 Struct 时退化为空对象，不让整页失败。
		detail, err := structpb.NewStruct(l.Detail)
		if err != nil {
			detail = &structpb.Struct{}
		}
		items = append(items, &v1.ChangeLog{
			Id: l.ID, GroupId: l.GroupID, ScheduleId: l.ScheduleID, Action: l.Action, Summary: l.Summary, Detail: detail,
			OperatorId: l.OperatorID, OperatorName: l.OperatorName, CreateTime: timestamppb.New(l.CreateTime),
		})
	}
	return &v1.ListChangeLogsResponse{Items: items, Total: total}, nil
}
