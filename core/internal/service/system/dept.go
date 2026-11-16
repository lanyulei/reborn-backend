package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/system/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// DeptService 实现 api/system/v1 生成的 DeptServiceServer。
type DeptService struct {
	uc *biz.DeptUsecase
}

// NewDeptService new a DeptService.
func NewDeptService(uc *biz.DeptUsecase) *DeptService {
	return &DeptService{uc: uc}
}

// CreateDept implements proto DeptService/CreateDept.
func (s *DeptService) CreateDept(ctx context.Context, req *v1.CreateDeptRequest) (*v1.CreateDeptResponse, error) {
	dept, err := s.uc.Create(ctx, &biz.Dept{
		ParentID:    req.GetParentId(),
		Name:        req.GetName(),
		Code:        req.GetCode(),
		Sort:        req.GetSort(),
		Description: req.GetDescription(),
	}, req.GetLeaderIds())
	if err != nil {
		return nil, err
	}
	return &v1.CreateDeptResponse{Dept: toProtoDept(dept)}, nil
}

// UpdateDept implements proto DeptService/UpdateDept：leaders 消息出现即整组替换
// （空列表 = 清空），未出现则不改。
func (s *DeptService) UpdateDept(ctx context.Context, req *v1.UpdateDeptRequest) (*v1.UpdateDeptResponse, error) {
	upd := biz.DeptUpdate{
		Name:        req.Name,
		Code:        req.Code,
		Sort:        req.Sort,
		Status:      req.Status,
		Description: req.Description,
	}
	if req.GetLeaders() != nil {
		ids := req.GetLeaders().GetUserIds()
		if ids == nil {
			ids = []string{}
		}
		upd.LeaderIDs = &ids
	}
	dept, err := s.uc.Update(ctx, req.GetId(), upd)
	if err != nil {
		return nil, err
	}
	return &v1.UpdateDeptResponse{Dept: toProtoDept(dept)}, nil
}

// MoveDept implements proto DeptService/MoveDept.
func (s *DeptService) MoveDept(ctx context.Context, req *v1.MoveDeptRequest) (*v1.MoveDeptResponse, error) {
	if err := s.uc.Move(ctx, req.GetId(), req.GetParentId()); err != nil {
		return nil, err
	}
	return &v1.MoveDeptResponse{}, nil
}

// DeleteDept implements proto DeptService/DeleteDept.
func (s *DeptService) DeleteDept(ctx context.Context, req *v1.DeleteDeptRequest) (*v1.DeleteDeptResponse, error) {
	if err := s.uc.Delete(ctx, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.DeleteDeptResponse{}, nil
}

// GetDept implements proto DeptService/GetDept.
func (s *DeptService) GetDept(ctx context.Context, req *v1.GetDeptRequest) (*v1.GetDeptResponse, error) {
	dept, err := s.uc.Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.GetDeptResponse{Dept: toProtoDept(dept)}, nil
}

// ListDepts implements proto DeptService/ListDepts.
func (s *DeptService) ListDepts(ctx context.Context, _ *v1.ListDeptsRequest) (*v1.ListDeptsResponse, error) {
	depts, err := s.uc.List(ctx)
	if err != nil {
		return nil, err
	}
	resp := &v1.ListDeptsResponse{Items: make([]*v1.Dept, 0, len(depts))}
	for _, d := range depts {
		resp.Items = append(resp.Items, toProtoDept(d))
	}
	return resp, nil
}

// AddDeptMembers implements proto DeptService/AddDeptMembers.
func (s *DeptService) AddDeptMembers(ctx context.Context, req *v1.AddDeptMembersRequest) (*v1.AddDeptMembersResponse, error) {
	if err := s.uc.AddMembers(ctx, req.GetId(), req.GetUserIds()); err != nil {
		return nil, err
	}
	return &v1.AddDeptMembersResponse{}, nil
}

// ListDeptMembers implements proto DeptService/ListDeptMembers：最小披露的成员视图。
func (s *DeptService) ListDeptMembers(ctx context.Context, req *v1.ListDeptMembersRequest) (*v1.ListDeptMembersResponse, error) {
	users, depts, total, err := s.uc.Members(ctx, req.GetId(), req.GetIncludeSubDepts(), req.GetKeyword(), req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListDeptMembersResponse{Items: make([]*v1.DeptMember, 0, len(users)), Total: total}
	for _, u := range users {
		resp.Items = append(resp.Items, &v1.DeptMember{
			Id:       u.ID,
			Username: u.Username,
			Nickname: u.Nickname,
			Avatar:   u.Avatar,
			Status:   u.Status,
			Depts:    toProtoUserDepts(depts[u.ID]),
		})
	}
	return resp, nil
}

// ListUserCandidates implements proto DeptService/ListUserCandidates.
func (s *DeptService) ListUserCandidates(ctx context.Context, req *v1.ListUserCandidatesRequest) (*v1.ListUserCandidatesResponse, error) {
	users, err := s.uc.Candidates(ctx, req.GetKeyword())
	if err != nil {
		return nil, err
	}
	items := make([]*v1.DeptUserCandidate, 0, len(users))
	for _, u := range users {
		items = append(items, &v1.DeptUserCandidate{Id: u.ID, Username: u.Username, Nickname: u.Nickname})
	}
	return &v1.ListUserCandidatesResponse{Items: items}, nil
}

// RemoveDeptMember implements proto DeptService/RemoveDeptMember.
func (s *DeptService) RemoveDeptMember(ctx context.Context, req *v1.RemoveDeptMemberRequest) (*v1.RemoveDeptMemberResponse, error) {
	if err := s.uc.RemoveMember(ctx, req.GetId(), req.GetUserId()); err != nil {
		return nil, err
	}
	return &v1.RemoveDeptMemberResponse{}, nil
}

// toProtoDept 将领域模型（DO）显式转换为 proto 实体。
func toProtoDept(d *biz.Dept) *v1.Dept {
	if d == nil {
		return nil
	}
	leaders := make([]*v1.DeptLeader, 0, len(d.Leaders))
	for _, l := range d.Leaders {
		leaders = append(leaders, &v1.DeptLeader{
			UserId:   l.UserID,
			Username: l.Username,
			Nickname: l.Nickname,
			Avatar:   l.Avatar,
			Status:   l.Status,
		})
	}
	return &v1.Dept{
		Id:          d.ID,
		ParentId:    d.ParentID,
		Name:        d.Name,
		Code:        d.Code,
		Sort:        d.Sort,
		Status:      d.Status,
		Description: d.Description,
		Leaders:     leaders,
		MemberCount: d.MemberCount,
		CreateTime:  timestamppb.New(d.CreateTime),
		UpdateTime:  timestamppb.New(d.UpdateTime),
	}
}

// toProtoUserDepts 将用户所属部门视图显式转换为 proto。
func toProtoUserDepts(depts []biz.UserDept) []*v1.UserDept {
	if len(depts) == 0 {
		return nil
	}
	out := make([]*v1.UserDept, 0, len(depts))
	for _, d := range depts {
		out = append(out, &v1.UserDept{Id: d.ID, Name: d.Name, IsPrimary: d.Primary})
	}
	return out
}

// toDeptAssignment 将 proto 所属部门载荷转换为领域入参；未携带返回 nil（不改）。
func toDeptAssignment(a *v1.UserDeptAssignment) *biz.UserDeptAssignment {
	if a == nil {
		return nil
	}
	return &biz.UserDeptAssignment{DeptIDs: a.GetDeptIds(), PrimaryDeptID: a.GetPrimaryDeptId()}
}
