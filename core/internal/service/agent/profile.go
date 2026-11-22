package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/agent/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/agent"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// ProfileService 实现 AgentProfileServiceHTTPServer。
type ProfileService struct {
	uc    *biz.ProfileUsecase
	tasks *biz.TaskUsecase
}

// NewProfileService new a ProfileService.
func NewProfileService(uc *biz.ProfileUsecase, tasks *biz.TaskUsecase) *ProfileService {
	return &ProfileService{uc: uc, tasks: tasks}
}

// ListProfiles implements proto AgentProfileService/ListProfiles.
func (s *ProfileService) ListProfiles(ctx context.Context, req *v1.ListProfilesRequest) (*v1.ListProfilesResponse, error) {
	items, total, err := s.uc.List(ctx, req.GetKeyword(), req.Enabled, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	out := &v1.ListProfilesResponse{Items: make([]*v1.Profile, 0, len(items)), Total: total}
	for _, it := range items {
		out.Items = append(out.Items, toProtoProfile(it))
	}
	return out, nil
}

// GetProfile implements proto AgentProfileService/GetProfile.
func (s *ProfileService) GetProfile(ctx context.Context, req *v1.GetProfileRequest) (*v1.GetProfileResponse, error) {
	v, err := s.uc.Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.GetProfileResponse{Profile: toProtoProfile(v)}, nil
}

// CreateProfile implements proto AgentProfileService/CreateProfile.
func (s *ProfileService) CreateProfile(ctx context.Context, req *v1.CreateProfileRequest) (*v1.CreateProfileResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.uc.Create(ctx, uid, toSpec(req.GetSpec()))
	if err != nil {
		return nil, err
	}
	return &v1.CreateProfileResponse{Profile: toProtoProfile(v)}, nil
}

// UpdateProfile implements proto AgentProfileService/UpdateProfile.
func (s *ProfileService) UpdateProfile(ctx context.Context, req *v1.UpdateProfileRequest) (*v1.UpdateProfileResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.uc.Update(ctx, uid, req.GetId(), toSpec(req.GetSpec()), req.GetVersion(), req.GetResetBreaker())
	if err != nil {
		return nil, err
	}
	return &v1.UpdateProfileResponse{Profile: toProtoProfile(v)}, nil
}

// DeleteProfile implements proto AgentProfileService/DeleteProfile.
func (s *ProfileService) DeleteProfile(ctx context.Context, req *v1.DeleteProfileRequest) (*v1.DeleteProfileResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.uc.Delete(ctx, uid, req.GetId(), req.GetVersion()); err != nil {
		return nil, err
	}
	return &v1.DeleteProfileResponse{}, nil
}

// TestProfile implements proto AgentProfileService/TestProfile：异步干跑，返回任务 ID。
func (s *ProfileService) TestProfile(ctx context.Context, req *v1.TestProfileRequest) (*v1.TestProfileResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	t, err := s.tasks.DryRun(ctx, uid, req.GetId(), req.GetIncidentId())
	if err != nil {
		return nil, err
	}
	return &v1.TestProfileResponse{TaskId: t.ID}, nil
}

// ListProfileHistory implements proto AgentProfileService/ListProfileHistory.
func (s *ProfileService) ListProfileHistory(ctx context.Context, req *v1.ListProfileHistoryRequest) (*v1.ListProfileHistoryResponse, error) {
	items, total, err := s.uc.History(ctx, req.GetId(), req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	out := &v1.ListProfileHistoryResponse{Items: make([]*v1.ProfileHistory, 0, len(items)), Total: total}
	for _, h := range items {
		item := &v1.ProfileHistory{Id: h.ID, Version: h.Version, Actor: named(h.ActorID, h.ActorName), After: toProtoSpec(&h.After), Note: h.Note, CreateTime: timestamppb.New(h.CreateTime)}
		if h.Before != nil {
			item.Before = toProtoSpec(h.Before)
		}
		out.Items = append(out.Items, item)
	}
	return out, nil
}

// ListProfileTools implements proto AgentProfileService/ListProfileTools.
func (s *ProfileService) ListProfileTools(context.Context, *v1.ListProfileToolsRequest) (*v1.ListProfileToolsResponse, error) {
	opts := s.uc.ToolOptions()
	out := &v1.ListProfileToolsResponse{Items: make([]*v1.ToolOption, 0, len(opts))}
	for _, o := range opts {
		out.Items = append(out.Items, &v1.ToolOption{Name: o.Name, Title: o.Title, Module: o.Module, Write: o.Write, Stages: o.Stages})
	}
	return out, nil
}
