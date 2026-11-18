package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/job/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/job"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// ScriptService 实现 JobScriptServiceHTTPServer。
type ScriptService struct {
	uc *biz.ScriptUsecase
}

// NewScriptService new a ScriptService.
func NewScriptService(uc *biz.ScriptUsecase) *ScriptService { return &ScriptService{uc: uc} }

func toScript(id string, spec *v1.JobScriptSpec) *biz.Script {
	return &biz.Script{ID: id, Name: spec.GetName(), Interpreter: spec.GetInterpreter(), Content: spec.GetContent(), Description: spec.GetDescription()}
}

func toProtoScript(s *biz.Script) *v1.JobScript {
	return &v1.JobScript{
		Id:   s.ID,
		Spec: &v1.JobScriptSpec{Name: s.Name, Interpreter: s.Interpreter, Content: s.Content, Description: s.Description},
		Size: s.Size, Version: s.Version, CreateTime: timestamppb.New(s.CreateTime), UpdateTime: timestamppb.New(s.UpdateTime),
		UpdateUserId: s.UpdateUser, UpdateUserName: s.UpdateUserName,
	}
}

// ListJobScripts implements proto JobScriptService/ListJobScripts.
func (s *ScriptService) ListJobScripts(ctx context.Context, req *v1.ListJobScriptsRequest) (*v1.ListJobScriptsResponse, error) {
	items, total, err := s.uc.List(ctx, biz.ScriptFilter{Keyword: req.GetKeyword(), Interpreter: req.GetInterpreter()}, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListJobScriptsResponse{Items: make([]*v1.JobScript, 0, len(items)), Total: total}
	for _, it := range items {
		resp.Items = append(resp.Items, toProtoScript(it))
	}
	return resp, nil
}

// GetJobScript implements proto JobScriptService/GetJobScript.
func (s *ScriptService) GetJobScript(ctx context.Context, req *v1.GetJobScriptRequest) (*v1.GetJobScriptResponse, error) {
	it, err := s.uc.Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.GetJobScriptResponse{Script: toProtoScript(it)}, nil
}

// CreateJobScript implements proto JobScriptService/CreateJobScript.
func (s *ScriptService) CreateJobScript(ctx context.Context, req *v1.CreateJobScriptRequest) (*v1.CreateJobScriptResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	it, err := s.uc.Create(ctx, userID, toScript("", req.GetSpec()))
	if err != nil {
		return nil, err
	}
	return &v1.CreateJobScriptResponse{Script: toProtoScript(it)}, nil
}

// UpdateJobScript implements proto JobScriptService/UpdateJobScript.
func (s *ScriptService) UpdateJobScript(ctx context.Context, req *v1.UpdateJobScriptRequest) (*v1.UpdateJobScriptResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	it, err := s.uc.Update(ctx, userID, toScript(req.GetId(), req.GetSpec()), req.GetVersion())
	if err != nil {
		return nil, err
	}
	return &v1.UpdateJobScriptResponse{Script: toProtoScript(it)}, nil
}

// DeleteJobScript implements proto JobScriptService/DeleteJobScript.
func (s *ScriptService) DeleteJobScript(ctx context.Context, req *v1.DeleteJobScriptRequest) (*v1.DeleteJobScriptResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.uc.Delete(ctx, userID, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.DeleteJobScriptResponse{}, nil
}
