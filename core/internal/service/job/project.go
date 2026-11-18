package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/job/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/job"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// ProjectService 实现 JobProjectServiceHTTPServer。
type ProjectService struct {
	uc *biz.ProjectUsecase
}

// NewProjectService new a ProjectService.
func NewProjectService(uc *biz.ProjectUsecase) *ProjectService { return &ProjectService{uc: uc} }

func toProjectInput(spec *v1.JobProjectSpec) biz.ProjectInput {
	return biz.ProjectInput{
		Name: spec.GetName(), RepoURL: spec.GetRepoUrl(), Ref: spec.GetRef(), AuthType: spec.GetAuthType(),
		Username: spec.GetUsername(), Secret: spec.Secret, Description: spec.GetDescription(),
	}
}

// toProtoProject 密文只写不读：响应里只有 has_secret。
func toProtoProject(p *biz.Project) *v1.JobProject {
	return &v1.JobProject{
		Id: p.ID,
		Spec: &v1.JobProjectSpec{
			Name: p.Name, RepoUrl: p.RepoURL, Ref: p.Ref, AuthType: p.AuthType, Username: p.Username, Description: p.Description,
		},
		HasSecret: p.HasSecret, TemplateCount: p.TemplateCount, Version: p.Version,
		CreateTime: timestamppb.New(p.CreateTime), UpdateTime: timestamppb.New(p.UpdateTime),
		UpdateUserId: p.UpdateUser, UpdateUserName: p.UpdateUserName,
	}
}

// ListJobProjects implements proto JobProjectService/ListJobProjects.
func (s *ProjectService) ListJobProjects(ctx context.Context, req *v1.ListJobProjectsRequest) (*v1.ListJobProjectsResponse, error) {
	items, total, err := s.uc.List(ctx, req.GetKeyword(), req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListJobProjectsResponse{Items: make([]*v1.JobProject, 0, len(items)), Total: total}
	for _, p := range items {
		resp.Items = append(resp.Items, toProtoProject(p))
	}
	return resp, nil
}

// GetJobProject implements proto JobProjectService/GetJobProject.
func (s *ProjectService) GetJobProject(ctx context.Context, req *v1.GetJobProjectRequest) (*v1.GetJobProjectResponse, error) {
	p, err := s.uc.Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.GetJobProjectResponse{Project: toProtoProject(p)}, nil
}

// CreateJobProject implements proto JobProjectService/CreateJobProject.
func (s *ProjectService) CreateJobProject(ctx context.Context, req *v1.CreateJobProjectRequest) (*v1.CreateJobProjectResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	p, err := s.uc.Create(ctx, userID, toProjectInput(req.GetSpec()))
	if err != nil {
		return nil, err
	}
	return &v1.CreateJobProjectResponse{Project: toProtoProject(p)}, nil
}

// UpdateJobProject implements proto JobProjectService/UpdateJobProject.
func (s *ProjectService) UpdateJobProject(ctx context.Context, req *v1.UpdateJobProjectRequest) (*v1.UpdateJobProjectResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	p, err := s.uc.Update(ctx, userID, req.GetId(), toProjectInput(req.GetSpec()), req.GetVersion())
	if err != nil {
		return nil, err
	}
	return &v1.UpdateJobProjectResponse{Project: toProtoProject(p)}, nil
}

// DeleteJobProject implements proto JobProjectService/DeleteJobProject.
func (s *ProjectService) DeleteJobProject(ctx context.Context, req *v1.DeleteJobProjectRequest) (*v1.DeleteJobProjectResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.uc.Delete(ctx, userID, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.DeleteJobProjectResponse{}, nil
}

// ListJobProjectOptions implements proto JobProjectService/ListJobProjectOptions.
func (s *ProjectService) ListJobProjectOptions(ctx context.Context, _ *v1.ListJobProjectOptionsRequest) (*v1.ListJobProjectOptionsResponse, error) {
	items, err := s.uc.ListOptions(ctx)
	if err != nil {
		return nil, err
	}
	resp := &v1.ListJobProjectOptionsResponse{Items: make([]*v1.JobProjectOption, 0, len(items))}
	for _, p := range items {
		resp.Items = append(resp.Items, &v1.JobProjectOption{Id: p.ID, Name: p.Name, Ref: p.Ref})
	}
	return resp, nil
}
