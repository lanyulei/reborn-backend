package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/job/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/job"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// CredentialService 实现 JobCredentialServiceHTTPServer。
type CredentialService struct {
	uc *biz.CredentialUsecase
}

// NewCredentialService new a CredentialService.
func NewCredentialService(uc *biz.CredentialUsecase) *CredentialService {
	return &CredentialService{uc: uc}
}

func toCredentialInput(spec *v1.JobCredentialSpec) biz.CredentialInput {
	return biz.CredentialInput{
		Name: spec.GetName(), Type: spec.GetType(), Username: spec.GetUsername(), Secret: spec.Secret,
		BecomeMethod: spec.GetBecomeMethod(), BecomeUser: spec.GetBecomeUser(), BecomeSecret: spec.BecomeSecret,
		Description: spec.GetDescription(),
	}
}

// toProtoCredential 密文只写不读：响应里只有 has_become_secret 与公钥指纹。
func toProtoCredential(c *biz.Credential) *v1.JobCredential {
	return &v1.JobCredential{
		Id: c.ID,
		Spec: &v1.JobCredentialSpec{
			Name: c.Name, Type: c.Type, Username: c.Username,
			BecomeMethod: c.BecomeMethod, BecomeUser: c.BecomeUser, Description: c.Description,
		},
		HasBecomeSecret: c.HasBecomeSecret, Fingerprint: c.Fingerprint, TemplateCount: c.TemplateCount, Version: c.Version,
		CreateTime: timestamppb.New(c.CreateTime), UpdateTime: timestamppb.New(c.UpdateTime),
		UpdateUserId: c.UpdateUser, UpdateUserName: c.UpdateUserName,
	}
}

// ListJobCredentials implements proto JobCredentialService/ListJobCredentials.
func (s *CredentialService) ListJobCredentials(ctx context.Context, req *v1.ListJobCredentialsRequest) (*v1.ListJobCredentialsResponse, error) {
	items, total, err := s.uc.List(ctx, biz.CredentialFilter{Keyword: req.GetKeyword(), Type: req.Type}, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListJobCredentialsResponse{Items: make([]*v1.JobCredential, 0, len(items)), Total: total}
	for _, c := range items {
		resp.Items = append(resp.Items, toProtoCredential(c))
	}
	return resp, nil
}

// GetJobCredential implements proto JobCredentialService/GetJobCredential.
func (s *CredentialService) GetJobCredential(ctx context.Context, req *v1.GetJobCredentialRequest) (*v1.GetJobCredentialResponse, error) {
	c, err := s.uc.Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.GetJobCredentialResponse{Credential: toProtoCredential(c)}, nil
}

// CreateJobCredential implements proto JobCredentialService/CreateJobCredential.
func (s *CredentialService) CreateJobCredential(ctx context.Context, req *v1.CreateJobCredentialRequest) (*v1.CreateJobCredentialResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	c, err := s.uc.Create(ctx, userID, toCredentialInput(req.GetSpec()))
	if err != nil {
		return nil, err
	}
	return &v1.CreateJobCredentialResponse{Credential: toProtoCredential(c)}, nil
}

// UpdateJobCredential implements proto JobCredentialService/UpdateJobCredential.
func (s *CredentialService) UpdateJobCredential(ctx context.Context, req *v1.UpdateJobCredentialRequest) (*v1.UpdateJobCredentialResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	c, err := s.uc.Update(ctx, userID, req.GetId(), toCredentialInput(req.GetSpec()), req.GetVersion())
	if err != nil {
		return nil, err
	}
	return &v1.UpdateJobCredentialResponse{Credential: toProtoCredential(c)}, nil
}

// DeleteJobCredential implements proto JobCredentialService/DeleteJobCredential.
func (s *CredentialService) DeleteJobCredential(ctx context.Context, req *v1.DeleteJobCredentialRequest) (*v1.DeleteJobCredentialResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.uc.Delete(ctx, userID, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.DeleteJobCredentialResponse{}, nil
}

// ListJobCredentialOptions implements proto JobCredentialService/ListJobCredentialOptions.
func (s *CredentialService) ListJobCredentialOptions(ctx context.Context, _ *v1.ListJobCredentialOptionsRequest) (*v1.ListJobCredentialOptionsResponse, error) {
	items, err := s.uc.ListOptions(ctx)
	if err != nil {
		return nil, err
	}
	resp := &v1.ListJobCredentialOptionsResponse{Items: make([]*v1.JobCredentialOption, 0, len(items))}
	for _, c := range items {
		resp.Items = append(resp.Items, &v1.JobCredentialOption{
			Id: c.ID, Name: c.Name, Type: c.Type, Username: c.Username, BecomeMethod: c.BecomeMethod,
		})
	}
	return resp, nil
}
