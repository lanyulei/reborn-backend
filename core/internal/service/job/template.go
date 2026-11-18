package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/job/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/job"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"
	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// TemplateService 实现 JobTemplateServiceHTTPServer。
type TemplateService struct {
	uc    *biz.TemplateUsecase
	execs *biz.ExecutionUsecase
}

// NewTemplateService new a TemplateService.
func NewTemplateService(uc *biz.TemplateUsecase, execs *biz.ExecutionUsecase) *TemplateService {
	return &TemplateService{uc: uc, execs: execs}
}

func toTarget(t *v1.JobTarget) jc.Target {
	return jc.Target{
		ModelCode: t.GetModelCode(), Mode: t.GetMode(), CIIDs: t.GetCiIds(),
		Env: t.GetEnv(), Tag: t.GetTag(), Keyword: t.GetKeyword(), AddressField: t.GetAddressField(),
	}
}

func toProtoTarget(t jc.Target) *v1.JobTarget {
	return &v1.JobTarget{
		ModelCode: t.ModelCode, Mode: t.Mode, CiIds: t.CIIDs,
		Env: t.Env, Tag: t.Tag, Keyword: t.Keyword, AddressField: t.AddressField,
	}
}

func toOptions(o *v1.JobOptions) jc.Options {
	return jc.Options{
		Forks: o.GetForks(), TimeoutS: o.GetTimeoutS(), Port: o.GetPort(),
		Become: o.GetBecome(), Check: o.GetCheck(), Diff: o.GetDiff(), Verbosity: o.GetVerbosity(),
		BatchSize: o.GetBatchSize(), MaxFailPercent: o.GetMaxFailPercent(),
	}
}

func toProtoOptions(o jc.Options) *v1.JobOptions {
	return &v1.JobOptions{
		Forks: o.Forks, TimeoutS: o.TimeoutS, Port: o.Port,
		Become: o.Become, Check: o.Check, Diff: o.Diff, Verbosity: o.Verbosity,
		BatchSize: o.BatchSize, MaxFailPercent: o.MaxFailPercent,
	}
}

func toTemplate(id string, spec *v1.JobTemplateSpec) *biz.Template {
	params := make([]jc.Param, 0, len(spec.GetParams()))
	for _, p := range spec.GetParams() {
		params = append(params, jc.Param{
			Code: p.GetCode(), Name: p.GetName(), Type: p.GetType(), Required: p.GetRequired(),
			Default: p.GetDefault(), Options: p.GetOptions(), Description: p.GetDescription(),
		})
	}
	return &biz.Template{
		ID: id, Name: spec.GetName(), Description: spec.GetDescription(), ProjectID: spec.GetProjectId(),
		Playbook: spec.GetPlaybook(), CredentialID: spec.GetCredentialId(), Target: toTarget(spec.GetTarget()),
		Params: params, Options: toOptions(spec.GetOptions()), RunnerZone: spec.GetRunnerZone(),
		Exclusive: spec.GetExclusive(), Enabled: spec.GetEnabled(),
		RequireApproval: spec.GetRequireApproval(), SliceSize: spec.GetSliceSize(),
	}
}

func toProtoTemplate(t *biz.Template) *v1.JobTemplate {
	params := make([]*v1.JobParam, 0, len(t.Params))
	for _, p := range t.Params {
		params = append(params, &v1.JobParam{
			Code: p.Code, Name: p.Name, Type: p.Type, Required: p.Required,
			Default: p.Default, Options: p.Options, Description: p.Description,
		})
	}
	return &v1.JobTemplate{
		Id: t.ID,
		Spec: &v1.JobTemplateSpec{
			Name: t.Name, Description: t.Description, ProjectId: t.ProjectID, Playbook: t.Playbook,
			CredentialId: t.CredentialID, Target: toProtoTarget(t.Target), Params: params, Options: toProtoOptions(t.Options),
			RunnerZone: t.RunnerZone, Exclusive: t.Exclusive, Enabled: t.Enabled,
			RequireApproval: t.RequireApproval, SliceSize: t.SliceSize,
		},
		ProjectName: t.ProjectName, ProjectRef: t.ProjectRef, CredentialName: t.CredentialName, ModelName: t.ModelName,
		Version: t.Version, CreateTime: timestamppb.New(t.CreateTime), UpdateTime: timestamppb.New(t.UpdateTime),
		UpdateUserId: t.UpdateUser, UpdateUserName: t.UpdateUserName,
	}
}

func toProtoTargetHosts(hosts []biz.Host) []*v1.JobTargetHost {
	out := make([]*v1.JobTargetHost, 0, len(hosts))
	for _, h := range hosts {
		out = append(out, &v1.JobTargetHost{CiId: h.CIID, Name: h.Name, Env: h.Env, Address: h.Address, Problem: h.Problem})
	}
	return out
}

// ListJobTemplates implements proto JobTemplateService/ListJobTemplates.
func (s *TemplateService) ListJobTemplates(ctx context.Context, req *v1.ListJobTemplatesRequest) (*v1.ListJobTemplatesResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	filter := biz.TemplateFilter{Keyword: req.GetKeyword(), ProjectID: req.GetProjectId(), Enabled: req.Enabled}
	items, total, err := s.uc.List(ctx, userID, filter, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListJobTemplatesResponse{Items: make([]*v1.JobTemplate, 0, len(items)), Total: total}
	for _, t := range items {
		resp.Items = append(resp.Items, toProtoTemplate(t))
	}
	return resp, nil
}

// GetJobTemplate implements proto JobTemplateService/GetJobTemplate.
func (s *TemplateService) GetJobTemplate(ctx context.Context, req *v1.GetJobTemplateRequest) (*v1.GetJobTemplateResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	t, err := s.uc.Get(ctx, userID, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.GetJobTemplateResponse{Template: toProtoTemplate(t)}, nil
}

// CreateJobTemplate implements proto JobTemplateService/CreateJobTemplate.
func (s *TemplateService) CreateJobTemplate(ctx context.Context, req *v1.CreateJobTemplateRequest) (*v1.CreateJobTemplateResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	t, err := s.uc.Create(ctx, userID, toTemplate("", req.GetSpec()))
	if err != nil {
		return nil, err
	}
	return &v1.CreateJobTemplateResponse{Template: toProtoTemplate(t)}, nil
}

// UpdateJobTemplate implements proto JobTemplateService/UpdateJobTemplate.
func (s *TemplateService) UpdateJobTemplate(ctx context.Context, req *v1.UpdateJobTemplateRequest) (*v1.UpdateJobTemplateResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	t, err := s.uc.Update(ctx, userID, toTemplate(req.GetId(), req.GetSpec()), req.GetVersion())
	if err != nil {
		return nil, err
	}
	return &v1.UpdateJobTemplateResponse{Template: toProtoTemplate(t)}, nil
}

// DeleteJobTemplate implements proto JobTemplateService/DeleteJobTemplate.
func (s *TemplateService) DeleteJobTemplate(ctx context.Context, req *v1.DeleteJobTemplateRequest) (*v1.DeleteJobTemplateResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.uc.Delete(ctx, userID, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.DeleteJobTemplateResponse{}, nil
}

// PreviewJobTarget implements proto JobTemplateService/PreviewJobTarget.
func (s *TemplateService) PreviewJobTarget(ctx context.Context, req *v1.PreviewJobTargetRequest) (*v1.PreviewJobTargetResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	set, err := s.uc.PreviewTarget(ctx, userID, toTarget(req.GetTarget()))
	if err != nil {
		return nil, err
	}
	return &v1.PreviewJobTargetResponse{Hosts: toProtoTargetHosts(set.Hosts), Truncated: set.Truncated}, nil
}

// ListJobTemplateHosts implements proto JobTemplateService/ListJobTemplateHosts.
func (s *TemplateService) ListJobTemplateHosts(ctx context.Context, req *v1.ListJobTemplateHostsRequest) (*v1.ListJobTemplateHostsResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	pf, err := s.execs.TemplatePreflight(ctx, userID, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.ListJobTemplateHostsResponse{
		Hosts: toProtoTargetHosts(pf.Hosts), Truncated: pf.Truncated,
		OutOfScope:      int32(pf.OutOfScope), //nolint:gosec // 主机数有界
		ApprovalReasons: pf.ApprovalReasons, ApprovalReady: pf.ApprovalReady,
		MaxHosts: int32(pf.MaxHosts), //nolint:gosec // 至多 MaxHosts × MaxSlices
	}, nil
}
