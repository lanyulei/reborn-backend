package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/job/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/job"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// RunnerService 实现 JobRunnerServiceHTTPServer。
type RunnerService struct {
	uc *biz.RunnerUsecase
}

// NewRunnerService new a RunnerService.
func NewRunnerService(uc *biz.RunnerUsecase) *RunnerService { return &RunnerService{uc: uc} }

func toRunnerInput(spec *v1.JobRunnerSpec) biz.RunnerInput {
	return biz.RunnerInput{Name: spec.GetName(), Zone: spec.GetZone(), Enabled: spec.GetEnabled(), Description: spec.GetDescription()}
}

func toProtoRunner(r *biz.Runner) *v1.JobRunner {
	return &v1.JobRunner{
		Id:     r.ID,
		Spec:   &v1.JobRunnerSpec{Name: r.Name, Zone: r.Zone, Enabled: r.Enabled, Description: r.Description},
		Online: r.Online, LastSeen: protoTime(r.LastSeen), AgentVersion: r.AgentVersion, Hostname: r.Hostname,
		Capacity: r.Capacity, Running: r.Running, Version: r.Version,
		CreateTime: timestamppb.New(r.CreateTime), UpdateTime: timestamppb.New(r.UpdateTime),
	}
}

// ListJobRunners implements proto JobRunnerService/ListJobRunners.
func (s *RunnerService) ListJobRunners(ctx context.Context, req *v1.ListJobRunnersRequest) (*v1.ListJobRunnersResponse, error) {
	items, total, err := s.uc.List(ctx, req.GetKeyword(), req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListJobRunnersResponse{Items: make([]*v1.JobRunner, 0, len(items)), Total: total}
	for _, r := range items {
		resp.Items = append(resp.Items, toProtoRunner(r))
	}
	return resp, nil
}

// CreateJobRunner implements proto JobRunnerService/CreateJobRunner。令牌明文只在这里返回一次。
func (s *RunnerService) CreateJobRunner(ctx context.Context, req *v1.CreateJobRunnerRequest) (*v1.CreateJobRunnerResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	r, token, err := s.uc.Create(ctx, userID, toRunnerInput(req.GetSpec()))
	if err != nil {
		return nil, err
	}
	return &v1.CreateJobRunnerResponse{Runner: toProtoRunner(r), Token: token}, nil
}

// UpdateJobRunner implements proto JobRunnerService/UpdateJobRunner.
func (s *RunnerService) UpdateJobRunner(ctx context.Context, req *v1.UpdateJobRunnerRequest) (*v1.UpdateJobRunnerResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	r, err := s.uc.Update(ctx, userID, req.GetId(), toRunnerInput(req.GetSpec()), req.GetVersion())
	if err != nil {
		return nil, err
	}
	return &v1.UpdateJobRunnerResponse{Runner: toProtoRunner(r)}, nil
}

// ResetJobRunnerToken implements proto JobRunnerService/ResetJobRunnerToken.
func (s *RunnerService) ResetJobRunnerToken(ctx context.Context, req *v1.ResetJobRunnerTokenRequest) (*v1.ResetJobRunnerTokenResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	token, err := s.uc.ResetToken(ctx, userID, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.ResetJobRunnerTokenResponse{Token: token}, nil
}

// DeleteJobRunner implements proto JobRunnerService/DeleteJobRunner.
func (s *RunnerService) DeleteJobRunner(ctx context.Context, req *v1.DeleteJobRunnerRequest) (*v1.DeleteJobRunnerResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.uc.Delete(ctx, userID, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.DeleteJobRunnerResponse{}, nil
}

// ListJobRunnerZones implements proto JobRunnerService/ListJobRunnerZones.
func (s *RunnerService) ListJobRunnerZones(ctx context.Context, _ *v1.ListJobRunnerZonesRequest) (*v1.ListJobRunnerZonesResponse, error) {
	zones, err := s.uc.Zones(ctx)
	if err != nil {
		return nil, err
	}
	return &v1.ListJobRunnerZonesResponse{Zones: zones}, nil
}
