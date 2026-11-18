package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/job/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/job"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ScheduleService 实现 JobScheduleServiceHTTPServer。
type ScheduleService struct {
	uc *biz.ScheduleUsecase
}

// NewScheduleService new a ScheduleService.
func NewScheduleService(uc *biz.ScheduleUsecase) *ScheduleService { return &ScheduleService{uc: uc} }

func toSchedule(id string, spec *v1.JobScheduleSpec) *biz.Schedule {
	return &biz.Schedule{
		ID: id, Name: spec.GetName(), TemplateID: spec.GetTemplateId(), Cron: spec.GetCron(), Timezone: spec.GetTimezone(),
		Params: spec.GetParams().AsMap(), Misfire: spec.GetMisfire(), Enabled: spec.GetEnabled(), Description: spec.GetDescription(),
		TemplateVersion: spec.GetTemplateVersion(),
	}
}

func toProtoSchedule(s *biz.Schedule) (*v1.JobSchedule, error) {
	params, err := structpb.NewStruct(s.Params)
	if err != nil {
		return nil, err
	}
	return &v1.JobSchedule{
		Id: s.ID,
		Spec: &v1.JobScheduleSpec{
			Name: s.Name, TemplateId: s.TemplateID, Cron: s.Cron, Timezone: s.Timezone, Params: params,
			Misfire: s.Misfire, Enabled: s.Enabled, Description: s.Description, TemplateVersion: s.TemplateVersion,
		},
		TemplateName: s.TemplateName, OwnerId: s.Owner, OwnerName: s.OwnerName,
		NextTime: protoTime(s.NextTime), LastTime: protoTime(s.LastTime), LastExecutionId: s.LastExecutionID, LastError: s.LastError,
		Version: s.Version, CreateTime: timestamppb.New(s.CreateTime), UpdateTime: timestamppb.New(s.UpdateTime),
	}, nil
}

// ListJobSchedules implements proto JobScheduleService/ListJobSchedules.
func (s *ScheduleService) ListJobSchedules(ctx context.Context, req *v1.ListJobSchedulesRequest) (*v1.ListJobSchedulesResponse, error) {
	filter := biz.ScheduleFilter{Keyword: req.GetKeyword(), TemplateID: req.GetTemplateId(), Enabled: req.Enabled}
	items, total, err := s.uc.List(ctx, filter, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListJobSchedulesResponse{Items: make([]*v1.JobSchedule, 0, len(items)), Total: total}
	for _, it := range items {
		out, err := toProtoSchedule(it)
		if err != nil {
			return nil, err
		}
		resp.Items = append(resp.Items, out)
	}
	return resp, nil
}

// CreateJobSchedule implements proto JobScheduleService/CreateJobSchedule.
func (s *ScheduleService) CreateJobSchedule(ctx context.Context, req *v1.CreateJobScheduleRequest) (*v1.CreateJobScheduleResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	it, err := s.uc.Create(ctx, userID, toSchedule("", req.GetSpec()))
	if err != nil {
		return nil, err
	}
	out, err := toProtoSchedule(it)
	if err != nil {
		return nil, err
	}
	return &v1.CreateJobScheduleResponse{Schedule: out}, nil
}

// UpdateJobSchedule implements proto JobScheduleService/UpdateJobSchedule.
func (s *ScheduleService) UpdateJobSchedule(ctx context.Context, req *v1.UpdateJobScheduleRequest) (*v1.UpdateJobScheduleResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	it, err := s.uc.Update(ctx, userID, toSchedule(req.GetId(), req.GetSpec()), req.GetVersion())
	if err != nil {
		return nil, err
	}
	out, err := toProtoSchedule(it)
	if err != nil {
		return nil, err
	}
	return &v1.UpdateJobScheduleResponse{Schedule: out}, nil
}

// DeleteJobSchedule implements proto JobScheduleService/DeleteJobSchedule.
func (s *ScheduleService) DeleteJobSchedule(ctx context.Context, req *v1.DeleteJobScheduleRequest) (*v1.DeleteJobScheduleResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.uc.Delete(ctx, userID, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.DeleteJobScheduleResponse{}, nil
}

// PreviewJobSchedule implements proto JobScheduleService/PreviewJobSchedule.
func (s *ScheduleService) PreviewJobSchedule(ctx context.Context, req *v1.PreviewJobScheduleRequest) (*v1.PreviewJobScheduleResponse, error) {
	times, err := s.uc.Preview(ctx, req.GetCron(), req.GetTimezone())
	if err != nil {
		return nil, err
	}
	resp := &v1.PreviewJobScheduleResponse{NextTimes: make([]*timestamppb.Timestamp, 0, len(times))}
	for _, t := range times {
		resp.NextTimes = append(resp.NextTimes, timestamppb.New(t))
	}
	return resp, nil
}
