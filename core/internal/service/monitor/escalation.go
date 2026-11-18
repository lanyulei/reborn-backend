package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/monitor/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
	mp "github.com/lanyulei/reborn/backend/kit/monitorpipeline"

	"github.com/google/wire"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// escalationProviders 升级策略的 service providers。
var escalationProviders = wire.NewSet(NewEscalationService)

var _ v1.MonitorEscalationServiceHTTPServer = (*EscalationService)(nil)

// EscalationService 实现 MonitorEscalationServiceHTTPServer。
type EscalationService struct {
	uc *biz.EscalationUsecase
}

// NewEscalationService new an EscalationService.
func NewEscalationService(uc *biz.EscalationUsecase) *EscalationService {
	return &EscalationService{uc: uc}
}

func toEscalationSpec(in *v1.EscalationSpec) biz.EscalationSpec {
	spec := biz.EscalationSpec{
		GroupID: in.GetGroupId(), Name: in.GetName(), Description: in.GetDescription(), Mode: in.GetMode(),
		MinSeverity: in.GetMinSeverity(), Enabled: in.GetEnabled(), Levels: make([]mc.EscalationLevel, 0, len(in.GetLevels())),
	}
	for _, l := range in.GetLevels() {
		t := l.GetTargets()
		targets := mc.EscalationTargets{UserIDs: t.GetUserIds(), OncallGroupIDs: t.GetOncallGroupIds()}
		for _, s := range t.GetSchedules() {
			targets.Schedules = append(targets.Schedules, mc.EscalationSchedule{ID: s.GetId(), BackupOnly: s.GetBackupOnly()})
		}
		for _, d := range t.GetDepts() {
			targets.Depts = append(targets.Depts, mc.EscalationDept{ID: d.GetId(), Up: d.GetUp()})
		}
		spec.Levels = append(spec.Levels, mc.EscalationLevel{
			Trigger: l.GetTrigger(), AfterM: l.GetAfterM(), Targets: targets, RepeatIntervalM: l.GetRepeatIntervalM(), MaxNotify: l.GetMaxNotify(),
		})
	}
	return spec
}

func toProtoEscalationSpec(s *biz.EscalationSpec) *v1.EscalationSpec {
	out := &v1.EscalationSpec{
		GroupId: s.GroupID, Name: s.Name, Description: s.Description, Mode: s.Mode, MinSeverity: s.MinSeverity, Enabled: s.Enabled,
		Levels: make([]*v1.EscalationLevel, 0, len(s.Levels)),
	}
	for _, l := range s.Levels {
		targets := &v1.EscalationTargets{
			UserIds: l.Targets.UserIDs, OncallGroupIds: l.Targets.OncallGroupIDs,
			Schedules: make([]*v1.EscalationSchedule, 0, len(l.Targets.Schedules)), Depts: make([]*v1.EscalationDept, 0, len(l.Targets.Depts)),
		}
		for _, sch := range l.Targets.Schedules {
			targets.Schedules = append(targets.Schedules, &v1.EscalationSchedule{Id: sch.ID, BackupOnly: sch.BackupOnly})
		}
		for _, d := range l.Targets.Depts {
			targets.Depts = append(targets.Depts, &v1.EscalationDept{Id: d.ID, Up: d.Up})
		}
		out.Levels = append(out.Levels, &v1.EscalationLevel{
			Trigger: l.Trigger, AfterM: l.AfterM, Targets: targets, RepeatIntervalM: l.RepeatIntervalM, MaxNotify: l.MaxNotify,
		})
	}
	return out
}

func toProtoEstimate(e mp.VolumeEstimate) *v1.EscalationEstimate {
	return &v1.EscalationEstimate{Rule: e.Rule, Levels: e.Levels, Total: e.Total}
}

func toProtoEscalation(e *biz.EscalationView) *v1.Escalation {
	out := &v1.Escalation{
		Id: e.ID, Spec: toProtoEscalationSpec(&e.Spec), GroupName: e.GroupName, RuleCount: e.RuleCount,
		Estimate: toProtoEstimate(e.Estimate), Version: e.Version, CanManage: e.CanManage,
		CreateTime: timestamppb.New(e.CreateTime), UpdateTime: timestamppb.New(e.UpdateTime),
		UpdateUserId: e.UpdateUser, UpdateUserName: e.UpdateUserName, LevelRefs: make([]*v1.EscalationLevelRefs, 0, len(e.LevelRefs)),
	}
	for _, r := range e.LevelRefs {
		out.LevelRefs = append(out.LevelRefs, &v1.EscalationLevelRefs{
			Users: toProtoRefs(r.Users), Schedules: toProtoRefs(r.Schedules), OncallGroups: toProtoRefs(r.OncallGroups), Depts: toProtoRefs(r.Depts),
		})
	}
	return out
}

// ListEscalations implements proto MonitorEscalationService/ListEscalations.
func (s *EscalationService) ListEscalations(ctx context.Context, req *v1.ListEscalationsRequest) (*v1.ListEscalationsResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	items, total, err := s.uc.List(ctx, userID, biz.EscalationListQuery{
		GroupID: req.GetGroupId(), Global: req.Global, Keyword: req.GetKeyword(), Enabled: req.Enabled,
	}, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListEscalationsResponse{Items: make([]*v1.Escalation, 0, len(items)), Total: total}
	for _, e := range items {
		resp.Items = append(resp.Items, toProtoEscalation(e))
	}
	return resp, nil
}

// GetEscalation implements proto MonitorEscalationService/GetEscalation.
func (s *EscalationService) GetEscalation(ctx context.Context, req *v1.GetEscalationRequest) (*v1.GetEscalationResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	e, err := s.uc.Get(ctx, userID, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.GetEscalationResponse{Escalation: toProtoEscalation(e)}, nil
}

// CreateEscalation implements proto MonitorEscalationService/CreateEscalation.
func (s *EscalationService) CreateEscalation(ctx context.Context, req *v1.CreateEscalationRequest) (*v1.CreateEscalationResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	e, err := s.uc.Create(ctx, userID, toEscalationSpec(req.GetSpec()))
	if err != nil {
		return nil, err
	}
	return &v1.CreateEscalationResponse{Escalation: toProtoEscalation(e)}, nil
}

// UpdateEscalation implements proto MonitorEscalationService/UpdateEscalation.
func (s *EscalationService) UpdateEscalation(ctx context.Context, req *v1.UpdateEscalationRequest) (*v1.UpdateEscalationResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	e, err := s.uc.Update(ctx, userID, req.GetId(), toEscalationSpec(req.GetSpec()), req.GetVersion())
	if err != nil {
		return nil, err
	}
	return &v1.UpdateEscalationResponse{Escalation: toProtoEscalation(e)}, nil
}

// DeleteEscalation implements proto MonitorEscalationService/DeleteEscalation.
func (s *EscalationService) DeleteEscalation(ctx context.Context, req *v1.DeleteEscalationRequest) (*v1.DeleteEscalationResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.uc.Delete(ctx, userID, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.DeleteEscalationResponse{}, nil
}

// EstimateEscalation implements proto MonitorEscalationService/EstimateEscalation。纯计算，不读任何数据。
func (s *EscalationService) EstimateEscalation(ctx context.Context, req *v1.EstimateEscalationRequest) (*v1.EstimateEscalationResponse, error) {
	if _, err := systemservice.UserIDFromContext(ctx); err != nil {
		return nil, err
	}
	est, err := s.uc.Estimate(toEscalationSpec(req.GetSpec()), req.GetRuleRepeatIntervalM(), req.GetRuleMaxNotify())
	if err != nil {
		return nil, err
	}
	return &v1.EstimateEscalationResponse{Estimate: toProtoEstimate(est)}, nil
}

// ListEscalationOptions implements proto MonitorEscalationService/ListEscalationOptions.
func (s *EscalationService) ListEscalationOptions(ctx context.Context, req *v1.ListEscalationOptionsRequest) (*v1.ListEscalationOptionsResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	options, err := s.uc.Options(ctx, userID, req.GetGroupId())
	if err != nil {
		return nil, err
	}
	resp := &v1.ListEscalationOptionsResponse{Items: make([]*v1.EscalationOption, 0, len(options))}
	for _, o := range options {
		resp.Items = append(resp.Items, &v1.EscalationOption{
			Id: o.ID, Name: o.Name, Global: o.Global, GroupName: o.GroupName, LevelCount: o.LevelCount, Mode: o.Mode, MinSeverity: o.MinSeverity,
		})
	}
	return resp, nil
}

// ListOncallGroupOptions implements proto MonitorEscalationService/ListOncallGroupOptions.
func (s *EscalationService) ListOncallGroupOptions(ctx context.Context, _ *v1.ListOncallGroupOptionsRequest) (*v1.ListOncallGroupOptionsResponse, error) {
	if _, err := systemservice.UserIDFromContext(ctx); err != nil {
		return nil, err
	}
	groups, err := s.uc.OncallGroupOptions(ctx)
	if err != nil {
		return nil, err
	}
	resp := &v1.ListOncallGroupOptionsResponse{Items: make([]*v1.OncallGroupOption, 0, len(groups))}
	for _, g := range groups {
		resp.Items = append(resp.Items, &v1.OncallGroupOption{Id: g.ID, Name: g.Name})
	}
	return resp, nil
}
