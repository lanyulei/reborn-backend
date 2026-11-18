package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/monitor/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// EngineService 实现 MonitorEngineServiceHTTPServer。
type EngineService struct {
	uc *biz.EngineUsecase
}

// NewEngineService new an EngineService.
func NewEngineService(uc *biz.EngineUsecase) *EngineService { return &EngineService{uc: uc} }

// GetEngineStatus implements proto MonitorEngineService/GetEngineStatus.
func (s *EngineService) GetEngineStatus(ctx context.Context, _ *v1.GetEngineStatusRequest) (*v1.GetEngineStatusResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	st, err := s.uc.Status(ctx, userID)
	if err != nil {
		return nil, err
	}
	sum := st.Summary
	resp := &v1.GetEngineStatusResponse{
		Healthy: st.Healthy, TaskTotal: sum.TaskTotal, TaskOverdue: sum.TaskOverdue, TaskError: sum.TaskError, MaxLagMs: sum.MaxLagMS,
		RecordTaskTotal: sum.RecordTaskTotal, RecordTaskError: sum.RecordTaskError, NotifyPending: sum.NotifyPending,
		CiSyncTime: protoTime(st.CISyncTime), CiCount: st.CICount,
		Instances:   make([]*v1.EngineInstance, 0, len(st.Instances)),
		Datasources: make([]*v1.EngineDatasourceIssue, 0, len(st.Datasources)),
		Rules:       make([]*v1.EngineRuleIssue, 0, len(st.Rules)),
	}
	for _, i := range st.Instances {
		resp.Instances = append(resp.Instances, &v1.EngineInstance{
			Instance: i.Instance, Roles: i.Roles, Version: i.Version, Leader: i.Leader, Alive: i.Alive,
			StartTime: timestamppb.New(i.StartTime), LastSeen: timestamppb.New(i.LastSeen),
			TaskCount: i.TaskCount, EvalLagMs: i.EvalLagMS, EvalCount: i.EvalCount, FailCount: i.FailCount,
		})
	}
	for _, d := range st.Datasources {
		resp.Datasources = append(resp.Datasources, &v1.EngineDatasourceIssue{Id: d.ID, Name: d.Name, Error: d.Error, Since: protoTime(d.Since), RuleCount: d.RuleCount})
	}
	for _, r := range st.Rules {
		resp.Rules = append(resp.Rules, &v1.EngineRuleIssue{
			RuleId: r.RuleID, RuleName: r.RuleName, GroupName: r.GroupName, DatasourceId: r.DatasourceID, DatasourceName: r.DatasourceName,
			Error: r.Error, Since: protoTime(r.Since), Failures: r.Failures,
		})
	}
	return resp, nil
}
