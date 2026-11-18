package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/monitor/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// RecordRuleService 实现 MonitorRecordRuleServiceHTTPServer。
type RecordRuleService struct {
	uc *biz.RecordRuleUsecase
}

// NewRecordRuleService new a RecordRuleService.
func NewRecordRuleService(uc *biz.RecordRuleUsecase) *RecordRuleService {
	return &RecordRuleService{uc: uc}
}

func toRecordRuleSpec(in *v1.RecordRuleSpec) biz.RecordRuleSpec {
	return biz.RecordRuleSpec{
		GroupID: in.GetGroupId(), Name: in.GetName(), Description: in.GetDescription(), DatasourceID: in.GetDatasourceId(),
		Expr: in.GetExpr(), IntervalS: in.GetIntervalS(), Labels: in.GetLabels(), Enabled: in.GetEnabled(),
	}
}

func toProtoRecordRule(v *biz.RecordRuleView) *v1.RecordRule {
	s := &v.Spec
	out := &v1.RecordRule{
		Id: v.ID,
		Spec: &v1.RecordRuleSpec{
			GroupId: s.GroupID, Name: s.Name, Description: s.Description, DatasourceId: s.DatasourceID, Expr: s.Expr,
			IntervalS: s.IntervalS, Labels: s.Labels, Enabled: s.Enabled,
		},
		GroupName: v.GroupName, DatasourceName: v.DatasourceName, Writable: v.Writable, Version: v.Version, CanManage: v.CanManage,
		CreateTime: timestamppb.New(v.CreateTime), UpdateTime: timestamppb.New(v.UpdateTime),
		UpdateUserId: v.UpdateUser, UpdateUserName: v.UpdateUserName,
	}
	if t := v.Task; t != nil {
		out.LastStatus, out.LastEvalTime, out.LastError = t.LastStatus, protoTime(t.LastEvalTime), t.LastError
		out.LastDurationMs, out.LastSeries, out.NextEvalTime = t.LastDurationMS, t.LastSeries, protoTime(t.NextEvalTime)
	}
	return out
}

// ListRecordRules implements proto MonitorRecordRuleService/ListRecordRules.
func (s *RecordRuleService) ListRecordRules(ctx context.Context, req *v1.ListRecordRulesRequest) (*v1.ListRecordRulesResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	items, total, err := s.uc.List(ctx, userID, biz.RecordRuleListQuery{
		GroupID: req.GetGroupId(),
		RecordRuleFilter: biz.RecordRuleFilter{
			DatasourceID: req.GetDatasourceId(), Keyword: req.GetKeyword(), Enabled: req.Enabled, LastStatus: req.LastStatus,
		},
	}, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListRecordRulesResponse{Items: make([]*v1.RecordRule, 0, len(items)), Total: total}
	for _, it := range items {
		resp.Items = append(resp.Items, toProtoRecordRule(it))
	}
	return resp, nil
}

// GetRecordRule implements proto MonitorRecordRuleService/GetRecordRule.
func (s *RecordRuleService) GetRecordRule(ctx context.Context, req *v1.GetRecordRuleRequest) (*v1.GetRecordRuleResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.uc.Get(ctx, userID, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.GetRecordRuleResponse{RecordRule: toProtoRecordRule(v)}, nil
}

// CreateRecordRule implements proto MonitorRecordRuleService/CreateRecordRule.
func (s *RecordRuleService) CreateRecordRule(ctx context.Context, req *v1.CreateRecordRuleRequest) (*v1.CreateRecordRuleResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.uc.Create(ctx, userID, toRecordRuleSpec(req.GetSpec()))
	if err != nil {
		return nil, err
	}
	return &v1.CreateRecordRuleResponse{RecordRule: toProtoRecordRule(v)}, nil
}

// UpdateRecordRule implements proto MonitorRecordRuleService/UpdateRecordRule.
func (s *RecordRuleService) UpdateRecordRule(ctx context.Context, req *v1.UpdateRecordRuleRequest) (*v1.UpdateRecordRuleResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.uc.Update(ctx, userID, req.GetId(), toRecordRuleSpec(req.GetSpec()), req.GetVersion())
	if err != nil {
		return nil, err
	}
	return &v1.UpdateRecordRuleResponse{RecordRule: toProtoRecordRule(v)}, nil
}

// DeleteRecordRule implements proto MonitorRecordRuleService/DeleteRecordRule.
func (s *RecordRuleService) DeleteRecordRule(ctx context.Context, req *v1.DeleteRecordRuleRequest) (*v1.DeleteRecordRuleResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.uc.Delete(ctx, userID, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.DeleteRecordRuleResponse{}, nil
}

// ToggleRecordRule implements proto MonitorRecordRuleService/ToggleRecordRule.
func (s *RecordRuleService) ToggleRecordRule(ctx context.Context, req *v1.ToggleRecordRuleRequest) (*v1.ToggleRecordRuleResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.uc.Toggle(ctx, userID, req.GetId(), req.GetEnabled())
	if err != nil {
		return nil, err
	}
	return &v1.ToggleRecordRuleResponse{RecordRule: toProtoRecordRule(v)}, nil
}

// ValidateRecordRule implements proto MonitorRecordRuleService/ValidateRecordRule.
func (s *RecordRuleService) ValidateRecordRule(ctx context.Context, req *v1.ValidateRecordRuleRequest) (*v1.ValidateRecordRuleResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	issues, err := s.uc.Validate(ctx, userID, toRecordRuleSpec(req.GetSpec()), req.GetId())
	if err != nil {
		return nil, err
	}
	resp := &v1.ValidateRecordRuleResponse{Valid: len(issues) == 0, Issues: make([]*v1.RecordRuleIssue, 0, len(issues))}
	for _, is := range issues {
		resp.Issues = append(resp.Issues, &v1.RecordRuleIssue{Field: is.Field, Reason: is.Reason, Message: is.Message})
	}
	return resp, nil
}
