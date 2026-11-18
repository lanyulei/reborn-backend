package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/monitor/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	"github.com/google/wire"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// InhibitProviderSet 抑制规则与记录规则的 service providers（由组合根并入 ProviderSet）。
var InhibitProviderSet = wire.NewSet(NewInhibitService, NewRecordRuleService)

// InhibitService 实现 MonitorInhibitServiceHTTPServer。
type InhibitService struct {
	uc *biz.InhibitUsecase
}

// NewInhibitService new an InhibitService.
func NewInhibitService(uc *biz.InhibitUsecase) *InhibitService { return &InhibitService{uc: uc} }

func toInhibitSelector(in *v1.InhibitSelector) mc.InhibitSelector {
	return mc.InhibitSelector{RuleIDs: in.GetRuleIds(), Severities: in.GetSeverities(), Matchers: toMatchers(in.GetMatchers())}
}

func toProtoInhibitSelector(s mc.InhibitSelector) *v1.InhibitSelector {
	return &v1.InhibitSelector{RuleIds: s.RuleIDs, Severities: s.Severities, Matchers: toProtoMatchers(s.Matchers)}
}

func toInhibitSpec(in *v1.InhibitSpec) biz.InhibitSpec {
	return biz.InhibitSpec{
		GroupID: in.GetGroupId(), Name: in.GetName(), Description: in.GetDescription(),
		Source: toInhibitSelector(in.GetSource()), Target: toInhibitSelector(in.GetTarget()),
		EqualLabels: in.GetEqualLabels(), Enabled: in.GetEnabled(),
	}
}

func toProtoInhibit(v *biz.InhibitView) *v1.Inhibit {
	s := &v.Spec
	return &v1.Inhibit{
		Id: v.ID,
		Spec: &v1.InhibitSpec{
			GroupId: s.GroupID, Name: s.Name, Description: s.Description, Source: toProtoInhibitSelector(s.Source),
			Target: toProtoInhibitSelector(s.Target), EqualLabels: s.EqualLabels, Enabled: s.Enabled,
		},
		GroupName: v.GroupName, SourceRules: toProtoRefs(v.SourceRules), TargetRules: toProtoRefs(v.TargetRules),
		InhibitedCount: v.InhibitedCount, Version: v.Version, CanManage: v.CanManage,
		CreateTime: timestamppb.New(v.CreateTime), UpdateTime: timestamppb.New(v.UpdateTime),
		UpdateUserId: v.UpdateUser, UpdateUserName: v.UpdateUserName, CreateUserId: v.CreateUser, CreateUserName: v.CreateUserName,
	}
}

// ListInhibits implements proto MonitorInhibitService/ListInhibits.
func (s *InhibitService) ListInhibits(ctx context.Context, req *v1.ListInhibitsRequest) (*v1.ListInhibitsResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	items, total, err := s.uc.List(ctx, userID, biz.InhibitListQuery{GroupID: req.GetGroupId(), Keyword: req.GetKeyword(), Enabled: req.Enabled},
		req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListInhibitsResponse{Items: make([]*v1.Inhibit, 0, len(items)), Total: total}
	for _, it := range items {
		resp.Items = append(resp.Items, toProtoInhibit(it))
	}
	return resp, nil
}

// GetInhibit implements proto MonitorInhibitService/GetInhibit.
func (s *InhibitService) GetInhibit(ctx context.Context, req *v1.GetInhibitRequest) (*v1.GetInhibitResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.uc.Get(ctx, userID, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.GetInhibitResponse{Inhibit: toProtoInhibit(v)}, nil
}

// CreateInhibit implements proto MonitorInhibitService/CreateInhibit.
func (s *InhibitService) CreateInhibit(ctx context.Context, req *v1.CreateInhibitRequest) (*v1.CreateInhibitResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.uc.Create(ctx, userID, toInhibitSpec(req.GetSpec()))
	if err != nil {
		return nil, err
	}
	return &v1.CreateInhibitResponse{Inhibit: toProtoInhibit(v)}, nil
}

// UpdateInhibit implements proto MonitorInhibitService/UpdateInhibit.
func (s *InhibitService) UpdateInhibit(ctx context.Context, req *v1.UpdateInhibitRequest) (*v1.UpdateInhibitResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.uc.Update(ctx, userID, req.GetId(), toInhibitSpec(req.GetSpec()), req.GetVersion())
	if err != nil {
		return nil, err
	}
	return &v1.UpdateInhibitResponse{Inhibit: toProtoInhibit(v)}, nil
}

// DeleteInhibit implements proto MonitorInhibitService/DeleteInhibit.
func (s *InhibitService) DeleteInhibit(ctx context.Context, req *v1.DeleteInhibitRequest) (*v1.DeleteInhibitResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.uc.Delete(ctx, userID, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.DeleteInhibitResponse{}, nil
}

// PreviewInhibit implements proto MonitorInhibitService/PreviewInhibit.
func (s *InhibitService) PreviewInhibit(ctx context.Context, req *v1.PreviewInhibitRequest) (*v1.PreviewInhibitResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	p, err := s.uc.Preview(ctx, userID, toInhibitSpec(req.GetSpec()))
	if err != nil {
		return nil, err
	}
	return &v1.PreviewInhibitResponse{
		Sources: toProtoAlerts(p.Sources), SourceTotal: int32(p.SourceTotal), // #nosec G115 -- 预览扫描有上限
		Targets: toProtoAlerts(p.Targets), TargetTotal: int32(p.TargetTotal), // #nosec G115 -- 预览扫描有上限
	}, nil
}
