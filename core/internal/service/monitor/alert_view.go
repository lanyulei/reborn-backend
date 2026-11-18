package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/monitor/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// AlertViewService 实现 MonitorAlertViewServiceHTTPServer。
type AlertViewService struct {
	uc *biz.AlertViewUsecase
}

// NewAlertViewService new an AlertViewService.
func NewAlertViewService(uc *biz.AlertViewUsecase) *AlertViewService {
	return &AlertViewService{uc: uc}
}

func toSavedViewSpec(in *v1.AlertViewSpec) biz.SavedViewSpec {
	f := in.GetFilters()
	if f == nil {
		// 过滤条件可省略：下面直接取可选字段的指针，nil 消息要先换成空消息。
		f = &v1.AlertViewFilter{}
	}
	return biz.SavedViewSpec{
		Name: in.GetName(), By: in.GetBy(), IsPublic: in.GetIsPublic(),
		Filters: mc.AlertViewFilter{
			GroupID: f.GetGroupId(), DatasourceID: f.GetDatasourceId(), Severities: f.GetSeverities(),
			Muted: f.Muted, Acked: f.Acked, Inhibited: f.Inhibited, Labels: f.GetLabels(),
		},
	}
}

func toProtoSavedView(v *biz.SavedViewItem) *v1.AlertView {
	f := v.Spec.Filters
	filters := &v1.AlertViewFilter{Severities: f.Severities, Muted: f.Muted, Acked: f.Acked, Inhibited: f.Inhibited, Labels: f.Labels}
	if f.GroupID != "" {
		filters.GroupId = &f.GroupID
	}
	if f.DatasourceID != "" {
		filters.DatasourceId = &f.DatasourceID
	}
	return &v1.AlertView{
		Id: v.ID, Spec: &v1.AlertViewSpec{Name: v.Spec.Name, By: v.Spec.By, Filters: filters, IsPublic: v.Spec.IsPublic},
		OwnerId: v.OwnerID, OwnerName: v.OwnerName, Mine: v.Mine, CanEdit: v.CanEdit,
		CreateTime: timestamppb.New(v.CreateTime), UpdateTime: timestamppb.New(v.UpdateTime),
	}
}

// ListAlertViews implements proto MonitorAlertViewService/ListAlertViews.
func (s *AlertViewService) ListAlertViews(ctx context.Context, _ *v1.ListAlertViewsRequest) (*v1.ListAlertViewsResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	views, err := s.uc.List(ctx, userID)
	if err != nil {
		return nil, err
	}
	resp := &v1.ListAlertViewsResponse{Items: make([]*v1.AlertView, 0, len(views))}
	for _, v := range views {
		resp.Items = append(resp.Items, toProtoSavedView(v))
	}
	return resp, nil
}

// CreateAlertView implements proto MonitorAlertViewService/CreateAlertView.
func (s *AlertViewService) CreateAlertView(ctx context.Context, req *v1.CreateAlertViewRequest) (*v1.CreateAlertViewResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.uc.Create(ctx, userID, toSavedViewSpec(req.GetSpec()))
	if err != nil {
		return nil, err
	}
	return &v1.CreateAlertViewResponse{View: toProtoSavedView(v)}, nil
}

// UpdateAlertView implements proto MonitorAlertViewService/UpdateAlertView.
func (s *AlertViewService) UpdateAlertView(ctx context.Context, req *v1.UpdateAlertViewRequest) (*v1.UpdateAlertViewResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.uc.Update(ctx, userID, req.GetId(), toSavedViewSpec(req.GetSpec()))
	if err != nil {
		return nil, err
	}
	return &v1.UpdateAlertViewResponse{View: toProtoSavedView(v)}, nil
}

// DeleteAlertView implements proto MonitorAlertViewService/DeleteAlertView.
func (s *AlertViewService) DeleteAlertView(ctx context.Context, req *v1.DeleteAlertViewRequest) (*v1.DeleteAlertViewResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.uc.Delete(ctx, userID, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.DeleteAlertViewResponse{}, nil
}
