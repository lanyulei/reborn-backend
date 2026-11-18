package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/monitor/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"

	"github.com/google/wire"
)

// AlertP2ProviderSet 告警事件第二、三期的 service providers：以 ProvideAlertService 取代 NewAlertService。
var AlertP2ProviderSet = wire.NewSet(ProvideAlertService)

// ProvideAlertService 装配用：NewAlertService + WithActions + WithShiftCounts。
func ProvideAlertService(uc *biz.AlertUsecase, actions *biz.AlertActionUsecase, shifts *biz.ShiftCountUsecase) *AlertService {
	return NewAlertService(uc).WithActions(actions).WithShiftCounts(shifts)
}

// WithActions 接上事件操作用例（认领 / 转交 / 备注 / 关闭 / 转工单）。
func (s *AlertService) WithActions(actions *biz.AlertActionUsecase) *AlertService {
	s.actions = actions
	return s
}

// alertActions 告警事件操作（认领 / 取消认领 / 转交 / 备注 / 关闭 / 批量 / 转工单）的接口实现，内嵌在
// AlertService 里（方法随之提升）。用例未接上时统一返回 501。
type alertActions struct {
	actions *biz.AlertActionUsecase
}

// errNotImplemented 尚未接上用例的接口的统一返回。
func errNotImplemented() error {
	return v1.ErrorMonitorNotImplemented("not implemented yet")
}

// caller 取事件操作用例与当前用户。
func (s *alertActions) caller(ctx context.Context) (*biz.AlertActionUsecase, string, error) {
	if s.actions == nil {
		return nil, "", errNotImplemented()
	}
	userID, err := systemservice.UserIDFromContext(ctx)
	return s.actions, userID, err
}

// AckAlert implements proto MonitorAlertService/AckAlert.
func (s *alertActions) AckAlert(ctx context.Context, req *v1.AckAlertRequest) (*v1.AckAlertResponse, error) {
	uc, userID, err := s.caller(ctx)
	if err != nil {
		return nil, err
	}
	a, err := uc.Ack(ctx, userID, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.AckAlertResponse{Alert: toProtoAlert(a)}, nil
}

// UnackAlert implements proto MonitorAlertService/UnackAlert.
func (s *alertActions) UnackAlert(ctx context.Context, req *v1.UnackAlertRequest) (*v1.UnackAlertResponse, error) {
	uc, userID, err := s.caller(ctx)
	if err != nil {
		return nil, err
	}
	a, err := uc.Unack(ctx, userID, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.UnackAlertResponse{Alert: toProtoAlert(a)}, nil
}

// TransferAlert implements proto MonitorAlertService/TransferAlert.
func (s *alertActions) TransferAlert(ctx context.Context, req *v1.TransferAlertRequest) (*v1.TransferAlertResponse, error) {
	uc, userID, err := s.caller(ctx)
	if err != nil {
		return nil, err
	}
	a, err := uc.Transfer(ctx, userID, req.GetId(), req.GetUserId(), req.GetNote())
	if err != nil {
		return nil, err
	}
	return &v1.TransferAlertResponse{Alert: toProtoAlert(a)}, nil
}

// CommentAlert implements proto MonitorAlertService/CommentAlert.
func (s *alertActions) CommentAlert(ctx context.Context, req *v1.CommentAlertRequest) (*v1.CommentAlertResponse, error) {
	uc, userID, err := s.caller(ctx)
	if err != nil {
		return nil, err
	}
	item, err := uc.Comment(ctx, userID, req.GetId(), req.GetText())
	if err != nil {
		return nil, err
	}
	return &v1.CommentAlertResponse{Item: toProtoTimelineItem(item)}, nil
}

// CloseAlert implements proto MonitorAlertService/CloseAlert.
func (s *alertActions) CloseAlert(ctx context.Context, req *v1.CloseAlertRequest) (*v1.CloseAlertResponse, error) {
	uc, userID, err := s.caller(ctx)
	if err != nil {
		return nil, err
	}
	a, err := uc.Close(ctx, userID, req.GetId(), req.GetReason())
	if err != nil {
		return nil, err
	}
	return &v1.CloseAlertResponse{Alert: toProtoAlert(a), Pending: true}, nil
}

// toProtoActionResults 批量结果 → proto，并数出成功 / 失败条数。
func toProtoActionResults(results []biz.AlertActionResult) (items []*v1.AlertActionResult, succeeded, failed int32) {
	items = make([]*v1.AlertActionResult, 0, len(results))
	for _, r := range results {
		items = append(items, &v1.AlertActionResult{Id: r.ID, Ok: r.OK, Reason: r.Reason, Message: r.Message})
		if r.OK {
			succeeded++
		} else {
			failed++
		}
	}
	return items, succeeded, failed
}

// BatchAckAlerts implements proto MonitorAlertService/BatchAckAlerts.
func (s *alertActions) BatchAckAlerts(ctx context.Context, req *v1.BatchAckAlertsRequest) (*v1.BatchAckAlertsResponse, error) {
	uc, userID, err := s.caller(ctx)
	if err != nil {
		return nil, err
	}
	results, err := uc.BatchAck(ctx, userID, req.GetIds())
	if err != nil {
		return nil, err
	}
	items, ok, failed := toProtoActionResults(results)
	return &v1.BatchAckAlertsResponse{Results: items, Succeeded: ok, Failed: failed}, nil
}

// BatchCloseAlerts implements proto MonitorAlertService/BatchCloseAlerts.
func (s *alertActions) BatchCloseAlerts(ctx context.Context, req *v1.BatchCloseAlertsRequest) (*v1.BatchCloseAlertsResponse, error) {
	uc, userID, err := s.caller(ctx)
	if err != nil {
		return nil, err
	}
	results, err := uc.BatchClose(ctx, userID, req.GetIds(), req.GetReason())
	if err != nil {
		return nil, err
	}
	items, ok, failed := toProtoActionResults(results)
	return &v1.BatchCloseAlertsResponse{Results: items, Succeeded: ok, Failed: failed}, nil
}

// CreateAlertTicket implements proto MonitorAlertService/CreateAlertTicket.
func (s *alertActions) CreateAlertTicket(ctx context.Context, req *v1.CreateAlertTicketRequest) (*v1.CreateAlertTicketResponse, error) {
	uc, userID, err := s.caller(ctx)
	if err != nil {
		return nil, err
	}
	ticket, a, err := uc.CreateTicket(ctx, userID, req.GetId(), biz.AlertTicketInput{
		TemplateID: req.GetTemplateId(), Title: req.GetTitle(), Priority: req.GetPriority(), Data: req.GetData().AsMap(),
		CreatorDept: req.GetCreatorDept(), RequestID: req.GetRequestId(),
	})
	if err != nil {
		return nil, err
	}
	return &v1.CreateAlertTicketResponse{Ticket: toProtoAlertTicket(ticket), Alert: toProtoAlert(a)}, nil
}
