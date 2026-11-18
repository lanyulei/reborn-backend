package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/monitor/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// shiftCounts 值班班次告警数的接口实现，内嵌在 AlertService 里。用例经 WithShiftCounts 接上；未接上时
// （只装配了第一期用例的测试）返回 501。
type shiftCounts struct {
	shifts *biz.ShiftCountUsecase
}

// WithShiftCounts 接上值班班次告警数的用例，返回 s 本身（装配时链式调用）。
func (s *AlertService) WithShiftCounts(uc *biz.ShiftCountUsecase) *AlertService {
	s.shifts = uc
	return s
}

// ListShiftAlertCounts implements proto MonitorAlertService/ListShiftAlertCounts.
func (s *shiftCounts) ListShiftAlertCounts(ctx context.Context, req *v1.ListShiftAlertCountsRequest) (*v1.ListShiftAlertCountsResponse, error) {
	if s.shifts == nil {
		return nil, v1.ErrorMonitorNotImplemented("shift alert counts are not wired")
	}
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	items, err := s.shifts.List(ctx, userID, req.GetScheduleId(), req.GetStartTime().AsTime(), req.GetEndTime().AsTime())
	if err != nil {
		return nil, err
	}
	resp := &v1.ListShiftAlertCountsResponse{Items: make([]*v1.ShiftAlertCount, 0, len(items))}
	for _, it := range items {
		resp.Items = append(resp.Items, &v1.ShiftAlertCount{
			UserId: it.UserID, UserName: it.UserName, Role: it.Role,
			StartTime: timestamppb.New(it.Start), EndTime: timestamppb.New(it.End), AlertCount: it.AlertCount,
		})
	}
	return resp, nil
}
