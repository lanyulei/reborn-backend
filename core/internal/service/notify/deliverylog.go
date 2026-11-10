package service

import (
	"context"

	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// NotifyDeliveryLogService 实现 api/notify/v1 生成的
// NotifyDeliveryLogServiceServer：只读查询运行表（管道写 / core 读），无写路径。
type NotifyDeliveryLogService struct {
	notifyv1.UnimplementedNotifyDeliveryLogServiceServer

	uc       *biz.DeliveryLogUsecase
	guard    bizGuard
	channels *biz.ChannelUsecase
}

// NewNotifyDeliveryLogService new a NotifyDeliveryLogService.
func NewNotifyDeliveryLogService(uc *biz.DeliveryLogUsecase, channels *biz.ChannelUsecase, scope biz.BizScopeResolver) *NotifyDeliveryLogService {
	return &NotifyDeliveryLogService{uc: uc, channels: channels, guard: newBizGuard(scope)}
}

// toProtoDeliveryLog 领域模型转 proto 实体（全列标量，无 jsonb 解码）。
func toProtoDeliveryLog(log *biz.DeliveryLog) *notifyv1.NotifyDeliveryLog {
	return &notifyv1.NotifyDeliveryLog{
		Id:            log.ID,
		DispatchId:    log.DispatchID,
		ChannelId:     log.ChannelID,
		Attempt:       log.Attempt,
		Ok:            log.OK,
		LatencyMs:     log.LatencyMS,
		ProviderMsgId: log.ProviderMsgID,
		Error:         log.Error,
		CreateTime:    timestamppb.New(log.CreateTime),
	}
}

// ListDeliveryLogs implements proto NotifyDeliveryLogService/ListDeliveryLogs：
// 四维过滤透传。
func (s *NotifyDeliveryLogService) ListDeliveryLogs(ctx context.Context, req *notifyv1.ListDeliveryLogsRequest) (*notifyv1.ListDeliveryLogsResponse, error) {
	if err := s.guard.requireChannelFilter(ctx, req.GetChannelId(), s.channels); err != nil {
		return nil, err
	}
	filter := biz.DeliveryLogFilter{
		DispatchID: req.GetDispatchId(),
		ChannelID:  req.GetChannelId(),
		OK:         req.Ok,
	}
	if ts := req.GetCreateTimeStart(); ts != nil {
		t := ts.AsTime()
		filter.CreateTimeStart = &t
	}
	if ts := req.GetCreateTimeEnd(); ts != nil {
		t := ts.AsTime()
		filter.CreateTimeEnd = &t
	}
	logs, total, err := s.uc.List(ctx, filter, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &notifyv1.ListDeliveryLogsResponse{
		Items: make([]*notifyv1.NotifyDeliveryLog, 0, len(logs)),
		Total: total,
	}
	for _, log := range logs {
		resp.Items = append(resp.Items, toProtoDeliveryLog(log))
	}
	return resp, nil
}
