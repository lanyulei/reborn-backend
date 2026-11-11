package service

import (
	"context"

	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// NotifyDispatchService 实现 api/notify/v1 生成的 NotifyDispatchServiceServer：
// 只读查询运行表（管道写 / core 读），无写路径。
type NotifyDispatchService struct {
	uc       *biz.DispatchUsecase
	guard    bizGuard
	channels *biz.ChannelUsecase
}

// NewNotifyDispatchService new a NotifyDispatchService.
func NewNotifyDispatchService(uc *biz.DispatchUsecase, channels *biz.ChannelUsecase, scope biz.BizScopeResolver) *NotifyDispatchService {
	return &NotifyDispatchService{uc: uc, channels: channels, guard: newBizGuard(scope)}
}

// toProtoDispatch 领域模型转 proto 实体；content 原始 JSON 转 Struct（HTTP
// 原生 JSON 对象渲染），非法 JSON 视为数据损坏直接报错（写入侧为 jsonb 列，
// 数据库已保证合法）；NextRetry 为 nil 时不设置（无待重试）。
func toProtoDispatch(dispatch *biz.Dispatch) (*notifyv1.NotifyDispatch, error) {
	out := &notifyv1.NotifyDispatch{
		Id:         dispatch.ID,
		EventId:    dispatch.EventID,
		ChannelId:  dispatch.ChannelID,
		Recipient:  dispatch.Recipient,
		TemplateId: dispatch.TemplateID,
		Status:     dispatch.Status,
		Attempts:   dispatch.Attempts,
		LastError:  dispatch.LastError,
		SendKey:    dispatch.SendKey,
		CreateTime: timestamppb.New(dispatch.CreateTime),
	}
	if dispatch.NextRetry != nil {
		out.NextRetryTime = timestamppb.New(*dispatch.NextRetry)
	}
	out.Content = jsonToStruct(dispatch.Content)
	return out, nil
}

// ListDispatches implements proto NotifyDispatchService/ListDispatches：四维
// 过滤透传。
func (s *NotifyDispatchService) ListDispatches(ctx context.Context, req *notifyv1.ListDispatchesRequest) (*notifyv1.ListDispatchesResponse, error) {
	if err := s.guard.requireChannelFilter(ctx, req.GetChannelId(), s.channels); err != nil {
		return nil, err
	}
	filter := biz.DispatchFilter{
		EventID:   req.GetEventId(),
		ChannelID: req.GetChannelId(),
		Status:    req.Status,
	}
	if ts := req.GetCreateTimeStart(); ts != nil {
		t := ts.AsTime()
		filter.CreateTimeStart = &t
	}
	if ts := req.GetCreateTimeEnd(); ts != nil {
		t := ts.AsTime()
		filter.CreateTimeEnd = &t
	}
	dispatches, total, err := s.uc.List(ctx, filter, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &notifyv1.ListDispatchesResponse{
		Items: make([]*notifyv1.NotifyDispatch, 0, len(dispatches)),
		Total: total,
	}
	for _, dispatch := range dispatches {
		item, err := toProtoDispatch(dispatch)
		if err != nil {
			return nil, err
		}
		resp.Items = append(resp.Items, item)
	}
	return resp, nil
}
