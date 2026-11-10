package service

import (
	"context"

	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// NotifyEventService 实现 api/notify/v1 生成的 NotifyEventServiceServer：
// 只读查询运行表（管道写 / core 读），无写路径。
type NotifyEventService struct {
	notifyv1.UnimplementedNotifyEventServiceServer

	uc    *biz.EventUsecase
	guard bizGuard
}

// NewNotifyEventService new a NotifyEventService.
func NewNotifyEventService(uc *biz.EventUsecase, scope biz.BizScopeResolver) *NotifyEventService {
	return &NotifyEventService{uc: uc, guard: newBizGuard(scope)}
}

// toProtoEvent 领域模型转 proto 实体；payload 原始 JSON 转 Struct（HTTP
// 原生 JSON 对象渲染），非法 JSON 视为数据损坏直接报错（入口侧已校验）。
func toProtoEvent(event *biz.Event) (*notifyv1.NotifyEvent, error) {
	out := &notifyv1.NotifyEvent{
		Id:              event.ID,
		BizId:           event.BizID,
		EventType:       event.EventType,
		Severity:        event.Severity,
		Title:           event.Title,
		DedupKey:        event.DedupKey,
		GroupKey:        event.GroupKey,
		Recipients:      event.Recipients,
		Source:          event.Source,
		Status:          event.Status,
		PublishState:    event.PublishState,
		PublishAttempts: event.PublishAttempts,
		NextPublishTime: timestamppb.New(event.NextPublishTime),
		CreateTime:      timestamppb.New(event.CreateTime),
	}
	out.Payload = jsonToStruct(event.Payload)
	return out, nil
}

// ListEvents implements proto NotifyEventService/ListEvents：四维过滤透传。
func (s *NotifyEventService) ListEvents(ctx context.Context, req *notifyv1.ListEventsRequest) (*notifyv1.ListEventsResponse, error) {
	bizID, err := s.guard.listBiz(ctx, req.BizId)
	if err != nil {
		return nil, err
	}
	filter := biz.EventFilter{
		BizID:     bizID,
		EventType: req.GetEventType(),
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
	events, total, err := s.uc.List(ctx, filter, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &notifyv1.ListEventsResponse{
		Items: make([]*notifyv1.NotifyEvent, 0, len(events)),
		Total: total,
	}
	for _, event := range events {
		item, err := toProtoEvent(event)
		if err != nil {
			return nil, err
		}
		resp.Items = append(resp.Items, item)
	}
	return resp, nil
}
