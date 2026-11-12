package service

import (
	"context"
	"time"

	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	"github.com/lanyulei/reborn/backend/kit/paging"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// NotifyEventService 实现 api/notify/v1 生成的 NotifyEventServiceServer：
// 运行表查询（管道写 / core 读）、事件下钻与事件级重发（经 EventWriter 直插）。
type NotifyEventService struct {
	uc         *biz.EventUsecase
	dispatches *biz.DispatchUsecase
	logs       *biz.DeliveryLogUsecase
	guard      bizGuard
}

// NewNotifyEventService new a NotifyEventService.
func NewNotifyEventService(uc *biz.EventUsecase, dispatches *biz.DispatchUsecase, logs *biz.DeliveryLogUsecase, scope biz.BizScopeResolver) *NotifyEventService {
	return &NotifyEventService{uc: uc, dispatches: dispatches, logs: logs, guard: newBizGuard(scope)}
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
		Severity:  req.Severity,
		Source:    req.GetSource(),
		Keyword:   req.GetKeyword(),
		GroupKey:  req.GetGroupKey(),
	}
	start, end, err := listTimeRange(req.GetCreateTimeStart(), req.GetCreateTimeEnd(), time.Now())
	if err != nil {
		return nil, err
	}
	filter.CreateTimeStart, filter.CreateTimeEnd = start, end
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

// GetEvent implements proto NotifyEventService/GetEvent：事件 + 其发送任务（≤100，
// 生成时间倒序）+ 每个任务最近一次投递明细；受限用户须事件 biz 在范围内。
func (s *NotifyEventService) GetEvent(ctx context.Context, req *notifyv1.GetEventRequest) (*notifyv1.GetEventResponse, error) {
	event, err := s.uc.Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	if err := s.guard.requireBiz(ctx, event.BizID); err != nil {
		return nil, err
	}
	item, err := toProtoEvent(event)
	if err != nil {
		return nil, err
	}
	resp := &notifyv1.GetEventResponse{Event: item}
	dispatches, _, err := s.dispatches.List(ctx, biz.DispatchFilter{EventID: event.ID}, 1, paging.MaxPageSize)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(dispatches))
	for _, d := range dispatches {
		pd, err := toProtoDispatch(d)
		if err != nil {
			return nil, err
		}
		resp.Dispatches = append(resp.Dispatches, pd)
		ids = append(ids, d.ID)
	}
	logs, err := s.logs.Latest(ctx, ids)
	if err != nil {
		return nil, err
	}
	for _, log := range logs {
		resp.LatestLogs = append(resp.LatestLogs, toProtoDeliveryLog(log))
	}
	return resp, nil
}

// ReplayEvent implements proto NotifyEventService/ReplayEvent：按源事件直插一条
// 新事件（source=internal、新 id、不带 dedup_key）；受限用户须事件 biz 在范围内。
func (s *NotifyEventService) ReplayEvent(ctx context.Context, req *notifyv1.ReplayEventRequest) (*notifyv1.ReplayEventResponse, error) {
	source, err := s.uc.Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	if err := s.guard.requireBiz(ctx, source.BizID); err != nil {
		return nil, err
	}
	event, err := s.uc.Replay(ctx, source)
	if err != nil {
		return nil, err
	}
	item, err := toProtoEvent(event)
	if err != nil {
		return nil, err
	}
	return &notifyv1.ReplayEventResponse{Event: item}, nil
}
