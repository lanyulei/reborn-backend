package service

import (
	"context"
	"time"

	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// NotifyRouteService 实现 api/notify/v1 生成的 NotifyRouteServiceServer；
// proto 与领域模型显式转换，调优项默认值在此解析（proto 三态 optional：
// severity_min=1、priority=5、aggregate_window_s=0、suppress_threshold=500、
// suppress_duration_m=30、enabled=true）。
type NotifyRouteService struct {
	uc      *biz.RouteUsecase
	preview *biz.PreviewUsecase
	guard   bizGuard
}

// NewNotifyRouteService new a NotifyRouteService.
func NewNotifyRouteService(uc *biz.RouteUsecase, preview *biz.PreviewUsecase, scope biz.BizScopeResolver) *NotifyRouteService {
	return &NotifyRouteService{uc: uc, preview: preview, guard: newBizGuard(scope)}
}

// 路由规则调优项默认值（与 proto 注释一致）。
const (
	routeSeverityDefault    = 1
	routePriorityDefault    = 5
	routeThresholdDefault   = 500
	routeDurationDefaultMin = 30
)

// ruleFromProto 转换 proto 路由规则载荷并解析可选调优项默认值
// （nil 字段走 GetXxx 零值，此处显式兜底）。
func ruleFromProto(spec *notifyv1.RouteSpec) *biz.RouteRule {
	rule := &biz.RouteRule{
		Name:              spec.GetName(),
		Description:       spec.GetDescription(),
		EventPattern:      spec.GetEventPattern(),
		SeverityMin:       spec.GetSeverityMin(),
		TagMatch:          spec.GetTagMatch(),
		Priority:          spec.GetPriority(),
		ChannelIDs:        spec.GetChannelIds(),
		AggregateWindowS:  spec.GetAggregateWindowS(),
		SuppressThreshold: spec.GetSuppressThreshold(),
		SuppressDurationM: spec.GetSuppressDurationM(),
	}
	if spec.SeverityMin == nil {
		rule.SeverityMin = routeSeverityDefault
	}
	if spec.Priority == nil {
		rule.Priority = routePriorityDefault
	}
	if spec.SuppressThreshold == nil {
		rule.SuppressThreshold = routeThresholdDefault
	}
	if spec.SuppressDurationM == nil {
		rule.SuppressDurationM = routeDurationDefaultMin
	}
	// aggregate_window_s 缺省 0（不聚合），与零值一致，无需兜底。
	if r := spec.GetRecipient(); r != nil {
		rule.Recipient = biz.RouteRecipient{
			Fixed:        r.GetFixed(),
			Subscription: r.GetSubscription(),
		}
	}
	return rule
}

// toProtoRoute 领域模型转 proto 实体。
func toProtoRoute(route *biz.Route) *notifyv1.NotifyRoute {
	return &notifyv1.NotifyRoute{
		Id:           route.ID,
		BizId:        route.BizID,
		Name:         route.Name,
		Description:  route.Description,
		EventPattern: route.EventPattern,
		SeverityMin:  route.SeverityMin,
		TagMatch:     route.TagMatch,
		Priority:     route.Priority,
		ChannelIds:   route.ChannelIDs,
		Recipient: &notifyv1.RouteRecipient{
			Fixed:        route.Recipient.Fixed,
			Subscription: route.Recipient.Subscription,
		},
		AggregateWindowS:  route.AggregateWindowS,
		SuppressThreshold: route.SuppressThreshold,
		SuppressDurationM: route.SuppressDurationM,
		Enabled:           route.Enabled,
		CreateTime:        timestamppb.New(route.CreateTime),
		UpdateTime:        timestamppb.New(route.UpdateTime),
	}
}

// CreateRoute implements proto NotifyRouteService/CreateRoute.
func (s *NotifyRouteService) CreateRoute(ctx context.Context, req *notifyv1.CreateRouteRequest) (*notifyv1.CreateRouteResponse, error) {
	rule := ruleFromProto(req.GetSpec())
	enabled := true // 与渠道/模板一致：未指定默认启用
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	if err := s.guard.requireBiz(ctx, req.GetBizId()); err != nil {
		return nil, err
	}
	route, err := s.uc.Create(ctx, &biz.Route{
		BizID:     req.GetBizId(),
		RouteRule: *rule,
		Enabled:   enabled,
	})
	if err != nil {
		return nil, err
	}
	return &notifyv1.CreateRouteResponse{Route: toProtoRoute(route)}, nil
}

// UpdateRoute implements proto NotifyRouteService/UpdateRoute：spec 整组替换
// 或 enabled 独立启停。
func (s *NotifyRouteService) UpdateRoute(ctx context.Context, req *notifyv1.UpdateRouteRequest) (*notifyv1.UpdateRouteResponse, error) {
	upd := biz.RouteUpdate{Enabled: req.Enabled}
	if req.Spec != nil {
		upd.Rule = ruleFromProto(req.GetSpec())
	}
	if err := s.guard.requireRow(ctx, s.bizOf(ctx, req.GetId())); err != nil {
		return nil, err
	}
	if err := s.uc.Update(ctx, req.GetId(), upd); err != nil {
		return nil, err
	}
	route, err := s.uc.Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &notifyv1.UpdateRouteResponse{Route: toProtoRoute(route)}, nil
}

// DeleteRoute implements proto NotifyRouteService/DeleteRoute。
func (s *NotifyRouteService) DeleteRoute(ctx context.Context, req *notifyv1.DeleteRouteRequest) (*notifyv1.DeleteRouteResponse, error) {
	if err := s.guard.requireRow(ctx, s.bizOf(ctx, req.GetId())); err != nil {
		return nil, err
	}
	if err := s.uc.Delete(ctx, req.GetId()); err != nil {
		return nil, err
	}
	return &notifyv1.DeleteRouteResponse{}, nil
}

// GetRoute implements proto NotifyRouteService/GetRoute。
func (s *NotifyRouteService) GetRoute(ctx context.Context, req *notifyv1.GetRouteRequest) (*notifyv1.GetRouteResponse, error) {
	route, err := s.uc.Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	if err := s.guard.requireBiz(ctx, route.BizID); err != nil {
		return nil, err
	}
	return &notifyv1.GetRouteResponse{Route: toProtoRoute(route)}, nil
}

// ListRoutes implements proto NotifyRouteService/ListRoutes。
func (s *NotifyRouteService) ListRoutes(ctx context.Context, req *notifyv1.ListRoutesRequest) (*notifyv1.ListRoutesResponse, error) {
	bizID, err := s.guard.listBiz(ctx, req.BizId)
	if err != nil {
		return nil, err
	}
	filter := biz.RouteFilter{
		BizID:   bizID,
		Keyword: req.GetKeyword(),
		Enabled: req.Enabled,
	}
	routes, total, err := s.uc.List(ctx, filter, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &notifyv1.ListRoutesResponse{
		Items: make([]*notifyv1.NotifyRoute, 0, len(routes)),
		Total: total,
	}
	for _, route := range routes {
		resp.Items = append(resp.Items, toProtoRoute(route))
	}
	return resp, nil
}

// bizOf 返回「读路由取其 biz」的惰性回调（受限用户才执行）。
func (s *NotifyRouteService) bizOf(ctx context.Context, id string) func() (string, error) {
	return func() (string, error) {
		row, err := s.uc.Get(ctx, id)
		if err != nil {
			return "", err
		}
		return row.BizID, nil
	}
}

// PreviewRouting implements proto NotifyRouteService/PreviewRouting：只读试算；
// 受限用户须目标 biz 在范围内。
func (s *NotifyRouteService) PreviewRouting(ctx context.Context, req *notifyv1.PreviewRoutingRequest) (*notifyv1.PreviewRoutingResponse, error) {
	if err := s.guard.requireBiz(ctx, req.GetBizId()); err != nil {
		return nil, err
	}
	in := biz.PreviewInput{
		BizID: req.GetBizId(), EventType: req.GetEventType(), Severity: req.GetSeverity(), GroupKey: req.GetGroupKey(),
	}
	if p := req.GetPayload(); p != nil {
		in.Payload = p.AsMap()
	}
	out, err := s.preview.Preview(ctx, in, time.Now())
	if err != nil {
		return nil, err
	}
	resp := &notifyv1.PreviewRoutingResponse{Silenced: out.Silenced, SilenceIds: out.SilenceIDs, Aggregate: out.Aggregate}
	if out.Matched != nil {
		resp.Matched = toProtoRoute(out.Matched)
	}
	for _, r := range out.Candidates {
		resp.Candidates = append(resp.Candidates, toProtoRoute(r))
	}
	for _, pc := range out.Channels {
		resp.Channels = append(resp.Channels, &notifyv1.PreviewChannel{
			Id: pc.Channel.ID, Name: pc.Channel.Name, Type: pc.Channel.Type, Enabled: pc.Channel.Enabled, TemplateId: pc.TemplateID,
		})
	}
	return resp, nil
}
