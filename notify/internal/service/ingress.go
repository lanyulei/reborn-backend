// Package service 实现 proto 生成的 ingress 服务接口：只做参数编排、模型转换
// 与错误透传（鉴权在 server 中间件，业务规则在 biz）。
package service

import (
	"context"

	ingressv1 "github.com/lanyulei/reborn/backend/notify/api/ingress/v1"
	"github.com/lanyulei/reborn/backend/notify/internal/biz"

	"github.com/google/wire"
)

// ProviderSet is service providers.
var ProviderSet = wire.NewSet(NewIngressService)

// ProducerResolver 从 ctx 取鉴权中间件注入的生产者（server 层实现，避免 service
// 反向依赖 server）。
type ProducerResolver func(ctx context.Context) (*biz.Producer, bool)

// IngressService 实现 NotifyIngressServiceHTTPServer（数据面仅 HTTP）。
type IngressService struct {
	uc       *biz.IngressUsecase
	producer ProducerResolver
}

// NewIngressService new an IngressService.
func NewIngressService(uc *biz.IngressUsecase, resolver ProducerResolver) *IngressService {
	return &IngressService{uc: uc, producer: resolver}
}

// SendEvent implements NotifyIngressServiceHTTPServer.
func (s *IngressService) SendEvent(ctx context.Context, req *ingressv1.SendEventRequest) (*ingressv1.SendEventResponse, error) {
	results, err := s.accept(ctx, []*ingressv1.IngressEvent{req.GetEvent()})
	if err != nil {
		return nil, err
	}
	return results[0], nil
}

// BatchSend implements NotifyIngressServiceHTTPServer.
func (s *IngressService) BatchSend(ctx context.Context, req *ingressv1.BatchSendRequest) (*ingressv1.BatchSendResponse, error) {
	results, err := s.accept(ctx, req.GetEvents())
	if err != nil {
		return nil, err
	}
	return &ingressv1.BatchSendResponse{Results: results}, nil
}

func (s *IngressService) accept(ctx context.Context, in []*ingressv1.IngressEvent) ([]*ingressv1.SendEventResponse, error) {
	producer, ok := s.producer(ctx)
	if !ok {
		return nil, biz.ErrUnauthorized
	}
	events := make([]*biz.Event, 0, len(in))
	for _, e := range in {
		events = append(events, &biz.Event{
			BizID: e.GetBizId(), EventType: e.GetEventType(), Severity: e.GetSeverity(), Title: e.GetTitle(),
			Payload: e.GetPayload(), DedupKey: e.GetDedupKey(), GroupKey: e.GetGroupKey(), Recipients: e.GetRecipients(),
		})
	}
	results, err := s.uc.Accept(ctx, producer, events)
	if err != nil {
		return nil, err
	}
	out := make([]*ingressv1.SendEventResponse, 0, len(results))
	for _, r := range results {
		out = append(out, &ingressv1.SendEventResponse{EventId: r.EventID, Deduplicated: r.Deduplicated})
	}
	return out, nil
}
