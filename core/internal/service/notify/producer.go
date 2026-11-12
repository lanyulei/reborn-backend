package service

import (
	"context"

	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// NotifyProducerService 实现 api/notify/v1 生成的 NotifyProducerServiceServer；
// token 明文只在创建/轮换响应出现一次（toProtoProducer 恒不携带 token 与
// 哈希），更新语义与路由/静默一致：spec 整组替换 + enabled 独立启停。
type NotifyProducerService struct {
	uc    *biz.ProducerUsecase
	guard bizGuard
}

// NewNotifyProducerService new a NotifyProducerService.
func NewNotifyProducerService(uc *biz.ProducerUsecase, scope biz.BizScopeResolver) *NotifyProducerService {
	return &NotifyProducerService{uc: uc, guard: newBizGuard(scope)}
}

// specFromProto 转换 proto 生产者配置载荷（expire_time 未设置 = nil 永不过期）。
func specFromProto(spec *notifyv1.ProducerSpec) *biz.ProducerSpec {
	out := &biz.ProducerSpec{
		Name:           spec.GetName(),
		EventWhitelist: spec.GetEventWhitelist(),
		QPSLimit:       spec.GetQpsLimit(),
		DailyLimit:     spec.GetDailyLimit(),
	}
	if ts := spec.GetExpireTime(); ts != nil {
		t := ts.AsTime()
		out.ExpireTime = &t
	}
	return out
}

// toProtoProducer 领域模型转 proto 实体（不含 token 与哈希）。
func toProtoProducer(producer *biz.Producer) *notifyv1.NotifyProducer {
	out := &notifyv1.NotifyProducer{
		Id:             producer.ID,
		Name:           producer.Name,
		BizId:          producer.BizID,
		EventWhitelist: producer.EventWhitelist,
		QpsLimit:       producer.QPSLimit,
		DailyLimit:     producer.DailyLimit,
		Enabled:        producer.Enabled,
		CreateTime:     timestamppb.New(producer.CreateTime),
		UpdateTime:     timestamppb.New(producer.UpdateTime),
	}
	if producer.ExpireTime != nil {
		out.ExpireTime = timestamppb.New(*producer.ExpireTime)
	}
	return out
}

// CreateProducer implements proto NotifyProducerService/CreateProducer：
// 创建并签发 token（明文仅本响应一次）。
func (s *NotifyProducerService) CreateProducer(ctx context.Context, req *notifyv1.CreateProducerRequest) (*notifyv1.CreateProducerResponse, error) {
	enabled := true // 与渠道/模板/路由一致：未指定默认启用
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	if err := s.guard.requireBiz(ctx, req.GetBizId()); err != nil {
		return nil, err
	}
	producer, token, err := s.uc.Create(ctx, &biz.Producer{
		BizID:        req.GetBizId(),
		ProducerSpec: *specFromProto(req.GetSpec()),
		Enabled:      enabled,
	})
	if err != nil {
		return nil, err
	}
	return &notifyv1.CreateProducerResponse{
		Producer: toProtoProducer(producer),
		Token:    token,
	}, nil
}

// UpdateProducer implements proto NotifyProducerService/UpdateProducer：
// spec 整组替换或 enabled 独立启停。
func (s *NotifyProducerService) UpdateProducer(ctx context.Context, req *notifyv1.UpdateProducerRequest) (*notifyv1.UpdateProducerResponse, error) {
	upd := biz.ProducerUpdate{Enabled: req.Enabled}
	if req.Spec != nil {
		upd.Spec = specFromProto(req.GetSpec())
	}
	if err := s.guard.requireRow(ctx, s.bizOf(ctx, req.GetId())); err != nil {
		return nil, err
	}
	if err := s.uc.Update(ctx, req.GetId(), upd); err != nil {
		return nil, err
	}
	producer, err := s.uc.Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &notifyv1.UpdateProducerResponse{Producer: toProtoProducer(producer)}, nil
}

// RotateProducerToken implements proto NotifyProducerService/RotateProducerToken：
// 轮换 token（新明文仅本响应一次，旧 token 立即失效）。
func (s *NotifyProducerService) RotateProducerToken(ctx context.Context, req *notifyv1.RotateProducerTokenRequest) (*notifyv1.RotateProducerTokenResponse, error) {
	if err := s.guard.requireRow(ctx, s.bizOf(ctx, req.GetId())); err != nil {
		return nil, err
	}
	producer, token, err := s.uc.RotateToken(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &notifyv1.RotateProducerTokenResponse{
		Producer: toProtoProducer(producer),
		Token:    token,
	}, nil
}

// DeleteProducer implements proto NotifyProducerService/DeleteProducer。
func (s *NotifyProducerService) DeleteProducer(ctx context.Context, req *notifyv1.DeleteProducerRequest) (*notifyv1.DeleteProducerResponse, error) {
	if err := s.guard.requireRow(ctx, s.bizOf(ctx, req.GetId())); err != nil {
		return nil, err
	}
	if err := s.uc.Delete(ctx, req.GetId()); err != nil {
		return nil, err
	}
	return &notifyv1.DeleteProducerResponse{}, nil
}

// GetProducer implements proto NotifyProducerService/GetProducer。
func (s *NotifyProducerService) GetProducer(ctx context.Context, req *notifyv1.GetProducerRequest) (*notifyv1.GetProducerResponse, error) {
	producer, err := s.uc.Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	if err := s.guard.requireBiz(ctx, producer.BizID); err != nil {
		return nil, err
	}
	return &notifyv1.GetProducerResponse{Producer: toProtoProducer(producer)}, nil
}

// ListProducers implements proto NotifyProducerService/ListProducers。
func (s *NotifyProducerService) ListProducers(ctx context.Context, req *notifyv1.ListProducersRequest) (*notifyv1.ListProducersResponse, error) {
	bizID, err := s.guard.listBiz(ctx, req.BizId)
	if err != nil {
		return nil, err
	}
	filter := biz.ProducerFilter{
		BizID:   bizID,
		Keyword: req.GetKeyword(),
		Enabled: req.Enabled,
	}
	producers, total, err := s.uc.List(ctx, filter, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &notifyv1.ListProducersResponse{
		Items: make([]*notifyv1.NotifyProducer, 0, len(producers)),
		Total: total,
	}
	for _, producer := range producers {
		resp.Items = append(resp.Items, toProtoProducer(producer))
	}
	return resp, nil
}

// bizOf 返回「读生产者取其 biz」的惰性回调（受限用户才执行）。
func (s *NotifyProducerService) bizOf(ctx context.Context, id string) func() (string, error) {
	return func() (string, error) {
		row, err := s.uc.Get(ctx, id)
		if err != nil {
			return "", err
		}
		return row.BizID, nil
	}
}
