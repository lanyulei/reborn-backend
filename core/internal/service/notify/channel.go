// Package service 实现 notify 域 proto 生成的服务接口：只做参数编排、模型转换
// 与错误透传，业务规则全部收敛在 biz/notify。
package service

import (
	"context"
	"fmt"
	"time"

	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// NotifyChannelService 实现 api/notify/v1 生成的 NotifyChannelServiceServer。
type NotifyChannelService struct {
	uc     *biz.ChannelUsecase
	health *biz.ChannelHealthUsecase
	guard  bizGuard
}

// NewNotifyChannelService new a NotifyChannelService.
func NewNotifyChannelService(uc *biz.ChannelUsecase, health *biz.ChannelHealthUsecase, scope biz.BizScopeResolver) *NotifyChannelService {
	return &NotifyChannelService{uc: uc, health: health, guard: newBizGuard(scope)}
}

// channelTypeEnum 是 proto 枚举 → 落库类型字符串映射。
var channelTypeEnum = map[notifyv1.NotifyChannelType]string{
	notifyv1.NotifyChannelType_NOTIFY_CHANNEL_TYPE_EMAIL:    biz.ChannelTypeEmail,
	notifyv1.NotifyChannelType_NOTIFY_CHANNEL_TYPE_WEBHOOK:  biz.ChannelTypeWebhook,
	notifyv1.NotifyChannelType_NOTIFY_CHANNEL_TYPE_DINGTALK: biz.ChannelTypeDingtalk,
	notifyv1.NotifyChannelType_NOTIFY_CHANNEL_TYPE_WECOM:    biz.ChannelTypeWecom,
	notifyv1.NotifyChannelType_NOTIFY_CHANNEL_TYPE_FEISHU:   biz.ChannelTypeFeishu,
}

// channelTypeString 是落库类型字符串 → proto 枚举映射（与 channelTypeEnum 互逆）。
var channelTypeString = map[string]notifyv1.NotifyChannelType{
	biz.ChannelTypeEmail:    notifyv1.NotifyChannelType_NOTIFY_CHANNEL_TYPE_EMAIL,
	biz.ChannelTypeWebhook:  notifyv1.NotifyChannelType_NOTIFY_CHANNEL_TYPE_WEBHOOK,
	biz.ChannelTypeDingtalk: notifyv1.NotifyChannelType_NOTIFY_CHANNEL_TYPE_DINGTALK,
	biz.ChannelTypeWecom:    notifyv1.NotifyChannelType_NOTIFY_CHANNEL_TYPE_WECOM,
	biz.ChannelTypeFeishu:   notifyv1.NotifyChannelType_NOTIFY_CHANNEL_TYPE_FEISHU,
}

// emailFromProto 将 proto 邮件配置转为 DO。
func emailFromProto(p *notifyv1.EmailChannelConfig) *biz.EmailConfig {
	if p == nil {
		return nil
	}
	return &biz.EmailConfig{
		Host:     p.GetHost(),
		Port:     p.GetPort(),
		Username: p.GetUsername(),
		FromAddr: p.GetFromAddr(),
		StartTLS: p.GetStarttls(),
	}
}

// webhookFromProto 将 proto Webhook 配置转为 DO；timeout_s 未指定时取缺省值。
func webhookFromProto(p *notifyv1.WebhookChannelConfig) *biz.WebhookConfig {
	if p == nil {
		return nil
	}
	timeout := biz.DefaultWebhookTimeoutS
	if p.TimeoutS != nil {
		timeout = p.GetTimeoutS()
	}
	return &biz.WebhookConfig{
		URL:      p.GetUrl(),
		Method:   p.GetMethod(),
		Headers:  p.GetHeaders(),
		TimeoutS: timeout,
	}
}

// robotFromProto 将 proto 群机器人配置转为 DO。
func robotFromProto(p *notifyv1.RobotChannelConfig) *biz.RobotConfig {
	if p == nil {
		return nil
	}
	return &biz.RobotConfig{AtMobiles: p.GetAtMobiles(), CardURL: p.GetCardUrl(), CardButton: p.GetCardButton()}
}

// emailToProto 将 DO 邮件配置转为 proto。
func emailToProto(c *biz.EmailConfig) *notifyv1.EmailChannelConfig {
	if c == nil {
		return nil
	}
	return &notifyv1.EmailChannelConfig{
		Host:     c.Host,
		Port:     c.Port,
		Username: c.Username,
		FromAddr: c.FromAddr,
		Starttls: c.StartTLS,
	}
}

// webhookToProto 将 DO Webhook 配置转为 proto。
func webhookToProto(c *biz.WebhookConfig) *notifyv1.WebhookChannelConfig {
	if c == nil {
		return nil
	}
	return &notifyv1.WebhookChannelConfig{
		Url:      c.URL,
		Method:   c.Method,
		Headers:  c.Headers,
		TimeoutS: &c.TimeoutS,
	}
}

// robotToProto 将 DO 群机器人配置转为 proto。
func robotToProto(c *biz.RobotConfig) *notifyv1.RobotChannelConfig {
	if c == nil {
		return nil
	}
	out := &notifyv1.RobotChannelConfig{AtMobiles: c.AtMobiles}
	if c.CardURL != "" {
		out.CardUrl = &c.CardURL
	}
	if c.CardButton != "" {
		out.CardButton = &c.CardButton
	}
	return out
}

// configFromCreateDetail 将创建请求的类型化 oneof 转为 DO 配置；
// 生成代码的 oneof 接口未导出，此处经 GetDetail 断言具体包装类型。
func configFromCreateDetail(req *notifyv1.CreateChannelRequest) *biz.ChannelConfig {
	switch v := req.GetDetail().(type) {
	case *notifyv1.CreateChannelRequest_Email:
		return &biz.ChannelConfig{Email: emailFromProto(v.Email)}
	case *notifyv1.CreateChannelRequest_Webhook:
		return &biz.ChannelConfig{Webhook: webhookFromProto(v.Webhook)}
	case *notifyv1.CreateChannelRequest_Robot:
		return &biz.ChannelConfig{Robot: robotFromProto(v.Robot)}
	}
	return nil
}

// configFromUpdateDetail 将更新请求的类型化 oneof 转为 DO 配置。
func configFromUpdateDetail(req *notifyv1.UpdateChannelRequest) *biz.ChannelConfig {
	switch v := req.GetDetail().(type) {
	case *notifyv1.UpdateChannelRequest_Email:
		return &biz.ChannelConfig{Email: emailFromProto(v.Email)}
	case *notifyv1.UpdateChannelRequest_Webhook:
		return &biz.ChannelConfig{Webhook: webhookFromProto(v.Webhook)}
	case *notifyv1.UpdateChannelRequest_Robot:
		return &biz.ChannelConfig{Robot: robotFromProto(v.Robot)}
	}
	return nil
}

// rateLimitFromProto 将可选 proto 限流转为 DO 指针；nil 表示本次不更新/不限。
func rateLimitFromProto(p *notifyv1.NotifyRateLimit) *biz.RateLimit {
	if p == nil {
		return nil
	}
	return &biz.RateLimit{QPS: p.GetQps(), Burst: p.GetBurst(), PerMinute: p.GetPerMinute()}
}

// toProtoChannel 将 DO 显式转换为 proto 实体（secret 不存在出参语义）。
func toProtoChannel(ch *biz.Channel) *notifyv1.NotifyChannel {
	out := &notifyv1.NotifyChannel{
		Id:         ch.ID,
		BizId:      ch.BizID,
		Type:       channelTypeString[ch.Type],
		Name:       ch.Name,
		RateLimit:  &notifyv1.NotifyRateLimit{Qps: ch.RateLimit.QPS, Burst: ch.RateLimit.Burst, PerMinute: ch.RateLimit.PerMinute},
		Enabled:    ch.Enabled,
		CreateTime: timestamppb.New(ch.CreateTime),
		UpdateTime: timestamppb.New(ch.UpdateTime),
	}
	switch {
	case ch.Config == nil:
	case ch.Config.Email != nil:
		out.Detail = &notifyv1.NotifyChannel_Email{Email: emailToProto(ch.Config.Email)}
	case ch.Config.Webhook != nil:
		out.Detail = &notifyv1.NotifyChannel_Webhook{Webhook: webhookToProto(ch.Config.Webhook)}
	case ch.Config.Robot != nil:
		out.Detail = &notifyv1.NotifyChannel_Robot{Robot: robotToProto(ch.Config.Robot)}
	}
	return out
}

// CreateChannel implements proto NotifyChannelService/CreateChannel。
func (s *NotifyChannelService) CreateChannel(ctx context.Context, req *notifyv1.CreateChannelRequest) (*notifyv1.CreateChannelResponse, error) {
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	if err := s.guard.requireBiz(ctx, req.GetBizId()); err != nil {
		return nil, err
	}
	ch, err := s.uc.Create(ctx, &biz.Channel{
		BizID:      req.GetBizId(),
		Type:       channelTypeEnum[req.GetType()],
		Name:       req.GetName(),
		Config:     configFromCreateDetail(req),
		Secret:     req.GetSecret(),
		SignSecret: req.GetSignSecret(),
		RateLimit:  derefRateLimit(rateLimitFromProto(req.GetRateLimit())),
		Enabled:    enabled,
	})
	if err != nil {
		return nil, err
	}
	return &notifyv1.CreateChannelResponse{Channel: toProtoChannel(ch)}, nil
}

// UpdateChannel implements proto NotifyChannelService/UpdateChannel；
// 未传的 detail 保持 nil（biz 不更新 config）。
func (s *NotifyChannelService) UpdateChannel(ctx context.Context, req *notifyv1.UpdateChannelRequest) (*notifyv1.UpdateChannelResponse, error) {
	upd := biz.ChannelUpdate{
		Name:       req.Name,
		RateLimit:  rateLimitFromProto(req.GetRateLimit()),
		Enabled:    req.Enabled,
		Secret:     req.Secret,
		SignSecret: req.SignSecret,
	}
	if req.GetDetail() != nil {
		upd.Config = configFromUpdateDetail(req)
	}
	if err := s.guard.requireRow(ctx, s.bizOf(ctx, req.GetId())); err != nil {
		return nil, err
	}
	if err := s.uc.Update(ctx, req.GetId(), upd); err != nil {
		return nil, err
	}
	ch, err := s.uc.Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &notifyv1.UpdateChannelResponse{Channel: toProtoChannel(ch)}, nil
}

// DeleteChannel implements proto NotifyChannelService/DeleteChannel。
func (s *NotifyChannelService) DeleteChannel(ctx context.Context, req *notifyv1.DeleteChannelRequest) (*notifyv1.DeleteChannelResponse, error) {
	if err := s.guard.requireRow(ctx, s.bizOf(ctx, req.GetId())); err != nil {
		return nil, err
	}
	if err := s.uc.Delete(ctx, req.GetId()); err != nil {
		return nil, err
	}
	return &notifyv1.DeleteChannelResponse{}, nil
}

// GetChannel implements proto NotifyChannelService/GetChannel。
func (s *NotifyChannelService) GetChannel(ctx context.Context, req *notifyv1.GetChannelRequest) (*notifyv1.GetChannelResponse, error) {
	ch, err := s.uc.Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	if err := s.guard.requireBiz(ctx, ch.BizID); err != nil {
		return nil, err
	}
	return &notifyv1.GetChannelResponse{Channel: toProtoChannel(ch)}, nil
}

// ListChannels implements proto NotifyChannelService/ListChannels；
// type 过滤仅在显式指定（非 UNSPECIFIED）时生效。
func (s *NotifyChannelService) ListChannels(ctx context.Context, req *notifyv1.ListChannelsRequest) (*notifyv1.ListChannelsResponse, error) {
	bizID, err := s.guard.listBiz(ctx, req.BizId)
	if err != nil {
		return nil, err
	}
	filter := biz.ChannelFilter{BizID: bizID, Keyword: req.GetKeyword(), Enabled: req.Enabled}
	if req.Type != nil && *req.Type != notifyv1.NotifyChannelType_NOTIFY_CHANNEL_TYPE_UNSPECIFIED {
		typ := channelTypeEnum[*req.Type]
		filter.Type = &typ
	}
	channels, total, err := s.uc.List(ctx, filter, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &notifyv1.ListChannelsResponse{
		Items: make([]*notifyv1.NotifyChannel, 0, len(channels)),
		Total: total,
	}
	for _, ch := range channels {
		resp.Items = append(resp.Items, toProtoChannel(ch))
	}
	return resp, nil
}

// derefRateLimit 解引用限流指针；nil 返回零值（qps=0/burst=0 即不限）。
func derefRateLimit(rl *biz.RateLimit) biz.RateLimit {
	if rl == nil {
		return biz.RateLimit{}
	}
	return *rl
}

// SendTestEvent implements proto NotifyChannelService/SendTestEvent：可覆盖项
// 透传 biz（空值在 biz 取缺省），payload Struct 序列化为原始 JSON 字节。
func (s *NotifyChannelService) SendTestEvent(ctx context.Context, req *notifyv1.SendTestEventRequest) (*notifyv1.SendTestEventResponse, error) {
	spec := biz.TestEventSpec{
		EventType:  req.GetEventType(),
		Title:      req.GetTitle(),
		Recipients: req.GetRecipients(),
	}
	if p := req.GetPayload(); p != nil {
		raw, err := protojson.Marshal(p)
		if err != nil {
			return nil, fmt.Errorf("encode payload: %w", err)
		}
		spec.Payload = raw
	}
	if err := s.guard.requireRow(ctx, s.bizOf(ctx, req.GetId())); err != nil {
		return nil, err
	}
	event, err := s.uc.SendTestEvent(ctx, req.GetId(), spec)
	if err != nil {
		return nil, err
	}
	item, err := toProtoEvent(event)
	if err != nil {
		return nil, err
	}
	return &notifyv1.SendTestEventResponse{Event: item}, nil
}

// bizOf 返回「读渠道取其 biz」的惰性回调（受限用户才执行）。
func (s *NotifyChannelService) bizOf(ctx context.Context, id string) func() (string, error) {
	return func() (string, error) {
		ch, err := s.uc.Get(ctx, id)
		if err != nil {
			return "", err
		}
		return ch.BizID, nil
	}
}

// ListChannelHealth implements proto NotifyChannelService/ListChannelHealth：受限
// 用户须携带 biz_id 且在范围内（与列表同口径）。
func (s *NotifyChannelService) ListChannelHealth(ctx context.Context, req *notifyv1.ListChannelHealthRequest) (*notifyv1.ListChannelHealthResponse, error) {
	bizID, err := s.guard.listBiz(ctx, req.BizId)
	if err != nil {
		return nil, err
	}
	items, err := s.health.List(ctx, bizID)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	resp := &notifyv1.ListChannelHealthResponse{Items: make([]*notifyv1.NotifyChannelHealth, 0, len(items))}
	for _, h := range items {
		item := &notifyv1.NotifyChannelHealth{
			ChannelId: h.ChannelID, ChannelName: h.ChannelName, ChannelType: h.ChannelType, BizId: h.BizID,
			Samples: h.Samples, Failures: h.Failures, Open: h.Open(now), Probing: h.Probing, UpdateTime: timestamppb.New(h.UpdateTime),
		}
		if h.OpenUntil != nil {
			item.OpenUntil = timestamppb.New(*h.OpenUntil)
		}
		resp.Items = append(resp.Items, item)
	}
	return resp, nil
}
