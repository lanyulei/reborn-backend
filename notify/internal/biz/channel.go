package biz

import (
	"context"
	"errors"
)

// RenderedMessage 已渲染的待发送消息（渠道适配器入参）。
type RenderedMessage struct {
	EventID   string
	BizID     string
	EventType string
	Severity  int32
	Title     string
	Subject   string
	Body      string
	Payload   []byte
	Recipient string
	SendKey   string
	Agg       *AggContext
}

// SendResult 适配器返回：ProviderMsgID 渠道侧标识（可空）；Retryable=false
// 表示确定性失败（地址缺失/4xx/签名错）→ 立即死信。
type SendResult struct {
	ProviderMsgID string
	Retryable     bool
}

// ChannelSender 渠道适配器窄接口（设计 §4.6）：biz 定义，data/channel 实现；
// 单次调用须在 ctx 超时内返回（出站 ≤3s）。
type ChannelSender interface {
	// Supports 报告是否支持该渠道类型。
	Supports(channelType string) bool
	// Send 发送；err 非 nil 时按 Retryable 决定重试或死信。
	Send(ctx context.Context, ch *Channel, msg RenderedMessage) (SendResult, error)
}

// ErrChannelTypeUnsupported 渠道类型无适配器（P1 仅 email/webhook）→ 不可重试。
var ErrChannelTypeUnsupported = errors.New("channel type not supported by this pipeline build")

// SenderMux 按类型分派到适配器集合。
type SenderMux struct {
	senders []ChannelSender
}

// NewSenderMux new a SenderMux.
func NewSenderMux(senders ...ChannelSender) *SenderMux {
	return &SenderMux{senders: senders}
}

// Supports implements ChannelSender.
func (m *SenderMux) Supports(channelType string) bool {
	for _, s := range m.senders {
		if s.Supports(channelType) {
			return true
		}
	}
	return false
}

// Send implements ChannelSender。
func (m *SenderMux) Send(ctx context.Context, ch *Channel, msg RenderedMessage) (SendResult, error) {
	for _, s := range m.senders {
		if s.Supports(ch.Type) {
			return s.Send(ctx, ch, msg)
		}
	}
	return SendResult{Retryable: false}, ErrChannelTypeUnsupported
}
