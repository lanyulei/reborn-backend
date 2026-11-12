package biz

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/lanyulei/reborn/backend/kit/notifycontract"
)

// 状态/来源常量：全部取自 kit/notifycontract（与 core biz 同一字面量口径）。
const (
	EventSourceAPI      = notifycontract.EventSourceAPI
	EventSourceInternal = notifycontract.EventSourceInternal

	EventStatusReceived   = notifycontract.EventStatusReceived
	EventStatusRouted     = notifycontract.EventStatusRouted
	EventStatusSuppressed = notifycontract.EventStatusSuppressed
	EventStatusFailed     = notifycontract.EventStatusFailed

	// PublishPending/PublishPublished：publish_state。PG 骨干下 published 语义
	// = 已交付 router 处理（与事件终态同事务翻转）。
	PublishPending   = notifycontract.PublishPending
	PublishPublished = notifycontract.PublishPublished
)

// 发送任务状态（notify_dispatch.status）。
const (
	DispatchPending = notifycontract.DispatchPending
	DispatchSending = notifycontract.DispatchSending
	DispatchSent    = notifycontract.DispatchSent
	DispatchFailed  = notifycontract.DispatchFailed
	DispatchDead    = notifycontract.DispatchDead
)

// 死信重放状态（notify_dlq.replay_state）。
const (
	ReplayNone     = notifycontract.ReplayNone
	ReplayPending  = notifycontract.ReplayPending
	ReplayReplayed = notifycontract.ReplayReplayed
)

// 聚合组状态（notify_agg_group.status）。
const (
	AggOpen   = notifycontract.AggOpen
	AggClosed = notifycontract.AggClosed
)

// Event 事件领域模型（notify_event 一行）。Payload 为原始 JSON 字节
// （nil=无载荷），Recipients 空=交路由解析。
type Event struct {
	ID              string
	BizID           string
	EventType       string
	Severity        int32
	Title           string
	Payload         []byte
	DedupKey        string
	GroupKey        string
	Recipients      []string
	Source          string
	Status          int32
	PublishState    int32
	PublishAttempts int32
	NextPublishTime time.Time
	CreateTime      time.Time
}

// PayloadObject 把 payload 解码为对象；非对象/非法 JSON 返回 nil。
func (e *Event) PayloadObject() map[string]any {
	if len(e.Payload) == 0 {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(e.Payload, &m); err != nil {
		return nil
	}
	return m
}

// PayloadValue 把 payload 解码为模板 .Payload 注入值：恒为对象（map），非对象载荷
// （数组/标量/非法 JSON）包成 {"_raw": 值}——core 写入校验的试渲染按 map 口径执行，
// 模板里 {{.Payload.host}} 对任何载荷形态都只会得到 "<no value>" 而非执行错误死信。
func (e *Event) PayloadValue() any {
	if len(e.Payload) == 0 {
		return nil
	}
	var v any
	if err := json.Unmarshal(e.Payload, &v); err != nil {
		return map[string]any{"_raw": string(e.Payload)}
	}
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return map[string]any{"_raw": v}
}

// Dispatch 发送任务领域模型（notify_dispatch 一行）。TemplateID 空 = NULL；
// Content 为 jsonb 快照（DispatchContent 编码），nil = 未渲染。
type Dispatch struct {
	ID            string
	EventID       string
	ChannelID     string
	Recipient     string
	TemplateID    string
	Content       []byte
	Status        int32
	Attempts      int32
	NextRetryTime time.Time
	LastError     string
	SendKey       string
	CreateTime    time.Time
}

// DispatchContent 是 dispatch.content 的结构：渲染前仅含聚合上下文（Agg），
// 首次尝试渲染后补 Subject/Body/Template*，重试复用快照不重渲染。
type DispatchContent struct {
	Subject         string      `json:"subject,omitempty"`
	Body            string      `json:"body,omitempty"`
	Format          string      `json:"format,omitempty"` // 生效输出格式（随快照固化，重试不变）
	TemplateID      string      `json:"template_id,omitempty"`
	TemplateVersion int32       `json:"template_version,omitempty"`
	Agg             *AggContext `json:"agg,omitempty"`
}

// Rendered 报告快照是否已含渲染结果。
func (c *DispatchContent) Rendered() bool {
	return c != nil && (c.Subject != "" || c.Body != "")
}

// AggContext 聚合汇总上下文（sweeper 关闭窗口时写入汇总 dispatch）：
// Digest=true 为超阈值摘要模式（不列明细）。
type AggContext struct {
	Count   int         `json:"count"`
	Samples []AggSample `json:"samples,omitempty"`
	Digest  bool        `json:"digest,omitempty"`
}

// AggSample 聚合样本（仅标量字段，控制 jsonb 体积）。
type AggSample struct {
	EventID    string    `json:"event_id"`
	Title      string    `json:"title"`
	Severity   int32     `json:"severity"`
	CreateTime time.Time `json:"create_time"`
}

// DeliveryLog 投递明细（notify_delivery_log 一行，append-only）。
type DeliveryLog struct {
	ID            string
	DispatchID    string
	ChannelID     string
	Attempt       int32
	OK            bool
	LatencyMs     int32
	ProviderMsgID string
	Error         string
	CreateTime    time.Time
}

// DeadLetter 死信（notify_dlq 一行）。
type DeadLetter struct {
	ID          string
	DispatchID  string
	EventID     string
	ChannelID   string
	Recipient   string
	LastError   string
	Attempts    int32
	ReplayState int32
	CreateTime  time.Time
	UpdateTime  time.Time
	// Content 死 dispatch 的 content 快照（重放保留聚合上下文用）；源 dispatch 已清理则 nil。
	Content []byte
}

// SendKey 对外幂等键：hex(sha256(event|channel|recipient|dispatch))[:32]。
// 同一 dispatch 崩溃双发同键（接收端去重）；重放 = 新 dispatch = 新键（刻意再投）。
func SendKey(eventID, channelID, recipient, dispatchID string) string {
	sum := sha256.Sum256([]byte(eventID + "|" + channelID + "|" + recipient + "|" + dispatchID))
	return hex.EncodeToString(sum[:16])
}

// Truncate 把错误文案截到落库列宽（varchar(512)），按 rune 边界。
func Truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	runes := []rune(s)
	if len(runes) <= max {
		return s
	}
	return string(runes[:max])
}
