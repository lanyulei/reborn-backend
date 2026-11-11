// Package biz 承载通知域业务核心：领域模型、仓储接口（依赖倒置）与用例。
// 本包禁止 import 传输层与任何存储驱动（depguard 构建期强制）。
package biz

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/kit/notifycontract"

	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/paging"

	"github.com/go-kratos/kratos/v3/errors"
)

// 渠道类型常量（落库字符串；与 api NotifyChannelType 枚举一一对应，service 层转换）。
const (
	// ChannelTypeEmail 邮件（SMTP）。
	ChannelTypeEmail = notifycontract.ChannelTypeEmail
	// ChannelTypeWebhook 通用 Webhook（仅 HTTPS）。
	ChannelTypeWebhook = notifycontract.ChannelTypeWebhook
	// ChannelTypeDingtalk 钉钉群机器人。
	ChannelTypeDingtalk = notifycontract.ChannelTypeDingtalk
	// ChannelTypeWecom 企业微信群机器人。
	ChannelTypeWecom = notifycontract.ChannelTypeWecom
	// ChannelTypeFeishu 飞书群机器人。
	ChannelTypeFeishu = notifycontract.ChannelTypeFeishu
)

// DefaultBizID 默认业务租户标识。
const DefaultBizID = notifycontract.DefaultBizID

// EmailConfig 等 jsonb 列结构是与管道共享的数据契约（kit/notifycontract，json 标签即
// 落库 schema；两端同时发布）。
type EmailConfig = notifycontract.EmailConfig

// WebhookConfig Webhook 渠道配置（type=webhook）。
type WebhookConfig = notifycontract.WebhookConfig

// DefaultWebhookTimeoutS 是 webhook 出站超时缺省值（秒）：入参未指定时由
// service 层补齐后落库，保证管道侧总能拿到 1-10 范围内的确定超时。
const DefaultWebhookTimeoutS int32 = 5

// RobotConfig 群机器人渠道配置（type=dingtalk/wecom/feishu）。
type RobotConfig = notifycontract.RobotConfig

// ChannelConfig 类型化渠道配置：恰好一个分支非 nil，且须与渠道 type 匹配
// （email→Email、webhook→Webhook、三类群机器人→Robot）。
type ChannelConfig struct {
	Email   *EmailConfig
	Webhook *WebhookConfig
	Robot   *RobotConfig
}

// RateLimit 令牌桶限流参数；QPS=0 表示不限。
type RateLimit = notifycontract.RateLimit

// Channel 渠道领域模型（DO）；与 proto 实体、data 层 PO 显式转换，禁止跨层直传。
// 一行渠道 = 一个可独立发送/限流/启停的端点实例。Secret 仅在写入链路携带明文
// （data 层 AES-256-GCM 加密落库），任何读路径不回填。
type Channel struct {
	ID         string
	BizID      string
	Type       string
	Name       string
	Config     *ChannelConfig
	Secret     string
	SignSecret string // 机器人签名密钥（钉钉加签 / 飞书签名校验）；仅 dingtalk/feishu；只写不读
	RateLimit  RateLimit
	Enabled    bool
	CreateTime time.Time
	UpdateTime time.Time
}

// ChannelFilter 渠道列表过滤条件；指针为 nil 表示不过滤。
type ChannelFilter struct {
	BizID   string // 空 = 不过滤（受限用户由 service 层强制携带）
	Keyword string
	Type    *string
	Enabled *bool
}

// ChannelUpdate 渠道部分更新；nil 字段不更新（Type 不可改；Secret 不传=保留旧密文）。
type ChannelUpdate struct {
	Name       *string
	Config     *ChannelConfig
	RateLimit  *RateLimit
	Enabled    *bool
	Secret     *string
	SignSecret *string // nil=保留；非空=轮换；空串=清除（停用签名）
}

// 通知域哨兵错误：reason 与 api/notify/v1 的 ErrorReason 枚举对齐。
var (
	// ErrChannelNotFound 目标渠道不存在。
	ErrChannelNotFound = errors.NotFound(notifyv1.ErrorReason_NOTIFY_CHANNEL_NOT_FOUND.String(), "channel not found")
	// ErrChannelNameExists 同 biz 内渠道名已被占用（未删除行）。
	ErrChannelNameExists = errors.Conflict(notifyv1.ErrorReason_NOTIFY_CHANNEL_NAME_EXISTS.String(), "channel name already exists")
	// ErrChannelConfigInvalid 渠道配置不合法（type 与 config 结构不匹配 / 必需 secret 缺失）。
	ErrChannelConfigInvalid = errors.BadRequest(notifyv1.ErrorReason_NOTIFY_CHANNEL_CONFIG_INVALID.String(), "invalid channel config")
	// ErrChannelSecretUnavailable 凭据加密主密钥未配置，无法落库带 secret 的渠道。
	ErrChannelSecretUnavailable = errors.New(500, notifyv1.ErrorReason_NOTIFY_SECRET_UNAVAILABLE.String(), "notify master key unavailable")
	// ErrChannelDisabled 目标渠道已停用（停用实例被路由跳过，连通性测试
	// 无意义）。
	ErrChannelDisabled = errors.Conflict(notifyv1.ErrorReason_NOTIFY_CHANNEL_DISABLED.String(), "channel disabled")
	// ErrChannelInUse 渠道仍被未删除路由引用，须先解绑；否则路由携带悬挂
	// channel_id 被管道静默跳过，通知"看起来配了、实际不发"。
	ErrChannelInUse = errors.Conflict(notifyv1.ErrorReason_NOTIFY_CHANNEL_IN_USE.String(), "channel is referenced by routes")
)

// ChannelReferenceChecker 渠道引用计数窄接口（路由仓储实现）：删除渠道前
// 确认无未删除路由绑定它。
type ChannelReferenceChecker interface {
	CountRoutesUsingChannel(ctx context.Context, channelID string) (int64, error)
}

// ChannelRepo 渠道仓储接口，由 data 层实现（依赖倒置）。
type ChannelRepo interface {
	// CreateChannel 创建渠道；同 biz 未删除行内重名返回 ErrChannelNameExists。
	CreateChannel(ctx context.Context, ch *Channel) error
	// UpdateChannel 按 ID 部分更新未删除渠道；不存在返回 ErrChannelNotFound；
	// Secret 为 nil 时保留旧密文。
	UpdateChannel(ctx context.Context, id string, upd ChannelUpdate) error
	// DeleteChannel 软删渠道（名称可复用）；不存在返回 ErrChannelNotFound。
	DeleteChannel(ctx context.Context, id string) error
	// GetChannel 按 ID 查询未删除渠道（不含 secret）；不存在返回 ErrChannelNotFound。
	GetChannel(ctx context.Context, id string) (*Channel, error)
	// ListChannelsByIDs 按 ID 集合单条查询未删除渠道（不含 secret；缺失的 ID
	// 不在结果中、不报错），供路由绑定批量校验；空输入返回空集。
	ListChannelsByIDs(ctx context.Context, ids []string) ([]*Channel, error)
	// ListChannels 分页查询未删除渠道；page 从 1 起，pageSize 上限 100，按创建时间倒序。
	ListChannels(ctx context.Context, filter ChannelFilter, page, pageSize int64) ([]*Channel, int64, error)
}

// ChannelUsecase 渠道用例：承载业务规则（type/config/secret 匹配约束、删除前
// 路由引用校验）。
type ChannelUsecase struct {
	repo   ChannelRepo
	events EventWriter
	refs   ChannelReferenceChecker
}

// NewChannelUsecase new a ChannelUsecase.
func NewChannelUsecase(repo ChannelRepo, events EventWriter, refs ChannelReferenceChecker) *ChannelUsecase {
	return &ChannelUsecase{repo: repo, events: events, refs: refs}
}

// secretRequired 报告渠道类型是否必须携带 secret（webhook 的 HMAC 签名密钥可选）。
func secretRequired(channelType string) bool {
	return channelType != ChannelTypeWebhook
}

// signSecretAllowed 报告渠道类型是否支持签名密钥（钉钉加签、飞书签名校验）。
func signSecretAllowed(channelType string) bool {
	return channelType == ChannelTypeDingtalk || channelType == ChannelTypeFeishu
}

// validateConfigKind 校验 config 分支与渠道 type 匹配（恰好一个分支且种类正确）。
func validateConfigKind(channelType string, cfg *ChannelConfig) error {
	var matched bool
	switch channelType {
	case ChannelTypeEmail:
		matched = cfg != nil && cfg.Email != nil && cfg.Webhook == nil && cfg.Robot == nil
	case ChannelTypeWebhook:
		matched = cfg != nil && cfg.Webhook != nil && cfg.Email == nil && cfg.Robot == nil
	case ChannelTypeDingtalk, ChannelTypeWecom, ChannelTypeFeishu:
		matched = cfg != nil && cfg.Robot != nil && cfg.Email == nil && cfg.Webhook == nil
	default:
		return fmt.Errorf("%w: unknown channel type %q", ErrChannelConfigInvalid, channelType)
	}
	if !matched {
		return fmt.Errorf("%w: config kind mismatch for type %q", ErrChannelConfigInvalid, channelType)
	}
	if cfg.Webhook != nil {
		return validateWebhookHeaders(cfg.Webhook.Headers)
	}
	return nil
}

// sensitiveHeaderKeys 是禁止写进 webhook config.headers 的凭据类请求头
// （小写比较）：config 列明文落库、Get/List 原样回显、请求日志按 proto text
// 打印——凭据放这里等于绕过「secret 只写不读」模型。鉴权密钥一律走
// secret（HMAC 签名）。
var sensitiveHeaderKeys = map[string]struct{}{
	"authorization":       {},
	"proxy-authorization": {},
	"cookie":              {},
	"x-api-key":           {},
	"x-auth-token":        {},
	"api-key":             {},
}

// validateWebhookHeaders 拒绝凭据类请求头键名（大小写不敏感）。
func validateWebhookHeaders(headers map[string]string) error {
	for key := range headers {
		if _, bad := sensitiveHeaderKeys[strings.ToLower(strings.TrimSpace(key))]; bad {
			return fmt.Errorf("%w: header %q carries credentials; use secret (HMAC signing) instead", ErrChannelConfigInvalid, key)
		}
	}
	return nil
}

// Create 创建渠道：校验 type/config/secret 匹配，生成 ID 与审计时间后落库；
// BizID 缺省 default；出参不携带 secret 明文。
func (uc *ChannelUsecase) Create(ctx context.Context, ch *Channel) (*Channel, error) {
	if err := validateConfigKind(ch.Type, ch.Config); err != nil {
		return nil, err
	}
	if secretRequired(ch.Type) && ch.Secret == "" {
		return nil, fmt.Errorf("%w: secret required for type %q", ErrChannelConfigInvalid, ch.Type)
	}
	if ch.SignSecret != "" && !signSecretAllowed(ch.Type) {
		return nil, fmt.Errorf("%w: sign_secret not supported for type %q", ErrChannelConfigInvalid, ch.Type)
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	ch.ID = id
	if ch.BizID == "" {
		ch.BizID = DefaultBizID
	}
	ch.CreateTime, ch.UpdateTime = now, now
	if err := uc.repo.CreateChannel(ctx, ch); err != nil {
		return nil, err
	}
	ch.Secret, ch.SignSecret = "", ""
	return ch, nil
}

// Update 部分更新渠道；config 替换 / 设置签名密钥前先按渠道现有 type 校验
// （type 不可改；sign_secret 仅 dingtalk/feishu）。
func (uc *ChannelUsecase) Update(ctx context.Context, id string, upd ChannelUpdate) error {
	if upd.Secret != nil && *upd.Secret == "" {
		return fmt.Errorf("%w: secret must not be empty", ErrChannelConfigInvalid)
	}
	if upd.Config != nil || (upd.SignSecret != nil && *upd.SignSecret != "") {
		ch, err := uc.repo.GetChannel(ctx, id)
		if err != nil {
			return err
		}
		if upd.Config != nil {
			if err := validateConfigKind(ch.Type, upd.Config); err != nil {
				return err
			}
		}
		if upd.SignSecret != nil && *upd.SignSecret != "" && !signSecretAllowed(ch.Type) {
			return fmt.Errorf("%w: sign_secret not supported for type %q", ErrChannelConfigInvalid, ch.Type)
		}
	}
	return uc.repo.UpdateChannel(ctx, id, upd)
}

// Delete 软删渠道；不存在返回 ErrChannelNotFound；仍被未删除路由引用返回
// ErrChannelInUse（先从路由解绑）。
func (uc *ChannelUsecase) Delete(ctx context.Context, id string) error {
	n, err := uc.refs.CountRoutesUsingChannel(ctx, id)
	if err != nil {
		return err
	}
	if n > 0 {
		return fmt.Errorf("%w: %d route(s)", ErrChannelInUse, n)
	}
	return uc.repo.DeleteChannel(ctx, id)
}

// Get 查询渠道详情（不含 secret）。
func (uc *ChannelUsecase) Get(ctx context.Context, id string) (*Channel, error) {
	return uc.repo.GetChannel(ctx, id)
}

// List 分页查询渠道；分页参数先规整再委托仓储。
func (uc *ChannelUsecase) List(ctx context.Context, filter ChannelFilter, page, pageSize int64) ([]*Channel, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	return uc.repo.ListChannels(ctx, filter, page, pageSize)
}

// 测试事件缺省值（SendTestEvent 用）。
const (
	// TestEventTypeDefault 缺省测试事件类型：独立命名空间，避开常规业务
	// 路由与静默的 event_pattern（如 monitor.*）——要实际投递须建覆盖它的
	// 路由并把目标实例纳入 channel_ids。
	TestEventTypeDefault = "notify.test"
	// TestEventTitleDefault 缺省测试事件标题。
	TestEventTitleDefault = "通知渠道连通性测试"
	// TestEventSeverity 测试事件严重级：2 warning——测试目标是连通性而非
	// 告警语义；若被显式静默覆盖，事件状态机呈现 suppressed 同样是排障
	// 信息（ListEvents 可见）。
	TestEventSeverity int32 = 2
)

// TestEventSpec 测试事件可覆盖项（空值取缺省）。
type TestEventSpec struct {
	// EventType 事件类型（须为小写点分事件名，非路由模式）。
	EventType string
	// Title 事件标题。
	Title string
	// Recipients 显式收件（空=交路由按 recipient_policy 解析）。
	Recipients []string
	// Payload 附加载荷原始 JSON 对象（nil=仅缺省测试上下文；非空时与缺省
	// 测试上下文合并，test/channel_id 两键恒由系统填充）。
	Payload []byte
}

// eventTypePattern 事件类型取值：小写字母/数字开头的点分串（比路由模式
// 严——不允许 * 与尾部通配；与 ingress 侧事件名校验一致）。
var eventTypePattern = notifycontract.EventTypePattern

// SendTestEvent 按渠道实例发送测试事件（§5.1.1 连通性测试）：校验实例
// 存在且启用后，经 EventWriter 直插一条 source=internal 的测试事件，与
// 生产事件同链路（relay → 路由 → 渲染 → 发送，管道 P1）；返回落库事件，
// 投递结果经 ListEvents/ListDispatches/ListDeliveryLogs 异步跟踪。
func (uc *ChannelUsecase) SendTestEvent(ctx context.Context, channelID string, spec TestEventSpec) (*Event, error) {
	channel, err := uc.repo.GetChannel(ctx, channelID)
	if err != nil {
		return nil, err
	}
	if !channel.Enabled {
		return nil, ErrChannelDisabled
	}
	eventType := spec.EventType
	if eventType == "" {
		eventType = TestEventTypeDefault
	}
	if !eventTypePattern.MatchString(eventType) {
		return nil, fmt.Errorf("%w: event_type %q 须为小写点分事件名（非路由模式）", ErrChannelConfigInvalid, eventType)
	}
	title := spec.Title
	if title == "" {
		title = TestEventTitleDefault
	}
	if len(spec.Recipients) > maxEventRecipients {
		return nil, fmt.Errorf("%w: recipients %d exceeds %d", ErrChannelConfigInvalid, len(spec.Recipients), maxEventRecipients)
	}
	payload, err := testEventPayload(channelID, spec.Payload)
	if err != nil {
		return nil, err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, fmt.Errorf("generate test event id: %w", err)
	}
	now := time.Now().UTC()
	event := &Event{
		ID:              id,
		BizID:           channel.BizID,
		EventType:       eventType,
		Severity:        TestEventSeverity,
		Title:           title,
		Payload:         payload,
		Recipients:      spec.Recipients,
		Source:          EventSourceInternal,
		Status:          EventStatusReceived,
		PublishState:    0,
		NextPublishTime: now,
		CreateTime:      now,
	}
	if err := uc.events.InsertEvent(ctx, event); err != nil {
		return nil, err
	}
	return event, nil
}

// testEventPayload 构造测试事件载荷：调用方附加载荷（须为 JSON 对象、
// ≤maxEventPayloadBytes——与 NotificationWriter 同一上限，否则多 MB 的
// Struct 可绕过事件载荷约束直落 jsonb）与缺省测试上下文合并，
// test=true / channel_id 恒由系统填充（标识测试事件，调用方不可覆盖）。
func testEventPayload(channelID string, extra []byte) ([]byte, error) {
	merged := map[string]any{}
	if len(extra) > 0 {
		if len(extra) > maxEventPayloadBytes {
			return nil, fmt.Errorf("%w: payload %d bytes exceeds %d", ErrChannelConfigInvalid, len(extra), maxEventPayloadBytes)
		}
		if err := json.Unmarshal(extra, &merged); err != nil || merged == nil {
			return nil, fmt.Errorf("%w: payload must be a JSON object", ErrChannelConfigInvalid)
		}
	}
	merged["test"] = true
	merged["channel_id"] = channelID
	raw, err := json.Marshal(merged)
	if err != nil {
		return nil, fmt.Errorf("encode test payload: %w", err)
	}
	if len(raw) > maxEventPayloadBytes {
		return nil, fmt.Errorf("%w: payload %d bytes exceeds %d", ErrChannelConfigInvalid, len(raw), maxEventPayloadBytes)
	}
	return raw, nil
}
