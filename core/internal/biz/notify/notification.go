package biz

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/idgen"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"

	"github.com/go-kratos/kratos/v3/errors"
)

// NotificationWriter 入参上限（对齐 notify_event DDL 列约束/注释）。
const (
	// maxEventTitleLen title 上限（varchar(255)）。
	maxEventTitleLen = 255
	// maxEventKeyLen dedup_key/group_key 上限（varchar(128)）。
	maxEventKeyLen = 128
	// maxEventPayloadBytes payload 上限（DDL 注释 ≤32KB）。
	maxEventPayloadBytes = 32 * 1024
	// maxEventRecipients 显式收件上限（与管道 ingress 入口约束一致）。
	maxEventRecipients = 20
	// defaultEventSeverity severity 缺省：2 warning——业务事件未显式定级
	// 时的保守缺省（与测试事件同级）。
	defaultEventSeverity int32 = 2
	// defaultDedupTTL 直插去重门闩缺省有效期（与管道 ingress 一致）。
	defaultDedupTTL = 24 * time.Hour
)

// ErrNotificationInvalid 业务事件直插入参不合法（内部写路径，无独立
// HTTP 端点；错误经调用方业务端点透传为 400）。
var ErrNotificationInvalid = errors.BadRequest(notifyv1.ErrorReason_NOTIFY_EVENT_INVALID.String(), "invalid notification event")

// EventTxRepo 事务内事件直插原语（data 层实现，依赖倒置）：与 EventWriter
// 同一 INSERT 语义，但绑定调用方事务句柄。本原语由 NotificationUsecase
// 独占使用；业务域一律经 NotificationWriter 编排（先校验补全再落库），
// 不直接触碰。
type EventTxRepo interface {
	// InsertEventTx 经调用方事务句柄直插一行事件（可空列空值落 NULL）；
	// 提交/回滚由事务属主（txrun.Manager）决定。
	InsertEventTx(ctx context.Context, tx txrun.Tx, event *Event) error
	// GateDedupTx 去重门闩（与管道 ingress 同一 notify_dedup 表、同一单语句
	// upsert 语义）：返回占位成功的 event_id——等于入参 eventID 表示本次为首次
	// （应继续插事件），否则为有效期内首次落库的事件（本次去重）。
	GateDedupTx(ctx context.Context, tx txrun.Tx, bizID, dedupKey, eventID string, ttl time.Duration) (string, error)
}

// NotificationWriter 跨域业务事件窄接口（设计 §11「core 集成指南」）：
// 业务域在自身业务写事务内追加通知事件——业务提交 = 事件必然落库，
// 任一失败整体回滚，通知与业务永不脱节。消费方（如 system 域
// PermissionUsecase）持有本接口，经顶层 biz 聚合 wire.Bind 装配。
// 入参 Event 仅需填业务语义字段（BizID/EventType/Severity/Title/Payload/
// DedupKey/GroupKey/Recipients）；ID/Source/Status/PublishState/时间戳是
// 管道状态机语义，由实现恒定覆写，调用方无权指定。
type NotificationWriter interface {
	// NotifyEvent 校验并补全事件后经 tx 直插；返回补全后的落库事件
	//（含 ID，供调用方跟踪/审计）。
	NotifyEvent(ctx context.Context, tx txrun.Tx, event Event) (*Event, error)
}

// NotificationUsecase 业务事件直插用例：入参校验与管道缺省补全收敛在此
// （业务规则在 biz），原语写经 EventTxRepo。不 bump cfg_version——运行表
// 非配置表，落库后由 relay（管道 P1）按 pending 拾取。
type NotificationUsecase struct {
	events   EventTxRepo
	dedupTTL time.Duration
}

// NewNotificationUsecase new a NotificationUsecase（c 可为 nil：门闩 TTL 取缺省 24h）。
func NewNotificationUsecase(events EventTxRepo, c *conf.Notify) *NotificationUsecase {
	ttl := defaultDedupTTL
	if d := c.GetDedupTtl(); d != nil && d.AsDuration() > 0 {
		ttl = d.AsDuration()
	}
	return &NotificationUsecase{events: events, dedupTTL: ttl}
}

// 编译期断言：本用例即 NotificationWriter 的唯一实现（wire.Bind 目标）。
var _ NotificationWriter = (*NotificationUsecase)(nil)

// NotifyEvent implements NotificationWriter：校验 → 补全管道恒定字段 →
// 经调用方事务直插。事件校验失败即返回（未触碰事务，调用方事务整体
// 回滚由其错误透传自然达成）。
func (uc *NotificationUsecase) NotifyEvent(ctx context.Context, tx txrun.Tx, event Event) (*Event, error) {
	if event.BizID == "" {
		event.BizID = DefaultBizID
	}
	if !eventTypePattern.MatchString(event.EventType) {
		return nil, fmt.Errorf("%w: event_type %q 须为小写点分事件名", ErrNotificationInvalid, event.EventType)
	}
	if event.Severity == 0 {
		event.Severity = defaultEventSeverity
	}
	if event.Severity < 1 || event.Severity > 4 {
		return nil, fmt.Errorf("%w: severity %d 越界（1-4）", ErrNotificationInvalid, event.Severity)
	}
	if event.Title == "" {
		return nil, fmt.Errorf("%w: title 不能为空", ErrNotificationInvalid)
	}
	if len(event.Title) > maxEventTitleLen {
		return nil, fmt.Errorf("%w: title 长度 %d 超上限 %d", ErrNotificationInvalid, len(event.Title), maxEventTitleLen)
	}
	if len(event.Payload) > maxEventPayloadBytes {
		return nil, fmt.Errorf("%w: payload %d 字节超上限 %d", ErrNotificationInvalid, len(event.Payload), maxEventPayloadBytes)
	}
	// 非法 JSON 在此拒绝（400），而非落到 jsonb 列报 22P02 让调用方业务
	// 事务整体回滚成 500。
	if len(event.Payload) > 0 && !json.Valid(event.Payload) {
		return nil, fmt.Errorf("%w: payload 不是合法 JSON", ErrNotificationInvalid)
	}
	if len(event.Recipients) > maxEventRecipients {
		return nil, fmt.Errorf("%w: recipients %d 超上限 %d", ErrNotificationInvalid, len(event.Recipients), maxEventRecipients)
	}
	if len(event.DedupKey) > maxEventKeyLen || len(event.GroupKey) > maxEventKeyLen {
		return nil, fmt.Errorf("%w: dedup/group key 超上限 %d", ErrNotificationInvalid, maxEventKeyLen)
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, fmt.Errorf("generate event id: %w", err)
	}
	if event.DedupKey != "" {
		// 去重门闩与事件插入同事务：业务写提交即门闩占位，回滚即释放。
		winner, err := uc.events.GateDedupTx(ctx, tx, event.BizID, event.DedupKey, id, uc.dedupTTL)
		if err != nil {
			return nil, fmt.Errorf("dedup gate: %w", err)
		}
		if winner != id {
			event.ID = winner
			event.Deduplicated = true
			return &event, nil
		}
	}
	now := time.Now().UTC()
	event.ID = id
	event.Source = EventSourceInternal
	event.Status = EventStatusReceived
	event.PublishState = 0
	event.PublishAttempts = 0
	event.NextPublishTime = now
	event.CreateTime = now
	if err := uc.events.InsertEventTx(ctx, tx, &event); err != nil {
		return nil, err
	}
	return &event, nil
}
