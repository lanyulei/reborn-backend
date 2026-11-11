package biz

import (
	"context"
	"time"

	"github.com/lanyulei/reborn/backend/kit/notifycontract"

	"github.com/lanyulei/reborn/backend/kit/paging"
)

// 事件来源与状态常量（与 notify_event 列注释对齐，管道状态机的字面量口径）。
const (
	// EventSourceAPI 生产者经管道 ingress 接入。
	EventSourceAPI = notifycontract.EventSourceAPI
	// EventSourceInternal core 业务同事务直插（NotificationWriter）。
	EventSourceInternal = notifycontract.EventSourceInternal

	// EventStatusReceived 已落库待路由。
	EventStatusReceived = notifycontract.EventStatusReceived
	// EventStatusRouted 已生成投递任务。
	EventStatusRouted = notifycontract.EventStatusRouted
	// EventStatusSuppressed 静默命中被抑制。
	EventStatusSuppressed = notifycontract.EventStatusSuppressed
	// EventStatusFailed 无匹配路由等终态失败。
	EventStatusFailed = notifycontract.EventStatusFailed
)

// Event 事件领域模型（DO）：notify_event 运行表一行的排障视图（管道写 /
// core 读，设计 §5.2）。Payload 为原始 JSON 字节（nil=无载荷），由 service
// 层转 proto Struct；Recipients 空切片=交路由解析。append-only：无写路径、
// 无软删概念，状态由管道状态机推进。
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
	// Deduplicated 仅 NotificationWriter 出参使用：true 表示 dedup_key 在有效期内
	// 已有首次事件，本次未插入，ID 为首次事件标识（不落库）。
	Deduplicated bool
}

// EventFilter 事件过滤条件（设计 §6.1：biz/event_type/时间/状态四维）；
// BizID/EventType 精确匹配，指针为 nil 表示不过滤。
type EventFilter struct {
	BizID           string
	EventType       string
	Status          *int32
	CreateTimeStart *time.Time
	CreateTimeEnd   *time.Time
}

// EventRepo 事件仓储接口，由 data 层实现（依赖倒置）；只读——事件经管道
// ingress 或 NotificationWriter 落库，core 无管理写路径，故不 bump
// notify_cfg_version（运行表非配置表）。
type EventRepo interface {
	// ListEvents 分页查询事件；page 从 1 起，pageSize 上限 100，按落库
	// 时间倒序（查询索引 idx_notify_event_query）。
	ListEvents(ctx context.Context, filter EventFilter, page, pageSize int64) ([]*Event, int64, error)
}

// EventWriter 事件落库窄接口（设计 §5.3：core 对 notify_event 仅 INSERT，
// 无 UPDATE/DELETE——投递完的事件归管道状态机管理）。core 侧两条写入路径
// 共用本原语：业务事务直插（NotificationWriter，后续阶段）与渠道连通性
// 测试（ChannelUsecase.SendTestEvent）；管道 ingress 落库不经此口。落库后
// 由 relay（管道 P1）按 publish_state=pending 拾取发布。
type EventWriter interface {
	// InsertEvent 直插一行事件（先落库后应答，§7.1 第一道防线）；ID 须
	// 由调用方生成（UUIDv7），CreateTime/NextPublishTime 为落库时间。
	InsertEvent(ctx context.Context, event *Event) error
}

// EventUsecase 事件查询用例：纯读编排（分页规整后委托仓储）。
type EventUsecase struct {
	repo EventRepo
}

// NewEventUsecase new an EventUsecase.
func NewEventUsecase(repo EventRepo) *EventUsecase {
	return &EventUsecase{repo: repo}
}

// List 分页查询事件；分页参数先规整再委托仓储。
func (uc *EventUsecase) List(ctx context.Context, filter EventFilter, page, pageSize int64) ([]*Event, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	return uc.repo.ListEvents(ctx, filter, page, pageSize)
}
