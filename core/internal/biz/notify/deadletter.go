package biz

import (
	"context"
	"time"

	"github.com/lanyulei/reborn/backend/kit/notifycontract"

	"github.com/go-kratos/kratos/v3/errors"

	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	"github.com/lanyulei/reborn/backend/kit/paging"
)

// 重放状态常量（与 notify_dlq 列注释对齐，§4.7 重放状态机）。
const (
	// ReplayStateNone 未重放（仅此状态可被控制台标记）。
	ReplayStateNone = notifycontract.ReplayNone
	// ReplayStatePending 已标记待重放（dlq worker 轮询中）。
	ReplayStatePending = notifycontract.ReplayPending
	// ReplayStateReplayed 已重放（dlq worker 生成新 dispatch 后终态）。
	ReplayStateReplayed = notifycontract.ReplayReplayed
)

// 死信哨兵错误：reason 与 api/notify/v1 的 ErrorReason 枚举对齐。
var (
	// ErrDeadLetterNotFound 目标死信不存在。
	ErrDeadLetterNotFound = errors.NotFound(notifyv1.ErrorReason_NOTIFY_DLQ_NOT_FOUND.String(), "dead letter not found")
	// ErrDeadLetterInvalidState 死信状态不满足重放条件（仅 none 可标记：
	// pending 已在重放队列、replayed 已重放完成）。
	ErrDeadLetterInvalidState = errors.Conflict(notifyv1.ErrorReason_NOTIFY_DLQ_INVALID_STATE.String(), "dead letter not replayable")
)

// DeadLetter 死信领域模型（DO）：notify_dlq 运行表一行的排障视图（sender
// 在 dispatch 重试超限后写入，设计 §4.7/§5.2）。与事件/任务/明细三个纯
// 读运行表的差异：core 有唯一写动作——标记重放（replay_state: none →
// pending），由 dlq worker（管道 P1）消费 pending 行重新生成 dispatch 后
// 置 replayed；core 不改错误详情、不删除行（§5.3 列级授权）。运行表非
// 配置表，不 bump notify_cfg_version（dlq worker 轮询本表而非配置缓存）。
// LastError 为可空列（空=无）。
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
}

// DeadLetterFilter 死信过滤条件（设计 §6.1：事件下钻/渠道/重放状态/时间
// 四维）；EventID/ChannelID 精确匹配，指针为 nil 表示不过滤。
type DeadLetterFilter struct {
	EventID         string
	ChannelID       string
	ReplayState     *int32
	CreateTimeStart *time.Time
	CreateTimeEnd   *time.Time
}

// DeadLetterRepo 死信仓储接口，由 data 层实现（依赖倒置）；读列表 + 唯一
// 写动作（条件 UPDATE 保证 none→pending 状态机，并发标记只有一方成功）。
type DeadLetterRepo interface {
	// ListDeadLetters 分页查询死信；page 从 1 起，pageSize 上限 100，
	// 按转入时间倒序。
	ListDeadLetters(ctx context.Context, filter DeadLetterFilter, page, pageSize int64) ([]*DeadLetter, int64, error)
	// MarkReplayPending 标记死信待重放（replay_state: none → pending）并
	// 返回更新后的行；行不存在返回 ErrDeadLetterNotFound，状态非 none
	// 返回 ErrDeadLetterInvalidState。
	MarkReplayPending(ctx context.Context, id string) (*DeadLetter, error)
	// GetDeadLetter 按 ID 查询死信；不存在返回 ErrDeadLetterNotFound。
	GetDeadLetter(ctx context.Context, id string) (*DeadLetter, error)
}

// DeadLetterUsecase 死信用例：查询编排 + 重放标记。
type DeadLetterUsecase struct {
	repo DeadLetterRepo
}

// NewDeadLetterUsecase new a DeadLetterUsecase.
func NewDeadLetterUsecase(repo DeadLetterRepo) *DeadLetterUsecase {
	return &DeadLetterUsecase{repo: repo}
}

// List 分页查询死信；分页参数先规整再委托仓储。
func (uc *DeadLetterUsecase) List(ctx context.Context, filter DeadLetterFilter, page, pageSize int64) ([]*DeadLetter, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	return uc.repo.ListDeadLetters(ctx, filter, page, pageSize)
}

// Get 查询死信详情（按 biz 隔离时取渠道归属用）。
func (uc *DeadLetterUsecase) Get(ctx context.Context, id string) (*DeadLetter, error) {
	return uc.repo.GetDeadLetter(ctx, id)
}

// Replay 标记死信待重放；状态机由仓储条件 UPDATE 保证（none → pending）。
func (uc *DeadLetterUsecase) Replay(ctx context.Context, id string) (*DeadLetter, error) {
	return uc.repo.MarkReplayPending(ctx, id)
}
