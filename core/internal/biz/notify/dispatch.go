package biz

import (
	"context"
	"time"

	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	"github.com/lanyulei/reborn/backend/kit/notifycontract"
	"github.com/lanyulei/reborn/backend/kit/paging"

	"github.com/go-kratos/kratos/v3/errors"
)

// 任务状态常量（与 notify_dispatch 列注释对齐，管道 sender 状态机的字面量口径）。
const (
	// DispatchStatusPending 已生成待发送。
	DispatchStatusPending = notifycontract.DispatchPending
	// DispatchStatusSending 发送中（行条件更新占用，防双发）。
	DispatchStatusSending = notifycontract.DispatchSending
	// DispatchStatusSent 已送达（终态）。
	DispatchStatusSent = notifycontract.DispatchSent
	// DispatchStatusFailed 发送失败，指数退避重试中。
	DispatchStatusFailed = notifycontract.DispatchFailed
	// DispatchStatusDead 重试超限转死信（终态，可重放）。
	DispatchStatusDead = notifycontract.DispatchDead
)

// Dispatch 发送任务领域模型（DO）：notify_dispatch 运行表一行的排障视图
// （管道写 / core 读，设计 §5.2）。一行 = 路由命中后生成的一个发送任务
// （事件 × 渠道 × 收件人）。append-only 生成、状态机由管道 sender 推进：
// core 无写路径、无软删概念，故不 bump notify_cfg_version（运行表非配置表）。
// Content 为渲染快照原始 JSON 字节（nil=无快照），由 service 层转 proto
// Struct；NextRetryTime/LastError 为可空列（nil/空=无待重试/未失败）。
type Dispatch struct {
	ID         string
	EventID    string
	ChannelID  string
	Recipient  string
	TemplateID string
	Content    []byte
	Status     int32
	Attempts   int32
	NextRetry  *time.Time
	LastError  string
	SendKey    string
	CreateTime time.Time
}

// DispatchFilter 发送任务过滤条件（设计 §6.1：事件下钻/渠道/状态/时间
// 四维）；EventID/ChannelID 精确匹配，指针为 nil 表示不过滤。
type DispatchFilter struct {
	EventID         string
	ChannelID       string
	Status          *int32
	Recipient       string // 精确；空 = 不过滤
	CreateTimeStart *time.Time
	CreateTimeEnd   *time.Time
}

// ErrDispatchNotFound 目标发送任务不存在。
var ErrDispatchNotFound = errors.NotFound(notifyv1.ErrorReason_NOTIFY_DISPATCH_NOT_FOUND.String(), "dispatch not found")

// DispatchRepo 发送任务仓储接口，由 data 层实现（依赖倒置）；只读——任务
// 由管道 router 生成、sender 推进状态，core 无管理写路径。
type DispatchRepo interface {
	// ListDispatches 分页查询发送任务；page 从 1 起，pageSize 上限 100，
	// 按生成时间倒序（查询索引 idx_notify_dispatch_query）。
	ListDispatches(ctx context.Context, filter DispatchFilter, page, pageSize int64) ([]*Dispatch, int64, error)
	// GetDispatch 按 ID 查询发送任务；不存在返回 ErrDispatchNotFound。
	GetDispatch(ctx context.Context, id string) (*Dispatch, error)
}

// DispatchUsecase 发送任务查询用例：纯读编排（分页规整后委托仓储）。
type DispatchUsecase struct {
	repo DispatchRepo
}

// NewDispatchUsecase new a DispatchUsecase.
func NewDispatchUsecase(repo DispatchRepo) *DispatchUsecase {
	return &DispatchUsecase{repo: repo}
}

// Get 查询发送任务详情。
func (uc *DispatchUsecase) Get(ctx context.Context, id string) (*Dispatch, error) {
	return uc.repo.GetDispatch(ctx, id)
}

// List 分页查询发送任务；分页参数先规整再委托仓储。
func (uc *DispatchUsecase) List(ctx context.Context, filter DispatchFilter, page, pageSize int64) ([]*Dispatch, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	return uc.repo.ListDispatches(ctx, filter, page, pageSize)
}
