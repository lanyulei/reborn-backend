package biz

import (
	"context"
	"time"

	"github.com/lanyulei/reborn/backend/core/internal/pkg/paging"
)

// DeliveryLog 投递明细领域模型（DO）：notify_delivery_log 运行表一行的排障
// 视图（管道写 / core 读，设计 §5.2）。一行 = 发送任务的一次投递尝试结果；
// append-only（sender 每次尝试后追加，不更新不删除，清理走 DROP PARTITION）：
// core 无写路径、无软删概念，故不 bump notify_cfg_version（运行表非配置表）。
// ProviderMsgID/Error 为可空列（空=渠道未返回/本次成功）。
type DeliveryLog struct {
	ID            string
	DispatchID    string
	ChannelID     string
	Attempt       int32
	OK            bool
	LatencyMS     int32
	ProviderMsgID string
	Error         string
	CreateTime    time.Time
}

// DeliveryLogFilter 投递明细过滤条件（设计 §6.1：任务下钻/渠道/结果/时间
// 四维）；DispatchID/ChannelID 精确匹配，OK 为 nil 表示不过滤，时间指针为
// nil 表示不过滤。
type DeliveryLogFilter struct {
	DispatchID      string
	ChannelID       string
	OK              *bool
	CreateTimeStart *time.Time
	CreateTimeEnd   *time.Time
}

// DeliveryLogRepo 投递明细仓储接口，由 data 层实现（依赖倒置）；只读——
// 日志由管道 sender 追加，core 无管理写路径。
type DeliveryLogRepo interface {
	// ListDeliveryLogs 分页查询投递明细；page 从 1 起，pageSize 上限 100，
	// 按落库时间倒序（查询索引 idx_notify_delivery_query）。
	ListDeliveryLogs(ctx context.Context, filter DeliveryLogFilter, page, pageSize int64) ([]*DeliveryLog, int64, error)
}

// DeliveryLogUsecase 投递明细查询用例：纯读编排（分页规整后委托仓储）。
type DeliveryLogUsecase struct {
	repo DeliveryLogRepo
}

// NewDeliveryLogUsecase new a DeliveryLogUsecase.
func NewDeliveryLogUsecase(repo DeliveryLogRepo) *DeliveryLogUsecase {
	return &DeliveryLogUsecase{repo: repo}
}

// List 分页查询投递明细；分页参数先规整再委托仓储。
func (uc *DeliveryLogUsecase) List(ctx context.Context, filter DeliveryLogFilter, page, pageSize int64) ([]*DeliveryLog, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	return uc.repo.ListDeliveryLogs(ctx, filter, page, pageSize)
}
