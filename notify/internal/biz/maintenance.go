package biz

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// PartitionedTables 三张按月分区运行表。
var PartitionedTables = []string{"notify_event", "notify_dispatch", "notify_delivery_log"}

// Retention 保留期配置。
type Retention struct {
	Event       time.Duration
	Dispatch    time.Duration
	DeliveryLog time.Duration
	Dlq         time.Duration
	Dedup       time.Duration
	AggGroup    time.Duration
}

// MaintenanceRepo 维护仓储（分区 DDL / 行级清理）。实现须幂等：选主是优化而非
// 正确性依赖（评审 #19）。
type MaintenanceRepo interface {
	// EnsurePartition 确保 table 的 month 月分区存在：独立建表 + CHECK → 单短事务
	// （lock_timeout）迁出 DEFAULT 中该区间行并 ATTACH（评审 #17）。
	EnsurePartition(ctx context.Context, table string, month time.Time) error
	// DropPartitionsBefore 删除上界 ≤ before 的月分区（lock_timeout，失败下轮重试）；返回删除数。
	DropPartitionsBefore(ctx context.Context, table string, before time.Time) (int, error)
	// DeleteExpiredDedup / DeleteOldDlq / DeleteClosedAggGroups 批量行清理；返回删除数。
	DeleteExpiredDedup(ctx context.Context, limit int) (int, error)
	DeleteOldDlq(ctx context.Context, before time.Time, limit int) (int, error)
	DeleteClosedAggGroups(ctx context.Context, before time.Time, limit int) (int, error)
	// DeleteStaleChannelHealth 清理已关闭且久未更新的渠道健康行；返回删除数。
	DeleteStaleChannelHealth(ctx context.Context, before time.Time, limit int) (int, error)
}

// Locker 分布式互斥（PG advisory lock，专用连接会话级持有）。
type Locker interface {
	// TryLock 非阻塞尝试；ok=false 表示他人持有。release 释放锁与连接。
	TryLock(ctx context.Context, key string) (release func(), ok bool, err error)
}

// 锁名。
const (
	LockMaintenance = "notify_maintenance"
	LockSweeper     = "notify_agg_sweeper"
)

// MaintenanceUsecase 分区维护与保留期清理（设计 §5.4）。
type MaintenanceUsecase struct {
	repo      MaintenanceRepo
	retention Retention
	ahead     int
	log       *slog.Logger
}

// NewMaintenanceUsecase new a MaintenanceUsecase（缺省保留期：event/dispatch 180d、
// delivery_log 90d、dlq 180d、dedup 24h、agg 7d；预建 2 个月）。
func NewMaintenanceUsecase(repo MaintenanceRepo, logger *slog.Logger) *MaintenanceUsecase {
	return &MaintenanceUsecase{
		repo: repo, log: logger, ahead: 2,
		retention: Retention{
			Event: 180 * 24 * time.Hour, Dispatch: 180 * 24 * time.Hour, DeliveryLog: 90 * 24 * time.Hour,
			Dlq: 180 * 24 * time.Hour, Dedup: 24 * time.Hour, AggGroup: 7 * 24 * time.Hour,
		},
	}
}

// WithRetention / WithPartitionsAhead 覆盖参数（零值保留缺省）。
func (uc *MaintenanceUsecase) WithRetention(r Retention) *MaintenanceUsecase {
	set := func(dst *time.Duration, v time.Duration) {
		if v > 0 {
			*dst = v
		}
	}
	set(&uc.retention.Event, r.Event)
	set(&uc.retention.Dispatch, r.Dispatch)
	set(&uc.retention.DeliveryLog, r.DeliveryLog)
	set(&uc.retention.Dlq, r.Dlq)
	set(&uc.retention.Dedup, r.Dedup)
	set(&uc.retention.AggGroup, r.AggGroup)
	return uc
}

// WithPartitionsAhead 预建月数。
func (uc *MaintenanceUsecase) WithPartitionsAhead(n int) *MaintenanceUsecase {
	if n > 0 {
		uc.ahead = n
	}
	return uc
}

// RunOnce 执行一轮：预建分区 → 过期分区 DROP → 小表清理。单步失败记日志继续
// 其余步骤（下轮重试），整体返回首个错误。
func (uc *MaintenanceUsecase) RunOnce(ctx context.Context, now time.Time) error {
	var first error
	keep := func(err error) {
		if err != nil && first == nil {
			first = err
		}
	}
	month := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	for _, table := range PartitionedTables {
		for i := 0; i <= uc.ahead; i++ {
			if err := uc.repo.EnsurePartition(ctx, table, month.AddDate(0, i, 0)); err != nil {
				uc.log.ErrorContext(ctx, "ensure partition failed", "table", table, "month", month.AddDate(0, i, 0).Format("2006-01"), "error", err)
				keep(fmt.Errorf("ensure partition %s: %w", table, err))
			}
		}
	}
	for table, ret := range map[string]time.Duration{
		"notify_event": uc.retention.Event, "notify_dispatch": uc.retention.Dispatch, "notify_delivery_log": uc.retention.DeliveryLog,
	} {
		n, err := uc.repo.DropPartitionsBefore(ctx, table, now.Add(-ret))
		if err != nil {
			uc.log.ErrorContext(ctx, "drop expired partitions failed", "table", table, "error", err)
			keep(fmt.Errorf("drop partitions %s: %w", table, err))
		} else if n > 0 {
			uc.log.InfoContext(ctx, "expired partitions dropped", "table", table, "count", n)
		}
	}
	const batch = 5000
	if _, err := uc.repo.DeleteExpiredDedup(ctx, batch); err != nil {
		keep(fmt.Errorf("delete expired dedup: %w", err))
	}
	if _, err := uc.repo.DeleteOldDlq(ctx, now.Add(-uc.retention.Dlq), batch); err != nil {
		keep(fmt.Errorf("delete old dlq: %w", err))
	}
	if _, err := uc.repo.DeleteClosedAggGroups(ctx, now.Add(-uc.retention.AggGroup), batch); err != nil {
		keep(fmt.Errorf("delete closed agg groups: %w", err))
	}
	if _, err := uc.repo.DeleteStaleChannelHealth(ctx, now.Add(-uc.retention.AggGroup), batch); err != nil {
		keep(fmt.Errorf("delete stale channel health: %w", err))
	}
	return first
}

// DedupTTL 门闩有效期（ingress 写 expire_time 用）。
func (uc *MaintenanceUsecase) DedupTTL() time.Duration { return uc.retention.Dedup }

// Backlog 积压快照（指标观测）。
type Backlog struct {
	PendingEvents int64
	DueDispatches int64
}

// BacklogRepo 积压查询（部分索引上的 count，指标抓取时执行）。
type BacklogRepo interface {
	Backlog(ctx context.Context) (Backlog, error)
}

// Pinger 数据库连通性探测（健康检查）。
type Pinger interface {
	Ping() error
}
