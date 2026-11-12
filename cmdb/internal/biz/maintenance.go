package biz

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// HistoryTable 按月分区的变更历史表。
const HistoryTable = "cmdb_ci_history"

// MaintenanceRepo 分区维护仓储（实现须幂等：选主是优化而非正确性依赖）。
type MaintenanceRepo interface {
	// EnsurePartition 确保 table 的 month 月分区存在（独立建表 + CHECK → 短事务迁出 DEFAULT 行并 ATTACH）。
	EnsurePartition(ctx context.Context, table string, month time.Time) error
	// DropPartitionsBefore 删除上界 ≤ before 的月分区；返回删除数。
	DropPartitionsBefore(ctx context.Context, table string, before time.Time) (int, error)
}

// MaintenanceOptions 维护参数。
type MaintenanceOptions struct {
	// PartitionsAhead 预建未来分区月数。
	PartitionsAhead int
	// HistoryRetention 历史保留期；0 = 不删除。
	HistoryRetention time.Duration
}

// MaintenanceUsecase 历史分区维护：预建未来分区、按保留期删除旧分区。
type MaintenanceUsecase struct {
	repo MaintenanceRepo
	opts MaintenanceOptions
	log  *slog.Logger
}

// NewMaintenanceUsecase new a MaintenanceUsecase.
func NewMaintenanceUsecase(repo MaintenanceRepo, log *slog.Logger) *MaintenanceUsecase {
	return &MaintenanceUsecase{repo: repo, log: log, opts: MaintenanceOptions{PartitionsAhead: 2, HistoryRetention: 365 * 24 * time.Hour}}
}

// WithOptions 注入参数（非法值回退缺省；HistoryRetention 允许 0 = 不删除）。
func (uc *MaintenanceUsecase) WithOptions(o MaintenanceOptions) *MaintenanceUsecase {
	if o.PartitionsAhead > 0 {
		uc.opts.PartitionsAhead = o.PartitionsAhead
	}
	uc.opts.HistoryRetention = o.HistoryRetention
	return uc
}

// RunOnce 一轮维护：当前月至未来 N 月分区预建；上界早于 now - retention 的分区删除。
// 单项失败只记日志并继续（下一轮重试），最终返回首个错误供循环退避。
func (uc *MaintenanceUsecase) RunOnce(ctx context.Context, now time.Time) error {
	var first error
	keep := func(err error) {
		if err != nil && first == nil {
			first = err
		}
	}
	month := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i <= uc.opts.PartitionsAhead; i++ {
		m := month.AddDate(0, i, 0)
		if err := uc.repo.EnsurePartition(ctx, HistoryTable, m); err != nil {
			uc.log.ErrorContext(ctx, "ensure history partition failed", "month", m.Format("2006-01"), "error", err)
			keep(fmt.Errorf("ensure partition %s: %w", m.Format("2006-01"), err))
		}
	}
	if uc.opts.HistoryRetention > 0 {
		n, err := uc.repo.DropPartitionsBefore(ctx, HistoryTable, now.Add(-uc.opts.HistoryRetention))
		if err != nil {
			uc.log.ErrorContext(ctx, "drop expired history partitions failed", "error", err)
			keep(fmt.Errorf("drop partitions: %w", err))
		} else if n > 0 {
			uc.log.InfoContext(ctx, "expired history partitions dropped", "count", n)
		}
	}
	return first
}
