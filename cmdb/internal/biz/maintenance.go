package biz

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/lanyulei/reborn/backend/kit/loop"
)

// HistoryTable 按月分区的变更历史表。
const HistoryTable = "cmdb_ci_history"

// 同步任务与运行记录清理的参数。
const (
	// DefaultRunRetention 同步任务与运行记录的缺省保留期。
	DefaultRunRetention = 180 * 24 * time.Hour
	// runPurgeBatch 每条 DELETE 至多删除的行数：单条语句只锁一小段，不积压 WAL。
	runPurgeBatch = 1000
	// runPurgeStatements 一轮至多发出的 DELETE 条数（运行记录与任务各一条，算两条），是时间预算之外的第二道闸
	// （语句意外变慢时兜底）。
	runPurgeStatements = 100
	// runPurgeBudget 一轮用于清理的时间预算：删满一批且预算没用完才发下一条。首次启用（或长期停机后）历史积压
	// 可能很大，每轮（每小时）只在这么多时间里分批删，剩下的留给后续轮次，不在一轮里猛删；预算也要盖得住日常
	// 流入（采集源多、周期短时每天新增十几万行），稳态下第一条语句就不足一批，预算用不上。
	runPurgeBudget = 10 * time.Second
)

// runPurgeOpts 同步任务与运行记录清理一轮的预算，取值见上。
var runPurgeOpts = loop.PurgeOptions{Batch: runPurgeBatch, MaxStatements: runPurgeStatements, Budget: runPurgeBudget}

// ErrPurgeDenied 当前数据库账号没有删除同步任务 / 运行记录的权限。数据面账号是 cmdb_sync_role 的成员，这两张表的
// DELETE 授权来自迁移；授权没跟上时它不是一轮维护的失败（见 MaintenanceUsecase.RunOnce）。
var ErrPurgeDenied = errors.New("purge sync records: permission denied")

// MaintenanceRepo 维护仓储：历史分区维护（实现须幂等：选主是优化而非正确性依赖）。
type MaintenanceRepo interface {
	// EnsurePartition 确保 table 的 month 月分区存在（独立建表 + CHECK → 短事务迁出 DEFAULT 行并 ATTACH）。
	EnsurePartition(ctx context.Context, table string, month time.Time) error
	// DropPartitionsBefore 删除上界 ≤ before 的月分区；返回删除数。
	DropPartitionsBefore(ctx context.Context, table string, before time.Time) (int, error)
}

// SyncRecordPurgeRepo 同步任务与运行记录的保留期清理（与 MaintenanceRepo 是同一个仓储实现，拆开是为了让每个
// 接口都不超过三个方法）。
type SyncRecordPurgeRepo interface {
	// PurgeRuns 清理已结束（succeeded / failed）且 finish_time 早于 now() - olderThan 的运行记录，至多 limit 条；
	// 各采集源最近一次运行（cmdb_sync_source.last_run_id 指向的）不删。返回清理条数；账号无 DELETE 权限返回
	// ErrPurgeDenied。
	PurgeRuns(ctx context.Context, olderThan time.Duration, limit int) (int, error)
	// PurgeTasks 清理自身已结束、finish_time 早于 now() - olderThan、且它指向的运行记录已不存在的同步任务，
	// 至多 limit 条。返回清理条数；账号无 DELETE 权限返回 ErrPurgeDenied。
	PurgeTasks(ctx context.Context, olderThan time.Duration, limit int) (int, error)
}

// MaintenanceOptions 维护参数。
type MaintenanceOptions struct {
	// PartitionsAhead 预建未来分区月数。
	PartitionsAhead int
	// HistoryRetention 历史保留期；0 = 不删除。
	HistoryRetention time.Duration
	// RunRetention 同步任务与运行记录保留期；0 = 不清理。
	RunRetention time.Duration
}

// MaintenanceUsecase 历史分区维护：预建未来分区、按保留期删除旧分区与保留期外的同步任务 / 运行记录。
type MaintenanceUsecase struct {
	repo  MaintenanceRepo
	purge SyncRecordPurgeRepo
	opts  MaintenanceOptions
	log   *slog.Logger
}

// NewMaintenanceUsecase new a MaintenanceUsecase.
func NewMaintenanceUsecase(repo MaintenanceRepo, purge SyncRecordPurgeRepo, log *slog.Logger) *MaintenanceUsecase {
	return &MaintenanceUsecase{repo: repo, purge: purge, log: log, opts: MaintenanceOptions{
		PartitionsAhead: 2, HistoryRetention: 365 * 24 * time.Hour, RunRetention: DefaultRunRetention,
	}}
}

// WithOptions 注入参数（非法值回退缺省；两个保留期都允许 0 = 不删除 / 不清理）。
func (uc *MaintenanceUsecase) WithOptions(o MaintenanceOptions) *MaintenanceUsecase {
	if o.PartitionsAhead > 0 {
		uc.opts.PartitionsAhead = o.PartitionsAhead
	}
	uc.opts.HistoryRetention = o.HistoryRetention
	uc.opts.RunRetention = o.RunRetention
	return uc
}

// RunOnce 一轮维护：当前月至未来 N 月分区预建；上界早于 now - retention 的分区删除；保留期外的同步任务与运行记录
// 分批清理。单项失败只记日志并继续（下一轮重试），最终返回首个错误供循环退避。
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
	if uc.opts.RunRetention > 0 {
		if err := uc.purgeSyncRecords(ctx); err != nil {
			if errors.Is(err, ErrPurgeDenied) {
				// 授权缺失不算这一轮的失败：返回错误会让维护循环进入错误退避（封顶 30 秒一次），分区维护被
				// 连带着反复重跑。记告警，等授权补上后的下一轮自然恢复。
				uc.log.WarnContext(ctx, "purge sync records skipped: this database role has no DELETE privilege on cmdb_sync_task / cmdb_sync_run")
			} else {
				uc.log.ErrorContext(ctx, "purge sync records failed", "error", err)
				keep(fmt.Errorf("purge sync records: %w", err))
			}
		}
	}
	return first
}

// purgeSyncRecords 分批清理保留期外的同步任务与运行记录，两条语句交替：先删一批运行记录，再删一批「运行记录已不存在」
// 的任务——任务只在它指向的运行记录删掉之后才可删，同一轮里成对消失。清到行时各记一条日志（带表名与条数）。
func (uc *MaintenanceUsecase) purgeSyncRecords(ctx context.Context) error {
	retention, batch := uc.opts.RunRetention, runPurgeOpts.Batch
	var runs, tasks int
	_, err := loop.PurgeBatched(ctx, runPurgeOpts,
		func(ctx context.Context) (int, error) {
			n, err := uc.purge.PurgeRuns(ctx, retention, batch)
			runs += n
			return n, err
		},
		func(ctx context.Context) (int, error) {
			n, err := uc.purge.PurgeTasks(ctx, retention, batch)
			tasks += n
			return n, err
		})
	if runs > 0 {
		uc.log.InfoContext(ctx, "purged sync runs past retention", "table", "cmdb_sync_run", "count", runs)
	}
	if tasks > 0 {
		uc.log.InfoContext(ctx, "purged sync tasks past retention", "table", "cmdb_sync_task", "count", tasks)
	}
	return err
}
