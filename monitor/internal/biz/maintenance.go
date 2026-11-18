package biz

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// MaintenanceOptions 维护参数。
type MaintenanceOptions struct {
	// PartitionsAhead 判定记录表预建的未来分区天数。
	PartitionsAhead int
	// EvalRecordRetention 判定记录保留期，到期整分区删除。
	EvalRecordRetention time.Duration
	// AlertRetention 非活跃告警（连同其通知记录、时间线、事件动作、事件指令、升级进度）、已派发发件箱行、
	// 已派发的自监控事件、已处理的事件指令的保留期；0 = 不删除。
	AlertRetention time.Duration
	// PurgeBatch 行级清理单批行数；MaxBatches 单轮每类清理最多跑的批数。
	PurgeBatch int
	MaxBatches int
}

// MaintenanceUsecase 维护（选主单实例执行，语句自身幂等）：判定记录分区的预建与过期删除，
// 过期告警（连同其附属行）/ 发件箱行 / 自监控事件 / 事件指令的分批清理。
type MaintenanceUsecase struct {
	partitions PartitionRepo
	retention  RetentionRepo
	log        *slog.Logger
	opts       MaintenanceOptions
}

// NewMaintenanceUsecase new a MaintenanceUsecase.
func NewMaintenanceUsecase(partitions PartitionRepo, retention RetentionRepo, log *slog.Logger) *MaintenanceUsecase {
	return &MaintenanceUsecase{
		partitions: partitions, retention: retention, log: log,
		opts: MaintenanceOptions{
			PartitionsAhead: 2, EvalRecordRetention: 7 * 24 * time.Hour, AlertRetention: 180 * 24 * time.Hour,
			PurgeBatch: 1000, MaxBatches: 100,
		},
	}
}

// WithOptions 注入参数（非法值保持缺省；AlertRetention 允许 0 = 不删除，负值按 0）。
func (uc *MaintenanceUsecase) WithOptions(o MaintenanceOptions) *MaintenanceUsecase {
	if o.PartitionsAhead > 0 {
		uc.opts.PartitionsAhead = o.PartitionsAhead
	}
	if o.EvalRecordRetention > 0 {
		uc.opts.EvalRecordRetention = o.EvalRecordRetention
	}
	uc.opts.AlertRetention = max(o.AlertRetention, 0)
	if o.PurgeBatch > 0 {
		uc.opts.PurgeBatch = o.PurgeBatch
	}
	if o.MaxBatches > 0 {
		uc.opts.MaxBatches = o.MaxBatches
	}
	return uc
}

// RunOnce 一轮维护。单项失败只记日志并继续（下一轮重试），最终返回首个错误供循环退避。
func (uc *MaintenanceUsecase) RunOnce(ctx context.Context) error {
	var first error
	keep := func(what string, err error) {
		if err == nil {
			return
		}
		uc.log.ErrorContext(ctx, "maintenance step failed", "step", what, "error", err)
		if first == nil {
			first = fmt.Errorf("%s: %w", what, err)
		}
	}
	keep("ensure eval record partitions", uc.partitions.EnsurePartitions(ctx, uc.opts.PartitionsAhead))
	dropped, err := uc.partitions.DropExpiredPartitions(ctx, uc.opts.EvalRecordRetention)
	keep("drop expired eval record partitions", err)
	stray, err := uc.batched(ctx, func(ctx context.Context) (int, error) {
		return uc.partitions.PurgeDefaultPartition(ctx, uc.opts.EvalRecordRetention, uc.opts.PurgeBatch)
	})
	keep("purge default eval record partition", err)
	var alerts, outbox, events, commands int
	if uc.opts.AlertRetention > 0 {
		alerts, err = uc.batched(ctx, func(ctx context.Context) (int, error) {
			return uc.retention.PurgeAlerts(ctx, uc.opts.AlertRetention, uc.opts.PurgeBatch)
		})
		keep("purge expired alerts", err)
		outbox, err = uc.batched(ctx, func(ctx context.Context) (int, error) {
			return uc.retention.PurgeOutbox(ctx, uc.opts.AlertRetention, uc.opts.PurgeBatch)
		})
		keep("purge dispatched outbox rows", err)
		events, err = uc.batched(ctx, func(ctx context.Context) (int, error) {
			return uc.retention.PurgeEngineEvents(ctx, uc.opts.AlertRetention, uc.opts.PurgeBatch)
		})
		keep("purge dispatched engine events", err)
		commands, err = uc.batched(ctx, func(ctx context.Context) (int, error) {
			return uc.retention.PurgeCommands(ctx, uc.opts.AlertRetention, uc.opts.PurgeBatch)
		})
		keep("purge handled alert commands", err)
	}
	if dropped+stray+alerts+outbox+events+commands > 0 {
		uc.log.InfoContext(ctx, "maintenance finished", "partitions_dropped", dropped, "default_rows_purged", stray,
			"alerts_purged", alerts, "outbox_purged", outbox, "engine_events_purged", events, "commands_purged", commands)
	}
	return first
}

// batched 反复执行一个「删一批、返回删除数」的步骤，直到不足一批、达到批数上限或 ctx 结束。
func (uc *MaintenanceUsecase) batched(ctx context.Context, step func(ctx context.Context) (int, error)) (int, error) {
	total := 0
	for range uc.opts.MaxBatches {
		n, err := step(ctx)
		total += n
		if err != nil {
			return total, err
		}
		if n < uc.opts.PurgeBatch {
			break
		}
		if err := ctx.Err(); err != nil {
			return total, err
		}
	}
	return total, nil
}
