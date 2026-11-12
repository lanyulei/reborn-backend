package biz

import (
	"context"
	"log/slog"
	"time"
)

// SchedulerOptions 调度器参数。
type SchedulerOptions struct {
	StaleMultiplier  int32
	StagingRetention time.Duration
}

// SchedulerUsecase 调度器：到期采集源入队、陈旧标记、遗留暂存清理。由选主循环驱动，
// 但每条 SQL 自身幂等（任务唯一约束、stale 条件更新、暂存按运行隔离），选主只是优化。
type SchedulerUsecase struct {
	repo     SchedulerRepo
	notifier Notifier
	opts     SchedulerOptions
	log      *slog.Logger
	last     time.Time
}

// NewSchedulerUsecase new a SchedulerUsecase.
func NewSchedulerUsecase(repo SchedulerRepo, notifier Notifier, log *slog.Logger) *SchedulerUsecase {
	return &SchedulerUsecase{repo: repo, notifier: notifier, log: log, opts: SchedulerOptions{StaleMultiplier: 3, StagingRetention: 24 * time.Hour}}
}

// WithOptions 注入参数（非法值回退缺省）。
func (uc *SchedulerUsecase) WithOptions(o SchedulerOptions) *SchedulerUsecase {
	if o.StaleMultiplier > 0 {
		uc.opts.StaleMultiplier = o.StaleMultiplier
	}
	if o.StagingRetention > 0 {
		uc.opts.StagingRetention = o.StagingRetention
	}
	return uc
}

// TickOnce 一轮调度：入队到期源；每分钟顺带做一次陈旧标记与暂存清理（低频维护）。
// 返回入队数（>0 表示忙，循环立即再跑一轮）。
func (uc *SchedulerUsecase) TickOnce(ctx context.Context, now time.Time) (int, error) {
	n, err := uc.repo.EnqueueDue(ctx, now)
	if err != nil {
		return 0, err
	}
	if n > 0 {
		uc.log.InfoContext(ctx, "sync tasks enqueued", "count", n)
	}
	if now.Sub(uc.last) >= time.Minute {
		uc.last = now
		if stale, err := uc.repo.MarkStale(ctx, uc.opts.StaleMultiplier); err != nil {
			uc.log.ErrorContext(ctx, "mark stale resources failed", "error", err)
		} else if stale > 0 {
			uc.log.WarnContext(ctx, "resources marked stale", "count", stale)
			if uc.notifier != nil {
				if err := uc.notifier.Notify(ctx, staleEvent(stale)); err != nil {
					uc.log.ErrorContext(ctx, "emit stale event failed", "error", err)
				}
			}
		}
		if purged, err := uc.repo.PurgeStaging(ctx, uc.opts.StagingRetention); err != nil {
			uc.log.ErrorContext(ctx, "purge staging failed", "error", err)
		} else if purged > 0 {
			uc.log.WarnContext(ctx, "orphan staging rows purged", "count", purged)
		}
	}
	return n, nil
}
