package biz

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/kit/promclient"

	"golang.org/x/sync/errgroup"
)

// PlannerOptions 规划器参数。
type PlannerOptions struct {
	// ProbeTimeout 单次探活请求超时。
	ProbeTimeout time.Duration
	// ProbeConcurrency 探活并发数。
	ProbeConcurrency int
	// MaterializeBatch 每轮至多删除的失效任务数（有余量时循环立即进入下一轮）。
	MaterializeBatch int
}

// PlannerUsecase 规划器（选主单实例执行，语句自身幂等）：评估任务与记录规则任务的物化、数据源探活。
type PlannerUsecase struct {
	repo  PlannerRepo
	cache *ConfigCache
	obs   Observer
	log   *slog.Logger
	opts  PlannerOptions
	newID func() (string, error)
	// probes 探活失败日志的限频状态（按数据源）。
	probes failureLog
}

// NewPlannerUsecase new a PlannerUsecase.
func NewPlannerUsecase(repo PlannerRepo, cache *ConfigCache, obs Observer, log *slog.Logger) *PlannerUsecase {
	return &PlannerUsecase{
		repo: repo, cache: cache, obs: obs, log: log, newID: idgen.NewID,
		opts: PlannerOptions{ProbeTimeout: 5 * time.Second, ProbeConcurrency: 8, MaterializeBatch: 500},
	}
}

// WithOptions 注入参数（非法值保持缺省）。
func (uc *PlannerUsecase) WithOptions(o PlannerOptions) *PlannerUsecase {
	if o.ProbeTimeout > 0 {
		uc.opts.ProbeTimeout = min(o.ProbeTimeout, promclient.MaxTimeout)
	}
	if o.ProbeConcurrency > 0 {
		uc.opts.ProbeConcurrency = o.ProbeConcurrency
	}
	if o.MaterializeBatch > 0 {
		uc.opts.MaterializeBatch = o.MaterializeBatch
	}
	return uc
}

// Materialize 一轮任务物化：为「启用规则 × 其生效的启用数据源」补任务；删除不再成立的任务，
// 并同事务删除其判定状态、关闭其活跃告警（不发恢复通知）；记录规则的任务同样补齐与清理。
// 返回删除的任务数——删满一批时循环立即再跑一轮，直到清完。
func (uc *PlannerUsecase) Materialize(ctx context.Context) (int, error) {
	res, err := uc.repo.Materialize(ctx, uc.opts.MaterializeBatch)
	if err != nil {
		return 0, fmt.Errorf("materialize eval tasks: %w", err)
	}
	if res.Added > 0 || res.Removed > 0 || res.Hastened > 0 {
		uc.log.InfoContext(ctx, "eval tasks materialized", "added", res.Added, "removed", res.Removed,
			"alerts_closed", res.ClosedAlerts, "hastened", res.Hastened)
	}
	rec, err := uc.repo.MaterializeRecords(ctx, uc.opts.MaterializeBatch)
	if err != nil {
		return 0, fmt.Errorf("materialize record tasks: %w", err)
	}
	if rec.Added > 0 || rec.Removed > 0 || rec.Hastened > 0 {
		uc.log.InfoContext(ctx, "record tasks materialized", "added", rec.Added, "removed", rec.Removed, "hastened", rec.Hastened)
	}
	if res.Removed < uc.opts.MaterializeBatch && rec.Removed < uc.opts.MaterializeBatch {
		return 0, nil
	}
	return res.Removed + rec.Removed, nil
}

// Probe 一轮数据源探活：对全部启用的数据源并发执行 query=1（有界并发、各自超时），
// 一条语句写回健康状态。恒返回 0（按周期执行，不连续）。
func (uc *PlannerUsecase) Probe(ctx context.Context) (int, error) {
	if err := uc.cache.Refresh(ctx); err != nil {
		if errors.Is(err, ErrCacheNotLoaded) {
			return 0, nil
		}
		return 0, err
	}
	handles := uc.cache.EnabledDatasources()
	if len(handles) == 0 {
		return 0, nil
	}
	results := make([]HealthResult, len(handles))
	var g errgroup.Group
	g.SetLimit(uc.opts.ProbeConcurrency)
	for i, h := range handles {
		g.Go(func() error {
			results[i] = HealthResult{DatasourceID: h.Info.ID, Error: uc.probeOne(ctx, h)}
			return nil
		})
	}
	_ = g.Wait()
	if err := ctx.Err(); err != nil {
		// 停机中被打断的探测不是数据源故障：不写回。
		return 0, err
	}
	// 从异常转为正常的数据源：随状态翻转同事务写一条恢复事件（只在那次故障的 down 事件发过时才真的写入）。
	recovered, err := uc.repo.WriteHealth(ctx, results, func(r HealthRecovery) (*EngineEventInsert, error) {
		return datasourceRecoveredEvent(uc.newID, r)
	})
	if err != nil {
		return 0, fmt.Errorf("write datasource health: %w", err)
	}
	if recovered > 0 {
		uc.obs.AddEngineEvents(ctx, monitorcontract.EventEngineDatasourceUp, recovered)
	}
	// 只在状态变化时（以及故障持续期间每隔一段时间）记日志，不逐轮刷屏。
	now := time.Now()
	for i, h := range handles {
		failed := results[i].Error != ""
		if !uc.probes.observe(h.Info.ID, failed, now) {
			continue
		}
		if failed {
			uc.log.WarnContext(ctx, "datasource health probe is failing", "datasource_id", h.Info.ID,
				"datasource_name", h.Info.Name, "error", results[i].Error)
		} else {
			uc.log.InfoContext(ctx, "datasource health probe recovered", "datasource_id", h.Info.ID, "datasource_name", h.Info.Name)
		}
	}
	return 0, nil
}

// probeOne 探测一个数据源；返回空串 = 正常，否则为不含凭据的失败原因。
func (uc *PlannerUsecase) probeOne(ctx context.Context, h *DatasourceHandle) string {
	if msg := h.Unavailable(); msg != "" {
		return msg
	}
	pctx, cancel := context.WithTimeout(ctx, uc.opts.ProbeTimeout)
	defer cancel()
	err := h.Ping(pctx)
	if err == nil {
		return ""
	}
	var perr *promclient.Error
	if errors.As(err, &perr) {
		return truncate(perr.Error(), maxErrorBytes)
	}
	return "probe did not start before the timeout: datasource busy"
}
