package biz

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/kit/cmdbcontract"

	"golang.org/x/sync/errgroup"
)

// 哨兵错误。
var (
	// ErrLeaseLost 任务租约已被他人接管或任务已不在运行态。
	ErrLeaseLost = errors.New("sync task lease lost")
	// ErrProviderUnknown 没有对应提供方的采集器。
	ErrProviderUnknown = errors.New("no collector registered for provider")
	// ErrCredentialMissing 采集源未配置凭据。
	ErrCredentialMissing = errors.New("sync source credential is not configured")
)

// WorkerOptions 执行器参数。
type WorkerOptions struct {
	Lease          time.Duration
	CollectTimeout time.Duration
	StagingBatch   int
}

// WorkerUsecase 任务执行器：领取 → 解密凭据 → 采集入暂存 → 对账 → 完成；全程租约续约，
// 失主即中止（ctx 取消）。一次 RunOnce 处理一个任务；并发由多条循环 / 多实例提供。
type WorkerUsecase struct {
	sources    SourceRepo
	tasks      TaskRepo
	staging    StagingRepo
	reconcile  ReconcileRepo
	meta       MetaRepo
	opener     CredentialOpener
	notifier   Notifier
	collectors map[string]Collector
	opts       WorkerOptions
	log        *slog.Logger
}

// NewWorkerUsecase new a WorkerUsecase（collectors 由 data 层注册表提供）。
func NewWorkerUsecase(sources SourceRepo, tasks TaskRepo, staging StagingRepo, reconcile ReconcileRepo, meta MetaRepo,
	opener CredentialOpener, notifier Notifier, collectors []Collector, log *slog.Logger,
) *WorkerUsecase {
	reg := make(map[string]Collector, len(collectors))
	for _, c := range collectors {
		reg[c.Provider()] = c
	}
	return &WorkerUsecase{
		sources: sources, tasks: tasks, staging: staging, reconcile: reconcile, meta: meta, opener: opener, notifier: notifier, collectors: reg, log: log,
		opts: WorkerOptions{Lease: 5 * time.Minute, CollectTimeout: 10 * time.Minute, StagingBatch: 500},
	}
}

// WithOptions 注入参数（非法值回退缺省）。
func (uc *WorkerUsecase) WithOptions(o WorkerOptions) *WorkerUsecase {
	if o.Lease > 0 {
		uc.opts.Lease = o.Lease
	}
	if o.CollectTimeout > 0 {
		uc.opts.CollectTimeout = o.CollectTimeout
	}
	if o.StagingBatch > 0 {
		uc.opts.StagingBatch = o.StagingBatch
	}
	return uc
}

// RunOnce 领取并执行一个任务；无任务返回 0。任务级失败写入任务/运行终态后返回 nil
// （不是循环错误）；只有领取本身失败（DB 不可达）才返回 error。
func (uc *WorkerUsecase) RunOnce(ctx context.Context, token string) (int, error) {
	task, err := uc.tasks.Claim(ctx, token, uc.opts.Lease)
	if err != nil {
		return 0, err
	}
	if task == nil {
		return 0, nil
	}
	log := uc.log.With("task_id", task.ID, "run_id", task.RunID, "source_id", task.SourceID, "attempt", task.Attempts)
	log.InfoContext(ctx, "sync task claimed")
	src, counts, samples, runErr := uc.execute(ctx, task, log)
	if runErr != nil && ctx.Err() != nil {
		// 进程停机打断的同步不是任务失败：不写终态、不发失败告警，任务保持 running，
		// 租约到期后由存活副本 / 重启后的进程重领重跑（重领时上一条运行记录置失败）。
		log.WarnContext(ctx, "sync task interrupted by shutdown: left for re-claim after lease expiry")
		if err := uc.staging.Clear(context.WithoutCancel(ctx), task.RunID); err != nil {
			log.ErrorContext(ctx, "clear staging failed (scheduler will purge)", "error", err)
		}
		return 1, nil
	}
	status := cmdbcontract.TaskSucceeded
	msg := ""
	if runErr != nil {
		status, msg = cmdbcontract.TaskFailed, truncate(runErr.Error(), 1024)
		log.ErrorContext(ctx, "sync task failed", "error", runErr)
	} else {
		log.InfoContext(ctx, "sync task finished", "discovered", counts.Discovered, "created", counts.Created, "updated", counts.Updated,
			"unchanged", counts.Unchanged, "disappeared", counts.Disappeared, "retired", counts.Retired, "failed", counts.Failed)
	}
	// 完成写入用不可取消的 ctx：停机期间也要把终态落库（租约围栏保证不覆盖他人结果）。
	if err := uc.tasks.Finish(context.WithoutCancel(ctx), task, status, counts, msg, samples); err != nil {
		if errors.Is(err, ErrLeaseLost) {
			log.WarnContext(ctx, "sync task finish skipped: lease lost")
			return 1, nil
		}
		return 1, err
	}
	if err := uc.staging.Clear(context.WithoutCancel(ctx), task.RunID); err != nil {
		log.ErrorContext(ctx, "clear staging failed (scheduler will purge)", "error", err)
	}
	uc.emit(context.WithoutCancel(ctx), src, task, counts, msg, log)
	return 1, nil
}

// emit 同步结果通知：失败 → critical（按源去重一个周期）；有消失/退役 → warning。
// 通知失败只记日志（事件是旁路，不影响对账结果）。
func (uc *WorkerUsecase) emit(ctx context.Context, src *Source, task *Task, counts Counts, runErr string, log *slog.Logger) {
	if uc.notifier == nil || src == nil {
		return
	}
	var events []SyncEvent
	switch {
	case runErr != "":
		events = append(events, failedEvent(src, task.RunID, runErr))
	default:
		if counts.Created > 0 || counts.Updated > 0 {
			events = append(events, changedEvent(src, task.RunID, counts))
		}
		if counts.Disappeared > 0 || counts.Retired > 0 {
			events = append(events, disappearedEvent(src, task.RunID, counts))
		}
	}
	for _, e := range events {
		if err := uc.notifier.Notify(ctx, e); err != nil {
			log.ErrorContext(ctx, "emit sync event failed", "event_type", e.Type, "error", err)
		}
	}
}

// execute 任务主体：续约协程与采集/对账并行，任一失败即取消另一方。返回的 src 供通知使用（可能为 nil）。
func (uc *WorkerUsecase) execute(ctx context.Context, task *Task, log *slog.Logger) (*Source, Counts, []cmdbcontract.RunError, error) {
	src, err := uc.sources.Get(ctx, task.SourceID)
	if err != nil {
		return nil, Counts{}, nil, err
	}
	counts, samples, err := uc.run(ctx, task, src, log)
	return src, counts, samples, err
}

func (uc *WorkerUsecase) run(ctx context.Context, task *Task, src *Source, log *slog.Logger) (Counts, []cmdbcontract.RunError, error) {
	collector, ok := uc.collectors[src.Provider]
	if !ok {
		return Counts{}, nil, fmt.Errorf("%w: %s", ErrProviderUnknown, src.Provider)
	}
	if len(src.Credential) == 0 {
		return Counts{}, nil, ErrCredentialMissing
	}
	credential, err := uc.opener.Open(src.Credential)
	if err != nil {
		return Counts{}, nil, fmt.Errorf("open credential: %w", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	g, gctx := errgroup.WithContext(runCtx)
	g.Go(func() error { return uc.keepAlive(gctx, task) })

	var counts Counts
	var samples []cmdbcontract.RunError
	g.Go(func() error {
		defer cancel() // 主体结束即停止续约
		var err error
		counts, samples, err = uc.collectAndReconcile(gctx, task, src, collector, credential, log)
		return err
	})
	if err := g.Wait(); err != nil {
		if ctx.Err() != nil {
			return counts, samples, fmt.Errorf("sync interrupted: %w", ctx.Err())
		}
		// 上层未取消时出现的任何错误（含采集器内部包装的 context.Canceled）都按失败处理，
		// 不得当作成功——否则一次没采完的同步会以 0 计数记为成功。
		return counts, samples, err
	}
	return counts, samples, nil
}

// keepAlive 每 lease/3 续约一次；失主返回 ErrLeaseLost（取消采集）。瞬时续约错误（DB
// 抖动）不中止整次同步：改按 lease/10 快速重试，直到续上，或租约剩余不足 lease/3（再拖
// 就可能与重领者并行对账）才放弃。
func (uc *WorkerUsecase) keepAlive(ctx context.Context, task *Task) error {
	interval, retry := uc.opts.Lease/3, uc.opts.Lease/10
	expiry := time.Now().Add(uc.opts.Lease) // 领取略早于此刻：近似值，由 lease/3 余量覆盖
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			start := time.Now()
			err := uc.tasks.Renew(ctx, task, uc.opts.Lease)
			switch {
			case err == nil:
				expiry = start.Add(uc.opts.Lease)
				ticker.Reset(interval)
			case ctx.Err() != nil:
				return nil
			case errors.Is(err, ErrLeaseLost) || time.Until(expiry) <= interval:
				return err
			default:
				uc.log.WarnContext(ctx, "renew sync task lease failed, retrying", "task_id", task.ID, "error", err)
				ticker.Reset(retry)
			}
		}
	}
}

func (uc *WorkerUsecase) collectAndReconcile(ctx context.Context, task *Task, src *Source, collector Collector, credential string, log *slog.Logger) (Counts, []cmdbcontract.RunError, error) {
	sink := newStagingSink(uc.staging, uc.meta, task.RunID, uc.opts.StagingBatch)
	sink.fieldMap = src.fieldMap()
	collectCtx, cancel := context.WithTimeout(ctx, uc.opts.CollectTimeout)
	defer cancel()
	if err := collector.Discover(collectCtx, src, credential, sink); err != nil {
		return Counts{}, nil, fmt.Errorf("collect %s: %w", src.Provider, err)
	}
	if err := sink.Flush(ctx); err != nil {
		return Counts{}, nil, err
	}
	discovered, err := uc.staging.Count(ctx, task.RunID)
	if err != nil {
		return Counts{}, nil, err
	}
	counts := Counts{Discovered: discovered}
	// 空集保护：采集成功但一条资源都没有时，不做消失判定（避免凭据/权限变更导致整批退役）。
	skipDisappear := discovered == 0
	if skipDisappear {
		log.WarnContext(ctx, "empty discovery: disappearance check skipped")
	}
	source := cmdbcontract.SourceOf(src.ID)
	grace := time.Duration(src.GracePeriodSeconds) * time.Second
	models, err := uc.staging.Models(ctx, task.RunID)
	if err != nil {
		return counts, nil, err
	}
	staged := len(models) // models[:staged] 本次有暂存行；其后为补入的、本次 0 行的类型
	// 本提供方可写但本次未出现的类型也要参与消失判定（如 VMware 全部虚拟机被删）。
	for _, m := range cmdbcontract.ProviderModels(src.Provider) {
		if !contains(models, m) {
			models = append(models, m)
		}
	}
	var samples []cmdbcontract.RunError
	for i, model := range models {
		fields, err := uc.meta.Fields(ctx, model)
		if err != nil {
			return counts, samples, err
		}
		// 按类型的空集保护：本次该类型一条都没采到（账号失去该类资源的可见性），或类型
		// 停用/不存在（采集行在入暂存前已被丢弃）时，不做该类型的消失/退役判定——否则
		// 只要别的类型有数据，这一类型的全部资源就会被标记消失并在宽限期后整批退役。
		// 代价：某类型资源真的全部下线时不会自动退役（由陈旧标记体现，需人工处理）。
		skip := skipDisappear
		if !skip && (i >= staged || len(fields) == 0) {
			skip = true
			active, err := uc.reconcile.HasActive(ctx, model, source)
			if err != nil {
				return counts, samples, err
			}
			if active {
				log.WarnContext(ctx, "no resources discovered for model with active resources: disappearance check skipped",
					"model", model, "model_enabled", len(fields) > 0)
			}
		}
		var manual, unique []string
		for _, f := range fields {
			if f.Owner == cmdbcontract.FieldOwnerManual {
				manual = append(manual, f.Code)
			}
			if f.Unique {
				unique = append(unique, f.Code)
			}
		}
		c, errs, err := uc.reconcile.ReconcileModel(ctx, ReconcileParams{
			RunID: task.RunID, ModelCode: model, Source: source, Env: src.Env, Grace: grace,
			ManualKeys: manual, UniqueCodes: unique, SkipDisappear: skip,
		})
		if err != nil {
			return counts, samples, fmt.Errorf("reconcile %s: %w", model, err)
		}
		counts.add(c)
		samples = appendSamples(samples, errs)
	}
	if err := uc.reconcile.ReconcileRelations(ctx, task.RunID, source); err != nil {
		return counts, samples, fmt.Errorf("reconcile relations: %w", err)
	}
	return counts, samples, nil
}

func appendSamples(dst, src []cmdbcontract.RunError) []cmdbcontract.RunError {
	for _, e := range src {
		if len(dst) >= cmdbcontract.RunErrorSampleLimit {
			break
		}
		dst = append(dst, e)
	}
	return dst
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return strings.ToValidUTF8(s[:n], "")
}

// stagingSink 采集结果接收器：按类型字段过滤属性（未定义字段丢弃；人工归属字段不
// 由同步写入）、拼唯一键、分批写暂存。类型字段按 modelCode 缓存于本次运行。
type stagingSink struct {
	staging StagingRepo
	meta    MetaRepo
	runID   string
	batch   int
	buf     []*Resource
	fields  map[string][]ModelField
	// fieldMap 采集器输出键 → 模型字段编码（采集源配置 field_map），在字段过滤前应用。
	fieldMap map[string]string
}

func newStagingSink(staging StagingRepo, meta MetaRepo, runID string, batch int) *stagingSink {
	return &stagingSink{staging: staging, meta: meta, runID: runID, batch: batch, fields: map[string][]ModelField{}}
}

// Put implements Sink.
func (s *stagingSink) Put(ctx context.Context, r *Resource) error {
	if r == nil || r.ModelCode == "" || r.ExternalID == "" {
		return errors.New("resource model_code and external_id are required")
	}
	fields, ok := s.fields[r.ModelCode]
	if !ok {
		var err error
		if fields, err = s.meta.Fields(ctx, r.ModelCode); err != nil {
			return err
		}
		s.fields[r.ModelCode] = fields
	}
	if len(fields) == 0 {
		// 类型不存在/停用：丢弃（运行记录由 reconcile 阶段的 0 行自然体现）。
		return nil
	}
	src := r.Attrs
	if len(s.fieldMap) > 0 {
		src = make(map[string]any, len(r.Attrs))
		for k, v := range r.Attrs {
			src[k] = v
		}
		for from, to := range s.fieldMap {
			if v, ok := r.Attrs[from]; ok {
				src[to] = v
			}
		}
	}
	attrs := make(map[string]any, len(fields))
	for _, f := range fields {
		if f.Owner == cmdbcontract.FieldOwnerManual {
			continue
		}
		if v, ok := src[f.Code]; ok && v != nil {
			attrs[f.Code] = v
		}
	}
	name := strings.TrimSpace(r.Name)
	if name == "" {
		name = r.ExternalID
	}
	s.buf = append(s.buf, &Resource{ModelCode: r.ModelCode, ExternalID: r.ExternalID, Name: truncate(name, 128), Attrs: attrs, Relations: r.Relations})
	if len(s.buf) >= s.batch {
		return s.Flush(ctx)
	}
	return nil
}

// Flush 写出缓冲。
func (s *stagingSink) Flush(ctx context.Context) error {
	if len(s.buf) == 0 {
		return nil
	}
	if err := s.staging.Append(ctx, s.runID, s.buf); err != nil {
		return fmt.Errorf("append staging: %w", err)
	}
	s.buf = s.buf[:0]
	return nil
}
