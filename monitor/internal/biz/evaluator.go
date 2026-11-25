package biz

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/kit/monitorpipeline"
	"github.com/lanyulei/reborn/backend/kit/promclient"

	"golang.org/x/sync/errgroup"
)

// 评估器的固定参数。
const (
	// retryDelay 任务暂时无法评估（引擎自身故障、规则 / 数据源待清理）时归还并推迟的时长。
	retryDelay = 5 * time.Second
	// roundQueryBudgets 一轮评估里以「数据源查询超时」为上限的阶段数：等并发槽、判定查询、恢复取值查询。
	// 租约（TaskRepo.Claim）与单轮评估的总时限都按它计。
	roundQueryBudgets = 3
	// maxCommitAttempts 一轮评估的提交次数上限（第一次之后的每一次都是补渲染附加信息后的重试）。
	maxCommitAttempts = 3
	// evalJitter 每轮评估周期上附加的随机抖动上限（且不超过周期的 1/10）。
	evalJitter = time.Second
	// incompletePrefix 结果不完整时任务 last_error 与判定记录 error 的固定前缀（此时状态仍是评估结果）。
	incompletePrefix = "incomplete result: "
	// labelLimitPrefix 有序列的标签超过上限时，任务 last_error 与判定记录 error 里那段说明的固定前缀
	// （结果同时不完整时接在不完整的说明之后，以 "; " 分隔）。
	labelLimitPrefix = "label limit: "
	// dropErrorPrefix 条件丢弃表达式对部分序列渲染失败（因而没有丢弃）时那段说明的固定前缀。
	dropErrorPrefix = "drop expression: "
	// settleTimeout 查询之外的数据库往返（读规则、落库、归还）的时间预算，也是任务租约余量的基数：单轮评估的
	// 总时限（roundQueryBudgets × 查询超时 + settleTimeout）与租约（roundQueryBudgets × 查询超时 + leaseMargin）
	// 共用它，两者不会再各算各的而漂移。
	settleTimeout = 30 * time.Second
	// leaseMargin 租约在三个查询超时（等并发槽 + 判定查询 + 恢复取值查询）之外留给数据库往返的余量。必须大于
	// settleTimeout：否则数据源与数据库同时很慢时，单轮评估还在自己的总时限之内、租约却已到期被别的实例
	// 重领，本轮作废（ErrLeaseLost）、状态不推进。多出的部分留给领取结果的回传与调度——租约从数据库执行
	// 领取语句时起算，早于本实例开始为单轮评估计时。
	leaseMargin = settleTimeout + 5*time.Second
	// minInterval 评估周期下限：表约束保证 ≥ 15s，这里只防脏数据把任务变成空转循环。
	minInterval = time.Second
	// maxRecoveryQueries 单轮最多执行的恢复取值查询数；超出的事件不带恢复值（恢复不受影响）。
	maxRecoveryQueries = 20
	// recoveryConcurrency 恢复取值查询的并发数。
	recoveryConcurrency = 4
)

// EvaluatorOptions 评估器参数。
type EvaluatorOptions struct {
	// Batch 单次领取的任务数上限。
	Batch int
	// Lease 任务租约下限；实际租约 = max(Lease, roundQueryBudgets × 数据源查询超时 + leaseMargin)。
	Lease time.Duration
	// QueryTimeoutMax 单次查询超时的上限。
	QueryTimeoutMax time.Duration
}

// EvaluatorUsecase 评估器：领取到期任务 → 查询数据源 → 状态机 → 单事务落库。
// 查询（网络 I/O）与模板渲染全部发生在事务之外；状态转移由纯函数 Advance 计算。
type EvaluatorUsecase struct {
	tasks    TaskRepo
	rules    RuleRepo
	rounds   RoundRepo
	cache    *ConfigCache
	inhibits *InhibitCache
	stats    *InstanceStats
	obs      Observer
	log      *slog.Logger
	opts     EvaluatorOptions
	newID    func() (string, error)
	// render 渲染新事件的附加信息（renderAnnotations；测试可替换）。
	render func(source map[string]string, samples []*Sample) map[string]map[string]string
	// 日志限频状态：incomplete「结果不完整」（按规则 × 数据源）；busy「并发槽排不上」与
	// upstream「数据源侧查询故障」（按数据源）。
	incomplete failureLog
	busy       failureLog
	upstream   failureLog
}

// NewEvaluatorUsecase new an EvaluatorUsecase.
func NewEvaluatorUsecase(tasks TaskRepo, rules RuleRepo, rounds RoundRepo, cache *ConfigCache, inhibits *InhibitCache,
	stats *InstanceStats, obs Observer, log *slog.Logger,
) *EvaluatorUsecase {
	return &EvaluatorUsecase{
		tasks: tasks, rules: rules, rounds: rounds, cache: cache, inhibits: inhibits, stats: stats, obs: obs, log: log,
		newID: idgen.NewID, render: renderAnnotations,
		opts: EvaluatorOptions{Batch: 10, Lease: time.Minute, QueryTimeoutMax: promclient.MaxTimeout},
	}
}

// WithOptions 注入参数（非法值保持缺省）。
func (uc *EvaluatorUsecase) WithOptions(o EvaluatorOptions) *EvaluatorUsecase {
	if o.Batch > 0 {
		uc.opts.Batch = o.Batch
	}
	if o.Lease > 0 {
		uc.opts.Lease = o.Lease
	}
	if o.QueryTimeoutMax > 0 {
		uc.opts.QueryTimeoutMax = min(o.QueryTimeoutMax, promclient.MaxTimeout)
	}
	return uc
}

// RunOnce 领取一批到期任务并发评估（并发度即批大小；对单个数据源的并发另受其并发槽约束），
// 返回领取数（> 0 时循环立即进入下一轮）。批内不串行：一个慢数据源上的任务不会让同批其余任务
// 攥着租约排队。
//
// 停机：ctx 在领取后已取消则整批归还（其他实例可立刻重领）；已开始的任务在不受取消的 ctx 上跑完
// （受自身超时约束），避免把「被取消的查询」落成一轮查询错误。进程被强杀时租约到期即可被重领。
func (uc *EvaluatorUsecase) RunOnce(ctx context.Context, token string) (int, error) {
	if err := refreshCaches(ctx, uc.cache, uc.inhibits); err != nil || uc.inhibits.View() == nil {
		// 启动瞬间另一条循环正在做首次加载（err 为 nil）：本轮空转，不算故障。
		return 0, err
	}
	return claimAndRun(ctx, uc.tasks, token, uc.opts, uc.log, uc.evaluateOne)
}

// refreshCaches 刷新配置缓存与（可选的）抑制快照。启动瞬间别的循环正在做首次加载时返回 nil：
// 调用方自己看缓存是否已就绪。
func refreshCaches(ctx context.Context, cache *ConfigCache, inhibits *InhibitCache) error {
	err := cache.Refresh(ctx)
	if err == nil && inhibits != nil {
		err = inhibits.Refresh(ctx)
	}
	if errors.Is(err, ErrCacheNotLoaded) {
		return nil
	}
	return err
}

// claimAndRun 领取一批到期任务并发执行（评估任务与记录规则任务共用），返回领取数。
func claimAndRun(ctx context.Context, tasks TaskRepo, token string, o EvaluatorOptions, log *slog.Logger,
	run func(ctx context.Context, task *Task),
) (int, error) {
	claimed, err := tasks.Claim(ctx, token, o.Batch, o.Lease, o.QueryTimeoutMax, leaseMargin)
	if err != nil {
		return 0, fmt.Errorf("claim tasks: %w", err)
	}
	work := context.WithoutCancel(ctx)
	if ctx.Err() != nil {
		releaseTasks(work, tasks, token, claimed, 0, log)
		return len(claimed), nil
	}
	var g errgroup.Group
	for _, task := range claimed {
		g.Go(func() error {
			defer recoverTask(work, tasks, task, log)
			run(work, task)
			return nil
		})
	}
	_ = g.Wait()
	return len(claimed), nil
}

// recoverTask 兜住单个任务执行中的 panic（须直接 defer）。errgroup 与 kit/loop 都不 recover：一条规则的脏配置 /
// 异常的上游返回体若能确定性地触发 panic，进程重启后同一任务再被领取，整个引擎就因一条规则陷入重启循环。
// 这里按「引擎自身故障」的既有路径处理：记 ERROR（带堆栈）并归还任务、retryDelay 后重试——不能只吞掉，
// 否则任务悬在租约里要等到期才会被重领。没有 panic 时什么都不做。
func recoverTask(ctx context.Context, repo TaskRepo, task *Task, log *slog.Logger) {
	r := recover()
	if r == nil {
		return
	}
	log.ErrorContext(ctx, "task panicked: released for a delayed retry",
		"rule_id", task.RuleID, "datasource_id", task.DatasourceID, "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
	releaseTasks(ctx, repo, task.Token, []*Task{task}, retryDelay, log)
}

// queryPanic 查询子 goroutine 里兜住的 panic：带上原始堆栈，回到任务 goroutine 上重新抛出。
type queryPanic struct {
	value any
	stack []byte
}

// fanOut 并发执行 run(0..n-1) 并等全部返回。子 goroutine 里的 panic 不会被任务 goroutine 的 recover 兜住：
// 在这里接住，等全部返回后回到调用方 goroutine 上重新抛出（带原始堆栈），交给 claimAndRun 的 recoverTask
// 按引擎自身故障处理。
func fanOut(n int, run func(i int)) {
	var (
		g        errgroup.Group
		panicked atomic.Pointer[queryPanic]
	)
	for i := range n {
		g.Go(func() error {
			defer func() {
				if r := recover(); r != nil {
					panicked.CompareAndSwap(nil, &queryPanic{value: r, stack: debug.Stack()})
				}
			}()
			run(i)
			return nil
		})
	}
	_ = g.Wait()
	if p := panicked.Load(); p != nil {
		panic(fmt.Sprintf("%v\n%s", p.value, p.stack))
	}
}

// releaseTasks 归还任务；失败只记日志（租约到期后自然可被重领）。
func releaseTasks(ctx context.Context, repo TaskRepo, token string, tasks []*Task, delay time.Duration, log *slog.Logger) {
	if len(tasks) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, settleTimeout)
	defer cancel()
	if err := repo.Release(ctx, token, tasks, delay); err != nil {
		log.WarnContext(ctx, "release tasks failed, leases will expire", "error", err, "tasks", len(tasks))
	}
}

// evaluateOne 评估一个任务并记指标；失败只影响该任务（记日志、归还后稍后重试），不向循环返回错误。
func (uc *EvaluatorUsecase) evaluateOne(ctx context.Context, task *Task) {
	start := time.Now()
	ectx, cancel := context.WithTimeout(ctx, roundQueryBudgets*uc.opts.QueryTimeoutMax+settleTimeout)
	result, err := uc.evaluate(ectx, task, start)
	cancel()
	switch {
	case errors.Is(err, ErrLeaseLost):
		result = ResultLeaseLost
		uc.log.InfoContext(ctx, "eval round discarded: lease lost", "rule_id", task.RuleID, "datasource_id", task.DatasourceID)
	case err != nil:
		result = ResultFailed
		uc.log.ErrorContext(ctx, "eval round failed", "rule_id", task.RuleID, "datasource_id", task.DatasourceID, "error", err)
		releaseTasks(ctx, uc.tasks, task.Token, []*Task{task}, retryDelay, uc.log)
	}
	uc.stats.observe(result, task.Lag)
	uc.obs.ObserveEval(ctx, result, time.Since(start), task.Lag)
}

// evaluate 一轮评估（设计 §4.2 第 2-7 步）。
func (uc *EvaluatorUsecase) evaluate(ctx context.Context, task *Task, start time.Time) (string, error) {
	rule, now, err := uc.rules.LoadRule(ctx, task.RuleID)
	if err != nil && !errors.Is(err, ErrRuleNotFound) {
		return "", fmt.Errorf("load rule: %w", err)
	}
	h := uc.cache.Datasource(task.DatasourceID)
	if err != nil || !rule.Active || h == nil || !h.Info.Enabled {
		// 规则或数据源已不可评估：不动状态，等规划器删除任务并关闭告警。
		if err := uc.tasks.Release(ctx, task.Token, []*Task{task}, retryDelay); err != nil {
			return "", fmt.Errorf("release unevaluable task: %w", err)
		}
		return ResultSkipped, nil
	}
	round := &Round{
		Now: now, Interval: nextInterval(rule.Interval), Exprs: make([]string, len(rule.Queries)),
		Alert: AlertMeta{RuleName: rule.Name, GroupID: rule.GroupID, DatasourceName: h.Info.Name},
	}
	for i, q := range rule.Queries {
		round.Exprs[i] = q.Expr
	}
	// skip 记一轮没有走到状态机的评估。查询错误的 WARN 限频：数据源侧的故障（连不上、超时、凭据不可用）
	// 按数据源每 failureRemind 至多一条；规则自身的问题（表达式错误、结果类型不对）只在该任务进入查询错误的
	// 那一轮记（上一轮的结果随任务行领取，多实例下同样成立）。都不逐轮刷屏。
	skip := func(status int32, result, msg string, upstream bool) (string, error) {
		round.Status, round.Error, round.Duration = status, truncate(msg, maxErrorBytes), time.Since(start)
		if err := uc.rounds.Record(ctx, task, round); err != nil {
			return "", err
		}
		attrs := []any{"rule_id", task.RuleID, "rule_name", rule.Name, "datasource_id", task.DatasourceID, "datasource_name", h.Info.Name, "error", round.Error}
		switch {
		case status != monitorcontract.EvalError:
		case upstream:
			if uc.upstream.remind(task.DatasourceID, time.Now()) {
				uc.log.WarnContext(ctx, "datasource queries are failing: affected rules keep their state and report query errors", attrs...)
			}
		case task.LastStatus != monitorcontract.EvalError:
			uc.log.WarnContext(ctx, "rule evaluation is failing", attrs...)
		}
		return result, nil
	}
	if !rule.Effective.ContainsWithCalendar(now, uc.cache.Calendar()) {
		return skip(monitorcontract.EvalInactive, ResultInactive, "", false)
	}
	if msg := checkQueries(rule); msg != "" {
		return skip(monitorcontract.EvalError, ResultError, msg, false)
	}
	// 流水线配置与 core 保存规则时调的是同一个 Compile：编译不过只可能是脏数据，按规则自身的错误处理。
	pipe, err := monitorpipeline.Compile(rule.Pipeline)
	if err != nil {
		return skip(monitorcontract.EvalError, ResultError, "rule pipeline config is invalid: "+err.Error(), false)
	}
	if msg := h.Unavailable(); msg != "" {
		return skip(monitorcontract.EvalError, ResultError, msg, true)
	}

	// 网络 I/O 全部在事务之前。任一查询出错：整轮记为查询错误，不改任何状态、绝不判恢复。
	q := uc.runQueries(ctx, h, rule)
	if uc.busy.observe(task.DatasourceID, q.busy, time.Now()) && q.busy {
		uc.log.WarnContext(ctx, "datasource query slots are saturated: evaluations are being postponed",
			"datasource_id", task.DatasourceID, "datasource_name", h.Info.Name, "slots", datasourceConcurrency)
	}
	if q.busy {
		// 排不上本实例的并发槽是引擎的自我保护，不是数据源故障：不落查询错误，归还任务稍后重试；积压体现在
		// 评估延迟指标上。但这一轮确实什么都没观察到：在任务行上留下标记，下一轮据此让待触发 / 留观计时从
		// 那一轮重新起算，与查询错误、不在生效时间的轮次同一口径（没观察的时间不算数）。
		task.Postponed = true
		if err := uc.tasks.Release(ctx, task.Token, []*Task{task}, retryDelay); err != nil {
			return "", fmt.Errorf("release postponed task: %w", err)
		}
		return ResultBusy, nil
	}
	round.Detail.Queries = q.details
	if q.err != "" {
		return skip(monitorcontract.EvalError, ResultError, q.err, q.upstream)
	}
	// 结果不完整（截断 / 部分结果）：照常评估见到的序列，没见到的一律按未知处理；原因写进任务与判定记录。
	incomplete := incompleteNote(q.details)
	// 流水线的前半段（改写 → 指纹 → 富化 → 丢弃 → 范围 → 级别抑制）在这里完成；后半段（抑制 → 屏蔽 →
	// 落库 → 通知）在状态机里。两段都是纯计算，事务之外只做前半段。
	cis := uc.cache.CIs()
	in := &RoundInput{
		Rule: rule, DatasourceID: task.DatasourceID, Samples: mergeSamples(rule, task.DatasourceID, q.results, pipe, cis),
		Incomplete: incomplete != "", LastStatus: task.LastStatus, LastEval: task.LastEval, Postponed: task.Postponed,
		Mutes: uc.cache.Mutes(rule, task.DatasourceID, now), Inhibits: uc.inhibits.View(), CIs: cis,
		Escalation: uc.cache.Escalation(rule.EscalationID), Now: now, NewID: uc.newID,
	}
	round.Error = truncate(joinNotes(incomplete, labelLimitNote(in.Samples.Limited), dropErrorNote(in.Samples.DropErrors)), maxErrorBytes)
	// 任务此前处于连续查询错误：这一轮评估成了，随任务写回同事务写一条恢复事件（只在故障事件发过时才真的写入）。
	if !task.ErrorSince.IsZero() {
		if round.EngineEvent, err = ruleRecoveredEvent(uc.newID, task, rule.Name, h.Info.Name, now); err != nil {
			return "", err
		}
	}
	in.Recovery = uc.recoveryValues(ctx, h, task, in)

	// decide 在事务内执行：只做纯计算（Advance），不查询、不渲染模板。
	commit := func() error {
		return uc.rounds.Commit(ctx, task, in.Escalation != nil, func(prev []SeriesState) (*Round, error) {
			in.Prev = prev
			out, err := Advance(in)
			if err != nil {
				return nil, err
			}
			round.Transition = &out.Transition
			round.Status = roundStatus(out.Transition.FiringCount, in.Samples.Series)
			round.Series, round.Anomalies, round.Recovered = in.Samples.Series, out.Anomalies, out.Recovered
			round.Detail.Points, round.Detail.Truncated = out.Points, out.Truncated
			round.Duration = time.Since(start)
			return round, nil
		})
	}
	// 本轮有新事件且规则配置了附加信息时，第一次提交会带着「缺哪些样本的附加信息」回滚（什么都没写）：
	// 模板在事务之外渲染后重新提交。要渲染哪些样本由加锁读到的状态决定；租约围栏保证两次提交之间
	// 状态不会被别人改动，重试次数有界只是防御。
	for range maxCommitAttempts {
		err = commit()
		var missing *MissingAnnotationsError
		if !errors.As(err, &missing) {
			break
		}
		if in.Annotations == nil {
			in.Annotations = make(map[string]map[string]string, len(missing.Samples))
		}
		maps.Copy(in.Annotations, uc.render(rule.Annotations, missing.Samples))
	}
	if err != nil {
		return "", err
	}
	uc.logOutcome(ctx, task, rule, h, round)
	var kinds [monitorcontract.NotifyKindEscalation + 1]int
	for i := range round.Transition.Outbox {
		kinds[round.Transition.Outbox[i].Kind]++
	}
	for kind, n := range kinds {
		if n > 0 {
			uc.obs.AddOutbox(ctx, int32(kind), n) // #nosec G115 -- 下标 0-4
		}
	}
	switch round.Status {
	case monitorcontract.EvalFiring:
		return ResultFiring, nil
	case monitorcontract.EvalNoData:
		return ResultNoData, nil
	}
	return ResultOK, nil
}

// logOutcome 一轮成功评估之后的状态变化日志：从查询错误恢复；结果不完整的开始、持续提醒与结束。
func (uc *EvaluatorUsecase) logOutcome(ctx context.Context, task *Task, rule *Rule, h *DatasourceHandle, round *Round) {
	attrs := []any{"rule_id", task.RuleID, "rule_name", rule.Name, "datasource_id", task.DatasourceID, "datasource_name", h.Info.Name}
	if task.LastStatus == monitorcontract.EvalError {
		uc.log.InfoContext(ctx, "rule evaluation recovered", attrs...)
	}
	incomplete := strings.HasPrefix(round.Error, incompletePrefix)
	if !uc.incomplete.observe(task.RuleID+"/"+task.DatasourceID, incomplete, time.Now()) {
		return
	}
	if incomplete {
		uc.log.WarnContext(ctx, "rule evaluation sees an incomplete result: series not returned keep their state", append(attrs, "reason", round.Error)...)
	} else {
		uc.log.InfoContext(ctx, "rule evaluation sees complete results again", attrs...)
	}
}

// incompleteNote 说明哪些查询的结果不完整；空串 = 全部完整。
func incompleteNote(details []monitorcontract.EvalQueryDetail) string {
	var parts []string
	for i := range details {
		d := &details[i]
		switch {
		case d.Truncated && d.Partial:
			parts = append(parts, "query "+strconv.Itoa(i)+" truncated to "+strconv.Itoa(int(d.Series))+" series and partial")
		case d.Truncated:
			parts = append(parts, "query "+strconv.Itoa(i)+" truncated to "+strconv.Itoa(int(d.Series))+" series")
		case d.Partial:
			parts = append(parts, "query "+strconv.Itoa(i)+" returned a partial result")
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return incompletePrefix + strings.Join(parts, "; ") + "; series not returned were left unchanged"
}

// labelLimitNote 说明有多少条序列的标签超过了上限（已删减 / 截断后照常判定）；0 条时为空串。
func labelLimitNote(series int32) string {
	if series == 0 {
		return ""
	}
	return labelLimitPrefix + strconv.Itoa(int(series)) + " series exceeded the label limits (at most " +
		strconv.Itoa(maxSeriesLabels) + " labels per series, " + strconv.Itoa(maxLabelNameBytes) + " bytes per name, " +
		strconv.Itoa(maxLabelValueBytes) + " bytes per value); extra labels were dropped and long values truncated"
}

// dropErrorNote 说明有多少条序列的条件丢弃表达式渲染失败（这些序列没有被丢弃，照常判定）；0 条时为空串。
func dropErrorNote(series int32) string {
	if series == 0 {
		return ""
	}
	return dropErrorPrefix + "rendering failed for " + strconv.Itoa(int(series)) + " series, they were kept"
}

// joinNotes 用 "; " 连接非空的说明。
func joinNotes(notes ...string) string {
	return strings.Join(slices.DeleteFunc(notes, func(s string) bool { return s == "" }), "; ")
}

// nextInterval 距下一次评估的时长 = 评估周期 + 至多 evalJitter 的随机抖动。没有抖动时，任务到期的相位
// 被锁在上次评估它的实例的轮询节拍之后（循环处理完一批立即再轮询一次，节拍随之对齐），稳态下所有
// 评估都落在同一个实例上；抖动让到期时刻在各实例的轮询节拍之间均匀分布。
func nextInterval(interval time.Duration) time.Duration {
	interval = max(interval, minInterval)
	//nolint:gosec // G404：调度抖动，不需要密码学随机数。
	return interval + rand.N(min(evalJitter, interval/10)+1)
}

// checkQueries 校验规则的查询配置（core 写入时已校验，这里防御脏数据）；返回空串 = 合法。
func checkQueries(rule *Rule) string {
	if len(rule.Queries) == 0 || len(rule.Queries) > monitorcontract.MaxRuleQueries {
		return "rule has no valid queries"
	}
	for i, q := range rule.Queries {
		if !monitorcontract.ValidSeverity(q.Severity) {
			return "rule query " + strconv.Itoa(i) + " has an invalid severity"
		}
	}
	return ""
}

// queryOutcome 一轮判定查询的结果。
type queryOutcome struct {
	// results 各查询的序列（有错误时为 nil）；details 判定记录明细。
	results [][]promclient.Series
	details []monitorcontract.EvalQueryDetail
	// err 首个错误的说明；空串 = 全部成功。
	err string
	// upstream 错误出在数据源一侧（网络、超时、上游故障），而不是规则的表达式。
	upstream bool
	// busy 有查询因排不上本实例的并发槽而根本没有发出。
	busy bool
}

// runQueries 并发执行规则的全部查询。等并发槽与查询各有各的时限：等槽共用一个预算（一个数据源查询
// 超时，从这里起算），超出即按 busy 顺延；每条查询的超时从它拿到槽之后才起算。把两段算在同一个截止
// 时刻里，晚拿到槽的查询会被等槽的时间挤成一次上游超时——健康但慢的数据源在槽位排满时就会被记成查询
// 错误，待触发 / 留观计时随之被重置。
func (uc *EvaluatorUsecase) runQueries(ctx context.Context, h *DatasourceHandle, rule *Rule) queryOutcome {
	n := len(rule.Queries)
	out := queryOutcome{results: make([][]promclient.Series, n), details: make([]monitorcontract.EvalQueryDetail, n)}
	unsent, upstream := make([]bool, n), make([]bool, n)
	slotBy := time.Now().Add(h.Timeout())
	run := func(i int) {
		q := rule.Queries[i]
		d := &out.details[i]
		*d = monitorcontract.EvalQueryDetail{Index: int32(i), Severity: q.Severity} // #nosec G115 -- 至多 MaxRuleQueries
		res, err := h.Query(ctx, slotBy, q.Expr)
		var perr *promclient.Error
		switch {
		case errors.As(err, &perr):
			upstream[i] = !perr.BadQuery()
			d.Error = truncate(perr.Error(), maxErrorBytes)
		case err != nil:
			// promclient 的失败恒为 *promclient.Error；其余只可能是没排上并发槽（errSlotBusy）。
			unsent[i] = true
			d.Error = err.Error()
		case res.Type != promclient.ResultVector:
			// 标量（如 time()）恒有值，会无条件触发；与区间向量、字符串一样不是可判定的结果。
			d.Error = "query must return an instant vector, got a " + resultTypeName(res.Type)
		default:
			out.results[i] = res.Series
			d.Series = int32(len(res.Series)) // #nosec G115 -- 受 max_series 约束
			d.Truncated, d.Partial = res.Truncated, res.Partial
		}
	}
	if n == 1 {
		run(0)
	} else {
		fanOut(n, run)
	}
	for i := range out.details {
		if out.details[i].Error != "" {
			out.results = nil
			out.err = "query " + strconv.Itoa(i) + ": " + out.details[i].Error
			out.upstream, out.busy = slices.Contains(upstream, true), slices.Contains(unsent, true)
			break
		}
	}
	return out
}

// resultTypeName 结果类型在错误信息里的叫法。
func resultTypeName(typ string) string {
	if typ == promclient.ResultMatrix {
		return "range vector"
	}
	return typ
}

// recoveryValues 为本轮将判恢复的事件执行恢复取值查询（事务之前）。任何失败都只是没有恢复值，
// 不阻塞恢复：预判用的是无锁快照，租约期内只有本实例会改这些状态行，事务内再以加锁读到的
// 状态重新计算，预判与实际不一致的事件同样只是没有恢复值。
func (uc *EvaluatorUsecase) recoveryValues(ctx context.Context, h *DatasourceHandle, task *Task, in *RoundInput) map[string]float64 {
	// 结果不完整的一轮不会判恢复。
	if in.Rule.RecoveryExpr == "" || in.Incomplete {
		return nil
	}
	tpl, err := monitorpipeline.CompileTemplate(in.Rule.RecoveryExpr)
	if err != nil {
		return nil
	}
	prev, err := uc.rounds.Snapshot(ctx, task)
	if err != nil {
		uc.log.WarnContext(ctx, "snapshot eval states for recovery values failed", "rule_id", task.RuleID, "error", err)
		return nil
	}
	due := resolving(in, prev)
	if len(due) == 0 {
		return nil
	}
	due = due[:min(len(due), maxRecoveryQueries)]
	values, ok := make([]float64, len(due)), make([]bool, len(due))
	// 恢复取值整体（等槽 + 查询）不超过一个查询超时：没赶上的事件只是没有恢复值。
	qctx, cancel := context.WithTimeout(ctx, h.Timeout()+time.Second)
	defer cancel()
	var g errgroup.Group
	g.SetLimit(recoveryConcurrency)
	for i, st := range due {
		g.Go(func() error {
			expr, rendered := renderRecoveryExpr(tpl, decodeLabels(st.Labels), st.Value)
			if !rendered {
				return nil
			}
			res, err := h.Query(qctx, time.Time{}, expr)
			if err != nil || len(res.Series) == 0 || len(res.Series[0].Points) == 0 {
				return nil
			}
			points := res.Series[0].Points
			values[i], ok[i] = points[len(points)-1].Value, true
			return nil
		})
	}
	_ = g.Wait()
	out := make(map[string]float64, len(due))
	for i, st := range due {
		if ok[i] {
			out[st.Fingerprint] = values[i]
		}
	}
	return out
}
