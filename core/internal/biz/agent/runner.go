package biz

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	bizassistant "github.com/lanyulei/reborn/backend/core/internal/biz/assistant"
	"github.com/lanyulei/reborn/backend/kit/idgen"
)

// 运行器参数。
const (
	// claimLeaseGrace 任务领取租约在任务时限之外多留的时间（收尾写入仍在租约内）。
	claimLeaseGrace = 2 * time.Minute
	// leaseGraceS 故障 Agent 租约在任务时限之外多留的秒数（与令牌一致，设计 §5.5）。
	leaseGraceS = 120
	// tokenGrace 机器令牌在任务时限之外多留的时间。
	tokenGrace = 2 * time.Minute
	// respondedSkipAfter 故障已有人响应且超过此时长：首轮分析价值不大，自动任务跳过（人工召唤不受限）。
	respondedSkipAfter = 15 * time.Minute
	// llmPauseAfter / llmPause 模型侧降级：连续这么多次模型不可用 → 运行器暂停这么久（任务留在队列）。
	llmPauseAfter = 5
	llmPause      = 5 * time.Minute
	// denialBreakerAfter 连续这么多条任务遇到授权拒绝（权限被收回）→ 打开 Profile 熔断并通知管理员。
	denialBreakerAfter = 3
	// metricWindow 指标查询允许的时间窗：故障发生前后各这么久。
	metricWindow = 30 * time.Minute
	// minStageTime 进入一个阶段至少要剩这么多时间，否则跳过（结论阶段除外：至少 minConcludeTime）。
	minStageTime    = 45 * time.Second
	minConcludeTime = 20 * time.Second
	// maxSeenIDs 一次运行里记住的对象 ID 上限（写工具参数校验用）。
	maxSeenIDs = 5000
	// 每次运行里写工具的次数上限。
	maxComments  = 2
	maxProposals = 2
	maxAttach    = 20
	// breakerWindow / breakerMinSamples / breakerFailurePct Profile 级熔断：24 小时内自动执行的建议验证失败率 > 30%（样本 ≥ 5）。
	breakerWindow     = 24 * time.Hour
	breakerMinSamples = 5
	breakerFailurePct = 30
	// housekeepingBatch 每轮回收 / 过期的条数上限。
	housekeepingBatch = 100
	// purgeBatch 每轮清理的运行条数上限。
	purgeBatch = 200
	// logTailLines 日志类工具输出只保留尾部这么多行（设计 §5.3）。
	logTailLines = 200
	// logTailMinBytes 输出超过这么多字节才截尾。
	logTailMinBytes = 4 << 10
	// agentPriorNote 时间线里 Agent 先前产出的标记（设计 §5.3 自身产出打标）。
	agentPriorNote = "[Agent 先前结论，待验证]"
	// retryReason 任务放回队列时运行记录上的结束原因（恢复时据此续跑）。
	retryReason = "retrying"
	// maxDocContent 经验沉淀草稿正文上限（知识库接口上限 512 KiB，这里收紧）。
	maxDocContent = 60000
	// maxCommentText 评论正文上限（与接口校验一致）。
	maxCommentText = 8000
	// transformMaxBytes 超过这个大小的工具输出不做整形（交给通用截断），避免大日志的解码 / 编码开销。
	transformMaxBytes = 1 << 20
	// finalizeTimeout 任务收尾写入（运行 / 任务终态）的时限；收尾不受运行 ctx 取消影响。
	finalizeTimeout = 20 * time.Second
	// 对象 ID 的来源分类（写工具参数只认对应来源里出现过的 ID，设计 §5.2）。
	seenChange   = "change"
	seenAlert    = "alert"
	seenPlaybook = "playbook"
)

// uuidPattern 工具输出里的对象 ID。
var uuidPattern = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)

// RunnerUsecase 运行器：领取任务 → 选 Profile → 过滤与配额 → 取令牌与租约 → 分阶段无人值守运行 → 回写。
type RunnerUsecase struct {
	d   *Deps
	now func() time.Time

	mu          sync.Mutex
	inflight    int
	llmFailures int
	pausedUntil time.Time
	denials     map[string]int
	// tasks 在途任务 goroutine；停机时 Wait 等它们收尾（写回与释放租约）。
	tasks sync.WaitGroup
}

// Wait 等待在途任务全部收尾，或 ctx 到期。
func (uc *RunnerUsecase) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		uc.tasks.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// NewRunnerUsecase new a RunnerUsecase.
func NewRunnerUsecase(d *Deps) *RunnerUsecase {
	return &RunnerUsecase{d: d, now: time.Now, denials: make(map[string]int)}
}

// Run 一轮：回收过期租约与排队超时 → 按空余槽位领取 → 并行跑完（本轮阻塞到全部结束）。返回处理条数。
func (uc *RunnerUsecase) Run(ctx context.Context) (int, error) {
	if !uc.d.Config.Enabled {
		return 0, nil
	}
	now := uc.now()
	if n, err := uc.d.Tasks.ReleaseStale(ctx, now, housekeepingBatch); err != nil {
		return 0, err
	} else if n > 0 {
		uc.d.Logger.WarnContext(ctx, "agent: released stale running tasks", "count", n)
	}
	if n, err := uc.d.Tasks.ExpireQueued(ctx, now.Add(-uc.d.Config.QueueTTL), housekeepingBatch); err != nil {
		return 0, err
	} else if n > 0 {
		uc.d.Logger.InfoContext(ctx, "agent: expired queued tasks", "count", n)
	}
	uc.mu.Lock()
	paused := now.Before(uc.pausedUntil)
	slots := uc.d.Config.MaxConcurrentTasks - uc.inflight
	uc.mu.Unlock()
	if paused || slots <= 0 {
		return 0, nil
	}
	tasks, err := uc.d.Tasks.Claim(ctx, slots, uc.d.Config.TaskTimeout+claimLeaseGrace)
	if err != nil {
		return 0, err
	}
	if len(tasks) == 0 {
		return 0, nil
	}
	// 领取后立即返回：任务在各自的 goroutine 里跑完，槽位由 inflight 控制；一条慢任务不再阻塞本实例的领取与回收。
	for _, t := range tasks {
		uc.mu.Lock()
		uc.inflight++
		uc.mu.Unlock()
		uc.tasks.Add(1)
		go func(t *Task) {
			defer uc.tasks.Done()
			defer func() {
				uc.mu.Lock()
				uc.inflight--
				uc.mu.Unlock()
			}()
			defer func() {
				if r := recover(); r != nil {
					uc.d.Logger.ErrorContext(ctx, "agent: task panicked", "task", t.ID, "panic", r)
					fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finalizeTimeout)
					defer cancel()
					uc.retryOrFail(fctx, t, fmt.Sprintf("运行器异常：%v", r))
				}
			}()
			uc.runTask(ctx, t)
		}(t)
	}
	return len(tasks), nil
}

// Purge 保留期清理：删除结束时间早于保留期的运行及其助手会话（物理删除）、终态任务。
func (uc *RunnerUsecase) Purge(ctx context.Context) (int, error) {
	before := uc.now().Add(-uc.d.Config.Retention)
	convs, err := uc.d.Runs.PurgeBefore(ctx, before, purgeBatch)
	if err != nil {
		return 0, err
	}
	if len(convs) > 0 {
		if err := uc.d.Conversations.Purge(ctx, convs); err != nil {
			return len(convs), err
		}
	}
	n, err := uc.d.Tasks.Purge(ctx, before, purgeBatch)
	return len(convs) + n, err
}

// runState 一次运行里裁决 / 观察 / 改写回调要用的可变状态。
type runState struct {
	mu            sync.Mutex
	run           *Run
	profile       *Profile
	inc           *IncidentSnapshot
	dryRun        bool
	seen          map[string]struct{}
	seenBy        map[string]map[string]struct{}
	envs          map[string]struct{}
	resumed       bool
	metricEvid    bool
	finding       *Finding
	findingDone   bool
	comments      int
	proposals     int
	documents     int
	leaseLost     bool
	authDenied    bool
	deniedCalls   []DeniedCall
	captured      []CapturedCall
	proposalsRefs []Ref
}

// runTask 跑一条已领取的任务；所有退出路径都写任务终态。任务已绑定运行（重试）时在原运行与会话上续跑。
func (uc *RunnerUsecase) runTask(ctx context.Context, t *Task) {
	log := uc.d.Logger.With("task", t.ID, "incident", t.IncidentID, "attempt", t.Attempts, "kind", t.Kind)
	// finish 写任务终态；收尾不受运行 ctx 取消影响。任务已绑定运行（续跑）且运行仍为 running 时一并收口。
	finish := func(status, result string) {
		fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finalizeTimeout)
		defer cancel()
		if t.RunID != "" {
			uc.closeRun(fctx, t.RunID, status)
		}
		ok, err := uc.d.Tasks.Finish(fctx, t.ID, t.LockToken, status, clip(result, 255))
		if err != nil {
			log.ErrorContext(fctx, "agent: finish task", "error", err)
			return
		}
		if !ok {
			// 持有权已丢失（被取消 / 被回收）：不再记指标。
			log.WarnContext(fctx, "agent: task no longer owned at finish", "status", status)
			return
		}
		uc.metrics().TaskFinished(fctx, status)
	}
	inc, err := uc.d.Incidents.Snapshot(ctx, t.IncidentID)
	if err != nil {
		finish(TaskFailed, "故障不存在或读取失败："+err.Error())
		return
	}
	kind := t.Kind
	if kind == "" {
		kind = KindDiagnose
	}
	if kind == KindDiagnose && !t.DryRun && !inc.Active {
		finish(TaskCancelled, "故障已结束")
		return
	}
	// 收尾写入全部走 fctx（不随运行 ctx 取消）；时限在真正收尾时按 finalizeTimeout 另加。
	fctx := context.WithoutCancel(ctx)
	profile, skip, err := uc.pickProfile(ctx, t, kind)
	if err != nil {
		uc.retryOrFail(ctx, t, "选择 Profile 失败："+err.Error())
		return
	}
	if profile == nil {
		finish(TaskSkipped, skip)
		return
	}
	now := uc.now()
	if !t.DryRun && t.RunID == "" {
		if t.Source == SourceIncident && kind == KindDiagnose {
			if inc.RespondTime != nil && now.Sub(*inc.RespondTime) > respondedSkipAfter {
				finish(TaskSkipped, "已有人响应超过 15 分钟，不再自动分析")
				return
			}
		}
		if t.Source == SourceIncident || t.Source == SourcePostmortem {
			hour, err := uc.d.Budgets.Get(ctx, profile.ID, hourKey(now))
			if err != nil {
				uc.retryOrFail(ctx, t, "读取配额失败："+err.Error())
				return
			}
			if hour.Tasks >= profile.Spec.HourlyLimit {
				finish(TaskSkipped, fmt.Sprintf("超过 Profile 小时配额（%d）", profile.Spec.HourlyLimit))
				return
			}
		}
		day, err := uc.d.Budgets.Total(ctx, dayKey(now))
		if err != nil {
			uc.retryOrFail(ctx, t, "读取预算失败："+err.Error())
			return
		}
		if day.PromptTokens >= uc.d.Config.DailyPromptBudget || day.CompletionTokens >= uc.d.Config.DailyCompletion {
			finish(TaskExpired, "日 token 预算已耗尽")
			uc.notify(ctx, "agent.budget", "运维 Agent 日预算已耗尽", "今日 token 预算已用完，新任务直接过期；可在配置里调整 agent.daily_*_token_budget。", "budget:"+dayKey(now))
			return
		}
	}
	machine, err := uc.d.Accounts.IsMachine(ctx, profile.Spec.AgentUserID)
	if err != nil {
		uc.retryOrFail(ctx, t, "读取机器账号失败："+err.Error())
		return
	}
	if !machine {
		finish(TaskFailed, "Profile 绑定的机器账号不可用（不存在、已停用或不是机器账号）")
		uc.notify(ctx, "agent.account", "运维 Agent 机器账号不可用", "Profile「"+profile.Spec.Name+"」绑定的机器账号不可用，任务无法运行。", "account:"+profile.ID)
		return
	}
	timeout := time.Duration(profile.Spec.Budget.TimeoutS) * time.Second
	if timeout <= 0 {
		timeout = uc.d.Config.TaskTimeout
	}
	token, _, err := uc.d.Accounts.IssueToken(ctx, profile.Spec.AgentUserID, timeout+tokenGrace)
	if err != nil {
		finish(TaskFailed, "签发机器令牌失败："+err.Error())
		return
	}
	// 租约：诊断任务对活跃故障必取；经验沉淀不写故障，不取租约；干跑的历史故障多已关闭，取不到就全程捕获。
	leaseHeld := false
	if kind == KindDiagnose && inc.Active {
		res, err := uc.loopback(ctx, toolAcquireLease, map[string]any{"id": inc.ID, "ttlS": int(timeout.Seconds()) + leaseGraceS}, token)
		switch {
		case err != nil:
			uc.retryOrFail(ctx, t, "获取 Agent 租约失败："+err.Error())
			return
		case res.StatusCode >= 200 && res.StatusCode < 300:
			leaseHeld = true
		case res.StatusCode == 409 || strings.HasPrefix(res.Reason, "INCIDENT_"):
			finish(TaskSkipped, "Agent 租约被占用："+res.Message)
			return
		case res.StatusCode == 401 || res.StatusCode == 403:
			finish(TaskFailed, "机器账号无权获取租约（"+res.Reason+"）")
			uc.countDenial(ctx, profile, true)
			return
		default:
			uc.retryOrFail(ctx, t, fmt.Sprintf("获取 Agent 租约失败（HTTP %d %s）", res.StatusCode, res.Reason))
			return
		}
	}
	if leaseHeld {
		defer func() {
			rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			if _, err := uc.loopback(rctx, toolReleaseLease, map[string]any{"id": inc.ID}, token); err != nil {
				log.WarnContext(rctx, "agent: release lease", "error", err)
			}
		}()
	}
	// 运行：重试时复用任务上已绑定的运行与会话（历史延续，已完成的阶段不重做）。
	run, resumed, err := uc.openRun(ctx, t, profile, inc, kind, now)
	if err != nil {
		uc.retryOrFail(ctx, t, "创建运行失败："+err.Error())
		return
	}
	if !resumed {
		if err := uc.d.Tasks.BindRun(ctx, t.ID, t.LockToken, run.ID, profile.ID); err != nil {
			log.ErrorContext(ctx, "agent: bind run", "error", err)
		}
		// 内存里的任务也要带上运行 ID：之后的 finish / retryOrFail 据此收口运行。
		t.RunID = run.ID
		if !t.DryRun {
			_ = uc.d.Budgets.Add(ctx, profile.ID, hourKey(now), 1, 0, 0)
			_ = uc.d.Budgets.Add(ctx, profile.ID, dayKey(now), 1, 0, 0)
		}
	}
	st := &runState{run: run, profile: profile, inc: inc, dryRun: t.DryRun, resumed: resumed, seen: map[string]struct{}{inc.ID: {}}, seenBy: map[string]map[string]struct{}{}, envs: map[string]struct{}{}}
	if resumed {
		uc.rebuildState(ctx, st)
	}
	startTokens := [2]int64{run.PromptTokens, run.CompletionTokens}
	outcome := uc.runStages(ctx, t, st, token, timeout, log)
	run.EndTime = ptr(uc.now())
	run.Finding, run.Proposals, run.Denied, run.Captured = st.finding, st.proposalsRefs, st.deniedCalls, st.captured
	if run.Proposals == nil {
		run.Proposals = []Ref{}
	}
	if run.Denied == nil {
		run.Denied = []DeniedCall{}
	}
	if run.Captured == nil {
		run.Captured = []CapturedCall{}
	}
	switch outcome.kind {
	case outcomeDone:
		if st.findingDone {
			run.Status, run.EndReason = RunSucceeded, "completed"
		} else {
			run.Status, run.EndReason = RunNoFinding, "no_finding"
		}
	case outcomeCancelled:
		run.Status, run.EndReason = RunCancelled, outcome.reason
	case outcomeRetry:
		// 放回队列：运行保持 running，记下原因；重新领取时续跑未完成的阶段。
		run.Status, run.EndReason, run.EndTime = RunRunning, retryReason, nil
	default:
		run.Status, run.EndReason = RunFailed, outcome.reason
	}
	sctx, scancel := context.WithTimeout(fctx, finalizeTimeout)
	defer scancel()
	// 只在运行仍为 running 时写回：运行中途被取消（Cancel 已置 cancelled）时不覆盖终态。
	owned, err := uc.d.Runs.SaveRunning(sctx, run)
	if err != nil {
		log.ErrorContext(sctx, "agent: save run", "error", err)
	} else if !owned {
		log.WarnContext(sctx, "agent: run already closed elsewhere, result discarded", "run", run.ID)
	}
	if !t.DryRun {
		// 干跑不计配额（含 token 预算）。
		_ = uc.d.Budgets.Add(sctx, profile.ID, dayKey(now), 0, run.PromptTokens-startTokens[0], run.CompletionTokens-startTokens[1])
	}
	uc.metrics().Tokens(sctx, run.PromptTokens-startTokens[0], run.CompletionTokens-startTokens[1])
	switch outcome.kind {
	case outcomeDone:
		if st.findingDone {
			finish(TaskSucceeded, "")
		} else {
			finish(TaskNoFinding, "Agent 已分析，无明确结论")
		}
	case outcomeCancelled:
		finish(TaskCancelled, outcome.message)
	case outcomeRetry:
		uc.retryOrFail(sctx, t, outcome.message)
	default:
		finish(TaskFailed, outcome.message)
	}
	uc.countDenial(sctx, profile, st.authDenied)
	uc.checkBreaker(sctx, profile)
	if run.EndTime != nil {
		log.InfoContext(ctx, "agent: task finished", "run", run.ID, "status", run.Status, "reason", run.EndReason,
			"prompt_tokens", run.PromptTokens, "completion_tokens", run.CompletionTokens, "duration_ms", run.EndTime.Sub(run.StartTime).Milliseconds())
	}
}

// openRun 取得本次运行：任务已绑定运行且运行仍为 running（上次放回队列）→ 复用；否则新建运行与 kind=agent 的会话。
func (uc *RunnerUsecase) openRun(ctx context.Context, t *Task, profile *Profile, inc *IncidentSnapshot, kind string, now time.Time) (*Run, bool, error) {
	if t.RunID != "" {
		run, err := uc.d.Runs.Get(ctx, t.RunID)
		if err == nil && run.Status == RunRunning && run.TaskID == t.ID && run.ProfileID == profile.ID {
			run.EndReason = ""
			return run, true, nil
		}
	}
	convID, err := uc.d.Conversations.Create(ctx, profile.Spec.AgentUserID, "Agent · "+inc.Number)
	if err != nil {
		return nil, false, err
	}
	runID, err := idgen.NewID()
	if err != nil {
		return nil, false, err
	}
	run := &Run{
		ID: runID, TaskID: t.ID, ProfileID: profile.ID, IncidentID: inc.ID, AgentUserID: profile.Spec.AgentUserID, ConversationID: convID,
		Status: RunRunning, DryRun: t.DryRun, StartTime: now, CreateTime: now, Proposals: []Ref{}, Denied: []DeniedCall{}, Captured: []CapturedCall{},
	}
	for _, key := range StageOrderFor(kind) {
		run.Stages = append(run.Stages, StageTrace{Key: key, Status: StagePending})
	}
	if err := uc.d.Runs.Create(ctx, run); err != nil {
		return nil, false, err
	}
	return run, false, nil
}

// closeRun 任务在运行之外的路径上到达终态时，把仍为 running 的运行一并收口（续跑早退：Profile 停用、账号不可用、租约被占等）。
func (uc *RunnerUsecase) closeRun(ctx context.Context, runID, taskStatus string) {
	status, reason := RunFailed, "error"
	switch taskStatus {
	case TaskCancelled, TaskSkipped, TaskExpired:
		status, reason = RunCancelled, "cancelled"
	case TaskSucceeded:
		status, reason = RunSucceeded, "completed"
	case TaskNoFinding:
		status, reason = RunNoFinding, "no_finding"
	}
	if _, err := uc.d.Runs.Close(ctx, runID, status, reason, uc.now()); err != nil {
		uc.d.Logger.ErrorContext(ctx, "agent: close run", "run", runID, "error", err)
	}
}

// rebuildState 续跑前从会话转录里恢复「出现过的对象 ID」与证据标记（写工具参数校验要用），并接上已有产出与写入计数。
func (uc *RunnerUsecase) rebuildState(ctx context.Context, st *runState) {
	_, calls, err := uc.d.Conversations.Transcript(ctx, st.run.ConversationID)
	if err != nil {
		uc.d.Logger.WarnContext(ctx, "agent: rebuild state from transcript", "run", st.run.ID, "error", err)
	}
	for _, c := range calls {
		if c.Status != bizassistant.ToolStatusSucceeded && c.Status != bizassistant.ToolStatusCaptured {
			continue
		}
		// 写入计数也要接上：续跑不能再多发评论 / 再建一篇草稿。
		switch c.ToolName {
		case "IncidentService_AddComment":
			st.comments++
		case toolCreateDocument:
			st.documents++
		}
		if c.Status != bizassistant.ToolStatusSucceeded || len(c.Output) == 0 {
			continue
		}
		st.observe(c, c.Output)
	}
	st.findingDone = st.run.FindingEventID != ""
	st.finding = st.run.Finding
	// 运行记录上已有的产出与转录里恢复出来的合并（按 ID 去重：上次放回队列前已同步过一次）。
	for _, ref := range st.run.Proposals {
		if !slices.ContainsFunc(st.proposalsRefs, func(r Ref) bool { return r.ID == ref.ID }) {
			st.proposalsRefs = append(st.proposalsRefs, ref)
		}
	}
	if len(st.deniedCalls) == 0 {
		st.deniedCalls = append(st.deniedCalls, st.run.Denied...)
	}
	if len(st.captured) == 0 {
		st.captured = append(st.captured, st.run.Captured...)
	}
	st.proposals = len(st.proposalsRefs)
}

// syncRun 把回调状态里的产出同步到运行记录（每次落库前调用，崩溃后续跑能接上）。
func (st *runState) syncRun() {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.run.Proposals = append([]Ref{}, st.proposalsRefs...)
	st.run.Denied = append([]DeniedCall{}, st.deniedCalls...)
	st.run.Captured = append([]CapturedCall{}, st.captured...)
	if st.finding != nil {
		st.run.Finding = st.finding
	}
}

// 运行结果种类。
const (
	outcomeDone      = "done"
	outcomeFailed    = "failed"
	outcomeRetry     = "retry"
	outcomeCancelled = "cancelled"
)

// outcome 分阶段运行的整体结果。
type outcome struct {
	kind    string
	reason  string
	message string
}

// stageLogger 运行器要用的日志面。
type stageLogger interface {
	WarnContext(context.Context, string, ...any)
	InfoContext(context.Context, string, ...any)
}

// runStages 按阶段顺序跑完（已完成的阶段跳过；预算 / 时间不够时跳到结论），返回整体结果。
func (uc *RunnerUsecase) runStages(ctx context.Context, t *Task, st *runState, token string, timeout time.Duration, log stageLogger) outcome {
	run, profile := st.run, st.profile
	deadline := uc.now().Add(timeout)
	prev, prevMsg := "", ""
	totalSteps := 0
	resumeNote := ""
	for i := range run.Stages {
		trace := &run.Stages[i]
		if trace.Status == StageDone {
			// 续跑：沿用已完成阶段的结论与消息边界。
			totalSteps += int(trace.Steps)
			if trace.Summary != "" {
				prev = trace.Summary
			}
			prevMsg = trace.LastMessageID
			continue
		}
		if st.resumed && trace.Steps > 0 {
			// 上次尝试在本阶段中断：已花掉的步数计入预算，并提示模型这是续跑。
			totalSteps += int(trace.Steps)
			resumeNote = "（续跑：上次尝试在本阶段中断，会话里可能有未完成的工具调用，请从头完成本阶段任务。）\n"
		}
		key := trace.Key
		stage := profile.Spec.Stage(key)
		if stage == nil {
			trace.Status, trace.Summary = StageSkipped, "Profile 未配置本阶段"
			continue
		}
		now := uc.now()
		remaining := deadline.Sub(now)
		isConclude := key == StageConclude
		over := totalSteps >= int(profile.Spec.Budget.MaxSteps) || run.PromptTokens >= profile.Spec.Budget.PromptTokens || run.CompletionTokens >= profile.Spec.Budget.CompletionTokens
		if (!isConclude && (remaining < minStageTime || over)) || (isConclude && remaining < minConcludeTime) {
			trace.Status, trace.Summary = StageSkipped, "预算或时间不足，跳过"
			if isConclude {
				return outcome{kind: outcomeFailed, reason: bizassistant.HeadlessTimeout, message: "任务时限已到，未能进入结论阶段"}
			}
			prev = "（上一阶段因预算或时间不足被跳过）"
			continue
		}
		if ok, err := uc.d.Tasks.Renew(ctx, t.ID, t.LockToken, remaining+claimLeaseGrace); err != nil || !ok {
			return outcome{kind: outcomeCancelled, reason: "cancelled", message: "任务已被取消"}
		}
		maxSteps := int(stage.MaxSteps)
		if left := int(profile.Spec.Budget.MaxSteps) - totalSteps; left < maxSteps {
			maxSteps = left
		}
		if maxSteps <= 0 {
			maxSteps = 1
		}
		input := stageInput(profile.Spec.Kind, key, prev, "")
		if key == StageCollect {
			input = stageInput(profile.Spec.Kind, key, prev, t.Input)
		}
		input, resumeNote = resumeNote+input, ""
		system := stageSystem(profile.Spec.Kind, key, st.inc)
		trace.Status = StageRunning
		started := uc.now()
		res, err := uc.d.Headless.RunHeadless(ctx, bizassistant.HeadlessSpec{
			ConversationID: run.ConversationID, UserID: profile.Spec.AgentUserID, Bearer: "Bearer " + token,
			System: system, Tools: stage.Tools, Input: input, HistoryAfter: prevMsg,
			Decide: st.decide, Observe: st.observe, Transform: st.transform, MaxSteps: maxSteps, Timeout: remaining,
		})
		trace.DurationMS = uc.now().Sub(started).Milliseconds()
		stageDur := time.Duration(trace.DurationMS) * time.Millisecond
		if err != nil {
			trace.Status, trace.EndReason = StageFailed, "error"
			uc.metrics().StageFinished(ctx, key, StageFailed, stageDur)
			if ctx.Err() != nil {
				// 运行器停机：不算模型失败，放回队列续跑。
				return outcome{kind: outcomeRetry, reason: "shutdown", message: "运行器停机，稍后续跑"}
			}
			uc.noteLLM(false)
			if errors.Is(err, bizassistant.ErrRunBusy) {
				return outcome{kind: outcomeRetry, reason: "busy", message: "会话被占用，稍后重试"}
			}
			if errors.Is(err, bizassistant.ErrDisabled) {
				return outcome{kind: outcomeFailed, reason: "disabled", message: "助手模型未配置"}
			}
			if isConclude {
				return outcome{kind: outcomeFailed, reason: "error", message: "结论阶段出错，不再重试：" + err.Error()}
			}
			return outcome{kind: outcomeRetry, reason: "error", message: "运行出错：" + err.Error()}
		}
		uc.absorb(st, trace, res)
		totalSteps += res.Steps
		prevMsg = res.LastMessageID
		switch res.EndReason {
		case bizassistant.HeadlessLLMRejected:
			trace.Status = StageFailed
			uc.noteLLM(true)
			uc.metrics().ModelFailure(ctx, "rejected")
			uc.metrics().StageFinished(ctx, key, StageFailed, stageDur)
			return outcome{kind: outcomeFailed, reason: res.EndReason, message: "模型拒绝了请求（上下文过长等），不重试"}
		case bizassistant.HeadlessSessionExpired:
			trace.Status = StageFailed
			uc.metrics().StageFinished(ctx, key, StageFailed, stageDur)
			return outcome{kind: outcomeCancelled, reason: res.EndReason, message: "机器账号令牌已失效（账号可能已停用）"}
		case bizassistant.HeadlessLLMUnavailable:
			trace.Status = StageFailed
			uc.noteLLM(false)
			uc.metrics().ModelFailure(ctx, "unavailable")
			uc.metrics().StageFinished(ctx, key, StageFailed, stageDur)
			if isConclude {
				return outcome{kind: outcomeFailed, reason: res.EndReason, message: "模型不可用，结论阶段不重试"}
			}
			return outcome{kind: outcomeRetry, reason: res.EndReason, message: "模型不可用，稍后重试"}
		case bizassistant.HeadlessTimeout:
			uc.metrics().StageFinished(ctx, key, StageFailed, stageDur)
			if isConclude {
				trace.Status, trace.Summary = StageFailed, "本阶段超时"
				return outcome{kind: outcomeFailed, reason: res.EndReason, message: "结论阶段超时"}
			}
			// 非结论阶段超时：带着已掌握的信息继续；阶段记为完成（续跑时不再重做）。
			trace.Status = StageDone
			trace.Summary = clip("（本阶段超时，以下为已掌握的信息）\n"+strings.TrimSpace(res.Content), maxSummaryRunes)
			trace.LastMessageID = prevMsg
			prev = trace.Summary
			if oc, ok := uc.checkpoint(ctx, t, st, deadline, log); !ok {
				return oc
			}
			continue
		}
		uc.noteLLM(true)
		if st.leaseLost {
			trace.Status = StageDone
			uc.metrics().StageFinished(ctx, key, StageDone, stageDur)
			return outcome{kind: outcomeCancelled, reason: "lease_released", message: "Agent 租约已被释放（指挥停止了 Agent）"}
		}
		if st.authDenied && !st.dryRun {
			// 权限被收回（授权层 403）：立即结束，不再逐个阶段撞墙；干跑继续跑完以汇总「权限与范围检查」。
			trace.Status = StageDone
			uc.metrics().StageFinished(ctx, key, StageDone, stageDur)
			return outcome{kind: outcomeCancelled, reason: "permission_denied", message: "机器账号权限被拒绝（授权层 403），任务结束"}
		}
		content := strings.TrimSpace(res.Content)
		// 结构化输出解析失败：再给模型一次只输出 JSON 的机会（一步、不带工具）。
		if content != "" && looseJSON(content) == nil && totalSteps < int(profile.Spec.Budget.MaxSteps) && deadline.Sub(uc.now()) > minConcludeTime {
			fix, ferr := uc.d.Headless.RunHeadless(ctx, bizassistant.HeadlessSpec{
				ConversationID: run.ConversationID, UserID: profile.Spec.AgentUserID, Bearer: "Bearer " + token,
				System: system, Input: reformatPrompt, HistoryAfter: prevMsg,
				Decide: st.decide, Observe: st.observe, Transform: st.transform, MaxSteps: 1, Timeout: deadline.Sub(uc.now()),
			})
			if ferr == nil {
				uc.absorb(st, trace, fix)
				totalSteps += fix.Steps
				prevMsg = fix.LastMessageID
				if looseJSON(fix.Content) != nil {
					content = strings.TrimSpace(fix.Content)
				}
			}
		}
		trace.Status = StageDone
		uc.metrics().StageFinished(ctx, key, StageDone, stageDur)
		summary := content
		if summary == "" {
			summary = "（本阶段未给出结构化结论）"
		}
		if j := looseJSON(summary); j != nil {
			summary = string(j)
		}
		trace.Summary = clip(summary, maxSummaryRunes)
		trace.LastMessageID = prevMsg
		prev = trace.Summary
		if isConclude && st.finding == nil {
			st.finding = parseFinding(looseJSON(content))
		}
		if isConclude && !st.findingDone && st.finding != nil {
			// 模型只在结论里给出了发现却没有调用写工具：由运行器代为写入（干跑则记为捕获），避免整轮分析无产出。
			uc.writeFinding(ctx, st, token)
		}
		// 阶段结束即落一次运行记录：续跑时能看到已完成的阶段。运行已被外部取消或任务已易主时不写回。
		if oc, ok := uc.checkpoint(ctx, t, st, deadline, log); !ok {
			return oc
		}
	}
	return outcome{kind: outcomeDone}
}

// checkpoint 阶段结束时落运行记录：先确认任务租约仍归自己（失联被回收后别的实例可能已接手同一运行），再按
// 「运行仍为 running」写回。返回 ok=false 时携带应返回的结果。
func (uc *RunnerUsecase) checkpoint(ctx context.Context, t *Task, st *runState, deadline time.Time, log stageLogger) (outcome, bool) {
	st.syncRun()
	if ok, err := uc.d.Tasks.Renew(ctx, t.ID, t.LockToken, deadline.Sub(uc.now())+claimLeaseGrace); err != nil || !ok {
		return outcome{kind: outcomeCancelled, reason: "cancelled", message: "任务已被取消或已易主"}, false
	}
	owned, err := uc.d.Runs.SaveRunning(ctx, st.run)
	if err != nil {
		log.WarnContext(ctx, "agent: save run after stage", "run", st.run.ID, "error", err)
		return outcome{}, true
	}
	if !owned {
		return outcome{kind: outcomeCancelled, reason: "cancelled", message: "任务已被取消"}, false
	}
	return outcome{}, true
}

// writeFinding 运行器代模型写入发现（结论阶段给出了结构化发现但没调用 IncidentService_AddFinding 时）。
func (uc *RunnerUsecase) writeFinding(ctx context.Context, st *runState, token string) {
	st.mu.Lock()
	f := st.finding
	st.mu.Unlock()
	args := map[string]any{"id": st.inc.ID, "finding": map[string]any{
		"title": clip(f.Title, maxFindingTitle), "conclusion": clip(f.Conclusion, maxFindingText), "confidence": clampInt(int(f.Confidence), 0, 100),
		"evidence": f.Evidence, "nextStep": clip(f.NextStep, maxFindingText), "runId": st.run.ID,
	}}
	raw, _ := json.Marshal(args)
	if st.dryRun {
		st.mu.Lock()
		st.captured = append(st.captured, CapturedCall{Tool: toolAddFinding, Input: string(raw)})
		st.findingDone = true
		st.mu.Unlock()
		return
	}
	res, err := uc.loopback(ctx, toolAddFinding, args, token)
	if err != nil {
		uc.d.Logger.WarnContext(ctx, "agent: write finding on behalf of model", "run", st.run.ID, "error", err)
		return
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		uc.d.Logger.WarnContext(ctx, "agent: write finding on behalf of model", "run", st.run.ID, "status", res.StatusCode, "reason", res.Reason)
		if res.StatusCode == 403 {
			st.recordDenied(bizassistant.HeadlessDenied{Tool: toolAddFinding, Reason: res.Reason, Message: res.Message})
		}
		return
	}
	st.observe(&bizassistant.ToolCall{ToolName: toolAddFinding, Write: true}, res.Body)
}

// absorb 把一个阶段（或补救调用）的结果累进轨迹与运行状态。
func (uc *RunnerUsecase) absorb(st *runState, trace *StageTrace, res *bizassistant.HeadlessResult) {
	run := st.run
	trace.Steps += i32(res.Steps)
	trace.PromptTokens += res.PromptTokens
	trace.CompletionTokens += res.CompletionTokens
	trace.EndReason, trace.LastMessageID = res.EndReason, res.LastMessageID
	run.PromptTokens += res.PromptTokens
	run.CompletionTokens += res.CompletionTokens
	run.TokensEstimated = run.TokensEstimated || res.Estimated
	for _, d := range res.Denied {
		st.recordDenied(d)
	}
	for _, c := range res.Captured {
		st.captured = append(st.captured, CapturedCall{Tool: c.Tool, Input: string(c.Input)})
	}
	for _, w := range res.Writes {
		if w.Tool == toolAddFinding {
			st.mu.Lock()
			st.findingDone = true
			st.mu.Unlock()
		}
	}
}

// pickProfile 选 Profile：任务已绑定的直接用；否则按优先级取第一个启用且适用（等级 / 业务）的同类型 Profile。
// 返回 (nil, 原因, nil) 表示跳过。
func (uc *RunnerUsecase) pickProfile(ctx context.Context, t *Task, kind string) (*Profile, string, error) {
	if t.ProfileID != "" {
		p, err := uc.d.Profiles.Get(ctx, t.ProfileID)
		if err != nil {
			if errors.Is(err, ErrProfileNotFound) {
				return nil, "Profile 已删除", nil
			}
			return nil, "", err
		}
		if !p.Spec.Enabled && !t.DryRun {
			return nil, "Profile 已停用", nil
		}
		return p, "", nil
	}
	profiles, err := uc.d.Profiles.ListEnabled(ctx, kind)
	if err != nil {
		return nil, "", err
	}
	for _, p := range profiles {
		if !uc.applies(p, t) {
			continue
		}
		return p, "", nil
	}
	return nil, "没有适用的 Profile", nil
}

// applies 适用范围：等级与业务（人工召唤不受等级范围限制）。
func (uc *RunnerUsecase) applies(p *Profile, t *Task) bool {
	if t.Source == SourceIncident || t.Source == SourcePostmortem {
		found := false
		for _, s := range p.Spec.Severities {
			if s == t.Severity {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if len(p.Spec.BusinessIDs) > 0 {
		found := false
		for _, b := range p.Spec.BusinessIDs {
			if b == t.BusinessID {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// retryOrFail 可重试的失败：未达尝试上限按退避放回队列（运行保留，重新领取时续跑），否则置 failed（运行一并收口）。
func (uc *RunnerUsecase) retryOrFail(ctx context.Context, t *Task, reason string) {
	reason = clip(reason, 255)
	if int(t.Attempts) < uc.d.Config.MaxAttempts() {
		ok, err := uc.d.Tasks.Requeue(ctx, t.ID, t.LockToken, uc.d.Config.Backoff(int(t.Attempts)), reason)
		if err != nil {
			uc.d.Logger.ErrorContext(ctx, "agent: requeue task", "task", t.ID, "error", err)
		} else if !ok {
			uc.d.Logger.WarnContext(ctx, "agent: task no longer owned at requeue", "task", t.ID)
		}
		return
	}
	if t.RunID != "" {
		uc.closeRun(ctx, t.RunID, TaskFailed)
	}
	ok, err := uc.d.Tasks.Finish(ctx, t.ID, t.LockToken, TaskFailed, reason)
	if err != nil {
		uc.d.Logger.ErrorContext(ctx, "agent: finish task", "task", t.ID, "error", err)
		return
	}
	if ok {
		uc.metrics().TaskFinished(ctx, TaskFailed)
	}
}

// loopback 运行器自己的回环调用（不经模型）：按工具名找目录项，以机器令牌调用。
func (uc *RunnerUsecase) loopback(ctx context.Context, tool string, input any, bearer string) (*bizassistant.InvokeResult, error) {
	spec := uc.d.Tools.Find(tool)
	if spec == nil {
		return nil, fmt.Errorf("agent: tool %s not in catalog", tool)
	}
	raw, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	return uc.d.Invoker.Invoke(ctx, *spec, raw, "Bearer "+bearer)
}

// metrics nil 安全的指标面。
func (uc *RunnerUsecase) metrics() Metrics {
	if uc.d.Metrics == nil {
		return noopMetrics{}
	}
	return uc.d.Metrics
}

// noteLLM 记录模型侧成功 / 不可用：连续 llmPauseAfter 次不可用 → 暂停领取 llmPause。
func (uc *RunnerUsecase) noteLLM(ok bool) {
	uc.mu.Lock()
	defer uc.mu.Unlock()
	if ok {
		uc.llmFailures = 0
		return
	}
	uc.llmFailures++
	if uc.llmFailures < llmPauseAfter {
		return
	}
	until := uc.now().Add(llmPause)
	uc.pausedUntil = until
	uc.llmFailures = 0
	uc.mu.Unlock()
	// 通知落库走事务，不能持锁。
	uc.d.Logger.Warn("agent: model unavailable repeatedly, runner paused", "until", until)
	nctx, cancel := context.WithTimeout(context.Background(), finalizeTimeout)
	defer cancel()
	uc.notify(nctx, "agent.paused", "运维 Agent 运行器已暂停",
		fmt.Sprintf("模型连续 %d 次不可用，运行器暂停 %s，任务留在队列。", llmPauseAfter, llmPause), "paused:"+until.Format(time.RFC3339))
	uc.mu.Lock()
}

// countDenial 连续遇到授权拒绝的任务计数：达到阈值打开 Profile 熔断并通知管理员；一次正常运行即清零。
func (uc *RunnerUsecase) countDenial(ctx context.Context, p *Profile, denied bool) {
	uc.mu.Lock()
	if !denied {
		delete(uc.denials, p.ID)
		uc.mu.Unlock()
		return
	}
	uc.denials[p.ID]++
	n := uc.denials[p.ID]
	uc.mu.Unlock()
	if n < denialBreakerAfter {
		return
	}
	reason := fmt.Sprintf("连续 %d 条任务遇到权限拒绝，机器账号权限可能已被收回", n)
	if err := uc.d.Profiles.SetBreaker(ctx, p.ID, true, reason); err != nil {
		uc.d.Logger.ErrorContext(ctx, "agent: set breaker", "profile", p.ID, "error", err)
	}
	uc.notify(ctx, "agent.breaker", "运维 Agent Profile 已熔断", "Profile「"+p.Spec.Name+"」："+reason+"。请检查机器账号的角色授权后在 Profile 里恢复。", "breaker:"+p.ID)
}

// checkBreaker Profile 级熔断：近 24 小时自动执行的建议验证失败率超限 → 停用自动通过能力。
func (uc *RunnerUsecase) checkBreaker(ctx context.Context, p *Profile) {
	if p.BreakerOpen || !p.Spec.Auto.Enabled {
		return
	}
	total, failed, err := uc.d.Runs.RecentAutoOutcome(ctx, p.ID, breakerWindow)
	if err != nil || total < breakerMinSamples || failed*100 <= total*breakerFailurePct {
		return
	}
	reason := fmt.Sprintf("24 小时内自动执行的建议验证失败 %d/%d", failed, total)
	if err := uc.d.Profiles.SetBreaker(ctx, p.ID, true, reason); err != nil {
		uc.d.Logger.ErrorContext(ctx, "agent: set breaker", "profile", p.ID, "error", err)
		return
	}
	uc.notify(ctx, "agent.breaker", "运维 Agent Profile 已熔断", "Profile「"+p.Spec.Name+"」："+reason+"，自动通过能力已停用，需人工恢复。", "breaker:"+p.ID)
}

// notify 平台自监控通知（失败只记日志）。
func (uc *RunnerUsecase) notify(ctx context.Context, kind, title, text, dedup string) {
	if uc.d.Notifier == nil {
		return
	}
	if err := uc.d.Notifier.Notify(ctx, kind, title, text, dedup); err != nil {
		uc.d.Logger.WarnContext(ctx, "agent: notify", "kind", kind, "error", err)
	}
}

// dayKey / hourKey 预算周期键（服务端本地时区）。
func dayKey(t time.Time) string  { return "d:" + t.Format("2006-01-02") }
func hourKey(t time.Time) string { return "h:" + t.Format("2006-01-02T15") }

// ---------------------------------------------------------------------------
// 裁决 / 观察 / 改写（设计 §5.2、§5.3：参数校验、捕获模式、指标时间窗、自动通过申请、日志截尾、先前产出打标）
// ---------------------------------------------------------------------------

// decide 每次工具调用前的裁决。
func (s *runState) decide(_ context.Context, tc *bizassistant.ToolCall, raw json.RawMessage) bizassistant.HeadlessDecision {
	s.mu.Lock()
	defer s.mu.Unlock()
	args := map[string]any{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &args); err != nil {
			return bizassistant.HeadlessDecision{Reason: "参数不是合法的 JSON 对象"}
		}
	}
	approve := func(rewrite map[string]any) bizassistant.HeadlessDecision {
		d := bizassistant.HeadlessDecision{Approve: true}
		if rewrite != nil {
			d.Rewrite, _ = json.Marshal(rewrite)
		}
		return d
	}
	write := func(rewrite map[string]any) bizassistant.HeadlessDecision {
		d := approve(rewrite)
		if s.dryRun {
			d.Approve, d.Capture = false, true
		}
		return d
	}
	reject := func(format string, a ...any) bizassistant.HeadlessDecision {
		return bizassistant.HeadlessDecision{Reason: fmt.Sprintf(format, a...)}
	}
	// 故障域的按故障接口：id 一律换成当前故障的 UUID（模型常把故障编号当 id）。
	if incidentScoped(tc.ToolName) {
		args["id"] = s.inc.ID
	}
	switch tc.ToolName {
	case toolQueryRange:
		return approve(s.clampWindow(args, "start", "end", true))
	case toolQueryInstant:
		return approve(s.clampInstant(args))
	case toolK8sMetrics:
		return approve(s.clampWindow(args, "start", "end", false))
	case toolAddFinding:
		if s.findingDone {
			return reject("本次运行已写入发现，不要重复写入；如需补充请用 IncidentService_AddComment")
		}
		f, ok := args["finding"].(map[string]any)
		if !ok {
			return reject("finding 必须是对象")
		}
		title := strings.TrimSpace(str(f["title"]))
		if title == "" {
			return reject("finding.title 不能为空")
		}
		f["title"] = clip(title, maxFindingTitle)
		f["conclusion"] = clip(str(f["conclusion"]), maxFindingText)
		f["nextStep"] = clip(firstStr(f, "nextStep", "next_step"), maxFindingText)
		delete(f, "next_step")
		f["confidence"] = clampInt(f["confidence"], 0, 100)
		f["evidence"] = strList(f["evidence"], maxEvidence)
		f["runId"] = s.run.ID
		delete(f, "run_id")
		args["id"], args["finding"] = s.inc.ID, f
		s.finding = &Finding{Title: f["title"].(string), Conclusion: f["conclusion"].(string), Confidence: i32(f["confidence"].(int)), Evidence: f["evidence"].([]string), NextStep: f["nextStep"].(string)}
		if s.dryRun {
			s.findingDone = true
		}
		return write(args)
	case "IncidentService_AddComment":
		if s.comments >= maxComments {
			return reject("本次运行的评论已达上限（%d）", maxComments)
		}
		if strings.TrimSpace(str(args["text"])) == "" {
			return reject("text 不能为空")
		}
		args["id"], args["text"] = s.inc.ID, clip(strings.TrimSpace(str(args["text"])), maxCommentText)
		delete(args, "fileIds")
		delete(args, "file_ids")
		s.comments++
		return write(args)
	case toolMarkChange:
		ref := strings.TrimSpace(firstStr(args, "refId", "ref_id"))
		if ref == "" || !s.knownIn(seenChange, ref) {
			return reject("ref_id 必须是本次运行里 ListChanges / 故障上下文返回过的变更")
		}
		args["id"], args["refId"] = s.inc.ID, ref
		delete(args, "ref_id")
		args["relation"] = clampInt(args["relation"], 0, 2)
		if note := firstStr(args, "note", "reason"); note != "" {
			args["note"] = clip(note, maxFindingText)
		}
		delete(args, "reason")
		return write(args)
	case toolAttachAlerts:
		ids := strList(firstAny(args, "alertIds", "alert_ids"), maxAttach)
		if len(ids) == 0 {
			return reject("alert_ids 不能为空")
		}
		for _, id := range ids {
			if !s.knownIn(seenAlert, id) {
				return reject("alert_ids 里的 %s 不是本次运行里出现过的告警", id)
			}
		}
		args["id"], args["alertIds"] = s.inc.ID, ids
		delete(args, "alert_ids")
		return write(args)
	case toolCreateProposal:
		if s.proposals >= maxProposals {
			return reject("本次运行的建议已达上限（%d）", maxProposals)
		}
		pb := firstStr(args, "playbookId", "playbook_id")
		if pb == "" || !s.knownIn(seenPlaybook, pb) {
			return reject("只能引用故障上下文里推荐预案的 playbook_id")
		}
		delete(args, "action")
		delete(args, "approve")
		args["id"], args["playbookId"] = s.inc.ID, pb
		delete(args, "playbook_id")
		args["stepIndex"] = clampInt(firstAny(args, "stepIndex", "step_index"), 0, 19)
		delete(args, "step_index")
		args["reason"] = clip(str(args["reason"]), maxFindingText)
		ev := strList(args["evidence"], maxEvidence-1)
		args["evidence"] = append(ev, "agent_run:"+s.run.ID)
		args["autoEligible"] = s.autoEligible()
		args["autoMaxTargets"] = s.profile.Spec.Auto.MaxTargets
		delete(args, "auto_eligible")
		delete(args, "auto_max_targets")
		s.proposals++
		return write(args)
	case toolCreateDocument:
		if s.profile.Spec.Kind != KindPostmortem {
			return reject("只有经验沉淀类 Profile 可以新建知识文档")
		}
		if s.documents >= 1 {
			return reject("本次运行已创建过知识草稿，不要重复创建")
		}
		if s.profile.Spec.KnowledgeSpaceID == "" {
			return reject("Profile 未配置知识空间")
		}
		title := strings.TrimSpace(str(args["title"]))
		if title == "" {
			return reject("title 不能为空")
		}
		args["spaceId"] = s.profile.Spec.KnowledgeSpaceID
		delete(args, "space_id")
		args["title"] = clip(title, maxFindingTitle)
		args["content"] = clip(str(args["content"]), maxDocContent)
		args["summary"] = clip(str(args["summary"]), 1000)
		args["tags"] = strList(args["tags"], 10)
		args["source"], args["sourceRef"], args["publish"] = "postmortem", s.inc.ID, false
		delete(args, "source_ref")
		delete(args, "ownerId")
		delete(args, "owner_id")
		delete(args, "slug")
		s.documents++
		return write(args)
	default:
		if tc.Write {
			return reject("写工具 %s 不在本次运行允许的范围内", tc.ToolName)
		}
		if incidentScoped(tc.ToolName) {
			return approve(args)
		}
		return approve(nil)
	}
}

// incidentScoped 报告工具是否为故障域的「按故障 ID」接口（路径里带故障 id）；列表 / 检索类除外。
func incidentScoped(tool string) bool {
	if !strings.HasPrefix(tool, "Incident") {
		return false
	}
	switch tool {
	case "IncidentService_ListIncidents", "IncidentService_SearchIncidents", "IncidentService_ListCandidates",
		"IncidentService_ListIncidentsByAlert", "IncidentService_ListIncidentOptions", "IncidentService_CreateIncident",
		"IncidentConfigService_ListPlaybooks", "IncidentConfigService_ListBusinesses", "IncidentConfigService_ListRules":
		return false
	}
	return !strings.HasPrefix(tool, "IncidentConfigService_")
}

// window 指标查询允许的时间窗 [故障发生 − 30 分钟, 解决（或现在）+ 30 分钟]。
func (s *runState) window() (time.Time, time.Time) {
	lo := s.inc.OccurTime.Add(-metricWindow)
	hi := time.Now().Add(metricWindow)
	if s.inc.ResolveTime != nil {
		hi = s.inc.ResolveTime.Add(metricWindow)
	}
	return lo, hi
}

// clampWindow 把区间查询的起止夹在允许的时间窗内；withStep 时步长按窗口给定（monitor 的 step_s）。
func (s *runState) clampWindow(args map[string]any, startKey, endKey string, withStep bool) map[string]any {
	lo, hi := s.window()
	start, end := parseTime(args[startKey]), parseTime(args[endKey])
	if start.IsZero() || start.Before(lo) {
		start = lo
	}
	if end.IsZero() || end.After(hi) {
		end = hi
	}
	if !end.After(start) {
		start, end = lo, hi
	}
	args[startKey], args[endKey] = start.UTC().Format(time.RFC3339), end.UTC().Format(time.RFC3339)
	if withStep {
		step := int(end.Sub(start).Seconds() / 200)
		if step < 15 {
			step = 15
		}
		args["stepS"] = step
		delete(args, "step_s")
	}
	return args
}

// clampInstant 即时查询的时间点夹在允许的时间窗内（缺省取窗口上界 = 最新）。
func (s *runState) clampInstant(args map[string]any) map[string]any {
	lo, hi := s.window()
	at := parseTime(args["time"])
	if at.IsZero() || at.After(hi) {
		at = hi
	}
	if at.Before(lo) {
		at = lo
	}
	args["time"] = at.UTC().Format(time.RFC3339)
	return args
}

// transform 成功输出的改写：日志类工具按行截尾；故障上下文 / 时间线里 Agent 先前产出打标。
func (s *runState) transform(tc *bizassistant.ToolCall, output json.RawMessage) json.RawMessage {
	if len(output) > transformMaxBytes {
		return nil
	}
	switch {
	case strings.HasSuffix(tc.ToolName, "Log") || strings.HasSuffix(tc.ToolName, "Logs"):
		return tailLogs(output)
	case tc.ToolName == toolGetContext || tc.ToolName == "IncidentService_ListTimeline":
		if !bytes.Contains(output, []byte(`"actorType":3`)) && !bytes.Contains(output, []byte(`"actorType": 3`)) {
			return nil
		}
		return markAgentEvents(output)
	}
	return nil
}

// tailLogs 把输出里较长的多行字符串字段只保留尾部 logTailLines 行（日志越新越有价值；通用截断会砍掉尾部）。
func tailLogs(output json.RawMessage) json.RawMessage {
	if len(output) < logTailMinBytes {
		return nil
	}
	var v any
	if err := json.Unmarshal(output, &v); err != nil {
		return nil
	}
	changed := false
	var walk func(any) any
	walk = func(x any) any {
		switch t := x.(type) {
		case map[string]any:
			for k, val := range t {
				t[k] = walk(val)
			}
			return t
		case []any:
			for i := range t {
				t[i] = walk(t[i])
			}
			return t
		case string:
			if len(t) < logTailMinBytes || !strings.Contains(t, "\n") {
				return t
			}
			lines := strings.Split(t, "\n")
			if len(lines) <= logTailLines {
				return t
			}
			changed = true
			return fmt.Sprintf("（日志共 %d 行，只保留最后 %d 行）\n", len(lines), logTailLines) + strings.Join(lines[len(lines)-logTailLines:], "\n")
		}
		return x
	}
	v = walk(v)
	if !changed {
		return nil
	}
	out, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return out
}

// markAgentEvents 给时间线里操作者类型为 Agent（3）的记录加标记：它们是待验证的假设，不是事实。
func markAgentEvents(output json.RawMessage) json.RawMessage {
	var v any
	if err := json.Unmarshal(output, &v); err != nil {
		return nil
	}
	changed := false
	var walk func(any, int)
	walk = func(x any, depth int) {
		if depth > 6 {
			return
		}
		switch t := x.(type) {
		case map[string]any:
			if at, ok := t["actorType"]; ok && clampInt(at, 0, 9) == 3 {
				if _, has := t["note"]; !has {
					t["note"] = agentPriorNote
					changed = true
				}
				if d, ok := t["detail"].(map[string]any); ok {
					if f, ok := d["finding"].(map[string]any); ok {
						if title, ok := f["title"].(string); ok && !strings.HasPrefix(title, agentPriorNote) {
							f["title"] = agentPriorNote + " " + title
							changed = true
						}
					}
					if text, ok := d["text"].(string); ok && text != "" && !strings.HasPrefix(text, agentPriorNote) {
						d["text"] = agentPriorNote + " " + text
						changed = true
					}
				}
			}
			for _, val := range t {
				walk(val, depth+1)
			}
		case []any:
			for _, item := range t {
				walk(item, depth+1)
			}
		}
	}
	walk(v, 0)
	if !changed {
		return nil
	}
	out, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return out
}

// observe 成功的工具调用：收集输出里出现过的对象 ID 与环境编码；记录指标 / 诊断类证据与产出引用。
func (s *runState) observe(tc *bizassistant.ToolCall, output json.RawMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch tc.ToolName {
	case toolAddFinding:
		s.findingDone = true
		var resp struct {
			Item struct {
				ID string `json:"id"`
			} `json:"item"`
		}
		if json.Unmarshal(output, &resp) == nil && resp.Item.ID != "" {
			s.run.FindingEventID = resp.Item.ID
		}
	case toolCreateProposal:
		var resp struct {
			Proposal struct {
				ID    string `json:"id"`
				Title string `json:"title"`
			} `json:"proposal"`
		}
		if json.Unmarshal(output, &resp) == nil && resp.Proposal.ID != "" {
			s.proposalsRefs = append(s.proposalsRefs, Ref{ID: resp.Proposal.ID, Name: resp.Proposal.Title})
		}
	case toolCreateDocument:
		// 经验沉淀：知识草稿 ID 记为「发现」产出（运行状态据此判成功）。
		var resp struct {
			Document struct {
				ID    string `json:"id"`
				Title string `json:"title"`
			} `json:"document"`
		}
		if json.Unmarshal(output, &resp) == nil && resp.Document.ID != "" {
			s.findingDone = true
			s.run.FindingEventID = resp.Document.ID
			s.finding = &Finding{Title: resp.Document.Title, Conclusion: "知识草稿已创建（待人工审阅发布）：" + resp.Document.ID}
		}
	}
	if strings.HasPrefix(tc.ToolName, "MonitorDatasourceService_Query") || strings.HasPrefix(tc.ToolName, "K8sInsightService_Diagnose") || tc.ToolName == toolK8sMetrics {
		s.metricEvid = true
	}
	s.collectSources(tc.ToolName, output)
	if len(s.seen) < maxSeenIDs {
		for _, id := range uuidPattern.FindAllString(string(output), maxSeenIDs-len(s.seen)) {
			s.seen[strings.ToLower(id)] = struct{}{}
		}
	}
	var v any
	if json.Unmarshal(output, &v) == nil {
		collectEnvs(v, s.envs, 0)
	}
}

// recordDenied 记录一次 403：incident 业务层的拒绝（租约被释放 / 被占用）只结束本次任务；授权中间件的拒绝计入权限收回。
func (s *runState) recordDenied(d bizassistant.HeadlessDenied) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deniedCalls = append(s.deniedCalls, DeniedCall{Tool: d.Tool, Reason: d.Reason, Message: d.Message})
	switch {
	case d.Reason == "INCIDENT_FORBIDDEN":
		s.leaseLost = true
	case !strings.HasPrefix(d.Reason, "INCIDENT_"):
		s.authDenied = true
	}
}

// knownIn 对象 ID 是否来自指定来源（变更只认 ListChanges / 上下文的 changes，告警只认故障的关联告警，预案只认上下文推荐）。
func (s *runState) knownIn(category, id string) bool {
	_, ok := s.seenBy[category][strings.ToLower(strings.TrimSpace(id))]
	return ok
}

func (s *runState) addSource(category, id string) {
	id = strings.ToLower(strings.TrimSpace(id))
	if id == "" || len(s.seenBy[category]) >= maxSeenIDs {
		return
	}
	if s.seenBy == nil {
		s.seenBy = map[string]map[string]struct{}{}
	}
	if s.seenBy[category] == nil {
		s.seenBy[category] = map[string]struct{}{}
	}
	s.seenBy[category][id] = struct{}{}
}

// collectSources 从故障上下文 / 关联告警 / 变更列表的输出里按来源收集对象 ID。
func (s *runState) collectSources(tool string, output json.RawMessage) {
	switch tool {
	case toolGetContext:
		var v struct {
			Alerts []struct {
				AlertID string `json:"alertId"`
			} `json:"alerts"`
			Changes []struct {
				RefID string `json:"refId"`
			} `json:"changes"`
			Playbooks []struct {
				ID string `json:"id"`
			} `json:"playbooks"`
		}
		if json.Unmarshal(output, &v) != nil {
			return
		}
		for _, a := range v.Alerts {
			s.addSource(seenAlert, a.AlertID)
		}
		for _, c := range v.Changes {
			s.addSource(seenChange, c.RefID)
		}
		for _, p := range v.Playbooks {
			s.addSource(seenPlaybook, p.ID)
		}
	case "IncidentService_ListIncidentAlerts":
		var v struct {
			Items []struct {
				AlertID string `json:"alertId"`
			} `json:"items"`
		}
		if json.Unmarshal(output, &v) == nil {
			for _, a := range v.Items {
				s.addSource(seenAlert, a.AlertID)
			}
		}
	case "IncidentService_ListChanges":
		var v struct {
			Items []struct {
				RefID string `json:"refId"`
			} `json:"items"`
		}
		if json.Unmarshal(output, &v) == nil {
			for _, c := range v.Items {
				s.addSource(seenChange, c.RefID)
			}
		}
	}
}

// autoEligible Agent 侧的自动通过条件：Profile 开启且未熔断、发现置信度达标、有指标 / 诊断类证据、
// 观察到的全部资源环境都在允许集合内（没观察到环境信息时不申请）。靶标数由故障侧按 auto_max_targets 判定。
func (s *runState) autoEligible() bool {
	a := s.profile.Spec.Auto
	if !a.Enabled || s.profile.BreakerOpen || s.finding == nil || s.finding.Confidence < a.MinConfidence {
		return false
	}
	if a.RequireMetricEvidence && !s.metricEvid {
		return false
	}
	if len(s.envs) == 0 {
		return false
	}
	for env := range s.envs {
		allowed := false
		for _, e := range a.Envs {
			if strings.EqualFold(e, env) {
				allowed = true
				break
			}
		}
		if !allowed {
			return false
		}
	}
	return true
}

// collectEnvs 递归收集 JSON 里键为 env 的字符串值（深度封顶）。
func collectEnvs(v any, out map[string]struct{}, depth int) {
	if depth > 8 {
		return
	}
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			if k == "env" {
				if s, ok := val.(string); ok && s != "" {
					out[s] = struct{}{}
				}
				continue
			}
			collectEnvs(val, out, depth+1)
		}
	case []any:
		for _, item := range t {
			collectEnvs(item, out, depth+1)
		}
	}
}

// ---- 参数小工具 ----

// i32 把 int 收窄到 int32（溢出取边界）。
func i32(n int) int32 {
	if n > math.MaxInt32 {
		return math.MaxInt32
	}
	if n < math.MinInt32 {
		return math.MinInt32
	}
	return int32(n) // #nosec G115 -- 已夹在 int32 范围内
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

func firstAny(m map[string]any, keys ...string) any {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			return v
		}
	}
	return nil
}

func firstStr(m map[string]any, keys ...string) string { return str(firstAny(m, keys...)) }

func clampInt(v any, lo, hi int) int {
	n := 0
	switch t := v.(type) {
	case float64:
		n = int(t)
	case int:
		n = t
	case string:
		n, _ = strconv.Atoi(strings.TrimSpace(t))
	}
	if n < lo {
		return lo
	}
	if n > hi {
		return hi
	}
	return n
}

func strList(v any, limit int) []string {
	out := []string{}
	items, ok := v.([]any)
	if !ok {
		return out
	}
	for _, it := range items {
		if s, ok := it.(string); ok && strings.TrimSpace(s) != "" {
			out = append(out, clip(strings.TrimSpace(s), 2000))
		}
		if len(out) >= limit {
			break
		}
	}
	return out
}

func parseTime(v any) time.Time {
	s, _ := v.(string)
	if s == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05", "2006-01-02T15:04:05"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t
		}
	}
	return time.Time{}
}
