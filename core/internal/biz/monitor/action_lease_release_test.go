package biz_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// 领取时整批的下次可领取时间都被推后一个租约（5 分钟）。一批中途停下（续租或落账出错、正在停机）时，
// 还没落账的动作必须收回租约，否则要空等 5 分钟才会被重领。回归 monitor 模块评审 dispatch P3-1。

// leaseRetry 一次 Retry 调用的入参：reason 非空是记失败，backoff 为 0 且 reason 为空是收回租约。
type leaseRetry struct {
	id      string
	backoff time.Duration
	reason  string
}

// leaseSpy 在内存仓储之上注入续租 / 记失败 / 收回租约的错误，并记下每次 Retry 的入参。
type leaseSpy struct {
	biz.ActionRepo
	renewErr   map[string]error
	recordErr  error
	releaseErr error
	retries    []leaseRetry
}

func (s *leaseSpy) Renew(ctx context.Context, a *biz.AlertAction, lease time.Duration) (bool, error) {
	if err := s.renewErr[a.ID]; err != nil {
		return false, err
	}
	return s.ActionRepo.Renew(ctx, a, lease)
}

func (s *leaseSpy) Retry(ctx context.Context, a *biz.AlertAction, backoff time.Duration, reason string) error {
	s.retries = append(s.retries, leaseRetry{id: a.ID, backoff: backoff, reason: reason})
	if reason != "" && s.recordErr != nil {
		return s.recordErr
	}
	if reason == "" && s.releaseErr != nil {
		return s.releaseErr
	}
	return s.ActionRepo.Retry(ctx, a, backoff, reason)
}

// withActions 以给定的动作仓储另建用例（其余依赖不变）。
func (a *actionEnv) withActions(repo biz.ActionRepo, linker biz.AlertLinkWriter, summary biz.Summarizer) *biz.ActionUsecase {
	a.t.Helper()
	uc, err := biz.NewActionUsecase(biz.NewActionDeps(testfake.NewTxManager(), repo, a.env.store.Alerts(), a.world, a.world, linker,
		a.sync.CIIndex(), a.env.scopes, a.env.org, a.world, a.world, a.world, a.world, summary, quietLog(), nil))
	if err != nil {
		a.t.Fatalf("NewActionUsecase() error = %v", err)
	}
	uc.SetNow(func() time.Time { return a.now })
	return uc
}

// wantReleased 断言动作仍待执行、尝试次数与状态未变、不记失败，且租约已收回（可以立刻被重领）。
func (a *actionEnv) wantReleased(id string, attempts int32) {
	a.t.Helper()
	row := a.status(id)
	if row.Status != mc.ActionPending || row.Attempts != attempts || row.LastError != "" || !row.NextTime.Equal(a.now) {
		a.t.Fatalf("action %s = %+v, want pending with attempts %d and the lease pulled back to now", id, row, attempts)
	}
}

func TestAction_RunRenewErrorStopsTheBatchAndReleasesLeases(t *testing.T) {
	a := newActionEnv(t, nil)
	a.seed("ac-1", mc.ActionAISummary, mc.ActionPayload{})
	a.seed("ac-2", mc.ActionAISummary, mc.ActionPayload{})
	a.seed("ac-3", mc.ActionAISummary, mc.ActionPayload{})
	spy := &leaseSpy{ActionRepo: a.sync.Actions(), renewErr: map[string]error{"ac-2": errBoom}}
	uc := a.withActions(spy, a.world, a.world)

	n, err := uc.Run(context.Background())
	if err == nil || n != 1 {
		t.Fatalf("Run() = %d, %v; want 1 recorded and the renew error returned", n, err)
	}
	// 续租出错的那一条与排在它后面的都没有执行；租约收回，尝试次数不动，也不记失败。
	if len(a.world.prompts) != 1 {
		t.Fatalf("summaries attempted = %d, want only the first", len(a.world.prompts))
	}
	a.wantReleased("ac-2", 1)
	a.wantReleased("ac-3", 1)
	if got := spy.retries; !slices.Equal(got, []leaseRetry{{id: "ac-2"}, {id: "ac-3"}}) {
		t.Fatalf("release calls = %+v, want the two unfinished actions released with no backoff and no reason", got)
	}
	if row := a.status("ac-1"); row.Status != mc.ActionDone {
		t.Fatalf("the recorded action must stay done: %+v", row)
	}
	if len(a.world.eventsOf(mc.AlertEventActionFailed)) != 0 {
		t.Fatal("an interrupted batch must not record failures")
	}

	// 下一轮（数据库恢复）：两条立刻被重领并执行，不必等领取时推后的租约到期。
	delete(spy.renewErr, "ac-2")
	if n, err := uc.Run(context.Background()); err != nil || n != 2 {
		t.Fatalf("Run() after recovery = %d, %v; want 2", n, err)
	}
	for _, id := range []string{"ac-2", "ac-3"} {
		if row := a.status(id); row.Status != mc.ActionDone || row.Attempts != 2 {
			t.Fatalf("action %s = %+v, want done on the second attempt", id, row)
		}
	}
}

// stoppingSummarizer 生成摘要的途中收到停机信号（循环的 ctx 被取消）。
type stoppingSummarizer struct {
	*actionWorld
	cancel context.CancelFunc
}

func (s stoppingSummarizer) Summarize(ctx context.Context, system, prompt string) (string, error) {
	s.cancel()
	return s.actionWorld.Summarize(ctx, system, prompt)
}

func TestAction_RunShutdownReleasesUnfinishedLeases(t *testing.T) {
	a := newActionEnv(t, nil)
	a.world.aiBlock = true // 摘要请求一直等到 ctx 结束
	for _, id := range []string{"ac-1", "ac-2", "ac-3"} {
		a.seed(id, mc.ActionAISummary, mc.ActionPayload{})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	uc := a.withActions(a.sync.Actions(), a.world, stoppingSummarizer{actionWorld: a.world, cancel: cancel})

	n, err := uc.Run(ctx)
	if err == nil || n != 0 {
		t.Fatalf("Run() = %d, %v; want nothing recorded and the shutdown returned", n, err)
	}
	// 第一条执行到一半被取消、后两条根本没轮到：三条都不记失败，租约全部收回，别的实例（或重启后的本实例）
	// 可以立刻重做，而不是等 5 分钟。
	if len(a.world.prompts) != 1 {
		t.Fatalf("summaries attempted = %d, want only the first", len(a.world.prompts))
	}
	for _, id := range []string{"ac-1", "ac-2", "ac-3"} {
		a.wantReleased(id, 1)
	}
	if len(a.world.eventsOf(mc.AlertEventActionFailed)) != 0 {
		t.Fatal("a shutdown must not record failures")
	}

	a.world.aiBlock = false
	if n, err := uc.Run(context.Background()); err != nil || n != 3 {
		t.Fatalf("Run() after restart = %d, %v; want 3", n, err)
	}
	for _, id := range []string{"ac-1", "ac-2", "ac-3"} {
		if row := a.status(id); row.Status != mc.ActionDone || row.Attempts != 2 {
			t.Fatalf("action %s = %+v, want done after the redo", id, row)
		}
	}
}

func TestAction_RunRecordingErrorReleasesTheLease(t *testing.T) {
	a := newActionEnv(t, nil)
	a.world.appendErr = errors.New("db down") // 落账（追加时间线）失败
	a.seed("ac-1", mc.ActionWorkflow, mc.ActionPayload{DefinitionKey: flowHeal, AgentUserID: uBob})
	a.seed("ac-2", mc.ActionAISummary, mc.ActionPayload{})
	// 记失败（带退避）也写不进去：这一轮对 ac-1 既没有落账、也没有排期。
	spy := &leaseSpy{ActionRepo: a.sync.Actions(), recordErr: errors.New("db still down")}
	uc := a.withActions(spy, a.world, a.world)

	n, err := uc.Run(context.Background())
	if err == nil || n != 0 {
		t.Fatalf("Run() = %d, %v; want nothing recorded and the error returned", n, err)
	}
	if len(a.world.starts) != 1 {
		t.Fatalf("workflows started = %d, want the first action executed once", len(a.world.starts))
	}
	// 执行了却落不了账的那一条：租约收回后可以立即重做（外部调用以事件 ID 幂等），不必等 5 分钟。
	a.wantReleased("ac-1", 1)
	if len(a.world.prompts) != 0 {
		t.Fatal("the action behind a failing commit must not run in this batch")
	}
	a.wantReleased("ac-2", 1)
}

func TestAction_RunReleaseFailureKeepsTheOriginalLease(t *testing.T) {
	a := newActionEnv(t, nil)
	a.seed("ac-1", mc.ActionAISummary, mc.ActionPayload{})
	a.seed("ac-2", mc.ActionAISummary, mc.ActionPayload{})
	spy := &leaseSpy{ActionRepo: a.sync.Actions(), renewErr: map[string]error{"ac-2": errBoom}, releaseErr: errors.New("db down")}
	uc := a.withActions(spy, a.world, a.world)

	if n, err := uc.Run(context.Background()); err == nil || n != 1 {
		t.Fatalf("Run() = %d, %v; want 1 recorded and the renew error returned", n, err)
	}
	// 收回租约也失败（数据库仍不可用）：这一行保持领取时推后的租约，到期后照常可以被重领。
	if row := a.status("ac-2"); row.Status != mc.ActionPending || row.Attempts != 1 || !row.NextTime.Equal(a.now.Add(biz.ActionLease)) {
		t.Fatalf("action = %+v, want pending with the lease taken at claim time", row)
	}
	if row := a.status("ac-1"); row.Status != mc.ActionDone {
		t.Fatalf("the recorded action must stay done: %+v", row)
	}
}

// stoppingLinker 摘要写回之后停机信号到达：这一条照常落账完成，同批后面的不再执行。
type stoppingLinker struct {
	*actionWorld
	cancel context.CancelFunc
}

func (s stoppingLinker) SetAISummaryTx(ctx context.Context, tx txrun.Tx, alertID, summary string) error {
	err := s.actionWorld.SetAISummaryTx(ctx, tx, alertID, summary)
	s.cancel()
	return err
}

func TestAction_RunShutdownAfterRecordingLeavesTheRecordedRowAlone(t *testing.T) {
	a := newActionEnv(t, nil)
	a.seed("ac-1", mc.ActionAISummary, mc.ActionPayload{})
	a.seed("ac-2", mc.ActionAISummary, mc.ActionPayload{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	uc := a.withActions(a.sync.Actions(), stoppingLinker{actionWorld: a.world, cancel: cancel}, a.world)

	if n, err := uc.Run(ctx); err == nil || n != 0 {
		t.Fatalf("Run() = %d, %v; want the shutdown returned", n, err)
	}
	// 已经落账的那一条不受收回租约影响（围栏：仍是待执行且尝试次数未变）；没轮到的那一条被收回。
	if row := a.status("ac-1"); row.Status != mc.ActionDone || !row.Done || row.Attempts != 1 || row.LastError != "" {
		t.Fatalf("recorded action = %+v, want it left alone", row)
	}
	a.wantReleased("ac-2", 1)
	if len(a.world.prompts) != 1 {
		t.Fatalf("summaries attempted = %d, want only the first", len(a.world.prompts))
	}
}
