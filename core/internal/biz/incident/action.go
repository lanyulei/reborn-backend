package biz

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// 动作发件箱 worker（功能设计 §11.3）：AI 摘要、执行已批准的建议、验证窗口检查、恢复时回写工单。
// SKIP LOCKED 领取、租约、退避；落账独立超时。

const (
	actionBatch       = 10
	actionLease       = 5 * time.Minute
	actionBackoffBase = 30 * time.Second
	actionBackoffMax  = 30 * time.Minute
	actionMaxAttempts = 5
	actionAIAttempts  = 2
	actionFinishWait  = 10 * time.Second
	actionRetention   = 30 * 24 * time.Hour
	proposalPollBatch = 50
	proposalVerifyMax = 50
)

// ActionUsecase 动作 worker。
type ActionUsecase struct {
	d         *Deps
	inc       *IncidentUsecase
	proposals *ProposalUsecase
	now       func() time.Time
}

// NewActionUsecase new an ActionUsecase.
func NewActionUsecase(d *Deps, inc *IncidentUsecase, proposals *ProposalUsecase) *ActionUsecase {
	return &ActionUsecase{d: d, inc: inc, proposals: proposals, now: time.Now}
}

// actionPermanent 不可重试的失败。
type actionPermanent struct{ msg string }

func (e *actionPermanent) Error() string { return e.msg }

func permanent(format string, args ...any) error {
	return &actionPermanent{msg: fmt.Sprintf(format, args...)}
}

// errRescheduled 动作已自行重排（Retry 已写回下次执行时间），本轮不落账。
var errRescheduled = errors.New("incident action: rescheduled")

// Run 领取并执行一批动作，返回已落账的条数。一条落不了账（数据库错误）不再牵连同批其余动作：逐条继续，整批
// 结束后把没落账的行的租约收回（下次可领取时间拉回现在，不必等 5 分钟租约到期），再把首个错误返回给循环。
// 正在停机（ctx 取消）时不再执行后面的动作，同样收回租约；进程被强杀时没有这一步，这些行仍要等租约到期。
func (uc *ActionUsecase) Run(ctx context.Context) (int, error) {
	items, err := uc.d.Actions.Claim(ctx, actionBatch, actionLease)
	if err != nil || len(items) == 0 {
		return 0, err
	}
	done := 0
	var held []*Action
	var firstErr error
	for i, a := range items {
		if err := uc.handle(ctx, a); err != nil {
			held = append(held, a)
			firstErr = cmp.Or(firstErr, err)
		} else {
			done++
		}
		if ctx.Err() != nil {
			held = append(held, items[i+1:]...)
			break
		}
	}
	if len(held) > 0 {
		uc.release(ctx, held)
	}
	return done, firstErr
}

// release 收回一批动作的租约：下次可领取时间拉回现在，别的实例或下一轮可以立刻重领（尝试次数不动，也不记失败
// 原因）。以「仍是待执行且尝试次数未变」围栏，已落账或已被别的实例领走的行不受影响。收回不受停机取消影响，整批
// 共用一个超时；收回失败的行保持原租约，到期后自然可以被重领。
func (uc *ActionUsecase) release(ctx context.Context, items []*Action) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), actionFinishWait)
	defer cancel()
	for _, a := range items {
		if err := uc.d.Actions.Retry(ctx, a, 0, ""); err != nil {
			uc.d.Logger.WarnContext(ctx, "incident action lease not released, it will expire", "action", a.ID, "kind", a.Kind, "incident", a.IncidentID, "error", err)
		}
	}
}

func maxAttempts(kind int32) int32 {
	if kind == ActionAISummary {
		return actionAIAttempts
	}
	return actionMaxAttempts
}

// handle 执行一条动作并落账。
func (uc *ActionUsecase) handle(ctx context.Context, a *Action) error {
	err := uc.execute(ctx, a)
	if errors.Is(err, errRescheduled) {
		return nil
	}
	if err != nil && ctx.Err() != nil {
		// 正在停机：执行被取消，不记失败（否则每次停机都消耗一次尝试，恰逢最后一次还会被直接丢弃）；
		// Run 收回租约，重启后立刻可以重做。
		return ctx.Err()
	}
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), actionFinishWait)
	defer cancel()
	if err == nil {
		return uc.finish(fctx, a, OutboxDone, "")
	}
	var perm *actionPermanent
	if errors.As(err, &perm) || a.Attempts >= maxAttempts(a.Kind) {
		uc.d.Logger.WarnContext(fctx, "incident action failed permanently", "action", a.ID, "kind", a.Kind, "incident", a.IncidentID, "attempts", a.Attempts, "error", err)
		return uc.finish(fctx, a, OutboxDropped, truncate(err.Error(), 500))
	}
	uc.d.Logger.WarnContext(fctx, "incident action failed, will retry", "action", a.ID, "kind", a.Kind, "attempts", a.Attempts, "error", err)
	return uc.d.Actions.Retry(fctx, a, backoff(a.Attempts, actionBackoffBase, actionBackoffMax), truncate(err.Error(), 500))
}

func (uc *ActionUsecase) finish(ctx context.Context, a *Action, status int32, note string) error {
	return uc.d.Txm.WithinTx(ctx, func(tx Tx) error {
		_, err := uc.d.Actions.FinishTx(ctx, tx, a, status, note)
		return err
	})
}

func (uc *ActionUsecase) execute(ctx context.Context, a *Action) error {
	switch a.Kind {
	case ActionAISummary:
		if uc.d.AI == nil || !uc.d.AI.Available() {
			return permanent("智能助手未配置模型密钥")
		}
		_, err := uc.inc.summarize(ctx, a.IncidentID)
		if isNotFound(err) {
			return permanent("故障不存在")
		}
		return err
	case ActionProposalExec:
		err := uc.proposals.execute(ctx, a.RefID)
		if isNotFound(err) {
			return permanent("建议不存在")
		}
		return err
	case ActionVerify:
		return uc.verify(ctx, a)
	case ActionTicketNote:
		return uc.ticketNote(ctx, a)
	}
	return permanent("unknown action kind %d", a.Kind)
}

// verify 验证窗口：窗口未到则重排到窗口结束。
func (uc *ActionUsecase) verify(ctx context.Context, a *Action) error {
	p, err := uc.d.Proposals.Get(ctx, a.RefID)
	if err != nil {
		if isNotFound(err) {
			return permanent("建议不存在")
		}
		return err
	}
	if p.Status != ProposalSucceeded || p.VerifyResult != nil {
		return nil
	}
	if p.VerifyUntil != nil && p.VerifyUntil.After(uc.now()) {
		// 还没到：按剩余时间重排（Retry 的退避改成精确等待），并告知 handle 不要把它落账为已完成。
		if err := uc.d.Actions.Retry(ctx, a, p.VerifyUntil.Sub(uc.now())+time.Second, "等待验证窗口结束"); err != nil {
			return err
		}
		return errRescheduled
	}
	var payload struct {
		AlertsBefore int32 `json:"alerts_before"`
	}
	_ = json.Unmarshal(a.Payload, &payload)
	return uc.proposals.verify(ctx, p, payload.AlertsBefore)
}

// ticketNote 故障恢复时在关联工单（转工单 / 改进项建单）上追加记录。
func (uc *ActionUsecase) ticketNote(ctx context.Context, a *Action) error {
	inc, err := uc.d.Incidents.Get(ctx, a.IncidentID)
	if err != nil {
		if isNotFound(err) {
			return permanent("故障不存在")
		}
		return err
	}
	events, err := uc.d.Events.Recent(ctx, inc.ID, nil, 500)
	if err != nil {
		return err
	}
	noted := map[string]struct{}{}
	for _, ev := range events {
		if ev.Type != EventTicket || ev.Detail.TicketID == "" {
			continue
		}
		if _, ok := noted[ev.Detail.TicketID]; ok {
			continue
		}
		noted[ev.Detail.TicketID] = struct{}{}
		creator := ev.ActorID
		if creator == "" {
			continue
		}
		text := fmt.Sprintf("故障 %s 已于 %s 恢复（持续 %s）。%s", inc.Number, formatOptTime(inc.ResolveTime), humanDuration(timeOf(inc.ResolveTime).Sub(inc.OccurTime)), inc.ResolveNote)
		if err := uc.d.Tickets.Comment(ctx, ev.Detail.TicketID, creator, truncate(text, 2000)); err != nil {
			if kratosCodeIs(err, 400) || kratosCodeIs(err, 403) || isNotFound(err) {
				uc.d.Logger.WarnContext(ctx, "incident: ticket note skipped", "ticket", ev.Detail.TicketID, "error", err)
				continue
			}
			return err
		}
	}
	return nil
}

// Purge 清理已终结的动作行。
func (uc *ActionUsecase) Purge(ctx context.Context) (int, error) {
	return uc.d.Actions.Purge(ctx, uc.now().Add(-actionRetention), 1000)
}

// PollProposals 轮询执行中的建议（作业 / 流程结束即回写）。
func (uc *ActionUsecase) PollProposals(ctx context.Context) (int, error) {
	items, err := uc.d.Proposals.ListRunning(ctx, proposalPollBatch)
	if err != nil {
		return 0, err
	}
	done := 0
	for _, p := range items {
		if err := uc.proposals.poll(ctx, p); err != nil {
			uc.d.Logger.WarnContext(ctx, "incident: poll proposal", "proposal", p.ID, "error", err)
			continue
		}
		done++
	}
	return done, nil
}
