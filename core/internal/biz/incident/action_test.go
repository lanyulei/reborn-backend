package biz

import (
	"context"
	"testing"
	"time"
)

// 验证窗口未到：动作按剩余时间重排，不能被当作「执行成功」落账为已完成（否则验证永远不会发生）。
func TestActionVerifyReschedulesUntilWindowEnds(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	until := now.Add(10 * time.Minute)
	proposals := &fakeProposals{get: &Proposal{ID: "p1", IncidentID: "i1", Status: ProposalSucceeded, VerifyUntil: &until}}
	actions := &fakeActions{}
	d := &Deps{Txm: fakeTxm{}, Proposals: proposals, Actions: actions, Logger: testLogger()}
	uc := &ActionUsecase{d: d, now: func() time.Time { return now }}

	if err := uc.handle(context.Background(), &Action{ID: "act", IncidentID: "i1", Kind: ActionVerify, RefID: "p1"}); err != nil {
		t.Fatalf("handle() error = %v", err)
	}
	if len(actions.retries) != 1 || actions.retries[0] != 10*time.Minute+time.Second {
		t.Fatalf("retries = %v, want one retry of 10m1s", actions.retries)
	}
	if len(actions.finished) != 0 {
		t.Fatalf("action must not be finished while waiting for the verify window, got %v", actions.finished)
	}
}

// 建议已不在「成功待验证」状态：动作正常完成。
func TestActionVerifyFinishesWhenNothingToVerify(t *testing.T) {
	t.Parallel()
	checked := time.Now()
	proposals := &fakeProposals{get: &Proposal{ID: "p1", Status: ProposalSucceeded, VerifyResult: &VerifyResult{CheckedTime: checked}}}
	actions := &fakeActions{}
	d := &Deps{Txm: fakeTxm{}, Proposals: proposals, Actions: actions, Logger: testLogger()}
	uc := &ActionUsecase{d: d, now: time.Now}

	if err := uc.handle(context.Background(), &Action{ID: "act", Kind: ActionVerify, RefID: "p1"}); err != nil {
		t.Fatalf("handle() error = %v", err)
	}
	if len(actions.retries) != 0 || len(actions.finished) != 1 || actions.finished[0] != OutboxDone {
		t.Fatalf("retries = %v finished = %v, want no retry and one Done", actions.retries, actions.finished)
	}
}
