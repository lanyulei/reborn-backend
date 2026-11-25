package biz

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

// ctxProposals 在 ctx 已取消时读取失败（模拟停机中的数据库调用）。
type ctxProposals struct{ *fakeProposals }

func (p *ctxProposals) Get(ctx context.Context, id string) (*Proposal, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return p.fakeProposals.Get(ctx, id)
}

func verifyActions(ids ...string) []*Action {
	items := make([]*Action, 0, len(ids))
	for _, id := range ids {
		items = append(items, &Action{ID: id, IncidentID: "i1", Kind: ActionVerify, RefID: "p1", Attempts: 1})
	}
	return items
}

// 一条动作落不了账（数据库错误）不再丢掉同批其余已领取的动作：后面的照常执行并落账，落不了账的那条租约立即
// 收回（下次可领取时间拉回现在，而不是等 5 分钟），首个错误返回给循环。
func TestActionRunContinuesAfterOneFailedRecord(t *testing.T) {
	t.Parallel()
	actions := &claimActions{items: verifyActions("a1", "a2", "a3"), failDone: map[string]error{"a1": errBoom}}
	proposals := &fakeProposals{get: &Proposal{ID: "p1", Status: ProposalSucceeded, VerifyResult: &VerifyResult{}}} // 无需验证：执行即完成
	d := &Deps{Txm: fakeTxm{}, Proposals: proposals, Actions: actions, Logger: testLogger()}
	uc := NewActionUsecase(d, nil, nil)

	n, err := uc.Run(context.Background())
	if !errors.Is(err, errBoom) {
		t.Fatalf("Run() error = %v, want the first failure errBoom", err)
	}
	if n != 2 || !slices.Equal(actions.finished, []string{"a2", "a3"}) {
		t.Fatalf("Run() = %d, finished %v; want 2 with a2 and a3 recorded", n, actions.finished)
	}
	if !slices.Equal(actions.retries["a1"], []time.Duration{0}) {
		t.Fatalf("retries of a1 = %v, want its lease released immediately", actions.retries["a1"])
	}
}

// 正在停机：执行被取消的动作不记失败（此前每次停机都消耗一次尝试，恰逢最后一次还会被直接丢弃），
// 整批的租约收回，重启后立刻可以重做。
func TestActionRunReleasesLeaseOnShutdown(t *testing.T) {
	t.Parallel()
	actions := &claimActions{items: verifyActions("a1", "a2", "a3")}
	proposals := &ctxProposals{fakeProposals: &fakeProposals{get: &Proposal{ID: "p1", Status: ProposalSucceeded}}}
	d := &Deps{Txm: fakeTxm{}, Proposals: proposals, Actions: actions, Logger: testLogger()}
	uc := NewActionUsecase(d, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	n, err := uc.Run(ctx)
	if !errors.Is(err, context.Canceled) || n != 0 {
		t.Fatalf("Run() = %d, %v; want 0 and the cancellation", n, err)
	}
	if len(actions.finished) != 0 {
		t.Fatalf("finished = %v, want nothing recorded while shutting down", actions.finished)
	}
	for _, id := range []string{"a1", "a2", "a3"} {
		if !slices.Equal(actions.retries[id], []time.Duration{0}) {
			t.Fatalf("retries of %s = %v, want only an immediate lease release (no failure, no backoff)", id, actions.retries[id])
		}
	}
}

// 全部成功：不收回任何租约，也不返回错误。
func TestActionRunAllSucceed(t *testing.T) {
	t.Parallel()
	actions := &claimActions{items: verifyActions("a1", "a2")}
	proposals := &fakeProposals{get: &Proposal{ID: "p1", Status: ProposalSucceeded, VerifyResult: &VerifyResult{}}}
	d := &Deps{Txm: fakeTxm{}, Proposals: proposals, Actions: actions, Logger: testLogger()}
	uc := NewActionUsecase(d, nil, nil)

	n, err := uc.Run(context.Background())
	if err != nil || n != 2 || len(actions.finished) != 2 || len(actions.retries) != 0 {
		t.Fatalf("Run() = %d, %v, finished %v, retries %v; want 2, nil, both recorded, no retries", n, err, actions.finished, actions.retries)
	}
}
