package biz

import (
	"context"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/promclient"
)

// claimingRepo 记下领取时用的租约余量；本身不发任务。
type claimingRepo struct {
	TaskRepo
	margin time.Duration
}

func (r *claimingRepo) Claim(_ context.Context, _ string, _ int, _, _, margin time.Duration) ([]*Task, error) {
	r.margin = margin
	return nil, nil
}

// 单轮评估的总时限不能超过任务租约（monitor 模块评审 P3-1）：数据源与数据库同时很慢时，评估还在自己的
// 总时限之内、租约却已到期被别的实例重领，本轮作废（ErrLeaseLost）、状态不推进。
//
//   - 租约（TaskRepo.Claim）= max(下限, roundQueryBudgets × 数据源查询超时 + leaseMargin)；
//   - 单轮总时限 = roundQueryBudgets × 查询超时上限 + settleTimeout。
//
// 两者共用 settleTimeout 作余量的基数；leaseMargin 另留出领取结果的回传与调度时间，必须严格大于 settleTimeout。
func TestLeaseMarginCoversTheRoundBudget(t *testing.T) {
	if leaseMargin <= settleTimeout {
		t.Fatalf("leaseMargin %v must exceed settleTimeout %v, or a slow round can outlive its lease", leaseMargin, settleTimeout)
	}
	for _, timeout := range []time.Duration{time.Second, 10 * time.Second, promclient.MaxTimeout} {
		lease := time.Duration(roundQueryBudgets)*timeout + leaseMargin
		budget := time.Duration(roundQueryBudgets)*timeout + settleTimeout
		if lease <= budget {
			t.Fatalf("lease %v must exceed the round budget %v (timeout %v)", lease, budget, timeout)
		}
	}
	// 评估任务与记录规则任务共用同一处领取：传给仓储的余量就是 leaseMargin。
	repo := &claimingRepo{}
	log, _ := captureLogger()
	if n, err := claimAndRun(context.Background(), repo, "token",
		EvaluatorOptions{Batch: 1, Lease: time.Minute, QueryTimeoutMax: promclient.MaxTimeout}, log, func(context.Context, *Task) {}); err != nil || n != 0 {
		t.Fatalf("claimAndRun() = %d, %v", n, err)
	}
	if repo.margin != leaseMargin {
		t.Fatalf("claim margin = %v, want %v", repo.margin, leaseMargin)
	}
}
