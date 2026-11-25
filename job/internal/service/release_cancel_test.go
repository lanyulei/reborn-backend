package service

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/job/internal/biz"
	"github.com/lanyulei/reborn/backend/job/internal/biztest"
	"github.com/lanyulei/reborn/backend/kit/jobcontract"
)

// 领取之后被请求取消、执行又被引擎放回（应答没送达节点 → Unclaim；组装任务时的瞬时故障 → Release 同理）：
// 落为已取消。放回队列的话它带着 cancel_requested——ClaimNext 不再领取它，租约清扫只看执行中的行，
// core 也要用户再点一次取消才会改它——执行就永远停在「排队中」。
func TestReleaseAfterCancelRequestSettlesAsCanceled(t *testing.T) {
	f := newFixture(t)
	_, token := f.runner("", true)
	victim := f.execution("", nil)
	other := f.execution("", nil)
	first, second := f.claim(token), f.claim(token)
	if first == nil || first.ExecutionID != victim || second == nil || second.ExecutionID != other {
		t.Fatalf("claimed %+v, %+v; want %s then %s", first, second, victim, other)
	}
	// 节点拿到应答之前用户点了取消：core 只登记请求，状态仍是执行中。
	f.store.Update(victim, func(e *biztest.Execution) { e.CancelRequested = true })

	f.uc.Unclaim(context.Background(), victim, first.LeaseToken)

	e := f.store.Get(victim)
	if e.Status != jobcontract.StatusCanceled || !e.Finished || e.Started || e.RunnerID != "" || e.LeaseToken != "" ||
		!e.LeaseUntil.IsZero() || e.Error != "" || !e.CancelRequested {
		t.Fatalf("after unclaim: %+v", e)
	}
	// 未请求取消的执行不受影响：仍是执行中。
	if e := f.store.Get(other); e.Status != jobcontract.StatusRunning || e.LeaseToken != second.LeaseToken {
		t.Fatalf("the other execution changed: %+v", e)
	}
	// 已取消的执行不会再被领取。
	if again := f.claim(token); again != nil {
		t.Fatalf("canceled execution %s claimed again", again.ExecutionID)
	}
	// 租约清扫把它当成已结束：不会被改成「结果未知」（对照组：仍在执行中的那条照常被清扫）。
	f.store.Advance(24 * time.Hour)
	if n, err := f.house.SweepLeases(context.Background()); err != nil || n != 1 {
		t.Fatalf("sweep = %d, %v; want exactly the other execution", n, err)
	}
	if e := f.store.Get(victim); e.Status != jobcontract.StatusCanceled || e.Error != "" {
		t.Fatalf("sweep changed the canceled execution: %+v", e)
	}
	// 令牌围栏：这次领取已经结清，旧令牌不能再改它。
	f.uc.Unclaim(context.Background(), victim, first.LeaseToken)
	if e := f.store.Get(victim); e.Status != jobcontract.StatusCanceled || e.Error != "" {
		t.Fatalf("a fenced unclaim changed the row: %+v", e)
	}
}

// cancelOnClaim 领取成功后立刻登记取消请求：模拟「领取与组装任务之间用户点了取消」。
type cancelOnClaim struct {
	biz.QueueRepo
	store *biztest.Store
}

func (q cancelOnClaim) ClaimNext(ctx context.Context, runnerID, zone, leaseToken string, lease time.Duration) (*biz.Claimed, error) {
	c, err := q.QueueRepo.ClaimNext(ctx, runnerID, zone, leaseToken, lease)
	if c != nil {
		q.store.Update(c.ID, func(e *biztest.Execution) { e.CancelRequested = true })
	}
	return c, err
}

// 组装任务遇到瞬时故障 → 放回队列：同样按 cancel_requested 分流，不留下带取消请求的「排队中」僵尸。
func TestClaimBuildFailureReleasesCancelRequestedAsCanceled(t *testing.T) {
	f := newFixture(t)
	uc := biz.NewRunnerUsecase(f.store, cancelOnClaim{QueueRepo: f.store, store: f.store}, f.store, f.store, f.store,
		f.opener, f.obs, slog.New(slog.NewTextHandler(io.Discard, nil)))
	id := f.execution("", nil)
	f.store.MaterialErr = errors.New("connection reset")

	if task, err := uc.Claim(context.Background(), &biz.Runner{ID: "r1", Enabled: true}); err == nil || task != nil {
		t.Fatalf("claim = %+v, %v; want the transient failure", task, err)
	}
	e := f.store.Get(id)
	if e.Status != jobcontract.StatusCanceled || !e.Finished || e.RunnerID != "" || e.LeaseToken != "" || e.Started {
		t.Fatalf("after the failed build: %+v", e)
	}
}
