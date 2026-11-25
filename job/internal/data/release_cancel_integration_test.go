//go:build integration

package data

import (
	"testing"

	"github.com/lanyulei/reborn/backend/kit/jobcontract"
)

// 引擎把执行放回队列时按 cancel_requested 分流：领取之后被请求取消的执行直接落为「已取消」（带结束时间，
// 满足 ck_job_execution_finish），不回到队列成为永远不会被领取的僵尸；互斥位随终态释放。
// 未请求取消的执行照常恢复成领取前的样子。
func TestReleaseSettlesCancelRequestedAsCanceled(t *testing.T) {
	e := newEnv(t)
	runnerNone := e.runner("")
	tpl := e.id()
	canceled := e.execution(execSpec{Exclusive: true, Template: tpl})

	token := e.claim(runnerNone, "", canceled)
	// core 对执行中的执行点取消：只登记请求，状态不变。
	e.exec(`UPDATE job_execution SET cancel_requested = true WHERE id = $1`, canceled)
	if err := e.repo.Release(ctxb(), canceled, token); err != nil {
		t.Fatal(err)
	}
	if r := e.row(canceled); r.Status != jobcontract.StatusCanceled || !r.Finished || r.Started || r.RunnerID != "" ||
		r.LeaseToken != "" || r.Leased || r.Error != "" {
		t.Fatalf("after release with cancel requested: %+v", r)
	}
	// 令牌围栏：已结清的领取不能再被撤销（令牌不对、令牌已随撤销失效，结果一样）。
	for _, tok := range []string{"wrong-token", token} {
		if err := e.repo.Release(ctxb(), canceled, tok); err != nil {
			t.Fatal(err)
		}
	}
	if r := e.row(canceled); r.Status != jobcontract.StatusCanceled || !r.Finished {
		t.Fatalf("a fenced release changed the row: %+v", r)
	}
	// 已取消：不再被领取。
	e.claim(runnerNone, "", "")
	// 终态不占互斥位：同一模板又可以建禁止并发的执行（仍占位会撞 uk_job_execution_exclusive）。
	plain := e.execution(execSpec{Exclusive: true, Template: tpl})

	// 未请求取消的执行：恢复成领取前的样子，可以再次被领取。
	token = e.claim(runnerNone, "", plain)
	if err := e.repo.Release(ctxb(), plain, token); err != nil {
		t.Fatal(err)
	}
	if r := e.row(plain); r.Status != jobcontract.StatusQueued || r.Finished || r.Started || r.RunnerID != "" ||
		r.LeaseToken != "" || r.Leased {
		t.Fatalf("after release: %+v", r)
	}
	e.claim(runnerNone, "", plain)
}
