package service

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/job/internal/biz"
	"github.com/lanyulei/reborn/backend/job/internal/biztest"
	"github.com/lanyulei/reborn/backend/kit/jobcontract"
)

// 引擎启动后的一个租约之内不清扫：停机期间仍在跑的节点来得及续租，不会被整批误判为失联。
func TestSweepGraceAfterStart(t *testing.T) {
	f := newFixture(t)
	_, token := f.runner("", true)
	alive, dead := f.execution("", nil), f.execution("", nil)
	taskAlive := f.claim(token)
	f.claim(token)
	// 引擎停机很久：两条执行的租约都已过期。
	f.store.Advance(time.Hour)

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f.house.WithOptions(biz.HousekeepOptions{SweepGrace: time.Minute, Clock: func() time.Time { return now }})
	for _, elapsed := range []time.Duration{0, 30 * time.Second, 59 * time.Second} {
		now = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Add(elapsed)
		if n, err := f.house.SweepLeases(context.Background()); err != nil || n != 0 {
			t.Fatalf("swept %d executions %s after start, want none during the grace period (%v)", n, elapsed, err)
		}
	}
	// 宽限期内，仍在跑的那个节点上报了一次。
	if code, _ := f.report(token, &jobcontract.ReportRequest{ExecutionID: alive, LeaseToken: taskAlive.LeaseToken}); code != http.StatusOK {
		t.Fatalf("report: %d", code)
	}
	now = now.Add(2 * time.Second)
	if n, err := f.house.SweepLeases(context.Background()); err != nil || n != 1 {
		t.Fatalf("sweep after the grace period = %d, %v; want exactly the silent execution", n, err)
	}
	if f.store.Get(alive).Status != jobcontract.StatusRunning || f.store.Get(dead).Status != jobcontract.StatusUnknown {
		t.Fatalf("alive %d dead %d", f.store.Get(alive).Status, f.store.Get(dead).Status)
	}
}

// 恢复为执行中时清掉「已通知」标记：core 会按真正的终态再通知一次。
func TestReviveClearsNotified(t *testing.T) {
	f := newFixture(t)
	_, token := f.runner("", true)
	id := f.execution("", nil)
	task := f.claim(token)
	f.store.Advance(2 * time.Minute)
	if n, _ := f.house.SweepLeases(context.Background()); n != 1 {
		t.Fatal("sweep")
	}
	f.store.Update(id, func(e *biztest.Execution) { e.Notified = true })
	if code, _ := f.report(token, &jobcontract.ReportRequest{ExecutionID: id, LeaseToken: task.LeaseToken}); code != http.StatusOK {
		t.Fatalf("report: %d", code)
	}
	if e := f.store.Get(id); e.Status != jobcontract.StatusRunning || e.Notified {
		t.Fatalf("after revive: status %d notified %v", e.Status, e.Notified)
	}
}

// 队首的执行一直组装不出来：有限次之后置为失败，后面的执行得以派发；短暂的故障不受影响。
func TestClaimGivesUpOnPersistentBuildFailure(t *testing.T) {
	f := newFixture(t)
	_, token := f.runner("", true)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f.uc.WithOptions(biz.RunnerOptions{Clock: func() time.Time { return now }})
	poisoned := f.execution("", nil)
	next := f.execution("", nil)
	f.store.MaterialErr = errors.New("invalid byte sequence for encoding")

	claim := func() int {
		code, _ := f.post(jobcontract.RunnerPathClaim, token, nil)
		return code
	}
	// 一分钟之内失败再多次也只是放回队列（多个节点同时轮询时次数涨得很快）。
	for range 20 {
		if code := claim(); code != http.StatusInternalServerError {
			t.Fatalf("claim: %d, want 500", code)
		}
		now = now.Add(2 * time.Second)
	}
	if e := f.store.Get(poisoned); e.Status != jobcontract.StatusQueued || e.RunnerID != "" {
		t.Fatalf("within the first minute the execution must go back to the queue: %+v", e)
	}
	// 持续超过一分钟：置为失败（这一次请求仍报 500）。
	now = now.Add(30 * time.Second)
	if code := claim(); code != http.StatusInternalServerError {
		t.Fatalf("claim: %d", code)
	}
	e := f.store.Get(poisoned)
	if e.Status != jobcontract.StatusFailed || !e.Finished || !strings.Contains(e.Error, "已放弃派发") || strings.Contains(e.Error, "byte sequence") {
		t.Fatalf("poisoned execution: status %d error %q", e.Status, e.Error)
	}
	// 故障消失后，后面的执行照常派发。
	f.store.MaterialErr = nil
	if task := f.claim(token); task == nil || task.ExecutionID != next {
		t.Fatalf("claimed %+v, want %s", task, next)
	}

	// 短暂的故障：中途成功一次即清零，之后再失败重新计数。
	blip := f.execution("", nil)
	f.store.MaterialErr = errors.New("connection reset")
	for range 4 {
		claim()
		now = now.Add(time.Minute)
	}
	f.store.MaterialErr = nil
	if task := f.claim(token); task == nil || task.ExecutionID != blip {
		t.Fatalf("claimed %+v, want %s", task, blip)
	}
}
