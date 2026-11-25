package biz

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/promclient"
	"github.com/lanyulei/reborn/backend/monitor/internal/promtest"
)

// panicRepo 发出固定的一批任务并记下归还。
type panicRepo struct {
	TaskRepo
	mu       sync.Mutex
	due      []*Task
	released []released
	tokens   []string
}

func (r *panicRepo) Claim(context.Context, string, int, time.Duration, time.Duration, time.Duration) ([]*Task, error) {
	return r.due, nil
}

func (r *panicRepo) Release(_ context.Context, token string, tasks []*Task, delay time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, t := range tasks {
		r.released = append(r.released, released{t, delay})
		r.tokens = append(r.tokens, token)
	}
	return nil
}

// 一个任务 panic 不得带垮进程，也不得悬在租约里（monitor-module-biz 优化项 2）：记 ERROR（规则 / 数据源 / 堆栈），
// 按引擎自身故障的既有路径归还并在 retryDelay 后重试；同批其余任务照常跑完。
func TestClaimAndRunRecoversPanickingTask(t *testing.T) {
	bad := &Task{RuleID: "rule-bad", DatasourceID: "ds-1", Token: "tok"}
	good := &Task{RuleID: "rule-good", DatasourceID: "ds-2", Token: "tok"}
	repo := &panicRepo{due: []*Task{bad, good}}
	log, logs := captureLogger()
	var ran atomic.Int32
	n, err := claimAndRun(context.Background(), repo, "tok",
		EvaluatorOptions{Batch: 2, Lease: time.Minute, QueryTimeoutMax: promclient.MaxTimeout}, log,
		func(_ context.Context, task *Task) {
			if task == bad {
				panic(errors.New("corrupted rule state"))
			}
			ran.Add(1)
		})
	if err != nil || n != 2 {
		t.Fatalf("claimAndRun() = %d, %v", n, err)
	}
	if ran.Load() != 1 {
		t.Fatalf("the healthy task of the batch must still run, ran = %d", ran.Load())
	}
	if len(repo.released) != 1 || repo.released[0].task != bad || repo.released[0].delay != retryDelay || repo.tokens[0] != "tok" {
		t.Fatalf("the panicking task must be released for a delayed retry, released = %+v", repo.released)
	}
	out := logs.String()
	for _, want := range []string{"level=ERROR", "task panicked", "rule_id=rule-bad", "datasource_id=ds-1", "corrupted rule state", "panic_recover_test.go"} {
		if !strings.Contains(out, want) {
			t.Fatalf("log must contain %q, got:\n%s", want, out)
		}
	}
}

// 没有 panic 时 recoverTask 什么都不做：不归还、不记日志（与加兜底之前完全一致）。
func TestClaimAndRunWithoutPanicReleasesNothing(t *testing.T) {
	repo := &panicRepo{due: []*Task{{RuleID: "r", DatasourceID: "d", Token: "tok"}}}
	log, logs := captureLogger()
	if n, err := claimAndRun(context.Background(), repo, "tok",
		EvaluatorOptions{Batch: 1, Lease: time.Minute, QueryTimeoutMax: promclient.MaxTimeout}, log,
		func(context.Context, *Task) {}); err != nil || n != 1 {
		t.Fatalf("claimAndRun() = %d, %v", n, err)
	}
	if len(repo.released) != 0 || logs.String() != "" {
		t.Fatalf("released = %+v, logs = %q", repo.released, logs.String())
	}
}

// 评估链路里的 panic（这里用生成事件 ID 的钩子触发）走同一条兜底：任务归还、进程存活、下一轮可正常评估。
func TestEvaluatorSurvivesPanicInEvaluation(t *testing.T) {
	h := newHarness(t, baseRule(func(r *Rule) { r.For = 0 }), nil)
	h.prom.Set("cpu > 85", promtest.S(91, "instance", "a"))
	newID := h.uc.newID
	h.uc.newID = func() (string, error) { panic("bad annotations") }
	h.run(at(0))
	if len(h.store.released) != 1 || h.store.released[0].delay != retryDelay || len(h.store.rounds) != 0 {
		t.Fatalf("released = %+v, rounds = %d", h.store.released, len(h.store.rounds))
	}
	if out := h.logs.String(); !strings.Contains(out, "task panicked") || !strings.Contains(out, "bad annotations") || !strings.Contains(out, "rule_id="+testRuleID) {
		t.Fatalf("logs = %s", out)
	}
	h.uc.newID = newID
	h.run(at(1))
	if len(h.store.rounds) != 1 {
		t.Fatalf("the task must be evaluable again after the panic, rounds = %d", len(h.store.rounds))
	}
}

// 并发查询的子 goroutine 里 panic：等全部返回后回到调用方 goroutine 上重新抛出，并带上原始堆栈。
func TestFanOutRethrowsOnTheCallerGoroutine(t *testing.T) {
	var ran atomic.Int32
	got := func() (v any) {
		defer func() { v = recover() }()
		fanOut(3, func(i int) {
			if i == 1 {
				panic("query blew up")
			}
			ran.Add(1)
		})
		return nil
	}()
	msg, _ := got.(string)
	if !strings.Contains(msg, "query blew up") || !strings.Contains(msg, "panic_recover_test.go") {
		t.Fatalf("recovered = %v", got)
	}
	if ran.Load() != 2 {
		t.Fatalf("the other queries must finish before the panic is rethrown, ran = %d", ran.Load())
	}
	// 没有 panic 时原样返回。
	fanOut(2, func(int) { ran.Add(1) })
	if ran.Load() != 4 {
		t.Fatalf("ran = %d", ran.Load())
	}
}
