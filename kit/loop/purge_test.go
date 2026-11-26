package loop

import (
	"context"
	"errors"
	"testing"
	"time"
)

// scripted 返回一个按给定序列回报删除条数的 step；序列用完后一直回报 0（已清完）。
func scripted(counts ...int) func(context.Context) (int, error) {
	return func(context.Context) (int, error) {
		if len(counts) == 0 {
			return 0, nil
		}
		n := counts[0]
		counts = counts[1:]
		return n, nil
	}
}

// 删满一批且预算没用完才发下一条；不足一批（已清完）即停。返回累计条数。
func TestPurgeBatched_StopsWhenABatchIsNotFull(t *testing.T) {
	t.Parallel()
	o := PurgeOptions{Batch: 100, MaxStatements: 10, Budget: time.Minute}
	for name, tc := range map[string]struct {
		counts []int
		want   int
	}{
		"nothing to purge":               {[]int{0}, 0},
		"one full batch then done":       {[]int{100, 40}, 140},
		"several full batches then done": {[]int{100, 100, 100, 1}, 301},
	} {
		if total, err := PurgeBatched(context.Background(), o, scripted(tc.counts...)); err != nil || total != tc.want {
			t.Errorf("%s: PurgeBatched() = %d, %v; want %d, nil", name, total, err, tc.want)
		}
	}
}

// 语句条数用尽即停：没删完的留给下一轮，不算错误。
func TestPurgeBatched_StopsAtTheStatementLimit(t *testing.T) {
	t.Parallel()
	o := PurgeOptions{Batch: 100, MaxStatements: 3, Budget: time.Minute}
	total, err := PurgeBatched(context.Background(), o, scripted(100, 100, 100, 100))
	if err != nil || total != 300 {
		t.Fatalf("PurgeBatched() = %d, %v; want 300 (three statements) and nil", total, err)
	}
}

// 时间预算用尽即停。
func TestPurgeBatched_StopsWhenTheBudgetIsSpent(t *testing.T) {
	t.Parallel()
	o := PurgeOptions{Batch: 1, MaxStatements: 1000, Budget: 20 * time.Millisecond}
	total, err := PurgeBatched(context.Background(), o, func(context.Context) (int, error) {
		time.Sleep(5 * time.Millisecond)
		return 1, nil
	})
	if err != nil || total <= 0 || total >= 1000 {
		t.Fatalf("PurgeBatched() = %d, %v; want it to stop part-way once the budget is spent, without error", total, err)
	}
}

// ctx 结束：原样返回 ctx 的错误，连同出错前已删的条数。
func TestPurgeBatched_ReturnsTheContextError(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	o := PurgeOptions{Batch: 1, MaxStatements: 100, Budget: time.Minute}
	total, err := PurgeBatched(ctx, o, func(context.Context) (int, error) {
		cancel()
		return 1, nil
	})
	if !errors.Is(err, context.Canceled) || total != 1 {
		t.Fatalf("PurgeBatched() = %d, %v; want 1 and context.Canceled", total, err)
	}
}

// step 出错：立即返回该错误，连同出错前已删的条数；后面的 step 不再执行。
func TestPurgeBatched_ReturnsTheStepError(t *testing.T) {
	t.Parallel()
	boom := errors.New("boom")
	o := PurgeOptions{Batch: 100, MaxStatements: 10, Budget: time.Minute}
	calls := 0
	total, err := PurgeBatched(context.Background(), o, scripted(100), func(context.Context) (int, error) {
		calls++
		return 0, boom
	})
	if !errors.Is(err, boom) || total != 100 || calls != 1 {
		t.Fatalf("PurgeBatched() = %d, %v after %d calls; want 100, boom, and exactly 1 call", total, err, calls)
	}
}

// 多个 step 轮流执行：某个 step 清完后这一轮里不再执行它；排在它后面的 step 因为它又删了行而重新参与。
// 场景取自同步记录的清理：任务只在它指向的运行记录删掉之后才可删。
func TestPurgeBatched_StepsTakeTurnsAndRevive(t *testing.T) {
	t.Parallel()
	const batch = 10
	o := PurgeOptions{Batch: batch, MaxStatements: 20, Budget: time.Minute}
	runsRemaining, tasksRemaining := 2*batch+3, 2*batch
	var calls [2]int
	total, err := PurgeBatched(context.Background(), o,
		func(context.Context) (int, error) {
			calls[0]++
			n := min(runsRemaining, batch)
			runsRemaining -= n
			return n, nil
		},
		func(context.Context) (int, error) {
			calls[1]++
			// 可删的任务数不超过已删掉的运行记录数。
			n := min(tasksRemaining, 2*batch+3-runsRemaining, batch)
			tasksRemaining -= n
			return n, nil
		})
	if err != nil || total != 2*batch+3+2*batch || runsRemaining != 0 || tasksRemaining != 0 {
		t.Fatalf("PurgeBatched() = %d, %v; runs left %d, tasks left %d; want everything purged", total, err, runsRemaining, tasksRemaining)
	}
	if calls[0] != 3 || calls[1] != 3 {
		t.Fatalf("step calls = %v; want runs 3 times (23 rows) and tasks 3 times (20 rows)", calls)
	}
}

// 非法的预算不会让循环停不下来：批大小不是正数按 1 算，语句条数不是正数或没有 step 时什么都不做，
// 时间预算不是正数时只发一条语句。
func TestPurgeBatched_ToleratesNonPositiveOptions(t *testing.T) {
	t.Parallel()
	if total, err := PurgeBatched(context.Background(), PurgeOptions{Batch: 0, MaxStatements: 0}, scripted(5)); err != nil || total != 0 {
		t.Fatalf("no statement budget: PurgeBatched() = %d, %v; want 0, nil", total, err)
	}
	if total, err := PurgeBatched(context.Background(), PurgeOptions{Batch: 10, MaxStatements: 5, Budget: time.Minute}); err != nil || total != 0 {
		t.Fatalf("no steps: PurgeBatched() = %d, %v; want 0, nil", total, err)
	}
	if total, err := PurgeBatched(context.Background(), PurgeOptions{Batch: -3, MaxStatements: 4, Budget: time.Minute}, scripted(5, 5, 5, 5, 5)); err != nil || total != 20 {
		t.Fatalf("negative batch: PurgeBatched() = %d, %v; want 20 (batch taken as 1, so every row counts as a full batch) and nil", total, err)
	}
	// 时间预算不是正数：只发得出一条语句。
	if total, err := PurgeBatched(context.Background(), PurgeOptions{Batch: 10, MaxStatements: 5}, scripted(10, 10, 10)); err != nil || total != 10 {
		t.Fatalf("no time budget: PurgeBatched() = %d, %v; want 10 (one statement) and nil", total, err)
	}
}
