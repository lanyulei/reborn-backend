package loop

import (
	"context"
	"time"
)

// PurgeOptions 一轮分批清理的预算。
type PurgeOptions struct {
	// Batch 每条语句至多删除的行数，也是「删满一批」的判定值。
	Batch int
	// MaxStatements 一轮至多发出的语句数，是时间预算之外的第二道闸（语句意外变慢时兜底）。
	MaxStatements int
	// Budget 一轮的时间预算，自进入 PurgeBatched 起算。不是正数时只发得出一条语句（第一条之后预算即已用尽）。
	Budget time.Duration
}

// PurgeBatched 轮流执行 steps（每个 step 一次删一批、返回删除条数）：删满一批且预算没用完才进入下一轮；某个 step
// 一旦不足一批，这一轮里就不再执行它（已清完）；所有 step 都清完、语句条数用尽、时间预算用尽、ctx 结束或出错即停。
//
// 给「保留期清理」用：首次启用（或长期停机后）历史积压可能很大，一轮只在预算之内分批删，剩下的留给后续轮次，
// 不在一轮里猛删；预算用尽不是错误。ctx 结束原样返回 ctx 的错误。返回已删除的条数（出错时也包含出错前已删的）。
// step 自身须幂等（多副本同时清理时后到者删到 0 行），本函数不加锁。
//
// steps 按依赖先后排列：排在后面的 step 只会因前面的 step 删了行而多出可删的行（例如任务在它的运行记录删掉之后
// 才可删），所以前面的 step 又删满一批时，后面已判「清完」的 step 重新参与，不必等到下一轮。
func PurgeBatched(ctx context.Context, o PurgeOptions, steps ...func(ctx context.Context) (int, error)) (int, error) {
	// 批大小不是正数时「不足一批」永远不成立，循环会停不下来：按 1 处理。
	batch := max(o.Batch, 1)
	if o.MaxStatements <= 0 || len(steps) == 0 {
		return 0, nil
	}
	deadline := time.Now().Add(o.Budget)
	done := make([]bool, len(steps))
	total, statements := 0, 0
	for statements < o.MaxStatements {
		cleared := true
		progress := -1
		for i, step := range steps {
			if done[i] {
				continue
			}
			if statements >= o.MaxStatements {
				cleared = false
				break
			}
			n, err := step(ctx)
			statements++
			total += n
			if err != nil {
				return total, err
			}
			done[i] = n < batch
			if done[i] {
				continue
			}
			cleared, progress = false, i
		}
		if cleared {
			return total, nil
		}
		if err := ctx.Err(); err != nil {
			return total, err
		}
		if time.Now().After(deadline) {
			return total, nil
		}
		if progress >= 0 {
			for j := progress + 1; j < len(steps); j++ {
				done[j] = false
			}
		}
	}
	return total, nil
}
