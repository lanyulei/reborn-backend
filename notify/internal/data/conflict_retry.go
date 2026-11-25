package data

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// 死锁（40P01）与序列化失败（40001）后的重做。
//
// 为什么需要：NATS 骨干下，relay 的「标记已发布」与 sender 的「声明任务」都是对同一批任务行的多行更新——
// relay 把一批消息发进流之后才落账，而消费侧可能抢在这条落账语句之前就把消息取走并声明这些行。两条语句
// 各按自己的行序加锁，对同一批行顺序相反时就互相等待，数据库挑一个作为牺牲者回滚。此前任何一方被挑中，
// 整个循环都要带着错误退避一轮（最长 30 秒），消息也要等重投；实际上只差一次重做：
// 牺牲者那条语句已经被整体回滚，没有留下半截状态。
//
// 只适用于「单条自动提交的语句」或「自己保证原子性的整段操作」，并且重做必须幂等（带围栏的条件更新）。
// 重做不保证不再撞上，所以有次数上限；连续失败后照常把错误交给调用方，走原有的退避与兜底。
const (
	// conflictRetries 首次之外最多重做的次数。
	conflictRetries = 3
	// conflictBackoff 第一次重做前的等待，之后逐次翻倍（10ms / 20ms / 40ms）。
	conflictBackoff = 10 * time.Millisecond
)

// SQLSTATE：死锁 / 序列化失败。
const (
	sqlstateDeadlockDetected  = "40P01"
	sqlstateSerializationFail = "40001"
)

// isConflict 报告 err 是否为死锁或序列化失败（沿错误链查找）。
func isConflict(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && (pe.Code == sqlstateDeadlockDetected || pe.Code == sqlstateSerializationFail)
}

// retryConflict 执行 fn，遇到死锁 / 序列化失败时短暂退避后重做，至多 conflictRetries 次；其它错误原样返回。
// fn 必须把「执行」与「读完结果」都包含在内：带 RETURNING 的语句可能在已经返回一部分行之后才报出死锁，
// 只在发起语句处判断会漏掉。ctx 结束时返回最后一次的错误，不让调用方把中断当成成功。
func retryConflict[T any](ctx context.Context, fn func() (T, error)) (T, error) {
	var zero T
	for attempt := 0; ; attempt++ {
		out, err := fn()
		if err == nil || !isConflict(err) || attempt >= conflictRetries {
			return out, err
		}
		timer := time.NewTimer(conflictBackoff << uint(attempt))
		select {
		case <-ctx.Done():
			timer.Stop()
			return zero, err
		case <-timer.C:
		}
	}
}

// execConflictRetry 在连接池上执行一条语句，遇到死锁 / 序列化失败时重做（见 retryConflict）。
func execConflictRetry(ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) (pgconn.CommandTag, error) {
	return retryConflict(ctx, func() (pgconn.CommandTag, error) { return pool.Exec(ctx, sql, args...) })
}
