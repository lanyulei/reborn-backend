package data

import (
	"context"
	"fmt"
	"hash/fnv"

	"github.com/lanyulei/reborn/backend/notify/internal/biz"
)

var _ biz.Locker = (*locker)(nil)

// locker PG advisory lock 分布式互斥：会话级锁须绑定专用连接——Acquire 后 Hijack
// 脱离连接池，释放时解锁并关闭连接（评审 #19：连接断 = 失主，PG 自动释放，无需 TTL）。
type locker struct {
	data *Data
}

// NewLocker new a locker.
func NewLocker(data *Data) *locker { return &locker{data: data} }

// TryLock implements biz.Locker.
func (l *locker) TryLock(ctx context.Context, key string) (func(), bool, error) {
	conn, err := l.data.Pool.Acquire(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("acquire lock conn: %w", err)
	}
	id := lockID(key)
	var ok bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", id).Scan(&ok); err != nil {
		conn.Release()
		return nil, false, fmt.Errorf("try advisory lock: %w", err)
	}
	if !ok {
		conn.Release()
		return nil, false, nil
	}
	raw := conn.Hijack()
	release := func() {
		// 解锁失败无妨：关闭会话即释放全部会话级锁。
		_, _ = raw.Exec(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", id)
		_ = raw.Close(context.WithoutCancel(ctx))
	}
	return release, true, nil
}

func lockID(key string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return int64(h.Sum64()) //nolint:gosec // 锁键哈希，溢出翻转无害
}
