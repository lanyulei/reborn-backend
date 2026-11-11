package data

import (
	"context"

	"github.com/lanyulei/reborn/backend/kit/dbx"

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

// TryLock implements biz.Locker（kit/dbx.TryAdvisoryLock：专用连接持有会话级锁）。
func (l *locker) TryLock(ctx context.Context, key string) (func(), bool, error) {
	return dbx.TryAdvisoryLock(ctx, l.data.Pool, key)
}
