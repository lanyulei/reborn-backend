// Package loop 把一个"处理一批、返回条数"的函数包装为 kratos 托管的后台循环
// （transport.Server）：忙则连续、闲则按周期轮询、出错指数退避、Stop 取消并等待在途
// 迭代结束。无裸 goroutine：每个循环恰一个 goroutine，由 kratos App 生命周期管理。
// 另提供 Leader：持 PG advisory lock 者执行，未持锁实例每 tick 尝试获取。
package loop

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Recorder 每轮迭代回调（指标钩子）；nil 安全。
type Recorder interface {
	Record(ctx context.Context, loop string, items int, err error)
}

// 退避参数：出错后 interval × 2^min(failures,5)，封顶 30s。
const (
	defaultInterval = 200 * time.Millisecond
	maxBackoff      = 30 * time.Second
	maxBackoffShift = 5
)

// Loop 周期循环：fn 返回处理条数；>0 立即进入下一轮（忙），否则等待 interval。
type Loop struct {
	name     string
	interval time.Duration
	fn       func(ctx context.Context) (int, error)
	log      *slog.Logger
	rec      Recorder

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

// New new a Loop（interval ≤0 取 200ms）。
func New(name string, interval time.Duration, log *slog.Logger, rec Recorder, fn func(ctx context.Context) (int, error)) *Loop {
	if interval <= 0 {
		interval = defaultInterval
	}
	return &Loop{name: name, interval: interval, fn: fn, log: log, rec: rec}
}

// Name 循环名。
func (l *Loop) Name() string { return l.name }

// Start implements transport.Server：阻塞运行直至 Stop。
func (l *Loop) Start(ctx context.Context) error {
	l.mu.Lock()
	ctx, l.cancel = context.WithCancel(context.WithoutCancel(ctx))
	l.done = make(chan struct{})
	done := l.done
	l.mu.Unlock()
	defer close(done)
	l.log.Info("worker loop started", "loop", l.name, "interval", l.interval)
	failures := 0
	for {
		n, err := l.fn(ctx)
		if l.rec != nil {
			l.rec.Record(ctx, l.name, n, err)
		}
		wait := l.interval
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			failures++
			wait = min(l.interval*time.Duration(1<<min(failures, maxBackoffShift)), maxBackoff)
			l.log.ErrorContext(ctx, "worker loop iteration failed", "loop", l.name, "error", err, "retry_in", wait)
		} else {
			failures = 0
			if n > 0 {
				wait = 0
			}
		}
		if wait == 0 {
			if ctx.Err() != nil {
				return nil
			}
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
	}
}

// Stop implements transport.Server：取消并等待在途迭代完成（受 ctx 限时）。
func (l *Loop) Stop(ctx context.Context) error {
	l.mu.Lock()
	cancel, done := l.cancel, l.done
	l.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
	case <-ctx.Done():
		l.log.Warn("worker loop stop timed out", "loop", l.name)
	}
	l.log.Info("worker loop stopped", "loop", l.name)
	return nil
}

// Locker 分布式互斥（PG advisory lock，专用连接会话级持有）。
type Locker interface {
	// TryLock 非阻塞尝试；ok=false 表示他人持有。release 释放锁与连接。
	TryLock(ctx context.Context, key string) (release func(), ok bool, err error)
}

// LockProbe 是持锁连接的健康探针槽位：Leader 经 ctx（WithLockProbe）把它
// 交给 Locker 实现，实现方在成功获取锁后填入 Check（对持锁连接做一次往返）。
// 走 ctx 而非扩展 Locker 签名：既有实现无需改动，未填 Check 的实现退化为
// 不探活（旧行为）。
type LockProbe struct {
	// Check 探测持锁连接是否存活；返回错误即视为锁已随会话丢失。
	Check func(ctx context.Context) error
}

type lockProbeKey struct{}

// WithLockProbe 返回携带探针槽位的 ctx。
func WithLockProbe(ctx context.Context, p *LockProbe) context.Context {
	return context.WithValue(ctx, lockProbeKey{}, p)
}

// LockProbeFromContext 取出探针槽位；未携带返回 nil。
func LockProbeFromContext(ctx context.Context) *LockProbe {
	p, _ := ctx.Value(lockProbeKey{}).(*LockProbe)
	return p
}

// lockCheckTimeout 单次持锁连接探活的超时。
const lockCheckTimeout = 3 * time.Second

// Leader 选主执行器：持锁者执行 fn；未持锁则每次调用尝试获取；持锁后由专用连接
// 保持会话，每次执行前探活该连接——连接断即失主（PG 已随会话释放锁，他人可能
// 已接任），本实例放弃领导权并重新竞争；fn 出错同样释放并重新竞争。
// 非并发安全：由单个 Loop 驱动。
type Leader struct {
	key     string
	locker  Locker
	log     *slog.Logger
	release func()
	check   func(ctx context.Context) error
	held    bool
}

// NewLeader new a Leader.
func NewLeader(key string, locker Locker, log *slog.Logger) *Leader {
	return &Leader{key: key, locker: locker, log: log}
}

// Run 尝试以主身份执行 fn；未持锁返回 (0, nil)。
func (ld *Leader) Run(ctx context.Context, fn func(ctx context.Context) (int, error)) (int, error) {
	if ld.held && ld.check != nil {
		cctx, cancel := context.WithTimeout(ctx, lockCheckTimeout)
		err := ld.check(cctx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
			ld.log.Warn("leader lock connection lost, dropping leadership", "key", ld.key, "error", err)
			ld.Release()
		}
	}
	if !ld.held {
		probe := &LockProbe{}
		release, ok, err := ld.locker.TryLock(WithLockProbe(ctx, probe), ld.key)
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, nil
		}
		ld.release, ld.check, ld.held = release, probe.Check, true
		ld.log.Info("leader lock acquired", "key", ld.key)
	}
	n, err := fn(ctx)
	if err != nil {
		ld.Release()
	}
	return n, err
}

// Release 释放锁（幂等）。
func (ld *Leader) Release() {
	if ld.held && ld.release != nil {
		ld.release()
		ld.log.Info("leader lock released", "key", ld.key)
	}
	ld.held, ld.release, ld.check = false, nil, nil
}
