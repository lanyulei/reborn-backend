// Package worker 把 biz 用例包装为 kratos 托管的后台循环（transport.Server）：
// 角色门控在 server 装配层，本包只关心节奏（忙则连续、闲则按周期轮询）、
// 选主与优雅停机（Stop 取消并等待在途迭代结束）。无裸 goroutine：每个循环
// 恰一个 goroutine，由 kratos App 生命周期管理（AGENTS.md）。
package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/lanyulei/reborn/backend/notify/internal/biz"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Hooks 指标钩子（server.Metrics 实现；nil 安全）。
type Hooks struct {
	Runs     metric.Int64Counter
	Items    metric.Int64Counter
	Observer biz.SendObserver
}

func (h *Hooks) record(ctx context.Context, loop string, items int, err error) {
	if h == nil || h.Runs == nil {
		return
	}
	result := "ok"
	if err != nil {
		result = "error"
	}
	h.Runs.Add(ctx, 1, metric.WithAttributes(attribute.String("loop", loop), attribute.String("result", result)))
	if items > 0 && h.Items != nil {
		h.Items.Add(ctx, int64(items), metric.WithAttributes(attribute.String("loop", loop)))
	}
}

// Loop 周期循环：fn 返回处理条数；>0 立即进入下一轮（忙），否则等待 interval；
// 出错按 errorBackoff 退避（最大 10×interval，封顶 30s）。
type Loop struct {
	name     string
	interval time.Duration
	fn       func(ctx context.Context) (int, error)
	log      *slog.Logger
	hooks    *Hooks

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

// NewLoop new a Loop.
func NewLoop(name string, interval time.Duration, log *slog.Logger, hooks *Hooks, fn func(ctx context.Context) (int, error)) *Loop {
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	return &Loop{name: name, interval: interval, fn: fn, log: log, hooks: hooks}
}

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
		l.hooks.record(ctx, l.name, n, err)
		wait := l.interval
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			failures++
			wait = min(l.interval*time.Duration(1<<min(failures, 5)), 30*time.Second)
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

// leaderLoop 选主循环：持锁者执行 fn；未持锁则每 tick 尝试获取；持锁后用专用
// 连接保持会话——连接断即失主（下一次 fn 出错后重新竞争）。
type leaderLoop struct {
	key     string
	locker  biz.Locker
	log     *slog.Logger
	release func()
	held    bool
}

func (ll *leaderLoop) run(ctx context.Context, fn func(ctx context.Context) (int, error)) (int, error) {
	if !ll.held {
		release, ok, err := ll.locker.TryLock(ctx, ll.key)
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, nil
		}
		ll.release, ll.held = release, true
		ll.log.InfoContext(ctx, "leadership acquired", "lock", ll.key)
	}
	n, err := fn(ctx)
	if err != nil {
		// 任一错误都放弃领导权（可能是连接断），下轮重新竞争；fn 自身幂等。
		ll.drop(ctx)
	}
	if ctx.Err() != nil {
		ll.drop(ctx)
	}
	return n, err
}

func (ll *leaderLoop) drop(ctx context.Context) {
	if ll.held {
		ll.release()
		ll.held = false
		ll.log.InfoContext(ctx, "leadership released", "lock", ll.key)
	}
}
