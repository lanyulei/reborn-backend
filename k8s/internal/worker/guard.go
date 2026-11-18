package worker

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/lanyulei/reborn/backend/kit/loop"
)

// guarded 同 loop.New，但每一轮都带 panic 恢复：后台循环处理的是集群返回的、形态不受控的数据，
// 一次意外的 panic 不应带走整个进程（连同其上全部集群的连接、终端与日志流）。panic 记为这一轮失败，
// 循环按失败退避后继续。
func guarded(name string, interval time.Duration, logger *slog.Logger, rec loop.Recorder, fn func(ctx context.Context) (int, error)) *loop.Loop {
	return loop.New(name, interval, logger, rec, func(ctx context.Context) (n int, err error) {
		defer func() {
			if p := recover(); p != nil {
				logger.Error("worker loop panicked", "loop", name, "panic", p, "stack", string(debug.Stack()))
				n, err = 0, fmt.Errorf("loop %s panicked: %v", name, p)
			}
		}()
		return fn(ctx)
	})
}
