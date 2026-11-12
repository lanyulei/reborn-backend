package biz

import (
	"context"
	"log/slog"
)

// Relayer 发布扫描器（设计 §4.3；仅 NATS 骨干装配）：把 PG 里未发布的事件/任务游标
// 发布到消息骨干并标记 publish_state=1，并把超期未处理的已发布行回拨重发。
type Relayer interface {
	// RelayOnce 一轮扫描；返回发布/回拨条数（0 = 空闲）。
	RelayOnce(ctx context.Context, batch int) (int, error)
}

// Backbone 骨干状态（/healthz）：Name = pg | nats；Ping 在 pg 下恒 nil。
type Backbone interface {
	Name() string
	Ping(ctx context.Context) error
}

// RelayUsecase relay 循环用例（pg 骨干下 Relayer 为 nil：循环不装配）。
type RelayUsecase struct {
	relayer Relayer
	log     *slog.Logger
}

// NewRelayUsecase new a RelayUsecase.
func NewRelayUsecase(relayer Relayer, logger *slog.Logger) *RelayUsecase {
	return &RelayUsecase{relayer: relayer, log: logger}
}

// Enabled 报告是否装配了骨干发布者。
func (uc *RelayUsecase) Enabled() bool { return uc.relayer != nil }

// RelayOnce 委托一轮。
func (uc *RelayUsecase) RelayOnce(ctx context.Context, batch int) (int, error) {
	if uc.relayer == nil {
		return 0, nil
	}
	return uc.relayer.RelayOnce(ctx, batch)
}
