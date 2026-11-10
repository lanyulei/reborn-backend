package server

import (
	"github.com/lanyulei/reborn/backend/notify/internal/service"
	"github.com/lanyulei/reborn/backend/notify/internal/worker"
)

// NewHooks 把指标计数器交给 worker 循环。
func NewHooks(m *Metrics) *worker.Hooks {
	return &worker.Hooks{Runs: m.LoopRuns, Items: m.LoopItems, Observer: m}
}

// NewProducerResolver service 层取生产者的解析器（避免 service 依赖 server）。
func NewProducerResolver() service.ProducerResolver { return ProducerFromContext }
