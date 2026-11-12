package server

import (
	"github.com/lanyulei/reborn/backend/notify/internal/biz"
	"github.com/lanyulei/reborn/backend/notify/internal/service"
	"github.com/lanyulei/reborn/backend/notify/internal/worker"
)

// NewHooks 把指标计数器交给 worker 循环；sender 装配后回注熔断视图。
func NewHooks(m *Metrics) *worker.Hooks {
	return &worker.Hooks{Runs: m.LoopRuns, Items: m.LoopItems, Observer: m, OnBreaker: m.RegisterBreaker}
}

// NewIngressObserver 接入事件计数器（service 层经 biz 接口消费，避免依赖 server）。
func NewIngressObserver(m *Metrics) biz.IngressObserver { return m }

// NewProducerResolver service 层取生产者的解析器（避免 service 依赖 server）。
func NewProducerResolver() service.ProducerResolver { return ProducerFromContext }
