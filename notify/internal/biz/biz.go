// Package biz 承载管道服务的业务核心：领域模型、仓储/骨干接口（依赖倒置）与
// 用例（router / sweeper / sender / dlq / maintenance / ingress）。本包禁止
// import 传输层与任何存储驱动（depguard 构建期强制，见 AGENTS.md §1）。
//
// 与 core 的契约：配置表（notify_channel/template/route/silence/subscription/
// producer）由 core 写、本服务只读；运行表（notify_event/dispatch/
// delivery_log/dlq）由本服务写、core 只读。字段编码与状态机取值与
// backend/core/internal/biz/notify 保持一致（设计 §5）。
package biz

import "github.com/google/wire"

// ProviderSet is biz providers.
var ProviderSet = wire.NewSet(
	NewConfigCache,
	NewRenderer,
	NewRouterUsecase,
	NewSweeperUsecase,
	NewSenderUsecase,
	NewDlqUsecase,
	NewMaintenanceUsecase,
	NewIngressUsecase,
	NewProducerAuth,
)
