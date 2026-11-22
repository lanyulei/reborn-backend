package biz

import "github.com/google/wire"

// ProviderSet is 运维 Agent 域 biz providers。Deps 由 wire.Struct 装配：仓储来自 data 层，跨域窄接口来自组合根
// （internal/biz/agent_adapters.go）。
var ProviderSet = wire.NewSet(
	wire.Struct(new(Deps), "*"),
	NewConfig, NewProfileUsecase, NewTaskUsecase, NewRunnerUsecase, NewStatsUsecase,
)
