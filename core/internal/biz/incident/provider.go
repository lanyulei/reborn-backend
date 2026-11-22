package biz

import "github.com/google/wire"

// ProviderSet is 故障管理域 biz providers。Deps 由 wire.Struct 装配：仓储来自 data 层，跨域窄接口来自组合根
// （internal/biz/incident_adapters.go）。
var ProviderSet = wire.NewSet(
	wire.Struct(new(Deps), "*"),
	NewSettingsUsecase, NewIncidentUsecase, NewBusinessUsecase, NewRuleUsecase, NewPlaybookUsecase, NewProposalUsecase,
	NewUpdateUsecase, NewPostmortemUsecase, NewSourceUsecase, NewOpenUsecase, NewWebhookUsecase, NewCorrelatorUsecase,
	NewActionUsecase, NewInspectorUsecase, NewStatsUsecase, NewFileAccess,
)
