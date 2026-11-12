package biz

import "github.com/google/wire"

// ProviderSet is cmdb 域 biz providers。
var ProviderSet = wire.NewSet(NewModelUsecase, NewCIUsecase, NewRelationUsecase, NewScopeUsecase, NewSyncUsecase, NewInsightUsecase)
