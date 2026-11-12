package service

import "github.com/google/wire"

// ProviderSet is cmdb 域 service providers。
var ProviderSet = wire.NewSet(NewCmdbModelService, NewCmdbCIService, NewCmdbRelationService, NewCmdbScopeService, NewCmdbSyncService, NewCmdbInsightService)
