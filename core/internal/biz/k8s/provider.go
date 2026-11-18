package biz

import "github.com/google/wire"

// ProviderSet is 容器平台域 biz providers。跨域窄接口（RoleSource / Accounts / Directory）与通知事件
// 写入在组合根 internal/biz/biz.go 绑定。
var ProviderSet = wire.NewSet(NewClusterUsecase, NewScopeUsecase, NewAuditUsecase, NewAccessResolver, NewPlaneUsecase, NewDispatchUsecase, NewInsightUsecase, NewHelmUsecase, NewGovernanceUsecase, NewChangeDeps, NewChangeUsecase, NewChangeSyncer, NewChangeWorker)
