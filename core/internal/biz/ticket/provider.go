package biz

import "github.com/google/wire"

// ProviderSet is ticket 域 biz providers。流程 / 附件 / 组织目录 / 管理视图判定等端口
// 由其他域实现，在顶层 biz 聚合处绑定。
var ProviderSet = wire.NewSet(NewCatalogUsecase, NewUsecase, NewAccess, NewStatusSyncer, NewExtensions)
