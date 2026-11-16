package biz

import "github.com/google/wire"

// ProviderSet is file 域 biz providers。AccessCheckers 由顶层 biz 聚合处装配
// （各业务域把自己的检查器登记进去）。
var ProviderSet = wire.NewSet(NewUsecase)
