// Package biz 容器平台数据面的业务核心：定义仓储与集群访问的窄接口、用例；不 import 传输层、
// 数据库驱动与 client-go（集群访问经 data 层实现的接口）。
package biz

import "github.com/google/wire"

// ProviderSet is biz providers.
var ProviderSet = wire.NewSet(NewRegistry, NewProber, NewScopeSyncer, NewAuditor, NewOps, NewExecManager, NewMaintenance, NewAgentKeeper, NewSummarizer, NewEventCollector, NewKubeGateway, NewFileTransfer, NewInspectionUsecase, NewChangeRunner)

// Pinger 数据库连通性检查（/healthz）。
type Pinger interface {
	Ping() error
}
