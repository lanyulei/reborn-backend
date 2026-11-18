package biz

import "github.com/google/wire"

// ProviderSet is 监控告警域 biz providers。跨域窄接口（RoleSource / RoleDirectory / Directory /
// DeptLeaders / Duty / Schedules / TargetSource / Permissions）在组合根 internal/biz/biz.go 绑定。
var ProviderSet = wire.NewSet(
	NewScopeUsecase, NewGroupUsecase, ProvideDatasourceUsecase, NewMuteUsecase, NewSubscriptionUsecase, NewTargetUsecase,
	// 第二、三期：各功能的 provider 集定义在各自的文件里。规则 / 告警 / 派发 / 数据源用例改由 Provide* 装配
	// （在第一期构造函数之上接入第二、三期的协作者）。
	AlertP2ProviderSet, escalationProviders, SyncProviderSet, ruleP2Providers, StatsProviders, InhibitProviderSet,
)
