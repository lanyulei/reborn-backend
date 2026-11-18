package biz

import "github.com/google/wire"

// StatsProviders 统计 / 引擎状态 / 聚合视图 / 班次告警数的用例。跨域窄接口 Shifts、OncallAccess 在组合根绑定。
var StatsProviders = wire.NewSet(NewStatsUsecase, NewEngineUsecase, NewAlertViewUsecase, NewShiftCountUsecase)
