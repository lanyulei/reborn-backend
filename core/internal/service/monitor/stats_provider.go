package service

import "github.com/google/wire"

// StatsProviders 统计 / 引擎状态 / 聚合视图的 service。班次告警数没有独立的 service：装配 AlertService 时
// 调 WithShiftCounts 接上 *biz.ShiftCountUsecase。
var StatsProviders = wire.NewSet(NewStatsService, NewEngineService, NewAlertViewService)
