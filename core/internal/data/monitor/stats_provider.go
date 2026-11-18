package data

import (
	"github.com/google/wire"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
)

// StatsProviders 统计 / 引擎状态 / 聚合视图 / 班次告警数的仓储。
var StatsProviders = wire.NewSet(
	NewStatsRepo, NewEngineRepo, NewAlertViewRepo, NewShiftCountRepo,
	wire.Bind(new(biz.StatsReader), new(*statsRepo)),
	wire.Bind(new(biz.StatsRanker), new(*statsRepo)),
	wire.Bind(new(biz.EngineReader), new(*engineRepo)),
	wire.Bind(new(biz.EngineIssues), new(*engineRepo)),
	wire.Bind(new(biz.SavedViewReader), new(*alertViewRepo)),
	wire.Bind(new(biz.SavedViewWriter), new(*alertViewRepo)),
	wire.Bind(new(biz.ShiftAlertCounter), new(*shiftCountRepo)),
)
