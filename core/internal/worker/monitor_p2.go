package worker

import (
	"log/slog"
	"time"

	bizmonitor "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	"github.com/lanyulei/reborn/backend/kit/loop"
)

// 监控告警域第二、三期后台循环的轮询周期。三个循环在每个副本上都会启动，各自多实例安全，无需选主：
//   - monitor-ci-sync：一轮在一个事务里完成并先取事务级 advisory lock，同一时刻只有一个实例在同步，
//     写入幂等（内容没变的行不写）；
//   - monitor-calendar：单条「内容不同才更新」的语句；
//   - monitor-action：以租约领取（SKIP LOCKED），落账带围栏；有积压时连续处理直到取空。
const (
	monitorCISyncInterval   = 30 * time.Second
	monitorCalendarInterval = time.Minute
	monitorActionInterval   = 5 * time.Second
)

// MonitorP2Loops 构造第二、三期的后台循环：CMDB 资源索引同步、工作日历快照、事件动作执行。
func MonitorP2Loops(ciSync *bizmonitor.CISyncUsecase, calendar *bizmonitor.CalendarSyncUsecase, actions *bizmonitor.ActionUsecase,
	logger *slog.Logger, rec loop.Recorder,
) []*loop.Loop {
	return []*loop.Loop{
		loop.New("monitor-ci-sync", monitorCISyncInterval, logger, rec, ciSync.Run),
		loop.New("monitor-calendar", monitorCalendarInterval, logger, rec, calendar.Run),
		loop.New("monitor-action", monitorActionInterval, logger, rec, actions.Run),
	}
}
