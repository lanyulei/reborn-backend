package worker

import (
	"log/slog"
	"time"

	bizmonitor "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	"github.com/lanyulei/reborn/backend/kit/loop"

	"github.com/go-kratos/kratos/v3/transport"
)

// monitorNotifyInterval 监控通知派发的轮询周期。派发自身并发安全（发件箱行锁 SKIP LOCKED 领取，
// 与通知事件、通知记录同事务提交），多实例部署无需选主；有积压时连续处理直到取空。
const monitorNotifyInterval = 2 * time.Second

// MonitorLoops 监控告警域后台循环。
type MonitorLoops struct {
	loops []*loop.Loop
}

// Servers 以 transport.Server 形式返回各循环，交给 kratos.App 托管。
func (l *MonitorLoops) Servers() []transport.Server {
	out := make([]transport.Server, 0, len(l.loops))
	for _, lp := range l.loops {
		out = append(out, lp)
	}
	return out
}

// monitorEngineNotifyInterval 平台自监控事件派发的轮询周期（行锁 SKIP LOCKED 领取，多实例无需选主）。
const monitorEngineNotifyInterval = 5 * time.Second

// MonitorP2Usecases 第二、三期的后台用例：自监控事件派发、CMDB 资源索引同步、工作日历快照、事件动作。
type MonitorP2Usecases struct {
	Engine   *bizmonitor.EngineEventUsecase
	CISync   *bizmonitor.CISyncUsecase
	Calendar *bizmonitor.CalendarSyncUsecase
	Actions  *bizmonitor.ActionUsecase
}

// NewMonitorLoops 构造监控告警域后台循环：把评估引擎写入的通知发件箱派发为通知事件，以及第二、三期的
// 自监控事件派发、资源索引同步、工作日历快照与事件动作（见 monitor_p2.go）。
func NewMonitorLoops(uc *bizmonitor.DispatchUsecase, p2 MonitorP2Usecases, logger *slog.Logger, rec loop.Recorder) *MonitorLoops {
	loops := []*loop.Loop{
		loop.New("monitor-notify", monitorNotifyInterval, logger, rec, uc.Run),
		loop.New("monitor-engine-notify", monitorEngineNotifyInterval, logger, rec, p2.Engine.Run),
	}
	return &MonitorLoops{loops: append(loops, MonitorP2Loops(p2.CISync, p2.Calendar, p2.Actions, logger, rec)...)}
}
