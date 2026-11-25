package worker

import (
	"context"
	"log/slog"
	"time"

	bizjob "github.com/lanyulei/reborn/backend/core/internal/biz/job"
	"github.com/lanyulei/reborn/backend/kit/loop"

	"github.com/go-kratos/kratos/v3/transport"
)

// 作业中心后台循环的轮询周期。各任务自身并发安全（幂等键 / 条件更新 / 行锁 SKIP LOCKED / 唯一键去重），
// 多实例部署无需选主。
const (
	// jobScheduleInterval 定时作业触发：Cron 最短间隔 1 分钟，10 秒一轮足够准时。
	jobScheduleInterval = 10 * time.Second
	// jobAlertInterval 告警自愈：告警通知本身 2 秒一轮，自愈稍慢一点即可。
	jobAlertInterval = 5 * time.Second
	// jobSignalInterval 流程信号回发。
	jobSignalInterval = 3 * time.Second
	// jobNotifyInterval 执行结束通知。
	jobNotifyInterval = 5 * time.Second
)

// JobLoops 作业中心域后台循环。
type JobLoops struct {
	loops []*loop.Loop
}

// Servers 以 transport.Server 形式返回各循环，交给 kratos.App 托管。
func (l *JobLoops) Servers() []transport.Server {
	out := make([]transport.Server, 0, len(l.loops))
	for _, lp := range l.loops {
		out = append(out, lp)
	}
	return out
}

// NewJobLoops 构造作业中心域后台循环：定时作业触发、告警自愈、流程信号回发、执行结束通知。
func NewJobLoops(schedules *bizjob.ScheduleUsecase, triggers *bizjob.TriggerUsecase, notify *bizjob.NotifyUsecase, housekeep *bizjob.HousekeepUsecase,
	logger *slog.Logger, rec loop.Recorder,
) *JobLoops {
	// 告警自愈与流程信号每轮处理完一批就等下一个周期（返回 0）：它们不是需要「有活就连着干」的队列。
	once := func(fn func(ctx context.Context) (int, error)) func(ctx context.Context) (int, error) {
		return func(ctx context.Context) (int, error) {
			_, err := fn(ctx)
			return 0, err
		}
	}
	return &JobLoops{loops: []*loop.Loop{
		// 定时作业触发直接用 Run 的返回值（本轮由本实例触发的条数）：>0 时循环立即续跑——同一时刻到期的
		// 计划多于一批时不再每批等 10 秒；没有到期的照常每 10 秒一轮。被触发的计划都已推进 next_time，
		// 积压处理完即回到周期轮询，不会空转。
		loop.New("job-schedule", jobScheduleInterval, logger, rec, schedules.Run),
		loop.New("job-alert", jobAlertInterval, logger, rec, once(triggers.RunAlerts)),
		loop.New("job-signal", jobSignalInterval, logger, rec, once(triggers.RunSignals)),
		// 通知有积压时连续处理直到取空。
		loop.New("job-notify", jobNotifyInterval, logger, rec, notify.Run),
		// 过期的分发文件内容与触发记录：每小时清一批，删满一批即接着清。
		loop.New("job-housekeep", time.Hour, logger, rec, housekeep.Run),
	}}
}
