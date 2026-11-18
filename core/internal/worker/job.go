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
	// 每轮处理完一批就等下一个周期（返回 0）：这些都不是需要「有活就连着干」的队列。
	once := func(fn func(ctx context.Context) (int, error)) func(ctx context.Context) (int, error) {
		return func(ctx context.Context) (int, error) {
			_, err := fn(ctx)
			return 0, err
		}
	}
	return &JobLoops{loops: []*loop.Loop{
		loop.New("job-schedule", jobScheduleInterval, logger, rec, once(schedules.Run)),
		loop.New("job-alert", jobAlertInterval, logger, rec, once(triggers.RunAlerts)),
		loop.New("job-signal", jobSignalInterval, logger, rec, once(triggers.RunSignals)),
		// 通知有积压时连续处理直到取空。
		loop.New("job-notify", jobNotifyInterval, logger, rec, notify.Run),
		// 过期的分发文件内容与触发记录：每小时清一批，删满一批即接着清。
		loop.New("job-housekeep", time.Hour, logger, rec, housekeep.Run),
	}}
}
