// Package worker 把 biz 用例包成 kratos 托管的后台循环（kit/loop），随 App 生命周期启停。
package worker

import (
	"context"
	"log/slog"
	"time"

	bizoncall "github.com/lanyulei/reborn/backend/core/internal/biz/oncall"
	"github.com/lanyulei/reborn/backend/kit/loop"

	"github.com/go-kratos/kratos/v3/transport"
	"github.com/google/wire"
)

// ProviderSet is worker providers.
var ProviderSet = wire.NewSet(NewOncallLoops, NewFileLoops, NewWorkflowLoops, NewTicketLoops, NewMonitorLoops, wire.Struct(new(MonitorP2Usecases), "*"), NewK8sLoops, NewJobLoops, NewKnowledgeLoops, NewIncidentLoops, NewAgentLoops)

// 值班后台循环的轮询周期。各任务自身并发安全（行锁 / SKIP LOCKED / 去重键），
// 多实例部署无需选主。
const (
	oncallExtendInterval  = 10 * time.Minute
	oncallRemindInterval  = 30 * time.Second
	oncallDigestInterval  = time.Minute
	oncallInspectInterval = time.Hour
)

// OncallLoops 值班域全部后台循环。
type OncallLoops struct {
	loops []*loop.Loop
}

// Servers 以 transport.Server 形式返回各循环，交给 kratos.App 托管。
func (l *OncallLoops) Servers() []transport.Server {
	out := make([]transport.Server, 0, len(l.loops))
	for _, lp := range l.loops {
		out = append(out, lp)
	}
	return out
}

// NewOncallLoops 构造值班后台循环：补排班、上班提醒、每日播报、异常巡检。
func NewOncallLoops(uc *bizoncall.WorkerUsecase, logger *slog.Logger, rec loop.Recorder) *OncallLoops {
	// idle 屏蔽"处理条数 > 0 即立刻再跑一轮"的忙循环语义：这些任务一轮即处理完，
	// 且巡检在异常持续存在期间每轮都会返回非零。
	idle := func(fn func(ctx context.Context) (int, error)) func(ctx context.Context) (int, error) {
		return func(ctx context.Context) (int, error) {
			_, err := fn(ctx)
			return 0, err
		}
	}
	return &OncallLoops{loops: []*loop.Loop{
		loop.New("oncall-extend", oncallExtendInterval, logger, rec, idle(uc.Extend)),
		// 提醒按批领取（每批 200），积压时连续处理直到取空。
		loop.New("oncall-remind", oncallRemindInterval, logger, rec, uc.Remind),
		loop.New("oncall-digest", oncallDigestInterval, logger, rec, idle(uc.Digest)),
		loop.New("oncall-inspect", oncallInspectInterval, logger, rec, idle(uc.Inspect)),
	}}
}
