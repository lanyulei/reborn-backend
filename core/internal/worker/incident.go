package worker

import (
	"context"
	"log/slog"
	"time"

	bizincident "github.com/lanyulei/reborn/backend/core/internal/biz/incident"
	"github.com/lanyulei/reborn/backend/kit/loop"

	"github.com/go-kratos/kratos/v3/transport"
)

// 故障管理域后台循环周期。
//   - incident-correlator：顺序消费告警事件流（单游标），必须单实例——以 advisory lock 选主（loop.Leader），
//     其余副本空转；主实例崩溃后锁随连接释放，下一轮由别的副本接管。
//   - incident-inspector：响应超时升级 / 停滞提醒 / 复盘到期 / 自动关闭等巡检，提醒类写「已提醒」标记幂等，
//     仍以选主避免多副本同一轮重复扫描。
//   - incident-action / incident-webhook：发件箱以 SKIP LOCKED 领取，多实例无需选主。
//   - incident-proposal-poll：轮询在途建议的执行结果与验证窗口，按建议行锁领取，多实例无需选主。
//   - incident-purge：清理已投递的发件箱与过期动作，幂等。
const (
	incidentCorrelatorInterval = 2 * time.Second
	incidentActionInterval     = 2 * time.Second
	incidentProposalInterval   = 10 * time.Second
	incidentInspectorInterval  = time.Minute
	incidentWebhookInterval    = 3 * time.Second
	incidentPurgeInterval      = time.Hour
)

// 选主锁键（与其他域的 advisory lock 键空间不同名）。
const (
	incidentCorrelatorLock = "incident-correlator"
	incidentInspectorLock  = "incident-inspector"
)

// IncidentLoops 故障管理域后台循环。
type IncidentLoops struct {
	loops []*loop.Loop
}

// Servers 以 transport.Server 形式返回各循环，交给 kratos.App 托管。
func (l *IncidentLoops) Servers() []transport.Server {
	out := make([]transport.Server, 0, len(l.loops))
	for _, lp := range l.loops {
		out = append(out, lp)
	}
	return out
}

// NewIncidentLoops 构造故障管理域后台循环。
func NewIncidentLoops(correlator *bizincident.CorrelatorUsecase, actions *bizincident.ActionUsecase, inspector *bizincident.InspectorUsecase,
	webhooks *bizincident.WebhookUsecase, locker loop.Locker, logger *slog.Logger, rec loop.Recorder,
) *IncidentLoops {
	corr := loop.NewLeader(incidentCorrelatorLock, locker, logger)
	insp := loop.NewLeader(incidentInspectorLock, locker, logger)
	return &IncidentLoops{loops: []*loop.Loop{
		loop.New("incident-correlator", incidentCorrelatorInterval, logger, rec, func(ctx context.Context) (int, error) { return corr.Run(ctx, correlator.Run) }),
		loop.New("incident-action", incidentActionInterval, logger, rec, actions.Run),
		loop.New("incident-proposal-poll", incidentProposalInterval, logger, rec, actions.PollProposals),
		loop.New("incident-inspector", incidentInspectorInterval, logger, rec, func(ctx context.Context) (int, error) { return insp.Run(ctx, inspector.Run) }),
		loop.New("incident-webhook", incidentWebhookInterval, logger, rec, webhooks.Run),
		loop.New("incident-purge", incidentPurgeInterval, logger, rec, func(ctx context.Context) (int, error) {
			n, err := actions.Purge(ctx)
			if err != nil {
				return n, err
			}
			m, err := webhooks.Purge(ctx)
			return n + m, err
		}),
	}}
}
