package worker

import (
	"log/slog"
	"time"

	bizticket "github.com/lanyulei/reborn/backend/core/internal/biz/ticket"
	"github.com/lanyulei/reborn/backend/kit/loop"

	"github.com/go-kratos/kratos/v3/transport"
)

// ticketSLAInterval 工单 SLA 巡检周期。巡检自身并发安全（SKIP LOCKED 领取 + 同事务打
// 标记），多实例部署无需选主；有积压时连续处理直到取空。
const ticketSLAInterval = time.Minute

// TicketLoops 工单域后台循环。
type TicketLoops struct {
	loops []*loop.Loop
}

// Servers 以 transport.Server 形式返回各循环，交给 kratos.App 托管。
func (l *TicketLoops) Servers() []transport.Server {
	out := make([]transport.Server, 0, len(l.loops))
	for _, lp := range l.loops {
		out = append(out, lp)
	}
	return out
}

// NewTicketLoops 构造工单域后台循环：为 SLA 已超时的工单发通知。
func NewTicketLoops(uc *bizticket.Usecase, logger *slog.Logger, rec loop.Recorder) *TicketLoops {
	return &TicketLoops{loops: []*loop.Loop{
		loop.New("ticket-sla", ticketSLAInterval, logger, rec, uc.NotifySLA),
	}}
}
