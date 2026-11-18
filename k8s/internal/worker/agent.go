package worker

import (
	"log/slog"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/loop"
)

const (
	agentFollowInterval = time.Second
	agentSweepInterval  = 5 * time.Second
)

// AgentLoops Agent 集群的后台循环：隧道增减后对齐归属；断开令牌已吊销的隧道、标记离线的集群。
// 每个实例都跑（各自只处理本实例上的隧道；离线标记的语句自身原子）。
type AgentLoops struct {
	Loops []*loop.Loop
}

// NewAgentLoops 构造 Agent 相关循环。
func NewAgentLoops(keeper *biz.AgentKeeper, rec loop.Recorder, logger *slog.Logger) *AgentLoops {
	return &AgentLoops{Loops: []*loop.Loop{
		guarded("agent-follow", agentFollowInterval, logger, rec, keeper.Follow),
		guarded("agent-sweep", agentSweepInterval, logger, rec, keeper.Sweep),
	}}
}
