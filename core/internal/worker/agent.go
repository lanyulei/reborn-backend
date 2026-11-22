package worker

import (
	"context"
	"log/slog"
	"time"

	bizagent "github.com/lanyulei/reborn/backend/core/internal/biz/agent"
	"github.com/lanyulei/reborn/backend/kit/loop"

	"github.com/go-kratos/kratos/v3/transport"
)

// 运维 Agent 域后台循环周期。
//   - agent-runner：领取任务（SKIP LOCKED，多实例无需选主）并分阶段运行；领取后立即返回，任务在各自的 goroutine
//     里跑完，并发由单实例槽位限制；回收过期租约与排队超时也在这一轮里。停机时 drain 等在途任务收尾。
//   - agent-purge：按保留期清理运行记录及其助手会话、终态任务。
const (
	agentRunnerInterval = 5 * time.Second
	agentPurgeInterval  = time.Hour
)

// AgentLoops 运维 Agent 域后台循环。
type AgentLoops struct {
	loops []*loop.Loop
	drain *runnerDrain
}

// Servers 以 transport.Server 形式返回各循环（以及停机时等待在途任务收尾的 drain），交给 kratos.App 托管。
func (l *AgentLoops) Servers() []transport.Server {
	out := make([]transport.Server, 0, len(l.loops)+1)
	for _, lp := range l.loops {
		out = append(out, lp)
	}
	if l.drain != nil {
		out = append(out, l.drain)
	}
	return out
}

// runnerDrain 停机时等待运行器在途任务收尾（写回运行 / 任务终态、释放故障租约），受 Stop 的 ctx 限时。
type runnerDrain struct {
	runner *bizagent.RunnerUsecase
	done   chan struct{}
}

// Start implements transport.Server：无事可做，阻塞到 Stop。
func (d *runnerDrain) Start(ctx context.Context) error {
	select {
	case <-ctx.Done():
	case <-d.done:
	}
	return nil
}

// Stop implements transport.Server.
func (d *runnerDrain) Stop(ctx context.Context) error {
	close(d.done)
	return d.runner.Wait(ctx)
}

// NewAgentLoops 构造运维 Agent 域后台循环（配置未启用时不建循环）。
func NewAgentLoops(runner *bizagent.RunnerUsecase, cfg bizagent.Config, logger *slog.Logger, rec loop.Recorder) *AgentLoops {
	if !cfg.Enabled {
		return &AgentLoops{}
	}
	return &AgentLoops{
		loops: []*loop.Loop{
			loop.New("agent-runner", agentRunnerInterval, logger, rec, runner.Run),
			loop.New("agent-purge", agentPurgeInterval, logger, rec, func(ctx context.Context) (int, error) { return runner.Purge(ctx) }),
		},
		drain: &runnerDrain{runner: runner, done: make(chan struct{})},
	}
}
