package worker

import (
	"log/slog"
	"time"

	bizworkflow "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"
	"github.com/lanyulei/reborn/backend/kit/loop"

	"github.com/go-kratos/kratos/v3/transport"
)

// workflowJobInterval 流程作业队列的轮询周期（定时节点、任务超时、自动任务、子流程衔接）。
// 领取本身并发安全（SKIP LOCKED + 租约），多实例部署无需选主；有积压时连续处理直到取空。
const workflowJobInterval = 5 * time.Second

// workflowPurgeInterval 清理早已了结的作业的周期。
const workflowPurgeInterval = time.Hour

// WorkflowLoops 流程引擎后台循环。
type WorkflowLoops struct {
	loops []*loop.Loop
}

// Servers 以 transport.Server 形式返回各循环，交给 kratos.App 托管。
func (l *WorkflowLoops) Servers() []transport.Server {
	out := make([]transport.Server, 0, len(l.loops))
	for _, lp := range l.loops {
		out = append(out, lp)
	}
	return out
}

// NewWorkflowLoops 构造流程引擎后台循环：执行到期的引擎内部作业、执行到期的自动任务、清理
// 早已了结的作业。自动任务（对外的 Webhook 调用）单独一个循环：外部端点再慢，也不会挡住
// 定时节点、任务超时与子流程衔接。
func NewWorkflowLoops(uc *bizworkflow.JobUsecase, logger *slog.Logger, rec loop.Recorder) *WorkflowLoops {
	return &WorkflowLoops{loops: []*loop.Loop{
		loop.New("workflow-job", workflowJobInterval, logger, rec, uc.RunDue),
		loop.New("workflow-service", workflowJobInterval, logger, rec, uc.RunServices),
		loop.New("workflow-job-purge", workflowPurgeInterval, logger, rec, uc.Purge),
	}}
}
