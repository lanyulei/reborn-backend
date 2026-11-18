package biz

import (
	"context"
	"errors"
	"log/slog"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// CommandOptions 事件指令消费参数。
type CommandOptions struct {
	// Batch 每轮至多处理的指令数。
	Batch int
}

// CommandUsecase 消费 core 下达的事件指令（monitor_alert_cmd）。目前只有手动关闭：core 不写引擎的状态表，
// 关闭由引擎执行——事件置为已关闭、删除它的判定状态，条件仍成立的序列下一轮按新序列重新走持续时长。
//
// 多实例：每个评估器实例都跑这条循环，不选主。每条指令一个事务，先以 FOR UPDATE SKIP LOCKED 锁指令行
// （别的实例正在处理就跳过，已处理的不会再处理），再锁事件所属的评估任务行——与评估的提交事务
// （同样先锁任务行）串行：关闭要么发生在某一轮提交之前（那一轮读不到状态行，样本按新序列处理），要么
// 发生在之后，不会与一轮评估交错。事务回滚则指令仍是待处理，下一轮重试。
type CommandUsecase struct {
	repo  CommandRepo
	obs   Observer
	log   *slog.Logger
	opts  CommandOptions
	newID func() (string, error)
}

// NewCommandUsecase new a CommandUsecase.
func NewCommandUsecase(repo CommandRepo, obs Observer, log *slog.Logger) *CommandUsecase {
	return &CommandUsecase{repo: repo, obs: obs, log: log, newID: idgen.NewID, opts: CommandOptions{Batch: 100}}
}

// WithOptions 注入参数（非法值保持缺省）。
func (uc *CommandUsecase) WithOptions(o CommandOptions) *CommandUsecase {
	if o.Batch > 0 {
		uc.opts.Batch = o.Batch
	}
	return uc
}

// RunOnce 处理一批待处理的指令，返回本实例处理的条数（处理满一批时循环立即再跑一轮）。
// 单条失败只记日志（它仍是待处理，下一轮重试），不影响其余指令；全部失败才向循环返回错误。
func (uc *CommandUsecase) RunOnce(ctx context.Context) (int, error) {
	cmds, err := uc.repo.PendingCommands(ctx, uc.opts.Batch)
	if err != nil {
		return 0, err
	}
	var (
		handled int
		last    error
	)
	for _, cmd := range cmds {
		if ctx.Err() != nil {
			break
		}
		// 不认识的指令留给认识它的版本处理（滚动发布期间新旧实例共存）。
		if cmd.Kind != monitorcontract.AlertCmdClose {
			continue
		}
		closed, err := uc.repo.CloseAlert(ctx, cmd, uc.newID)
		switch {
		case errors.Is(err, ErrCommandGone):
		case err != nil:
			last = err
			uc.obs.AddCommand(ctx, CommandFailed)
			uc.log.ErrorContext(ctx, "alert command failed, it stays pending", "command_id", cmd.ID, "alert_id", cmd.AlertID, "error", err)
		case closed:
			handled++
			uc.obs.AddCommand(ctx, CommandDone)
			uc.log.InfoContext(ctx, "alert closed by command", "command_id", cmd.ID, "alert_id", cmd.AlertID)
		default:
			handled++
			uc.obs.AddCommand(ctx, CommandRejected)
		}
	}
	if handled == 0 && last != nil {
		return 0, last
	}
	if handled < uc.opts.Batch {
		return 0, nil
	}
	return handled, nil
}
