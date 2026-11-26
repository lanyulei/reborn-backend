package biz

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// CommandOptions 事件指令消费参数。
type CommandOptions struct {
	// Batch 每轮至多处理的指令数。
	Batch int
}

// 一条指令「连续失败」的判定，时刻与比较都在数据库侧（CommandRepo.RecordCommandFailure）。
const (
	// commandGiveUpAfter 一条指令连续失败满这个时长就放弃：置未执行、时间线写明原因。不放弃的话，恒失败的
	// 指令（约束冲突、行损坏……）会永远停在待处理，用户在事件上一直看到「待关闭」。
	commandGiveUpAfter = 5 * time.Minute
	// commandFailureGap 两次失败之间超过这个时长就算中断过（引擎宕机、指令长期没被领取）：中断前的失败不计入
	// 这一段，从下一次失败重新计时——否则宕机前失败过一次、重启后数据库刚好抖一下，等了很久的指令会被立刻放弃。
	// 正常每 2s 左右重试一次，整批失败时循环的退避最长 30s，都远小于它。
	commandFailureGap = time.Minute
	// maxCommandResultBytes monitor_alert_cmd.result 的字节上限（列是 varchar(255)，按字符计；按字节截断更保守）。
	// 放弃的原因同时写进 result 与时间线：超长会让放弃事务每次都失败、指令永远放弃不掉，所以先在这里截断。
	maxCommandResultBytes = 255
)

// CommandUsecase 消费 core 下达的事件指令（monitor_alert_cmd）。目前只有手动关闭：core 不写引擎的状态表，
// 关闭由引擎执行——事件置为已关闭、删除它的判定状态，条件仍成立的序列下一轮按新序列重新走持续时长。
//
// 多实例：每个评估器实例都跑这条循环，不选主。每条指令一个事务，先以 FOR UPDATE SKIP LOCKED 锁指令行
// （别的实例正在处理就跳过，已处理的不会再处理），再锁事件所属的评估任务行——与评估的提交事务
// （同样先锁任务行）串行：关闭要么发生在某一轮提交之前（那一轮读不到状态行，样本按新序列处理），要么
// 发生在之后，不会与一轮评估交错。事务回滚则指令仍是待处理，下一轮重试；每次失败都记到指令行上，连续失败
// 满 commandGiveUpAfter 的指令被放弃（见 failed），不再无限重试。
type CommandUsecase struct {
	repo     CommandRepo
	failures CommandFailureRepo
	obs      Observer
	log      *slog.Logger
	opts     CommandOptions
	newID    func() (string, error)
}

// NewCommandUsecase new a CommandUsecase.
func NewCommandUsecase(repo CommandRepo, failures CommandFailureRepo, obs Observer, log *slog.Logger) *CommandUsecase {
	return &CommandUsecase{repo: repo, failures: failures, obs: obs, log: log, newID: idgen.NewID, opts: CommandOptions{Batch: 100}}
}

// WithOptions 注入参数（非法值保持缺省）。
func (uc *CommandUsecase) WithOptions(o CommandOptions) *CommandUsecase {
	if o.Batch > 0 {
		uc.opts.Batch = o.Batch
	}
	return uc
}

// RunOnce 处理一批待处理的指令，返回本实例处理的条数（处理满一批时循环立即再跑一轮）。
// 单条失败不影响其余指令：失败记到指令行上，连续失败满 commandGiveUpAfter 的被放弃（见 failed），其余仍是
// 待处理、下一轮重试。全部失败且没有一条被放弃才向循环返回错误。
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
			if uc.failed(ctx, cmd, err) {
				handled++
			} else {
				last = err
			}
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

// failed 处理一次执行失败：记指标，把失败记到指令行上；这一段连续失败已满上限就放弃它（置未执行、时间线写明
// 原因）。返回指令是否已被放弃——放弃了就不再是待处理。除了放弃的那一次，每次失败都打一条 ERROR：一条指令
// 至多连续失败 commandGiveUpAfter 这么久，日志与「待关闭」标记都不会再无休止地持续下去。
func (uc *CommandUsecase) failed(ctx context.Context, cmd AlertCommand, err error) bool {
	uc.obs.AddCommand(ctx, CommandFailed)
	attrs := []any{"command_id", cmd.ID, "alert_id", cmd.AlertID, "error", err}
	staysPending := func(extra ...any) bool {
		uc.log.ErrorContext(ctx, "alert command failed, it stays pending", append(attrs, extra...)...)
		return false
	}
	// 停机中被取消的那一次不算指令的失败：不记失败，更不会因此放弃。
	if ctx.Err() != nil {
		return staysPending()
	}
	exhausted, recErr := uc.failures.RecordCommandFailure(ctx, cmd.ID, commandFailureGap, commandGiveUpAfter)
	switch {
	case errors.Is(recErr, ErrCommandGone), recErr == nil && !exhausted:
		return staysPending()
	case recErr != nil:
		return staysPending("failure_not_recorded", recErr)
	}
	why := abandonReason(err)
	abandoned, abErr := uc.failures.AbandonCommand(ctx, cmd, why, commandGiveUpAfter, uc.newID)
	switch {
	case abErr != nil:
		return staysPending("abandon_error", abErr)
	case !abandoned:
		// 围栏未命中：别的实例正在处理或刚处理掉这条指令，放不放弃由它决定。
		return staysPending("give_up_skipped", true)
	}
	uc.obs.AddCommand(ctx, CommandAbandoned)
	uc.log.ErrorContext(ctx, "alert command abandoned after continuous failures",
		"command_id", cmd.ID, "alert_id", cmd.AlertID, "error", err, "reason", why)
	return true
}

// abandonReason 放弃手动关闭时写进指令 result 与时间线的说明（时长与自监控事件同一写法）。
func abandonReason(err error) string {
	return truncate(fmt.Sprintf("手动关闭未执行：引擎连续 %s执行失败：%s", humanDuration(commandGiveUpAfter), err), maxCommandResultBytes)
}
