package biz

import (
	"context"
	"log/slog"

	"github.com/lanyulei/reborn/backend/core/internal/conf"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// 升级通知（发件箱 kind = 4）的派发。评估引擎决定「何时升到哪一级、该级第几次提醒」并入箱；这里只负责
// 把入箱的行发出去：按策略的那一级解析通知对象、写 monitor.alert.escalated 事件与通知记录、回写该级的
// 通知进度（monitor_alert_escalation）。不动 monitor_alert.notify_count，不走订阅匹配。

// 升级通知的丢弃原因。策略被停用 / 删除、规则不再引用策略后，引擎不会再为它入箱，已入箱的行丢弃即可。
const (
	dropEscalationNotFiring = "事件已不在告警中，不再发升级通知"
	dropEscalationUnset     = "规则未引用升级策略"
	dropEscalationGone      = "升级策略已删除"
	dropEscalationDisabled  = "升级策略已停用"
	dropEscalationNoLevel   = "升级策略已没有这一级"
)

// EscalationProgress 升级通知的进度回写（monitor_alert_escalation；core 有 SELECT / INSERT / UPDATE）。
type EscalationProgress interface {
	// TouchLevelTx 把事件在某一级的已通知次数抬到 seq（级内序号；幂等，只增不减）并记下通知时间。
	TouchLevelTx(ctx context.Context, tx txrun.Tx, alertID string, level, seq int32) error
}

// DispatchExt 通知派发在第二、三期用到的协作者。
type DispatchExt struct {
	// Escalations 升级策略；Progress 各级通知进度的回写。
	Escalations EscalationReader
	Progress    EscalationProgress
	// CIs 资源索引：事件关联的资源（通知内容体里的 ci_id / ci_name、规则的「资源负责人」）。
	CIs CIIndexReader
	// Roster / Groups 升级策略通知对象里的「仅备班」与值班分组管理员。
	Roster DutyRoster
	Groups OncallGroups
}

// WithP2 注入第二、三期的协作者，返回自身。须在开始派发之前调用（不并发安全）。
func (uc *DispatchUsecase) WithP2(ext DispatchExt) *DispatchUsecase {
	uc.escalations, uc.progress, uc.cis = ext.Escalations, ext.Progress, ext.CIs
	r := uc.resolver
	uc.resolver = NewRecipientResolver(r.dir, r.leaders, r.duty, ext.Roster, ext.Groups)
	return uc
}

// ProvideDispatchUsecase 装配带第二、三期协作者的派发用例（wire 用它代替 NewDispatchUsecase）。
func ProvideDispatchUsecase(txm TxManager, outbox OutboxRepo, writer DispatchWriter, alerts AlertReader, rules RuleReader,
	subs SubscriptionReader, scopes *ScopeUsecase, dir Directory, leaders DeptLeaders, duty Duty, events EventWriter, logger *slog.Logger,
	c *conf.Monitor, ext DispatchExt,
) (*DispatchUsecase, error) {
	uc, err := NewDispatchUsecase(txm, outbox, writer, alerts, rules, subs, scopes, dir, leaders, duty, events, logger, c)
	if err != nil {
		return nil, err
	}
	return uc.WithP2(ext), nil
}

// NewDispatchExt 组装 DispatchExt（wire provider）。
func NewDispatchExt(escalations EscalationReader, progress EscalationProgress, cis CIIndexReader, roster DutyRoster, groups OncallGroups) DispatchExt {
	return DispatchExt{Escalations: escalations, Progress: progress, CIs: cis, Roster: roster, Groups: groups}
}

// errEscalationUnwired 没有注入升级策略仓储（装配问题）：按失败重试，不丢弃。
var errEscalationUnwired = detail(ErrEscalationInvalid, "escalation dispatch is not wired")

// processEscalation 派发一条升级通知。调用方已确认：行自洽（levelSeq 为级内序号）、事件存在、未被屏蔽、
// 未被抑制、未关闭。
func (uc *DispatchUsecase) processEscalation(ctx context.Context, tx txrun.Tx, item *OutboxItem, alert *Alert, levelSeq int32,
	rs *matchState, drop func(reason string) error,
) error {
	// 升级只对告警中的事件有意义：恢复 / 关闭之后迟到的行一律不发。
	if alert.Status != mc.AlertFiring {
		return drop(dropEscalationNotFiring)
	}
	rule, err := uc.rules.Get(ctx, alert.RuleID)
	if isNotFound(err) {
		return drop(dropRuleDeleted)
	}
	if err != nil {
		return err
	}
	if rule.Spec.EscalationID == "" {
		return drop(dropEscalationUnset)
	}
	if uc.escalations == nil || uc.progress == nil {
		return errEscalationUnwired
	}
	policy, err := uc.escalations.Get(ctx, rule.Spec.EscalationID)
	if isNotFound(err) {
		return drop(dropEscalationGone)
	}
	if err != nil {
		return err
	}
	if !policy.Spec.Enabled {
		return drop(dropEscalationDisabled)
	}
	if int(item.Level) > len(policy.Spec.Levels) {
		return drop(dropEscalationNoLevel)
	}
	if err := rs.load(ctx, uc.matcher.scopes); err != nil {
		return err
	}
	d, err := uc.newDispatch(ctx, tx, item, alert, rule, levelSeq)
	if err != nil {
		return err
	}
	d.tree = rs.tree
	users, _, err := d.sess.Escalation(ctx, policy.Spec.Levels[item.Level-1].Targets)
	if err != nil {
		return err
	}
	payload := d.payload("")
	payload.EscalationLevel, payload.EscalationSeq, payload.EscalationName = item.Level, levelSeq, policy.Spec.Name
	// 没有解析到通知对象：同规则通知，不带收件人写一条事件，退回 notify 路由的固定收件与订阅者。
	var recipients []string
	note := noteNoRecipients
	if len(users) > 0 {
		recipients, note = users, ""
	}
	ids, err := d.emit(ctx, mc.EventAlertEscalated, "", payload, recipients)
	if err != nil {
		return err
	}
	if err := d.writeLog(ctx, mc.NotifySourceEscalation, "", users, ids, note); err != nil {
		return err
	}
	// 引擎据此计算该级的重复提醒与次数上限：只在确实派发时回写（与通知事件同事务）。
	if err := uc.progress.TouchLevelTx(ctx, tx, alert.ID, item.Level, levelSeq); err != nil {
		return err
	}
	return uc.outbox.FinishTx(ctx, tx, item.ID, mc.OutboxDone, "")
}
