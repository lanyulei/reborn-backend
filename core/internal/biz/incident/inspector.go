package biz

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// 巡检（功能设计 §11.4）：每分钟一轮——未响应升级、无进展提醒、动作建议过期、复盘到期提醒、S3/S4 自动关闭、
// 候选故障过期、改进项工单状态同步。全部幂等：提醒类写「已提醒」标记，重启不重发。由 loop.Leader 保证单实例。

const (
	inspectBatch        = 200
	maxEscalation       = 3
	reviewRemindDue     = 1
	reviewRemindOverdue = 2
)

// InspectorUsecase 巡检。
type InspectorUsecase struct {
	d           *Deps
	inc         *IncidentUsecase
	proposals   *ProposalUsecase
	postmortems *PostmortemUsecase
	settings    *SettingsUsecase
	now         func() time.Time
}

// NewInspectorUsecase new an InspectorUsecase.
func NewInspectorUsecase(d *Deps, inc *IncidentUsecase, proposals *ProposalUsecase, postmortems *PostmortemUsecase, settings *SettingsUsecase) *InspectorUsecase {
	return &InspectorUsecase{d: d, inc: inc, proposals: proposals, postmortems: postmortems, settings: settings, now: time.Now}
}

// Run 一轮巡检，返回处理条数。单项失败只记日志，不影响其他项。
func (uc *InspectorUsecase) Run(ctx context.Context) (int, error) {
	settings, err := uc.settings.Get(ctx)
	if err != nil {
		return 0, err
	}
	total := 0
	for name, step := range map[string]func(context.Context, *Settings) (int, error){
		"escalate": uc.escalate, "stale": uc.stale, "expire-proposals": uc.expireProposals, "review-due": uc.reviewDue,
		"auto-close": uc.autoClose, "expire-candidates": uc.expireCandidates, "sync-items": uc.syncItems, "release-leases": uc.releaseLeases,
	} {
		n, err := step(ctx, settings)
		if err != nil {
			uc.d.Logger.ErrorContext(ctx, "incident inspector step failed", "step", name, "error", err)
			continue
		}
		total += n
	}
	return total, nil
}

// escalate 未响应升级：开启后超过等级的响应时限仍待响应 → 逐级通知（每级一次，至多 3 级，各级间隔同一时限）。
func (uc *InspectorUsecase) escalate(ctx context.Context, settings *Settings) (int, error) {
	done := 0
	for sev := SeverityS1; sev <= SeverityS4; sev++ {
		respond := settings.Sev(sev).RespondM
		if respond <= 0 {
			continue
		}
		before := uc.now().Add(-time.Duration(respond) * time.Minute)
		items, err := uc.d.Incidents.Inspect(ctx, InspectQuery{Statuses: []int32{StatusOpen}, Severities: []int32{sev}, OpenBefore: &before, ExcludeDrill: true}, inspectBatch)
		if err != nil {
			return done, err
		}
		for _, it := range items {
			// 上一次升级后再过一个响应时限才升下一级。
			if it.EscalateTime != nil && uc.now().Sub(*it.EscalateTime) < time.Duration(respond)*time.Minute {
				continue
			}
			if it.EscalationLevel >= maxEscalation {
				continue
			}
			if err := uc.escalateOne(ctx, it, settings); err != nil {
				uc.d.Logger.ErrorContext(ctx, "incident inspector: escalate", "incident", it.ID, "error", err)
				continue
			}
			done++
		}
	}
	return done, nil
}

func (uc *InspectorUsecase) escalateOne(ctx context.Context, inc *Incident, settings *Settings) error {
	level := inc.EscalationLevel + 1
	scheduleID, groupID := "", ""
	if inc.RuleID != "" {
		if rule, err := uc.d.Rules.Get(ctx, inc.RuleID); err == nil {
			scheduleID, groupID = rule.Spec.Response.CommanderScheduleID, rule.Spec.Response.ResponderOncallGroupID
		}
	}
	if scheduleID == "" && inc.BusinessID != "" {
		if b, err := uc.d.Businesses.Get(ctx, inc.BusinessID); err == nil {
			scheduleID = b.ScheduleID
		}
	}
	if scheduleID == "" {
		scheduleID = settings.FallbackScheduleID
	}
	users, err := uc.inc.escalationTargets(ctx, inc, level, scheduleID, groupID)
	if err != nil {
		return err
	}
	_, err = uc.inc.mutate(ctx, inc.ID, ActorSystem, "", func(m *mutation) error {
		if m.inc.Status != StatusOpen {
			return errNoOp
		}
		m.inc.EscalationLevel, m.inc.EscalateTime = level, ptr(m.now)
		text := fmt.Sprintf("开启 %s 仍无人响应", humanDuration(m.now.Sub(m.inc.OpenTime)))
		if len(users) == 0 {
			text += "，本级未解析到通知对象"
		}
		m.event(EventEscalate, EventDetail{Level: level, Text: text})
		n := notice{
			action: NotifyEscalated, dedup: fmt.Sprint(level), noGroups: len(users) > 0,
			extra: map[string]any{"level": level, "elapsed": humanDuration(m.now.Sub(m.inc.OpenTime))},
		}
		if len(users) > 0 {
			n.users = users
		}
		m.notify(n)
		return nil
	})
	if errors.Is(err, errNoOp) {
		return nil
	}
	return err
}

// stale 无进展提醒：处理中 / 已缓解，距上一条公告或人工备注超过间隔。
func (uc *InspectorUsecase) stale(ctx context.Context, settings *Settings) (int, error) {
	done := 0
	for sev := SeverityS1; sev <= SeverityS4; sev++ {
		cfg := settings.Sev(sev)
		if cfg.StaleM <= 0 {
			continue
		}
		before := uc.now().Add(-time.Duration(cfg.StaleM) * time.Minute)
		items, err := uc.d.Incidents.Inspect(ctx, InspectQuery{
			Statuses: []int32{StatusResponding, StatusMitigated}, Severities: []int32{sev}, LastActivityBefore: &before, StaleNotifyBefore: &before, ExcludeDrill: true,
		}, inspectBatch)
		if err != nil {
			return done, err
		}
		for _, it := range items {
			_, err := uc.inc.mutate(ctx, it.ID, ActorSystem, "", func(m *mutation) error {
				if m.inc.Status != StatusResponding && m.inc.Status != StatusMitigated {
					return errNoOp
				}
				needUpdate := cfg.FirstUpdateM > 0 && m.inc.UpdateCount == 0 && m.now.Sub(m.inc.OpenTime) > time.Duration(cfg.FirstUpdateM)*time.Minute
				m.inc.StaleNotifyTime = ptr(m.now)
				elapsed := humanDuration(m.now.Sub(m.inc.LastActivity))
				m.event(EventStale, EventDetail{Text: "已 " + elapsed + " 无新公告或备注"})
				n := notice{action: NotifyStale, dedup: fmt.Sprint(m.now.Unix()), noGroups: true, extra: map[string]any{"elapsed": elapsed, "need_update": needUpdate}}
				if m.inc.CommanderID != "" {
					n.users = []string{m.inc.CommanderID}
				} else {
					n.noGroups = false
				}
				m.notify(n)
				return nil
			})
			if err != nil && !errors.Is(err, errNoOp) {
				uc.d.Logger.ErrorContext(ctx, "incident inspector: stale", "incident", it.ID, "error", err)
				continue
			}
			done++
		}
	}
	return done, nil
}

// expireProposals 过期的待审批建议。
func (uc *InspectorUsecase) expireProposals(ctx context.Context, _ *Settings) (int, error) {
	items, err := uc.d.Proposals.ListExpired(ctx, uc.now(), inspectBatch)
	if err != nil {
		return 0, err
	}
	done := 0
	for _, p := range items {
		if err := uc.proposals.expire(ctx, p); err != nil {
			uc.d.Logger.ErrorContext(ctx, "incident inspector: expire proposal", "proposal", p.ID, "error", err)
			continue
		}
		done++
	}
	return done, nil
}

// reviewDue 复盘到期提醒：到期前 1 天与到期当天各一次。
func (uc *InspectorUsecase) reviewDue(ctx context.Context, settings *Settings) (int, error) {
	done := 0
	now := uc.now()
	for stage, before := range map[int32]time.Time{reviewRemindDue: now.Add(24 * time.Hour), reviewRemindOverdue: now} {
		items, err := uc.d.Incidents.Inspect(ctx, InspectQuery{
			ReviewStatuses: []int32{ReviewPending, ReviewDrafting}, ReviewDueBefore: &before, ReviewRemindLT: stage, ExcludeDrill: true,
		}, inspectBatch)
		if err != nil {
			return done, err
		}
		for _, it := range items {
			_, err := uc.inc.mutate(ctx, it.ID, ActorSystem, "", func(m *mutation) error {
				if m.inc.ReviewRemind >= stage || m.inc.ReviewStatus == ReviewDone || m.inc.ReviewStatus == ReviewNone {
					return errNoOp
				}
				m.inc.ReviewRemind = stage
				overdue := stage == reviewRemindOverdue
				m.event(EventReview, EventDetail{Text: map[bool]string{true: "复盘已超期", false: "复盘即将到期"}[overdue]})
				n := notice{action: NotifyReviewDue, dedup: fmt.Sprint(stage), noGroups: true, extra: map[string]any{"overdue": overdue}}
				if settings.Personal(NotifyReviewDue) {
					n.users = uc.inc.roleUsers(m)
				}
				m.notify(n)
				return nil
			})
			if err != nil && !errors.Is(err, errNoOp) {
				uc.d.Logger.ErrorContext(ctx, "incident inspector: review due", "incident", it.ID, "error", err)
				continue
			}
			done++
		}
	}
	return done, nil
}

// autoClose 无复盘要求的故障恢复 N 天后自动关闭。
func (uc *InspectorUsecase) autoClose(ctx context.Context, settings *Settings) (int, error) {
	before := uc.now().Add(-time.Duration(settings.AutoCloseDays) * 24 * time.Hour)
	items, err := uc.d.Incidents.Inspect(ctx, InspectQuery{Statuses: []int32{StatusResolved}, ReviewStatuses: []int32{ReviewNone, ReviewDone}, ResolveBefore: &before}, inspectBatch)
	if err != nil {
		return 0, err
	}
	done := 0
	for _, it := range items {
		_, err := uc.inc.mutate(ctx, it.ID, ActorSystem, "", func(m *mutation) error {
			if m.inc.Status != StatusResolved {
				return errNoOp
			}
			return uc.inc.transition(ctx, m, access{admin: true}, TransitionInput{Action: ActionClose, Note: fmt.Sprintf("恢复后 %d 天自动关闭", settings.AutoCloseDays)}, settings)
		})
		if err != nil && !errors.Is(err, errNoOp) {
			uc.d.Logger.ErrorContext(ctx, "incident inspector: auto close", "incident", it.ID, "error", err)
			continue
		}
		done++
	}
	return done, nil
}

// expireCandidates 过期的候选故障。
func (uc *InspectorUsecase) expireCandidates(ctx context.Context, settings *Settings) (int, error) {
	return uc.d.Candidates.Expire(ctx, uc.now().Add(-time.Duration(settings.CandidateTTLMinutes)*time.Minute))
}

// syncItems 改进项工单状态同步。
func (uc *InspectorUsecase) syncItems(ctx context.Context, _ *Settings) (int, error) {
	return uc.postmortems.syncItemTickets(ctx)
}

// releaseLeases 过期的 Agent 租约。
func (uc *InspectorUsecase) releaseLeases(ctx context.Context, _ *Settings) (int, error) {
	now := uc.now()
	items, err := uc.d.Incidents.Inspect(ctx, InspectQuery{Statuses: []int32{StatusOpen, StatusResponding, StatusMitigated}, AgentLeaseBefore: &now}, inspectBatch)
	if err != nil {
		return 0, err
	}
	done := 0
	for _, it := range items {
		_, err := uc.inc.mutate(ctx, it.ID, ActorSystem, "", func(m *mutation) error {
			if m.inc.AgentOwner == "" || m.inc.AgentLeaseUntil == nil || m.inc.AgentLeaseUntil.After(m.now) {
				return errNoOp
			}
			m.event(EventAgentLease, EventDetail{UserID: m.inc.AgentOwner, Text: "Agent 租约到期"})
			m.inc.AgentOwner, m.inc.AgentLeaseUntil = "", nil
			return nil
		})
		if err != nil && !errors.Is(err, errNoOp) {
			continue
		}
		done++
	}
	return done, nil
}
