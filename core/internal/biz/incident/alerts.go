package biz

import (
	"context"
	"slices"
	"time"

	"github.com/lanyulei/reborn/backend/kit/paging"
)

// 关联告警（功能设计 §3 / §5）。一条告警同一时刻只属于一个活跃故障；关联时按监控域数据范围校验调用者可见。

// AttachResult 关联结果。
type AttachResult struct {
	View     *View
	Attached int32
	Skipped  int32
}

// Attach 关联告警到故障；move=true 时把属于别的活跃故障的告警迁移过来。
func (uc *IncidentUsecase) Attach(ctx context.Context, userID, id string, alertIDs []string, move bool) (*AttachResult, error) {
	refs, err := uc.readableAlerts(ctx, userID, alertIDs)
	if err != nil {
		return nil, err
	}
	res := &AttachResult{}
	var inc *Incident
	inc, err = uc.mutate(ctx, id, ActorUser, userID, func(m *mutation) error {
		a, err := uc.accessOf(ctx, userID, m.inc, m.members)
		if err != nil {
			return err
		}
		if !a.canHandle() {
			return ErrForbidden
		}
		if !IsActive(m.inc.Status) && m.inc.Status != StatusResolved {
			return ErrStateInvalid
		}
		count, err := uc.d.Links.Count(ctx, m.inc.ID)
		if err != nil {
			return err
		}
		if count+len(refs) > maxAlertsPerIncident {
			return invalid("an incident can link at most %d alerts", maxAlertsPerIncident)
		}
		var attached []string
		for _, ref := range refs {
			ok, err := uc.attachOne(ctx, m, ref, LinkManual, userID)
			if err != nil {
				if !move || !ErrAlertLinked.Is(err) {
					return err
				}
				// 从原故障移出再关联到本故障，两侧都留痕。
				other, err := uc.d.Links.ActiveIncident(ctx, ref.ID)
				if err != nil {
					return err
				}
				if other == "" {
					return ErrAlertLinked
				}
				if _, err := uc.d.Links.DetachTx(ctx, m.tx, other, ref.ID); err != nil {
					return err
				}
				if err := uc.d.Events.AppendTx(ctx, m.tx, &Event{
					ID: mustID(), IncidentID: other, Type: EventAlertDetach, ActorType: ActorUser, ActorID: userID,
					Detail: EventDetail{AlertIDs: []string{ref.ID}, AlertCount: 1, IncidentID: m.inc.ID, IncidentNumber: m.inc.Number, Text: "移到 " + m.inc.Number},
				}); err != nil {
					return err
				}
				if ok, err = uc.attachOne(ctx, m, ref, LinkManual, userID); err != nil {
					return err
				}
			}
			if ok {
				attached = append(attached, ref.ID)
			} else {
				res.Skipped++
			}
		}
		if len(attached) > 0 {
			res.Attached = i32(len(attached))
			m.event(EventAlertAttach, EventDetail{AlertIDs: attached[:min(len(attached), 50)], AlertCount: i32(len(attached)), To: LinkManual})
			m.hook("alert", map[string]any{"attached": attached})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for _, ref := range refs {
		uc.noteAlert(ctx, ref.ID, userID, inc)
	}
	views, err := uc.views(ctx, userID, []*Incident{inc}, false)
	if err != nil {
		return nil, err
	}
	res.View = views[0]
	return res, nil
}

// Detach 移出告警。
func (uc *IncidentUsecase) Detach(ctx context.Context, userID, id, alertID string) (*View, error) {
	return uc.userMutate(ctx, userID, id, func(m *mutation, a access) error {
		if !a.canHandle() {
			return ErrForbidden
		}
		ok, err := uc.d.Links.DetachTx(ctx, m.tx, m.inc.ID, alertID)
		if err != nil {
			return err
		}
		if !ok {
			return ErrAlertNotFound
		}
		m.event(EventAlertDetach, EventDetail{AlertIDs: []string{alertID}, AlertCount: 1})
		return nil
	})
}

// AlertItem 关联告警及其当前状态。
type AlertItem struct {
	Link    *AlertLink
	Alert   *AlertRef
	CIName  string
	CanView bool
}

// ListAlerts 关联告警（带监控域的当前状态）。
func (uc *IncidentUsecase) ListAlerts(ctx context.Context, userID, id string, page, pageSize int64) ([]*AlertItem, int64, error) {
	if _, err := uc.d.Incidents.Get(ctx, id); err != nil {
		return nil, 0, err
	}
	page, pageSize = paging.Normalize(page, pageSize)
	links, total, err := uc.d.Links.List(ctx, id, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	items, err := uc.alertItems(ctx, userID, links)
	return items, total, err
}

// alertItems 补齐告警当前状态与可见性。
func (uc *IncidentUsecase) alertItems(ctx context.Context, userID string, links []*AlertLink) ([]*AlertItem, error) {
	if len(links) == 0 {
		return []*AlertItem{}, nil
	}
	ids := make([]string, 0, len(links))
	cis := make([]string, 0, len(links))
	for _, l := range links {
		ids = append(ids, l.AlertID)
		if l.CIID != "" {
			cis = append(cis, l.CIID)
		}
	}
	alerts, err := uc.d.Alerts.ListByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	byID := make(map[string]*AlertRef, len(alerts))
	for _, a := range alerts {
		byID[a.ID] = a
	}
	ns := &nameSet{}
	ns.ci(cis...)
	if err := uc.resolve(ctx, ns); err != nil {
		return nil, err
	}
	out := make([]*AlertItem, 0, len(links))
	for _, l := range links {
		it := &AlertItem{Link: l, Alert: byID[l.AlertID], CIName: ns.get("ci", l.CIID)}
		if it.Alert != nil && userID != "" {
			ok, err := uc.d.Alerts.CanRead(ctx, userID, it.Alert)
			if err != nil {
				return nil, err
			}
			it.CanView = ok
		}
		out = append(out, it)
	}
	return out, nil
}

// ByAlert 一条告警所属的故障（活跃的在前）。调用者须能看到这条告警；本接口不走接口授权（告警详情页要展示关联），
// 没有「查询故障详情」权限的人只能看到自己是指挥 / 成员的故障，其余过滤掉。
func (uc *IncidentUsecase) ByAlert(ctx context.Context, userID, alertID string) ([]*View, error) {
	reader, err := uc.d.Perms.CanReadIncidents(ctx, userID)
	if err != nil {
		return nil, err
	}
	ids, err := uc.d.Links.IncidentIDsByAlert(ctx, alertID)
	if err != nil || len(ids) == 0 {
		return []*View{}, err
	}
	items, err := uc.d.Incidents.ListByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	if !reader {
		members, err := uc.d.Members.ListMany(ctx, ids)
		if err != nil {
			return nil, err
		}
		items = slices.DeleteFunc(items, func(inc *Incident) bool {
			if inc.CommanderID == userID {
				return false
			}
			return !slices.ContainsFunc(members[inc.ID], func(m *Member) bool { return m.UserID == userID })
		})
		if len(items) == 0 {
			return []*View{}, nil
		}
	}
	slices.SortStableFunc(items, func(a, b *Incident) int {
		if IsActive(a.Status) != IsActive(b.Status) {
			if IsActive(a.Status) {
				return -1
			}
			return 1
		}
		return b.OpenTime.Compare(a.OpenTime)
	})
	return uc.views(ctx, userID, items, false)
}

// ---------------------------------------------------------------------------
// 候选故障
// ---------------------------------------------------------------------------

// CandidateView 候选故障及其展示信息。
type CandidateView struct {
	*Candidate
	RuleName       string
	IncidentNumber string
	Alerts         []*AlertItem
}

// ListCandidates 候选故障。
func (uc *IncidentUsecase) ListCandidates(ctx context.Context, userID string, status int32, page, pageSize int64) ([]*CandidateView, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	items, total, err := uc.d.Candidates.List(ctx, status, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	views, err := uc.candidateViews(ctx, userID, items)
	return views, total, err
}

func (uc *IncidentUsecase) candidateViews(ctx context.Context, userID string, items []*Candidate) ([]*CandidateView, error) {
	ns := &nameSet{}
	for _, c := range items {
		ns.rule(c.RuleID)
		ns.incident(c.IncidentID)
	}
	if err := uc.resolve(ctx, ns); err != nil {
		return nil, err
	}
	out := make([]*CandidateView, 0, len(items))
	for _, c := range items {
		v := &CandidateView{Candidate: c, RuleName: ns.get("rule", c.RuleID), IncidentNumber: ns.get("incident", c.IncidentID)}
		links := make([]*AlertLink, 0, min(len(c.AlertIDs), 5))
		for _, id := range c.AlertIDs[:min(len(c.AlertIDs), 5)] {
			links = append(links, &AlertLink{AlertID: id})
		}
		alerts, err := uc.alertItems(ctx, userID, links)
		if err != nil {
			return nil, err
		}
		v.Alerts = alerts
		out = append(out, v)
	}
	return out, nil
}

// PromoteCandidate 把候选故障转正（按规则属性开故障，可覆盖标题与等级）。
func (uc *IncidentUsecase) PromoteCandidate(ctx context.Context, userID, id, title string, severity int32) (*View, error) {
	c, err := uc.d.Candidates.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if c.Status != CandidateOpen {
		return nil, ErrCandidateMissing
	}
	rule, err := uc.d.Rules.Get(ctx, c.RuleID)
	if err != nil {
		return nil, err
	}
	refs, err := uc.d.Alerts.ListByIDs(ctx, c.AlertIDs)
	if err != nil {
		return nil, err
	}
	if len(refs) == 0 {
		return nil, ErrCandidateMissing
	}
	in := uc.ruleCreateInput(ctx, rule, c.CorrelationKey, refs)
	in.Source, in.LinkMethod = SourceManual, LinkManual
	if title = cleanText(title); title != "" {
		in.Title = title
	}
	if ValidSeverity(severity) {
		in.Severity = severity
	}
	inc, err := uc.open(ctx, ActorUser, userID, in)
	if err != nil {
		return nil, err
	}
	if err := uc.d.Txm.WithinTx(ctx, func(tx Tx) error {
		return uc.d.Candidates.SetStatusTx(ctx, tx, c.ID, CandidatePromoted, inc.ID)
	}); err != nil {
		return nil, err
	}
	views, err := uc.views(ctx, userID, []*Incident{inc}, false)
	if err != nil {
		return nil, err
	}
	return views[0], nil
}

// DismissCandidate 忽略候选故障。
func (uc *IncidentUsecase) DismissCandidate(ctx context.Context, userID, id, note string) error {
	c, err := uc.d.Candidates.Get(ctx, id)
	if err != nil {
		return err
	}
	if c.Status != CandidateOpen {
		return ErrCandidateMissing
	}
	_ = userID
	_ = note
	return uc.d.Txm.WithinTx(ctx, func(tx Tx) error {
		return uc.d.Candidates.SetStatusTx(ctx, tx, c.ID, CandidateDismissed, "")
	})
}

// ---------------------------------------------------------------------------
// 告警恢复后的自动处理（关联器调用）
// ---------------------------------------------------------------------------

// onAlertsResolved 告警恢复后检查故障：关联告警全部恢复时按规则配置提示 / 自动缓解 / 自动恢复。
func (uc *IncidentUsecase) onAlertsResolved(ctx context.Context, incidentID string, settings *Settings) error {
	links, err := uc.d.Links.ListAll(ctx, incidentID, maxAlertsPerIncident)
	if err != nil || len(links) == 0 {
		return err
	}
	ids := make([]string, 0, len(links))
	for _, l := range links {
		ids = append(ids, l.AlertID)
	}
	firing, err := uc.d.Alerts.CountFiring(ctx, ids)
	if err != nil {
		return err
	}
	if firing > 0 {
		return nil // 仍有活跃告警
	}
	_, err = uc.mutate(ctx, incidentID, ActorSystem, "", func(m *mutation) error {
		inc := m.inc
		if !IsActive(inc.Status) {
			return errNoOp
		}
		mode := AutoResolveHint
		if inc.RuleID != "" {
			if rule, err := uc.d.Rules.Get(ctx, inc.RuleID); err == nil && rule.Spec.Action.AutoResolve > 0 {
				mode = rule.Spec.Action.AutoResolve
			}
		}
		if !settings.Sev(inc.Severity).AllowAutoResolve || inc.Severity <= SeverityS2 {
			mode = AutoResolveHint
		}
		m.event(EventAlertResolved, EventDetail{AlertCount: i32(len(links)), Text: "关联告警已全部恢复"})
		switch mode {
		case AutoResolveMitigate:
			if inc.Status != StatusMitigated {
				return uc.transition(ctx, m, access{admin: true}, TransitionInput{Action: ActionMitigate, Note: "关联告警已全部恢复，自动缓解"}, settings)
			}
		case AutoResolveResolve:
			return uc.transition(ctx, m, access{admin: true}, TransitionInput{Action: ActionResolve, Note: "关联告警已全部恢复，自动恢复"}, settings)
		}
		return nil
	})
	if err != nil && err != errNoOp {
		return err
	}
	return nil
}

// autoReopenIfNeeded 自动恢复后 30 分钟内同键告警再次触发：自动重开。
func (uc *IncidentUsecase) autoReopenIfNeeded(ctx context.Context, inc *Incident, settings *Settings) (bool, error) {
	if inc.Status != StatusResolved || inc.ResolveTime == nil || uc.now().Sub(*inc.ResolveTime) > 30*time.Minute {
		return false, nil
	}
	_, err := uc.mutate(ctx, inc.ID, ActorSystem, "", func(m *mutation) error {
		if m.inc.Status != StatusResolved {
			return errNoOp
		}
		return uc.transition(ctx, m, access{admin: true}, TransitionInput{Action: ActionReopen, Note: "恢复后同一归并键的告警再次触发，自动重开"}, settings)
	})
	if err == errNoOp {
		return false, nil
	}
	return err == nil, err
}
