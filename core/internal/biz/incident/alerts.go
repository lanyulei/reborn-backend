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
			// 先查属主再写：告警属于别的活跃故障时，INSERT 会撞部分唯一索引 uk_incident_alert_active，
			// PostgreSQL 事务随即进入 aborted 状态，此后的语句（移出、留痕、再关联）必然以 25P02
			// 失败——「迁移告警」此前因此恒返回 500。
			other, err := uc.d.Links.ActiveIncident(ctx, ref.ID)
			if err != nil {
				return err
			}
			if other != "" && other != m.inc.ID {
				if !move {
					return ErrAlertLinked
				}
				// 从原故障移出再关联到本故障，两侧都留痕。
				if _, err := uc.d.Links.DetachTx(ctx, m.tx, other, ref.ID); err != nil {
					return err
				}
				if err := uc.d.Events.AppendTx(ctx, m.tx, &Event{
					ID: mustID(), IncidentID: other, Type: EventAlertDetach, ActorType: ActorUser, ActorID: userID,
					Detail: EventDetail{AlertIDs: []string{ref.ID}, AlertCount: 1, IncidentID: m.inc.ID, IncidentNumber: m.inc.Number, Text: "移到 " + m.inc.Number},
				}); err != nil {
					return err
				}
			}
			ok, err := uc.attachOne(ctx, m, ref, LinkManual, userID)
			if err != nil {
				// 查属主与写入之间仍撞唯一索引，只可能是并发抢占：事务已中止，不能再继续写，
				// 原样返回（409），由调用方重试。
				return err
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

// alertReader 一次请求内的告警可见性判定：监控域的数据范围（分组树 + 角色授权）到第一次真正要判断时才解析，
// 且只解析一次——此前每条告警各解析一次。没有告警要判断时不解析，不产生读取。
type alertReader struct {
	alerts Alerts
	userID string
	can    func(*AlertRef) bool
}

func (r *alertReader) canRead(ctx context.Context, a *AlertRef) (bool, error) {
	if r.can == nil {
		can, err := r.alerts.Reader(ctx, r.userID)
		if err != nil {
			return false, err
		}
		r.can = can
	}
	return r.can(a), nil
}

func (uc *IncidentUsecase) alertReader(userID string) *alertReader {
	return &alertReader{alerts: uc.d.Alerts, userID: userID}
}

// alertItems 补齐告警当前状态与可见性。
func (uc *IncidentUsecase) alertItems(ctx context.Context, userID string, links []*AlertLink) ([]*AlertItem, error) {
	if len(links) == 0 {
		return []*AlertItem{}, nil
	}
	reader := uc.alertReader(userID)
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
			ok, err := reader.canRead(ctx, it.Alert)
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

// candidatePreview 候选故障列表里每个候选展示的告警数。
const candidatePreview = 5

// candidateViews 候选故障的展示信息。一页候选要展示的告警合并成一次批量读取、可见性范围只解析一次
// （此前每个候选各读一次告警、每条告警各解析一次数据范围）；每个候选得到的内容与逐个组装相同。
func (uc *IncidentUsecase) candidateViews(ctx context.Context, userID string, items []*Candidate) ([]*CandidateView, error) {
	ns := &nameSet{}
	var alertIDs []string
	for _, c := range items {
		ns.rule(c.RuleID)
		ns.incident(c.IncidentID)
		alertIDs = append(alertIDs, c.AlertIDs[:min(len(c.AlertIDs), candidatePreview)]...)
	}
	if err := uc.resolve(ctx, ns); err != nil {
		return nil, err
	}
	byID := map[string]*AlertRef{}
	if alertIDs = uniq(alertIDs); len(alertIDs) > 0 {
		alerts, err := uc.d.Alerts.ListByIDs(ctx, alertIDs)
		if err != nil {
			return nil, err
		}
		for _, a := range alerts {
			byID[a.ID] = a
		}
	}
	reader := uc.alertReader(userID)
	out := make([]*CandidateView, 0, len(items))
	for _, c := range items {
		v := &CandidateView{Candidate: c, RuleName: ns.get("rule", c.RuleID), IncidentNumber: ns.get("incident", c.IncidentID)}
		preview := c.AlertIDs[:min(len(c.AlertIDs), candidatePreview)]
		v.Alerts = make([]*AlertItem, 0, len(preview))
		for _, id := range preview {
			// 候选里只存告警 ID：关联信息只有 ID，没有资源，资源名称恒为空（与此前相同）。
			it := &AlertItem{Link: &AlertLink{AlertID: id}, Alert: byID[id]}
			if it.Alert != nil && userID != "" {
				ok, err := reader.canRead(ctx, it.Alert)
				if err != nil {
					return nil, err
				}
				it.CanView = ok
			}
			v.Alerts = append(v.Alerts, it)
		}
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
	refs, err := uc.promotableRefs(ctx, c.AlertIDs, false)
	if err != nil {
		return nil, err
	}
	if len(refs) == 0 {
		// 桶里的告警都已归了别的活跃故障：没有能带进新故障的，不开故障。
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
		if !ErrCandidateMissing.Is(err) {
			return nil, err
		}
		// 故障已经开出，候选只是在开单期间被并发地处置了（忽略 / 关联器自动转正 / 过期），「置为转正」的围栏未命中：
		// 不算失败——此前原样返回 404，用户看到一次失败的转正，刷新后却多出一条故障。
		uc.d.Logger.WarnContext(ctx, "incident: candidate handled elsewhere while being promoted", "candidate", c.ID, "incident", inc.ID)
	}
	views, err := uc.views(ctx, userID, []*Incident{inc}, false)
	if err != nil {
		return nil, err
	}
	return views[0], nil
}

// promotableRefs 候选桶里还能带进新故障的告警（关联器自动转正与手动转正共用）：已被别的活跃故障关联的
// 不算——带着它开故障会以 ErrAlertLinked 整体失败，这个候选永远转不了正。
//
// activeOnly 为真（关联器自动转正）时已恢复、被屏蔽 / 抑制的也不算。手动转正不滤这些：是否把已经恢复的
// 告警补录进故障由用户看着候选列表决定，口径与此前一致——滤掉的话桶里的告警恰好都已恢复时候选就再也
// 转不了正（返回 404）。
func (uc *IncidentUsecase) promotableRefs(ctx context.Context, alertIDs []string, activeOnly bool) ([]*AlertRef, error) {
	refs, err := uc.d.Alerts.ListByIDs(ctx, alertIDs)
	if err != nil {
		return nil, err
	}
	if activeOnly {
		refs = slices.DeleteFunc(refs, func(r *AlertRef) bool { return r.Status != 1 || r.Muted || r.Inhibited })
	}
	free := refs[:0]
	for _, r := range refs {
		other, err := uc.d.Links.ActiveIncident(ctx, r.ID)
		if err != nil {
			return nil, err
		}
		if other == "" {
			free = append(free, r)
		}
	}
	return free, nil
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
	// 候选故障在转正之前不属于任何故障（没有故障时间线可记），表上也没有可落备注的列：谁忽略了、为什么，
	// 至少记进日志——此前操作者与说明被直接丢弃，无从追溯。
	uc.d.Logger.InfoContext(ctx, "incident: candidate dismissed",
		"candidate", c.ID, "rule", c.RuleID, "correlation_key", c.CorrelationKey, "alerts", len(c.AlertIDs), "operator", userID, "note", cleanText(note))
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
	// 仍在告警中的一律算未恢复，被屏蔽 / 抑制的也算：屏蔽 / 抑制只是不通知、不参与归并，告警本身没有恢复。
	// 此前用 CountFiring（不含被屏蔽 / 抑制的），一条告警被屏蔽、其余恢复时故障会被当成「全部恢复」而自动
	// 缓解 / 恢复。关联器开单「被屏蔽 / 抑制的不开单」的口径不变。
	unresolved, err := uc.d.Alerts.AnyUnresolved(ctx, ids)
	if err != nil {
		return err
	}
	if unresolved {
		return nil // 仍有告警中的告警
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
		resolved := EventDetail{AlertCount: i32(len(links)), Text: "关联告警已全部恢复"}
		switch mode {
		case AutoResolveMitigate:
			if inc.Status != StatusMitigated {
				m.event(EventAlertResolved, resolved)
				return uc.transition(ctx, m, access{admin: true}, TransitionInput{Action: ActionMitigate, Note: "关联告警已全部恢复，自动缓解"}, settings)
			}
		case AutoResolveResolve:
			m.event(EventAlertResolved, resolved)
			return uc.transition(ctx, m, access{admin: true}, TransitionInput{Action: ActionResolve, Note: "关联告警已全部恢复，自动恢复"}, settings)
		}
		// 只提示，或此前已自动缓解过：告警抖动时每个恢复信号都会走到这里（再次触发的告警已属于本故障，被关联器
		// 直接丢弃、不产生新关联），上一次记录之后关联没有变化就不重复记录，也不写回——否则时间线被同一条
		//「全部恢复」刷屏，版本号随之递增，用户随后的编辑会撞上版本冲突。
		recorded, err := uc.alertsAllResolvedRecorded(ctx, inc.ID)
		if err != nil {
			return err
		}
		if recorded {
			return errNoOp
		}
		m.event(EventAlertResolved, resolved)
		return nil
	})
	if err != nil && err != errNoOp {
		return err
	}
	return nil
}

// resolvedLookback 判重时回看的时间线条数。
const resolvedLookback = 50

// alertsAllResolvedRecorded 最近一次记下「关联告警已全部恢复」之后，关联集合没有变化（没有新的关联、移出，
// 也没有合并 / 风暴归并进来的告警）。只回看最近 resolvedLookback 条：更早的记不到就按「没记录过」处理，
// 至多多记一条，不会漏记。
func (uc *IncidentUsecase) alertsAllResolvedRecorded(ctx context.Context, incidentID string) (bool, error) {
	events, err := uc.d.Events.Recent(ctx, incidentID, nil, resolvedLookback)
	if err != nil {
		return false, err
	}
	for i := len(events) - 1; i >= 0; i-- {
		switch events[i].Type {
		case EventAlertResolved:
			return true, nil
		case EventAlertAttach, EventAlertDetach, EventMergedFrom, EventStorm:
			return false, nil
		}
	}
	return false, nil
}

// autoReopenIfNeeded 自动恢复后 30 分钟内同键告警再次触发：自动重开。
//
// 只重开由系统恢复的故障（关联告警全部恢复后的自动恢复）。人手动恢复的不动——那是指挥 / 处理人确认过的
// 结论，自动改回处理中会抹掉这次恢复并计入重开率；返回 false 后调用方按「没有可归并的活跃故障」继续走
// 候选 / 开单（已恢复的故障不占用告警，见 AlertLinkRepo.ActiveIncident / AttachTx），告警不会落空。
func (uc *IncidentUsecase) autoReopenIfNeeded(ctx context.Context, inc *Incident, settings *Settings) (bool, error) {
	if inc.Status != StatusResolved || inc.ResolveTime == nil || uc.now().Sub(*inc.ResolveTime) > 30*time.Minute {
		return false, nil
	}
	_, err := uc.mutate(ctx, inc.ID, ActorSystem, "", func(m *mutation) error {
		if m.inc.Status != StatusResolved {
			return errNoOp
		}
		// 在故障行锁下判断：改状态的写入都要先拿这把锁，读到的最近一次流转就是这次恢复。
		auto, err := uc.resolvedBySystem(ctx, m.inc.ID)
		if err != nil {
			return err
		}
		if !auto {
			return errNoOp
		}
		return uc.transition(ctx, m, access{admin: true}, TransitionInput{Action: ActionReopen, Note: "恢复后同一归并键的告警再次触发，自动重开"}, settings)
	})
	if err == errNoOp {
		return false, nil
	}
	return err == nil, err
}

// resolveLookback 判断恢复方式时回看的时间线条数。自动重开只发生在恢复后 30 分钟内，这次恢复的流转记录
// 之后攒不出这么多条。
const resolveLookback = 200

// resolvedBySystem 已恢复故障的这次恢复是否由系统完成：看时间线上最近一条状态流转——每次流转都记一条
// 带操作者类型的 EventStatus，不需要另存标记。回看范围内找不到、或最近一条不是「恢复」的，一律按
// 「不是」处理：宁可不重开（告警照常走候选 / 开单），也不去动一条说不清怎么恢复的故障。
func (uc *IncidentUsecase) resolvedBySystem(ctx context.Context, incidentID string) (bool, error) {
	events, err := uc.d.Events.Recent(ctx, incidentID, nil, resolveLookback)
	if err != nil {
		return false, err
	}
	for i := len(events) - 1; i >= 0; i-- {
		if ev := events[i]; ev.Type == EventStatus {
			return ev.Detail.To == StatusResolved && ev.ActorType == ActorSystem, nil
		}
	}
	return false, nil
}
