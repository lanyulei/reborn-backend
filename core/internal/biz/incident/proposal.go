package biz

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
)

// 动作建议（功能设计 §5.3 / §8.3）：人 / AI / Agent 想「做一件事」的统一入口。
// 提出 → 审批（指挥 / 自动）→ 执行（动作发件箱，以预案代理执行人身份经作业 / 流程域）→ 回写 → 验证窗口 → 熔断。

// ProposalUsecase 动作建议。
type ProposalUsecase struct {
	d        *Deps
	inc      *IncidentUsecase
	settings *SettingsUsecase
	now      func() time.Time
}

// NewProposalUsecase new a ProposalUsecase.
func NewProposalUsecase(d *Deps, inc *IncidentUsecase, settings *SettingsUsecase) *ProposalUsecase {
	return &ProposalUsecase{d: d, inc: inc, settings: settings, now: time.Now}
}

// ProposalView 建议及其展示信息。
type ProposalView struct {
	*Proposal
	PlaybookName  string
	ProposerName  string
	DecidedByName string
	ExecUserName  string
	CIRefs        []Named
	CanDecide     bool
	ExecURL       string
}

// ProposalInput 提出建议入参。
type ProposalInput struct {
	PlaybookID string
	StepIndex  int32
	Action     *ProposalAction
	Title      string
	Reason     string
	Evidence   []string
	Approve    bool
	// AutoEligible Agent 申请自动通过：只在提出者持有效租约且为机器账号时生效，其余忽略。
	AutoEligible bool
	// AutoMaxTargets Agent 申请自动通过时允许的靶标资源数上限（0 = 不限）。
	AutoMaxTargets int32
}

// idemKey 幂等键：动作种类 + 对象 + 参数 + 靶标。
func idemKey(a ProposalAction) string {
	keys := make([]string, 0, len(a.Params))
	for k := range a.Params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(a.Kind + "|" + a.Ref)
	for _, k := range keys {
		b.WriteString("|" + k + "=" + a.Params[k])
	}
	cis := slices.Clone(a.CIIDs)
	sort.Strings(cis)
	b.WriteString("|" + strings.Join(cis, ","))
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:16])
}

// renderParams 参数映射：{{severity}} {{number}} {{business}} {{title}} {{labels.x}} {{ci.attrs.x}}（取第一台受影响资源）。
func renderParams(params map[string]string, inc *Incident, alert *AlertRef, ci *CICard, business string) map[string]any {
	out := make(map[string]any, len(params))
	for k, v := range params {
		s := v
		s = strings.ReplaceAll(s, "{{severity}}", SeverityCode(inc.Severity))
		s = strings.ReplaceAll(s, "{{number}}", inc.Number)
		s = strings.ReplaceAll(s, "{{title}}", inc.Title)
		s = strings.ReplaceAll(s, "{{business}}", business)
		s = strings.ReplaceAll(s, "{{incident_id}}", inc.ID)
		if alert != nil {
			for lk, lv := range alert.Labels {
				s = strings.ReplaceAll(s, "{{labels."+lk+"}}", lv)
			}
		}
		if ci != nil {
			s = strings.ReplaceAll(s, "{{ci.name}}", ci.Name)
			for ak, av := range ci.Attrs {
				s = strings.ReplaceAll(s, "{{ci.attrs."+ak+"}}", fmt.Sprint(av))
			}
		}
		out[k] = s
	}
	return out
}

// targetsFor 预案步骤的作业靶标：只能是 CMDB 资源（受影响资源或其上游）。
func (uc *ProposalUsecase) targetsFor(ctx context.Context, inc *Incident, mode int32) ([]string, error) {
	switch mode {
	case TargetTemplate:
		return nil, nil
	case TargetUpstream:
		var out []string
		for _, ci := range inc.AffectedCIIDs[:min(len(inc.AffectedCIIDs), 50)] {
			ups, err := uc.d.CMDB.Upstream(ctx, ci)
			if err != nil {
				return nil, err
			}
			out = append(out, ups...)
		}
		return uniq(out), nil
	}
	return slices.Clone(inc.AffectedCIIDs), nil
}

// allowedTargets 靶标必须在受影响资源或其上游之内（告警标签里的 IP 不能直接成为靶标）。
func (uc *ProposalUsecase) allowedTargets(ctx context.Context, inc *Incident, ciIDs []string) error {
	if len(ciIDs) == 0 {
		return nil
	}
	allowed := map[string]struct{}{}
	for _, ci := range inc.AffectedCIIDs {
		allowed[ci] = struct{}{}
	}
	// 受影响资源的上游集合只算一次（首次遇到不在受影响资源内的靶标时）。
	upstreamLoaded := false
	for _, ci := range ciIDs {
		if _, ok := allowed[ci]; ok {
			continue
		}
		if !upstreamLoaded {
			upstreamLoaded = true
			for _, affected := range inc.AffectedCIIDs[:min(len(inc.AffectedCIIDs), 50)] {
				ups, err := uc.d.CMDB.Upstream(ctx, affected)
				if err != nil {
					return err
				}
				for _, up := range ups {
					allowed[up] = struct{}{}
				}
			}
		}
		if _, ok := allowed[ci]; !ok {
			return detail(ErrProposalInvalid, "target %s is not an affected resource or its upstream", ci)
		}
	}
	// Cards 一次至多读 MaxCardsPerLookup 个：靶标去重后超过它时整批比较长度会恒判「不存在」
	//（上游并集几百个是常见拓扑），按它分批校验。
	for batch := range slices.Chunk(uniq(ciIDs), MaxCardsPerLookup) {
		cards, err := uc.d.CMDB.Cards(ctx, batch)
		if err != nil {
			return err
		}
		if len(cards) != len(batch) {
			return detail(ErrProposalInvalid, "some targets do not exist in CMDB")
		}
	}
	return nil
}

// Create 提出建议（处理人以上；approve=true 须指挥）。
func (uc *ProposalUsecase) Create(ctx context.Context, userID, incidentID string, in ProposalInput) (*ProposalView, error) {
	settings, err := uc.settings.Get(ctx)
	if err != nil {
		return nil, err
	}
	var created *Proposal
	_, err = uc.inc.mutate(ctx, incidentID, ActorUser, userID, func(m *mutation) error {
		a, err := uc.inc.accessOf(ctx, userID, m.inc, m.members)
		if err != nil {
			return err
		}
		actorType := ActorUser
		if m.inc.AgentOwner == userID && m.inc.AgentLeaseUntil != nil && m.inc.AgentLeaseUntil.After(m.now) {
			actorType = ActorAgent
			m.actorType = ActorAgent
		} else if !a.canHandle() {
			return ErrForbidden
		}
		if in.Approve && !a.canManage() {
			return ErrForbidden
		}
		p, err := uc.build(ctx, m.inc, in, actorType, userID, settings)
		if err != nil {
			return err
		}
		if actorType == ActorAgent {
			if err := uc.applyAgentAuto(ctx, m.tx, m.inc, p, in, userID, settings); err != nil {
				return err
			}
		}
		if in.Approve && actorType == ActorUser {
			p.Status, p.DecidedBy, p.DecidedTime = ProposalApproved, userID, ptr(m.now)
		}
		if err := uc.submit(ctx, m, p, settings); err != nil {
			return err
		}
		created = p
		return nil
	})
	if err != nil {
		return nil, err
	}
	return uc.view(ctx, userID, created)
}

// build 组装一条建议（校验动作对象与靶标，计算风险与幂等键）。
func (uc *ProposalUsecase) build(ctx context.Context, inc *Incident, in ProposalInput, actorType int32, actorID string, settings *Settings) (*Proposal, error) {
	if !IsActive(inc.Status) {
		return nil, ErrStateInvalid
	}
	p := &Proposal{
		IncidentID: inc.ID, Title: cleanText(in.Title), Reason: cleanText(in.Reason), Evidence: in.Evidence, Risk: RiskMedium,
		Status: ProposalPending, ProposerType: actorType, ProposerID: actorID, ExpireTime: uc.now().Add(time.Duration(settings.ProposalTTLMinutes) * time.Minute),
	}
	if actorType == ActorSystem {
		p.ProposerID = ""
	}
	business := ""
	if inc.BusinessID != "" {
		if b, err := uc.d.Businesses.Get(ctx, inc.BusinessID); err == nil {
			business = b.Name
		}
	}
	var firstAlert *AlertRef
	if links, err := uc.d.Links.ListAll(ctx, inc.ID, 1); err == nil && len(links) > 0 {
		if refs, err := uc.d.Alerts.ListByIDs(ctx, []string{links[0].AlertID}); err == nil && len(refs) == 1 {
			firstAlert = refs[0]
		}
	}
	var firstCI *CICard
	if len(inc.AffectedCIIDs) > 0 {
		if cards, err := uc.d.CMDB.Cards(ctx, inc.AffectedCIIDs[:1]); err == nil && len(cards) == 1 {
			firstCI = cards[0]
		}
	}
	var approval int32
	agentUser := ""
	switch {
	case in.PlaybookID != "":
		pb, err := uc.d.Playbooks.Get(ctx, in.PlaybookID)
		if err != nil {
			return nil, err
		}
		if !pb.Spec.Enabled || int(in.StepIndex) >= len(pb.Spec.Steps) {
			return nil, detail(ErrProposalInvalid, "playbook step is not available")
		}
		st := pb.Spec.Steps[in.StepIndex]
		targets, err := uc.targetsFor(ctx, inc, st.TargetMode)
		if err != nil {
			return nil, err
		}
		if st.Kind == "job" && st.TargetMode != TargetTemplate && len(targets) == 0 {
			return nil, detail(ErrProposalInvalid, "the incident has no affected resource to target")
		}
		rendered := renderParams(st.Params, inc, firstAlert, firstCI, business)
		params := make(map[string]string, len(rendered))
		for k, v := range rendered {
			params[k] = fmt.Sprint(v)
		}
		p.Action = ProposalAction{Kind: st.Kind, Ref: st.Ref, Params: params, CIIDs: targets}
		p.PlaybookID, p.StepIndex, p.Risk, approval, agentUser = pb.ID, in.StepIndex, st.Risk, st.Approval, pb.Spec.AgentUserID
		if p.Title == "" {
			p.Title = pb.Spec.Name + " · " + st.Name
		}
	case in.Action != nil:
		p.Action = *in.Action
		p.Action.CIIDs = uniq(p.Action.CIIDs)
		if p.Action.Kind == "job" {
			if err := uc.allowedTargets(ctx, inc, p.Action.CIIDs); err != nil {
				return nil, err
			}
		}
		// 直接给出的动作：风险按最高档处理、须指挥审批、以提出者本人身份执行。
		p.Risk, approval, agentUser = RiskHigh, ApprovalCommander, actorID
		if actorType != ActorUser {
			return nil, detail(ErrProposalInvalid, "agents can only propose playbook steps")
		}
	default:
		return nil, detail(ErrProposalInvalid, "a playbook step or an action is required")
	}
	if p.Action.Kind == "job" && len(p.Action.CIIDs) > 0 {
		if err := uc.allowedTargets(ctx, inc, p.Action.CIIDs); err != nil {
			return nil, err
		}
	}
	name, err := uc.actionName(ctx, agentUser, p.Action.Kind, p.Action.Ref)
	if err != nil {
		return nil, err
	}
	if name == "" {
		return nil, detail(ErrProposalInvalid, "%s %q is not available", p.Action.Kind, p.Action.Ref)
	}
	p.Action.RefName = name
	if p.Title == "" {
		p.Title = name
	}
	p.Title = truncate(p.Title, 255)
	p.ExecUser = agentUser
	p.IdemKey = idemKey(p.Action)
	p.Auto = autoApproved(approval, p.ProposerType, p.Risk, inc, settings)
	return p, nil
}

// autoApproved 建议能否自动批准。两条规则都以「预案步骤不要求指挥审批 + 故障未熔断 + 等级允许自动批准（S1/S2 一律人工）」
// 为前提：（1）任何提出者 + 低风险；（2）人提出 + 中风险以下——人已经看过预案，不必再等指挥。
func autoApproved(approval, proposerType, risk int32, inc *Incident, settings *Settings) bool {
	if approval == ApprovalCommander || !inc.AutoApprove || inc.Severity < SeverityS3 || !settings.Sev(inc.Severity).AllowAutoApprove {
		return false
	}
	if risk == RiskLow {
		return true
	}
	return proposerType == ActorUser && risk <= RiskMedium
}

// applyAgentAuto 运维 Agent 提出的建议能否自动通过：在 build 的通用判定之上，再要求（1）提出者申请了自动且是机器账号
// （auto_eligible 只有机器账号可写），（2）故障侧的数据条件——同一资源同一预案步骤在冷却期内没有自动执行过、
// 全局与同业务自动执行中的建议未达上限。任一不满足即转人工，并把原因记进依据，审批者看得见。
func (uc *ProposalUsecase) applyAgentAuto(ctx context.Context, tx Tx, inc *Incident, p *Proposal, in ProposalInput, userID string, settings *Settings) error {
	requested := in.AutoEligible
	if requested && uc.d.Accounts != nil {
		machine, err := uc.d.Accounts.IsMachine(ctx, userID)
		if err != nil {
			return err
		}
		p.AutoEligible = machine
	}
	if !p.Auto {
		return nil
	}
	manual := func(reason string) {
		p.Auto = false
		p.Evidence = append(p.Evidence, "转人工审批："+reason)
	}
	if !p.AutoEligible {
		manual("Agent 未申请自动通过")
		return nil
	}
	if in.AutoMaxTargets > 0 && len(p.Action.CIIDs) > int(in.AutoMaxTargets) {
		manual(fmt.Sprintf("靶标资源 %d 个，超过 Agent 允许自动执行的上限 %d", len(p.Action.CIIDs), in.AutoMaxTargets))
		return nil
	}
	if p.PlaybookID != "" && settings.AgentAutoCooldownMinutes > 0 {
		last, err := uc.d.Proposals.LastAutoExec(ctx, p.Action.CIIDs, p.PlaybookID, p.StepIndex)
		if err != nil {
			return err
		}
		if last != nil && uc.now().Sub(*last) < time.Duration(settings.AgentAutoCooldownMinutes)*time.Minute {
			manual(fmt.Sprintf("同一资源的该预案步骤 %d 分钟内已自动执行过", settings.AgentAutoCooldownMinutes))
			return nil
		}
	}
	// 配额检查与随后的自动批准写入在同一把全局咨询锁下：只锁故障行挡不住别的故障上并发提出的 Agent 建议。
	if err := uc.d.Proposals.LockAutoExecTx(ctx, tx); err != nil {
		return err
	}
	global, business, err := uc.d.Proposals.CountAutoRunning(ctx, inc.BusinessID)
	if err != nil {
		return err
	}
	if settings.AgentAutoMaxRunning > 0 && global >= int(settings.AgentAutoMaxRunning) {
		manual(fmt.Sprintf("全局自动执行中的建议已达 %d 条", settings.AgentAutoMaxRunning))
		return nil
	}
	if inc.BusinessID != "" && settings.AgentAutoMaxRunningPerBusiness > 0 && business >= int(settings.AgentAutoMaxRunningPerBusiness) {
		manual(fmt.Sprintf("同一业务自动执行中的建议已达 %d 条", settings.AgentAutoMaxRunningPerBusiness))
	}
	return nil
}

// actionName 动作对象是否可用（以执行人身份）。
func (uc *ProposalUsecase) actionName(ctx context.Context, execUser, kind, ref string) (string, error) {
	switch kind {
	case "job":
		t, err := uc.d.Jobs.Template(ctx, execUser, ref)
		if err != nil {
			if isNotFound(err) || kratosCodeIs(err, 403) {
				return "", nil
			}
			return "", err
		}
		if t == nil || !t.Enabled {
			return "", nil
		}
		return t.Name, nil
	case "workflow":
		name, ok, err := uc.d.Flows.Startable(ctx, ref)
		if err != nil || !ok {
			return "", err
		}
		return name, nil
	}
	return "", detail(ErrProposalInvalid, "action kind must be job or workflow")
}

// submit 落库 + 时间线 + 通知；已批准（含自动批准）的立即入动作发件箱。
func (uc *ProposalUsecase) submit(ctx context.Context, m *mutation, p *Proposal, settings *Settings) error {
	id, err := idgen.NewID()
	if err != nil {
		return err
	}
	p.ID = id
	if p.Auto && p.Status == ProposalPending {
		p.Status, p.DecidedTime, p.DecisionNote = ProposalApproved, ptr(m.now), "自动批准（低风险）"
	}
	if err := uc.d.Proposals.CreateTx(ctx, m.tx, p); err != nil {
		return err
	}
	m.event(EventProposal, EventDetail{ProposalID: p.ID, ProposalTitle: p.Title, Text: p.Reason, To: p.Status})
	m.hook("proposal", map[string]any{"proposal_id": p.ID, "title": p.Title, "status": p.Status, "risk": p.Risk, "auto": p.Auto})
	if p.Status == ProposalApproved {
		m.action(ActionProposalExec, p.ID, nil)
		return nil
	}
	n := notice{action: NotifyProposal, dedup: p.ID, noGroups: true, extra: map[string]any{
		"proposal_title": p.Title, "risk_label": RiskLabel(p.Risk), "text": p.Reason,
	}}
	if settings.Personal(NotifyProposal) && m.inc.CommanderID != "" {
		n.users = []string{m.inc.CommanderID}
	}
	m.notify(n)
	return nil
}

// autoPropose 触发规则配置的自动处置：开故障时按预案第一个可执行步骤提出一条建议（mode=3 且低风险时自动批准），
// 后续步骤由处理人按预案手动推进；组装失败（靶标缺失、对象不可用等）的步骤跳过，取下一个。
func (uc *IncidentUsecase) autoPropose(ctx context.Context, m *mutation, trig *playbookTrigger, settings *Settings) error {
	pu := &ProposalUsecase{d: uc.d, inc: uc, settings: uc.settings, now: uc.now}
	pb, err := uc.d.Playbooks.Get(ctx, trig.PlaybookID)
	if err != nil || !pb.Spec.Enabled || len(pb.Spec.Steps) == 0 {
		return nil // 预案不可用：只跳过自动处置，不影响开故障
	}
	for i := range pb.Spec.Steps {
		p, err := pu.build(ctx, m.inc, ProposalInput{PlaybookID: pb.ID, StepIndex: i32(i), Reason: "触发规则自动处置"}, ActorSystem, "", settings)
		if err != nil {
			uc.d.Logger.WarnContext(ctx, "incident: auto propose skipped", "incident", m.inc.ID, "playbook", pb.ID, "step", i, "error", err)
			continue
		}
		if trig.Mode != PlaybookAuto {
			p.Auto = false
		}
		if err := pu.submit(ctx, m, p, settings); err != nil && !ErrProposalDuplicate.Is(err) {
			return err
		}
		return nil // 同样的动作已有建议也算提出过：第一个可执行步骤只提一条
	}
	return nil
}

// Decide 指挥批准 / 拒绝。
func (uc *ProposalUsecase) Decide(ctx context.Context, userID, incidentID, proposalID string, approved bool, note string) (*ProposalView, error) {
	var out *Proposal
	_, err := uc.inc.mutate(ctx, incidentID, ActorUser, userID, func(m *mutation) error {
		a, err := uc.inc.accessOf(ctx, userID, m.inc, m.members)
		if err != nil {
			return err
		}
		if !a.canManage() {
			return ErrForbidden
		}
		if !IsActive(m.inc.Status) {
			return ErrStateInvalid
		}
		p, err := uc.d.Proposals.LockTx(ctx, m.tx, proposalID)
		if err != nil {
			return err
		}
		if p.IncidentID != incidentID {
			return ErrProposalNotFound
		}
		if p.Status != ProposalPending {
			return ErrProposalNotPending
		}
		p.DecidedBy, p.DecidedTime, p.DecisionNote = userID, ptr(m.now), cleanText(note)
		if approved {
			p.Status = ProposalApproved
			m.action(ActionProposalExec, p.ID, nil)
		} else {
			p.Status = ProposalRejected
		}
		if _, err := uc.d.Proposals.SaveTx(ctx, m.tx, p, ProposalPending); err != nil {
			return err
		}
		m.event(EventProposalDecision, EventDetail{ProposalID: p.ID, ProposalTitle: p.Title, To: p.Status, Text: p.DecisionNote})
		m.hook("proposal", map[string]any{"proposal_id": p.ID, "status": p.Status})
		out = p
		return nil
	})
	if err != nil {
		return nil, err
	}
	return uc.view(ctx, userID, out)
}

// Cancel 提出者（或指挥）撤回待审批的建议。
func (uc *ProposalUsecase) Cancel(ctx context.Context, userID, incidentID, proposalID string) (*ProposalView, error) {
	var out *Proposal
	_, err := uc.inc.mutate(ctx, incidentID, ActorUser, userID, func(m *mutation) error {
		a, err := uc.inc.accessOf(ctx, userID, m.inc, m.members)
		if err != nil {
			return err
		}
		p, err := uc.d.Proposals.LockTx(ctx, m.tx, proposalID)
		if err != nil {
			return err
		}
		if p.IncidentID != incidentID {
			return ErrProposalNotFound
		}
		if p.ProposerID != userID && !a.canManage() {
			return ErrForbidden
		}
		if p.Status != ProposalPending {
			return ErrProposalNotPending
		}
		p.Status = ProposalCancelled
		if _, err := uc.d.Proposals.SaveTx(ctx, m.tx, p, ProposalPending); err != nil {
			return err
		}
		m.event(EventProposalDecision, EventDetail{ProposalID: p.ID, ProposalTitle: p.Title, To: p.Status, Text: "撤回"})
		out = p
		return nil
	})
	if err != nil {
		return nil, err
	}
	return uc.view(ctx, userID, out)
}

// ResetAutoApprove 指挥恢复自动批准（熔断后）。
func (uc *ProposalUsecase) ResetAutoApprove(ctx context.Context, userID, incidentID string) error {
	_, err := uc.inc.userMutate(ctx, userID, incidentID, func(m *mutation, a access) error {
		if !a.canManage() {
			return ErrForbidden
		}
		if m.inc.AutoApprove {
			return nil
		}
		m.inc.AutoApprove = true
		m.event(EventAutoApprove, EventDetail{Text: "指挥恢复了自动批准"})
		return nil
	})
	return err
}

// List 故障的建议。
func (uc *ProposalUsecase) List(ctx context.Context, userID, incidentID string, status int32) ([]*ProposalView, error) {
	inc, err := uc.d.Incidents.Get(ctx, incidentID)
	if err != nil {
		return nil, err
	}
	items, err := uc.d.Proposals.List(ctx, incidentID, status)
	if err != nil {
		return nil, err
	}
	return uc.views(ctx, userID, inc, items)
}

func (uc *ProposalUsecase) view(ctx context.Context, userID string, p *Proposal) (*ProposalView, error) {
	inc, err := uc.d.Incidents.Get(ctx, p.IncidentID)
	if err != nil {
		return nil, err
	}
	views, err := uc.views(ctx, userID, inc, []*Proposal{p})
	if err != nil {
		return nil, err
	}
	return views[0], nil
}

func (uc *ProposalUsecase) views(ctx context.Context, userID string, inc *Incident, items []*Proposal) ([]*ProposalView, error) {
	admin, err := uc.d.Admins.IsAdmin(ctx, userID)
	if err != nil {
		return nil, err
	}
	canDecide := admin || (inc.CommanderID != "" && inc.CommanderID == userID)
	ns := &nameSet{}
	var playbooks []string
	for _, p := range items {
		ns.user(p.ProposerID, p.DecidedBy, p.ExecUser)
		ns.ci(p.Action.CIIDs...)
		playbooks = append(playbooks, p.PlaybookID)
	}
	if err := uc.inc.resolve(ctx, ns); err != nil {
		return nil, err
	}
	pbNames := map[string]string{}
	if ids := uniq(playbooks); len(ids) > 0 {
		pbs, err := uc.d.Playbooks.ListByIDs(ctx, ids)
		if err != nil {
			return nil, err
		}
		for _, pb := range pbs {
			pbNames[pb.ID] = pb.Spec.Name
		}
	}
	out := make([]*ProposalView, 0, len(items))
	for _, p := range items {
		v := &ProposalView{
			Proposal: p, PlaybookName: pbNames[p.PlaybookID], ProposerName: ns.get("user", p.ProposerID), DecidedByName: ns.get("user", p.DecidedBy),
			ExecUserName: ns.get("user", p.ExecUser), CIRefs: ns.named("ci", p.Action.CIIDs), CanDecide: canDecide && p.Status == ProposalPending,
		}
		switch {
		case p.ExecKind == "job" && p.ExecRef != "":
			v.ExecURL = "/job/execution/" + p.ExecRef
		case p.ExecKind == "workflow" && p.ExecRef != "":
			v.ExecURL = "/workflow/instance/" + p.ExecRef
		}
		out = append(out, v)
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// 执行、轮询结果、验证、熔断（动作 worker 与巡检调用）
// ---------------------------------------------------------------------------

// execute 执行一条已批准的建议：以执行人身份提交作业 / 发起流程，回写执行对象。
func (uc *ProposalUsecase) execute(ctx context.Context, proposalID string) error {
	p, err := uc.d.Proposals.Get(ctx, proposalID)
	if err != nil {
		return err
	}
	if p.Status != ProposalApproved {
		return nil
	}
	if p.ExecUser != "" {
		active, err := uc.d.Dir.ActiveUserIDs(ctx, []string{p.ExecUser})
		if err != nil {
			return err
		}
		if len(active) != 1 {
			return uc.finish(ctx, p, ProposalFailed, "执行人已停用", nil)
		}
	}
	inc, err := uc.d.Incidents.Get(ctx, p.IncidentID)
	if err != nil {
		return err
	}
	if !IsActive(inc.Status) {
		// 已批准但故障在执行前恢复 / 关闭 / 合并了：不再提交，建议置为已取消。
		return uc.cancelApproved(ctx, p, "故障已不再活跃，未执行")
	}
	params := make(map[string]any, len(p.Action.Params))
	for k, v := range p.Action.Params {
		params[k] = v
	}
	requestID := "incident-proposal-" + p.ID
	switch p.Action.Kind {
	case "job":
		exec, err := uc.d.Jobs.Submit(ctx, p.ExecUser, JobSubmit{TemplateID: p.Action.Ref, Params: params, CIIDs: p.Action.CIIDs, RequestID: requestID})
		if err != nil {
			if kratosCodeIs(err, 400) || kratosCodeIs(err, 403) || kratosCodeIs(err, 404) || kratosCodeIs(err, 409) {
				return uc.finish(ctx, p, ProposalFailed, "提交失败："+errMessage(err), nil)
			}
			return err
		}
		p.ExecKind, p.ExecRef = "job", exec.ID
		if exec.Finished {
			return uc.finish(ctx, p, statusOf(exec.Succeeded), exec.Summary, nil)
		}
	case "workflow":
		id, err := uc.d.Flows.Start(ctx, WorkflowStart{
			DefinitionKey: p.Action.Ref, BusinessID: inc.ID, Title: inc.Number + " " + p.Title, Initiator: p.ExecUser, RequestID: requestID,
			Variables: map[string]any{"incident_id": inc.ID, "incident_number": inc.Number, "incident_title": inc.Title, "severity": inc.Severity, "params": params},
		})
		if err != nil {
			if kratosCodeIs(err, 400) || kratosCodeIs(err, 403) || kratosCodeIs(err, 404) || kratosCodeIs(err, 409) {
				return uc.finish(ctx, p, ProposalFailed, "发起失败："+errMessage(err), nil)
			}
			return err
		}
		p.ExecKind, p.ExecRef = "workflow", id
	default:
		return uc.finish(ctx, p, ProposalFailed, "未知的动作类型", nil)
	}
	p.Status = ProposalRunning
	return uc.d.Txm.WithinTx(ctx, func(tx Tx) error {
		ok, err := uc.d.Proposals.SaveTx(ctx, tx, p, ProposalApproved)
		if err != nil {
			return err
		}
		if !ok {
			// 围栏未命中：提交作业 / 流程期间故障已被恢复 / 关闭 / 合并，建议被置为已取消。作业已经提交出去
			//（RequestID 幂等，重放不会重复提交），但建议不再跟踪它的结果——不能再写回「执行中」，
			// 也不追加「执行中」时间线（否则后续结果永远不回写，时间线上多出一条悬空的执行中）。
			uc.d.Logger.WarnContext(ctx, "incident: proposal cancelled during submission, result will not be tracked",
				"proposal", p.ID, "incident", p.IncidentID, "exec_kind", p.ExecKind, "exec_ref", p.ExecRef)
			return nil
		}
		return uc.d.Events.AppendTx(ctx, tx, &Event{
			ID: mustID(), IncidentID: p.IncidentID, Type: EventProposalResult, ActorType: ActorSystem,
			Detail: EventDetail{ProposalID: p.ID, ProposalTitle: p.Title, To: ProposalRunning, ExecKind: p.ExecKind, ExecRef: p.ExecRef},
		})
	})
}

func statusOf(ok bool) int32 {
	if ok {
		return ProposalSucceeded
	}
	return ProposalFailed
}

// poll 轮询执行中的建议的结果。
func (uc *ProposalUsecase) poll(ctx context.Context, p *Proposal) error {
	switch p.ExecKind {
	case "job":
		exec, err := uc.d.Jobs.Execution(ctx, p.ExecRef)
		if err != nil {
			if isNotFound(err) {
				return uc.finish(ctx, p, ProposalFailed, "执行记录不存在", nil)
			}
			return err
		}
		if !exec.Finished {
			return nil
		}
		return uc.finish(ctx, p, statusOf(exec.Succeeded), exec.Summary, nil)
	case "workflow":
		status, err := uc.d.Flows.InstanceStatus(ctx, p.ExecRef)
		if err != nil {
			if isNotFound(err) {
				return uc.finish(ctx, p, ProposalFailed, "流程实例不存在", nil)
			}
			return err
		}
		switch status {
		case 1:
			return nil
		case 2:
			return uc.finish(ctx, p, ProposalSucceeded, "流程已完成", nil)
		default:
			return uc.finish(ctx, p, ProposalFailed, "流程未通过或被终止", nil)
		}
	}
	return uc.finish(ctx, p, ProposalFailed, "未知的执行类型", nil)
}

// finish 终结一条建议：回写状态与结果、时间线；失败且为自动批准的触发熔断；成功的开启验证窗口。
func (uc *ProposalUsecase) finish(ctx context.Context, p *Proposal, status int32, result string, verify *VerifyResult) error {
	settings, err := uc.settings.Get(ctx)
	if err != nil {
		return err
	}
	from := p.Status
	_, err = uc.inc.mutate(ctx, p.IncidentID, ActorSystem, "", func(m *mutation) error {
		p.Status, p.Result = status, truncate(result, 2000)
		if status == ProposalSucceeded && verify == nil {
			p.VerifyUntil = ptr(m.now.Add(time.Duration(settings.VerifyWindowMinutes) * time.Minute))
			// 记录验证基线：当前活跃告警数。
			n, err := uc.inc.activeAlerts(ctx, m.inc.ID)
			if err != nil {
				return err
			}
			p.VerifyResult = nil
			m.action(ActionVerify, p.ID, map[string]any{"alerts_before": n})
		}
		if verify != nil {
			p.VerifyResult = verify
		}
		ok, err := uc.d.Proposals.SaveTx(ctx, m.tx, p, from)
		if err != nil {
			return err
		}
		if !ok {
			// 围栏未命中：另一轮轮询已先终结它，不重复记时间线 / 出站箱 / 熔断。
			return errNoOp
		}
		m.event(EventProposalResult, EventDetail{ProposalID: p.ID, ProposalTitle: p.Title, To: status, Text: p.Result, ExecKind: p.ExecKind, ExecRef: p.ExecRef})
		m.hook("proposal", map[string]any{"proposal_id": p.ID, "status": status, "result": p.Result})
		if status == ProposalFailed && p.Auto && m.inc.AutoApprove {
			uc.trip(m, "自动执行的动作「"+p.Title+"」失败")
		}
		return nil
	})
	if err == errNoOp {
		return nil
	}
	return err
}

// trip 熔断：本故障后续建议全部人工审批，并通知指挥。
func (uc *ProposalUsecase) trip(m *mutation, reason string) {
	m.inc.AutoApprove = false
	m.event(EventAutoApprove, EventDetail{Text: "自动处置已熔断：" + reason})
	m.notify(notice{
		action: NotifyStale, dedup: "trip-" + fmt.Sprint(m.now.Unix()), noGroups: true, users: uniq([]string{m.inc.CommanderID}),
		extra: map[string]any{"text": "自动处置已熔断：" + reason, "elapsed": "", "need_update": false},
	})
}

// verify 验证窗口到期：比较告警数，恶化则熔断。
func (uc *ProposalUsecase) verify(ctx context.Context, p *Proposal, before int32) error {
	after, err := uc.inc.activeAlerts(ctx, p.IncidentID)
	if err != nil {
		return err
	}
	vr := &VerifyResult{AlertsBefore: before, AlertsAfter: after, Resolved: max(before-after, 0), NewAlerts: max(after-before, 0), Worse: after > before, CheckedTime: uc.now()}
	_, err = uc.inc.mutate(ctx, p.IncidentID, ActorSystem, "", func(m *mutation) error {
		p.VerifyResult = vr
		ok, err := uc.d.Proposals.SaveVerifiedTx(ctx, m.tx, p)
		if err != nil {
			return err
		}
		if !ok {
			// 围栏未命中：另一次领取已先写入验证结果，不重复记时间线 / 熔断。
			return errNoOp
		}
		text := fmt.Sprintf("验证窗口结束：活跃告警 %d → %d", before, after)
		m.event(EventProposalVerify, EventDetail{ProposalID: p.ID, ProposalTitle: p.Title, Text: text})
		if vr.Worse && p.Auto && m.inc.AutoApprove {
			uc.trip(m, "动作「"+p.Title+"」执行后告警增加")
		}
		return nil
	})
	if errors.Is(err, errNoOp) {
		return nil
	}
	return err
}

// expire 把过期的待审批建议置为已过期（巡检调用）。
func (uc *ProposalUsecase) expire(ctx context.Context, p *Proposal) error {
	_, err := uc.inc.mutate(ctx, p.IncidentID, ActorSystem, "", func(m *mutation) error {
		p.Status = ProposalExpired
		ok, err := uc.d.Proposals.SaveTx(ctx, m.tx, p, ProposalPending)
		if err != nil {
			return err
		}
		if !ok {
			return errNoOp
		}
		m.event(EventProposalDecision, EventDetail{ProposalID: p.ID, ProposalTitle: p.Title, To: ProposalExpired, Text: "超时未审批"})
		return nil
	})
	if err == errNoOp {
		return nil
	}
	return err
}

// cancelApproved 已批准但尚未开始执行的建议置为已取消（故障已不再活跃）：在故障写事务内重读并围栏在「已批准」，
// 与 cancelPendingProposals 在同一把故障行锁下互斥；状态已变（已被取消或已在执行）则不动。
func (uc *ProposalUsecase) cancelApproved(ctx context.Context, p *Proposal, reason string) error {
	_, err := uc.inc.mutate(ctx, p.IncidentID, ActorSystem, "", func(m *mutation) error {
		cur, err := uc.d.Proposals.LockTx(ctx, m.tx, p.ID)
		if err != nil {
			return err
		}
		if cur.Status != ProposalApproved {
			return errNoOp
		}
		cur.Status, cur.DecisionNote = ProposalCancelled, reason
		ok, err := uc.d.Proposals.SaveTx(ctx, m.tx, cur, ProposalApproved)
		if err != nil {
			return err
		}
		if !ok {
			return errNoOp
		}
		m.event(EventProposalDecision, EventDetail{ProposalID: cur.ID, ProposalTitle: cur.Title, To: ProposalCancelled, Text: reason})
		return nil
	})
	if errors.Is(err, errNoOp) {
		return nil
	}
	return err
}

// cancelPendingProposals 故障不再活跃（恢复 / 关闭 / 合并）时把待审批与已批准但尚未执行的建议全部置为已取消：建议只能在
// 活跃故障上审批与执行，待审批的留着只会在到期时变成「超时未审批」，已批准的则会在故障结束后仍被动作 worker 提交执行。
// 在调用方的故障写事务内执行（故障行锁已持有，建议的增删与状态推进都在该锁下），事务内读全量而不是接口分页的前 200 条。
func (uc *IncidentUsecase) cancelPendingProposals(ctx context.Context, m *mutation, reason string) error {
	items, err := uc.d.Proposals.ListByStatusTx(ctx, m.tx, m.inc.ID, []int32{ProposalPending, ProposalApproved})
	if err != nil {
		return err
	}
	for _, p := range items {
		from := p.Status
		p.Status, p.DecidedTime, p.DecisionNote = ProposalCancelled, ptr(m.now), reason
		ok, err := uc.d.Proposals.SaveTx(ctx, m.tx, p, from)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		m.event(EventProposalDecision, EventDetail{ProposalID: p.ID, ProposalTitle: p.Title, To: ProposalCancelled, Text: reason})
	}
	return nil
}

// errMessage 取 Kratos 错误的说明（没有则原样）。
func errMessage(err error) string {
	if err == nil {
		return ""
	}
	return truncate(err.Error(), 500)
}

// mustID UUIDv7；熵源不可用是进程级故障，这里退回空串由数据库拒绝写入。
func mustID() string {
	id, err := idgen.NewID()
	if err != nil {
		return ""
	}
	return id
}
