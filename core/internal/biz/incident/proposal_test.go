package biz

import (
	"context"
	"testing"
	"time"
)

// 自动批准的两条规则都以「未熔断 + 等级允许自动批准」为前提；人提出的中风险建议不能绕过。
func TestAutoApproved(t *testing.T) {
	t.Parallel()
	settings := DefaultSettings()
	noAuto := DefaultSettings()
	noAuto.Severities[SeverityS3] = SeveritySetting{AllowAutoApprove: false}
	inc := func(sev int32, autoApprove bool) *Incident { return &Incident{Severity: sev, AutoApprove: autoApprove} }
	cases := []struct {
		name         string
		approval     int32
		proposer     int32
		risk         int32
		inc          *Incident
		settings     *Settings
		wantApproved bool
	}{
		{"agent low risk", ApprovalDirect, ActorAgent, RiskLow, inc(SeverityS3, true), settings, true},
		{"user medium risk", ApprovalDirect, ActorUser, RiskMedium, inc(SeverityS3, true), settings, true},
		{"user medium risk, existing approval", ApprovalExisting, ActorUser, RiskMedium, inc(SeverityS4, true), settings, true},
		{"user high risk", ApprovalDirect, ActorUser, RiskHigh, inc(SeverityS3, true), settings, false},
		{"agent medium risk", ApprovalDirect, ActorAgent, RiskMedium, inc(SeverityS3, true), settings, false},
		{"commander approval required", ApprovalCommander, ActorUser, RiskLow, inc(SeverityS3, true), settings, false},
		{"tripped incident, low risk", ApprovalDirect, ActorUser, RiskLow, inc(SeverityS3, false), settings, false},
		{"tripped incident, user medium risk", ApprovalDirect, ActorUser, RiskMedium, inc(SeverityS3, false), settings, false},
		{"S2 never auto", ApprovalDirect, ActorUser, RiskLow, inc(SeverityS2, true), settings, false},
		{"severity disallows, low risk", ApprovalDirect, ActorUser, RiskLow, inc(SeverityS3, true), noAuto, false},
		{"severity disallows, user medium risk", ApprovalDirect, ActorUser, RiskMedium, inc(SeverityS3, true), noAuto, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if got := autoApproved(c.approval, c.proposer, c.risk, c.inc, c.settings); got != c.wantApproved {
				t.Fatalf("autoApproved() = %v, want %v", got, c.wantApproved)
			}
		})
	}
}

// 已恢复 / 已关闭 / 已合并的故障上不能再审批建议（批准会把动作送进发件箱执行）。
func TestProposalDecideRejectsInactiveIncident(t *testing.T) {
	t.Parallel()
	for _, status := range []int32{StatusResolved, StatusClosed, StatusMerged} {
		proposals := &fakeProposals{get: &Proposal{ID: "p1", IncidentID: "i1", Status: ProposalPending}}
		d := &Deps{
			Txm: fakeTxm{}, Writer: fakeWriter{inc: &Incident{ID: "i1", Status: status, CommanderID: "u1"}},
			Members: fakeMembers{}, Admins: fakeAdmins{}, Proposals: proposals, Logger: testLogger(),
		}
		settings := NewSettingsUsecase(fakeSettingRepo{})
		inc := &IncidentUsecase{d: d, settings: settings, now: time.Now}
		uc := NewProposalUsecase(d, inc, settings)

		_, err := uc.Decide(context.Background(), "u1", "i1", "p1", true, "")
		if !ErrStateInvalid.Is(err) {
			t.Fatalf("status %d: Decide() error = %v, want ErrStateInvalid", status, err)
		}
		if proposals.lockCalls != 0 {
			t.Fatalf("status %d: proposal must not be locked once the incident is inactive", status)
		}
	}
}

// 故障恢复 / 关闭 / 合并时，待审批与「已批准但尚未执行」的建议都要取消：已批准的留着会在故障结束后仍被动作 worker 提交执行。
func TestCancelPendingProposalsIncludesApproved(t *testing.T) {
	t.Parallel()
	proposals := &fakeProposals{list: []*Proposal{
		{ID: "p1", IncidentID: "i1", Status: ProposalPending},
		{ID: "p2", IncidentID: "i1", Status: ProposalApproved},
	}}
	uc := &IncidentUsecase{d: &Deps{Proposals: proposals, Logger: testLogger()}, now: time.Now}
	m := &mutation{uc: uc, tx: fakeTx{}, inc: &Incident{ID: "i1", Status: StatusResolved}, now: time.Now()}

	if err := uc.cancelPendingProposals(context.Background(), m, "故障已恢复"); err != nil {
		t.Fatalf("cancelPendingProposals() error = %v", err)
	}
	want := []proposalSave{{id: "p1", from: ProposalPending, to: ProposalCancelled}, {id: "p2", from: ProposalApproved, to: ProposalCancelled}}
	if len(proposals.saves) != len(want) || proposals.saves[0] != want[0] || proposals.saves[1] != want[1] {
		t.Fatalf("saves = %+v, want %+v", proposals.saves, want)
	}
	if len(m.events) != 2 {
		t.Fatalf("events = %d, want one cancellation event per proposal", len(m.events))
	}
}

// 已批准的建议轮到执行时故障已不再活跃：不提交作业 / 流程（Jobs / Flows 未装配，走到提交会空指针），建议置为已取消。
func TestProposalExecuteCancelsWhenIncidentInactive(t *testing.T) {
	t.Parallel()
	inc := &Incident{ID: "i1", Status: StatusResolved}
	proposals := &fakeProposals{get: &Proposal{ID: "p1", IncidentID: "i1", Status: ProposalApproved, Action: ProposalAction{Kind: "job", Ref: "t1"}}}
	d := &Deps{Txm: fakeTxm{}, Incidents: fakeIncidents{inc: inc}, Writer: fakeWriter{inc: inc}, Members: fakeMembers{}, Events: fakeEvents{}, Proposals: proposals, Logger: testLogger()}
	uc := NewProposalUsecase(d, &IncidentUsecase{d: d, now: time.Now}, nil)

	if err := uc.execute(context.Background(), "p1"); err != nil {
		t.Fatalf("execute() error = %v", err)
	}
	if len(proposals.saves) != 1 || proposals.saves[0] != (proposalSave{id: "p1", from: ProposalApproved, to: ProposalCancelled}) {
		t.Fatalf("saves = %+v, want p1 approved → cancelled", proposals.saves)
	}
}

// 触发规则的自动处置只按预案第一个可执行步骤提一条建议，不是把每个步骤都提一遍。
func TestAutoProposeSubmitsFirstStepOnly(t *testing.T) {
	t.Parallel()
	pb := &Playbook{ID: "pb", Spec: PlaybookSpec{Name: "重启", Enabled: true, Steps: []PlaybookStep{
		{Name: "第一步", Kind: "workflow", Ref: "w1", Approval: ApprovalCommander, Risk: RiskMedium},
		{Name: "第二步", Kind: "workflow", Ref: "w2", Approval: ApprovalCommander, Risk: RiskMedium},
	}}}
	proposals := &fakeProposals{}
	d := &Deps{Playbooks: fakePlaybooks{pb: pb}, Links: fakeLinks{}, Flows: fakeFlows{}, Proposals: proposals, Logger: testLogger()}
	uc := &IncidentUsecase{d: d, settings: NewSettingsUsecase(fakeSettingRepo{}), now: time.Now}
	m := &mutation{uc: uc, tx: fakeTx{}, inc: &Incident{ID: "i1", Status: StatusOpen, Severity: SeverityS3}, now: time.Now()}

	if err := uc.autoPropose(context.Background(), m, &playbookTrigger{PlaybookID: "pb", Mode: PlaybookPropose}, DefaultSettings()); err != nil {
		t.Fatalf("autoPropose() error = %v", err)
	}
	if len(proposals.created) != 1 || proposals.created[0].StepIndex != 0 || proposals.created[0].Action.Ref != "w1" {
		t.Fatalf("created = %d proposals (first step %+v), want exactly one for step 0", len(proposals.created), proposals.created)
	}
}
