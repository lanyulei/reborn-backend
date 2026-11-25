package biz

import (
	"context"
	"testing"
	"time"
)

// 作业已经提交，但写回「执行中」时围栏未命中（提交期间故障被恢复 / 关闭 / 合并，建议已被取消）：
// 作业结果不再跟踪，不能再追加「执行中」时间线，也不算执行失败。
func TestProposalExecuteSkipsRunningEventWhenFencedOut(t *testing.T) {
	t.Parallel()
	inc := &Incident{ID: "i1", Status: StatusOpen}
	proposals := &fencedProposals{fakeProposals: &fakeProposals{
		get: &Proposal{ID: "p1", IncidentID: "i1", Status: ProposalApproved, Action: ProposalAction{Kind: "job", Ref: "t1"}},
	}}
	events := &recordingEvents{}
	jobs := &fakeJobs{}
	d := &Deps{Txm: fakeTxm{}, Incidents: fakeIncidents{inc: inc}, Events: events, Proposals: proposals, Jobs: jobs, Logger: testLogger()}
	uc := NewProposalUsecase(d, &IncidentUsecase{d: d, now: time.Now}, nil)

	if err := uc.execute(context.Background(), "p1"); err != nil {
		t.Fatalf("execute() error = %v, want nil (the job was submitted, only the bookkeeping lost the fence)", err)
	}
	if len(jobs.submits) != 1 {
		t.Fatalf("job submits = %d, want 1", len(jobs.submits))
	}
	if len(events.appended) != 0 {
		t.Fatalf("timeline events = %d, want none once the save is fenced out", len(events.appended))
	}
	if len(proposals.saves) != 1 || proposals.saves[0] != (proposalSave{id: "p1", from: ProposalApproved, to: ProposalRunning}) {
		t.Fatalf("saves = %+v, want p1 fenced on approved", proposals.saves)
	}
}

// 围栏命中：照常写回并追加「执行中」时间线。
func TestProposalExecuteAppendsRunningEvent(t *testing.T) {
	t.Parallel()
	inc := &Incident{ID: "i1", Status: StatusOpen}
	proposals := &fakeProposals{
		get: &Proposal{ID: "p1", IncidentID: "i1", Status: ProposalApproved, Action: ProposalAction{Kind: "job", Ref: "t1"}},
	}
	events := &recordingEvents{}
	d := &Deps{Txm: fakeTxm{}, Incidents: fakeIncidents{inc: inc}, Events: events, Proposals: proposals, Jobs: &fakeJobs{}, Logger: testLogger()}
	uc := NewProposalUsecase(d, &IncidentUsecase{d: d, now: time.Now}, nil)

	if err := uc.execute(context.Background(), "p1"); err != nil {
		t.Fatalf("execute() error = %v", err)
	}
	if len(events.appended) != 1 || events.appended[0].Type != EventProposalResult {
		t.Fatalf("timeline events = %+v, want one proposal_result", events.appended)
	}
	detail := events.appended[0].Detail
	if detail.To != ProposalRunning || detail.ProposalID != "p1" || detail.ExecKind != "job" || detail.ExecRef != "e1" {
		t.Fatalf("running event = %+v, want p1 running with exec job/e1", detail)
	}
}
