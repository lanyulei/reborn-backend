package biz

import (
	"context"
	"errors"
	"slices"
	"testing"
)

func newPromoteHarness(candidate *Candidate, alerts map[string]*AlertRef, owner map[string]string) (*IncidentUsecase, *mapWriter, *stubLinks, *stubCandidates) {
	writer := &mapWriter{byID: map[string]*Incident{}}
	links := &stubLinks{owner: owner}
	candidates := &stubCandidates{c: candidate}
	d := &Deps{
		Txm: fakeTxm{}, Writer: writer, Members: fakeMembers{}, Admins: fakeAdmins{}, Events: &recordingEvents{}, Links: links,
		Dir: fakeDirectory{}, Candidates: candidates,
		Rules:  stubRules{rule: &Rule{ID: "r1", Spec: RuleSpec{Name: "数据库慢查询", Incident: RuleIncident{Severity: SeverityS3}}}},
		Alerts: &stubAlerts{byID: alerts}, Notify: &fakeNotify{}, Webhooks: fakeWebhookRepo{}, Logger: testLogger(),
	}
	return NewIncidentUsecase(d, NewSettingsUsecase(fakeSettingRepo{})), writer, links, candidates
}

// 候选里有告警已被别的活跃故障关联：转正时先滤掉。此前 open 以 ErrAlertLinked 整体失败，这个候选永远
// 转不了正。已恢复的告警照旧带进故障——手动转正是否补录由用户决定（关联器自动转正才只取告警中的）。
func TestPromoteCandidateSkipsAlertsThatCannotBeLinked(t *testing.T) {
	t.Parallel()
	candidate := &Candidate{ID: "c1", RuleID: "r1", CorrelationKey: "k", Status: CandidateOpen, AlertIDs: []string{"a1", "a2", "a3"}}
	alerts := map[string]*AlertRef{
		"a1": {ID: "a1", Status: 1, RuleName: "慢查询"},
		"a2": {ID: "a2", Status: 1}, // 已被别的活跃故障关联
		"a3": {ID: "a3", Status: 2}, // 已恢复：手动转正仍可补录
	}
	uc, writer, links, candidates := newPromoteHarness(candidate, alerts, map[string]string{"a2": "iX"})

	v, err := uc.PromoteCandidate(context.Background(), "u1", "c1", "", 0)
	if err != nil {
		t.Fatalf("PromoteCandidate() error = %v", err)
	}
	if !slices.Equal(links.attached, []string{"a1", "a3"}) {
		t.Fatalf("attached alerts = %v, want every alert that is free to link", links.attached)
	}
	if len(writer.created) != 1 {
		t.Fatalf("incidents created = %d, want 1", len(writer.created))
	}
	opened := writer.created[0]
	if opened.RuleID != "r1" || opened.CorrelationKey != "k" || opened.Source != SourceManual || v.ID != opened.ID {
		t.Fatalf("opened incident = rule %q key %q source %d (view %q); want the candidate's rule and key", opened.RuleID, opened.CorrelationKey, opened.Source, v.ID)
	}
	if !slices.Equal(candidates.statuses, []int32{CandidatePromoted}) {
		t.Fatalf("candidate statuses = %v, want promoted", candidates.statuses)
	}
}

// 桶里的告警都已归了别的活跃故障：没有可转正的，不开故障。
func TestPromoteCandidateFailsWhenNoAlertCanBePromoted(t *testing.T) {
	t.Parallel()
	candidate := &Candidate{ID: "c1", RuleID: "r1", CorrelationKey: "k", Status: CandidateOpen, AlertIDs: []string{"a1", "a2"}}
	alerts := map[string]*AlertRef{"a1": {ID: "a1", Status: 1}, "a2": {ID: "a2", Status: 1}}
	uc, writer, links, candidates := newPromoteHarness(candidate, alerts, map[string]string{"a1": "iX", "a2": "iX"})

	if _, err := uc.PromoteCandidate(context.Background(), "u1", "c1", "", 0); !ErrCandidateMissing.Is(err) {
		t.Fatalf("PromoteCandidate() error = %v, want ErrCandidateMissing", err)
	}
	if len(writer.created) != 0 || len(links.attached) != 0 || len(candidates.statuses) != 0 {
		t.Fatalf("nothing to promote must not open an incident: created %d, attached %v, statuses %v",
			len(writer.created), links.attached, candidates.statuses)
	}
}

// 开单期间候选被并发地处置了（忽略 / 关联器自动转正 / 过期）：「置为转正」的围栏未命中，但故障已经开出，
// 不能再报 404。
func TestPromoteCandidateSucceedsWhenCandidateHandledConcurrently(t *testing.T) {
	t.Parallel()
	candidate := &Candidate{ID: "c1", RuleID: "r1", CorrelationKey: "k", Status: CandidateOpen, AlertIDs: []string{"a1"}}
	uc, writer, _, candidates := newPromoteHarness(candidate, map[string]*AlertRef{"a1": {ID: "a1", Status: 1}}, nil)
	candidates.setErr = ErrCandidateMissing

	v, err := uc.PromoteCandidate(context.Background(), "u1", "c1", "", 0)
	if err != nil || v == nil {
		t.Fatalf("PromoteCandidate() = %v, %v; want the opened incident", v, err)
	}
	if len(writer.created) != 1 || !slices.Equal(candidates.statuses, []int32{CandidatePromoted}) {
		t.Fatalf("created %d, statuses %v; want one incident and one promote attempt", len(writer.created), candidates.statuses)
	}
}

// 置状态时的其他错误（数据库故障）照常返回。
func TestPromoteCandidatePropagatesOtherStatusErrors(t *testing.T) {
	t.Parallel()
	candidate := &Candidate{ID: "c1", RuleID: "r1", CorrelationKey: "k", Status: CandidateOpen, AlertIDs: []string{"a1"}}
	uc, _, _, candidates := newPromoteHarness(candidate, map[string]*AlertRef{"a1": {ID: "a1", Status: 1}}, nil)
	candidates.setErr = errBoom

	if _, err := uc.PromoteCandidate(context.Background(), "u1", "c1", "", 0); !errors.Is(err, errBoom) {
		t.Fatalf("PromoteCandidate() error = %v, want errBoom", err)
	}
}
