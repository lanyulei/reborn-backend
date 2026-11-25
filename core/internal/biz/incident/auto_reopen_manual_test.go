package biz

import (
	"context"
	"slices"
	"testing"
	"time"
)

func statusEvent(actorType, from, to int32) *Event {
	return &Event{Type: EventStatus, ActorType: actorType, Detail: EventDetail{From: from, To: to}}
}

// 只自动重开由系统恢复的故障：看时间线上最近一条状态流转的操作者。手动恢复的、说不清怎么恢复的都不动。
func TestAutoReopenOnlyReopensSystemResolvedIncidents(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name string
		tail []*Event
		want bool
	}{
		{"resolved by the system", []*Event{statusEvent(ActorUser, StatusOpen, StatusResponding), statusEvent(ActorSystem, StatusResponding, StatusResolved), {Type: EventComment, ActorType: ActorUser}}, true},
		{"resolved by a user", []*Event{statusEvent(ActorSystem, StatusOpen, StatusMitigated), statusEvent(ActorUser, StatusMitigated, StatusResolved)}, false},
		{"resolved by an agent", []*Event{statusEvent(ActorAgent, StatusResponding, StatusResolved)}, false},
		// 系统自动恢复过，之后被人重开又手动恢复：以最近一次为准。
		{"system resolve superseded by a manual one", []*Event{statusEvent(ActorSystem, StatusOpen, StatusResolved), statusEvent(ActorUser, StatusResolved, StatusResponding), statusEvent(ActorUser, StatusResponding, StatusResolved)}, false},
		{"no status event in sight", []*Event{{Type: EventComment, ActorType: ActorSystem}}, false},
		{"latest status event is not a resolve", []*Event{statusEvent(ActorSystem, StatusOpen, StatusMitigated)}, false},
	} {
		resolved := now.Add(-10 * time.Minute)
		inc := &Incident{ID: "i1", Number: "INC-1", Status: StatusResolved, ResolveTime: &resolved, RuleID: "r1", CorrelationKey: "k"}
		uc, writer, events, _ := newReopenUsecase(inc, now)
		events.tail = tc.tail

		ok, err := uc.autoReopenIfNeeded(context.Background(), inc, DefaultSettings())
		if err != nil || ok != tc.want {
			t.Fatalf("%s: autoReopenIfNeeded() = %v, %v; want %v", tc.name, ok, err, tc.want)
		}
		if tc.want {
			if inc.Status != StatusResponding || inc.ReopenCount != 1 {
				t.Fatalf("%s: incident = status %d, reopened %d times; want reopened", tc.name, inc.Status, inc.ReopenCount)
			}
			continue
		}
		if inc.Status != StatusResolved || inc.ReopenCount != 0 || inc.ResolveTime == nil || len(writer.saved) != 0 || len(events.appended) != 0 {
			t.Fatalf("%s: a manually resolved incident must be left untouched: status %d, reopened %d, saves %d, events %v",
				tc.name, inc.Status, inc.ReopenCount, len(writer.saved), eventTypes(events.appended))
		}
	}
}

// keyedIncidents 故障读取桩：List 按状态过滤返回给定的那一条（关联器按规则 + 归并键找故障）。
type keyedIncidents struct {
	IncidentReader
	item *Incident
}

func (r keyedIncidents) List(_ context.Context, f IncidentFilter, _, _ int64) ([]*Incident, int64, error) {
	if !slices.Contains(f.Statuses, r.item.Status) {
		return nil, 0, nil
	}
	return []*Incident{r.item}, 1, nil
}
func (r keyedIncidents) Get(context.Context, string) (*Incident, error) { return r.item, nil }
func (r keyedIncidents) CountRecentByRule(context.Context, string, time.Time) (int, error) {
	return 0, nil
}

// bucketCandidates 候选桶桩：没有待定候选，记下新建 / 更新的候选。
type bucketCandidates struct {
	stubCandidates
	upserts []*Candidate
}

func (c *bucketCandidates) LockOpenTx(context.Context, Tx, string, string) (*Candidate, error) {
	return nil, nil
}

func (c *bucketCandidates) UpsertTx(_ context.Context, _ Tx, cand *Candidate) error {
	c.upserts = append(c.upserts, cand)
	return nil
}

func newCorrelateHarness(resolved *Incident, tail []*Event, now time.Time) (*CorrelatorUsecase, *mapWriter, *stubLinks, *bucketCandidates) {
	writer := &mapWriter{byID: map[string]*Incident{resolved.ID: resolved}}
	links := &stubLinks{}
	candidates := &bucketCandidates{}
	d := &Deps{
		Txm: fakeTxm{}, Writer: writer, Members: fakeMembers{}, Admins: fakeAdmins{}, Events: &recordingEvents{tail: tail}, Links: links,
		Dir: fakeDirectory{}, Candidates: candidates, Incidents: keyedIncidents{item: resolved}, Rules: stubRules{},
		Alerts: &stubAlerts{byID: map[string]*AlertRef{"a1": {ID: "a1", Status: 1}}}, Notify: &fakeNotify{}, Webhooks: fakeWebhookRepo{}, Logger: testLogger(),
	}
	settings := NewSettingsUsecase(fakeSettingRepo{})
	inc := NewIncidentUsecase(d, settings)
	inc.now = func() time.Time { return now }
	uc := NewCorrelatorUsecase(d, inc, settings)
	uc.now = inc.now
	return uc, writer, links, candidates
}

// 手动恢复的故障在归并窗口内同键告警再次触发：不重开，按「没有可归并的活跃故障」走候选 / 开单——这条告警
// 开出一条新故障，没有落空（已恢复的故障不占用告警）。
func TestCorrelateOpensNewIncidentWhenResolvedManually(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	at := now.Add(-5 * time.Minute)
	old := &Incident{ID: "i1", Number: "INC-1", Status: StatusResolved, ResolveTime: &at, RuleID: "r1", CorrelationKey: "*"}
	uc, writer, links, candidates := newCorrelateHarness(old, []*Event{statusEvent(ActorUser, StatusResponding, StatusResolved)}, now)
	rule := &Rule{ID: "r1", Spec: RuleSpec{Name: "慢查询", Incident: RuleIncident{Severity: SeverityS3}, Correlation: RuleCorrelation{WindowM: 30}}}

	if err := uc.correlate(context.Background(), rule, &AlertRef{ID: "a1", Status: 1}, DefaultSettings()); err != nil {
		t.Fatalf("correlate() error = %v", err)
	}
	if old.Status != StatusResolved || old.ReopenCount != 0 {
		t.Fatalf("manually resolved incident = status %d, reopened %d times; want untouched", old.Status, old.ReopenCount)
	}
	if len(candidates.upserts) != 1 || !slices.Equal(candidates.upserts[0].AlertIDs, []string{"a1"}) {
		t.Fatalf("candidate upserts = %+v, want the alert to enter the candidate bucket", candidates.upserts)
	}
	if len(writer.created) != 1 || writer.created[0].RuleID != "r1" || writer.created[0].CorrelationKey != "*" || writer.created[0].ID == old.ID {
		t.Fatalf("incidents created = %+v, want one new incident for the same rule and key", writer.created)
	}
	if !slices.Equal(links.attached, []string{"a1"}) || !slices.Equal(candidates.statuses, []int32{CandidatePromoted}) {
		t.Fatalf("attached %v, candidate statuses %v; want the alert linked to the new incident and the candidate promoted", links.attached, candidates.statuses)
	}
}

// 系统自动恢复的故障照旧：窗口内同键告警再次触发即自动重开，并把告警归并进去，不开新故障。
func TestCorrelateReopensIncidentResolvedBySystem(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	at := now.Add(-5 * time.Minute)
	old := &Incident{ID: "i1", Number: "INC-1", Status: StatusResolved, ResolveTime: &at, RuleID: "r1", CorrelationKey: "*", OccurTime: at}
	uc, writer, links, candidates := newCorrelateHarness(old, []*Event{statusEvent(ActorSystem, StatusResponding, StatusResolved)}, now)
	rule := &Rule{ID: "r1", Spec: RuleSpec{Name: "慢查询", Incident: RuleIncident{Severity: SeverityS3}, Correlation: RuleCorrelation{WindowM: 30}}}

	if err := uc.correlate(context.Background(), rule, &AlertRef{ID: "a1", Status: 1}, DefaultSettings()); err != nil {
		t.Fatalf("correlate() error = %v", err)
	}
	if old.Status != StatusResponding || old.ReopenCount != 1 {
		t.Fatalf("auto-resolved incident = status %d, reopened %d times; want reopened", old.Status, old.ReopenCount)
	}
	if len(writer.created) != 0 || len(candidates.upserts) != 0 || !slices.Equal(links.attached, []string{"a1"}) {
		t.Fatalf("created %d, candidate upserts %d, attached %v; want the alert merged into the reopened incident", len(writer.created), len(candidates.upserts), links.attached)
	}
}
