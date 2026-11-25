package biz

import (
	"context"
	"strings"
	"testing"
	"time"
)

func newReopenUsecase(inc *Incident, now time.Time, listed ...*Incident) (*IncidentUsecase, *mapWriter, *recordingEvents, *inspectIncidents) {
	writer := &mapWriter{byID: map[string]*Incident{inc.ID: inc}}
	events := &recordingEvents{}
	incidents := &inspectIncidents{inc: inc, listed: listed}
	d := &Deps{
		Txm: fakeTxm{}, Writer: writer, Members: fakeMembers{}, Admins: fakeAdmins{}, Events: events, Incidents: incidents,
		Links: &stubLinks{}, Dir: fakeDirectory{}, Rules: stubRules{}, Notify: &fakeNotify{}, Webhooks: fakeWebhookRepo{}, Logger: testLogger(),
	}
	uc := NewIncidentUsecase(d, NewSettingsUsecase(fakeSettingRepo{}))
	uc.now = func() time.Time { return now }
	return uc, writer, events, incidents
}

// 手动重开撞上同规则、同归并键的活跃故障：提示改用合并。此前落库时撞唯一索引，被报成「已被他人修改，请刷新重试」，
// 刷新重试仍然失败。
func TestReopenConflictingWithActiveIncidentSuggestsMerge(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	resolved := now.Add(-time.Hour)
	inc := &Incident{ID: "i1", Number: "INC-1", Status: StatusResolved, ResolveTime: &resolved, RuleID: "r1", CorrelationKey: "k", Version: 3}
	other := &Incident{ID: "i2", Number: "INC-2", Status: StatusResponding}
	uc, writer, events, incidents := newReopenUsecase(inc, now, other)

	_, err := uc.Transition(context.Background(), "u1", "i1", TransitionInput{Action: ActionReopen, Note: "又复发了"})
	if !ErrStateInvalid.Is(err) || ErrVersionConflict.Is(err) {
		t.Fatalf("Transition() error = %v, want ErrStateInvalid (not a version conflict)", err)
	}
	if !strings.Contains(err.Error(), "INC-2") || !strings.Contains(err.Error(), "merge") {
		t.Fatalf("error %q should name the active incident and suggest merging", err)
	}
	if inc.Status != StatusResolved || inc.ReopenCount != 0 {
		t.Fatalf("incident = status %d, reopened %d times; want untouched", inc.Status, inc.ReopenCount)
	}
	if len(writer.saved) != 0 || len(events.appended) != 0 || incidents.listCall != 1 {
		t.Fatalf("a rejected reopen must write nothing: incident saves %d, events %+v, lookups %d",
			len(writer.saved), events.appended, incidents.listCall)
	}
}

// 没有同键的活跃故障：照常重开，并且复盘要求作废（再次恢复时按等级重算）。旧的到期日留着的话，处理中的故障
// 也会收到「复盘即将到期 / 已超期」，列表的「复盘已超期」也算它。
func TestReopenClearsReviewState(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	resolved, due := now.Add(-time.Hour), now.Add(24*time.Hour)
	inc := &Incident{
		ID: "i1", Number: "INC-1", Status: StatusResolved, ResolveTime: &resolved, RuleID: "r1", CorrelationKey: "k",
		ReviewStatus: ReviewPending, ReviewDue: &due, ReviewRemind: reviewRemindOverdue,
	}
	uc, writer, _, _ := newReopenUsecase(inc, now)

	v, err := uc.Transition(context.Background(), "u1", "i1", TransitionInput{Action: ActionReopen, Note: "又复发了"})
	if err != nil {
		t.Fatalf("Transition() error = %v", err)
	}
	if inc.Status != StatusResponding || inc.ReopenCount != 1 || inc.ResolveTime != nil || v.Status != StatusResponding {
		t.Fatalf("incident after reopen = status %d, reopened %d times, resolved %v; want responding", inc.Status, inc.ReopenCount, inc.ResolveTime)
	}
	if inc.ReviewStatus != ReviewNone || inc.ReviewDue != nil || inc.ReviewRemind != 0 {
		t.Fatalf("review state after reopen = %d / %v / %d, want none / nil / 0", inc.ReviewStatus, inc.ReviewDue, inc.ReviewRemind)
	}
	if len(writer.saved) != 1 {
		t.Fatalf("incident saves = %d, want 1", len(writer.saved))
	}
}

// 手动开出的故障没有归并键：不做这项检查（查也查不准），照常重开。
func TestReopenManualIncidentSkipsCorrelationCheck(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	resolved := now.Add(-time.Hour)
	inc := &Incident{ID: "i1", Number: "INC-1", Status: StatusResolved, ResolveTime: &resolved}
	uc, _, _, incidents := newReopenUsecase(inc, now)

	if _, err := uc.Transition(context.Background(), "u1", "i1", TransitionInput{Action: ActionReopen}); err != nil {
		t.Fatalf("Transition() error = %v", err)
	}
	if inc.Status != StatusResponding || incidents.listCall != 0 {
		t.Fatalf("status = %d, lookups = %d; want reopened without a correlation lookup", inc.Status, incidents.listCall)
	}
}

// 自动重开（恢复后同键告警再次触发）同样清掉复盘状态。
func TestAutoReopenClearsReviewState(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	resolved, due := now.Add(-10*time.Minute), now.Add(24*time.Hour)
	inc := &Incident{
		ID: "i1", Number: "INC-1", Status: StatusResolved, ResolveTime: &resolved, RuleID: "r1", CorrelationKey: "k",
		ReviewStatus: ReviewPending, ReviewDue: &due, ReviewRemind: reviewRemindDue,
	}
	uc, _, events, _ := newReopenUsecase(inc, now)
	// 这条故障是系统自动恢复的（只有这种才自动重开）。
	events.tail = []*Event{{Type: EventStatus, ActorType: ActorSystem, Detail: EventDetail{From: StatusResponding, To: StatusResolved}}}

	ok, err := uc.autoReopenIfNeeded(context.Background(), inc, DefaultSettings())
	if err != nil || !ok {
		t.Fatalf("autoReopenIfNeeded() = %v, %v; want reopened", ok, err)
	}
	if inc.Status != StatusResponding || inc.ReviewStatus != ReviewNone || inc.ReviewDue != nil || inc.ReviewRemind != 0 {
		t.Fatalf("incident after auto reopen = status %d, review %d / %v / %d; want responding with the review state cleared",
			inc.Status, inc.ReviewStatus, inc.ReviewDue, inc.ReviewRemind)
	}
}
