package biz

import (
	"context"
	"testing"
	"time"
)

func newResolveUsecase(inc *Incident, now time.Time) *IncidentUsecase {
	d := &Deps{
		Txm: fakeTxm{}, Writer: &mapWriter{byID: map[string]*Incident{inc.ID: inc}}, Members: fakeMembers{}, Admins: fakeAdmins{},
		Events: &recordingEvents{}, Links: &stubLinks{}, Dir: fakeDirectory{}, Rules: stubRules{}, Proposals: &fakeProposals{},
		Actions: &claimActions{}, Workdays: fakeWorkdays{}, Notify: &fakeNotify{}, Webhooks: fakeWebhookRepo{}, Logger: testLogger(),
	}
	uc := NewIncidentUsecase(d, NewSettingsUsecase(fakeSettingRepo{}))
	uc.now = func() time.Time { return now }
	return uc
}

func resolveNow(t *testing.T, uc *IncidentUsecase, id string) {
	t.Helper()
	if _, err := uc.Transition(context.Background(), "u1", id, TransitionInput{Action: ActionResolve, Note: "已修复"}); err != nil {
		t.Fatalf("Transition(resolve) error = %v", err)
	}
}

// 复盘可以在恢复之前完成。此前恢复时无条件置回「待复盘」并重设到期日，同一份复盘得再完成一次（或强制关闭）。
func TestResolveKeepsReviewCompletedBeforeResolve(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 5, 12, 0, 0, 0, time.UTC)
	inc := &Incident{ID: "i1", Number: "INC-1", Status: StatusResponding, Severity: SeverityS1, ReviewStatus: ReviewDone}
	uc := newResolveUsecase(inc, now)

	resolveNow(t, uc, "i1")
	if inc.Status != StatusResolved {
		t.Fatalf("status = %d, want resolved", inc.Status)
	}
	if inc.ReviewStatus != ReviewDone || inc.ReviewDue != nil {
		t.Fatalf("review after resolve = %d / %v, want still done with no due date", inc.ReviewStatus, inc.ReviewDue)
	}
	// 已完成的复盘不挡关闭。
	if _, err := uc.Transition(context.Background(), "u1", "i1", TransitionInput{Action: ActionClose}); err != nil {
		t.Fatalf("Transition(close) error = %v, want closable without force", err)
	}
}

// 恢复前已在起草：保持起草，只补到期日（提醒进度清零）。
func TestResolveKeepsDraftingReviewAndSetsDueDate(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 5, 12, 0, 0, 0, time.UTC)
	inc := &Incident{ID: "i1", Number: "INC-1", Status: StatusResponding, Severity: SeverityS1, ReviewStatus: ReviewDrafting, ReviewRemind: reviewRemindDue}
	uc := newResolveUsecase(inc, now)

	resolveNow(t, uc, "i1")
	want := now.Add(3 * 24 * time.Hour) // S1 缺省 3 个工作日
	if inc.ReviewStatus != ReviewDrafting || inc.ReviewDue == nil || !inc.ReviewDue.Equal(want) || inc.ReviewRemind != 0 {
		t.Fatalf("review after resolve = %d / %v / %d, want drafting due %v with reminders reset", inc.ReviewStatus, inc.ReviewDue, inc.ReviewRemind, want)
	}
}

// 没写过复盘的照旧：要求复盘的等级置为待复盘并设到期日。
func TestResolveSetsPendingReviewWhenNoneStarted(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 5, 12, 0, 0, 0, time.UTC)
	inc := &Incident{ID: "i1", Number: "INC-1", Status: StatusResponding, Severity: SeverityS2}
	uc := newResolveUsecase(inc, now)

	resolveNow(t, uc, "i1")
	want := now.Add(7 * 24 * time.Hour) // S2 缺省 7 个工作日
	if inc.ReviewStatus != ReviewPending || inc.ReviewDue == nil || !inc.ReviewDue.Equal(want) {
		t.Fatalf("review after resolve = %d / %v, want pending due %v", inc.ReviewStatus, inc.ReviewDue, want)
	}
}

// 不要求复盘的等级：没完成的草稿照旧清掉要求（留着会挡住关闭）；已完成的仍保留。
func TestResolveWithoutReviewRequirement(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 5, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name         string
		before, want int32
	}{
		{"drafting is cleared", ReviewDrafting, ReviewNone},
		{"done is kept", ReviewDone, ReviewDone},
	} {
		inc := &Incident{ID: "i1", Number: "INC-1", Status: StatusResponding, Severity: SeverityS3, ReviewStatus: tc.before}
		resolveNow(t, newResolveUsecase(inc, now), "i1")
		if inc.ReviewStatus != tc.want || inc.ReviewDue != nil {
			t.Fatalf("%s: review after resolve = %d / %v, want %d with no due date", tc.name, inc.ReviewStatus, inc.ReviewDue, tc.want)
		}
	}
}
