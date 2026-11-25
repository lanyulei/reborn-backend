package biz

import (
	"context"
	"testing"
	"time"
)

// 合并后被并入的故障不再有复盘：不清掉的话巡检会按旧的到期日照样提醒，而复盘接口对已合并的故障返回状态错误。
// 目标故障的复盘状态不受影响。
func TestMergeClearsReviewStateOfMergedIncident(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	due := now.Add(24 * time.Hour)
	src := &Incident{ID: "i1", Number: "INC-1", Status: StatusResolved, ReviewStatus: ReviewPending, ReviewDue: &due, ReviewRemind: reviewRemindDue}
	dst := &Incident{ID: "i2", Number: "INC-2", Status: StatusOpen, ReviewStatus: ReviewDrafting}
	writer := &mapWriter{byID: map[string]*Incident{"i1": src, "i2": dst}}
	d := &Deps{
		Txm: fakeTxm{}, Writer: writer, Members: fakeMembers{}, Admins: fakeAdmins{}, Events: &recordingEvents{}, Links: &stubLinks{},
		Incidents: &inspectIncidents{inc: src, listed: []*Incident{dst}},
		Dir:       fakeDirectory{}, Rules: stubRules{}, Proposals: &fakeProposals{}, Notify: &fakeNotify{}, Webhooks: fakeWebhookRepo{}, Logger: testLogger(),
	}
	uc := NewIncidentUsecase(d, NewSettingsUsecase(fakeSettingRepo{}))
	uc.now = func() time.Time { return now }

	v, err := uc.Merge(context.Background(), "u1", "i1", "i2", "重复故障")
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	if src.Status != StatusMerged || src.MergedInto != "i2" || v.MergedInto != "i2" {
		t.Fatalf("source after merge = status %d, merged into %q (view %q); want merged into i2", src.Status, src.MergedInto, v.MergedInto)
	}
	if src.ReviewStatus != ReviewNone || src.ReviewDue != nil || src.ReviewRemind != 0 {
		t.Fatalf("source review state after merge = %d / %v / %d, want none / nil / 0", src.ReviewStatus, src.ReviewDue, src.ReviewRemind)
	}
	if dst.Status != StatusOpen || dst.ReviewStatus != ReviewDrafting {
		t.Fatalf("target after merge = status %d, review %d; want untouched", dst.Status, dst.ReviewStatus)
	}
}
