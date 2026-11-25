package biz

import (
	"context"
	"slices"
	"testing"
	"time"
)

// 复盘到期提醒只取「已恢复」的故障：重开（处理中）或已合并的故障带着旧的到期日，提醒了也无法处理，
// 列表的「复盘已超期」同样不算它们。到期前 1 天与到期当天两个阶段的其他筛选条件不变。
func TestInspectorReviewDueOnlyRemindsResolved(t *testing.T) {
	t.Parallel()
	incidents := &inspectIncidents{}
	uc := &InspectorUsecase{d: &Deps{Incidents: incidents, Logger: testLogger()}, now: time.Now}

	if _, err := uc.reviewDue(context.Background(), DefaultSettings()); err != nil {
		t.Fatalf("reviewDue() error = %v", err)
	}
	if len(incidents.queries) != 2 {
		t.Fatalf("inspect queries = %d, want 2 (due soon and overdue)", len(incidents.queries))
	}
	reminds := map[int32]bool{}
	for i, q := range incidents.queries {
		if !slices.Equal(q.Statuses, []int32{StatusResolved}) {
			t.Fatalf("query %d statuses = %v, want only resolved", i, q.Statuses)
		}
		if !slices.Equal(q.ReviewStatuses, []int32{ReviewPending, ReviewDrafting}) {
			t.Fatalf("query %d review statuses = %v, want pending and drafting", i, q.ReviewStatuses)
		}
		if q.ReviewDueBefore == nil || !q.ExcludeDrill || q.ReviewRemindLT == 0 {
			t.Fatalf("query %d = %+v, want a due-before bound, drills excluded and a remind stage", i, q)
		}
		reminds[q.ReviewRemindLT] = true
	}
	if !reminds[reviewRemindDue] || !reminds[reviewRemindOverdue] {
		t.Fatalf("remind stages = %v, want both %d and %d", reminds, reviewRemindDue, reviewRemindOverdue)
	}
}
