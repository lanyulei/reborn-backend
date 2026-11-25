package biz

import (
	"context"
	"testing"
	"time"
)

// 改进项的逾期标记与「只看超期」的过滤同一口径：到期日当天不算，过了到期日才算。
// 此前拿 DueDate（date 列，0 点 UTC）与当前时刻比，到期日当天 08:00（UTC+8）起就标记逾期，而过滤查不到它。
func TestActionItemOverdueMatchesListFilter(t *testing.T) {
	t.Parallel()
	// 到期日当天的 09:00（UTC+8）：此时 DueDate（当天 0 点 UTC）已经早于当前时刻。
	now := time.Date(2026, 1, 15, 9, 0, 0, 0, time.FixedZone("CST", 8*3600))
	due := func(day int) *time.Time {
		t := time.Date(2026, 1, day, 0, 0, 0, 0, time.UTC) // date 列经驱动读出是 0 点 UTC
		return &t
	}
	items := []*ActionItem{
		{ID: "past", Status: ItemOpen, DueDate: due(14)},  // 昨天到期
		{ID: "today", Status: ItemOpen, DueDate: due(15)}, // 今天到期
		{ID: "future", Status: ItemOpen, DueDate: due(16)},
		{ID: "none", Status: ItemOpen},
		{ID: "done", Status: ItemDone, DueDate: due(14)},
	}
	uc := NewPostmortemUsecase(&Deps{Dir: fakeDirectory{}, Logger: testLogger()}, nil, nil)
	uc.now = func() time.Time { return now }

	views, err := uc.itemViews(context.Background(), items)
	if err != nil {
		t.Fatalf("itemViews() error = %v", err)
	}
	want := map[string]bool{"past": true, "today": false, "future": false, "none": false, "done": false}
	if len(views) != len(items) {
		t.Fatalf("views = %d, want %d", len(views), len(items))
	}
	for _, v := range views {
		if v.Overdue != want[v.ID] {
			t.Fatalf("%s (due %v): overdue = %v, want %v", v.ID, v.DueDate, v.Overdue, want[v.ID])
		}
	}
}
