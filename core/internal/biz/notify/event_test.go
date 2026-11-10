package biz

import (
	"context"
	"testing"
	"time"
)

// eventRepoStub 事件仓储内存桩：承载用例断言所需的最小语义（四维过滤 +
// 分页透传）。
type eventRepoStub struct {
	events []*Event
}

func (s *eventRepoStub) ListEvents(_ context.Context, filter EventFilter, page, pageSize int64) ([]*Event, int64, error) {
	var matched []*Event
	for _, event := range s.events {
		if filter.BizID != "" && event.BizID != filter.BizID {
			continue
		}
		if filter.EventType != "" && event.EventType != filter.EventType {
			continue
		}
		if filter.Status != nil && event.Status != *filter.Status {
			continue
		}
		if filter.CreateTimeStart != nil && event.CreateTime.Before(*filter.CreateTimeStart) {
			continue
		}
		if filter.CreateTimeEnd != nil && event.CreateTime.After(*filter.CreateTimeEnd) {
			continue
		}
		matched = append(matched, event)
	}
	total := int64(len(matched))
	start := (page - 1) * pageSize
	if start >= total {
		return nil, total, nil
	}
	end := min(start+pageSize, total)
	return matched[start:end], total, nil
}

func TestEventUsecase_ListPagingNormalize(t *testing.T) {
	t.Parallel()
	stub := &eventRepoStub{}
	base := time.Now().UTC().Add(-time.Hour)
	for i := range 5 {
		stub.events = append(stub.events, &Event{
			ID:         string(rune('a' + i)),
			BizID:      "monitor",
			EventType:  "monitor.alert.firing",
			Status:     EventStatusReceived,
			CreateTime: base.Add(time.Duration(i) * time.Minute),
		})
	}
	uc := NewEventUsecase(stub)
	ctx := context.Background()

	// 非法分页参数规整为默认值。
	items, total, err := uc.List(ctx, EventFilter{}, 0, 0)
	if err != nil {
		t.Fatalf("List(zero paging) error = %v", err)
	}
	if total != 5 || len(items) != 5 {
		t.Fatalf("List(zero paging) = %d items, total %d, want 5/5", len(items), total)
	}

	// 分页透传。
	items, total, err = uc.List(ctx, EventFilter{}, 2, 2)
	if err != nil || total != 5 || len(items) != 2 {
		t.Fatalf("List(paged) = (%d items, total %d, %v), want 2/5", len(items), total, err)
	}

	// 四维过滤：status + 时间区间（含边界）。第 2 分钟行初始 status=received，
	// 区间内无 suppressed 行 → 0。
	suppressed := EventStatusSuppressed
	from := base.Add(2 * time.Minute) // 含第 2 分钟起的行
	to := base.Add(3 * time.Minute)
	items, total, err = uc.List(ctx, EventFilter{
		BizID:           "monitor",
		Status:          &suppressed,
		CreateTimeStart: &from,
		CreateTimeEnd:   &to,
	}, 1, 20)
	if err != nil {
		t.Fatalf("List(filters) error = %v", err)
	}
	if total != 0 || len(items) != 0 {
		t.Fatalf("List(filters) = %d items, total %d, want 0（区间内行 status 仍 received）", len(items), total)
	}
	// 管道状态机推进：该行转 suppressed 后命中同一过滤。
	stub.events[2].Status = suppressed
	items, total, err = uc.List(ctx, EventFilter{
		BizID:           "monitor",
		EventType:       "monitor.alert.firing",
		Status:          &suppressed,
		CreateTimeStart: &from,
		CreateTimeEnd:   &to,
	}, 1, 20)
	if err != nil {
		t.Fatalf("List(filters hit) error = %v", err)
	}
	if total != 1 || len(items) != 1 || items[0].ID != "c" {
		t.Fatalf("List(filters hit) = %+v, total %d, want only row c", items, total)
	}
}
