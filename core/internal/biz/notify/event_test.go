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
	uc := NewEventUsecase(stub, new(eventWriterStub))
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

func (s *eventRepoStub) GetEvent(_ context.Context, id string) (*Event, error) {
	for _, ev := range s.events {
		if ev.ID == id {
			cp := *ev
			return &cp, nil
		}
	}
	return nil, ErrEventNotFound
}

// TestEventUsecase_Replay 事件级重发：新 id、source=internal、不带 dedup_key，其余
// 字段复制自源事件。
func TestEventUsecase_Replay(t *testing.T) {
	t.Parallel()
	writer := &eventWriterStub{}
	uc := NewEventUsecase(&eventRepoStub{}, writer)
	src := &Event{
		ID: "018f3e1a-0000-7000-8000-000000000001", BizID: "monitor", EventType: "monitor.alert.firing", Severity: 3,
		Title: "cpu", Payload: []byte(`{"host":"a"}`), DedupKey: "d1", GroupKey: "g1", Recipients: []string{"ops@example.com"}, Source: EventSourceAPI,
	}
	out, err := uc.Replay(context.Background(), src)
	if err != nil {
		t.Fatal(err)
	}
	if out.ID == src.ID || out.ID == "" || out.Source != EventSourceInternal || out.DedupKey != "" || out.Status != EventStatusReceived || out.PublishState != 0 {
		t.Fatalf("replayed = %+v", out)
	}
	if out.EventType != src.EventType || out.Severity != 3 || out.Title != "cpu" || string(out.Payload) != `{"host":"a"}` || out.GroupKey != "g1" || len(out.Recipients) != 1 {
		t.Fatalf("replayed must copy routing-relevant fields: %+v", out)
	}
	if len(writer.inserted) != 1 || writer.inserted[0].ID != out.ID {
		t.Fatalf("inserted = %+v", writer.inserted)
	}
}
