package biz

import (
	"context"
	"testing"
	"time"
)

// dispatchRepoStub 发送任务仓储内存桩：承载用例断言所需的最小语义（四维
// 过滤 + 分页透传）。
type dispatchRepoStub struct {
	dispatches []*Dispatch
}

func (s *dispatchRepoStub) ListDispatches(_ context.Context, filter DispatchFilter, page, pageSize int64) ([]*Dispatch, int64, error) {
	var matched []*Dispatch
	for _, dispatch := range s.dispatches {
		if filter.EventID != "" && dispatch.EventID != filter.EventID {
			continue
		}
		if filter.ChannelID != "" && dispatch.ChannelID != filter.ChannelID {
			continue
		}
		if filter.Status != nil && dispatch.Status != *filter.Status {
			continue
		}
		if filter.CreateTimeStart != nil && dispatch.CreateTime.Before(*filter.CreateTimeStart) {
			continue
		}
		if filter.CreateTimeEnd != nil && dispatch.CreateTime.After(*filter.CreateTimeEnd) {
			continue
		}
		matched = append(matched, dispatch)
	}
	total := int64(len(matched))
	start := (page - 1) * pageSize
	if start >= total {
		return nil, total, nil
	}
	end := min(start+pageSize, total)
	return matched[start:end], total, nil
}

func TestDispatchUsecase_ListPagingNormalize(t *testing.T) {
	t.Parallel()
	stub := &dispatchRepoStub{}
	base := time.Now().UTC().Add(-time.Hour)
	for i := range 5 {
		stub.dispatches = append(stub.dispatches, &Dispatch{
			ID:         string(rune('a' + i)),
			EventID:    "018f3e1a-0304-7000-8000-0000000000e1",
			ChannelID:  "018f3e1a-0304-7000-8000-0000000000c1",
			Recipient:  "ops@example.com",
			Status:     DispatchStatusPending,
			CreateTime: base.Add(time.Duration(i) * time.Minute),
		})
	}
	uc := NewDispatchUsecase(stub)
	ctx := context.Background()

	// 非法分页参数规整为默认值。
	items, total, err := uc.List(ctx, DispatchFilter{}, 0, 0)
	if err != nil {
		t.Fatalf("List(zero paging) error = %v", err)
	}
	if total != 5 || len(items) != 5 {
		t.Fatalf("List(zero paging) = %d items, total %d, want 5/5", len(items), total)
	}

	// 分页透传。
	items, total, err = uc.List(ctx, DispatchFilter{}, 2, 2)
	if err != nil || total != 5 || len(items) != 2 {
		t.Fatalf("List(paged) = (%d items, total %d, %v), want 2/5", len(items), total, err)
	}

	// 四维过滤：事件下钻 + 渠道 + status + 时间区间（含边界）。第 2 分钟行
	// 初始 status=pending，区间内无 failed 行 → 0。
	failed := DispatchStatusFailed
	from := base.Add(2 * time.Minute) // 含第 2 分钟起的行
	to := base.Add(3 * time.Minute)
	items, total, err = uc.List(ctx, DispatchFilter{
		EventID:         "018f3e1a-0304-7000-8000-0000000000e1",
		ChannelID:       "018f3e1a-0304-7000-8000-0000000000c1",
		Status:          &failed,
		CreateTimeStart: &from,
		CreateTimeEnd:   &to,
	}, 1, 20)
	if err != nil {
		t.Fatalf("List(filters) error = %v", err)
	}
	if total != 0 || len(items) != 0 {
		t.Fatalf("List(filters) = %d items, total %d, want 0（区间内行 status 仍 pending）", len(items), total)
	}
	// sender 状态机推进：该行重试失败转 failed 后命中同一过滤。
	stub.dispatches[2].Status = failed
	items, total, err = uc.List(ctx, DispatchFilter{
		EventID:         "018f3e1a-0304-7000-8000-0000000000e1",
		Status:          &failed,
		CreateTimeStart: &from,
		CreateTimeEnd:   &to,
	}, 1, 20)
	if err != nil {
		t.Fatalf("List(filters hit) error = %v", err)
	}
	if total != 1 || len(items) != 1 || items[0].ID != "c" {
		t.Fatalf("List(filters hit) = %+v, total %d, want only row c", items, total)
	}
	// 事件下钻不匹配：另一事件的行被排除。
	items, total, err = uc.List(ctx, DispatchFilter{
		EventID: "018f3e1a-0304-7000-8000-0000000000ff",
	}, 1, 20)
	if err != nil || total != 0 || items != nil {
		t.Fatalf("List(other event) = (%v, %d, %v), want nil/0", items, total, err)
	}
}
