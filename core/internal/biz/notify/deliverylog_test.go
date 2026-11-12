package biz

import (
	"context"
	"testing"
	"time"
)

// deliveryLogRepoStub 投递明细仓储内存桩：承载用例断言所需的最小语义
// （四维过滤 + 分页透传）。
type deliveryLogRepoStub struct {
	logs []*DeliveryLog
}

func (s *deliveryLogRepoStub) ListDeliveryLogs(_ context.Context, filter DeliveryLogFilter, page, pageSize int64) ([]*DeliveryLog, int64, error) {
	var matched []*DeliveryLog
	for _, log := range s.logs {
		if filter.DispatchID != "" && log.DispatchID != filter.DispatchID {
			continue
		}
		if filter.ChannelID != "" && log.ChannelID != filter.ChannelID {
			continue
		}
		if filter.OK != nil && log.OK != *filter.OK {
			continue
		}
		if filter.CreateTimeStart != nil && log.CreateTime.Before(*filter.CreateTimeStart) {
			continue
		}
		if filter.CreateTimeEnd != nil && log.CreateTime.After(*filter.CreateTimeEnd) {
			continue
		}
		matched = append(matched, log)
	}
	total := int64(len(matched))
	start := (page - 1) * pageSize
	if start >= total {
		return nil, total, nil
	}
	end := min(start+pageSize, total)
	return matched[start:end], total, nil
}

func TestDeliveryLogUsecase_ListPagingNormalize(t *testing.T) {
	t.Parallel()
	stub := &deliveryLogRepoStub{}
	base := time.Now().UTC().Add(-time.Hour)
	for i := range 5 {
		stub.logs = append(stub.logs, &DeliveryLog{
			ID:         string(rune('a' + i)),
			DispatchID: "018f3e1a-0304-7000-8000-0000000000d1",
			ChannelID:  "018f3e1a-0304-7000-8000-0000000000c1",
			Attempt:    int32(i + 1),
			OK:         true,
			CreateTime: base.Add(time.Duration(i) * time.Minute),
		})
	}
	uc := NewDeliveryLogUsecase(stub)
	ctx := context.Background()

	// 非法分页参数规整为默认值。
	items, total, err := uc.List(ctx, DeliveryLogFilter{}, 0, 0)
	if err != nil {
		t.Fatalf("List(zero paging) error = %v", err)
	}
	if total != 5 || len(items) != 5 {
		t.Fatalf("List(zero paging) = %d items, total %d, want 5/5", len(items), total)
	}

	// 分页透传。
	items, total, err = uc.List(ctx, DeliveryLogFilter{}, 2, 2)
	if err != nil || total != 5 || len(items) != 2 {
		t.Fatalf("List(paged) = (%d items, total %d, %v), want 2/5", len(items), total, err)
	}

	// 四维过滤：任务下钻 + 渠道 + ok=false + 时间区间（含边界）。种子全为
	// ok=true，区间内无失败行 → 0。
	failed := false
	from := base.Add(2 * time.Minute) // 含第 2 分钟起的行
	to := base.Add(3 * time.Minute)
	items, total, err = uc.List(ctx, DeliveryLogFilter{
		DispatchID:      "018f3e1a-0304-7000-8000-0000000000d1",
		ChannelID:       "018f3e1a-0304-7000-8000-0000000000c1",
		OK:              &failed,
		CreateTimeStart: &from,
		CreateTimeEnd:   &to,
	}, 1, 20)
	if err != nil {
		t.Fatalf("List(filters) error = %v", err)
	}
	if total != 0 || len(items) != 0 {
		t.Fatalf("List(filters) = %d items, total %d, want 0（区间内行均 ok=true）", len(items), total)
	}
	// sender 重试落败：该行 ok 翻 false 后命中同一过滤。
	stub.logs[2].OK = false
	stub.logs[2].Error = "smtp: 421 service unavailable"
	items, total, err = uc.List(ctx, DeliveryLogFilter{
		DispatchID:      "018f3e1a-0304-7000-8000-0000000000d1",
		OK:              &failed,
		CreateTimeStart: &from,
		CreateTimeEnd:   &to,
	}, 1, 20)
	if err != nil {
		t.Fatalf("List(filters hit) error = %v", err)
	}
	if total != 1 || len(items) != 1 || items[0].ID != "c" {
		t.Fatalf("List(filters hit) = %+v, total %d, want only row c", items, total)
	}
	// 任务下钻不匹配：另一任务的行被排除。
	items, total, err = uc.List(ctx, DeliveryLogFilter{
		DispatchID: "018f3e1a-0304-7000-8000-0000000000ff",
	}, 1, 20)
	if err != nil || total != 0 || items != nil {
		t.Fatalf("List(other dispatch) = (%v, %d, %v), want nil/0", items, total, err)
	}
}

func (s *deliveryLogRepoStub) LatestByDispatchIDs(_ context.Context, ids []string) ([]*DeliveryLog, error) {
	latest := map[string]*DeliveryLog{}
	for _, l := range s.logs {
		for _, id := range ids {
			if l.DispatchID == id && (latest[id] == nil || l.CreateTime.After(latest[id].CreateTime)) {
				latest[id] = l
			}
		}
	}
	var out []*DeliveryLog
	for _, id := range ids {
		if l, ok := latest[id]; ok {
			out = append(out, l)
		}
	}
	return out, nil
}
