package biz_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// 时间线（时间线条目 + 通知记录）合并后超过上限时保留最新的一段（monitor 模块评审 core-monitor-alert P2）：
// 此前升序后切头部，条目多的 longest-lived 事件永远看不到最近的通知与操作。
func TestAlertDetail_TimelineKeepsTheLatestItems(t *testing.T) {
	p := newP2Env(t)
	ctx := context.Background()
	host := p.ids["host"]
	base := time.Now().Add(-2 * time.Hour)
	// 400 条时间线条目与 300 条通知记录在时间上交错；详情取 400 条条目 + 200 条通知记录，合并后 600 条，超过上限 500。
	for i := range 400 {
		p.ops.SeedEvent(&biz.AlertEvent{
			ID: fmt.Sprintf("ev-%03d", i), AlertID: host, Type: mc.AlertEventComment, Operator: uAlice, CreateTime: base.Add(time.Duration(i) * time.Second),
		})
		if i < 300 {
			p.ops.SeedNotifyLog(&biz.NotifyLog{
				ID: fmt.Sprintf("log-%03d", i), AlertID: host, Kind: mc.NotifyKindRepeat, Seq: int32(i + 2), Source: mc.NotifySourceRule,
				Recipients: []string{uAlice}, CreateTime: base.Add(time.Duration(i)*time.Second + 500*time.Millisecond),
			})
		}
	}
	_, x, err := p.alerts.GetDetail(ctx, uAlice, host)
	if err != nil {
		t.Fatalf("GetDetail: %v", err)
	}
	var ids []string
	for _, it := range x.Timeline {
		ids = append(ids, it.ID)
	}
	if len(x.Timeline) != 500 {
		t.Fatalf("timeline has %d items, want the cap of 500", len(ids))
	}
	if ids[0] != "ev-050" || ids[1] != "log-050" || ids[len(ids)-1] != "ev-399" {
		t.Fatalf("timeline = %s … %s, want the newest 500 items starting at ev-050 and ending at ev-399", ids[0], ids[len(ids)-1])
	}
	// 升序不变；丢掉的是最旧的 100 条（ev-000 … log-049）。
	if !slices.IsSortedFunc(x.Timeline, func(a, b *biz.TimelineItem) int { return a.Time.Compare(b.Time) }) {
		t.Fatal("the timeline must stay in ascending order")
	}
	if slices.Contains(ids, "ev-049") || slices.Contains(ids, "log-049") || !slices.Contains(ids, "log-199") {
		t.Fatalf("the oldest items must be the ones dropped: first = %s", ids[0])
	}
}
