//go:build integration

package data

import (
	"context"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/notify/internal/biz"
)

func TestIntegration_AggregateLifecycle(t *testing.T) {
	d := newIntegrationData(t)
	seedConfig(t, d, "http://127.0.0.1:9/hook")
	ctx := context.Background()
	cache := newCache(t, d)
	router := biz.NewRouterUsecase(NewEventRepo(d), cache, testLogger()).WithOptions(biz.RouterOptions{MaxAggWindow: time.Hour})
	sweeper := biz.NewSweeperUsecase(NewAggRepo(d), cache, testLogger())
	key := biz.AggKey("default", "room-a")

	// 3 条同组事件（阈值 2）→ count=3，窗口开放。
	for i := 1; i <= 3; i++ {
		seedEvent(t, d, "018f3e1a-0e01-7000-8000-00000000000"+hex2(i)[1:], "host.down", 2, "room-a", nil)
	}
	if n, err := router.RouteOnce(ctx, 100); err != nil || n != 3 {
		t.Fatalf("RouteOnce = %d, %v", n, err)
	}
	// 窗口未到期：sweeper 无事。
	if n, err := sweeper.SweepOnce(ctx, 10); err != nil || n != 0 {
		t.Fatalf("SweepOnce(open) = %d, %v", n, err)
	}
	// 人为把窗口拨到过去 → 关闭：count 3 > 阈值 2 → 摘要模式 + 抑制 30 分钟；1 条汇总 dispatch（webhook）。
	if _, err := d.Pool.Exec(ctx, "UPDATE notify_agg_group SET window_until = now() - interval '1 second' WHERE group_key = $1", key); err != nil {
		t.Fatal(err)
	}
	if n, err := sweeper.SweepOnce(ctx, 10); err != nil || n != 1 {
		t.Fatalf("SweepOnce(close) = %d, %v", n, err)
	}
	var status int
	var suppressedUntil *time.Time
	if err := d.Pool.QueryRow(ctx, "SELECT status, suppressed_until FROM notify_agg_group WHERE group_key = $1", key).Scan(&status, &suppressedUntil); err != nil ||
		status != 2 || suppressedUntil == nil || suppressedUntil.Before(time.Now().Add(29*time.Minute)) {
		t.Fatalf("closed group = %d/%v, %v", status, suppressedUntil, err)
	}
	var content []byte
	if err := d.Pool.QueryRow(ctx, "SELECT content FROM notify_dispatch WHERE event_id = $1", "018f3e1a-0e01-7000-8000-000000000001").Scan(&content); err != nil {
		t.Fatalf("summary dispatch missing: %v", err)
	}
	c, _ := biz.DecodeContent(content)
	if c.Agg == nil || c.Agg.Count != 3 || !c.Agg.Digest || len(c.Agg.Samples) != 0 {
		t.Fatalf("summary content = %+v", c.Agg)
	}
	// 抑制期内同组新事件：仅计数、事件 suppressed、不重开。
	seedEvent(t, d, "018f3e1a-0e01-7000-8000-000000000004", "host.down", 2, "room-a", nil)
	_, _ = router.RouteOnce(ctx, 100)
	if count(t, d, "SELECT status FROM notify_event WHERE id = $1", "018f3e1a-0e01-7000-8000-000000000004") != 3 ||
		count(t, d, "SELECT count FROM notify_agg_group WHERE group_key = $1", key) != 4 ||
		count(t, d, "SELECT status FROM notify_agg_group WHERE group_key = $1", key) != 2 {
		t.Fatal("suppressed window must count without reopening")
	}
	// 抑制期过 → 同键事件重开新窗口（count 重置为 1，first_event 更新）。
	_, _ = d.Pool.Exec(ctx, "UPDATE notify_agg_group SET suppressed_until = now() - interval '1 second' WHERE group_key = $1", key)
	seedEvent(t, d, "018f3e1a-0e01-7000-8000-000000000005", "host.down", 2, "room-a", nil)
	_, _ = router.RouteOnce(ctx, 100)
	var cnt int
	var first string
	if err := d.Pool.QueryRow(ctx, "SELECT count, first_event_id::text, status FROM notify_agg_group WHERE group_key = $1", key).Scan(&cnt, &first, &status); err != nil ||
		cnt != 1 || first != "018f3e1a-0e01-7000-8000-000000000005" || status != 1 {
		t.Fatalf("reopened group = %d/%s/%d, %v", cnt, first, status, err)
	}
	// 窗口封顶：first_event_time 很久以前 → window_until 不超过 first + max_window。
	_, _ = d.Pool.Exec(ctx, "UPDATE notify_agg_group SET first_event_time = now() - interval '2 hours' WHERE group_key = $1", key)
	seedEvent(t, d, "018f3e1a-0e01-7000-8000-000000000006", "host.down", 2, "room-a", nil)
	_, _ = router.RouteOnce(ctx, 100)
	if count(t, d, "SELECT count(*) FROM notify_agg_group WHERE group_key = $1 AND window_until < now()", key) != 1 {
		t.Fatal("window must be capped at first_event_time + max_window")
	}
	// 关闭且过期的组可被清理。
	n, err := sweeper.SweepOnce(ctx, 10)
	if err != nil || n != 1 {
		t.Fatalf("SweepOnce(capped) = %d, %v", n, err)
	}
	_, _ = d.Pool.Exec(ctx, "UPDATE notify_agg_group SET update_time = now() - interval '8 days', suppressed_until = NULL WHERE group_key = $1", key)
	if n, err := NewMaintenanceRepo(d).DeleteClosedAggGroups(ctx, time.Now().Add(-7*24*time.Hour), 100); err != nil || n != 1 {
		t.Fatalf("DeleteClosedAggGroups = %d, %v", n, err)
	}
}

// TestIntegration_AggregateRouteDeleted 窗口关闭时路由已被删除：汇总发不出去，本窗口内
// 归入该组的全部事件（不止样本）改标 failed；窗口之前的同键事件、有自己 dispatch 的
// 同键事件不受影响。
func TestIntegration_AggregateRouteDeleted(t *testing.T) {
	d := newIntegrationData(t)
	seedConfig(t, d, "http://127.0.0.1:9/hook")
	ctx := context.Background()
	router := biz.NewRouterUsecase(NewEventRepo(d), newCache(t, d), testLogger())
	const (
		before = "018f3e1a-2401-7000-8000-000000000001"
		direct = "018f3e1a-2401-7000-8000-000000000002"
	)
	seedEvent(t, d, before, "host.down", 1, "room-z", nil)
	if _, err := d.Pool.Exec(ctx, "UPDATE notify_event SET status = 2, publish_state = 1, create_time = now() - interval '1 minute' WHERE id = $1", before); err != nil {
		t.Fatal(err)
	}
	for i := range 25 { // 超过样本上限 20
		seedEvent(t, d, "018f3e1a-2401-7000-8000-0000000001"+string(rune('a'+i/10))+string(rune('0'+i%10)), "host.down", 1, "room-z", nil)
	}
	seedEvent(t, d, direct, "monitor.alert.firing", 2, "room-z", nil) // 同键、走直发路由（有 dispatch）
	if n, err := router.RouteOnce(ctx, 100); err != nil || n != 26 {
		t.Fatalf("RouteOnce() = %d, %v", n, err)
	}
	// 路由删除后的快照 + 窗口到期。
	if _, err := d.Pool.Exec(ctx, "UPDATE notify_route SET delete_time = now() WHERE id = $1", idRouteAgg); err != nil {
		t.Fatal(err)
	}
	key := biz.AggKey("default", "room-z")
	if _, err := d.Pool.Exec(ctx, "UPDATE notify_agg_group SET window_until = now() - interval '1 second' WHERE group_key = $1", key); err != nil {
		t.Fatal(err)
	}
	sweeper := biz.NewSweeperUsecase(NewAggRepo(d), newCache(t, d), testLogger())
	if n, err := sweeper.SweepOnce(ctx, 10); err != nil || n != 1 {
		t.Fatalf("SweepOnce() = %d, %v", n, err)
	}
	if got := count(t, d, "SELECT count(*) FROM notify_event WHERE group_key = 'room-z' AND status = 4"); got != 25 {
		t.Fatalf("failed events = %d, want all 25 of the window", got)
	}
	if count(t, d, "SELECT count(*) FROM notify_event WHERE id = ANY($1::uuid[]) AND status = 2", []string{before, direct}) != 2 {
		t.Fatal("events outside the window / with their own dispatch must stay routed")
	}
}
