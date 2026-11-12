//go:build integration

package data

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/lanyulei/reborn/backend/notify/internal/biz"
)

func TestIntegration_RouterProcessPending(t *testing.T) {
	d := newIntegrationData(t)
	seedConfig(t, d, "http://127.0.0.1:9/hook")
	ctx := context.Background()
	cache := newCache(t, d)
	router := biz.NewRouterUsecase(NewEventRepo(d), cache, testLogger())

	seedEvent(t, d, "018f3e1a-0c01-7000-8000-000000000001", "monitor.alert.firing", 3, "", nil)
	seedEvent(t, d, "018f3e1a-0c01-7000-8000-000000000002", "monitor.silent", 2, "", nil)
	seedEvent(t, d, "018f3e1a-0c01-7000-8000-000000000003", "resource.change", 2, "", nil)
	seedEvent(t, d, "018f3e1a-0c01-7000-8000-000000000004", "host.down", 2, "room-a", nil)
	seedEvent(t, d, "018f3e1a-0c01-7000-8000-000000000005", "host.down", 2, "room-a", nil)
	seedEvent(t, d, "018f3e1a-0c01-7000-8000-000000000006", "monitor.alert.firing", 3, "", []byte(`["vip@example.com"]`))
	// 显式收件为平台用户 ID（+ 无机器人渠道可用的群标识 + 无法解析项）：解析为 alice 的邮箱。
	seedEvent(t, d, "018f3e1a-0c01-7000-8000-000000000007", "monitor.alert.firing", 3, "", []byte(`["`+idUser+`","$group:ops","nobody"]`))

	n, err := router.RouteOnce(ctx, 500)
	if err != nil || n != 7 {
		t.Fatalf("RouteOnce() = %d, %v", n, err)
	}
	// 全部事件已交付（publish_state=1），状态按语义分布。
	if count(t, d, "SELECT count(*) FROM notify_event WHERE publish_state = 0") != 0 {
		t.Fatal("events still pending")
	}
	status := func(id string) int {
		return count(t, d, "SELECT status FROM notify_event WHERE id = $1", id)
	}
	if status("018f3e1a-0c01-7000-8000-000000000001") != 2 || status("018f3e1a-0c01-7000-8000-000000000002") != 3 ||
		status("018f3e1a-0c01-7000-8000-000000000003") != 4 || status("018f3e1a-0c01-7000-8000-000000000004") != 2 {
		t.Fatal("event statuses not as expected")
	}
	// 直发事件 1：webhook(url) + email(fixed ops + 订阅 alice) = 3 个 dispatch；停用渠道跳过。
	if n := count(t, d, "SELECT count(*) FROM notify_dispatch WHERE event_id = $1", "018f3e1a-0c01-7000-8000-000000000001"); n != 3 {
		t.Fatalf("dispatches for direct event = %d, want 3", n)
	}
	if count(t, d, "SELECT count(*) FROM notify_dispatch WHERE event_id = $1 AND channel_id = $2 AND recipient = 'alice@example.com'",
		"018f3e1a-0c01-7000-8000-000000000001", idChanEmail) != 1 {
		t.Fatal("subscriber email dispatch missing")
	}
	// 显式收件事件 6：email 仅 vip（替代策略），webhook 1 → 2 个。
	if n := count(t, d, "SELECT count(*) FROM notify_dispatch WHERE event_id = $1", "018f3e1a-0c01-7000-8000-000000000006"); n != 2 {
		t.Fatalf("dispatches for explicit-recipient event = %d, want 2", n)
	}
	// 显式收件事件 7：用户 ID → alice@example.com（email 渠道），webhook 1 → 2 个。
	if n := count(t, d, "SELECT count(*) FROM notify_dispatch WHERE event_id = $1 AND recipient = 'alice@example.com' AND channel_id = $2",
		"018f3e1a-0c01-7000-8000-000000000007", idChanEmail); n != 1 {
		t.Fatalf("user-id recipient should resolve to email dispatch, got %d", n)
	}
	if n := count(t, d, "SELECT count(*) FROM notify_dispatch WHERE event_id = $1", "018f3e1a-0c01-7000-8000-000000000007"); n != 2 {
		t.Fatalf("dispatches for user-id recipient event = %d, want 2", n)
	}
	// 聚合：无 dispatch，组 count=2、samples=2、open。
	var cnt, samples, st int
	if err := d.Pool.QueryRow(ctx, "SELECT count, jsonb_array_length(samples), status FROM notify_agg_group WHERE group_key = $1",
		biz.AggKey("default", "room-a")).Scan(&cnt, &samples, &st); err != nil || cnt != 2 || samples != 2 || st != 1 {
		t.Fatalf("agg group = %d/%d/%d, %v", cnt, samples, st, err)
	}
	if count(t, d, "SELECT count(*) FROM notify_dispatch WHERE event_id IN ($1,$2)", "018f3e1a-0c01-7000-8000-000000000004", "018f3e1a-0c01-7000-8000-000000000005") != 0 {
		t.Fatal("aggregated events must not produce direct dispatches")
	}
	// 幂等：再跑一轮无事可做。
	if n, err := router.RouteOnce(ctx, 500); err != nil || n != 0 {
		t.Fatalf("second RouteOnce() = %d, %v", n, err)
	}
	// dispatch 恒有到期时间（CHECK 约束）且立即可领。
	if count(t, d, "SELECT count(*) FROM notify_dispatch WHERE next_retry_time IS NULL") != 0 {
		t.Fatal("dispatch without next_retry_time")
	}
}

// TestIntegration_RouterConcurrencyAndRollback 两个 router 实例并发声明互不重复；
// 回调失败整批回滚（事件仍 pending、无 dispatch）。
func TestIntegration_RouterConcurrencyAndRollback(t *testing.T) {
	d := newIntegrationData(t)
	seedConfig(t, d, "http://127.0.0.1:9/hook")
	ctx := context.Background()
	for i := 0; i < 40; i++ {
		seedEvent(t, d, "018f3e1a-0c02-7000-8000-0000000000"+hex2(i), "monitor.alert.firing", 2, "", nil)
	}
	repo := NewEventRepo(d)
	// 回滚路径：回调报错 → 无状态变化。
	boom := errors.New("boom")
	if _, err := repo.ProcessPending(ctx, 10, func(ctx context.Context, tx biz.RouterTx, events []*biz.Event) error {
		marks := make([]biz.EventMark, 0, len(events))
		for _, ev := range events {
			marks = append(marks, biz.EventMark{ID: ev.ID, CreateTime: ev.CreateTime, Status: biz.EventStatusRouted})
		}
		if err := tx.MarkEvents(ctx, marks); err != nil {
			return err
		}
		return boom
	}); !errors.Is(err, boom) {
		t.Fatalf("ProcessPending(rollback) error = %v", err)
	}
	if count(t, d, "SELECT count(*) FROM notify_event WHERE publish_state = 1") != 0 {
		t.Fatal("rollback must leave events pending")
	}
	// 并发：两个实例各自声明批，事件总处理数恰为 40，无重复。
	cache := newCache(t, d)
	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := map[string]int{}
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := biz.NewRouterUsecase(&countingSource{inner: repo, seen: seen, mu: &mu}, cache, testLogger())
			for {
				n, err := r.RouteOnce(ctx, 7)
				if err != nil {
					t.Error(err)
					return
				}
				if n == 0 {
					return
				}
			}
		}()
	}
	wg.Wait()
	if len(seen) != 40 {
		t.Fatalf("processed %d distinct events, want 40", len(seen))
	}
	for id, c := range seen {
		if c != 1 {
			t.Fatalf("event %s processed %d times", id, c)
		}
	}
	if count(t, d, "SELECT count(*) FROM notify_dispatch") != 40*3 {
		t.Fatalf("dispatches = %d, want 120", count(t, d, "SELECT count(*) FROM notify_dispatch"))
	}
}

// countingSource 包装 EventSource 记录每个事件被交付的次数。
type countingSource struct {
	inner biz.EventSource
	seen  map[string]int
	mu    *sync.Mutex
}

func (s *countingSource) ProcessPending(ctx context.Context, limit int, fn func(context.Context, biz.RouterTx, []*biz.Event) error) (int, error) {
	return s.inner.ProcessPending(ctx, limit, func(ctx context.Context, tx biz.RouterTx, events []*biz.Event) error {
		s.mu.Lock()
		for _, ev := range events {
			s.seen[ev.ID]++
		}
		s.mu.Unlock()
		return fn(ctx, tx, events)
	})
}

func hex2(i int) string {
	const digits = "0123456789abcdef"
	return string([]byte{digits[i/16], digits[i%16]})
}
