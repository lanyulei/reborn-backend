//go:build integration

package data

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/notify/internal/biz"
)

func seedDispatch(t *testing.T, d *Data, id, eventID, channelID string, status int32, due time.Time) {
	t.Helper()
	if _, err := d.Pool.Exec(context.Background(),
		`INSERT INTO notify_dispatch (id, event_id, channel_id, recipient, status, attempts, next_retry_time, send_key, create_time)
		 VALUES ($1, $2, $3, 'ops@example.com', $4, 0, $5, 'k', now())`, id, eventID, channelID, status, due); err != nil {
		t.Fatalf("seed dispatch: %v", err)
	}
}

func TestIntegration_DispatchLeaseAndFence(t *testing.T) {
	d := newIntegrationData(t)
	seedConfig(t, d, "http://127.0.0.1:9/hook")
	ctx := context.Background()
	repo := NewDispatchRepo(d)
	const ev = "018f3e1a-0d01-7000-8000-000000000001"
	seedEvent(t, d, ev, "monitor.alert.firing", 2, "", nil)
	now := time.Now()
	seedDispatch(t, d, "018f3e1a-0d02-7000-8000-000000000001", ev, idChanHook, 1, now)
	seedDispatch(t, d, "018f3e1a-0d02-7000-8000-000000000002", ev, idChanEmail, 4, now.Add(-time.Minute))
	seedDispatch(t, d, "018f3e1a-0d02-7000-8000-000000000003", ev, idChanHook, 1, now.Add(time.Hour)) // 未到期
	seedDispatch(t, d, "018f3e1a-0d02-7000-8000-000000000004", ev, idChanOff, 1, now)                 // paused 渠道

	claimed, err := repo.Claim(ctx, 10, 5*time.Minute, []string{idChanOff})
	if err != nil || len(claimed) != 2 {
		t.Fatalf("Claim() = %d, %v; want 2", len(claimed), err)
	}
	for _, c := range claimed {
		if c.Status != 2 || c.Attempts != 1 || c.NextRetryTime.Before(now.Add(4*time.Minute)) {
			t.Fatalf("claimed = %+v", c.Dispatch)
		}
	}
	// 二次声明：剩余行不到期/被排除 → 0。
	if again, _ := repo.Claim(ctx, 10, time.Minute, []string{idChanOff}); len(again) != 0 {
		t.Fatalf("second claim = %d, want 0", len(again))
	}
	hook, mail := claimed[0], claimed[1]
	if hook.ChannelID != idChanHook {
		hook, mail = mail, hook
	}
	// 渲染快照 + 成功落账。
	if err := repo.SaveContent(ctx, hook, []byte(`{"subject":"s","body":"b"}`), idTpl); err != nil {
		t.Fatal(err)
	}
	if count(t, d, "SELECT count(*) FROM notify_dispatch WHERE id = $1 AND template_id = $2", hook.ID, idTpl) != 1 {
		t.Fatal("template_id column must be backfilled")
	}
	if err := repo.Finish(ctx, hook, biz.FinishResult{OK: true, ProviderMsgID: "p", LatencyMs: 12}, 6); err != nil {
		t.Fatal(err)
	}
	if count(t, d, "SELECT count(*) FROM notify_dispatch WHERE id = $1 AND status = 3 AND next_retry_time IS NULL", hook.ID) != 1 ||
		count(t, d, "SELECT count(*) FROM notify_delivery_log WHERE dispatch_id = $1 AND ok AND attempt = 1", hook.ID) != 1 {
		t.Fatal("sent dispatch / delivery log not persisted")
	}
	// 围栏：已终态的旧持有者再写被拒。
	if err := repo.Finish(ctx, hook, biz.FinishResult{OK: true}, 6); !errors.Is(err, biz.ErrStaleClaim) {
		t.Fatalf("stale finish error = %v", err)
	}
	// 释放：回 pending、尝试撤销、到期后移。
	if err := repo.Release(ctx, mail, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if count(t, d, "SELECT count(*) FROM notify_dispatch WHERE id = $1 AND status = 1 AND attempts = 0 AND next_retry_time > now()", mail.ID) != 1 {
		t.Fatal("release did not restore pending state")
	}
	if err := repo.Release(ctx, mail, time.Second); !errors.Is(err, biz.ErrStaleClaim) {
		t.Fatalf("double release error = %v", err)
	}
	// 可重试失败 → 4 + next_retry；超限 → 5 + dlq。
	_, _ = d.Pool.Exec(ctx, "UPDATE notify_dispatch SET next_retry_time = now() WHERE id = $1", mail.ID)
	claimed, _ = repo.Claim(ctx, 10, time.Minute, []string{idChanOff})
	if len(claimed) != 1 {
		t.Fatalf("reclaim = %d", len(claimed))
	}
	mail = claimed[0]
	if err := repo.Finish(ctx, mail, biz.FinishResult{Retryable: true, Error: "timeout", NextRetry: time.Now().Add(time.Hour)}, 6); err != nil {
		t.Fatal(err)
	}
	if count(t, d, "SELECT count(*) FROM notify_dispatch WHERE id = $1 AND status = 4 AND last_error = 'timeout' AND next_retry_time > now() + interval '50 minutes'", mail.ID) != 1 {
		t.Fatal("retryable failure not persisted")
	}
	_, _ = d.Pool.Exec(ctx, "UPDATE notify_dispatch SET next_retry_time = now(), attempts = 5 WHERE id = $1", mail.ID)
	claimed, _ = repo.Claim(ctx, 10, time.Minute, []string{idChanOff})
	mail = claimed[0]
	if mail.Attempts != 6 {
		t.Fatalf("attempts = %d, want 6", mail.Attempts)
	}
	if err := repo.Finish(ctx, mail, biz.FinishResult{Retryable: true, Error: "still failing", NextRetry: time.Now()}, 6); err != nil {
		t.Fatal(err)
	}
	if count(t, d, "SELECT count(*) FROM notify_dispatch WHERE id = $1 AND status = 5", mail.ID) != 1 ||
		count(t, d, "SELECT count(*) FROM notify_dlq WHERE id = $1 AND dispatch_id = $1 AND event_id = $2 AND attempts = 6 AND replay_state = 1", mail.ID, ev) != 1 {
		t.Fatal("dead letter not persisted")
	}
	if count(t, d, "SELECT count(*) FROM notify_delivery_log WHERE dispatch_id = $1", mail.ID) != 2 {
		t.Fatal("every attempt must be logged")
	}
}

// TestIntegration_DispatchReapExpired 租约过期回收：记一次失败尝试并按退避转 4；
// 迟到的旧持有者被围栏拒绝；超限转死信。
func TestIntegration_DispatchReapExpired(t *testing.T) {
	d := newIntegrationData(t)
	seedConfig(t, d, "http://127.0.0.1:9/hook")
	ctx := context.Background()
	repo := NewDispatchRepo(d)
	const ev = "018f3e1a-0d03-7000-8000-000000000001"
	seedEvent(t, d, ev, "monitor.alert.firing", 2, "", nil)
	seedDispatch(t, d, "018f3e1a-0d04-7000-8000-000000000001", ev, idChanHook, 1, time.Now())
	seedDispatch(t, d, "018f3e1a-0d04-7000-8000-000000000002", ev, idChanHook, 1, time.Now())
	_, _ = d.Pool.Exec(ctx, "UPDATE notify_dispatch SET attempts = 5 WHERE id = $1", "018f3e1a-0d04-7000-8000-000000000002")

	claimed, err := repo.Claim(ctx, 10, time.Millisecond, nil) // 极短租约模拟崩溃
	if err != nil || len(claimed) != 2 {
		t.Fatalf("Claim() = %d, %v", len(claimed), err)
	}
	time.Sleep(20 * time.Millisecond)
	n, err := repo.ReapExpired(ctx, 10, 6, func(int32) time.Duration { return 30 * time.Second })
	if err != nil || n != 2 {
		t.Fatalf("ReapExpired() = %d, %v", n, err)
	}
	if count(t, d, "SELECT count(*) FROM notify_dispatch WHERE id = $1 AND status = 4 AND attempts = 1 AND last_error LIKE 'lease expired%'", "018f3e1a-0d04-7000-8000-000000000001") != 1 {
		t.Fatal("reaped dispatch should be failed with backoff")
	}
	if count(t, d, "SELECT count(*) FROM notify_dispatch WHERE id = $1 AND status = 5", "018f3e1a-0d04-7000-8000-000000000002") != 1 ||
		count(t, d, "SELECT count(*) FROM notify_dlq WHERE id = $1", "018f3e1a-0d04-7000-8000-000000000002") != 1 {
		t.Fatal("reaped dispatch over max attempts should be dead-lettered")
	}
	// 迟到的旧持有者：围栏拒绝，不覆盖状态。
	for _, c := range claimed {
		if err := repo.Finish(ctx, c, biz.FinishResult{OK: true}, 6); !errors.Is(err, biz.ErrStaleClaim) {
			t.Fatalf("stale finisher error = %v, want ErrStaleClaim", err)
		}
	}
	if count(t, d, "SELECT count(*) FROM notify_dispatch WHERE status = 3") != 0 {
		t.Fatal("stale finisher must not mark sent")
	}
	if count(t, d, "SELECT count(*) FROM notify_delivery_log") != 2 {
		t.Fatal("reaper logs exactly one attempt per dispatch")
	}
}

func TestIntegration_RateBucket(t *testing.T) {
	d := newIntegrationData(t)
	ctx := context.Background()
	repo := NewRateBucketRepo(d)
	ch := &biz.Channel{ID: idChanEmail, RateLimit: biz.RateLimit{QPS: 2, Burst: 2}}
	if err := repo.SyncBuckets(ctx, []*biz.Channel{ch}); err != nil {
		t.Fatal(err)
	}
	taken := 0
	var wait time.Duration
	for i := 0; i < 5; i++ {
		ok, w, err := repo.Take(ctx, ch)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			taken++
		} else {
			wait = w
		}
	}
	if taken != 2 || wait <= 0 || wait > time.Second {
		t.Fatalf("taken = %d (want 2), wait = %v", taken, wait)
	}
	// 无桶（qps=0）直接放行。
	if ok, _, err := repo.Take(ctx, &biz.Channel{ID: idChanHook}); err != nil || !ok {
		t.Fatalf("unlimited channel: %v %v", ok, err)
	}
	// 参数变化幂等同步：容量缩小不超过新容量。
	ch.RateLimit = biz.RateLimit{QPS: 1, Burst: 1}
	if err := repo.SyncBuckets(ctx, []*biz.Channel{ch}); err != nil {
		t.Fatal(err)
	}
	var capacity float64
	_ = d.Pool.QueryRow(ctx, "SELECT capacity FROM notify_rate_bucket WHERE channel_id = $1", idChanEmail).Scan(&capacity)
	if capacity != 1 {
		t.Fatalf("capacity = %v, want 1", capacity)
	}
	// qps 归零：桶行删除。
	ch.RateLimit = biz.RateLimit{}
	_ = repo.SyncBuckets(ctx, []*biz.Channel{ch})
	if count(t, d, "SELECT count(*) FROM notify_rate_bucket") != 0 {
		t.Fatal("bucket row should be removed when qps=0")
	}
}

func TestIntegration_AdvisoryLock(t *testing.T) {
	d := newIntegrationData(t)
	ctx := context.Background()
	l := NewLocker(d)
	release, ok, err := l.TryLock(ctx, biz.LockSweeper)
	if err != nil || !ok {
		t.Fatalf("TryLock() = %v, %v", ok, err)
	}
	if _, ok2, err := l.TryLock(ctx, biz.LockSweeper); err != nil || ok2 {
		t.Fatalf("second TryLock while held = %v, %v; want false", ok2, err)
	}
	if _, ok3, _ := l.TryLock(ctx, biz.LockMaintenance); !ok3 {
		t.Fatal("different key must be independently lockable")
	}
	release()
	release2, ok, err := l.TryLock(ctx, biz.LockSweeper)
	if err != nil || !ok {
		t.Fatalf("TryLock after release = %v, %v", ok, err)
	}
	release2()
}
