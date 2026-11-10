//go:build integration

package data

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/notify/internal/biz"
)

// TestIntegration_ProducerQuota 按生产者配额：秒窗 2 条 → 第 3 条 429 且整批不落库；
// 秒窗翻滚后恢复；日窗独立计数；无配额生产者不受限。
func TestIntegration_ProducerQuota(t *testing.T) {
	d := newIntegrationData(t)
	ctx := context.Background()
	repo := NewIngressRepo(d)
	mk := func(n int) []*biz.Event {
		out := make([]*biz.Event, 0, n)
		for i := 0; i < n; i++ {
			out = append(out, &biz.Event{
				BizID: "monitor", EventType: "monitor.alert.firing", Severity: 2, Title: "q",
				Source: biz.EventSourceAPI, Status: 1, NextPublishTime: time.Now(), CreateTime: time.Now(),
			})
		}
		return out
	}
	producer := &biz.Producer{ID: idProducer, BizID: "monitor", QPSLimit: 2, DailyLimit: 3}
	uc := biz.NewIngressUsecase(repo)
	if _, err := uc.Accept(ctx, producer, mk(2)); err != nil {
		t.Fatalf("first 2 within qps: %v", err)
	}
	if _, err := uc.Accept(ctx, producer, mk(1)); !errors.Is(err, biz.ErrQuotaExceeded) {
		t.Fatalf("third in same second: %v, want ErrQuotaExceeded", err)
	}
	if count(t, d, "SELECT count(*) FROM notify_event") != 2 {
		t.Fatal("rejected batch must not persist any event")
	}
	// 秒窗翻滚（把窗口起点拨到过去）→ 放行 1 条；日窗累计 3 → 第 4 条被日配额拒绝。
	_, _ = d.Pool.Exec(ctx, "UPDATE notify_quota_counter SET window_start = window_start - interval '2 seconds' WHERE window_kind = 1")
	if _, err := uc.Accept(ctx, producer, mk(1)); err != nil {
		t.Fatalf("after second rollover: %v", err)
	}
	_, _ = d.Pool.Exec(ctx, "UPDATE notify_quota_counter SET window_start = window_start - interval '2 seconds' WHERE window_kind = 1")
	if _, err := uc.Accept(ctx, producer, mk(1)); !errors.Is(err, biz.ErrQuotaExceeded) {
		t.Fatalf("daily limit: %v, want ErrQuotaExceeded", err)
	}
	if count(t, d, "SELECT count(*) FROM notify_event") != 3 {
		t.Fatal("daily-rejected batch must not persist")
	}
	// 批量超限：limit 2 一次投 3 条 → 整批拒绝。
	fresh := &biz.Producer{ID: "018f3e1a-0b05-7000-8000-000000000002", BizID: "monitor", QPSLimit: 2}
	if _, err := uc.Accept(ctx, fresh, mk(3)); !errors.Is(err, biz.ErrQuotaExceeded) {
		t.Fatalf("batch over limit: %v", err)
	}
	// 无配额生产者不受限。
	free := &biz.Producer{ID: "018f3e1a-0b05-7000-8000-000000000003", BizID: "monitor"}
	if _, err := uc.Accept(ctx, free, mk(50)); err != nil {
		t.Fatalf("unlimited producer: %v", err)
	}
}

// TestIntegration_SharedBreakerOnPG 两个 SharedBreaker 实例经真实健康表共享状态：
// A 打开 → B 刷新后拒绝；到期仅一方领探测；探测成功双方放行；清理只删关闭态旧行。
func TestIntegration_SharedBreakerOnPG(t *testing.T) {
	d := newIntegrationData(t)
	ctx := context.Background()
	repo := NewHealthRepo(d)
	a, b := biz.NewSharedBreaker(repo, testLogger()), biz.NewSharedBreaker(repo, testLogger())
	ch := idChanHook
	for i := 0; i < 10; i++ {
		if !a.Allow(ctx, ch) {
			t.Fatal("must allow before trip")
		}
		a.Record(ctx, ch, false)
	}
	if !a.Open(ch) {
		t.Fatal("breaker should be open after 10 failures")
	}
	// 强制刷新（周期节流：直接读仓储装入）。
	open, _ := repo.ListOpen(ctx)
	if _, ok := open[ch]; !ok {
		t.Fatal("open state not persisted")
	}
	b.Refresh(ctx)
	if b.Allow(ctx, ch) {
		t.Fatal("B must deny once it has refreshed")
	}
	// 迟到结果不改状态；到期后仅一方领到探测。
	a.Record(ctx, ch, true)
	if count(t, d, "SELECT count(*) FROM notify_channel_health WHERE channel_id = $1 AND open_until > now()", ch) != 1 {
		t.Fatal("late result must not close an open breaker")
	}
	_, _ = d.Pool.Exec(ctx, "UPDATE notify_channel_health SET open_until = now() - interval '1 second' WHERE channel_id = $1", ch)
	open, _ = repo.ListOpen(ctx)
	if _, ok := open[ch]; !ok {
		t.Fatal("recently expired open must still be listed for probing")
	}
	claimedA, _, _ := repo.ClaimProbe(ctx, ch)
	claimedB, closedB, _ := repo.ClaimProbe(ctx, ch)
	if !claimedA || claimedB || closedB {
		t.Fatalf("probe claims: a=%v b=%v closed=%v, want only A", claimedA, claimedB, closedB)
	}
	if until, err := repo.Record(ctx, ch, true); err != nil || !until.IsZero() {
		t.Fatalf("probe success must close: %v %v", until, err)
	}
	if _, closed, _ := repo.ClaimProbe(ctx, ch); !closed {
		t.Fatal("after close, ClaimProbe must report closed")
	}
	// 探测失败 → 重开。
	_, _ = d.Pool.Exec(ctx, "UPDATE notify_channel_health SET open_until = now() - interval '1 second' WHERE channel_id = $1", ch)
	repo.ClaimProbe(ctx, ch)
	if until, _ := repo.Record(ctx, ch, false); until.IsZero() {
		t.Fatal("probe failure must reopen")
	}
	// 清理：关闭态旧行删除，打开态保留。
	_, _ = d.Pool.Exec(ctx, "INSERT INTO notify_channel_health (channel_id, window_start, update_time) VALUES ($1, now(), now() - interval '8 days')", idChanEmail)
	n, err := NewMaintenanceRepo(d).DeleteStaleChannelHealth(ctx, time.Now().Add(-7*24*time.Hour), 100)
	if err != nil || n != 1 {
		t.Fatalf("DeleteStaleChannelHealth = %d, %v", n, err)
	}
	if count(t, d, "SELECT count(*) FROM notify_channel_health") != 1 {
		t.Fatal("open breaker row must survive cleanup")
	}
}
