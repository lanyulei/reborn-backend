//go:build integration

package data

import (
	"context"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/notify/internal/biz"
)

func TestIntegration_IngressDedupGate(t *testing.T) {
	d := newIntegrationData(t)
	ctx := context.Background()
	repo := NewIngressRepo(d)
	mk := func(id, dedup string) *biz.Event {
		return &biz.Event{
			ID: id, BizID: "monitor", EventType: "monitor.alert.firing", Severity: 2, Title: "t", Payload: []byte(`{"a":1}`),
			DedupKey: dedup, Recipients: []string{"ops@example.com"}, Source: biz.EventSourceAPI, Status: 1, NextPublishTime: time.Now(), CreateTime: time.Now(),
		}
	}
	res, err := repo.SaveEvents(ctx, nil, []*biz.Event{
		mk("018f3e1a-0f01-7000-8000-000000000001", "fp-1"),
		mk("018f3e1a-0f01-7000-8000-000000000002", "fp-1"), // 同批重复
		mk("018f3e1a-0f01-7000-8000-000000000003", ""),     // 无 dedup
	}, time.Hour)
	if err != nil || len(res) != 3 {
		t.Fatalf("SaveEvents() = %v, %v", res, err)
	}
	if res[0].Deduplicated || !res[1].Deduplicated || res[1].EventID != res[0].EventID || res[2].Deduplicated {
		t.Fatalf("results = %+v", res)
	}
	if count(t, d, "SELECT count(*) FROM notify_event") != 2 || count(t, d, "SELECT count(*) FROM notify_dedup") != 1 {
		t.Fatal("dedup must suppress the duplicate insert")
	}
	// 跨批重复仍命中。
	res, _ = repo.SaveEvents(ctx, nil, []*biz.Event{mk("018f3e1a-0f01-7000-8000-000000000004", "fp-1")}, time.Hour)
	if !res[0].Deduplicated || res[0].EventID != "018f3e1a-0f01-7000-8000-000000000001" {
		t.Fatalf("cross-batch dedup = %+v", res[0])
	}
	// 门闩过期：同键视为新事件并原地替换。
	_, _ = d.Pool.Exec(ctx, "UPDATE notify_dedup SET expire_time = now() - interval '1 second'")
	res, _ = repo.SaveEvents(ctx, nil, []*biz.Event{mk("018f3e1a-0f01-7000-8000-000000000005", "fp-1")}, time.Hour)
	if res[0].Deduplicated || count(t, d, "SELECT count(*) FROM notify_dedup WHERE event_id = $1", "018f3e1a-0f01-7000-8000-000000000005") != 1 {
		t.Fatalf("expired gate must admit new event: %+v", res[0])
	}
	// 落库口径：recipients jsonb 数组、payload 原文、source=api。
	var recipients []byte
	var source string
	_ = d.Pool.QueryRow(ctx, "SELECT recipients, source FROM notify_event WHERE id = $1", "018f3e1a-0f01-7000-8000-000000000001").Scan(&recipients, &source)
	if string(recipients) != `["ops@example.com"]` || source != "api" {
		t.Fatalf("stored recipients=%s source=%s", recipients, source)
	}
	// 清理过期门闩。
	_, _ = d.Pool.Exec(ctx, "UPDATE notify_dedup SET expire_time = now() - interval '1 second'")
	if n, err := NewMaintenanceRepo(d).DeleteExpiredDedup(ctx, 100); err != nil || n != 1 {
		t.Fatalf("DeleteExpiredDedup = %d, %v", n, err)
	}
}

func TestIntegration_DlqReplay(t *testing.T) {
	d := newIntegrationData(t)
	seedConfig(t, d, "http://127.0.0.1:9/hook")
	ctx := context.Background()
	const ev = "018f3e1a-1001-7000-8000-000000000001"
	seedEvent(t, d, ev, "monitor.alert.firing", 2, "", nil)
	exec := func(sql string, args ...any) {
		if _, err := d.Pool.Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec(`INSERT INTO notify_dlq (id, dispatch_id, event_id, channel_id, recipient, last_error, attempts, replay_state) VALUES
		('018f3e1a-1002-7000-8000-000000000001', '018f3e1a-1002-7000-8000-000000000001', $1, $2, 'ops@example.com', 'boom', 6, 2),
		('018f3e1a-1002-7000-8000-000000000002', '018f3e1a-1002-7000-8000-000000000002', $1, $2, 'ops@example.com', 'boom', 6, 1)`, ev, idChanHook)
	uc := biz.NewDlqUsecase(NewDlqRepo(d), testLogger())
	n, err := uc.ReplayOnce(ctx, 10)
	if err != nil || n != 1 {
		t.Fatalf("ReplayOnce() = %d, %v", n, err)
	}
	if count(t, d, "SELECT count(*) FROM notify_dlq WHERE replay_state = 3") != 1 || count(t, d, "SELECT count(*) FROM notify_dlq WHERE replay_state = 1") != 1 {
		t.Fatal("replay state transitions wrong")
	}
	if count(t, d, "SELECT count(*) FROM notify_dispatch WHERE event_id = $1 AND status = 1 AND channel_id = $2 AND next_retry_time <= now()", ev, idChanHook) != 1 {
		t.Fatal("replay must create a fresh pending dispatch")
	}
	// 新 dispatch 的 send_key 与死信源不同（刻意再投）。
	if count(t, d, "SELECT count(*) FROM notify_dispatch WHERE send_key = 'k'") != 0 {
		t.Fatal("replayed dispatch must carry a new send_key")
	}
	if n, _ := uc.ReplayOnce(ctx, 10); n != 0 {
		t.Fatal("replay must be idempotent")
	}
}

func TestIntegration_PartitionMaintenance(t *testing.T) {
	d := newIntegrationData(t)
	ctx := context.Background()
	repo := NewMaintenanceRepo(d)
	uc := biz.NewMaintenanceUsecase(repo, testLogger()).WithPartitionsAhead(3)
	now := time.Now().UTC()
	target := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, 3, 0)
	name := partitionName("notify_event", target)
	_, _ = d.Pool.Exec(ctx, "DROP TABLE IF EXISTS "+name)
	// DEFAULT 分区里预埋一行该月数据：EnsurePartition 须先迁出再 ATTACH。
	if _, err := d.Pool.Exec(ctx,
		`INSERT INTO notify_event (id, biz_id, event_type, severity, title, source, status, publish_state, publish_attempts, next_publish_time, create_time)
		 VALUES ('018f3e1a-1101-7000-8000-000000000001', 'default', 'a.b', 1, 't', 'api', 1, 1, 0, $1, $1)`, target.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if count(t, d, "SELECT count(*) FROM notify_event_default") != 1 {
		t.Fatal("row should have landed in DEFAULT partition")
	}
	if err := uc.RunOnce(ctx, now); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if count(t, d, "SELECT count(*) FROM pg_inherits WHERE inhparent = 'notify_event'::regclass AND inhrelid = to_regclass($1)", name) != 1 {
		t.Fatalf("partition %s not attached", name)
	}
	if count(t, d, "SELECT count(*) FROM notify_event_default") != 0 || count(t, d, "SELECT count(*) FROM "+name) != 1 {
		t.Fatal("row must be moved from DEFAULT into the new partition")
	}
	for _, table := range []string{"notify_dispatch", "notify_delivery_log"} {
		if count(t, d, "SELECT count(*) FROM pg_inherits WHERE inhparent = $1::regclass AND inhrelid = to_regclass($2)", table, partitionName(table, target)) != 1 {
			t.Fatalf("partition for %s not created", table)
		}
	}
	// 幂等：再跑一轮无错。
	if err := uc.RunOnce(ctx, now); err != nil {
		t.Fatalf("second RunOnce() error = %v", err)
	}
	// 过期分区 DROP：造一个 2020-01 分区再按保留期清理。
	old := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := repo.EnsurePartition(ctx, "notify_dispatch", old); err != nil {
		t.Fatal(err)
	}
	n, err := repo.DropPartitionsBefore(ctx, "notify_dispatch", now.Add(-180*24*time.Hour))
	if err != nil || n != 1 {
		t.Fatalf("DropPartitionsBefore = %d, %v", n, err)
	}
	if count(t, d, "SELECT count(*) FROM pg_class WHERE relname = $1", partitionName("notify_dispatch", old)) != 0 {
		t.Fatal("old partition should be dropped")
	}
	// 清理：本测试新建的未来分区删掉，避免影响他测/本地库形态。
	for _, table := range biz.PartitionedTables {
		_, _ = d.Pool.Exec(ctx, "DROP TABLE IF EXISTS "+partitionName(table, target))
	}
}
