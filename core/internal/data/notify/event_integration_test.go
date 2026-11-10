//go:build integration

package data

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
)

func TestIntegration_EventList(t *testing.T) {
	d := newIntegrationNotifyData(t)
	repo := NewEventRepo(d)
	ctx := context.Background()

	// 种子：两 biz、三种状态、显式收件与载荷形态各异的 5 行；末行落在
	// 2026-12 分区（跨分区查询——父表查询自动 UNION 命中分区）。
	now := time.Now().UTC().Truncate(time.Microsecond)
	base := now.Add(-time.Hour)
	rows := []struct {
		id, bizID, eventType, title, payload, recipients string
		severity, status, publishState                   int32
		createTime                                       time.Time
	}{
		{
			"018f3e1a-0304-7000-8000-0000000000d1", "monitor", "monitor.alert.firing", "CPU 告警",
			`{"host":"web-1","cpu":92.5}`, `["ops@example.com"]`, 3, 2, 1, base,
		},
		{
			"018f3e1a-0304-7000-8000-0000000000d2", "monitor", "monitor.host.down", "主机失联",
			``, ``, 4, 3, 1, base.Add(time.Minute),
		},
		{
			"018f3e1a-0304-7000-8000-0000000000d3", "core", "permission.change", "角色权限变更",
			`{"role_id":"r-1","nested":{"k":[1,2]}}`, ``, 2, 1, 0, base.Add(2 * time.Minute),
		},
		{
			"018f3e1a-0304-7000-8000-0000000000d4", "core", "permission.change", "用户授权变更",
			`{"user":"u-9"}`, `["admin@example.com","ops@example.com"]`, 2, 4, 1, base.Add(3 * time.Minute),
		},
		{
			"018f3e1a-0304-7000-8000-0000000000d5", "monitor", "monitor.alert.firing", "跨分区行",
			`{"host":"db-1"}`, ``, 3, 1, 0, time.Date(2026, 12, 5, 0, 0, 0, 0, time.UTC),
		},
	}
	for _, row := range rows {
		var payload, recipients any
		if row.payload != "" {
			payload = row.payload
		}
		if row.recipients != "" {
			recipients = row.recipients
		}
		if _, err := d.Pool.Exec(ctx,
			`INSERT INTO notify_event (id, biz_id, event_type, severity, title, payload, recipients,
			     source, status, publish_state, publish_attempts, next_publish_time, create_time)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, 'api', $8, $9, 0, $10, $11)`,
			row.id, row.bizID, row.eventType, row.severity, row.title, payload, recipients,
			row.status, row.publishState, row.createTime, row.createTime); err != nil {
			t.Fatalf("seed event %s: %v", row.id, err)
		}
	}

	// 全量：倒序（跨分区的 12 月行最新在前）+ jsonb 往返。
	items, total, err := repo.ListEvents(ctx, biz.EventFilter{}, 1, 20)
	if err != nil {
		t.Fatalf("ListEvents() error = %v", err)
	}
	if total != 5 || len(items) != 5 {
		t.Fatalf("ListEvents() = %d items, total %d, want 5/5", len(items), total)
	}
	if items[0].ID != "018f3e1a-0304-7000-8000-0000000000d5" {
		t.Fatalf("ListEvents() first = %s, want cross-partition newest row", items[0].ID)
	}
	alert := items[4]
	// jsonb 往返会规范化键序与空白，做语义比对而非字节比对。
	var gotPayload, wantPayload map[string]any
	if err := json.Unmarshal(alert.Payload, &gotPayload); err != nil {
		t.Fatalf("ListEvents() payload = %q: %v", alert.Payload, err)
	}
	if err := json.Unmarshal([]byte(`{"host":"web-1","cpu":92.5}`), &wantPayload); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotPayload, wantPayload) {
		t.Fatalf("ListEvents() payload = %q, want jsonb roundtrip of %v", alert.Payload, wantPayload)
	}
	if len(alert.Recipients) != 1 || alert.Recipients[0] != "ops@example.com" {
		t.Fatalf("ListEvents() recipients = %v", alert.Recipients)
	}
	down := items[3]
	if down.Payload != nil || down.Recipients != nil {
		t.Fatalf("ListEvents() nullable jsonb = (%v, %v), want nil/nil", down.Payload, down.Recipients)
	}

	// biz + event_type 精确过滤。
	items, total, err = repo.ListEvents(ctx, biz.EventFilter{BizID: "monitor", EventType: "monitor.alert.firing"}, 1, 20)
	if err != nil || total != 2 {
		t.Fatalf("ListEvents(biz+type) = (%d, %v), want 2", total, err)
	}
	// status 过滤。
	failed := int32(biz.EventStatusFailed)
	items, total, err = repo.ListEvents(ctx, biz.EventFilter{Status: &failed}, 1, 20)
	if err != nil || total != 1 || items[0].ID != "018f3e1a-0304-7000-8000-0000000000d4" {
		t.Fatalf("ListEvents(status) = (%+v, %v)", items, err)
	}
	// 时间区间（含边界）：[base, base+2min] 命中 3 行（base、+1min、+2min）。
	end := base.Add(2 * time.Minute)
	items, total, err = repo.ListEvents(ctx, biz.EventFilter{CreateTimeStart: &base, CreateTimeEnd: &end}, 1, 20)
	if err != nil || total != 3 {
		t.Fatalf("ListEvents(time range) = (%d, %v), want 3（含边界）", total, err)
	}
	// 分页 + 倒序稳定（create_time DESC, id DESC）：5 行 pageSize=2，第 3 页
	// 恰剩最旧一行。
	items, total, err = repo.ListEvents(ctx, biz.EventFilter{}, 3, 2)
	if err != nil || total != 5 || len(items) != 1 || items[0].ID != "018f3e1a-0304-7000-8000-0000000000d1" {
		t.Fatalf("ListEvents(paged) = (%d items, total %d, %v)", len(items), total, err)
	}
	// 无匹配。
	items, total, err = repo.ListEvents(ctx, biz.EventFilter{BizID: "no-such-biz"}, 1, 20)
	if err != nil || total != 0 || items != nil {
		t.Fatalf("ListEvents(no match) = (%v, %d, %v), want nil/0", items, total, err)
	}

	// 运行表不参与配置轮询：cfg_version 无 notify_event 行（且本测试无写路径）。
	var n int
	if err := d.Pool.QueryRow(ctx,
		"SELECT count(*) FROM notify_cfg_version WHERE table_name = 'notify_event'").Scan(&n); err != nil {
		t.Fatalf("count cfg_version rows: %v", err)
	}
	if n != 0 {
		t.Fatalf("notify_cfg_version has %d notify_event rows, want 0（运行表不 bump）", n)
	}
}
