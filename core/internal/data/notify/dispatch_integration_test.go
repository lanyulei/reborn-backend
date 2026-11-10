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

func TestIntegration_DispatchList(t *testing.T) {
	d := newIntegrationNotifyData(t)
	repo := NewDispatchRepo(d)
	ctx := context.Background()

	// 种子：两事件、两渠道、三种状态、可空列形态各异的 5 行；末行落在
	// 2026-12 分区（跨分区查询——父表查询自动 UNION 命中分区）。
	now := time.Now().UTC().Truncate(time.Microsecond)
	base := now.Add(-time.Hour)
	eventID := "018f3e1a-0304-7000-8000-0000000000e1"
	eventIDAlt := "018f3e1a-0304-7000-8000-0000000000e2"
	channelID := "018f3e1a-0304-7000-8000-0000000000c1"
	channelIDAlt := "018f3e1a-0304-7000-8000-0000000000c2"
	rows := []struct {
		id, channel, recipient, template, content string
		status, attempts                          int32
		createTime                                time.Time
	}{
		{
			"018f3e1a-0304-7000-8000-0000000000d1", channelID, "ops@example.com",
			"018f3e1a-0304-7000-8000-0000000000a1", `{"subject":"CPU 告警"}`, 4, 2, base,
		},
		{
			"018f3e1a-0304-7000-8000-0000000000d2", channelIDAlt, "https://hook.example.com/ops",
			"", ``, 3, 1, base.Add(time.Minute),
		},
		{
			"018f3e1a-0304-7000-8000-0000000000d3", channelID, "ops-group",
			"", ``, 5, 6, base.Add(2 * time.Minute),
		},
		{
			"018f3e1a-0304-7000-8000-0000000000d4", channelID, "dev@example.com",
			"", `{"subject":"低盘通知"}`, 1, 0, base.Add(3 * time.Minute),
		},
		{
			"018f3e1a-0304-7000-8000-0000000000d5", channelIDAlt, "ops@example.com",
			"", `{"subject":"跨分区行"}`, 2, 1, time.Date(2026, 12, 5, 0, 0, 0, 0, time.UTC),
		},
	}
	for _, row := range rows {
		var template, content any
		if row.template != "" {
			template = row.template
		}
		if row.content != "" {
			content = row.content
		}
		if _, err := d.Pool.Exec(ctx,
			`INSERT INTO notify_dispatch (id, event_id, channel_id, recipient, template_id, content,
			     status, attempts, next_retry_time, last_error, send_key, create_time)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
			row.id, eventID, row.channel, row.recipient, template, content,
			row.status, row.attempts,
			// pending/sending/failed 行恒有到期时间（0021 CHECK：status 1/2/4 须非 NULL；
			// sending 为租约到期）；sent 行为 NULL；dead 行保留转死信时的退避时间。
			map[int32]any{
				1: base,
				2: base.Add(5 * time.Minute),
				4: base.Add(5 * time.Minute),
				5: base.Add(10 * time.Minute),
			}[row.status],
			map[int32]any{
				4: "smtp: 421 service unavailable",
				5: "dingtalk: token invalid",
			}[row.status],
			row.id[:20], row.createTime); err != nil {
			t.Fatalf("seed dispatch %s: %v", row.id, err)
		}
	}
	// 第二事件独立一行（验证事件下钻排除法）。
	if _, err := d.Pool.Exec(ctx,
		`INSERT INTO notify_dispatch (id, event_id, channel_id, recipient, template_id, content,
		     status, attempts, next_retry_time, last_error, send_key, create_time)
		 VALUES ($1, $2, $3, $4, NULL, NULL, 3, 1, NULL, NULL, $5, $6)`,
		"018f3e1a-0304-7000-8000-0000000000d6", eventIDAlt, channelID,
		"alt@example.com", "018f3e1a-0304-7000-8", base.Add(4*time.Minute)); err != nil {
		t.Fatalf("seed alt dispatch: %v", err)
	}

	// 全量：倒序（跨分区的 12 月行最新在前）+ jsonb 往返 + 可空列 NULL。
	items, total, err := repo.ListDispatches(ctx, biz.DispatchFilter{}, 1, 20)
	if err != nil {
		t.Fatalf("ListDispatches() error = %v", err)
	}
	if total != 6 || len(items) != 6 {
		t.Fatalf("ListDispatches() = %d items, total %d, want 6/6", len(items), total)
	}
	if items[0].ID != "018f3e1a-0304-7000-8000-0000000000d5" {
		t.Fatalf("ListDispatches() first = %s, want cross-partition newest row", items[0].ID)
	}
	failed := items[5]
	// jsonb 往返会规范化键序与空白，做语义比对而非字节比对。
	var gotContent, wantContent map[string]any
	if err := json.Unmarshal(failed.Content, &gotContent); err != nil {
		t.Fatalf("ListDispatches() content = %q: %v", failed.Content, err)
	}
	if err := json.Unmarshal([]byte(`{"subject":"CPU 告警"}`), &wantContent); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotContent, wantContent) {
		t.Fatalf("ListDispatches() content = %q, want jsonb roundtrip of %v", failed.Content, wantContent)
	}
	if failed.TemplateID != "018f3e1a-0304-7000-8000-0000000000a1" {
		t.Fatalf("ListDispatches() templateId = %q", failed.TemplateID)
	}
	if failed.NextRetry == nil || failed.NextRetry.Sub(base) != 5*time.Minute {
		t.Fatalf("ListDispatches() nextRetry = %v, want base+5m", failed.NextRetry)
	}
	if failed.LastError != "smtp: 421 service unavailable" {
		t.Fatalf("ListDispatches() lastError = %q", failed.LastError)
	}
	sent := items[4]
	if sent.Content != nil || sent.NextRetry != nil || sent.LastError != "" || sent.TemplateID != "" {
		t.Fatalf("ListDispatches() nullable cols = (%v, %v, %q, %q), want nil/nil/空",
			sent.Content, sent.NextRetry, sent.LastError, sent.TemplateID)
	}

	// 事件下钻：第一事件 5 行（另一事件的行被排除）。
	items, total, err = repo.ListDispatches(ctx, biz.DispatchFilter{EventID: eventID}, 1, 20)
	if err != nil || total != 5 {
		t.Fatalf("ListDispatches(event) = (%d, %v), want 5", total, err)
	}
	// 渠道过滤。
	items, total, err = repo.ListDispatches(ctx, biz.DispatchFilter{ChannelID: channelIDAlt}, 1, 20)
	if err != nil || total != 2 {
		t.Fatalf("ListDispatches(channel) = (%d, %v), want 2", total, err)
	}
	// status 过滤（死信行）。
	dead := int32(biz.DispatchStatusDead)
	items, total, err = repo.ListDispatches(ctx, biz.DispatchFilter{Status: &dead}, 1, 20)
	if err != nil || total != 1 || items[0].ID != "018f3e1a-0304-7000-8000-0000000000d3" {
		t.Fatalf("ListDispatches(status) = (%+v, %v)", items, err)
	}
	// 时间区间（含边界）：[base, base+2min] 命中 3 行（base、+1min、+2min）。
	end := base.Add(2 * time.Minute)
	items, total, err = repo.ListDispatches(ctx, biz.DispatchFilter{CreateTimeStart: &base, CreateTimeEnd: &end}, 1, 20)
	if err != nil || total != 3 {
		t.Fatalf("ListDispatches(time range) = (%d, %v), want 3（含边界）", total, err)
	}
	// 分页 + 倒序稳定（create_time DESC, id DESC）：6 行 pageSize=5，第 2 页
	// 恰剩最旧一行。
	items, total, err = repo.ListDispatches(ctx, biz.DispatchFilter{}, 2, 5)
	if err != nil || total != 6 || len(items) != 1 || items[0].ID != "018f3e1a-0304-7000-8000-0000000000d1" {
		t.Fatalf("ListDispatches(paged) = (%d items, total %d, %v)", len(items), total, err)
	}
	// 无匹配。
	items, total, err = repo.ListDispatches(ctx, biz.DispatchFilter{EventID: "018f3e1a-0304-7000-8000-0000000000ff"}, 1, 20)
	if err != nil || total != 0 || items != nil {
		t.Fatalf("ListDispatches(no match) = (%v, %d, %v), want nil/0", items, total, err)
	}

	// 运行表不参与配置轮询：cfg_version 无 notify_dispatch 行（且本测试无写路径）。
	var n int
	if err := d.Pool.QueryRow(ctx,
		"SELECT count(*) FROM notify_cfg_version WHERE table_name = 'notify_dispatch'").Scan(&n); err != nil {
		t.Fatalf("count cfg_version rows: %v", err)
	}
	if n != 0 {
		t.Fatalf("notify_cfg_version has %d notify_dispatch rows, want 0（运行表不 bump）", n)
	}
}
