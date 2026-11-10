//go:build integration

package data

import (
	"context"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
)

func TestIntegration_DeliveryLogList(t *testing.T) {
	d := newIntegrationNotifyData(t)
	repo := NewDeliveryLogRepo(d)
	ctx := context.Background()

	// 种子：两任务、两渠道、成功/失败形态各异的 5 行；末行落在 2026-12
	// 分区（跨分区查询——父表查询自动 UNION 命中分区）。
	now := time.Now().UTC().Truncate(time.Microsecond)
	base := now.Add(-time.Hour)
	dispatchID := "018f3e1a-0304-7000-8000-0000000000d1"
	dispatchIDAlt := "018f3e1a-0304-7000-8000-0000000000d2"
	channelID := "018f3e1a-0304-7000-8000-0000000000c1"
	channelIDAlt := "018f3e1a-0304-7000-8000-0000000000c2"
	rows := []struct {
		id, dispatch, channel string
		attempt, latency      int32
		ok                    bool
		providerMsgID, errMsg string
		createTime            time.Time
	}{
		{
			"018f3e1a-0304-7000-8000-0000000000b1", dispatchID, channelID,
			1, 5021, false, "", "smtp: 421 service unavailable", base,
		},
		{
			"018f3e1a-0304-7000-8000-0000000000b2", dispatchID, channelID,
			2, 830, true, "<20261110@a.example.com>", "", base.Add(time.Minute),
		},
		{
			"018f3e1a-0304-7000-8000-0000000000b3", dispatchID, channelID,
			3, 65500, false, "", "dial tcp: i/o timeout", base.Add(2 * time.Minute),
		},
		{
			"018f3e1a-0304-7000-8000-0000000000b4", dispatchIDAlt, channelIDAlt,
			1, 120, true, "ding-msg-9f2c", "", base.Add(3 * time.Minute),
		},
		{
			"018f3e1a-0304-7000-8000-0000000000b5", dispatchIDAlt, channelID,
			1, 210, true, "hook-2026-11-10-01", "", time.Date(2026, 12, 5, 0, 0, 0, 0, time.UTC),
		},
	}
	for _, row := range rows {
		var providerMsgID, errMsg any
		if row.providerMsgID != "" {
			providerMsgID = row.providerMsgID
		}
		if row.errMsg != "" {
			errMsg = row.errMsg
		}
		if _, err := d.Pool.Exec(ctx,
			`INSERT INTO notify_delivery_log (id, dispatch_id, channel_id, attempt, ok,
			     latency_ms, provider_msg_id, error, create_time)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			row.id, row.dispatch, row.channel, row.attempt, row.ok,
			row.latency, providerMsgID, errMsg, row.createTime); err != nil {
			t.Fatalf("seed delivery log %s: %v", row.id, err)
		}
	}

	// 全量：倒序（跨分区的 12 月行最新在前）+ 可空列 NULL 形态。
	items, total, err := repo.ListDeliveryLogs(ctx, biz.DeliveryLogFilter{}, 1, 20)
	if err != nil {
		t.Fatalf("ListDeliveryLogs() error = %v", err)
	}
	if total != 5 || len(items) != 5 {
		t.Fatalf("ListDeliveryLogs() = %d items, total %d, want 5/5", len(items), total)
	}
	if items[0].ID != "018f3e1a-0304-7000-8000-0000000000b5" {
		t.Fatalf("ListDeliveryLogs() first = %s, want cross-partition newest row", items[0].ID)
	}
	// 失败行：error 有值、provider_msg_id 空（NULL 扫描）。
	failed := items[4]
	if failed.OK || failed.Error != "smtp: 421 service unavailable" ||
		failed.ProviderMsgID != "" || failed.Attempt != 1 || failed.LatencyMS != 5021 {
		t.Fatalf("ListDeliveryLogs() failed row = %+v", failed)
	}
	// 成功行：provider_msg_id 有值、error 空（NULL 扫描）。
	okRow := items[3]
	if !okRow.OK || okRow.Error != "" || okRow.ProviderMsgID != "<20261110@a.example.com>" {
		t.Fatalf("ListDeliveryLogs() ok row = %+v", okRow)
	}

	// 任务下钻：第一任务 3 行（退避重试轨迹 1 失败 → 2 成功 → 3 失败）。
	items, total, err = repo.ListDeliveryLogs(ctx, biz.DeliveryLogFilter{DispatchID: dispatchID}, 1, 20)
	if err != nil || total != 3 || len(items) != 3 {
		t.Fatalf("ListDeliveryLogs(dispatch) = (%d, %v), want 3", total, err)
	}
	if items[2].Attempt != 1 || items[1].Attempt != 2 || items[0].Attempt != 3 {
		t.Fatalf("ListDeliveryLogs(dispatch) attempts = %d,%d,%d, want 1,2,3",
			items[2].Attempt, items[1].Attempt, items[0].Attempt)
	}
	// 渠道过滤。
	items, total, err = repo.ListDeliveryLogs(ctx, biz.DeliveryLogFilter{ChannelID: channelIDAlt}, 1, 20)
	if err != nil || total != 1 {
		t.Fatalf("ListDeliveryLogs(channel) = (%d, %v), want 1", total, err)
	}
	// ok=false 过滤：两行失败。
	onlyFailed := false
	items, total, err = repo.ListDeliveryLogs(ctx, biz.DeliveryLogFilter{OK: &onlyFailed}, 1, 20)
	if err != nil || total != 2 {
		t.Fatalf("ListDeliveryLogs(ok=false) = (%d, %v), want 2", total, err)
	}
	// ok=true 过滤。
	onlyOK := true
	items, total, err = repo.ListDeliveryLogs(ctx, biz.DeliveryLogFilter{OK: &onlyOK}, 1, 20)
	if err != nil || total != 3 {
		t.Fatalf("ListDeliveryLogs(ok=true) = (%d, %v), want 3", total, err)
	}
	// 时间区间（含边界）：[base, base+2min] 命中 3 行（base、+1min、+2min）。
	end := base.Add(2 * time.Minute)
	items, total, err = repo.ListDeliveryLogs(ctx, biz.DeliveryLogFilter{CreateTimeStart: &base, CreateTimeEnd: &end}, 1, 20)
	if err != nil || total != 3 {
		t.Fatalf("ListDeliveryLogs(time range) = (%d, %v), want 3（含边界）", total, err)
	}
	// 分页 + 倒序稳定（create_time DESC, id DESC）：5 行 pageSize=3，第 2 页
	// 恰剩最旧两行。
	items, total, err = repo.ListDeliveryLogs(ctx, biz.DeliveryLogFilter{}, 2, 3)
	if err != nil || total != 5 || len(items) != 2 ||
		items[1].ID != "018f3e1a-0304-7000-8000-0000000000b1" {
		t.Fatalf("ListDeliveryLogs(paged) = (%d items, total %d, %v)", len(items), total, err)
	}
	// 无匹配。
	items, total, err = repo.ListDeliveryLogs(ctx, biz.DeliveryLogFilter{DispatchID: "018f3e1a-0304-7000-8000-0000000000ff"}, 1, 20)
	if err != nil || total != 0 || items != nil {
		t.Fatalf("ListDeliveryLogs(no match) = (%v, %d, %v), want nil/0", items, total, err)
	}

	// 运行表不参与配置轮询：cfg_version 无 notify_delivery_log 行（且本测试
	// 无写路径）。
	var n int
	if err := d.Pool.QueryRow(ctx,
		"SELECT count(*) FROM notify_cfg_version WHERE table_name = 'notify_delivery_log'").Scan(&n); err != nil {
		t.Fatalf("count cfg_version rows: %v", err)
	}
	if n != 0 {
		t.Fatalf("notify_cfg_version has %d notify_delivery_log rows, want 0（运行表不 bump）", n)
	}
}
