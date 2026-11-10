package service

import (
	"testing"
	"time"

	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// bptr 返回布尔字面量的指针（optional bool 过滤字段的测试便捷构造）。
func bptr(b bool) *bool { return &b }

func TestNotifyDeliveryLogService_ListDeliveryLogs(t *testing.T) {
	t.Parallel()
	store := testfake.NewStore()
	svc := NewNotifyDeliveryLogService(biz.NewDeliveryLogUsecase(store), biz.NewChannelUsecase(store, store, store), testfake.Unrestricted())
	ctx := testCtx()
	base := time.Now().UTC().Add(-time.Hour)

	dispatchID := "018f3e1a-0304-7000-8000-0000000000d1"
	dispatchIDAlt := "018f3e1a-0304-7000-8000-0000000000d2"
	channelID := "018f3e1a-0304-7000-8000-0000000000c1"
	channelIDAlt := "018f3e1a-0304-7000-8000-0000000000c2"
	rows := []*biz.DeliveryLog{
		{
			ID: "018f3e1a-0304-7000-8000-0000000000l1", DispatchID: dispatchID,
			ChannelID: channelID, Attempt: 1, OK: false, LatencyMS: 5021,
			Error: "smtp: 421 service unavailable", CreateTime: base,
		},
		{
			ID: "018f3e1a-0304-7000-8000-0000000000l2", DispatchID: dispatchID,
			ChannelID: channelID, Attempt: 2, OK: true, LatencyMS: 830,
			ProviderMsgID: "<20261110@a.example.com>", CreateTime: base.Add(time.Minute),
		},
		{
			ID: "018f3e1a-0304-7000-8000-0000000000l3", DispatchID: dispatchIDAlt,
			ChannelID: channelIDAlt, Attempt: 1, OK: true, LatencyMS: 120,
			ProviderMsgID: "ding-msg-9f2c", CreateTime: base.Add(2 * time.Minute),
		},
	}
	for _, row := range rows {
		store.SeedDeliveryLog(row)
	}

	// 全量：倒序（最新在前）+ 全量排障字段 + 可空列形态。
	list, err := svc.ListDeliveryLogs(ctx, &notifyv1.ListDeliveryLogsRequest{Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("ListDeliveryLogs() error = %v", err)
	}
	if list.GetTotal() != 3 || len(list.GetItems()) != 3 {
		t.Fatalf("ListDeliveryLogs() = %d items, total %d, want 3/3", len(list.GetItems()), list.GetTotal())
	}
	first := list.GetItems()[0]
	if first.GetId() != rows[2].ID || !first.GetOk() || first.GetAttempt() != 1 ||
		first.GetLatencyMs() != 120 || first.GetProviderMsgId() != "ding-msg-9f2c" {
		t.Fatalf("ListDeliveryLogs() first = %+v, want newest ok row", first)
	}
	// 失败行：error 有值、provider_msg_id 空。
	failed := list.GetItems()[2]
	if failed.GetOk() || failed.GetError() != "smtp: 421 service unavailable" ||
		failed.GetProviderMsgId() != "" || failed.GetAttempt() != 1 {
		t.Fatalf("ListDeliveryLogs() failed row = %+v", failed)
	}
	// 成功行：provider_msg_id 有值、error 空。
	okRow := list.GetItems()[1]
	if !okRow.GetOk() || okRow.GetError() != "" || okRow.GetProviderMsgId() != "<20261110@a.example.com>" {
		t.Fatalf("ListDeliveryLogs() ok row = %+v", okRow)
	}

	// 四维过滤：任务下钻 + 渠道 + ok + 时间区间。
	resp, err := svc.ListDeliveryLogs(ctx, &notifyv1.ListDeliveryLogsRequest{
		Page: 1, PageSize: 10, DispatchId: sptr(dispatchID),
	})
	if err != nil || resp.GetTotal() != 2 {
		t.Fatalf("ListDeliveryLogs(dispatch) = (%d, %v), want 2", resp.GetTotal(), err)
	}
	resp, err = svc.ListDeliveryLogs(ctx, &notifyv1.ListDeliveryLogsRequest{
		Page: 1, PageSize: 10, ChannelId: sptr(channelIDAlt),
	})
	if err != nil || resp.GetTotal() != 1 || resp.GetItems()[0].GetId() != rows[2].ID {
		t.Fatalf("ListDeliveryLogs(channel) = (%+v, %v)", resp.GetItems(), err)
	}
	resp, err = svc.ListDeliveryLogs(ctx, &notifyv1.ListDeliveryLogsRequest{
		Page: 1, PageSize: 10, Ok: bptr(false),
	})
	if err != nil || resp.GetTotal() != 1 || resp.GetItems()[0].GetError() != "smtp: 421 service unavailable" {
		t.Fatalf("ListDeliveryLogs(ok=false) = (%+v, %v)", resp.GetItems(), err)
	}
	resp, err = svc.ListDeliveryLogs(ctx, &notifyv1.ListDeliveryLogsRequest{
		Page: 1, PageSize: 10,
		CreateTimeStart: timestamppb.New(base.Add(30 * time.Second)),
		CreateTimeEnd:   timestamppb.New(base.Add(90 * time.Second)),
	})
	if err != nil || resp.GetTotal() != 1 || resp.GetItems()[0].GetId() != rows[1].ID {
		t.Fatalf("ListDeliveryLogs(time range) = (%+v, %v)", resp.GetItems(), err)
	}

	// 分页。
	resp, err = svc.ListDeliveryLogs(ctx, &notifyv1.ListDeliveryLogsRequest{Page: 2, PageSize: 2})
	if err != nil || resp.GetTotal() != 3 || len(resp.GetItems()) != 1 {
		t.Fatalf("ListDeliveryLogs(paged) = (%d items, total %d, %v), want 1/3", len(resp.GetItems()), resp.GetTotal(), err)
	}
}
