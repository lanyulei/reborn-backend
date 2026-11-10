package service

import (
	"testing"
	"time"

	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// 常量种子标识（事件/渠道/模板/任务，模拟管道 router 已生成的任务）。
const (
	dispatchEventID    = "018f3e1a-0304-7000-8000-0000000000e1"
	dispatchEventIDAlt = "018f3e1a-0304-7000-8000-0000000000e2"
	dispatchChannelID  = "018f3e1a-0304-7000-8000-0000000000c1"
	dispatchChannelAlt = "018f3e1a-0304-7000-8000-0000000000c2"
	dispatchTemplateID = "018f3e1a-0304-7000-8000-0000000000a1"
)

func TestNotifyDispatchService_ListDispatches(t *testing.T) {
	t.Parallel()
	store := testfake.NewStore()
	svc := NewNotifyDispatchService(biz.NewDispatchUsecase(store), biz.NewChannelUsecase(store, store, store), testfake.Unrestricted())
	ctx := testCtx()
	base := time.Now().UTC().Add(-time.Hour)

	retryAt := base.Add(5 * time.Minute)
	rows := []*biz.Dispatch{
		{
			ID: "018f3e1a-0304-7000-8000-0000000000d1", EventID: dispatchEventID,
			ChannelID: dispatchChannelID, Recipient: "ops@example.com",
			TemplateID: dispatchTemplateID, Content: []byte(`{"subject":"CPU 告警","body":"web-1 cpu 92.5%"}`),
			Status: biz.DispatchStatusFailed, Attempts: 2, NextRetry: &retryAt,
			LastError: "smtp: 421 service unavailable", SendKey: "9f86d081884c7d659a2f",
			CreateTime: base,
		},
		{
			ID: "018f3e1a-0304-7000-8000-0000000000d2", EventID: dispatchEventID,
			ChannelID: dispatchChannelAlt, Recipient: "https://hook.example.com/ops",
			Status: biz.DispatchStatusSent, Attempts: 1, SendKey: "2c26b46b68ffc68ff9",
			CreateTime: base.Add(time.Minute),
		},
		{
			ID: "018f3e1a-0304-7000-8000-0000000000d3", EventID: dispatchEventIDAlt,
			ChannelID: dispatchChannelID, Recipient: "ops-group",
			Status: biz.DispatchStatusDead, Attempts: 6, SendKey: "5891b5b522d5b5b5b5",
			LastError: "dingtalk: token invalid", CreateTime: base.Add(2 * time.Minute),
		},
	}
	for _, row := range rows {
		store.SeedDispatch(row)
	}

	// 全量：倒序（最新在前）+ 排障字段完整 + content Struct 往返。
	list, err := svc.ListDispatches(ctx, &notifyv1.ListDispatchesRequest{Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("ListDispatches() error = %v", err)
	}
	if list.GetTotal() != 3 || len(list.GetItems()) != 3 {
		t.Fatalf("ListDispatches() = %d items, total %d, want 3/3", len(list.GetItems()), list.GetTotal())
	}
	first := list.GetItems()[0]
	if first.GetId() != rows[2].ID || first.GetStatus() != biz.DispatchStatusDead {
		t.Fatalf("ListDispatches() first = %+v, want newest dead row", first)
	}
	failed := list.GetItems()[2]
	if failed.GetStatus() != biz.DispatchStatusFailed || failed.GetAttempts() != 2 ||
		failed.GetLastError() != "smtp: 421 service unavailable" ||
		failed.GetSendKey() != "9f86d081884c7d659a2f" || failed.GetTemplateId() != dispatchTemplateID {
		t.Fatalf("ListDispatches() failed row = %+v", failed)
	}
	if failed.GetNextRetryTime().AsTime() != retryAt {
		t.Fatalf("ListDispatches() nextRetryTime = %v, want %v", failed.GetNextRetryTime().AsTime(), retryAt)
	}
	content := failed.GetContent()
	if content == nil || content.GetFields()["subject"].GetStringValue() != "CPU 告警" {
		t.Fatalf("ListDispatches() content = %v", content)
	}
	// 无快照行：content nil（区别于空对象）；无待重试行 nextRetryTime nil。
	second := list.GetItems()[1]
	if second.GetContent() != nil || second.GetNextRetryTime() != nil || second.GetLastError() != "" {
		t.Fatalf("ListDispatches() sent row = %+v, want nil/nil/empty nullable fields", second)
	}

	// 四维过滤：事件下钻 + 渠道 + status + 时间区间。
	resp, err := svc.ListDispatches(ctx, &notifyv1.ListDispatchesRequest{
		Page: 1, PageSize: 10, EventId: sptr(dispatchEventID),
	})
	if err != nil || resp.GetTotal() != 2 {
		t.Fatalf("ListDispatches(event) = (%d, %v), want 2", resp.GetTotal(), err)
	}
	dead := int32(biz.DispatchStatusDead)
	resp, err = svc.ListDispatches(ctx, &notifyv1.ListDispatchesRequest{
		Page: 1, PageSize: 10, Status: &dead,
	})
	if err != nil || resp.GetTotal() != 1 || resp.GetItems()[0].GetRecipient() != "ops-group" {
		t.Fatalf("ListDispatches(status) = (%+v, %v)", resp.GetItems(), err)
	}
	resp, err = svc.ListDispatches(ctx, &notifyv1.ListDispatchesRequest{
		Page: 1, PageSize: 10, ChannelId: sptr(dispatchChannelAlt),
	})
	if err != nil || resp.GetTotal() != 1 || resp.GetItems()[0].GetId() != rows[1].ID {
		t.Fatalf("ListDispatches(channel) = (%+v, %v)", resp.GetItems(), err)
	}
	resp, err = svc.ListDispatches(ctx, &notifyv1.ListDispatchesRequest{
		Page: 1, PageSize: 10,
		CreateTimeStart: timestamppb.New(base.Add(30 * time.Second)),
		CreateTimeEnd:   timestamppb.New(base.Add(90 * time.Second)),
	})
	if err != nil || resp.GetTotal() != 1 || resp.GetItems()[0].GetId() != rows[1].ID {
		t.Fatalf("ListDispatches(time range) = (%+v, %v)", resp.GetItems(), err)
	}

	// 分页。
	resp, err = svc.ListDispatches(ctx, &notifyv1.ListDispatchesRequest{Page: 2, PageSize: 2})
	if err != nil || resp.GetTotal() != 3 || len(resp.GetItems()) != 1 {
		t.Fatalf("ListDispatches(paged) = (%d items, total %d, %v), want 1/3", len(resp.GetItems()), resp.GetTotal(), err)
	}
}
