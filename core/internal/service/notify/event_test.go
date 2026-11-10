package service

import (
	"testing"
	"time"

	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// seedEventRow 预置一行事件运行数据并返回 DO（core 无事件写路径，测试经
// testfake.SeedEvent 模拟管道落库）。
func seedEventRow(t *testing.T, store *testfake.Store, event *biz.Event) *biz.Event {
	t.Helper()
	store.SeedEvent(event)
	return event
}

func TestNotifyEventService_ListEvents(t *testing.T) {
	t.Parallel()
	store := testfake.NewStore()
	svc := NewNotifyEventService(biz.NewEventUsecase(store), testfake.Unrestricted())
	ctx := testCtx()
	base := time.Now().UTC().Add(-time.Hour)

	rows := []*biz.Event{
		{
			ID: "018f3e1a-0304-7000-8000-0000000000e1", BizID: "monitor",
			EventType: "monitor.alert.firing", Severity: 3, Title: "CPU 告警",
			Payload: []byte(`{"host":"web-1","cpu":92.5}`), DedupKey: "alert-web-1",
			GroupKey: "alert.web-1", Recipients: []string{"ops@example.com"},
			Source: biz.EventSourceAPI, Status: biz.EventStatusRouted, PublishState: 1,
			NextPublishTime: base, CreateTime: base,
		},
		{
			ID: "018f3e1a-0304-7000-8000-0000000000e2", BizID: "core",
			EventType: "permission.change", Severity: 2, Title: "角色权限变更",
			Source: biz.EventSourceInternal, Status: biz.EventStatusReceived, PublishState: 0,
			PublishAttempts: 1, NextPublishTime: base.Add(time.Minute), CreateTime: base.Add(time.Minute),
		},
		{
			ID: "018f3e1a-0304-7000-8000-0000000000e3", BizID: "monitor",
			EventType: "monitor.host.down", Severity: 4, Title: "主机失联",
			Source: biz.EventSourceAPI, Status: biz.EventStatusSuppressed, PublishState: 1,
			NextPublishTime: base.Add(2 * time.Minute), CreateTime: base.Add(2 * time.Minute),
		},
	}
	for _, row := range rows {
		seedEventRow(t, store, row)
	}

	// 全量：倒序（最新在前）+ 排障字段完整 + payload Struct / recipients 往返。
	list, err := svc.ListEvents(ctx, &notifyv1.ListEventsRequest{Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("ListEvents() error = %v", err)
	}
	if list.GetTotal() != 3 || len(list.GetItems()) != 3 {
		t.Fatalf("ListEvents() = %d items, total %d, want 3/3", len(list.GetItems()), list.GetTotal())
	}
	first := list.GetItems()[0]
	if first.GetId() != rows[2].ID || first.GetStatus() != biz.EventStatusSuppressed {
		t.Fatalf("ListEvents() first = %+v, want newest suppressed row", first)
	}
	alert := list.GetItems()[2]
	if alert.GetSource() != biz.EventSourceAPI || alert.GetPublishState() != 1 ||
		alert.GetDedupKey() != "alert-web-1" || alert.GetGroupKey() != "alert.web-1" {
		t.Fatalf("ListEvents() alert row = %+v", alert)
	}
	if len(alert.GetRecipients()) != 1 || alert.GetRecipients()[0] != "ops@example.com" {
		t.Fatalf("ListEvents() recipients = %v", alert.GetRecipients())
	}
	payload := alert.GetPayload()
	if payload == nil || payload.GetFields()["host"].GetStringValue() != "web-1" ||
		payload.GetFields()["cpu"].GetNumberValue() != 92.5 {
		t.Fatalf("ListEvents() payload = %v", payload)
	}
	// 无载荷行：payload nil（区别于空对象）。
	if list.GetItems()[1].GetPayload() != nil {
		t.Fatalf("ListEvents() payload of row without payload = %v, want nil", list.GetItems()[1].GetPayload())
	}

	// 四维过滤：biz + event_type 精确 + status + 时间区间。
	resp, err := svc.ListEvents(ctx, &notifyv1.ListEventsRequest{
		Page: 1, PageSize: 10, BizId: sptr("monitor"),
	})
	if err != nil || resp.GetTotal() != 2 {
		t.Fatalf("ListEvents(biz) = (%d, %v), want 2", resp.GetTotal(), err)
	}
	suppressed := int32(biz.EventStatusSuppressed)
	resp, err = svc.ListEvents(ctx, &notifyv1.ListEventsRequest{
		Page: 1, PageSize: 10, BizId: sptr("monitor"), Status: &suppressed,
	})
	if err != nil || resp.GetTotal() != 1 || resp.GetItems()[0].GetEventType() != "monitor.host.down" {
		t.Fatalf("ListEvents(biz+status) = (%+v, %v)", resp.GetItems(), err)
	}
	resp, err = svc.ListEvents(ctx, &notifyv1.ListEventsRequest{
		Page: 1, PageSize: 10, EventType: sptr("permission.change"),
	})
	if err != nil || resp.GetTotal() != 1 || resp.GetItems()[0].GetSource() != biz.EventSourceInternal {
		t.Fatalf("ListEvents(eventType) = (%+v, %v)", resp.GetItems(), err)
	}
	resp, err = svc.ListEvents(ctx, &notifyv1.ListEventsRequest{
		Page: 1, PageSize: 10,
		CreateTimeStart: timestamppb.New(base.Add(30 * time.Second)),
		CreateTimeEnd:   timestamppb.New(base.Add(90 * time.Second)),
	})
	if err != nil || resp.GetTotal() != 1 || resp.GetItems()[0].GetId() != rows[1].ID {
		t.Fatalf("ListEvents(time range) = (%+v, %v)", resp.GetItems(), err)
	}

	// 分页。
	resp, err = svc.ListEvents(ctx, &notifyv1.ListEventsRequest{Page: 2, PageSize: 2})
	if err != nil || resp.GetTotal() != 3 || len(resp.GetItems()) != 1 {
		t.Fatalf("ListEvents(paged) = (%d items, total %d, %v), want 1/3", len(resp.GetItems()), resp.GetTotal(), err)
	}
}

// TestNotifyEventService_NonObjectPayload 运行表只保证 payload 是合法 JSON，
// 非对象形态（数组/标量/null）不得让整页 500：包成 {"value": ...} 渲染。
func TestNotifyEventService_NonObjectPayload(t *testing.T) {
	t.Parallel()
	store := testfake.NewStore()
	svc := NewNotifyEventService(biz.NewEventUsecase(store), testfake.Unrestricted())
	base := time.Now().UTC().Add(-time.Hour)
	for i, raw := range []string{`[1,2]`, `"text"`, `null`, `{"ok":true}`} {
		seedEventRow(t, store, &biz.Event{
			ID: "018f3e1a-0304-7000-8000-00000000f0a" + string(rune('0'+i)), BizID: "monitor",
			EventType: "monitor.alert.firing", Severity: 2, Title: "t", Payload: []byte(raw),
			Source: biz.EventSourceAPI, Status: biz.EventStatusReceived,
			NextPublishTime: base.Add(time.Duration(i) * time.Minute), CreateTime: base.Add(time.Duration(i) * time.Minute),
		})
	}
	list, err := svc.ListEvents(testCtx(), &notifyv1.ListEventsRequest{Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("ListEvents() error = %v", err)
	}
	if len(list.GetItems()) != 4 {
		t.Fatalf("ListEvents() items = %d, want 4", len(list.GetItems()))
	}
	// 倒序：最新（对象）在前，其后为 null / 标量 / 数组。
	if list.GetItems()[0].GetPayload().GetFields()["ok"] == nil {
		t.Fatalf("object payload = %v, want ok field", list.GetItems()[0].GetPayload())
	}
	for _, item := range list.GetItems()[1:] {
		if item.GetPayload() == nil || item.GetPayload().GetFields()["value"] == nil {
			t.Errorf("non-object payload of %s = %v, want wrapped {\"value\": ...}", item.GetId(), item.GetPayload())
		}
	}
}
