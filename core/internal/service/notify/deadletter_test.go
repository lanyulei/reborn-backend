package service

import (
	"errors"
	"testing"
	"time"

	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"

	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNotifyDeadLetterService_ListAndReplay(t *testing.T) {
	t.Parallel()
	store := testfake.NewStore()
	svc := NewNotifyDeadLetterService(biz.NewDeadLetterUsecase(store), biz.NewChannelUsecase(store, store, store), testfake.Unrestricted())
	ctx := testCtx()
	base := time.Now().UTC().Add(-time.Hour)

	eventID := "018f3e1a-0304-7000-8000-0000000000e1"
	eventIDAlt := "018f3e1a-0304-7000-8000-0000000000e2"
	channelID := "018f3e1a-0304-7000-8000-0000000000c1"
	channelIDAlt := "018f3e1a-0304-7000-8000-0000000000c2"
	rows := []*biz.DeadLetter{
		{
			ID:         "018f3e1a-0304-7000-8000-0000000000q1",
			DispatchID: "018f3e1a-0304-7000-8000-0000000000d1", EventID: eventID,
			ChannelID: channelID, Recipient: "ops@example.com",
			LastError: "smtp: 421 service unavailable", Attempts: 6,
			ReplayState: biz.ReplayStateNone,
			CreateTime:  base, UpdateTime: base,
		},
		{
			ID:         "018f3e1a-0304-7000-8000-0000000000q2",
			DispatchID: "018f3e1a-0304-7000-8000-0000000000d2", EventID: eventID,
			ChannelID: channelIDAlt, Recipient: "https://hook.example.com/ops",
			LastError: "dial tcp: i/o timeout", Attempts: 6,
			ReplayState: biz.ReplayStatePending,
			CreateTime:  base.Add(time.Minute), UpdateTime: base.Add(5 * time.Minute),
		},
		{
			ID:         "018f3e1a-0304-7000-8000-0000000000q3",
			DispatchID: "018f3e1a-0304-7000-8000-0000000000d3", EventID: eventIDAlt,
			ChannelID: channelID, Recipient: "ops-group",
			Attempts: 6, ReplayState: biz.ReplayStateReplayed,
			CreateTime: base.Add(2 * time.Minute), UpdateTime: base.Add(10 * time.Minute),
		},
	}
	for _, row := range rows {
		store.SeedDeadLetter(row)
	}

	// 全量：倒序（最新在前）+ 排障字段 + 状态三态。
	list, err := svc.ListDeadLetters(ctx, &notifyv1.ListDeadLettersRequest{Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("ListDeadLetters() error = %v", err)
	}
	if list.GetTotal() != 3 || len(list.GetItems()) != 3 {
		t.Fatalf("ListDeadLetters() = %d items, total %d, want 3/3", len(list.GetItems()), list.GetTotal())
	}
	first := list.GetItems()[0]
	if first.GetId() != rows[2].ID || first.GetReplayState() != biz.ReplayStateReplayed ||
		first.GetLastError() != "" || first.GetAttempts() != 6 {
		t.Fatalf("ListDeadLetters() first = %+v, want newest replayed row", first)
	}
	if list.GetItems()[1].GetReplayState() != biz.ReplayStatePending ||
		list.GetItems()[1].GetUpdateTime().AsTime() != base.Add(5*time.Minute) {
		t.Fatalf("ListDeadLetters() pending row = %+v", list.GetItems()[1])
	}

	// 四维过滤：事件下钻 + 渠道 + replay_state + 时间区间。
	resp, err := svc.ListDeadLetters(ctx, &notifyv1.ListDeadLettersRequest{
		Page: 1, PageSize: 10, EventId: sptr(eventID),
	})
	if err != nil || resp.GetTotal() != 2 {
		t.Fatalf("ListDeadLetters(event) = (%d, %v), want 2", resp.GetTotal(), err)
	}
	resp, err = svc.ListDeadLetters(ctx, &notifyv1.ListDeadLettersRequest{
		Page: 1, PageSize: 10, ChannelId: sptr(channelIDAlt),
	})
	if err != nil || resp.GetTotal() != 1 || resp.GetItems()[0].GetId() != rows[1].ID {
		t.Fatalf("ListDeadLetters(channel) = (%+v, %v)", resp.GetItems(), err)
	}
	none := int32(biz.ReplayStateNone)
	resp, err = svc.ListDeadLetters(ctx, &notifyv1.ListDeadLettersRequest{
		Page: 1, PageSize: 10, ReplayState: &none,
	})
	if err != nil || resp.GetTotal() != 1 || resp.GetItems()[0].GetLastError() != "smtp: 421 service unavailable" {
		t.Fatalf("ListDeadLetters(none) = (%+v, %v)", resp.GetItems(), err)
	}
	resp, err = svc.ListDeadLetters(ctx, &notifyv1.ListDeadLettersRequest{
		Page: 1, PageSize: 10,
		CreateTimeStart: timestamppb.New(base.Add(30 * time.Second)),
		CreateTimeEnd:   timestamppb.New(base.Add(90 * time.Second)),
	})
	if err != nil || resp.GetTotal() != 1 || resp.GetItems()[0].GetId() != rows[1].ID {
		t.Fatalf("ListDeadLetters(time range) = (%+v, %v)", resp.GetItems(), err)
	}

	// 分页。
	resp, err = svc.ListDeadLetters(ctx, &notifyv1.ListDeadLettersRequest{Page: 2, PageSize: 2})
	if err != nil || resp.GetTotal() != 3 || len(resp.GetItems()) != 1 {
		t.Fatalf("ListDeadLetters(paged) = (%d items, total %d, %v), want 1/3", len(resp.GetItems()), resp.GetTotal(), err)
	}

	// 标记重放：none → pending 成功且返回权威行。
	replayed, err := svc.ReplayDeadLetter(ctx, &notifyv1.ReplayDeadLetterRequest{Id: rows[0].ID})
	if err != nil {
		t.Fatalf("ReplayDeadLetter() error = %v", err)
	}
	if got := replayed.GetDeadLetter(); got.GetReplayState() != biz.ReplayStatePending ||
		got.GetId() != rows[0].ID || got.GetLastError() != "smtp: 421 service unavailable" {
		t.Fatalf("ReplayDeadLetter() = %+v, want pending authoritative row", got)
	}
	if !replayed.GetDeadLetter().GetUpdateTime().AsTime().After(rows[0].UpdateTime) {
		t.Fatalf("ReplayDeadLetter() update_time not advanced")
	}
	// 重复标记 → 409 INVALID_STATE；pending/replayed 行同理。
	for _, id := range []string{rows[0].ID, rows[1].ID, rows[2].ID} {
		if _, err = svc.ReplayDeadLetter(ctx, &notifyv1.ReplayDeadLetterRequest{Id: id}); !errors.Is(err, biz.ErrDeadLetterInvalidState) {
			t.Fatalf("ReplayDeadLetter(%s) error = %v, want ErrDeadLetterInvalidState", id, err)
		}
	}
	// 不存在 → 404。
	if _, err = svc.ReplayDeadLetter(ctx, &notifyv1.ReplayDeadLetterRequest{
		Id: "018f3e1a-0304-7000-8000-0000000000ff",
	}); !errors.Is(err, biz.ErrDeadLetterNotFound) {
		t.Fatalf("ReplayDeadLetter(missing) error = %v, want ErrDeadLetterNotFound", err)
	}
}
