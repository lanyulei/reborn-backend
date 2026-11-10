//go:build integration

package data

import (
	"context"
	"errors"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
)

func TestIntegration_DeadLetterListAndReplay(t *testing.T) {
	d := newIntegrationNotifyData(t)
	repo := NewDeadLetterRepo(d)
	ctx := context.Background()

	// 种子：两事件、两渠道、三种重放状态的 4 行（模拟 sender 在 dispatch
	// 重试超限后写入；行 id = 转死信的 dispatch id，全 hex）。
	now := time.Now().UTC().Truncate(time.Microsecond)
	base := now.Add(-time.Hour)
	eventID := "018f3e1a-0304-7000-8000-0000000000e1"
	eventIDAlt := "018f3e1a-0304-7000-8000-0000000000e2"
	channelID := "018f3e1a-0304-7000-8000-0000000000c1"
	channelIDAlt := "018f3e1a-0304-7000-8000-0000000000c2"
	rows := []struct {
		id, dispatch, event, channel, recipient, lastError string
		replayState                                        int32
		createTime                                         time.Time
	}{
		{
			"018f3e1a-0304-7000-8000-0000000000a1",
			"018f3e1a-0304-7000-8000-0000000000d1", eventID, channelID,
			"ops@example.com", "smtp: 421 service unavailable", 1, base,
		},
		{
			"018f3e1a-0304-7000-8000-0000000000a2",
			"018f3e1a-0304-7000-8000-0000000000d2", eventID, channelIDAlt,
			"https://hook.example.com/ops", "dial tcp: i/o timeout", 2, base.Add(time.Minute),
		},
		{
			"018f3e1a-0304-7000-8000-0000000000a3",
			"018f3e1a-0304-7000-8000-0000000000d3", eventIDAlt, channelID,
			"ops-group", "", 3, base.Add(2 * time.Minute),
		},
		{
			"018f3e1a-0304-7000-8000-0000000000a4",
			"018f3e1a-0304-7000-8000-0000000000d4", eventIDAlt, channelID,
			"dev@example.com", "smtp: 550 mailbox unavailable", 1, base.Add(3 * time.Minute),
		},
	}
	for _, row := range rows {
		var lastError any
		if row.lastError != "" {
			lastError = row.lastError
		}
		if _, err := d.Pool.Exec(ctx,
			`INSERT INTO notify_dlq (id, dispatch_id, event_id, channel_id, recipient,
			     last_error, attempts, replay_state, create_time, update_time)
			 VALUES ($1, $2, $3, $4, $5, $6, 6, $7, $8, $8)`,
			row.id, row.dispatch, row.event, row.channel, row.recipient,
			lastError, row.replayState, row.createTime); err != nil {
			t.Fatalf("seed dead letter %s: %v", row.id, err)
		}
	}

	// 全量：倒序 + 可空 last_error NULL 扫描。
	items, total, err := repo.ListDeadLetters(ctx, biz.DeadLetterFilter{}, 1, 20)
	if err != nil {
		t.Fatalf("ListDeadLetters() error = %v", err)
	}
	if total != 4 || len(items) != 4 {
		t.Fatalf("ListDeadLetters() = %d items, total %d, want 4/4", len(items), total)
	}
	if items[0].ID != "018f3e1a-0304-7000-8000-0000000000a4" {
		t.Fatalf("ListDeadLetters() first = %s, want newest row", items[0].ID)
	}
	if items[1].LastError != "" || items[1].Attempts != 6 || items[1].ReplayState != biz.ReplayStateReplayed {
		t.Fatalf("ListDeadLetters() replayed row = %+v, want empty last_error", items[1])
	}

	// 四维过滤：事件下钻 + 渠道 + replay_state + 时间区间（含边界）。
	items, total, err = repo.ListDeadLetters(ctx, biz.DeadLetterFilter{EventID: eventID}, 1, 20)
	if err != nil || total != 2 {
		t.Fatalf("ListDeadLetters(event) = (%d, %v), want 2", total, err)
	}
	items, total, err = repo.ListDeadLetters(ctx, biz.DeadLetterFilter{ChannelID: channelIDAlt}, 1, 20)
	if err != nil || total != 1 {
		t.Fatalf("ListDeadLetters(channel) = (%d, %v), want 1", total, err)
	}
	none := biz.ReplayStateNone
	items, total, err = repo.ListDeadLetters(ctx, biz.DeadLetterFilter{ReplayState: &none}, 1, 20)
	if err != nil || total != 2 {
		t.Fatalf("ListDeadLetters(none) = (%d, %v), want 2", total, err)
	}
	end := base.Add(2 * time.Minute)
	items, total, err = repo.ListDeadLetters(ctx, biz.DeadLetterFilter{CreateTimeStart: &base, CreateTimeEnd: &end}, 1, 20)
	if err != nil || total != 3 {
		t.Fatalf("ListDeadLetters(time range) = (%d, %v), want 3（含边界）", total, err)
	}
	// 分页：4 行 pageSize=3 第 2 页恰剩最旧一行。
	items, total, err = repo.ListDeadLetters(ctx, biz.DeadLetterFilter{}, 2, 3)
	if err != nil || total != 4 || len(items) != 1 || items[0].ID != "018f3e1a-0304-7000-8000-0000000000a1" {
		t.Fatalf("ListDeadLetters(paged) = (%d items, total %d, %v)", len(items), total, err)
	}
	// 无匹配。
	items, total, err = repo.ListDeadLetters(ctx, biz.DeadLetterFilter{EventID: "018f3e1a-0304-7000-8000-0000000000ef"}, 1, 20)
	if err != nil || total != 0 || items != nil {
		t.Fatalf("ListDeadLetters(no match) = (%v, %d, %v), want nil/0", items, total, err)
	}

	// 标记重放：none → pending，返回权威行且 update_time 推进。
	target := "018f3e1a-0304-7000-8000-0000000000a1"
	letter, err := repo.MarkReplayPending(ctx, target)
	if err != nil {
		t.Fatalf("MarkReplayPending() error = %v", err)
	}
	if letter.ReplayState != biz.ReplayStatePending || letter.LastError != "smtp: 421 service unavailable" {
		t.Fatalf("MarkReplayPending() = %+v, want pending authoritative row", letter)
	}
	if !letter.UpdateTime.After(letter.CreateTime) {
		t.Fatalf("MarkReplayPending() update_time = %v not advanced (create %v)",
			letter.UpdateTime, letter.CreateTime)
	}
	// 状态机：重复标记 / pending 行 / replayed 行均拒绝。
	if _, err = repo.MarkReplayPending(ctx, target); !errors.Is(err, biz.ErrDeadLetterInvalidState) {
		t.Fatalf("MarkReplayPending(twice) error = %v, want ErrDeadLetterInvalidState", err)
	}
	if _, err = repo.MarkReplayPending(ctx, "018f3e1a-0304-7000-8000-0000000000a2"); !errors.Is(err, biz.ErrDeadLetterInvalidState) {
		t.Fatalf("MarkReplayPending(pending) error = %v, want ErrDeadLetterInvalidState", err)
	}
	if _, err = repo.MarkReplayPending(ctx, "018f3e1a-0304-7000-8000-0000000000a3"); !errors.Is(err, biz.ErrDeadLetterInvalidState) {
		t.Fatalf("MarkReplayPending(replayed) error = %v, want ErrDeadLetterInvalidState", err)
	}
	// 不存在 → 404。
	if _, err = repo.MarkReplayPending(ctx, "018f3e1a-0304-7000-8000-0000000000af"); !errors.Is(err, biz.ErrDeadLetterNotFound) {
		t.Fatalf("MarkReplayPending(missing) error = %v, want ErrDeadLetterNotFound", err)
	}

	// 运行表不参与配置轮询：cfg_version 无 notify_dlq 行（标记重放不 bump）。
	var n int
	if err := d.Pool.QueryRow(ctx,
		"SELECT count(*) FROM notify_cfg_version WHERE table_name = 'notify_dlq'").Scan(&n); err != nil {
		t.Fatalf("count cfg_version rows: %v", err)
	}
	if n != 0 {
		t.Fatalf("notify_cfg_version has %d notify_dlq rows, want 0（运行表不 bump）", n)
	}
}
