package biz

import (
	"context"
	"errors"
	"testing"
	"time"
)

// deadLetterRepoStub 死信仓储内存桩：列表四维过滤 + 分页透传 + none→pending
// 状态机（对齐 data 层条件 UPDATE 语义）。
type deadLetterRepoStub struct {
	letters []*DeadLetter
	nowFunc func() time.Time
}

func (s *deadLetterRepoStub) ListDeadLetters(_ context.Context, filter DeadLetterFilter, page, pageSize int64) ([]*DeadLetter, int64, error) {
	var matched []*DeadLetter
	for _, letter := range s.letters {
		if filter.EventID != "" && letter.EventID != filter.EventID {
			continue
		}
		if filter.ChannelID != "" && letter.ChannelID != filter.ChannelID {
			continue
		}
		if filter.ReplayState != nil && letter.ReplayState != *filter.ReplayState {
			continue
		}
		if filter.CreateTimeStart != nil && letter.CreateTime.Before(*filter.CreateTimeStart) {
			continue
		}
		if filter.CreateTimeEnd != nil && letter.CreateTime.After(*filter.CreateTimeEnd) {
			continue
		}
		matched = append(matched, letter)
	}
	total := int64(len(matched))
	start := (page - 1) * pageSize
	if start >= total {
		return nil, total, nil
	}
	end := min(start+pageSize, total)
	return matched[start:end], total, nil
}

func (s *deadLetterRepoStub) GetDeadLetter(_ context.Context, id string) (*DeadLetter, error) {
	for _, letter := range s.letters {
		if letter.ID == id {
			return letter, nil
		}
	}
	return nil, ErrDeadLetterNotFound
}

func (s *deadLetterRepoStub) MarkReplayPending(_ context.Context, id string) (*DeadLetter, error) {
	for _, letter := range s.letters {
		if letter.ID != id {
			continue
		}
		if letter.ReplayState != ReplayStateNone {
			return nil, ErrDeadLetterInvalidState
		}
		letter.ReplayState = ReplayStatePending
		letter.UpdateTime = s.nowFunc()
		return letter, nil
	}
	return nil, ErrDeadLetterNotFound
}

func TestDeadLetterUsecase_ListPagingNormalize(t *testing.T) {
	t.Parallel()
	stub := &deadLetterRepoStub{}
	base := time.Now().UTC().Add(-time.Hour)
	eventID := "018f3e1a-0304-7000-8000-0000000000e1"
	for i := range 5 {
		stub.letters = append(stub.letters, &DeadLetter{
			ID:          string(rune('a' + i)),
			EventID:     eventID,
			ChannelID:   "018f3e1a-0304-7000-8000-0000000000c1",
			Recipient:   "ops@example.com",
			Attempts:    6,
			ReplayState: ReplayStateNone,
			CreateTime:  base.Add(time.Duration(i) * time.Minute),
			UpdateTime:  base.Add(time.Duration(i) * time.Minute),
		})
	}
	uc := NewDeadLetterUsecase(stub)
	ctx := context.Background()

	// 非法分页参数规整为默认值。
	items, total, err := uc.List(ctx, DeadLetterFilter{}, 0, 0)
	if err != nil {
		t.Fatalf("List(zero paging) error = %v", err)
	}
	if total != 5 || len(items) != 5 {
		t.Fatalf("List(zero paging) = %d items, total %d, want 5/5", len(items), total)
	}

	// 四维过滤：事件下钻 + 渠道 + replay_state + 时间区间（含边界）。
	pending := ReplayStatePending
	from := base.Add(2 * time.Minute)
	to := base.Add(3 * time.Minute)
	_, total, err = uc.List(ctx, DeadLetterFilter{
		EventID:         eventID,
		ChannelID:       "018f3e1a-0304-7000-8000-0000000000c1",
		ReplayState:     &pending,
		CreateTimeStart: &from,
		CreateTimeEnd:   &to,
	}, 1, 20)
	if err != nil || total != 0 {
		t.Fatalf("List(filters) = (%d, %v), want 0（种子均 none）", total, err)
	}
	// 控制台标记重放后命中同一过滤。
	stub.letters[2].ReplayState = ReplayStatePending
	items, total, err = uc.List(ctx, DeadLetterFilter{
		EventID:         eventID,
		ReplayState:     &pending,
		CreateTimeStart: &from,
		CreateTimeEnd:   &to,
	}, 1, 20)
	if err != nil || total != 1 || items[0].ID != "c" {
		t.Fatalf("List(filters hit) = (%+v, %d, %v), want only row c", items, total, err)
	}
	// 事件下钻不匹配。
	items, total, err = uc.List(ctx, DeadLetterFilter{
		EventID: "018f3e1a-0304-7000-8000-0000000000ff",
	}, 1, 20)
	if err != nil || total != 0 || items != nil {
		t.Fatalf("List(other event) = (%v, %d, %v), want nil/0", items, total, err)
	}
}

func TestDeadLetterUsecase_ReplayStateMachine(t *testing.T) {
	t.Parallel()
	marked := time.Now().UTC().Add(time.Minute) // 让 update_time 推进可见
	stub := &deadLetterRepoStub{nowFunc: func() time.Time { return marked }}
	base := time.Now().UTC().Add(-time.Hour)
	stub.letters = []*DeadLetter{
		{ID: "none-row", ReplayState: ReplayStateNone, CreateTime: base, UpdateTime: base},
		{ID: "pending-row", ReplayState: ReplayStatePending, CreateTime: base, UpdateTime: base},
		{ID: "replayed-row", ReplayState: ReplayStateReplayed, CreateTime: base, UpdateTime: base},
	}
	uc := NewDeadLetterUsecase(stub)
	ctx := context.Background()

	// none → pending：成功且 update_time 推进。
	letter, err := uc.Replay(ctx, "none-row")
	if err != nil {
		t.Fatalf("Replay(none) error = %v", err)
	}
	if letter.ReplayState != ReplayStatePending || !letter.UpdateTime.Equal(marked) {
		t.Fatalf("Replay(none) = (%d, %v), want pending + advanced update_time",
			letter.ReplayState, letter.UpdateTime)
	}
	// 重复标记（已 pending）→ 状态机拒绝。
	if _, err = uc.Replay(ctx, "none-row"); !errors.Is(err, ErrDeadLetterInvalidState) {
		t.Fatalf("Replay(twice) error = %v, want ErrDeadLetterInvalidState", err)
	}
	// pending 行（worker 处理中）→ 拒绝。
	if _, err = uc.Replay(ctx, "pending-row"); !errors.Is(err, ErrDeadLetterInvalidState) {
		t.Fatalf("Replay(pending) error = %v, want ErrDeadLetterInvalidState", err)
	}
	// replayed 行（终态）→ 拒绝。
	if _, err = uc.Replay(ctx, "replayed-row"); !errors.Is(err, ErrDeadLetterInvalidState) {
		t.Fatalf("Replay(replayed) error = %v, want ErrDeadLetterInvalidState", err)
	}
	// 不存在 → 404。
	if _, err = uc.Replay(ctx, "missing"); !errors.Is(err, ErrDeadLetterNotFound) {
		t.Fatalf("Replay(missing) error = %v, want ErrDeadLetterNotFound", err)
	}
}
