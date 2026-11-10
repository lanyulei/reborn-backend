package testfake

import (
	"context"
	"slices"
	"strings"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
)

// ListDeadLetters implements biz.DeadLetterRepo：死信是运行数据，测试经
// SeedDeadLetter 预置行后按四维过滤 + 转入时间倒序分页查询。
func (s *Store) ListDeadLetters(_ context.Context, filter biz.DeadLetterFilter, page, pageSize int64) ([]*biz.DeadLetter, int64, error) {
	s.mu.RLock()
	items := make([]*biz.DeadLetter, 0, len(s.deadLetters))
	for _, letter := range s.deadLetters {
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
		items = append(items, cloneDeadLetter(letter))
	}
	s.mu.RUnlock()

	slices.SortFunc(items, func(a, b *biz.DeadLetter) int {
		if a.CreateTime != b.CreateTime {
			return b.CreateTime.Compare(a.CreateTime)
		}
		return strings.Compare(b.ID, a.ID)
	})
	total := int64(len(items))
	start := (page - 1) * pageSize
	if start >= total {
		return nil, total, nil
	}
	end := min(start+pageSize, total)
	return items[start:end], total, nil
}

// MarkReplayPending implements biz.DeadLetterRepo：none → pending 条件更新
// （对齐 data 层条件 UPDATE 语义：并发/重复标记按状态机拒绝）。
func (s *Store) MarkReplayPending(_ context.Context, id string) (*biz.DeadLetter, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, letter := range s.deadLetters {
		if letter.ID != id {
			continue
		}
		if letter.ReplayState != biz.ReplayStateNone {
			return nil, biz.ErrDeadLetterInvalidState
		}
		letter.ReplayState = biz.ReplayStatePending
		letter.UpdateTime = time.Now().UTC()
		return cloneDeadLetter(letter), nil
	}
	return nil, biz.ErrDeadLetterNotFound
}

// cloneDeadLetter 拷贝死信 DO（全列标量，浅拷贝即深拷贝）。
func cloneDeadLetter(letter *biz.DeadLetter) *biz.DeadLetter {
	cp := *letter
	return &cp
}

// SeedDeadLetter 预置一行死信（模拟管道 sender 在 dispatch 重试超限后
// 写入），供 e2e 与 service 单测构造数据；时间缺省取当前。
func (s *Store) SeedDeadLetter(letter *biz.DeadLetter) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	if letter.CreateTime.IsZero() {
		letter.CreateTime = now
	}
	if letter.UpdateTime.IsZero() {
		letter.UpdateTime = letter.CreateTime
	}
	s.deadLetters = append(s.deadLetters, cloneDeadLetter(letter))
}

// GetDeadLetter implements biz.DeadLetterRepo。
func (s *Store) GetDeadLetter(_ context.Context, id string) (*biz.DeadLetter, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, letter := range s.deadLetters {
		if letter.ID == id {
			return cloneDeadLetter(letter), nil
		}
	}
	return nil, biz.ErrDeadLetterNotFound
}
