package testfake

import (
	"context"
	"slices"
	"strings"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
)

// ListDispatches implements biz.DispatchRepo：发送任务是 append-only 运行
// 数据，测试经 SeedDispatch 预置行后按四维过滤 + 生成时间倒序分页查询。
func (s *Store) ListDispatches(_ context.Context, filter biz.DispatchFilter, page, pageSize int64) ([]*biz.Dispatch, int64, error) {
	s.mu.RLock()
	items := make([]*biz.Dispatch, 0, len(s.dispatches))
	for _, dispatch := range s.dispatches {
		if filter.EventID != "" && dispatch.EventID != filter.EventID {
			continue
		}
		if filter.ChannelID != "" && dispatch.ChannelID != filter.ChannelID {
			continue
		}
		if filter.Status != nil && dispatch.Status != *filter.Status {
			continue
		}
		if filter.CreateTimeStart != nil && dispatch.CreateTime.Before(*filter.CreateTimeStart) {
			continue
		}
		if filter.CreateTimeEnd != nil && dispatch.CreateTime.After(*filter.CreateTimeEnd) {
			continue
		}
		items = append(items, cloneDispatch(dispatch))
	}
	s.mu.RUnlock()

	slices.SortFunc(items, func(a, b *biz.Dispatch) int {
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

// cloneDispatch 深拷贝发送任务 DO（Content 为引用类型、NextRetry 为指针）。
func cloneDispatch(dispatch *biz.Dispatch) *biz.Dispatch {
	cp := *dispatch
	cp.Content = slices.Clone(dispatch.Content)
	if dispatch.NextRetry != nil {
		next := *dispatch.NextRetry
		cp.NextRetry = &next
	}
	return &cp
}

// SeedDispatch 预置一行发送任务（模拟管道 router 生成），供 e2e 与 service
// 单测构造查询数据；时间缺省取当前。
func (s *Store) SeedDispatch(dispatch *biz.Dispatch) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if dispatch.CreateTime.IsZero() {
		dispatch.CreateTime = time.Now().UTC()
	}
	s.dispatches = append(s.dispatches, cloneDispatch(dispatch))
}
