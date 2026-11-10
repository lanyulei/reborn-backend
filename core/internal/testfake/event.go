package testfake

import (
	"context"
	"slices"
	"strings"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
)

// ListEvents implements biz.EventRepo：事件是 append-only 运行数据，测试经
// SeedEvent 预置行后按四维过滤 + 创建时间倒序分页查询。
func (s *Store) ListEvents(_ context.Context, filter biz.EventFilter, page, pageSize int64) ([]*biz.Event, int64, error) {
	s.mu.RLock()
	items := make([]*biz.Event, 0, len(s.events))
	for _, event := range s.events {
		if filter.BizID != "" && event.BizID != filter.BizID {
			continue
		}
		if filter.EventType != "" && event.EventType != filter.EventType {
			continue
		}
		if filter.Status != nil && event.Status != *filter.Status {
			continue
		}
		if filter.CreateTimeStart != nil && event.CreateTime.Before(*filter.CreateTimeStart) {
			continue
		}
		if filter.CreateTimeEnd != nil && event.CreateTime.After(*filter.CreateTimeEnd) {
			continue
		}
		items = append(items, cloneEvent(event))
	}
	s.mu.RUnlock()

	slices.SortFunc(items, func(a, b *biz.Event) int {
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

// cloneEvent 深拷贝事件 DO（Payload/Recipients 为引用类型）。
func cloneEvent(event *biz.Event) *biz.Event {
	cp := *event
	cp.Payload = slices.Clone(event.Payload)
	cp.Recipients = slices.Clone(event.Recipients)
	return &cp
}

// SeedEvent 预置一行事件（模拟管道 ingress / NotificationWriter 落库），
// 供 e2e 与 service 单测构造查询数据；时间缺省取当前。
func (s *Store) SeedEvent(event *biz.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if event.CreateTime.IsZero() {
		event.CreateTime = time.Now().UTC()
	}
	if event.NextPublishTime.IsZero() {
		event.NextPublishTime = event.CreateTime
	}
	s.events = append(s.events, cloneEvent(event))
}

// InsertEvent implements biz.EventWriter：core 侧事件直插（SendTestEvent
// 连通性测试 / 后续 NotificationWriter），落库语义与 SeedEvent 一致——
// append-only、时间缺省取当前、深拷贝入表。
func (s *Store) InsertEvent(_ context.Context, event *biz.Event) error {
	s.SeedEvent(event)
	return nil
}
