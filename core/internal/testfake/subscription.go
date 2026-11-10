package testfake

import (
	"context"
	"slices"
	"strings"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
)

// subscriptionEntry 是订阅内存条目：DO + 软删标记。
type subscriptionEntry struct {
	sub     *biz.Subscription
	deleted bool
}

// cloneSubscription 深拷贝订阅 DO（ChannelPrefs 为 map 引用类型）。
func cloneSubscription(sub *biz.Subscription) *biz.Subscription {
	cp := *sub
	cp.ChannelPrefs = make(biz.ChannelPrefs, len(sub.ChannelPrefs))
	for k, v := range sub.ChannelPrefs {
		cp.ChannelPrefs[k] = v
	}
	return &cp
}

// UpsertSubscription implements biz.SubscriptionRepo：同自然键（未删除行）
// 整组替换偏好与启停并保留原 id/创建时间，否则新建；返回权威行副本。
func (s *Store) UpsertSubscription(_ context.Context, sub *biz.Subscription) (*biz.Subscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.subscriptions {
		if e.deleted {
			continue
		}
		if e.sub.UserID == sub.UserID && e.sub.BizID == sub.BizID && e.sub.EventPattern == sub.EventPattern {
			e.sub.ChannelPrefs = make(biz.ChannelPrefs, len(sub.ChannelPrefs))
			for k, v := range sub.ChannelPrefs {
				e.sub.ChannelPrefs[k] = v
			}
			e.sub.Enabled = sub.Enabled
			e.sub.UpdateTime = time.Now()
			return cloneSubscription(e.sub), nil
		}
	}
	cp := cloneSubscription(sub)
	if cp.CreateTime.IsZero() {
		cp.CreateTime, cp.UpdateTime = time.Now(), time.Now()
	}
	s.subscriptions[cp.ID] = &subscriptionEntry{sub: cp}
	return cloneSubscription(cp), nil
}

// DeleteSubscription implements biz.SubscriptionRepo：软删。
func (s *Store) DeleteSubscription(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.subscriptions[id]
	if !ok || e.deleted {
		return biz.ErrSubscriptionNotFound
	}
	e.deleted = true
	return nil
}

// GetSubscription implements biz.SubscriptionRepo。
func (s *Store) GetSubscription(_ context.Context, id string) (*biz.Subscription, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.subscriptions[id]
	if !ok || e.deleted {
		return nil, biz.ErrSubscriptionNotFound
	}
	return cloneSubscription(e.sub), nil
}

// ListSubscriptions implements biz.SubscriptionRepo：按目标用户过滤 + 创建
// 时间倒序 + 分页。
func (s *Store) ListSubscriptions(_ context.Context, filter biz.SubscriptionFilter, page, pageSize int64) ([]*biz.Subscription, int64, error) {
	s.mu.RLock()
	items := make([]*biz.Subscription, 0, len(s.subscriptions))
	for _, e := range s.subscriptions {
		if e.deleted || e.sub.UserID != filter.UserID {
			continue
		}
		if filter.Keyword != "" && !strings.Contains(e.sub.EventPattern, filter.Keyword) {
			continue
		}
		if filter.Enabled != nil && e.sub.Enabled != *filter.Enabled {
			continue
		}
		items = append(items, cloneSubscription(e.sub))
	}
	s.mu.RUnlock()

	slices.SortFunc(items, func(a, b *biz.Subscription) int {
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
