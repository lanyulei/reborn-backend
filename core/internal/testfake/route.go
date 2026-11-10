package testfake

import (
	"context"
	"slices"
	"strings"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
)

// routeEntry 是路由内存条目：DO + 软删标记（路由设计即无唯一约束，多条可重叠）。
type routeEntry struct {
	route   *biz.Route
	deleted bool
}

// cloneRoute 深拷贝路由 DO（ChannelIDs/TagMatch/Recipient.Fixed 为引用类型）。
func cloneRoute(route *biz.Route) *biz.Route {
	cp := *route
	cp.ChannelIDs = slices.Clone(route.ChannelIDs)
	cp.TagMatch = mapsClone(route.TagMatch)
	cp.Recipient.Fixed = slices.Clone(route.Recipient.Fixed)
	return &cp
}

// mapsClone 拷贝字符串映射（nil 保持 nil）。
func mapsClone(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	cp := make(map[string]string, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
}

// CreateRoute implements biz.RouteRepo：无唯一约束，直接落条目。
func (s *Store) CreateRoute(_ context.Context, route *biz.Route) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := cloneRoute(route)
	if cp.CreateTime.IsZero() {
		cp.CreateTime, cp.UpdateTime = time.Now(), time.Now()
	}
	s.routes[cp.ID] = &routeEntry{route: cp}
	return nil
}

// applyRouteRule 整组替换规则字段（对齐 data 层 Rule 语义）。
func applyRouteRule(route *biz.Route, rule *biz.RouteRule) {
	route.EventPattern = rule.EventPattern
	route.SeverityMin = rule.SeverityMin
	route.TagMatch = mapsClone(rule.TagMatch)
	route.Priority = rule.Priority
	route.ChannelIDs = slices.Clone(rule.ChannelIDs)
	route.Recipient = biz.RouteRecipient{
		Fixed:        slices.Clone(rule.Recipient.Fixed),
		Subscription: rule.Recipient.Subscription,
	}
	route.AggregateWindowS = rule.AggregateWindowS
	route.SuppressThreshold = rule.SuppressThreshold
	route.SuppressDurationM = rule.SuppressDurationM
}

// UpdateRoute implements biz.RouteRepo：Rule 整组替换 / Enabled 启停。
func (s *Store) UpdateRoute(_ context.Context, id string, upd biz.RouteUpdate) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.routes[id]
	if !ok || e.deleted {
		return biz.ErrRouteNotFound
	}
	if upd.Rule != nil {
		applyRouteRule(e.route, upd.Rule)
	}
	if upd.Enabled != nil {
		e.route.Enabled = *upd.Enabled
	}
	e.route.UpdateTime = time.Now()
	return nil
}

// DeleteRoute implements biz.RouteRepo：软删。
func (s *Store) DeleteRoute(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.routes[id]
	if !ok || e.deleted {
		return biz.ErrRouteNotFound
	}
	e.deleted = true
	return nil
}

// GetRoute implements biz.RouteRepo。
func (s *Store) GetRoute(_ context.Context, id string) (*biz.Route, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.routes[id]
	if !ok || e.deleted {
		return nil, biz.ErrRouteNotFound
	}
	return cloneRoute(e.route), nil
}

// ListRoutes implements biz.RouteRepo：过滤 + 创建时间倒序 + 分页。
func (s *Store) ListRoutes(_ context.Context, filter biz.RouteFilter, page, pageSize int64) ([]*biz.Route, int64, error) {
	s.mu.RLock()
	items := make([]*biz.Route, 0, len(s.routes))
	for _, e := range s.routes {
		if e.deleted {
			continue
		}
		if filter.BizID != "" && e.route.BizID != filter.BizID {
			continue
		}
		if filter.Keyword != "" && !strings.Contains(e.route.EventPattern, filter.Keyword) {
			continue
		}
		if filter.Enabled != nil && e.route.Enabled != *filter.Enabled {
			continue
		}
		items = append(items, cloneRoute(e.route))
	}
	s.mu.RUnlock()

	slices.SortFunc(items, func(a, b *biz.Route) int {
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

// CountRoutesUsingChannel implements biz.ChannelReferenceChecker：未删除路由中引用该渠道的条数。
func (s *Store) CountRoutesUsingChannel(_ context.Context, channelID string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var n int64
	for _, e := range s.routes {
		if e.deleted {
			continue
		}
		for _, id := range e.route.ChannelIDs {
			if id == channelID {
				n++
				break
			}
		}
	}
	return n, nil
}
