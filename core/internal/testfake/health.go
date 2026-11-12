package testfake

import (
	"context"
	"slices"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
)

// SeedChannelHealth 预置一行渠道健康（模拟管道 sender 的 upsert；渠道名/类型/biz
// 取自已建渠道，调用方只需填 ChannelID 与状态字段）。
func (s *Store) SeedChannelHealth(h *biz.ChannelHealth) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *h
	if e, ok := s.channels[h.ChannelID]; ok {
		cp.ChannelName, cp.ChannelType, cp.BizID = e.ch.Name, e.ch.Type, e.ch.BizID
	}
	s.channelHealth = append(s.channelHealth, &cp)
}

// ListChannelHealth implements biz.ChannelHealthRepo：按 biz 过滤、已删渠道不返回、
// 打开优先。
func (s *Store) ListChannelHealth(_ context.Context, bizID string) ([]*biz.ChannelHealth, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*biz.ChannelHealth
	for _, h := range s.channelHealth {
		if e, ok := s.channels[h.ChannelID]; !ok || e.deleted {
			continue
		}
		if bizID != "" && h.BizID != bizID {
			continue
		}
		cp := *h
		out = append(out, &cp)
	}
	slices.SortStableFunc(out, func(a, b *biz.ChannelHealth) int {
		switch {
		case a.OpenUntil != nil && b.OpenUntil == nil:
			return -1
		case a.OpenUntil == nil && b.OpenUntil != nil:
			return 1
		default:
			return b.UpdateTime.Compare(a.UpdateTime)
		}
	})
	return out, nil
}
