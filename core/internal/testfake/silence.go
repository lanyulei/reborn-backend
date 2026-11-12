package testfake

import (
	"context"
	"slices"
	"strings"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
)

// silenceEntry 是静默内存条目：DO + 软删标记（设计即无唯一约束，多条可重叠）。
type silenceEntry struct {
	silence *biz.Silence
	deleted bool
}

// cloneSilence 深拷贝静默 DO（Window 为指针引用类型）。
func cloneSilence(silence *biz.Silence) *biz.Silence {
	cp := *silence
	if silence.Window != nil {
		window := *silence.Window
		cp.Window = &window
	}
	return &cp
}

// applySilenceRule 整组替换规则字段（对齐 data 层 Rule 语义：window nil 即
// 落 NULL 清除时间窗）。
func applySilenceRule(silence *biz.Silence, rule *biz.SilenceRule) {
	silence.EventPattern = rule.EventPattern
	silence.SeverityMax = rule.SeverityMax
	if rule.Window != nil {
		window := *rule.Window
		silence.Window = &window
	} else {
		silence.Window = nil
	}
}

// CreateSilence implements biz.SilenceRepo：无唯一约束，直接落条目。
func (s *Store) CreateSilence(_ context.Context, silence *biz.Silence) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := cloneSilence(silence)
	if cp.CreateTime.IsZero() {
		cp.CreateTime, cp.UpdateTime = time.Now(), time.Now()
	}
	s.silences[cp.ID] = &silenceEntry{silence: cp}
	return nil
}

// UpdateSilence implements biz.SilenceRepo：Rule 整组替换 / Enabled 启停。
func (s *Store) UpdateSilence(_ context.Context, id string, upd biz.SilenceUpdate) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.silences[id]
	if !ok || e.deleted {
		return biz.ErrSilenceNotFound
	}
	if upd.Rule != nil {
		applySilenceRule(e.silence, upd.Rule)
	}
	if upd.Enabled != nil {
		e.silence.Enabled = *upd.Enabled
	}
	e.silence.UpdateTime = time.Now()
	return nil
}

// DeleteSilence implements biz.SilenceRepo：软删。
func (s *Store) DeleteSilence(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.silences[id]
	if !ok || e.deleted {
		return biz.ErrSilenceNotFound
	}
	e.deleted = true
	return nil
}

// GetSilence implements biz.SilenceRepo。
func (s *Store) GetSilence(_ context.Context, id string) (*biz.Silence, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.silences[id]
	if !ok || e.deleted {
		return nil, biz.ErrSilenceNotFound
	}
	return cloneSilence(e.silence), nil
}

// ListSilences implements biz.SilenceRepo：过滤 + 创建时间倒序 + 分页。
func (s *Store) ListSilences(_ context.Context, filter biz.SilenceFilter, page, pageSize int64) ([]*biz.Silence, int64, error) {
	s.mu.RLock()
	items := make([]*biz.Silence, 0, len(s.silences))
	for _, e := range s.silences {
		if e.deleted {
			continue
		}
		if filter.BizID != "" && e.silence.BizID != filter.BizID {
			continue
		}
		if filter.Keyword != "" && !strings.Contains(e.silence.EventPattern, filter.Keyword) && !strings.Contains(e.silence.Name, filter.Keyword) {
			continue
		}
		if filter.Enabled != nil && e.silence.Enabled != *filter.Enabled {
			continue
		}
		items = append(items, cloneSilence(e.silence))
	}
	s.mu.RUnlock()

	slices.SortFunc(items, func(a, b *biz.Silence) int {
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

// ListSilencesByBiz implements biz 仓储接口：biz 内全部未删除行。
func (s *Store) ListSilencesByBiz(_ context.Context, bizID string) ([]*biz.Silence, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*biz.Silence
	for _, e := range s.silences {
		if e.deleted || e.silence.BizID != bizID {
			continue
		}
		out = append(out, cloneSilence(e.silence))
	}
	slices.SortFunc(out, func(a, b *biz.Silence) int { return strings.Compare(a.ID, b.ID) })
	return out, nil
}
