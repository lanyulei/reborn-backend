package testfake

import (
	"context"
	"slices"
	"strings"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
)

// templateEntry 是模板内存条目：DO + 软删标记，对齐 data 层"三维组合唯一仅
// 约束未删除行"的语义（删除后组合可复用）。
type templateEntry struct {
	tpl     *biz.Template
	deleted bool
}

// cloneTemplate 拷贝模板 DO（全标量字段，无引用类型成员）。
func cloneTemplate(tpl *biz.Template) *biz.Template {
	cp := *tpl
	return &cp
}

// templateSpecTaken 报告同 biz 未删除行内 (event_type, channel_type) 组合
// 是否已被占用（排除自身）。
func templateSpecTaken(s map[string]*templateEntry, bizID, eventType, channelType, excludeID string) bool {
	for id, e := range s {
		if e.deleted || id == excludeID {
			continue
		}
		if e.tpl.BizID == bizID && e.tpl.EventType == eventType && e.tpl.ChannelType == channelType {
			return true
		}
	}
	return false
}

// CreateTemplate implements biz.TemplateRepo。
func (s *Store) CreateTemplate(_ context.Context, tpl *biz.Template) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if templateSpecTaken(s.templates, tpl.BizID, tpl.EventType, tpl.ChannelType, "") {
		return biz.ErrTemplateSpecExists
	}
	cp := cloneTemplate(tpl)
	if cp.CreateTime.IsZero() {
		cp.CreateTime, cp.UpdateTime = time.Now(), time.Now()
	}
	s.templates[cp.ID] = &templateEntry{tpl: cp}
	return nil
}

// UpdateTemplate implements biz.TemplateRepo：nil 字段不更新；spec 改动做组合
// 唯一校验；任何更新 version+1（对齐 data 层版本化语义）。
func (s *Store) UpdateTemplate(_ context.Context, id string, upd biz.TemplateUpdate) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.templates[id]
	if !ok || e.deleted {
		return biz.ErrTemplateNotFound
	}
	eventType, channelType := e.tpl.EventType, e.tpl.ChannelType
	if upd.EventType != nil {
		eventType = *upd.EventType
	}
	if upd.ChannelType != nil {
		channelType = *upd.ChannelType
	}
	if templateSpecTaken(s.templates, e.tpl.BizID, eventType, channelType, id) {
		return biz.ErrTemplateSpecExists
	}
	if upd.EventType != nil {
		e.tpl.EventType = *upd.EventType
	}
	if upd.ChannelType != nil {
		e.tpl.ChannelType = *upd.ChannelType
	}
	if upd.Subject != nil {
		e.tpl.Subject = *upd.Subject
	}
	if upd.Body != nil {
		e.tpl.Body = *upd.Body
	}
	if upd.Enabled != nil {
		e.tpl.Enabled = *upd.Enabled
	}
	e.tpl.Version++
	e.tpl.UpdateTime = time.Now()
	return nil
}

// DeleteTemplate implements biz.TemplateRepo：软删（组合可复用）。
func (s *Store) DeleteTemplate(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.templates[id]
	if !ok || e.deleted {
		return biz.ErrTemplateNotFound
	}
	e.deleted = true
	return nil
}

// GetTemplate implements biz.TemplateRepo。
func (s *Store) GetTemplate(_ context.Context, id string) (*biz.Template, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.templates[id]
	if !ok || e.deleted {
		return nil, biz.ErrTemplateNotFound
	}
	return cloneTemplate(e.tpl), nil
}

// ListTemplates implements biz.TemplateRepo：过滤 + 创建时间倒序 + 分页。
func (s *Store) ListTemplates(_ context.Context, filter biz.TemplateFilter, page, pageSize int64) ([]*biz.Template, int64, error) {
	s.mu.RLock()
	items := make([]*biz.Template, 0, len(s.templates))
	for _, e := range s.templates {
		if e.deleted {
			continue
		}
		if filter.BizID != "" && e.tpl.BizID != filter.BizID {
			continue
		}
		if filter.Keyword != "" &&
			!strings.Contains(e.tpl.EventType, filter.Keyword) &&
			!strings.Contains(e.tpl.ChannelType, filter.Keyword) &&
			!strings.Contains(e.tpl.Subject, filter.Keyword) {
			continue
		}
		if filter.EventType != nil && e.tpl.EventType != *filter.EventType {
			continue
		}
		if filter.ChannelType != nil && e.tpl.ChannelType != *filter.ChannelType {
			continue
		}
		if filter.Enabled != nil && e.tpl.Enabled != *filter.Enabled {
			continue
		}
		items = append(items, cloneTemplate(e.tpl))
	}
	s.mu.RUnlock()

	slices.SortFunc(items, func(a, b *biz.Template) int {
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
