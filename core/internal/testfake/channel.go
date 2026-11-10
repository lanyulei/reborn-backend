package testfake

import (
	"context"
	"slices"
	"strings"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
)

// channelEntry 是渠道内存条目：DO + 软删标记，对齐 data 层"名称唯一约束仅
// 约束未删除行"的语义（删除后名称可复用）。
type channelEntry struct {
	ch      *biz.Channel
	deleted bool
}

// cloneChannel 深拷贝渠道 DO（Config 三分支指针、机器人 @ 名单、Webhook
// headers、限流值），杜绝内存仓储外泄内部引用。
func cloneChannel(ch *biz.Channel) *biz.Channel {
	cp := *ch
	cp.Config = cloneChannelConfig(ch.Config)
	return &cp
}

// cloneChannelConfig 深拷贝类型化配置。
func cloneChannelConfig(cfg *biz.ChannelConfig) *biz.ChannelConfig {
	if cfg == nil {
		return nil
	}
	cp := &biz.ChannelConfig{}
	if cfg.Email != nil {
		cp.Email = &biz.EmailConfig{}
		*cp.Email = *cfg.Email
	}
	if cfg.Webhook != nil {
		cp.Webhook = &biz.WebhookConfig{URL: cfg.Webhook.URL, Method: cfg.Webhook.Method, TimeoutS: cfg.Webhook.TimeoutS}
		if cfg.Webhook.Headers != nil {
			cp.Webhook.Headers = make(map[string]string, len(cfg.Webhook.Headers))
			for k, v := range cfg.Webhook.Headers {
				cp.Webhook.Headers[k] = v
			}
		}
	}
	if cfg.Robot != nil {
		cp.Robot = &biz.RobotConfig{AtMobiles: slices.Clone(cfg.Robot.AtMobiles)}
	}
	return cp
}

// channelNameTaken 报告同 biz 未删除行内名称是否已被占用（排除自身）。
func channelNameTaken(s map[string]*channelEntry, bizID, name, excludeID string) bool {
	for id, e := range s {
		if e.deleted || id == excludeID {
			continue
		}
		if e.ch.BizID == bizID && e.ch.Name == name {
			return true
		}
	}
	return false
}

// CreateChannel implements biz.ChannelRepo.
func (s *Store) CreateChannel(_ context.Context, ch *biz.Channel) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if channelNameTaken(s.channels, ch.BizID, ch.Name, "") {
		return biz.ErrChannelNameExists
	}
	cp := cloneChannel(ch)
	if cp.CreateTime.IsZero() {
		cp.CreateTime, cp.UpdateTime = time.Now(), time.Now()
	}
	s.channels[cp.ID] = &channelEntry{ch: cp}
	return nil
}

// UpdateChannel implements biz.ChannelRepo：nil 字段不更新；secret 为 nil
// 保留旧值（对齐 data 层"不传=保留旧密文"）。
func (s *Store) UpdateChannel(_ context.Context, id string, upd biz.ChannelUpdate) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.channels[id]
	if !ok || e.deleted {
		return biz.ErrChannelNotFound
	}
	if upd.Name != nil && channelNameTaken(s.channels, e.ch.BizID, *upd.Name, id) {
		return biz.ErrChannelNameExists
	}
	if upd.Name != nil {
		e.ch.Name = *upd.Name
	}
	if upd.Config != nil {
		e.ch.Config = cloneChannelConfig(upd.Config)
	}
	if upd.RateLimit != nil {
		e.ch.RateLimit = *upd.RateLimit
	}
	if upd.Enabled != nil {
		e.ch.Enabled = *upd.Enabled
	}
	if upd.Secret != nil {
		e.ch.Secret = *upd.Secret
	}
	if upd.SignSecret != nil {
		e.ch.SignSecret = *upd.SignSecret
	}
	e.ch.UpdateTime = time.Now()
	return nil
}

// DeleteChannel implements biz.ChannelRepo：软删（名称可复用）。
func (s *Store) DeleteChannel(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.channels[id]
	if !ok || e.deleted {
		return biz.ErrChannelNotFound
	}
	e.deleted = true
	return nil
}

// GetChannel implements biz.ChannelRepo：出参不含 secret（对齐 data 层不查该列）。
func (s *Store) GetChannel(_ context.Context, id string) (*biz.Channel, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.channels[id]
	if !ok || e.deleted {
		return nil, biz.ErrChannelNotFound
	}
	cp := cloneChannel(e.ch)
	cp.Secret, cp.SignSecret = "", ""
	return cp, nil
}

// ListChannelsByIDs implements biz.ChannelRepo：ID 集合内未删除渠道（不含
// secret），缺失 ID 静默跳过。
func (s *Store) ListChannelsByIDs(_ context.Context, ids []string) ([]*biz.Channel, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	items := make([]*biz.Channel, 0, len(ids))
	for _, id := range ids {
		e, ok := s.channels[id]
		if !ok || e.deleted {
			continue
		}
		cp := cloneChannel(e.ch)
		cp.Secret, cp.SignSecret = "", ""
		items = append(items, cp)
	}
	return items, nil
}

// ListChannels implements biz.ChannelRepo：过滤 + 创建时间倒序 + 分页。
func (s *Store) ListChannels(_ context.Context, filter biz.ChannelFilter, page, pageSize int64) ([]*biz.Channel, int64, error) {
	s.mu.RLock()
	items := make([]*biz.Channel, 0, len(s.channels))
	for _, e := range s.channels {
		if e.deleted {
			continue
		}
		if filter.BizID != "" && e.ch.BizID != filter.BizID {
			continue
		}
		if filter.Keyword != "" &&
			!strings.Contains(e.ch.Name, filter.Keyword) && !strings.Contains(e.ch.Type, filter.Keyword) {
			continue
		}
		if filter.Type != nil && e.ch.Type != *filter.Type {
			continue
		}
		if filter.Enabled != nil && e.ch.Enabled != *filter.Enabled {
			continue
		}
		cp := cloneChannel(e.ch)
		cp.Secret, cp.SignSecret = "", ""
		items = append(items, cp)
	}
	s.mu.RUnlock()

	slices.SortFunc(items, func(a, b *biz.Channel) int {
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
