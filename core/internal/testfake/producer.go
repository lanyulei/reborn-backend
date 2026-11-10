package testfake

import (
	"context"
	"slices"
	"strings"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
)

// producerEntry 是生产者内存条目：DO + 软删标记（名称唯一性按 biz 仅约束
// 未删除行，对齐 data 层部分唯一索引）。
type producerEntry struct {
	producer *biz.Producer
	deleted  bool
}

// cloneProducer 深拷贝生产者 DO（Whitelist 切片与 ExpireTime 指针为引用类型）。
func cloneProducer(producer *biz.Producer) *biz.Producer {
	cp := *producer
	cp.EventWhitelist = slices.Clone(producer.EventWhitelist)
	if producer.ExpireTime != nil {
		expire := *producer.ExpireTime
		cp.ExpireTime = &expire
	}
	cp.TokenHash = slices.Clone(producer.TokenHash)
	return &cp
}

// applyProducerSpec 整组替换配置字段（expire nil 即清除过期时间）。
func applyProducerSpec(producer *biz.Producer, spec *biz.ProducerSpec) {
	producer.Name = spec.Name
	producer.EventWhitelist = slices.Clone(spec.EventWhitelist)
	producer.QPSLimit, producer.DailyLimit = spec.QPSLimit, spec.DailyLimit
	if spec.ExpireTime != nil {
		expire := *spec.ExpireTime
		producer.ExpireTime = &expire
	} else {
		producer.ExpireTime = nil
	}
}

// producerNameTaken 报告同 biz 未删除行内名称是否已占用。
func producerNameTaken(s *Store, producer *biz.Producer) bool {
	for _, e := range s.producers {
		if e.deleted || e.producer == producer {
			continue
		}
		if e.producer.BizID == producer.BizID && e.producer.Name == producer.Name {
			return true
		}
	}
	return false
}

// CreateProducer implements biz.ProducerRepo：同 biz 未删除行内重名返回
// ErrProducerNameExists。
func (s *Store) CreateProducer(_ context.Context, producer *biz.Producer) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if producerNameTaken(s, producer) {
		return biz.ErrProducerNameExists
	}
	cp := cloneProducer(producer)
	if cp.CreateTime.IsZero() {
		cp.CreateTime, cp.UpdateTime = time.Now(), time.Now()
	}
	s.producers[cp.ID] = &producerEntry{producer: cp}
	return nil
}

// UpdateProducer implements biz.ProducerRepo：Spec 整组替换 / Enabled 启停。
func (s *Store) UpdateProducer(_ context.Context, id string, upd biz.ProducerUpdate) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.producers[id]
	if !ok || e.deleted {
		return biz.ErrProducerNotFound
	}
	if upd.Spec != nil {
		probe := cloneProducer(e.producer)
		applyProducerSpec(probe, upd.Spec)
		if probe.BizID == e.producer.BizID && producerNameTaken(s, probe) {
			return biz.ErrProducerNameExists
		}
		applyProducerSpec(e.producer, upd.Spec)
	}
	if upd.Enabled != nil {
		e.producer.Enabled = *upd.Enabled
	}
	e.producer.UpdateTime = time.Now()
	return nil
}

// UpdateProducerToken implements biz.ProducerRepo：轮换哈希并返回权威行。
func (s *Store) UpdateProducerToken(_ context.Context, id string, tokenHash []byte) (*biz.Producer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.producers[id]
	if !ok || e.deleted {
		return nil, biz.ErrProducerNotFound
	}
	e.producer.TokenHash = slices.Clone(tokenHash)
	e.producer.UpdateTime = time.Now()
	return cloneProducer(e.producer), nil
}

// DeleteProducer implements biz.ProducerRepo：软删。
func (s *Store) DeleteProducer(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.producers[id]
	if !ok || e.deleted {
		return biz.ErrProducerNotFound
	}
	e.deleted = true
	return nil
}

// GetProducer implements biz.ProducerRepo。
func (s *Store) GetProducer(_ context.Context, id string) (*biz.Producer, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.producers[id]
	if !ok || e.deleted {
		return nil, biz.ErrProducerNotFound
	}
	return cloneProducer(e.producer), nil
}

// ListProducers implements biz.ProducerRepo：过滤 + 创建时间倒序 + 分页。
func (s *Store) ListProducers(_ context.Context, filter biz.ProducerFilter, page, pageSize int64) ([]*biz.Producer, int64, error) {
	s.mu.RLock()
	items := make([]*biz.Producer, 0, len(s.producers))
	for _, e := range s.producers {
		if e.deleted {
			continue
		}
		if filter.BizID != "" && e.producer.BizID != filter.BizID {
			continue
		}
		if filter.Keyword != "" && !strings.Contains(e.producer.Name, filter.Keyword) {
			continue
		}
		if filter.Enabled != nil && e.producer.Enabled != *filter.Enabled {
			continue
		}
		items = append(items, cloneProducer(e.producer))
	}
	s.mu.RUnlock()

	slices.SortFunc(items, func(a, b *biz.Producer) int {
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
