package testfake

import (
	"context"
	"slices"
	"strings"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
)

// ListDeliveryLogs implements biz.DeliveryLogRepo：投递明细是 append-only
// 运行数据，测试经 SeedDeliveryLog 预置行后按四维过滤 + 落库时间倒序分页
// 查询。
func (s *Store) ListDeliveryLogs(_ context.Context, filter biz.DeliveryLogFilter, page, pageSize int64) ([]*biz.DeliveryLog, int64, error) {
	s.mu.RLock()
	items := make([]*biz.DeliveryLog, 0, len(s.deliveryLogs))
	for _, log := range s.deliveryLogs {
		if filter.DispatchID != "" && log.DispatchID != filter.DispatchID {
			continue
		}
		if filter.ChannelID != "" && log.ChannelID != filter.ChannelID {
			continue
		}
		if filter.OK != nil && log.OK != *filter.OK {
			continue
		}
		if filter.CreateTimeStart != nil && log.CreateTime.Before(*filter.CreateTimeStart) {
			continue
		}
		if filter.CreateTimeEnd != nil && log.CreateTime.After(*filter.CreateTimeEnd) {
			continue
		}
		items = append(items, cloneDeliveryLog(log))
	}
	s.mu.RUnlock()

	slices.SortFunc(items, func(a, b *biz.DeliveryLog) int {
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

// cloneDeliveryLog 拷贝投递明细 DO（全列标量，浅拷贝即深拷贝）。
func cloneDeliveryLog(log *biz.DeliveryLog) *biz.DeliveryLog {
	cp := *log
	return &cp
}

// SeedDeliveryLog 预置一行投递明细（模拟管道 sender 追加），供 e2e 与
// service 单测构造查询数据；时间缺省取当前。
func (s *Store) SeedDeliveryLog(log *biz.DeliveryLog) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if log.CreateTime.IsZero() {
		log.CreateTime = time.Now().UTC()
	}
	s.deliveryLogs = append(s.deliveryLogs, cloneDeliveryLog(log))
}
