package data

import (
	"context"
	"fmt"
	"time"

	"github.com/lanyulei/reborn/backend/notify/internal/biz"
)

var (
	_ biz.BacklogRepo = (*Data)(nil)
	_ biz.Pinger      = (*Data)(nil)
)

// Backlog implements biz.BacklogRepo：部分索引上的有界计数——超过 biz.BacklogCap
// 即停止扫描（百万积压时每次抓取不再全索引扫描；指标读数 ≥ BacklogCap 表示饱和）。
func (d *Data) Backlog(ctx context.Context) (biz.Backlog, error) {
	var b biz.Backlog
	var oldestPending, oldestDue *time.Time
	if err := d.Pool.QueryRow(ctx,
		"SELECT (SELECT count(*) FROM (SELECT 1 FROM notify_event WHERE publish_state = 0 LIMIT $1) p),"+
			" (SELECT count(*) FROM (SELECT 1 FROM notify_dispatch WHERE status IN (1, 4) AND next_retry_time <= now() LIMIT $1) q),"+
			" (SELECT min(next_publish_time) FROM notify_event WHERE publish_state = 0),"+
			" (SELECT min(next_retry_time) FROM notify_dispatch WHERE status IN (1, 4) AND next_retry_time <= now())",
		biz.BacklogCap).Scan(&b.PendingEvents, &b.DueDispatches, &oldestPending, &oldestDue); err != nil {
		return b, fmt.Errorf("query backlog: %w", err)
	}
	if oldestPending != nil {
		b.OldestPending = *oldestPending
	}
	if oldestDue != nil {
		b.OldestDue = *oldestDue
	}
	return b, nil
}

// Ping implements biz.Pinger（1s 超时）。
func (d *Data) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return d.Pool.Ping(ctx)
}
