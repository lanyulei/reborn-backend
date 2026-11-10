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

// Backlog implements biz.BacklogRepo（两条部分索引计数，不触碰历史分区）。
func (d *Data) Backlog(ctx context.Context) (biz.Backlog, error) {
	var b biz.Backlog
	if err := d.Pool.QueryRow(ctx,
		"SELECT (SELECT count(*) FROM notify_event WHERE publish_state = 0),"+
			" (SELECT count(*) FROM notify_dispatch WHERE status IN (1, 4) AND next_retry_time <= now())").Scan(&b.PendingEvents, &b.DueDispatches); err != nil {
		return b, fmt.Errorf("query backlog: %w", err)
	}
	return b, nil
}

// Ping implements biz.Pinger（1s 超时）。
func (d *Data) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return d.Pool.Ping(ctx)
}
