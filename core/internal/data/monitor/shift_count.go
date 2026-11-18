package data

import (
	"context"
	"fmt"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
)

var _ biz.ShiftAlertCounter = (*shiftCountRepo)(nil)

// shiftCountRepo 值班班次内通知到当班人的告警数（读 core 自己写的 monitor_notify_log）。
type shiftCountRepo struct {
	data *basedata.Data
}

// NewShiftCountRepo new a shiftCountRepo.
func NewShiftCountRepo(data *basedata.Data) *shiftCountRepo { return &shiftCountRepo{data: data} }

// CountNotified implements biz.ShiftAlertCounter。一条语句：窗口以三个等长数组传入，逐窗口用 LATERAL 子查询
// 统计——通知时间走 idx_monitor_notify_log_time，接收方用 @>（recipients 的 GIN 索引是 jsonb_path_ops）。
// 窗口数由调用方限制（至多 500）。
func (r *shiftCountRepo) CountNotified(ctx context.Context, windows []biz.ShiftWindow) ([]int32, error) {
	out := make([]int32, len(windows))
	if len(windows) == 0 {
		return out, nil
	}
	users := make([]string, len(windows))
	starts, ends := make([]time.Time, len(windows)), make([]time.Time, len(windows))
	for i, w := range windows {
		users[i], starts[i], ends[i] = w.UserID, w.Start, w.End
	}
	rows, err := r.data.Pool.Query(ctx,
		`SELECT w.ord, c.n
		 FROM unnest($1::text[], $2::timestamptz[], $3::timestamptz[]) WITH ORDINALITY AS w(uid, s, e, ord)
		 CROSS JOIN LATERAL (
		   SELECT count(DISTINCT l.alert_id) AS n FROM monitor_notify_log l
		   WHERE l.create_time >= w.s AND l.create_time < w.e AND l.recipients @> jsonb_build_array(w.uid)) c
		 ORDER BY w.ord`, users, starts, ends)
	if err != nil {
		return nil, fmt.Errorf("count shift monitor alerts: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var ord int64
		var n int32
		if err := rows.Scan(&ord, &n); err != nil {
			return nil, fmt.Errorf("scan shift monitor alert count: %w", err)
		}
		if ord >= 1 && ord <= int64(len(out)) {
			out[ord-1] = n
		}
	}
	return out, rows.Err()
}
