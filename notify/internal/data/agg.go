package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/lanyulei/reborn/backend/notify/internal/biz"

	"github.com/jackc/pgx/v5"
)

var _ biz.AggRepo = (*aggRepo)(nil)

// aggRepo 聚合组仓储。
type aggRepo struct {
	data *Data
}

// NewAggRepo new an aggRepo.
func NewAggRepo(data *Data) *aggRepo { return &aggRepo{data: data} }

// reopenCond 组已关闭且抑制期已过（或无抑制）→ 本次 upsert 重开新窗口。
const reopenCond = "(notify_agg_group.status = 2 AND (notify_agg_group.suppressed_until IS NULL OR notify_agg_group.suppressed_until <= now()))"

// upsertAggGroups 按调用方给定顺序（已排序）逐键 upsert：开放组累加并延长窗口
// （封顶 first_event_time + max_window，评审 #10）；关闭且抑制期内仅计数不重开
// （评审 #12）；关闭且抑制期过则重开。samples 在 SQL 内封顶 20（评审 #11）。
func upsertAggGroups(ctx context.Context, tx pgx.Tx, incs []biz.AggIncrement) (map[string]biz.AggVerdict, error) {
	const q = `INSERT INTO notify_agg_group
		(group_key, biz_id, event_type, route_id, first_event_id, first_event_time, count, samples, window_until, suppressed_until, status, create_time, update_time)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, now() + make_interval(secs => $9), NULL, 1, now(), now())
		ON CONFLICT (group_key) DO UPDATE SET
		  count = CASE WHEN ` + reopenCond + ` THEN EXCLUDED.count ELSE notify_agg_group.count + EXCLUDED.count END,
		  samples = CASE WHEN ` + reopenCond + ` THEN EXCLUDED.samples
		                 WHEN notify_agg_group.status = 1 THEN (
		                   SELECT COALESCE(jsonb_agg(e ORDER BY ord), '[]'::jsonb) FROM (
		                     SELECT e, ord FROM jsonb_array_elements(notify_agg_group.samples || EXCLUDED.samples) WITH ORDINALITY AS t(e, ord)
		                     ORDER BY ord LIMIT 20) s)
		                 ELSE notify_agg_group.samples END,
		  route_id = CASE WHEN ` + reopenCond + ` THEN EXCLUDED.route_id ELSE notify_agg_group.route_id END,
		  first_event_id = CASE WHEN ` + reopenCond + ` THEN EXCLUDED.first_event_id ELSE notify_agg_group.first_event_id END,
		  first_event_time = CASE WHEN ` + reopenCond + ` THEN EXCLUDED.first_event_time ELSE notify_agg_group.first_event_time END,
		  window_until = CASE WHEN ` + reopenCond + ` THEN EXCLUDED.window_until
		                      WHEN notify_agg_group.status = 1 THEN LEAST(now() + make_interval(secs => $9), notify_agg_group.first_event_time + make_interval(secs => $10))
		                      ELSE notify_agg_group.window_until END,
		  status = CASE WHEN ` + reopenCond + ` THEN 1 ELSE notify_agg_group.status END,
		  suppressed_until = CASE WHEN ` + reopenCond + ` THEN NULL ELSE notify_agg_group.suppressed_until END,
		  update_time = now()
		RETURNING (status = 2 AND suppressed_until IS NOT NULL AND suppressed_until > now())`
	out := make(map[string]biz.AggVerdict, len(incs))
	for _, inc := range incs {
		samples, err := json.Marshal(inc.Samples)
		if err != nil {
			return nil, fmt.Errorf("encode agg samples: %w", err)
		}
		var suppressed bool
		if err := tx.QueryRow(ctx, q, inc.Key, inc.BizID, inc.EventType, inc.RouteID, inc.FirstEventID, inc.FirstEventTime,
			inc.Count, samples, float64(inc.WindowS), inc.MaxWindow.Seconds()).Scan(&suppressed); err != nil {
			return nil, fmt.Errorf("upsert agg group %s: %w", inc.Key, err)
		}
		out[inc.Key] = biz.AggVerdict{Suppressed: suppressed}
	}
	return out, nil
}

// ListDueGroups implements biz.AggRepo.
func (r *aggRepo) ListDueGroups(ctx context.Context, limit int) ([]string, error) {
	rows, err := r.data.Pool.Query(ctx,
		"SELECT group_key FROM notify_agg_group WHERE status = 1 AND window_until < now() ORDER BY window_until LIMIT $1", limit)
	if err != nil {
		return nil, fmt.Errorf("list due agg groups: %w", err)
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	return keys, rows.Err()
}

// RouteOf implements biz.AggRepo.
func (r *aggRepo) RouteOf(ctx context.Context, key string) (string, error) {
	var routeID string
	err := r.data.Pool.QueryRow(ctx, "SELECT route_id::text FROM notify_agg_group WHERE group_key = $1", key).Scan(&routeID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("agg group route: %w", err)
	}
	return routeID, nil
}

// CloseGroup implements biz.AggRepo：条件 UPDATE…RETURNING 为汇总唯一数据源。
func (r *aggRepo) CloseGroup(ctx context.Context, key string, threshold, suppressMinutes int32,
	fn func(ctx context.Context, tx biz.RouterTx, group *biz.AggGroup) error,
) (bool, error) {
	closed := false
	err := WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		var (
			g       biz.AggGroup
			samples []byte
		)
		err := tx.QueryRow(ctx,
			`UPDATE notify_agg_group SET status = 2,
			   suppressed_until = CASE WHEN count > $2 THEN now() + make_interval(mins => $3) ELSE NULL END,
			   update_time = now()
			 WHERE group_key = $1 AND status = 1 AND window_until < now()
			 RETURNING group_key, biz_id, event_type, route_id::text, first_event_id::text, first_event_time, count, samples,
			           (suppressed_until IS NOT NULL)`,
			key, threshold, suppressMinutes).Scan(&g.Key, &g.BizID, &g.EventType, &g.RouteID, &g.FirstEventID, &g.FirstEventTime, &g.Count, &samples, &g.Suppressed)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil // 已被他人关闭或窗口被延长
		}
		if err != nil {
			return fmt.Errorf("close agg group: %w", err)
		}
		if err := json.Unmarshal(samples, &g.Samples); err != nil {
			return fmt.Errorf("decode agg samples: %w", err)
		}
		closed = true
		return fn(ctx, &routerTx{tx: tx}, &g)
	})
	return closed, err
}
