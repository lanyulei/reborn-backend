package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lanyulei/reborn/backend/notify/internal/biz"

	"github.com/jackc/pgx/v5"
)

var _ biz.IngressRepo = (*ingressRepo)(nil)

// ingressRepo 接入写仓储。
type ingressRepo struct {
	data *Data
}

// NewIngressRepo new an ingressRepo.
func NewIngressRepo(data *Data) *ingressRepo { return &ingressRepo{data: data} }

// SaveEvents implements biz.IngressRepo：单事务；dedup 门闩单语句 upsert RETURNING
// （过期行被新值替换；命中则返回首次 event_id），返回值 == 本次 id 才插入事件。
func (r *ingressRepo) SaveEvents(ctx context.Context, producer *biz.Producer, events []*biz.Event, dedupTTL time.Duration) ([]biz.IngressResult, error) {
	results := make([]biz.IngressResult, 0, len(events))
	err := WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if err := consumeQuota(ctx, tx, producer, len(events)); err != nil {
			return err
		}
		for _, ev := range events {
			if ev.DedupKey != "" {
				var winner string
				if err := tx.QueryRow(ctx,
					`INSERT INTO notify_dedup (biz_id, dedup_key, event_id, expire_time) VALUES ($1, $2, $3, now() + $4)
					 ON CONFLICT (biz_id, dedup_key) DO UPDATE SET
					   event_id = CASE WHEN notify_dedup.expire_time < now() THEN EXCLUDED.event_id ELSE notify_dedup.event_id END,
					   expire_time = CASE WHEN notify_dedup.expire_time < now() THEN EXCLUDED.expire_time ELSE notify_dedup.expire_time END
					 RETURNING event_id::text`,
					ev.BizID, ev.DedupKey, ev.ID, dedupTTL).Scan(&winner); err != nil {
					return fmt.Errorf("dedup gate: %w", err)
				}
				if winner != ev.ID {
					results = append(results, biz.IngressResult{EventID: winner, Deduplicated: true})
					continue
				}
			}
			if err := insertEvent(ctx, tx, ev); err != nil {
				return err
			}
			results = append(results, biz.IngressResult{EventID: ev.ID})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}

// insertEvent 与 core insertEventPlan 同一列集与可空口径（dedup/group 空串、
// payload/recipients 空集落 NULL）；时间列取数据库 now()（与 router 到期判定
// 同一时钟源，见 insertDispatches）。
func insertEvent(ctx context.Context, tx pgx.Tx, ev *biz.Event) error {
	var recipients any
	if len(ev.Recipients) > 0 {
		raw, err := json.Marshal(ev.Recipients)
		if err != nil {
			return fmt.Errorf("encode recipients: %w", err)
		}
		recipients = raw
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO notify_event (id, biz_id, event_type, severity, title, payload, dedup_key, group_key, recipients,
		   source, status, publish_state, publish_attempts, next_publish_time, create_time)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,now(),now())`,
		ev.ID, ev.BizID, ev.EventType, ev.Severity, ev.Title, nilIfEmptyBytes(ev.Payload), nilIfEmpty(ev.DedupKey), nilIfEmpty(ev.GroupKey),
		recipients, ev.Source, ev.Status, ev.PublishState, ev.PublishAttempts); err != nil {
		return fmt.Errorf("insert event: %w", err)
	}
	return nil
}

// 配额窗口种类（notify_quota_counter.window_kind）。
const (
	quotaWindowSecond int16 = 1
	quotaWindowDay    int16 = 2
)

// consumeQuota 按生产者配额扣减 n 条：每个启用的窗口一条条件 upsert——首行经
// INSERT…SELECT…WHERE n ≤ limit 守卫（单批即超限亦拒绝）；已有行按窗口起点落后
// 即重置计数（新窗口）、否则累加，累加后超过上限则 WHERE 不满足；两条路径 0 行
// 均 → ErrQuotaExceeded（事务回滚，整批不落库）。多实例共享同一计数行，PG 行锁
// 保证原子。limit=0 的窗口不计数。
func consumeQuota(ctx context.Context, tx pgx.Tx, producer *biz.Producer, n int) error {
	if producer == nil {
		return nil
	}
	windows := []struct {
		kind  int16
		limit int32
		start string
	}{
		{quotaWindowSecond, producer.QPSLimit, "date_trunc('second', now())"},
		{quotaWindowDay, producer.DailyLimit, "date_trunc('day', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'"},
	}
	for _, w := range windows {
		if w.limit <= 0 {
			continue
		}
		var count int
		err := tx.QueryRow(ctx,
			`INSERT INTO notify_quota_counter (producer_id, window_kind, window_start, count)
			 SELECT $1, $2, `+w.start+`, $3::int WHERE $3::int <= $4::int
			 ON CONFLICT (producer_id, window_kind) DO UPDATE SET
			   count = CASE WHEN notify_quota_counter.window_start < EXCLUDED.window_start THEN EXCLUDED.count
			                ELSE notify_quota_counter.count + EXCLUDED.count END,
			   window_start = GREATEST(notify_quota_counter.window_start, EXCLUDED.window_start)
			 WHERE (CASE WHEN notify_quota_counter.window_start < EXCLUDED.window_start THEN 0
			             ELSE notify_quota_counter.count END) + EXCLUDED.count <= $4::int
			 RETURNING count`,
			producer.ID, w.kind, n, w.limit).Scan(&count)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: window %d limit %d", biz.ErrQuotaExceeded, w.kind, w.limit)
		}
		if err != nil {
			return fmt.Errorf("consume quota: %w", err)
		}
	}
	return nil
}
