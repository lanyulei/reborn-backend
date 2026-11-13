package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lanyulei/reborn/backend/kit/dbx"
	"github.com/lanyulei/reborn/backend/kit/notifycontract"
	"github.com/lanyulei/reborn/backend/notify/internal/biz"

	"github.com/jackc/pgx/v5"
)

var _ biz.IngressRepo = (*ingressRepo)(nil)

// ingressRepo 接入写仓储。
type ingressRepo struct {
	data *Data
}

var _ biz.ReplayStore = (*ingressRepo)(nil)

// RegisterReplay implements biz.ReplayStore：(生产者, 签名) 首次登记返回 true；
// 已存在（任一副本受理过）返回 false。主键冲突即重放，单语句原子。
func (r *ingressRepo) RegisterReplay(ctx context.Context, producerID, signature string, until time.Time) (bool, error) {
	tag, err := r.data.Pool.Exec(ctx,
		`INSERT INTO notify_ingress_replay (producer_id, signature, expire_time) VALUES ($1, $2, $3)
		 ON CONFLICT (producer_id, signature) DO NOTHING`, producerID, signature, until)
	if err != nil {
		return false, fmt.Errorf("register ingress replay: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// ForgetReplay implements biz.ReplayStore：撤销登记（请求未被受理，允许原样重发）。
func (r *ingressRepo) ForgetReplay(ctx context.Context, producerID, signature string) error {
	if _, err := r.data.Pool.Exec(ctx,
		"DELETE FROM notify_ingress_replay WHERE producer_id = $1 AND signature = $2", producerID, signature); err != nil {
		return fmt.Errorf("forget ingress replay: %w", err)
	}
	return nil
}

// NewIngressRepo new an ingressRepo.
func NewIngressRepo(data *Data) *ingressRepo { return &ingressRepo{data: data} }

// SaveEvents implements biz.IngressRepo：单事务两轮 pgx.Batch（各一次往返）——
// 第一轮全部 dedup 门闩（单语句 upsert RETURNING：过期行被新值替换；命中返回首次
// event_id，== 本次 id 才是胜者），第二轮胜者事件插入；最后消费配额。此前逐事件
// 两条语句串行：500 条批 = 1000 次往返，5s 请求超时内放不下。
func (r *ingressRepo) SaveEvents(ctx context.Context, producer *biz.Producer, events []*biz.Event, dedupTTL time.Duration) ([]biz.IngressResult, error) {
	results := make([]biz.IngressResult, len(events))
	err := dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		winners := make([]bool, len(events))
		gate := &pgx.Batch{}
		gated := make([]int, 0, len(events))
		for i, ev := range events {
			if ev.DedupKey == "" {
				winners[i] = true
				continue
			}
			gate.Queue(notifycontract.DedupGateSQL, ev.BizID, ev.DedupKey, ev.ID, dedupTTL)
			gated = append(gated, i)
		}
		if len(gated) > 0 {
			res := tx.SendBatch(ctx, gate)
			for _, i := range gated {
				var winner string
				if err := res.QueryRow().Scan(&winner); err != nil {
					_ = res.Close()
					return fmt.Errorf("dedup gate: %w", err)
				}
				if winner == events[i].ID {
					winners[i] = true
				} else {
					results[i] = biz.IngressResult{EventID: winner, Deduplicated: true}
				}
			}
			if err := res.Close(); err != nil {
				return fmt.Errorf("dedup gate batch: %w", err)
			}
		}
		ins := &pgx.Batch{}
		inserted := 0
		for i, ev := range events {
			if !winners[i] {
				continue
			}
			recipients, err := encodeRecipients(ev.Recipients)
			if err != nil {
				return err
			}
			// 与 core 共用 notifycontract.InsertEventSQL（同一列集与可空口径：dedup/group
			// 空串、payload/recipients 空集落 NULL；时间列取数据库 now()）。
			ins.Queue(notifycontract.InsertEventSQL,
				ev.ID, ev.BizID, ev.EventType, ev.Severity, ev.Title, dbx.NilIfEmptyBytes(ev.Payload), dbx.NilIfEmpty(ev.DedupKey), dbx.NilIfEmpty(ev.GroupKey),
				recipients, ev.Source, ev.Status, ev.PublishState, ev.PublishAttempts)
			results[i] = biz.IngressResult{EventID: ev.ID}
			inserted++
		}
		if inserted > 0 {
			res := tx.SendBatch(ctx, ins)
			for range inserted {
				if _, err := res.Exec(); err != nil {
					_ = res.Close()
					return fmt.Errorf("insert event: %w", err)
				}
			}
			if err := res.Close(); err != nil {
				return fmt.Errorf("insert event batch: %w", err)
			}
		}
		// 配额放在事务末尾消费：其 upsert 的行锁 (producer_id, window_kind) 只跨提交，
		// 而非覆盖整段门闩 + 插入——否则同一生产者的并发请求全部串行（一次一个在途）。
		// 超限整体回滚（门闩与事件行一并撤销）。
		// 每秒窗口按请求内全部事件计（限流口径：重放 / 重复提交同样占用处理能力，
		// 不留绕过口子）；每日窗口只计真正落库的事件——被 dedup 门闩去重的重试
		// 不应消耗当天额度。
		return consumeQuota(ctx, tx, producer, len(events), inserted)
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}

// encodeRecipients 显式收件 JSON（空集 → NULL）。
func encodeRecipients(recipients []string) (any, error) {
	if len(recipients) == 0 {
		return nil, nil
	}
	raw, err := json.Marshal(recipients)
	if err != nil {
		return nil, fmt.Errorf("encode recipients: %w", err)
	}
	return raw, nil
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
func consumeQuota(ctx context.Context, tx pgx.Tx, producer *biz.Producer, requested, stored int) error {
	if producer == nil {
		return nil
	}
	windows := []struct {
		kind  int16
		limit int32
		start string
		n     int
	}{
		{quotaWindowSecond, producer.QPSLimit, "date_trunc('second', now())", requested},
		{quotaWindowDay, producer.DailyLimit, "date_trunc('day', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'", stored},
	}
	for _, w := range windows {
		if w.limit <= 0 || w.n <= 0 {
			continue
		}
		n := w.n
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
