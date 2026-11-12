package data

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lanyulei/reborn/backend/kit/dbx"
	"github.com/lanyulei/reborn/backend/notify/internal/biz"
	"github.com/lanyulei/reborn/backend/notify/internal/conf"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go/jetstream"
)

var _ biz.EventSource = (*natsEventSource)(nil)

// natsEventSource NATS 骨干事件源：从 EVENTS 流拉一批游标 → 单事务按主键锁定仍为
// received 的行 → 回调 router（与 PG 路径同一 routerTx 原语）→ 提交 → ack。
//   - 行已非 received（重投 / 他副本已处理）→ 直接 ack（幂等门闩，设计 §4.4 步 5）；
//   - 行被他人锁定或未到期 → NakWithDelay 短延后；
//   - 回调出错 → 整批回滚 + NakWithDelay（事件仍 received，PG 兜底）；
//   - router 延后（DeferEvent）的事件 → 按其退避 NakWithDelay。
type natsEventSource struct {
	data *Data
	nb   *NatsBackbone
	poll time.Duration
}

// NewEventSource 按骨干选择事件源（pg → eventRepo；nats → natsEventSource）。
func NewEventSource(c *conf.Pipeline, pg *eventRepo, nb *NatsBackbone) biz.EventSource {
	if nb == nil {
		return pg
	}
	poll := c.GetRouter().GetPollInterval().AsDuration()
	if poll <= 0 {
		poll = 200 * time.Millisecond
	}
	return &natsEventSource{data: pg.data, nb: nb, poll: poll}
}

// ProcessPending implements biz.EventSource.
func (s *natsEventSource) ProcessPending(ctx context.Context, limit int, fn func(ctx context.Context, tx biz.RouterTx, events []*biz.Event) error) (int, error) {
	msgs, err := fetch(ctx, s.nb.router, limit, s.poll)
	if err != nil || len(msgs) == 0 {
		return 0, err
	}
	byID := make(map[string]jetstream.Msg, len(msgs))
	ids := make([]string, 0, len(msgs))
	var minCreate time.Time
	for _, m := range msgs {
		var c cursor
		if err := json.Unmarshal(m.Data(), &c); err != nil || c.ID == "" {
			_ = m.Term() // 畸形消息：终止重投（PG 行若存在由 relay 超期重发）
			continue
		}
		if prev, dup := byID[c.ID]; dup {
			_ = prev.Ack()
		}
		byID[c.ID] = m
		ids = append(ids, c.ID)
		if minCreate.IsZero() || c.CreateTime.Before(minCreate) {
			minCreate = c.CreateTime
		}
	}
	if len(ids) == 0 {
		return 0, nil
	}
	deferred := map[string]time.Time{}
	var processed []string
	err = dbx.WithTx(ctx, s.data.Pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx,
			"SELECT "+eventColumns+" FROM notify_event WHERE id = ANY($1::uuid[]) AND create_time >= $2"+
				" AND status = $3 AND next_publish_time <= now() ORDER BY next_publish_time FOR UPDATE SKIP LOCKED",
			ids, minCreate.Add(-time.Hour), biz.EventStatusReceived)
		if err != nil {
			return fmt.Errorf("lock events: %w", err)
		}
		var events []*biz.Event
		for rows.Next() {
			ev, err := scanEvent(rows.Scan)
			if err != nil {
				rows.Close()
				return fmt.Errorf("scan event: %w", err)
			}
			events = append(events, ev)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, ev := range events {
			processed = append(processed, ev.ID)
		}
		if len(events) == 0 {
			return nil
		}
		return fn(ctx, &natsRouterTx{routerTx: routerTx{tx: tx}, deferred: deferred}, events)
	})
	if err != nil {
		for _, m := range byID {
			nakAfter(m, 5*time.Second)
		}
		return 0, err
	}
	seen := make(map[string]struct{}, len(processed))
	for _, id := range processed {
		seen[id] = struct{}{}
		m := byID[id]
		if next, ok := deferred[id]; ok {
			nakAfter(m, time.Until(next))
			continue
		}
		_ = m.Ack()
	}
	// 未被锁定的：已终态 → ack；仍 received（他人持有 / 未到期）→ 短延后重投。
	var missed []string
	for _, id := range ids {
		if _, ok := seen[id]; !ok {
			missed = append(missed, id)
		}
	}
	if len(missed) > 0 {
		s.settleMissed(ctx, missed, byID)
	}
	return len(processed), nil
}

// settleMissed 对本批未锁定到的游标按行当前状态定夺。
func (s *natsEventSource) settleMissed(ctx context.Context, ids []string, byID map[string]jetstream.Msg) {
	rows, err := s.data.Pool.Query(ctx,
		"SELECT id::text, status, next_publish_time FROM notify_event WHERE id = ANY($1::uuid[])", ids)
	if err != nil {
		for _, id := range ids {
			nakAfter(byID[id], 2*time.Second)
		}
		return
	}
	defer rows.Close()
	found := map[string]struct{}{}
	for rows.Next() {
		var (
			id     string
			status int16
			next   time.Time
		)
		if err := rows.Scan(&id, &status, &next); err != nil {
			continue
		}
		found[id] = struct{}{}
		m := byID[id]
		if int32(status) != biz.EventStatusReceived {
			_ = m.Ack() // 已被处理（重投）
			continue
		}
		nakAfter(m, max(time.Until(next), 2*time.Second))
	}
	for _, id := range ids {
		if _, ok := found[id]; !ok {
			_ = byID[id].Ack() // 行已不存在（保留期清理）
		}
	}
}

// natsRouterTx 在 PG routerTx 之上记录 DeferEvent 的延后时刻（提交后按此 Nak）。
type natsRouterTx struct {
	routerTx
	deferred map[string]time.Time
}

// DeferEvent implements biz.RouterTx.
func (t *natsRouterTx) DeferEvent(ctx context.Context, id string, createTime time.Time, next time.Time) error {
	if err := t.routerTx.DeferEvent(ctx, id, createTime, next); err != nil {
		return err
	}
	t.deferred[id] = next
	return nil
}
