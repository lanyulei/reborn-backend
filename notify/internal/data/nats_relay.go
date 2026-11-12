package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/lanyulei/reborn/backend/kit/dbx"
	"github.com/lanyulei/reborn/backend/notify/internal/biz"
	"github.com/lanyulei/reborn/backend/notify/internal/conf"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var _ biz.Relayer = (*natsRelay)(nil)

// natsRelay 发布扫描器（设计 §4.3）：唯一的 JetStream 发布者。
//   - 事件：publish_state=0 且到期 → 发布 notify.event.<biz> → publish_state=1；
//   - 任务：publish_state=0 且 status IN (1,4) 且到期 → 发布 notify.dispatch.<channel> → publish_state=1；
//   - 超期重发：已发布但长期未被处理（消息丢失 / MaxDeliver 耗尽）的行回拨 publish_state=0
//     由下一轮重新发布——PG 行永不孤儿。
//
// 发布失败不改 publish_state（事件另记 publish_attempts 并退避），NATS 不可用时行停留在
// 未发布态，恢复后自动续发。多副本靠 FOR UPDATE SKIP LOCKED 互斥。
type natsRelay struct {
	data *Data
	nb   *NatsBackbone
	log  *slog.Logger
	// staleAfter 已发布行视为"消息已丢"的时限（2×AckWait：至少经历一次完整重投周期）。
	staleAfter time.Duration
}

// NewRelayer 构造 relay（pg 骨干返回 nil：relay 角色为 no-op）。
func NewRelayer(c *conf.Pipeline, d *Data, nb *NatsBackbone, logger *slog.Logger) biz.Relayer {
	if nb == nil {
		return nil
	}
	_ = c
	return &natsRelay{data: d, nb: nb, log: logger, staleAfter: 2 * nb.opts.AckWait}
}

// RelayOnce implements biz.Relayer：一轮 = 事件发布 + 任务发布 + 超期回拨；返回处理条数。
func (r *natsRelay) RelayOnce(ctx context.Context, batch int) (int, error) {
	if batch <= 0 {
		batch = r.nb.opts.RelayBatch
	}
	n1, err := r.publishEvents(ctx, batch)
	if err != nil {
		return n1, err
	}
	n2, err := r.publishDispatches(ctx, batch)
	if err != nil {
		return n1 + n2, err
	}
	n3, err := r.requeueStale(ctx, batch)
	return n1 + n2 + n3, err
}

func (r *natsRelay) publishEvents(ctx context.Context, batch int) (int, error) {
	n := 0
	err := dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx,
			`SELECT id::text, biz_id, create_time, publish_attempts FROM notify_event
			 WHERE publish_state = 0 AND next_publish_time <= now()
			 ORDER BY next_publish_time LIMIT $1 FOR UPDATE SKIP LOCKED`, batch)
		if err != nil {
			return fmt.Errorf("scan unpublished events: %w", err)
		}
		type row struct {
			id, biz  string
			ct       time.Time
			attempts int32
		}
		var items []row
		for rows.Next() {
			var it row
			if err := rows.Scan(&it.id, &it.biz, &it.ct, &it.attempts); err != nil {
				rows.Close()
				return err
			}
			items = append(items, it)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if len(items) == 0 {
			return nil
		}
		futures := make([]jetstream.PubAckFuture, len(items))
		for i, it := range items {
			futures[i], err = r.publish(ctx, "notify.event."+it.biz, cursor{ID: it.id, CreateTime: it.ct}, msgID(it.id, it.attempts))
			if err != nil {
				futures[i] = nil
				r.log.WarnContext(ctx, "publish event failed", "event_id", it.id, "error", err)
			}
		}
		var ok, failed []string
		var failedAttempts []int32
		for i, f := range futures {
			if f != nil && awaitPub(ctx, f) == nil {
				ok = append(ok, items[i].id)
				continue
			}
			failed = append(failed, items[i].id)
			failedAttempts = append(failedAttempts, items[i].attempts)
		}
		if len(ok) > 0 {
			if _, err := tx.Exec(ctx, "UPDATE notify_event SET publish_state = 1 WHERE id = ANY($1::uuid[]) AND publish_state = 0", ok); err != nil {
				return fmt.Errorf("mark events published: %w", err)
			}
		}
		for i, id := range failed {
			if _, err := tx.Exec(ctx,
				"UPDATE notify_event SET publish_attempts = publish_attempts + 1, next_publish_time = now() + $2 WHERE id = $1 AND publish_state = 0",
				id, publishBackoff(failedAttempts[i])); err != nil {
				return fmt.Errorf("defer event publish: %w", err)
			}
		}
		n = len(ok)
		return nil
	})
	return n, err
}

func (r *natsRelay) publishDispatches(ctx context.Context, batch int) (int, error) {
	n := 0
	err := dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx,
			`SELECT id::text, channel_id::text, create_time, attempts FROM notify_dispatch
			 WHERE publish_state = 0 AND status IN (1, 4) AND next_retry_time <= now()
			 ORDER BY next_retry_time LIMIT $1 FOR UPDATE SKIP LOCKED`, batch)
		if err != nil {
			return fmt.Errorf("scan unpublished dispatches: %w", err)
		}
		type item struct {
			cursor
			attempts int32
		}
		var items []item
		for rows.Next() {
			var it item
			if err := rows.Scan(&it.ID, &it.ChannelID, &it.CreateTime, &it.attempts); err != nil {
				rows.Close()
				return err
			}
			items = append(items, it)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if len(items) == 0 {
			return nil
		}
		var ok []string
		for _, it := range items {
			f, err := r.publish(ctx, "notify.dispatch."+it.ChannelID, it.cursor, msgID(it.ID, it.attempts))
			if err != nil || awaitPub(ctx, f) != nil {
				r.log.WarnContext(ctx, "publish dispatch failed", "dispatch_id", it.ID, "error", err)
				continue // 行保持未发布，下一轮重试
			}
			ok = append(ok, it.ID)
		}
		if len(ok) > 0 {
			if _, err := tx.Exec(ctx, "UPDATE notify_dispatch SET publish_state = 1 WHERE id = ANY($1::uuid[]) AND publish_state = 0", ok); err != nil {
				return fmt.Errorf("mark dispatches published: %w", err)
			}
		}
		n = len(ok)
		return nil
	})
	return n, err
}

// requeueStale 已发布但超期仍未被处理的行回拨为未发布（消息丢失 / MaxDeliver 耗尽 /
// 消费者长期离线后恢复）；下一轮按正常路径重新发布。
func (r *natsRelay) requeueStale(ctx context.Context, batch int) (int, error) {
	total := 0
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE notify_event SET publish_state = 0 WHERE (id, create_time) IN (
		   SELECT id, create_time FROM notify_event
		   WHERE publish_state = 1 AND status = $1 AND next_publish_time < now() - $2::interval
		   ORDER BY next_publish_time LIMIT $3 FOR UPDATE SKIP LOCKED)`,
		biz.EventStatusReceived, r.staleAfter, batch)
	if err != nil {
		return 0, fmt.Errorf("requeue stale events: %w", err)
	}
	total += int(tag.RowsAffected())
	tag, err = r.data.Pool.Exec(ctx,
		`UPDATE notify_dispatch SET publish_state = 0 WHERE (id, create_time) IN (
		   SELECT id, create_time FROM notify_dispatch
		   WHERE publish_state = 1 AND status IN (1, 4) AND next_retry_time < now() - $1::interval
		   ORDER BY next_retry_time LIMIT $2 FOR UPDATE SKIP LOCKED)`,
		r.staleAfter, batch)
	if err != nil {
		return total, fmt.Errorf("requeue stale dispatches: %w", err)
	}
	total += int(tag.RowsAffected())
	if total > 0 {
		r.log.WarnContext(ctx, "stale published rows requeued (message lost or max deliveries exhausted)", "count", total)
	}
	return total, nil
}

// msgID 发布去重键：行 id + 代际（事件 publish_attempts / 任务 attempts）——同一代际的
// 重复发布被窗口去重，代际推进后的重发不受影响。
func msgID(id string, generation int32) string {
	return id + ":" + strconv.FormatInt(int64(generation), 10)
}

func (r *natsRelay) publish(ctx context.Context, subject string, c cursor, id string) (jetstream.PubAckFuture, error) {
	raw, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	msg := &nats.Msg{Subject: subject, Data: raw, Header: nats.Header{}}
	msg.Header.Set(jetstream.MsgIDHeader, id)
	_ = ctx
	return r.nb.js.PublishMsgAsync(msg)
}

// awaitPub 等待单条发布 ack（上限 5s）。
func awaitPub(ctx context.Context, f jetstream.PubAckFuture) error {
	select {
	case <-f.Ok():
		return nil
	case err := <-f.Err():
		return err
	case <-time.After(5 * time.Second):
		return errors.New("publish ack timeout")
	case <-ctx.Done():
		return ctx.Err()
	}
}

// publishBackoff 发布失败退避：5s · 2^n，上限 5 分钟。
func publishBackoff(attempts int32) time.Duration {
	shift := min(max(attempts, 0), 6)
	return min(time.Duration(1<<shift)*5*time.Second, 5*time.Minute)
}
