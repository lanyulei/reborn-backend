package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/lanyulei/reborn/backend/notify/internal/biz"
	"github.com/lanyulei/reborn/backend/notify/internal/conf"

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
// 发布失败不改 publish_state（按 relay 进程内的连续失败轮数退避；不动事件的
// publish_attempts——那是 router 的路由重试预算），NATS 不可用时行停留在未发布态，
// 恢复后自动续发。
//
// 多副本互斥靠认领租约（relay_claim_until，迁移 0046）而不是行锁：一条短语句认领一批
// 行（盖租约，FOR UPDATE SKIP LOCKED 只在该语句内防并发认领），发布与等待 ack 期间
// 不持有任何事务或连接，成功后再用一条短语句落账并清租约。租约到期仍未落账的行
// （relay 崩溃 / 落账语句失败）可被再次认领，以同一去重键重发——at-least-once 不变。
// 落账带围栏（认领时读到的代际：事件 publish_attempts；任务 attempts + 到期时刻）：
// 等 ack 期间行被消费侧推进到下一代际（旧消息在途）时不落账，下一代际照常重新发布。
//
// 发布成功时把到期列（next_publish_time / next_retry_time）盖成 now()：超期判定从
// "上次发布时刻"起算，而不是原到期时刻——否则积压超过 staleAfter 的行刚发布就被判超期，
// 回拨 → 重发 → 再回拨，relay 空转。
type natsRelay struct {
	data *Data
	nb   *NatsBackbone
	log  *slog.Logger
	// staleAfter 已发布行视为"消息已丢"的时限（2×AckWait：至少经历一次完整重投周期）。
	staleAfter time.Duration
	// eventFailStreak / dispatchFailStreak 发布连续全失败的轮数（退避指数；任一条成功即清零）。
	eventFailStreak    atomic.Int32
	dispatchFailStreak atomic.Int32
}

// NewRelayer 构造 relay（pg 骨干返回 nil：relay 角色为 no-op）。
func NewRelayer(c *conf.Pipeline, d *Data, nb *NatsBackbone, logger *slog.Logger) biz.Relayer {
	if nb == nil {
		return nil
	}
	_ = c
	return &natsRelay{data: d, nb: nb, log: logger, staleAfter: 2 * nb.opts.AckWait}
}

// RelayOnce implements biz.Relayer：一轮 = 事件发布 + 任务发布 + 超期回拨；返回发布条数
// （回拨不计：回拨的行下一轮才发布，只做了回拨的一轮按空闲处理、照常休眠）。
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
	_, err = r.requeueStale(ctx, batch)
	return n1 + n2, err
}

// relayClaimLease 认领租约：须覆盖 publishAckWait 加落账语句；过长则 relay 崩溃后
// 被认领的行要等更久才重发。租约内没落账而被他人重新认领只会造成同键重复发布
// （JetStream 去重窗口吸收，消费侧幂等兜底）。
const relayClaimLease = 3 * publishAckWait

// relayItem 一条被认领的待发布行（围栏字段 = 认领时读到的代际）。
type relayItem struct {
	cursor
	subject  string
	msgID    string
	attempts int32     // 事件 publish_attempts / 任务 attempts
	due      time.Time // 认领时的到期时刻（事件 next_publish_time / 任务 next_retry_time）
}

// relayRound 一轮「认领 → 发布并等 ack → 落账」：claim 与 settle 各自是独立的短语句，
// 两者之间（NATS 往返，至多 wait）不持有任何事务。返回发布成功条数。
func relayRound(ctx context.Context, wait time.Duration,
	claim func(ctx context.Context) ([]relayItem, error),
	publish func(it relayItem) (jetstream.PubAckFuture, error),
	settle func(ctx context.Context, ok, failed []relayItem, firstErr error) error,
) (int, error) {
	items, err := claim(ctx)
	if err != nil || len(items) == 0 {
		return 0, err
	}
	// 先整批异步发布，再以一个共享时限等全部 ack：一轮耗时上限为 wait，而不是逐条
	// wait × 批大小。
	futures := make([]jetstream.PubAckFuture, len(items))
	errs := make([]error, len(items))
	for i, it := range items {
		futures[i], errs[i] = publish(it)
	}
	awaitAll(ctx, futures, errs, wait)
	var ok, failed []relayItem
	var firstErr error
	for i, it := range items {
		if errs[i] == nil {
			ok = append(ok, it)
			continue
		}
		failed = append(failed, it)
		if firstErr == nil {
			firstErr = errs[i]
		}
	}
	return len(ok), settle(ctx, ok, failed, firstErr)
}

// failBackoff 推进连续失败计数并给出本轮失败行的退避：有成功即清零；整轮全失败才
// 累加（部分失败按当前计数退避，不升级）。
func failBackoff(streak *atomic.Int32, anyOK bool) time.Duration {
	if anyOK {
		streak.Store(0)
		return publishBackoff(0)
	}
	return publishBackoff(streak.Add(1) - 1)
}

// fenceArgs 把一批行拆成落账语句的 unnest 实参（id / 分区键 / 围栏代际 / 围栏到期时刻）。
func fenceArgs(items []relayItem) (ids []string, cts []time.Time, attempts []int32, dues []time.Time) {
	ids, cts = make([]string, len(items)), make([]time.Time, len(items))
	attempts, dues = make([]int32, len(items)), make([]time.Time, len(items))
	for i, it := range items {
		ids[i], cts[i], attempts[i], dues[i] = it.ID, it.CreateTime, it.attempts, it.due
	}
	return ids, cts, attempts, dues
}

// claimRows 执行认领语句并扫描（列序：id, 路由键, create_time, attempts, 到期时刻），
// 按到期时刻排序（UPDATE … RETURNING 不保证顺序；保持"先到期先发布"）。
func (r *natsRelay) claimRows(ctx context.Context, sql string, batch int, build func(it *relayItem, key string)) ([]relayItem, error) {
	rows, err := r.data.Pool.Query(ctx, sql, batch, relayClaimLease)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []relayItem
	for rows.Next() {
		var (
			it  relayItem
			key string
		)
		if err := rows.Scan(&it.ID, &key, &it.CreateTime, &it.attempts, &it.due); err != nil {
			return nil, err
		}
		build(&it, key)
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	slices.SortStableFunc(items, func(a, b relayItem) int { return a.due.Compare(b.due) })
	return items, nil
}

func (r *natsRelay) publishItem(it relayItem) (jetstream.PubAckFuture, error) {
	return r.publish(context.Background(), it.subject, it.cursor, it.msgID)
}

func (r *natsRelay) publishEvents(ctx context.Context, batch int) (int, error) {
	claim := func(ctx context.Context) ([]relayItem, error) {
		items, err := r.claimRows(ctx,
			`WITH c AS (
			   SELECT id, create_time FROM notify_event
			   WHERE publish_state = 0 AND next_publish_time <= now()
			     AND (relay_claim_until IS NULL OR relay_claim_until <= now())
			   ORDER BY next_publish_time LIMIT $1 FOR UPDATE SKIP LOCKED)
			 UPDATE notify_event e SET relay_claim_until = now() + $2::interval
			 FROM c WHERE e.id = c.id AND e.create_time = c.create_time
			 RETURNING e.id::text, e.biz_id, e.create_time, e.publish_attempts, e.next_publish_time`,
			batch, func(it *relayItem, bizID string) {
				it.subject, it.msgID = "notify.event."+bizID, msgID(it.ID, it.attempts)
			})
		if err != nil {
			return nil, fmt.Errorf("claim unpublished events: %w", err)
		}
		return items, nil
	}
	settle := func(ctx context.Context, ok, failed []relayItem, firstErr error) error {
		if len(ok) > 0 {
			ids, cts, attempts, _ := fenceArgs(ok)
			if _, err := r.data.Pool.Exec(ctx,
				`UPDATE notify_event e SET publish_state = 1, next_publish_time = now(), relay_claim_until = NULL
				 FROM unnest($1::uuid[], $2::timestamptz[], $3::int[]) AS m(id, create_time, attempts)
				 WHERE e.id = m.id AND e.create_time = m.create_time AND e.publish_state = 0 AND e.publish_attempts = m.attempts`,
				ids, cts, attempts); err != nil {
				return fmt.Errorf("mark events published: %w", err)
			}
		}
		if len(failed) > 0 {
			r.log.WarnContext(ctx, "publish events failed", "count", len(failed), "first_event_id", failed[0].ID, "error", firstErr)
			// 事件的去重键不含到期时刻：直接推后 next_publish_time 并归还认领。
			ids, cts, attempts, _ := fenceArgs(failed)
			if _, err := r.data.Pool.Exec(ctx,
				`UPDATE notify_event e SET next_publish_time = now() + $4, relay_claim_until = NULL
				 FROM unnest($1::uuid[], $2::timestamptz[], $3::int[]) AS m(id, create_time, attempts)
				 WHERE e.id = m.id AND e.create_time = m.create_time AND e.publish_state = 0 AND e.publish_attempts = m.attempts`,
				ids, cts, attempts, failBackoff(&r.eventFailStreak, len(ok) > 0)); err != nil {
				return fmt.Errorf("defer event publish: %w", err)
			}
		} else if len(ok) > 0 {
			r.eventFailStreak.Store(0)
		}
		return nil
	}
	return relayRound(ctx, publishAckWait, claim, r.publishItem, settle)
}

func (r *natsRelay) publishDispatches(ctx context.Context, batch int) (int, error) {
	claim := func(ctx context.Context) ([]relayItem, error) {
		items, err := r.claimRows(ctx,
			`WITH c AS (
			   SELECT id, create_time FROM notify_dispatch
			   WHERE publish_state = 0 AND status IN (1, 4) AND next_retry_time <= now()
			     AND (relay_claim_until IS NULL OR relay_claim_until <= now())
			   ORDER BY next_retry_time LIMIT $1 FOR UPDATE SKIP LOCKED)
			 UPDATE notify_dispatch d SET relay_claim_until = now() + $2::interval
			 FROM c WHERE d.id = c.id AND d.create_time = c.create_time
			 RETURNING d.id::text, d.channel_id::text, d.create_time, d.attempts, d.next_retry_time`,
			batch, func(it *relayItem, channelID string) {
				it.ChannelID = channelID
				it.subject, it.msgID = "notify.dispatch."+channelID, dispatchMsgID(it.ID, it.attempts, it.due)
			})
		if err != nil {
			return nil, fmt.Errorf("claim unpublished dispatches: %w", err)
		}
		return items, nil
	}
	const fenced = ` FROM unnest($1::uuid[], $2::timestamptz[], $3::int[], $4::timestamptz[]) AS m(id, create_time, attempts, due)
		 WHERE d.id = m.id AND d.create_time = m.create_time AND d.publish_state = 0 AND d.attempts = m.attempts AND d.next_retry_time = m.due`
	settle := func(ctx context.Context, ok, failed []relayItem, firstErr error) error {
		if len(ok) > 0 {
			ids, cts, attempts, dues := fenceArgs(ok)
			if _, err := r.data.Pool.Exec(ctx,
				"UPDATE notify_dispatch d SET publish_state = 1, next_retry_time = now(), relay_claim_until = NULL"+fenced,
				ids, cts, attempts, dues); err != nil {
				return fmt.Errorf("mark dispatches published: %w", err)
			}
		}
		if len(failed) > 0 {
			r.log.WarnContext(ctx, "publish dispatches failed", "count", len(failed), "first_dispatch_id", failed[0].ID, "error", firstErr)
			// 任务的去重键含到期时刻（ack 超时但消息其实已入流时，重发须同键才能被去重），
			// 所以不动 next_retry_time：把认领租约延长为退避时长，到期后原样重发。
			ids, cts, attempts, dues := fenceArgs(failed)
			if _, err := r.data.Pool.Exec(ctx,
				"UPDATE notify_dispatch d SET relay_claim_until = now() + $5"+fenced,
				ids, cts, attempts, dues, failBackoff(&r.dispatchFailStreak, len(ok) > 0)); err != nil {
				return fmt.Errorf("defer dispatch publish: %w", err)
			}
		} else if len(ok) > 0 {
			r.dispatchFailStreak.Store(0)
		}
		return nil
	}
	return relayRound(ctx, publishAckWait, claim, r.publishItem, settle)
}

// requeueStale 已发布但超期仍未被处理的行回拨为未发布（消息丢失 / MaxDeliver 耗尽 /
// 消费者长期离线后恢复）；下一轮按正常路径重新发布。超期从到期列起算——发布成功时
// 该列被盖成发布时刻，router/sender 延后时为其退避时刻。事件侧谓词用字面量
// status = 1（received）以命中部分索引 idx_notify_event_published（迁移 0044）；
// 任务侧由 idx_notify_dispatch_retry（next_retry_time WHERE status IN (1,4)）覆盖。
func (r *natsRelay) requeueStale(ctx context.Context, batch int) (int, error) {
	total := 0
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE notify_event SET publish_state = 0, relay_claim_until = NULL WHERE (id, create_time) IN (
		   SELECT id, create_time FROM notify_event
		   WHERE publish_state = 1 AND status = 1 AND next_publish_time < now() - $1::interval
		   ORDER BY next_publish_time LIMIT $2 FOR UPDATE SKIP LOCKED)`,
		r.staleAfter, batch)
	if err != nil {
		return 0, fmt.Errorf("requeue stale events: %w", err)
	}
	total += int(tag.RowsAffected())
	tag, err = r.data.Pool.Exec(ctx,
		`UPDATE notify_dispatch SET publish_state = 0, relay_claim_until = NULL WHERE (id, create_time) IN (
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

// msgID 发布去重键：行 id + 代际（事件 publish_attempts）——同一代际的重复发布被窗口
// 去重，代际推进后的重发不受影响。
func msgID(id string, generation int32) string {
	return id + ":" + strconv.FormatInt(int64(generation), 10)
}

// dispatchMsgID 任务去重键：行 id + attempts + 认领时读到的到期时刻。放回（Release /
// 渠道暂停）不推进 attempts 但会改到期时刻并回拨 publish_state，去重窗口内的重新发布
// 须是新键；发布后未落账（崩溃 / ack 超时）的重发读到同一到期时刻 → 同键 → 仍被去重。
func dispatchMsgID(id string, attempts int32, due time.Time) string {
	return msgID(id, attempts) + ":" + strconv.FormatInt(due.UnixMicro(), 10)
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

// publishAckWait 一批发布等待 ack 的共享时限（期间不持有事务；认领租约须长于它）。
const publishAckWait = 5 * time.Second

var errPublishAckTimeout = errors.New("publish ack timeout")

// awaitAll 以一个共享时限等待整批发布 ack，结果写入 errs（发布本身已失败的下标
// 跳过）。时限到后其余 future 只做一次非阻塞检查：总等待 ≤ wait，与批大小无关。
func awaitAll(ctx context.Context, futures []jetstream.PubAckFuture, errs []error, wait time.Duration) {
	deadline := time.NewTimer(wait)
	defer deadline.Stop()
	expired := false
	for i, f := range futures {
		if errs[i] != nil || f == nil {
			if errs[i] == nil {
				errs[i] = errPublishAckTimeout
			}
			continue
		}
		if expired {
			select {
			case <-f.Ok():
			case errs[i] = <-f.Err():
			default:
				errs[i] = errPublishAckTimeout
			}
			continue
		}
		select {
		case <-f.Ok():
		case errs[i] = <-f.Err():
		case <-deadline.C:
			expired = true
			errs[i] = errPublishAckTimeout
		case <-ctx.Done():
			expired = true
			errs[i] = ctx.Err()
		}
	}
}

// awaitPub 等待单条发布 ack（上限 publishAckWait）。
func awaitPub(ctx context.Context, f jetstream.PubAckFuture) error {
	errs := make([]error, 1)
	awaitAll(ctx, []jetstream.PubAckFuture{f}, errs, publishAckWait)
	return errs[0]
}

// publishBackoff 发布失败退避：5s · 2^n（n = relay 连续失败轮数），上限 5 分钟。
func publishBackoff(attempts int32) time.Duration {
	shift := min(max(attempts, 0), 6)
	return min(time.Duration(1<<shift)*5*time.Second, 5*time.Minute)
}
