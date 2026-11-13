package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lanyulei/reborn/backend/notify/internal/biz"
	"github.com/lanyulei/reborn/backend/notify/internal/conf"

	"github.com/nats-io/nats.go/jetstream"
)

var _ biz.DispatchRepo = (*natsDispatchRepo)(nil)

// natsDispatchRepo NATS 骨干任务源：Claim 从 DISPATCHES 流拉游标，再用与 PG 路径完全
// 相同的条件 UPDATE 声明（status IN (1,4) 且到期 → sending，attempts+1 作围栏）；
// Finish 先走 PG 落账，再按结果 ack / NakWithDelay。"现在不发"（Release / 渠道暂停）
// 不 Nak——每次重投都消耗 MaxDeliver，限流/熔断稍久消息就死在流里——而是把行回拨为
// 未发布后 ack，到期由 relay 重新发布。消息只是"该行可能该发了"的提示，围栏与状态机
// 不变（ErrStaleClaim 语义不变）。
type natsDispatchRepo struct {
	*dispatchRepo
	nb   *NatsBackbone
	poll time.Duration

	mu       sync.Mutex
	inflight map[string]natsMsg // dispatch id → 本副本持有的游标消息
}

// NewDispatchSource 按骨干选择任务仓储（pg → dispatchRepo；nats → natsDispatchRepo）。
func NewDispatchSource(c *conf.Pipeline, pg *dispatchRepo, nb *NatsBackbone) biz.DispatchRepo {
	if nb == nil {
		return pg
	}
	poll := c.GetSender().GetPollInterval().AsDuration()
	if poll <= 0 {
		poll = 200 * time.Millisecond
	}
	return &natsDispatchRepo{dispatchRepo: pg, nb: nb, poll: poll, inflight: map[string]jetstream.Msg{}}
}

// Claim implements biz.DispatchRepo.
func (r *natsDispatchRepo) Claim(ctx context.Context, limit int, lease time.Duration, paused []string) ([]*biz.ClaimedDispatch, error) {
	msgs, err := fetch(ctx, r.nb.sender, limit, r.poll)
	if err != nil || len(msgs) == 0 {
		return nil, err
	}
	pausedSet := make(map[string]struct{}, len(paused))
	for _, id := range paused {
		pausedSet[id] = struct{}{}
	}
	byID := make(map[string]jetstream.Msg, len(msgs))
	ids := make([]string, 0, len(msgs))
	pausedMsgs := map[string]jetstream.Msg{}
	var minCreate time.Time // 分区剪枝下界（游标带 create_time）
	unbounded := false
	for _, m := range msgs {
		var c cursor
		if err := json.Unmarshal(m.Data(), &c); err != nil || c.ID == "" {
			_ = m.Term()
			continue
		}
		if c.CreateTime.IsZero() {
			unbounded = true // 游标缺 create_time：本批不剪枝
		} else if minCreate.IsZero() || c.CreateTime.Before(minCreate) {
			minCreate = c.CreateTime
		}
		if _, p := pausedSet[c.ChannelID]; p {
			// 渠道熔断/限流中：与 PG 路径的 paused 排除等价。
			if prev, dup := pausedMsgs[c.ID]; dup {
				_ = prev.Ack()
			}
			pausedMsgs[c.ID] = m
			continue
		}
		if prev, dup := byID[c.ID]; dup {
			_ = prev.Ack()
		}
		byID[c.ID] = m
		ids = append(ids, c.ID)
	}
	if unbounded {
		minCreate = time.Unix(0, 0)
	} else {
		minCreate = minCreate.Add(-time.Hour)
	}
	r.deferPaused(ctx, pausedMsgs, minCreate)
	if len(ids) == 0 {
		return nil, nil
	}
	// publish_state = 1：能被声明说明游标消息已投递到——relay 发布后不持锁等 ack，
	// 消费侧可能先于 relay 的落账语句声明该行（relay 落账带代际围栏，届时不再改动）。
	rows, err := r.data.Pool.Query(ctx,
		`UPDATE notify_dispatch d SET status = 2, attempts = d.attempts + 1, next_retry_time = now() + $2, publish_state = 1
		 WHERE d.id = ANY($1::uuid[]) AND d.create_time >= $3 AND d.status IN (1, 4) AND d.next_retry_time <= now()
		 RETURNING `+claimedColumns, ids, lease, minCreate)
	if err != nil {
		for _, m := range byID {
			nakAfter(m, 2*time.Second)
		}
		return nil, fmt.Errorf("claim dispatches: %w", err)
	}
	out := make([]*biz.ClaimedDispatch, 0, len(ids))
	claimed := map[string]struct{}{}
	for rows.Next() {
		d, err := scanClaimed(rows.Scan)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan claimed dispatch: %w", err)
		}
		out = append(out, d)
		claimed[d.ID] = struct{}{}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	for _, d := range out {
		r.inflight[d.ID] = byID[d.ID]
	}
	r.mu.Unlock()
	var missed []string
	for _, id := range ids {
		if _, ok := claimed[id]; !ok {
			missed = append(missed, id)
		}
	}
	if len(missed) > 0 {
		r.settleMissed(ctx, missed, byID, minCreate)
	}
	return out, nil
}

// pausedDelay 暂停渠道的游标延后时长。
const pausedDelay = 5 * time.Second

// deferPaused 暂停渠道的游标：把仍待发的行回拨为未发布并延后 pausedDelay，然后 ack——
// 到期由 relay 重新发布（到期时刻变了 → 新去重键）。不 Nak：每 5s 重投一次，暂停超过
// MaxDeliver×5s 消息就死在流里。未回拨到的（行非待发 / 写失败）仍按旧口径 Nak。
func (r *natsDispatchRepo) deferPaused(ctx context.Context, msgs map[string]jetstream.Msg, minCreate time.Time) {
	if len(msgs) == 0 {
		return
	}
	ids := make([]string, 0, len(msgs))
	for id := range msgs {
		ids = append(ids, id)
	}
	deferred := map[string]struct{}{}
	rows, err := r.data.Pool.Query(ctx,
		`UPDATE notify_dispatch SET publish_state = 0, relay_claim_until = NULL, next_retry_time = GREATEST(next_retry_time, now() + $3)
		 WHERE id = ANY($1::uuid[]) AND create_time >= $2 AND status IN (1, 4) AND publish_state = 1
		 RETURNING id::text`, ids, minCreate, pausedDelay)
	if err == nil {
		for rows.Next() {
			var id string
			if rows.Scan(&id) == nil {
				deferred[id] = struct{}{}
			}
		}
		rows.Close()
		if rows.Err() != nil {
			// 语句失败即整体未生效：全部按 Nak 处理。
			deferred = map[string]struct{}{}
		}
	}
	for id, m := range msgs {
		if _, ok := deferred[id]; ok {
			_ = m.Ack()
			continue
		}
		nakAfter(m, pausedDelay)
	}
}

// settleMissed 未声明到的游标：终态/不存在 → ack；sending（他人持有或崩溃租约，回收器
// 会转 failed）/ 未到期 → 延后到 next_retry_time 重投。
func (r *natsDispatchRepo) settleMissed(ctx context.Context, ids []string, byID map[string]jetstream.Msg, minCreate time.Time) {
	rows, err := r.data.Pool.Query(ctx,
		"SELECT id::text, status, COALESCE(next_retry_time, now()) FROM notify_dispatch WHERE id = ANY($1::uuid[]) AND create_time >= $2", ids, minCreate)
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
		switch int32(status) {
		case biz.DispatchSent, biz.DispatchDead:
			_ = m.Ack()
		default:
			nakAfter(m, max(time.Until(next), 2*time.Second))
		}
	}
	for _, id := range ids {
		if _, ok := found[id]; !ok {
			_ = byID[id].Ack()
		}
	}
}

// natsMsg 游标消息（测试构造 natsDispatchRepo 时也用）。
type natsMsg = jetstream.Msg

func (r *natsDispatchRepo) take(id string) (jetstream.Msg, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	m, ok := r.inflight[id]
	delete(r.inflight, id)
	return m, ok
}

// Finish implements biz.DispatchRepo：PG 落账后 ack（终态）或按退避 Nak（可重试失败）。
func (r *natsDispatchRepo) Finish(ctx context.Context, d *biz.ClaimedDispatch, res biz.FinishResult, maxAttempts int32) error {
	err := r.dispatchRepo.Finish(ctx, d, res, maxAttempts)
	m, ok := r.take(d.ID)
	if !ok {
		return err
	}
	switch {
	case err == nil && (res.OK || !res.Retryable || d.Attempts >= maxAttempts):
		_ = m.Ack()
	case err == nil:
		nakAfter(m, res.RetryAfter)
	case errors.Is(err, biz.ErrStaleClaim):
		_ = m.Ack() // 行已被回收/他人重领：其自己的游标会处理
	default:
		nakAfter(m, 5*time.Second) // 落账失败：PG 仍 sending，租约到期由回收器转 failed 后重投
	}
	return err
}

// Release implements biz.DispatchRepo：PG 放回（同语句回拨 publish_state=0）后 ack——
// relay 在 next_retry_time 到期时重新发布，不消耗本消息的 MaxDeliver。
func (r *natsDispatchRepo) Release(ctx context.Context, d *biz.ClaimedDispatch, delay time.Duration) error {
	err := r.dispatchRepo.Release(ctx, d, delay)
	if m, ok := r.take(d.ID); ok {
		if err == nil || errors.Is(err, biz.ErrStaleClaim) {
			_ = m.Ack() // 围栏拒绝：行已被回收/他人重领，其自己的游标会处理
		} else {
			nakAfter(m, 5*time.Second) // 放回失败：PG 仍 sending，租约到期由回收器转 failed 后重投
		}
	}
	return err
}
