package data

import (
	"context"
	"fmt"
	"time"

	"github.com/lanyulei/reborn/backend/kit/dbx"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/notify/internal/biz"

	"github.com/jackc/pgx/v5"
)

var _ biz.DispatchRepo = (*dispatchRepo)(nil)

// dispatchRepo 发送任务仓储：租约声明 + 围栏写（评审 #4/#8）。
type dispatchRepo struct {
	data *Data
}

// NewDispatchRepo new a dispatchRepo.
func NewDispatchRepo(data *Data) *dispatchRepo { return &dispatchRepo{data: data} }

const claimedColumns = "d.id, d.event_id, d.channel_id, d.recipient, COALESCE(d.template_id::text, ''), d.content, d.status, d.attempts, " +
	"d.next_retry_time, COALESCE(d.last_error, ''), d.send_key, d.create_time"

func scanClaimed(scan func(dest ...any) error) (*biz.ClaimedDispatch, error) {
	var (
		d      biz.ClaimedDispatch
		status int16
	)
	if err := scan(&d.ID, &d.EventID, &d.ChannelID, &d.Recipient, &d.TemplateID, &d.Content, &status, &d.Attempts,
		&d.NextRetryTime, &d.LastError, &d.SendKey, &d.CreateTime); err != nil {
		return nil, err
	}
	d.Status = int32(status)
	return &d, nil
}

// Claim implements biz.DispatchRepo：status IN (1,4) 且到期 → 2，attempts+1 作围栏，
// 租约写入 next_retry_time；paused 渠道排除（空切片不加子句——`= ANY(NULL)` 会排除全部）。
func (r *dispatchRepo) Claim(ctx context.Context, limit int, lease time.Duration, paused []string) ([]*biz.ClaimedDispatch, error) {
	where := "status IN (1, 4) AND next_retry_time <= now()"
	args := []any{limit, lease}
	if len(paused) > 0 {
		where += " AND NOT (channel_id = ANY($3::uuid[]))"
		args = append(args, paused)
	}
	rows, err := r.data.Pool.Query(ctx,
		`UPDATE notify_dispatch d SET status = 2, attempts = d.attempts + 1, next_retry_time = now() + $2
		 WHERE (d.id, d.create_time) IN (
		   SELECT id, create_time FROM notify_dispatch WHERE `+where+`
		   ORDER BY next_retry_time LIMIT $1 FOR UPDATE SKIP LOCKED)
		 RETURNING `+claimedColumns, args...)
	if err != nil {
		return nil, fmt.Errorf("claim dispatches: %w", err)
	}
	defer rows.Close()
	out := make([]*biz.ClaimedDispatch, 0, limit)
	for rows.Next() {
		d, err := scanClaimed(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("scan claimed dispatch: %w", err)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// fence 围栏条件：主键 + 分区键 + 仍为 sending + 本次声明的 attempts。
const fence = " WHERE id = $1 AND create_time = $2 AND status = 2 AND attempts = $3"

// Release implements biz.DispatchRepo：放回 pending 且撤销本次尝试计数。publish_state
// 回拨为 0：nats 骨干下由 relay 在到期时重新发布（pg 骨干该列恒 0，无影响）；同时清掉
// relay 残留的认领租约（迁移 0046），否则到期后还要等租约过期才会被重新发布。
func (r *dispatchRepo) Release(ctx context.Context, d *biz.ClaimedDispatch, delay time.Duration) error {
	tag, err := r.data.Pool.Exec(ctx,
		"UPDATE notify_dispatch SET status = 1, attempts = attempts - 1, next_retry_time = now() + $4, publish_state = 0, relay_claim_until = NULL"+fence,
		d.ID, d.CreateTime, d.Attempts, delay)
	if err != nil {
		return fmt.Errorf("release dispatch: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrStaleClaim
	}
	return nil
}

// Finish implements biz.DispatchRepo：同事务写 delivery_log + 终态（含首次渲染快照
// 与 template_id 回填）；dead 时写 dlq（id = dispatch id，ON CONFLICT DO NOTHING
// 幂等）。三条语句一个 pgx.Batch：一次往返。
func (r *dispatchRepo) Finish(ctx context.Context, d *biz.ClaimedDispatch, res biz.FinishResult, maxAttempts int32) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		return finishDispatch(ctx, tx, d.ID, d.CreateTime, d.Attempts, d.EventID, d.ChannelID, d.Recipient, res, maxAttempts)
	})
}

// finishDispatch 落账原语（sender Finish 与租约回收共用）：围栏 UPDATE 0 行即
// ErrStaleClaim（整个事务回滚，delivery_log 不留痕）。
func finishDispatch(ctx context.Context, tx pgx.Tx, id string, createTime time.Time, attempts int32,
	eventID, channelID, recipient string, res biz.FinishResult, maxAttempts int32,
) error {
	logID, err := idgen.NewID()
	if err != nil {
		return err
	}
	batch := &pgx.Batch{}
	batch.Queue(
		`INSERT INTO notify_delivery_log (id, dispatch_id, channel_id, attempt, ok, latency_ms, provider_msg_id, error, create_time)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,now())`,
		logID, id, channelID, attempts, res.OK, res.LatencyMs, dbx.NilIfEmpty(res.ProviderMsgID), dbx.NilIfEmpty(res.Error))
	// 快照列：本次未渲染（NULL 实参）保持原值。
	const snapshotSet = ", content = COALESCE($4, content), template_id = COALESCE($5, template_id)"
	snapshot, tplID := dbx.NilIfEmptyBytes(res.Content), dbx.NilIfEmpty(res.TemplateID)
	dead := !res.OK && (!res.Retryable || attempts >= maxAttempts)
	switch {
	case res.OK:
		batch.Queue("UPDATE notify_dispatch SET status = 3, next_retry_time = NULL, last_error = NULL"+snapshotSet+fence,
			id, createTime, attempts, snapshot, tplID)
	case !dead:
		// 到期时刻取数据库 now()（Claim 亦按 now() 比较：同一时钟源，AGENTS §13.2）。
		batch.Queue("UPDATE notify_dispatch SET status = 4, next_retry_time = now() + $6, last_error = $7"+snapshotSet+fence,
			id, createTime, attempts, snapshot, tplID, res.RetryAfter, dbx.NilIfEmpty(res.Error))
	default:
		batch.Queue("UPDATE notify_dispatch SET status = 5, next_retry_time = NULL, last_error = $6"+snapshotSet+fence,
			id, createTime, attempts, snapshot, tplID, dbx.NilIfEmpty(res.Error))
	}
	if dead {
		batch.Queue(
			`INSERT INTO notify_dlq (id, dispatch_id, event_id, channel_id, recipient, last_error, attempts, replay_state, create_time, update_time)
			 VALUES ($1, $1, $2, $3, $4, $5, $6, 1, now(), now()) ON CONFLICT (id) DO NOTHING`,
			id, eventID, channelID, recipient, dbx.NilIfEmpty(res.Error), attempts)
	}
	br := tx.SendBatch(ctx, batch)
	if _, err := br.Exec(); err != nil {
		_ = br.Close()
		return fmt.Errorf("insert delivery log: %w", err)
	}
	tag, err := br.Exec()
	if err != nil {
		_ = br.Close()
		return fmt.Errorf("finish dispatch: %w", err)
	}
	if tag.RowsAffected() == 0 {
		_ = br.Close()
		return biz.ErrStaleClaim
	}
	if dead {
		if _, err := br.Exec(); err != nil {
			_ = br.Close()
			return fmt.Errorf("insert dead letter: %w", err)
		}
	}
	return br.Close()
}

// ReapExpired implements biz.DispatchRepo：租约过期的 sending 行（可能崩溃于发送中）
// 记一次失败尝试（attempts 已在声明时计数）并按退避转 failed，超限转 dead。
func (r *dispatchRepo) ReapExpired(ctx context.Context, limit int, maxAttempts int32, backoff func(int32) time.Duration) (int, error) {
	n := 0
	err := dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx,
			"SELECT "+claimedColumns+" FROM notify_dispatch d WHERE d.status = 2 AND d.next_retry_time < now()"+
				" ORDER BY d.next_retry_time LIMIT $1 FOR UPDATE SKIP LOCKED", limit)
		if err != nil {
			return fmt.Errorf("scan expired leases: %w", err)
		}
		var expired []*biz.ClaimedDispatch
		for rows.Next() {
			d, err := scanClaimed(rows.Scan)
			if err != nil {
				rows.Close()
				return fmt.Errorf("scan expired dispatch: %w", err)
			}
			expired = append(expired, d)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, d := range expired {
			res := biz.FinishResult{OK: false, Retryable: true, Error: "lease expired (sender crashed or stalled mid-send)", RetryAfter: backoff(d.Attempts)}
			if err := finishDispatch(ctx, tx, d.ID, d.CreateTime, d.Attempts, d.EventID, d.ChannelID, d.Recipient, res, maxAttempts); err != nil {
				return err
			}
			n++
		}
		return nil
	})
	return n, err
}

// GetEvents implements biz.DispatchRepo：id 集合一次取回；create_time 上界让分区
// 剪枝只探查不晚于最新 dispatch 的分区。
func (r *dispatchRepo) GetEvents(ctx context.Context, ids []string, maxCreateTime time.Time) (map[string]*biz.Event, error) {
	out := make(map[string]*biz.Event, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := r.data.Pool.Query(ctx,
		"SELECT "+eventColumns+" FROM notify_event WHERE id = ANY($1::uuid[]) AND create_time <= $2", ids, maxCreateTime)
	if err != nil {
		return nil, fmt.Errorf("get events: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		ev, err := scanEvent(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		out[ev.ID] = ev
	}
	return out, rows.Err()
}
