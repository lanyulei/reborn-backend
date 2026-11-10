package data

import (
	"context"
	"fmt"
	"time"

	"github.com/lanyulei/reborn/backend/notify/internal/biz"
	"github.com/lanyulei/reborn/backend/notify/internal/pkg/idgen"

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

// SaveContent implements biz.DispatchRepo：渲染快照 + template_id 列回填（控制台
// ListDispatches 展示用；版本号在 content.template_version）。
func (r *dispatchRepo) SaveContent(ctx context.Context, d *biz.ClaimedDispatch, content []byte, templateID string) error {
	tag, err := r.data.Pool.Exec(ctx, "UPDATE notify_dispatch SET content = $4, template_id = $5"+fence,
		d.ID, d.CreateTime, d.Attempts, content, nilIfEmpty(templateID))
	if err != nil {
		return fmt.Errorf("save dispatch content: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrStaleClaim
	}
	d.Content = content
	return nil
}

// Release implements biz.DispatchRepo：放回 pending 且撤销本次尝试计数。
func (r *dispatchRepo) Release(ctx context.Context, d *biz.ClaimedDispatch, delay time.Duration) error {
	tag, err := r.data.Pool.Exec(ctx,
		"UPDATE notify_dispatch SET status = 1, attempts = attempts - 1, next_retry_time = now() + $4"+fence,
		d.ID, d.CreateTime, d.Attempts, delay)
	if err != nil {
		return fmt.Errorf("release dispatch: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrStaleClaim
	}
	return nil
}

// Finish implements biz.DispatchRepo：同事务写 delivery_log + 终态；dead 时写 dlq
// （id = dispatch id，ON CONFLICT DO NOTHING 幂等）。
func (r *dispatchRepo) Finish(ctx context.Context, d *biz.ClaimedDispatch, res biz.FinishResult, maxAttempts int32) error {
	return WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		return finishDispatch(ctx, tx, d.ID, d.CreateTime, d.Attempts, d.EventID, d.ChannelID, d.Recipient, res, maxAttempts)
	})
}

// finishDispatch 落账原语（sender Finish 与租约回收共用）。
func finishDispatch(ctx context.Context, tx pgx.Tx, id string, createTime time.Time, attempts int32,
	eventID, channelID, recipient string, res biz.FinishResult, maxAttempts int32,
) error {
	logID, err := idgen.NewID()
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO notify_delivery_log (id, dispatch_id, channel_id, attempt, ok, latency_ms, provider_msg_id, error, create_time)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,now())`,
		logID, id, channelID, attempts, res.OK, res.LatencyMs, nilIfEmpty(res.ProviderMsgID), nilIfEmpty(res.Error)); err != nil {
		return fmt.Errorf("insert delivery log: %w", err)
	}
	var tag interface{ RowsAffected() int64 }
	switch {
	case res.OK:
		tag, err = tx.Exec(ctx, "UPDATE notify_dispatch SET status = 3, next_retry_time = NULL, last_error = NULL"+fence, id, createTime, attempts)
	case res.Retryable && attempts < maxAttempts:
		tag, err = tx.Exec(ctx, "UPDATE notify_dispatch SET status = 4, next_retry_time = $4, last_error = $5"+fence,
			id, createTime, attempts, res.NextRetry, nilIfEmpty(res.Error))
	default:
		tag, err = tx.Exec(ctx, "UPDATE notify_dispatch SET status = 5, next_retry_time = NULL, last_error = $4"+fence,
			id, createTime, attempts, nilIfEmpty(res.Error))
	}
	if err != nil {
		return fmt.Errorf("finish dispatch: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrStaleClaim
	}
	if !res.OK && (!res.Retryable || attempts >= maxAttempts) {
		if _, err := tx.Exec(ctx,
			`INSERT INTO notify_dlq (id, dispatch_id, event_id, channel_id, recipient, last_error, attempts, replay_state, create_time, update_time)
			 VALUES ($1, $1, $2, $3, $4, $5, $6, 1, now(), now()) ON CONFLICT (id) DO NOTHING`,
			id, eventID, channelID, recipient, nilIfEmpty(res.Error), attempts); err != nil {
			return fmt.Errorf("insert dead letter: %w", err)
		}
	}
	return nil
}

// ReapExpired implements biz.DispatchRepo：租约过期的 sending 行（可能崩溃于发送中）
// 记一次失败尝试（attempts 已在声明时计数）并按退避转 failed，超限转 dead。
func (r *dispatchRepo) ReapExpired(ctx context.Context, limit int, maxAttempts int32, backoff func(int32) time.Duration) (int, error) {
	n := 0
	err := WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
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
			res := biz.FinishResult{OK: false, Retryable: true, Error: "lease expired (sender crashed or stalled mid-send)", NextRetry: time.Now().Add(backoff(d.Attempts))}
			if err := finishDispatch(ctx, tx, d.ID, d.CreateTime, d.Attempts, d.EventID, d.ChannelID, d.Recipient, res, maxAttempts); err != nil {
				return err
			}
			n++
		}
		return nil
	})
	return n, err
}

// GetEvent implements biz.DispatchRepo.
func (r *dispatchRepo) GetEvent(ctx context.Context, id string) (*biz.Event, error) {
	return getEvent(ctx, r.data.Pool, id)
}
