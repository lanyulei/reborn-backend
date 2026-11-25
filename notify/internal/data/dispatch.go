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

// ReleaseBatch implements biz.DispatchRepo：Release 的多行形态，一条 unnest 条件 UPDATE
// （AGENTS §13.7）。每行各自过围栏（主键 + 分区键 + 仍为 sending + 声明时的 attempts），SET 与
// Release 逐字相同；围栏未过的行（已被回收 / 他人重领）不动、不报错。
func (r *dispatchRepo) ReleaseBatch(ctx context.Context, ds []*biz.ClaimedDispatch, delay time.Duration) error {
	if len(ds) == 0 {
		return nil
	}
	ids := make([]string, len(ds))
	createTimes := make([]time.Time, len(ds))
	attempts := make([]int32, len(ds))
	for i, d := range ds {
		ids[i], createTimes[i], attempts[i] = d.ID, d.CreateTime, d.Attempts
	}
	if _, err := r.data.Pool.Exec(ctx,
		`UPDATE notify_dispatch d SET status = 1, attempts = d.attempts - 1, next_retry_time = now() + $4::interval, publish_state = 0, relay_claim_until = NULL
		 FROM unnest($1::uuid[], $2::timestamptz[], $3::int[]) AS m(id, create_time, attempts)
		 WHERE d.id = m.id AND d.create_time = m.create_time AND d.status = 2 AND d.attempts = m.attempts`,
		ids, createTimes, attempts, delay); err != nil {
		return fmt.Errorf("release dispatches: %w", err)
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
		if err := reapDispatches(ctx, tx, expired, maxAttempts, backoff); err != nil {
			return err
		}
		n = len(expired)
		return nil
	})
	return n, err
}

// leaseExpiredError 租约回收写入 delivery_log.error / dispatch.last_error / dlq.last_error 的原因。
const leaseExpiredError = "lease expired (sender crashed or stalled mid-send)"

// reapDispatches 一批过期租约的落账：与逐行 finishDispatch(OK=false, Retryable=true) 同一状态机
// （attempts ≥ maxAttempts → dead + dlq，否则 failed + now()+backoff），合并成 delivery_log /
// dispatch / dlq 各一条 unnest 语句、一个 pgx.Batch 一次往返（AGENTS §13.7）。此前每行一个
// batch：100 行 = 100 次往返，期间持有全部行锁并阻塞同循环里的 SendOnce。
//
// dup: 与 finishDispatch 是同一落账口径的单行 / 多行两个形态——单行形态是 sender 热路径且带渲染
// 快照列，不为合并而改它的语句；改动任一处的状态机必须同步另一处。
func reapDispatches(ctx context.Context, tx pgx.Tx, expired []*biz.ClaimedDispatch, maxAttempts int32, backoff func(int32) time.Duration) error {
	if len(expired) == 0 {
		return nil
	}
	n := len(expired)
	var (
		logIDs      = make([]string, n)
		ids         = make([]string, n)
		createTimes = make([]time.Time, n)
		channelIDs  = make([]string, n)
		attempts    = make([]int32, n)
		deads       = make([]bool, n)
		retryMicros = make([]int64, n)
		// 死信子集（与上面同序）。
		deadIDs, deadEvents, deadChannels, deadRecipients []string
		deadAttempts                                      []int32
	)
	for i, d := range expired {
		logID, err := idgen.NewID()
		if err != nil {
			return err
		}
		logIDs[i], ids[i], createTimes[i], channelIDs[i], attempts[i] = logID, d.ID, d.CreateTime, d.ChannelID, d.Attempts
		// 间隔按微秒整数传：与 pgx 把 time.Duration 编码为 interval 的精度（纳秒截断到微秒）一致。
		retryMicros[i] = backoff(d.Attempts).Microseconds()
		if d.Attempts >= maxAttempts {
			deads[i] = true
			deadIDs, deadEvents = append(deadIDs, d.ID), append(deadEvents, d.EventID)
			deadChannels, deadRecipients = append(deadChannels, d.ChannelID), append(deadRecipients, d.Recipient)
			deadAttempts = append(deadAttempts, d.Attempts)
		}
	}
	batch := &pgx.Batch{}
	batch.Queue(
		`INSERT INTO notify_delivery_log (id, dispatch_id, channel_id, attempt, ok, latency_ms, provider_msg_id, error, create_time)
		 SELECT m.id, m.dispatch_id, m.channel_id, m.attempt, false, 0, NULL, $5::varchar, now()
		 FROM unnest($1::uuid[], $2::uuid[], $3::uuid[], $4::int[]) AS m(id, dispatch_id, channel_id, attempt)`,
		logIDs, ids, channelIDs, attempts, leaseExpiredError)
	// 围栏与单行形态相同：主键 + 分区键 + 仍为 sending + 声明时的 attempts。快照列不动
	// （单行形态此时传 NULL，COALESCE 后同样保持原值）。
	batch.Queue(
		`UPDATE notify_dispatch d SET
		   status = CASE WHEN m.dead THEN 5 ELSE 4 END,
		   next_retry_time = CASE WHEN m.dead THEN NULL ELSE now() + m.retry_us * interval '1 microsecond' END,
		   last_error = $6::varchar
		 FROM unnest($1::uuid[], $2::timestamptz[], $3::int[], $4::boolean[], $5::bigint[]) AS m(id, create_time, attempts, dead, retry_us)
		 WHERE d.id = m.id AND d.create_time = m.create_time AND d.status = 2 AND d.attempts = m.attempts`,
		ids, createTimes, attempts, deads, retryMicros, leaseExpiredError)
	if len(deadIDs) > 0 {
		batch.Queue(
			`INSERT INTO notify_dlq (id, dispatch_id, event_id, channel_id, recipient, last_error, attempts, replay_state, create_time, update_time)
			 SELECT m.id, m.id, m.event_id, m.channel_id, m.recipient, $6::varchar, m.attempts, 1, now(), now()
			 FROM unnest($1::uuid[], $2::uuid[], $3::uuid[], $4::varchar[], $5::int[]) AS m(id, event_id, channel_id, recipient, attempts)
			 ON CONFLICT (id) DO NOTHING`,
			deadIDs, deadEvents, deadChannels, deadRecipients, deadAttempts, leaseExpiredError)
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
	if tag.RowsAffected() != int64(n) {
		// 行已在本事务内 FOR UPDATE 锁住，正常不会发生；发生即与单行形态一样整事务回滚。
		_ = br.Close()
		return biz.ErrStaleClaim
	}
	if len(deadIDs) > 0 {
		if _, err := br.Exec(); err != nil {
			_ = br.Close()
			return fmt.Errorf("insert dead letter: %w", err)
		}
	}
	return br.Close()
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
