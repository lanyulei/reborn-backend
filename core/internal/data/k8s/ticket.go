package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/k8s"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.TicketRepo = (*ticketRepo)(nil)
	_ biz.OutboxRepo = (*outboxRepo)(nil)
)

type ticketRepo struct {
	data *basedata.Data
}

// NewTicketRepo new a ticketRepo.
func NewTicketRepo(data *basedata.Data) *ticketRepo { return &ticketRepo{data: data} }

// Create implements biz.TicketRepo。过期时间用数据库时钟；顺带清掉已过期的票据（表很小，走索引）。
func (r *ticketRepo) Create(ctx context.Context, hash []byte, t biz.ExecTicket, ttl time.Duration) error {
	params, err := json.Marshal(t.Params)
	if err != nil {
		return fmt.Errorf("encode k8s exec params: %w", err)
	}
	if _, err := r.data.Pool.Exec(ctx, "DELETE FROM k8s_exec_ticket WHERE expire_time < now() - interval '1 minute'"); err != nil {
		return fmt.Errorf("purge k8s exec tickets: %w", err)
	}
	if _, err := r.data.Pool.Exec(ctx,
		`INSERT INTO k8s_exec_ticket (token_hash, user_id, cluster_id, params, expire_time)
		 VALUES ($1, $2, $3, $4, now() + make_interval(secs => $5))`,
		hash, t.UserID, t.ClusterID, params, ttl.Seconds()); err != nil {
		return fmt.Errorf("insert k8s exec ticket: %w", err)
	}
	return nil
}

// Redeem implements biz.TicketRepo。单语句 DELETE … RETURNING：并发核销只有一个成功；已过期的不返回。
func (r *ticketRepo) Redeem(ctx context.Context, hash []byte) (*biz.ExecTicket, error) {
	var t biz.ExecTicket
	var params []byte
	err := r.data.Pool.QueryRow(ctx,
		`DELETE FROM k8s_exec_ticket WHERE token_hash = $1 AND expire_time > now()
		 RETURNING user_id::text, cluster_id::text, params`, hash).Scan(&t.UserID, &t.ClusterID, &params)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrTicketInvalid
	}
	if err != nil {
		return nil, fmt.Errorf("redeem k8s exec ticket: %w", err)
	}
	if err := json.Unmarshal(params, &t.Params); err != nil {
		return nil, fmt.Errorf("decode k8s exec params: %w", err)
	}
	return &t, nil
}

// 发件箱失败重试的退避：5s 起指数增长，上限 5 分钟。
const (
	outboxBackoffBaseSeconds = 5.0
	outboxBackoffMaxSeconds  = 300.0
)

type outboxRepo struct {
	data *basedata.Data
}

// NewOutboxRepo new an outboxRepo.
func NewOutboxRepo(data *basedata.Data) *outboxRepo { return &outboxRepo{data: data} }

// ClaimTx implements biz.OutboxRepo。行锁 + SKIP LOCKED：别的实例正在处理的行直接跳过。
func (r *outboxRepo) ClaimTx(ctx context.Context, tx txrun.Tx, exclude []string) (*biz.OutboxItem, error) {
	var it biz.OutboxItem
	var payload []byte
	err := tx.QueryRow(ctx,
		`SELECT id::text, cluster_id::text, event_type, severity, dedup_key, group_key, recipients::text[], payload, attempts
		 FROM k8s_notify_outbox
		 WHERE status = $1 AND id <> ALL($2::uuid[])
		   AND (attempts = 0 OR done_time IS NULL
		        OR done_time + make_interval(secs => LEAST($3::float8 * power(2, attempts - 1), $4::float8)) <= now())
		 ORDER BY create_time, id LIMIT 1 FOR UPDATE SKIP LOCKED`,
		biz.OutboxPending, dbx.TextArray(exclude), outboxBackoffBaseSeconds, outboxBackoffMaxSeconds).
		Scan(&it.ID, &it.ClusterID, &it.EventType, &it.Severity, &it.DedupKey, &it.GroupKey, &it.Recipients, &payload, &it.Attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim k8s notify outbox: %w", err)
	}
	it.Payload = payload
	return &it, nil
}

// FinishTx implements biz.OutboxRepo。只写 core 获准的列；以 status = 待派发 围栏。
func (r *outboxRepo) FinishTx(ctx context.Context, tx txrun.Tx, id string, status int32, reason string) error {
	n, err := tx.Exec(ctx,
		"UPDATE k8s_notify_outbox SET status = $2, last_error = $3, done_time = now() WHERE id = $1 AND status = $4",
		id, status, reason, biz.OutboxPending)
	if err != nil {
		return fmt.Errorf("finish k8s notify outbox: %w", err)
	}
	if n != 1 {
		return fmt.Errorf("finish k8s notify outbox %s: row is no longer pending", id)
	}
	return nil
}

// Fail implements biz.OutboxRepo。单语句：失败次数 +1 并记下失败时间（退避的起点），达到上限即置为已丢弃。
func (r *outboxRepo) Fail(ctx context.Context, id, reason string, maxAttempts int32) (bool, error) {
	var status int32
	err := r.data.Pool.QueryRow(ctx,
		`UPDATE k8s_notify_outbox SET attempts = attempts + 1, last_error = $2,
		   status = CASE WHEN attempts + 1 >= $3 THEN $4 ELSE status END, done_time = now()
		 WHERE id = $1 AND status = $5 RETURNING status`,
		id, reason, maxAttempts, biz.OutboxDropped, biz.OutboxPending).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("record k8s notify outbox failure: %w", err)
	}
	return status == biz.OutboxDropped, nil
}
