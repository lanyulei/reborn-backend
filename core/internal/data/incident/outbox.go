package data

import (
	"context"
	"errors"
	"fmt"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/incident"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.WebhookRepo = (*webhookRepo)(nil)
	_ biz.OutboxRepo  = (*outboxRepo)(nil)
	_ biz.ActionRepo  = (*actionRepo)(nil)
	_ biz.CursorRepo  = (*cursorRepo)(nil)
)

// ---------------------------------------------------------------------------
// Webhook 订阅
// ---------------------------------------------------------------------------

type webhookRepo struct {
	data *basedata.Data
}

// NewWebhookRepo new a webhookRepo.
func NewWebhookRepo(data *basedata.Data) *webhookRepo { return &webhookRepo{data: data} }

const webhookColumns = "w.id::text, w.name, w.url, w.secret, w.events, w.enabled, w.version, w.create_time, w.update_time"

func scanWebhook(scan func(dest ...any) error) (*biz.Webhook, error) {
	var w biz.Webhook
	if err := scan(&w.ID, &w.Name, &w.URL, &w.Secret, &w.Events, &w.Enabled, &w.Version, &w.CreateTime, &w.UpdateTime); err != nil {
		return nil, err
	}
	return &w, nil
}

func (r *webhookRepo) query(ctx context.Context, tail string, args ...any) ([]*biz.Webhook, error) {
	rows, err := r.data.Pool.Query(ctx, "SELECT "+webhookColumns+" FROM incident_webhook w WHERE w.delete_time IS NULL"+tail, args...)
	if err != nil {
		return nil, fmt.Errorf("list incident webhooks: %w", err)
	}
	defer rows.Close()
	var out []*biz.Webhook
	for rows.Next() {
		w, err := scanWebhook(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("scan incident webhook: %w", err)
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// List implements biz.WebhookRepo.
func (r *webhookRepo) List(ctx context.Context) ([]*biz.Webhook, error) {
	return r.query(ctx, " ORDER BY w.name")
}

// ListEnabled implements biz.WebhookRepo.
func (r *webhookRepo) ListEnabled(ctx context.Context) ([]*biz.Webhook, error) {
	return r.query(ctx, " AND w.enabled ORDER BY w.name")
}

// Get implements biz.WebhookRepo.
func (r *webhookRepo) Get(ctx context.Context, id string) (*biz.Webhook, error) {
	w, err := scanWebhook(r.data.Pool.QueryRow(ctx, "SELECT "+webhookColumns+" FROM incident_webhook w WHERE w.id = $1 AND w.delete_time IS NULL", id).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrWebhookNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get incident webhook: %w", err)
	}
	return w, nil
}

// Create implements biz.WebhookRepo.
func (r *webhookRepo) Create(ctx context.Context, w *biz.Webhook, actor string) error {
	if _, err := r.data.Pool.Exec(ctx,
		"INSERT INTO incident_webhook (id, name, url, secret, events, enabled, create_user, update_user) VALUES ($1, $2, $3, $4, $5::text[], $6, $7, $7)",
		w.ID, w.Name, w.URL, w.Secret, textArray(w.Events), w.Enabled, nilIfEmpty(actor)); err != nil {
		if isUniqueViolation(err, "") {
			return biz.ErrWebhookInvalid
		}
		return fmt.Errorf("insert incident webhook: %w", err)
	}
	return nil
}

// Update implements biz.WebhookRepo.
func (r *webhookRepo) Update(ctx context.Context, w *biz.Webhook, expectVersion int32, actor string) error {
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE incident_webhook SET name = $3, url = $4, secret = COALESCE($5, secret), events = $6::text[], enabled = $7, version = version + 1, update_time = now(), update_user = $8
		 WHERE id = $1 AND version = $2 AND delete_time IS NULL`, w.ID, expectVersion, w.Name, w.URL, w.Secret, textArray(w.Events), w.Enabled, nilIfEmpty(actor))
	if err != nil {
		if isUniqueViolation(err, "") {
			return biz.ErrWebhookInvalid
		}
		return fmt.Errorf("update incident webhook: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return missingOrConflict(ctx, r.data.Pool, "incident_webhook", w.ID, biz.ErrWebhookNotFound)
	}
	return nil
}

// Delete implements biz.WebhookRepo.
func (r *webhookRepo) Delete(ctx context.Context, id string, expectVersion int32, actor string) error {
	return softDeleteRow(ctx, r.data, "incident_webhook", id, expectVersion, actor, biz.ErrWebhookNotFound)
}

// ---------------------------------------------------------------------------
// 出站箱
// ---------------------------------------------------------------------------

type outboxRepo struct {
	data *basedata.Data
}

// NewOutboxRepo new an outboxRepo.
func NewOutboxRepo(data *basedata.Data) *outboxRepo { return &outboxRepo{data: data} }

// EnqueueTx implements biz.OutboxRepo.
func (r *outboxRepo) EnqueueTx(ctx context.Context, tx txrun.Tx, items []*biz.OutboxItem) error {
	for _, it := range items {
		if _, err := tx.Exec(ctx, "INSERT INTO incident_outbox (id, webhook_id, incident_id, event_type, payload) VALUES ($1, $2, $3, $4, $5)",
			it.ID, it.WebhookID, it.IncidentID, it.EventType, it.Payload); err != nil {
			return fmt.Errorf("enqueue incident outbox: %w", err)
		}
	}
	return nil
}

// Claim implements biz.OutboxRepo。单条语句：行锁 + SKIP LOCKED 选出到期的待投递行，尝试次数 +1 并把 next_time 推到租约之后。
func (r *outboxRepo) Claim(ctx context.Context, limit int, lease time.Duration) ([]*biz.OutboxItem, error) {
	rows, err := r.data.Pool.Query(ctx,
		`UPDATE incident_outbox o SET attempts = o.attempts + 1, next_time = now() + make_interval(secs => $2::float8)
		 WHERE o.id IN (SELECT p.id FROM incident_outbox p WHERE p.status = 1 AND p.next_time <= now() ORDER BY p.next_time, p.id LIMIT $1 FOR UPDATE SKIP LOCKED)
		 RETURNING o.id::text, o.webhook_id::text, o.incident_id::text, o.event_type, o.payload, o.attempts`, limit, lease.Seconds())
	if err != nil {
		return nil, fmt.Errorf("claim incident outbox: %w", err)
	}
	defer rows.Close()
	var out []*biz.OutboxItem
	for rows.Next() {
		var it biz.OutboxItem
		if err := rows.Scan(&it.ID, &it.WebhookID, &it.IncidentID, &it.EventType, &it.Payload, &it.Attempts); err != nil {
			return nil, fmt.Errorf("scan incident outbox: %w", err)
		}
		out = append(out, &it)
	}
	return out, rows.Err()
}

// Finish implements biz.OutboxRepo。围栏：仍待投递且尝试次数等于领取时的值。
func (r *outboxRepo) Finish(ctx context.Context, it *biz.OutboxItem, status int32, note string) error {
	if _, err := r.data.Pool.Exec(ctx, "UPDATE incident_outbox SET status = $3, last_error = $4, done_time = now() WHERE id = $1 AND status = 1 AND attempts = $2", it.ID, it.Attempts, status, note); err != nil {
		return fmt.Errorf("finish incident outbox: %w", err)
	}
	return nil
}

// Retry implements biz.OutboxRepo.
func (r *outboxRepo) Retry(ctx context.Context, it *biz.OutboxItem, backoff time.Duration, reason string) error {
	if _, err := r.data.Pool.Exec(ctx, "UPDATE incident_outbox SET last_error = $3, next_time = now() + make_interval(secs => $4::float8) WHERE id = $1 AND status = 1 AND attempts = $2",
		it.ID, it.Attempts, reason, backoff.Seconds()); err != nil {
		return fmt.Errorf("retry incident outbox: %w", err)
	}
	return nil
}

// Counts implements biz.OutboxRepo.
func (r *outboxRepo) Counts(ctx context.Context) (map[string]int32, map[string]int32, error) {
	rows, err := r.data.Pool.Query(ctx, "SELECT webhook_id::text, count(*) FILTER (WHERE status = 1)::int, count(*) FILTER (WHERE status = 3)::int FROM incident_outbox GROUP BY webhook_id")
	if err != nil {
		return nil, nil, fmt.Errorf("count incident outbox: %w", err)
	}
	defer rows.Close()
	pending, failed := map[string]int32{}, map[string]int32{}
	for rows.Next() {
		var id string
		var p, f int32
		if err := rows.Scan(&id, &p, &f); err != nil {
			return nil, nil, err
		}
		pending[id], failed[id] = p, f
	}
	return pending, failed, rows.Err()
}

// Purge implements biz.OutboxRepo.
func (r *outboxRepo) Purge(ctx context.Context, before time.Time, limit int) (int, error) {
	tag, err := r.data.Pool.Exec(ctx, "DELETE FROM incident_outbox WHERE id IN (SELECT id FROM incident_outbox WHERE status <> 1 AND done_time < $1 LIMIT $2)", before, limit)
	if err != nil {
		return 0, fmt.Errorf("purge incident outbox: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// ---------------------------------------------------------------------------
// 动作发件箱
// ---------------------------------------------------------------------------

type actionRepo struct {
	data *basedata.Data
}

// NewActionRepo new an actionRepo.
func NewActionRepo(data *basedata.Data) *actionRepo { return &actionRepo{data: data} }

// EnqueueTx implements biz.ActionRepo（同 (incident, kind, ref) 已有待执行行时忽略）。
func (r *actionRepo) EnqueueTx(ctx context.Context, tx txrun.Tx, a *biz.Action) error {
	if _, err := tx.Exec(ctx,
		"INSERT INTO incident_action (id, incident_id, kind, ref_id, payload) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (incident_id, kind, ref_id) WHERE status = 1 DO NOTHING",
		a.ID, a.IncidentID, a.Kind, nilIfEmpty(a.RefID), a.Payload); err != nil {
		return fmt.Errorf("enqueue incident action: %w", err)
	}
	return nil
}

// Claim implements biz.ActionRepo.
func (r *actionRepo) Claim(ctx context.Context, limit int, lease time.Duration) ([]*biz.Action, error) {
	rows, err := r.data.Pool.Query(ctx,
		`UPDATE incident_action a SET attempts = a.attempts + 1, next_time = now() + make_interval(secs => $2::float8)
		 WHERE a.id IN (SELECT p.id FROM incident_action p WHERE p.status = 1 AND p.next_time <= now() ORDER BY p.next_time, p.id LIMIT $1 FOR UPDATE SKIP LOCKED)
		 RETURNING a.id::text, a.incident_id::text, a.kind, COALESCE(a.ref_id::text, ''), a.payload, a.attempts`, limit, lease.Seconds())
	if err != nil {
		return nil, fmt.Errorf("claim incident actions: %w", err)
	}
	defer rows.Close()
	var out []*biz.Action
	for rows.Next() {
		var a biz.Action
		if err := rows.Scan(&a.ID, &a.IncidentID, &a.Kind, &a.RefID, &a.Payload, &a.Attempts); err != nil {
			return nil, fmt.Errorf("scan incident action: %w", err)
		}
		out = append(out, &a)
	}
	return out, rows.Err()
}

// FinishTx implements biz.ActionRepo.
func (r *actionRepo) FinishTx(ctx context.Context, tx txrun.Tx, a *biz.Action, status int32, note string) (bool, error) {
	n, err := tx.Exec(ctx, "UPDATE incident_action SET status = $3, last_error = $4, done_time = now() WHERE id = $1 AND status = 1 AND attempts = $2", a.ID, a.Attempts, status, note)
	if err != nil {
		return false, fmt.Errorf("finish incident action: %w", err)
	}
	return n == 1, nil
}

// Retry implements biz.ActionRepo.
func (r *actionRepo) Retry(ctx context.Context, a *biz.Action, backoff time.Duration, reason string) error {
	if _, err := r.data.Pool.Exec(ctx, "UPDATE incident_action SET last_error = $3, next_time = now() + make_interval(secs => $4::float8) WHERE id = $1 AND status = 1 AND attempts = $2",
		a.ID, a.Attempts, reason, backoff.Seconds()); err != nil {
		return fmt.Errorf("retry incident action: %w", err)
	}
	return nil
}

// Purge implements biz.ActionRepo.
func (r *actionRepo) Purge(ctx context.Context, before time.Time, limit int) (int, error) {
	tag, err := r.data.Pool.Exec(ctx, "DELETE FROM incident_action WHERE id IN (SELECT id FROM incident_action WHERE status <> 1 AND done_time < $1 LIMIT $2)", before, limit)
	if err != nil {
		return 0, fmt.Errorf("purge incident actions: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// ---------------------------------------------------------------------------
// 游标
// ---------------------------------------------------------------------------

type cursorRepo struct {
	data *basedata.Data
}

// NewCursorRepo new a cursorRepo.
func NewCursorRepo(data *basedata.Data) *cursorRepo { return &cursorRepo{data: data} }

// Get implements biz.CursorRepo.
func (r *cursorRepo) Get(ctx context.Context, name string) (int64, error) {
	var seq int64
	err := r.data.Pool.QueryRow(ctx, "SELECT seq FROM incident_cursor WHERE name = $1", name).Scan(&seq)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("get incident cursor: %w", err)
	}
	return seq, nil
}

// SetTx implements biz.CursorRepo.
func (r *cursorRepo) SetTx(ctx context.Context, tx txrun.Tx, name string, seq int64) error {
	if _, err := tx.Exec(ctx, "INSERT INTO incident_cursor (name, seq, update_time) VALUES ($1, $2, now()) ON CONFLICT (name) DO UPDATE SET seq = GREATEST(incident_cursor.seq, EXCLUDED.seq), update_time = now()", name, seq); err != nil {
		return fmt.Errorf("set incident cursor: %w", err)
	}
	return nil
}

// MarkSeenTx implements biz.FeedSeenRepo.
func (r *cursorRepo) MarkSeenTx(ctx context.Context, tx txrun.Tx, seqs []int64) error {
	if len(seqs) == 0 {
		return nil
	}
	if _, err := tx.Exec(ctx, "INSERT INTO incident_feed_seen (seq) SELECT unnest($1::bigint[]) ON CONFLICT (seq) DO NOTHING", seqs); err != nil {
		return fmt.Errorf("mark incident feed seen: %w", err)
	}
	return nil
}

// Seen implements biz.FeedSeenRepo.
func (r *cursorRepo) Seen(ctx context.Context, from, to int64) (map[int64]struct{}, error) {
	out := map[int64]struct{}{}
	if to <= from {
		return out, nil
	}
	rows, err := r.data.Pool.Query(ctx, "SELECT seq FROM incident_feed_seen WHERE seq > $1 AND seq <= $2", from, to)
	if err != nil {
		return nil, fmt.Errorf("list incident feed seen: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			return nil, fmt.Errorf("scan incident feed seen: %w", err)
		}
		out[seq] = struct{}{}
	}
	return out, rows.Err()
}

// PruneBelow implements biz.FeedSeenRepo.
func (r *cursorRepo) PruneBelow(ctx context.Context, seq int64) error {
	if _, err := r.data.Pool.Exec(ctx, "DELETE FROM incident_feed_seen WHERE seq < $1", seq); err != nil {
		return fmt.Errorf("prune incident feed seen: %w", err)
	}
	return nil
}

// tryAdvisoryLock 选主锁（kit/dbx）。
func tryAdvisoryLock(ctx context.Context, data *basedata.Data, key string) (func(), bool, error) {
	return dbx.TryAdvisoryLock(ctx, data.Pool, key)
}
