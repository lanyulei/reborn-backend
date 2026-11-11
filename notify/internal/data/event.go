package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lanyulei/reborn/backend/kit/dbx"
	"github.com/lanyulei/reborn/backend/notify/internal/biz"

	"github.com/jackc/pgx/v5"
)

var _ biz.EventSource = (*eventRepo)(nil)

// eventRepo PG 骨干事件源：单事务内 FOR UPDATE SKIP LOCKED 声明一批 pending 事件
// 并回调处理，提交即交付（崩溃回滚零丢失）。
type eventRepo struct {
	data *Data
}

// NewEventRepo new an eventRepo.
func NewEventRepo(data *Data) *eventRepo { return &eventRepo{data: data} }

const eventColumns = "id, biz_id, event_type, severity, title, payload, dedup_key, group_key, recipients, " +
	"source, status, publish_state, publish_attempts, next_publish_time, create_time"

func scanEvent(scan func(dest ...any) error) (*biz.Event, error) {
	var (
		ev                 biz.Event
		severity           int16
		status, pubState   int16
		dedupKey, groupKey *string
		recipientsRaw      []byte
	)
	if err := scan(&ev.ID, &ev.BizID, &ev.EventType, &severity, &ev.Title, &ev.Payload, &dedupKey, &groupKey,
		&recipientsRaw, &ev.Source, &status, &pubState, &ev.PublishAttempts, &ev.NextPublishTime, &ev.CreateTime); err != nil {
		return nil, err
	}
	ev.Severity, ev.Status, ev.PublishState = int32(severity), int32(status), int32(pubState)
	if dedupKey != nil {
		ev.DedupKey = *dedupKey
	}
	if groupKey != nil {
		ev.GroupKey = *groupKey
	}
	if len(recipientsRaw) > 0 {
		// 非数组形态（契约外）不让整批失败：按无显式收件处理。
		_ = json.Unmarshal(recipientsRaw, &ev.Recipients)
	}
	return &ev, nil
}

// ProcessPending implements biz.EventSource.
func (r *eventRepo) ProcessPending(ctx context.Context, limit int, fn func(ctx context.Context, tx biz.RouterTx, events []*biz.Event) error) (int, error) {
	n := 0
	err := dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx,
			"SELECT "+eventColumns+" FROM notify_event WHERE publish_state = 0 AND next_publish_time <= now()"+
				" ORDER BY next_publish_time LIMIT $1 FOR UPDATE SKIP LOCKED", limit)
		if err != nil {
			return fmt.Errorf("claim events: %w", err)
		}
		events := make([]*biz.Event, 0, limit)
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
		if len(events) == 0 {
			return nil
		}
		n = len(events)
		return fn(ctx, &routerTx{tx: tx}, events)
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

// routerTx implements biz.RouterTx over pgx.Tx.
type routerTx struct{ tx pgx.Tx }

func (t *routerTx) MarkEvent(ctx context.Context, id string, createTime time.Time, status int32) error {
	if _, err := t.tx.Exec(ctx,
		"UPDATE notify_event SET publish_state = 1, status = $3 WHERE id = $1 AND create_time = $2", id, createTime, status); err != nil {
		return fmt.Errorf("mark event: %w", err)
	}
	return nil
}

func (t *routerTx) DeferEvent(ctx context.Context, id string, createTime time.Time, next time.Time) error {
	if _, err := t.tx.Exec(ctx,
		"UPDATE notify_event SET publish_attempts = publish_attempts + 1, next_publish_time = $3 WHERE id = $1 AND create_time = $2",
		id, createTime, next); err != nil {
		return fmt.Errorf("defer event: %w", err)
	}
	return nil
}

func (t *routerTx) InsertDispatches(ctx context.Context, dispatches []*biz.Dispatch) error {
	return insertDispatches(ctx, t.tx, dispatches)
}

func (t *routerTx) UpsertAggGroups(ctx context.Context, incs []biz.AggIncrement) (map[string]biz.AggVerdict, error) {
	return upsertAggGroups(ctx, t.tx, incs)
}

func (t *routerTx) ListSubscribers(ctx context.Context, bizID, eventType string) ([]biz.Subscriber, error) {
	return listSubscribers(ctx, t.tx, bizID, eventType)
}

// LookupUserEmails implements biz.RouterTx：显式收件里的平台用户 ID → 邮箱
// （仅启用、未删、有邮箱的用户）。
func (t *routerTx) LookupUserEmails(ctx context.Context, userIDs []string) (map[string]string, error) {
	rows, err := t.tx.Query(ctx,
		`SELECT id::text, email FROM "system_user"
		 WHERE id = ANY($1::uuid[]) AND delete_time IS NULL AND status = 1 AND email IS NOT NULL AND email <> ''`, userIDs)
	if err != nil {
		return nil, fmt.Errorf("lookup user emails: %w", err)
	}
	defer rows.Close()
	out := make(map[string]string, len(userIDs))
	for rows.Next() {
		var id, email string
		if err := rows.Scan(&id, &email); err != nil {
			return nil, fmt.Errorf("scan user email: %w", err)
		}
		out[id] = email
	}
	return out, rows.Err()
}

// insertDispatches 批量插入（pgx.Batch 单往返）。时间列取数据库 now()：到期判定
// （Claim 的 next_retry_time <= now()）与写入同一时钟源，应用与 DB 时钟偏差
// 不会让刚生成的任务"在未来"而被跳过一轮。
func insertDispatches(ctx context.Context, tx pgx.Tx, dispatches []*biz.Dispatch) error {
	if len(dispatches) == 0 {
		return nil
	}
	const q = `INSERT INTO notify_dispatch (id, event_id, channel_id, recipient, template_id, content, status, attempts,
		next_retry_time, last_error, send_key, create_time) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,now(),$9,$10,now())`
	batch := &pgx.Batch{}
	for _, d := range dispatches {
		batch.Queue(q, d.ID, d.EventID, d.ChannelID, d.Recipient, dbx.NilIfEmpty(d.TemplateID), dbx.NilIfEmptyBytes(d.Content),
			d.Status, d.Attempts, dbx.NilIfEmpty(d.LastError), d.SendKey)
	}
	res := tx.SendBatch(ctx, batch)
	for range dispatches {
		if _, err := res.Exec(); err != nil {
			_ = res.Close()
			return fmt.Errorf("insert dispatch: %w", err)
		}
	}
	return res.Close()
}

// listSubscribers 订阅者实时解析：notify_subscription ⋈ "system_user"（启用、未删），
// 事件模式匹配在 SQL 内完成（'*' / 精确 / 前缀通配）。
func listSubscribers(ctx context.Context, tx pgx.Tx, bizID, eventType string) ([]biz.Subscriber, error) {
	rows, err := tx.Query(ctx,
		`SELECT s.user_id, COALESCE(u.email, ''), s.channel_prefs
		 FROM notify_subscription s
		 JOIN "system_user" u ON u.id = s.user_id AND u.delete_time IS NULL AND u.status = 1
		 WHERE s.biz_id = $1 AND s.enabled AND s.delete_time IS NULL
		   AND (s.event_pattern = '*' OR s.event_pattern = $2
		        OR (right(s.event_pattern, 2) = '.*' AND left($2, length(s.event_pattern) - 1) = left(s.event_pattern, length(s.event_pattern) - 1)))`,
		bizID, eventType)
	if err != nil {
		return nil, fmt.Errorf("list subscribers: %w", err)
	}
	defer rows.Close()
	var out []biz.Subscriber
	for rows.Next() {
		var (
			s   biz.Subscriber
			raw []byte
		)
		if err := rows.Scan(&s.UserID, &s.Email, &raw); err != nil {
			return nil, fmt.Errorf("scan subscriber: %w", err)
		}
		if err := json.Unmarshal(raw, &s.Prefs); err != nil {
			return nil, fmt.Errorf("decode channel_prefs of %s: %w", s.UserID, err)
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// getEvent 按 id 取事件（分区表无分区键：逐分区 PK 探查，分区数有限）。
func getEvent(ctx context.Context, q interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}, id string,
) (*biz.Event, error) {
	ev, err := scanEvent(q.QueryRow(ctx, "SELECT "+eventColumns+" FROM notify_event WHERE id = $1", id).Scan)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrEventNotFound
		}
		return nil, fmt.Errorf("get event: %w", err)
	}
	return ev, nil
}
