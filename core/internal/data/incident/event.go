package data

import (
	"context"
	"errors"
	"fmt"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/incident"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"

	"github.com/jackc/pgx/v5"
)

var _ biz.EventRepo = (*eventRepo)(nil)

type eventRepo struct {
	data *basedata.Data
}

// NewEventRepo new an eventRepo.
func NewEventRepo(data *basedata.Data) *eventRepo { return &eventRepo{data: data} }

const eventColumns = "e.id::text, e.incident_id::text, e.type, e.actor_type, COALESCE(e.actor_id::text, ''), e.detail, e.pinned, e.create_time"

func scanEvent(scan func(dest ...any) error) (*biz.Event, error) {
	var ev biz.Event
	var detail []byte
	if err := scan(&ev.ID, &ev.IncidentID, &ev.Type, &ev.ActorType, &ev.ActorID, &detail, &ev.Pinned, &ev.CreateTime); err != nil {
		return nil, err
	}
	if err := decodeJSON(detail, &ev.Detail, "incident event detail"); err != nil {
		return nil, err
	}
	return &ev, nil
}

func (r *eventRepo) query(ctx context.Context, what, tail string, args []any) ([]*biz.Event, error) {
	rows, err := r.data.Pool.Query(ctx, "SELECT "+eventColumns+" FROM incident_event e WHERE "+tail, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", what, err)
	}
	defer rows.Close()
	out := make([]*biz.Event, 0, 64)
	for rows.Next() {
		ev, err := scanEvent(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("%s: scan: %w", what, err)
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}

// AppendTx implements biz.EventRepo.
func (r *eventRepo) AppendTx(ctx context.Context, tx txrun.Tx, ev *biz.Event) error {
	detail, err := encodeJSON(ev.Detail, "incident event detail")
	if err != nil {
		return err
	}
	if err := tx.QueryRow(ctx,
		`INSERT INTO incident_event (id, incident_id, type, actor_type, actor_id, detail, pinned) VALUES ($1, $2, $3, $4, $5, $6, false) RETURNING create_time`,
		ev.ID, ev.IncidentID, ev.Type, ev.ActorType, nilIfEmpty(ev.ActorID), detail).Scan(&ev.CreateTime); err != nil {
		return fmt.Errorf("insert incident event: %w", err)
	}
	return nil
}

// List implements biz.EventRepo.
func (r *eventRepo) List(ctx context.Context, incidentID string, f biz.TimelineFilter, page, pageSize int64) ([]*biz.Event, int64, error) {
	var a sqlArgs
	where := "e.incident_id = " + a.add(incidentID)
	if len(f.Types) > 0 {
		where += " AND e.type = ANY(" + a.add(textArray(f.Types)) + ")"
	}
	if f.PinnedOnly {
		where += " AND e.pinned"
	}
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM incident_event e WHERE "+where, a.args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count incident events: %w", err)
	}
	items, err := r.query(ctx, "list incident events", where+" ORDER BY e.create_time, e.id LIMIT "+a.add(pageSize)+" OFFSET "+a.add((page-1)*pageSize), a.args)
	return items, total, err
}

// Get implements biz.EventRepo.
func (r *eventRepo) Get(ctx context.Context, incidentID, eventID string) (*biz.Event, error) {
	ev, err := scanEvent(r.data.Pool.QueryRow(ctx, "SELECT "+eventColumns+" FROM incident_event e WHERE e.incident_id = $1 AND e.id = $2", incidentID, eventID).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get incident event: %w", err)
	}
	return ev, nil
}

// SetPinnedTx implements biz.EventRepo.
func (r *eventRepo) SetPinnedTx(ctx context.Context, tx txrun.Tx, incidentID, eventID string, pinned bool) error {
	n, err := tx.Exec(ctx, "UPDATE incident_event SET pinned = $3 WHERE incident_id = $1 AND id = $2", incidentID, eventID, pinned)
	if err != nil {
		return fmt.Errorf("pin incident event: %w", err)
	}
	if n == 0 {
		return biz.ErrNotFound
	}
	return nil
}

// Recent implements biz.EventRepo：最近 limit 条，按时间升序返回。
func (r *eventRepo) Recent(ctx context.Context, incidentID string, excludeTypes []string, limit int) ([]*biz.Event, error) {
	var a sqlArgs
	where := "e.incident_id = " + a.add(incidentID)
	if len(excludeTypes) > 0 {
		where += " AND NOT (e.type = ANY(" + a.add(textArray(excludeTypes)) + "))"
	}
	items, err := r.query(ctx, "recent incident events", where+" ORDER BY e.create_time DESC, e.id DESC LIMIT "+a.add(limit), a.args)
	if err != nil {
		return nil, err
	}
	for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
		items[i], items[j] = items[j], items[i]
	}
	return items, nil
}

// CountSince implements biz.EventRepo.
func (r *eventRepo) CountSince(ctx context.Context, incidentID, typ string, since time.Time) (int, error) {
	var n int
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM incident_event WHERE incident_id = $1 AND type = $2 AND create_time >= $3", incidentID, typ, since).Scan(&n); err != nil {
		return 0, fmt.Errorf("count incident events: %w", err)
	}
	return n, nil
}
