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

var (
	_ biz.UpdateRepo     = (*updateRepo)(nil)
	_ biz.ShareRepo      = (*shareRepo)(nil)
	_ biz.PostmortemRepo = (*postmortemRepo)(nil)
	_ biz.ActionItemRepo = (*actionItemRepo)(nil)
)

// ---------------------------------------------------------------------------
// 公告
// ---------------------------------------------------------------------------

type updateRepo struct {
	data *basedata.Data
}

// NewUpdateRepo new an updateRepo.
func NewUpdateRepo(data *basedata.Data) *updateRepo { return &updateRepo{data: data} }

// CreateTx implements biz.UpdateRepo.
func (r *updateRepo) CreateTx(ctx context.Context, tx txrun.Tx, u *biz.Update) error {
	if err := tx.QueryRow(ctx, "INSERT INTO incident_update (id, incident_id, kind, content, author_id) VALUES ($1, $2, $3, $4, $5) RETURNING create_time",
		u.ID, u.IncidentID, u.Kind, u.Content, nilIfEmpty(u.AuthorID)).Scan(&u.CreateTime); err != nil {
		return fmt.Errorf("insert incident update: %w", err)
	}
	return nil
}

// List implements biz.UpdateRepo.
func (r *updateRepo) List(ctx context.Context, incidentID string, limit int) ([]*biz.Update, error) {
	rows, err := r.data.Pool.Query(ctx, "SELECT id::text, kind, content, COALESCE(author_id::text, ''), create_time FROM incident_update WHERE incident_id = $1 ORDER BY create_time DESC LIMIT $2", incidentID, limit)
	if err != nil {
		return nil, fmt.Errorf("list incident updates: %w", err)
	}
	defer rows.Close()
	var out []*biz.Update
	for rows.Next() {
		u := &biz.Update{IncidentID: incidentID}
		if err := rows.Scan(&u.ID, &u.Kind, &u.Content, &u.AuthorID, &u.CreateTime); err != nil {
			return nil, fmt.Errorf("scan incident update: %w", err)
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// 分享链接
// ---------------------------------------------------------------------------

type shareRepo struct {
	data *basedata.Data
}

// NewShareRepo new a shareRepo.
func NewShareRepo(data *basedata.Data) *shareRepo { return &shareRepo{data: data} }

const shareColumns = "s.id::text, s.incident_id::text, s.name, s.token_hash, s.token_sealed, s.expire_time, s.access_count, s.last_access_time, COALESCE(s.create_user::text, ''), s.create_time"

func scanShare(scan func(dest ...any) error) (*biz.ShareLink, error) {
	var l biz.ShareLink
	if err := scan(&l.ID, &l.IncidentID, &l.Name, &l.TokenHash, &l.TokenSealed, &l.ExpireTime, &l.AccessCount, &l.LastAccessTime, &l.CreateUser, &l.CreateTime); err != nil {
		return nil, err
	}
	return &l, nil
}

// Create implements biz.ShareRepo.
func (r *shareRepo) Create(ctx context.Context, l *biz.ShareLink) error {
	if err := r.data.Pool.QueryRow(ctx,
		"INSERT INTO incident_share (id, incident_id, name, token_hash, token_sealed, expire_time, create_user) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING create_time",
		l.ID, l.IncidentID, l.Name, l.TokenHash, l.TokenSealed, nilTime(l.ExpireTime), nilIfEmpty(l.CreateUser)).Scan(&l.CreateTime); err != nil {
		return fmt.Errorf("insert incident share: %w", err)
	}
	return nil
}

// List implements biz.ShareRepo.
func (r *shareRepo) List(ctx context.Context, incidentID string) ([]*biz.ShareLink, error) {
	rows, err := r.data.Pool.Query(ctx, "SELECT "+shareColumns+" FROM incident_share s WHERE s.incident_id = $1 AND s.delete_time IS NULL ORDER BY s.create_time DESC", incidentID)
	if err != nil {
		return nil, fmt.Errorf("list incident shares: %w", err)
	}
	defer rows.Close()
	var out []*biz.ShareLink
	for rows.Next() {
		l, err := scanShare(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("scan incident share: %w", err)
		}
		l.TokenSealed = nil
		out = append(out, l)
	}
	return out, rows.Err()
}

// Get implements biz.ShareRepo.
func (r *shareRepo) Get(ctx context.Context, id string) (*biz.ShareLink, error) {
	l, err := scanShare(r.data.Pool.QueryRow(ctx, "SELECT "+shareColumns+" FROM incident_share s WHERE s.id = $1 AND s.delete_time IS NULL", id).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrShareNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get incident share: %w", err)
	}
	return l, nil
}

// FindByHash implements biz.ShareRepo.
func (r *shareRepo) FindByHash(ctx context.Context, hash []byte) (*biz.ShareLink, error) {
	l, err := scanShare(r.data.Pool.QueryRow(ctx, "SELECT "+shareColumns+" FROM incident_share s WHERE s.token_hash = $1 AND s.delete_time IS NULL", hash).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrShareNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("find incident share: %w", err)
	}
	return l, nil
}

// SetToken implements biz.ShareRepo.
func (r *shareRepo) SetToken(ctx context.Context, id string, hash, sealed []byte) error {
	tag, err := r.data.Pool.Exec(ctx, "UPDATE incident_share SET token_hash = $2, token_sealed = $3 WHERE id = $1 AND delete_time IS NULL", id, hash, sealed)
	if err != nil {
		return fmt.Errorf("reset incident share token: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrShareNotFound
	}
	return nil
}

// Delete implements biz.ShareRepo.
func (r *shareRepo) Delete(ctx context.Context, id string) error {
	tag, err := r.data.Pool.Exec(ctx, "UPDATE incident_share SET delete_time = now(), token_hash = gen_random_bytes(32) WHERE id = $1 AND delete_time IS NULL", id)
	if err != nil {
		return fmt.Errorf("delete incident share: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrShareNotFound
	}
	return nil
}

// Touch implements biz.ShareRepo.
func (r *shareRepo) Touch(ctx context.Context, id string) error {
	if _, err := r.data.Pool.Exec(ctx, "UPDATE incident_share SET access_count = access_count + 1, last_access_time = now() WHERE id = $1", id); err != nil {
		return fmt.Errorf("touch incident share: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// 复盘
// ---------------------------------------------------------------------------

type postmortemRepo struct {
	data *basedata.Data
}

// NewPostmortemRepo new a postmortemRepo.
func NewPostmortemRepo(data *basedata.Data) *postmortemRepo { return &postmortemRepo{data: data} }

// Get implements biz.PostmortemRepo.
func (r *postmortemRepo) Get(ctx context.Context, incidentID string) (*biz.Postmortem, error) {
	pm := &biz.Postmortem{IncidentID: incidentID}
	var doc []byte
	err := basedata.QueryerFrom(ctx, r.data.Pool).QueryRow(ctx, "SELECT doc, current_version, COALESCE(review_instance_id::text, ''), COALESCE(update_user::text, ''), update_time FROM incident_postmortem WHERE incident_id = $1", incidentID).
		Scan(&doc, &pm.CurrentVersion, &pm.ReviewInstanceID, &pm.UpdateUser, &pm.UpdateTime)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get incident postmortem: %w", err)
	}
	if err := decodeJSON(doc, &pm.Doc, "postmortem doc"); err != nil {
		return nil, err
	}
	return pm, nil
}

// SaveTx implements biz.PostmortemRepo.
func (r *postmortemRepo) SaveTx(ctx context.Context, tx txrun.Tx, pm *biz.Postmortem) error {
	doc, err := encodeJSON(pm.Doc, "postmortem doc")
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO incident_postmortem (incident_id, doc, current_version, review_instance_id, update_user, update_time) VALUES ($1, $2, $3, $4, $5, now())
		 ON CONFLICT (incident_id) DO UPDATE SET doc = EXCLUDED.doc, current_version = EXCLUDED.current_version, review_instance_id = EXCLUDED.review_instance_id, update_user = EXCLUDED.update_user, update_time = now()`,
		pm.IncidentID, doc, pm.CurrentVersion, nilIfEmpty(pm.ReviewInstanceID), nilIfEmpty(pm.UpdateUser)); err != nil {
		return fmt.Errorf("save incident postmortem: %w", err)
	}
	return nil
}

// AddVersionTx implements biz.PostmortemRepo.
func (r *postmortemRepo) AddVersionTx(ctx context.Context, tx txrun.Tx, v *biz.PostmortemVersion) error {
	doc, err := encodeJSON(v.Doc, "postmortem doc")
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "INSERT INTO incident_postmortem_version (id, incident_id, version, doc, note, create_user) VALUES ($1, $2, $3, $4, $5, $6)",
		v.ID, v.IncidentID, v.Version, doc, v.Note, nilIfEmpty(v.CreateUser)); err != nil {
		return fmt.Errorf("insert incident postmortem version: %w", err)
	}
	return nil
}

// ListVersions implements biz.PostmortemRepo.
func (r *postmortemRepo) ListVersions(ctx context.Context, incidentID string) ([]*biz.PostmortemVersion, error) {
	rows, err := r.data.Pool.Query(ctx, "SELECT id::text, version, doc, note, COALESCE(create_user::text, ''), create_time FROM incident_postmortem_version WHERE incident_id = $1 ORDER BY version DESC", incidentID)
	if err != nil {
		return nil, fmt.Errorf("list incident postmortem versions: %w", err)
	}
	defer rows.Close()
	var out []*biz.PostmortemVersion
	for rows.Next() {
		v := &biz.PostmortemVersion{IncidentID: incidentID}
		var doc []byte
		if err := rows.Scan(&v.ID, &v.Version, &doc, &v.Note, &v.CreateUser, &v.CreateTime); err != nil {
			return nil, fmt.Errorf("scan incident postmortem version: %w", err)
		}
		if err := decodeJSON(doc, &v.Doc, "postmortem doc"); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// 改进项
// ---------------------------------------------------------------------------

type actionItemRepo struct {
	data *basedata.Data
}

// NewActionItemRepo new an actionItemRepo.
func NewActionItemRepo(data *basedata.Data) *actionItemRepo { return &actionItemRepo{data: data} }

const itemColumns = `a.id::text, a.incident_id::text, i.number, a.title, a.kind, COALESCE(a.owner_id::text, ''), a.due_date, a.status, COALESCE(a.ticket_id::text, ''), a.ticket_number,
	COALESCE(a.create_user::text, ''), a.create_time, a.update_time`

func scanItem(scan func(dest ...any) error) (*biz.ActionItem, error) {
	var it biz.ActionItem
	if err := scan(&it.ID, &it.IncidentID, &it.IncidentNumber, &it.Title, &it.Kind, &it.OwnerID, &it.DueDate, &it.Status, &it.TicketID, &it.TicketNumber,
		&it.CreateUser, &it.CreateTime, &it.UpdateTime); err != nil {
		return nil, err
	}
	return &it, nil
}

func (r *actionItemRepo) query(ctx context.Context, what, tail string, args []any) ([]*biz.ActionItem, error) {
	rows, err := r.data.Pool.Query(ctx, "SELECT "+itemColumns+" FROM incident_action_item a JOIN incident_record i ON i.id = a.incident_id WHERE "+tail, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", what, err)
	}
	defer rows.Close()
	out := make([]*biz.ActionItem, 0, 16)
	for rows.Next() {
		it, err := scanItem(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("%s: scan: %w", what, err)
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

// List implements biz.ActionItemRepo.
func (r *actionItemRepo) List(ctx context.Context, f biz.ActionItemFilter, page, pageSize int64) ([]*biz.ActionItem, int64, error) {
	var a sqlArgs
	where := "TRUE"
	if f.IncidentID != "" {
		where += " AND a.incident_id = " + a.add(f.IncidentID)
	}
	if f.Status != nil {
		where += " AND a.status = " + a.add(*f.Status)
	}
	if f.OwnerID != "" {
		where += " AND a.owner_id = " + a.add(f.OwnerID)
	}
	if f.Overdue {
		where += " AND a.status = 1 AND a.due_date IS NOT NULL AND a.due_date < " + a.add(f.Now.Format(time.DateOnly)) + "::date"
	}
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM incident_action_item a WHERE "+where, a.args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count incident action items: %w", err)
	}
	items, err := r.query(ctx, "list incident action items", where+" ORDER BY a.status, a.due_date NULLS LAST, a.create_time DESC LIMIT "+a.add(pageSize)+" OFFSET "+a.add((page-1)*pageSize), a.args)
	return items, total, err
}

// ListByIncident implements biz.ActionItemRepo.
func (r *actionItemRepo) ListByIncident(ctx context.Context, incidentID string) ([]*biz.ActionItem, error) {
	return r.query(ctx, "list incident action items", "a.incident_id = $1 ORDER BY a.create_time", []any{incidentID})
}

// Get implements biz.ActionItemRepo.
func (r *actionItemRepo) Get(ctx context.Context, id string) (*biz.ActionItem, error) {
	items, err := r.query(ctx, "get incident action item", "a.id = $1", []any{id})
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, biz.ErrActionItemNotFound
	}
	return items[0], nil
}

// SaveTx implements biz.ActionItemRepo.
func (r *actionItemRepo) SaveTx(ctx context.Context, tx txrun.Tx, it *biz.ActionItem) error {
	var due any
	if it.DueDate != nil {
		due = it.DueDate.Format(time.DateOnly)
	}
	if err := tx.QueryRow(ctx,
		`INSERT INTO incident_action_item (id, incident_id, title, kind, owner_id, due_date, status, ticket_id, ticket_number, create_user)
		 VALUES ($1, $2, $3, $4, $5, $6::date, $7, $8, $9, $10)
		 ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, kind = EXCLUDED.kind, owner_id = EXCLUDED.owner_id, due_date = EXCLUDED.due_date, status = EXCLUDED.status,
		   ticket_id = EXCLUDED.ticket_id, ticket_number = EXCLUDED.ticket_number, update_time = now()
		 RETURNING create_time, update_time`,
		it.ID, it.IncidentID, it.Title, it.Kind, nilIfEmpty(it.OwnerID), due, it.Status, nilIfEmpty(it.TicketID), it.TicketNumber, nilIfEmpty(it.CreateUser)).Scan(&it.CreateTime, &it.UpdateTime); err != nil {
		return fmt.Errorf("save incident action item: %w", err)
	}
	return nil
}

// DeleteTx implements biz.ActionItemRepo.
func (r *actionItemRepo) DeleteTx(ctx context.Context, tx txrun.Tx, incidentID, id string) error {
	n, err := tx.Exec(ctx, "DELETE FROM incident_action_item WHERE incident_id = $1 AND id = $2", incidentID, id)
	if err != nil {
		return fmt.Errorf("delete incident action item: %w", err)
	}
	if n == 0 {
		return biz.ErrActionItemNotFound
	}
	return nil
}

// ListTicketed implements biz.ActionItemRepo.
func (r *actionItemRepo) ListTicketed(ctx context.Context, limit int) ([]*biz.ActionItem, error) {
	return r.query(ctx, "list ticketed action items", "a.status = 1 AND a.ticket_id IS NOT NULL ORDER BY a.update_time LIMIT $1", []any{limit})
}

// Counts implements biz.ActionItemRepo.
func (r *actionItemRepo) Counts(ctx context.Context, sr biz.StatsRange) (int, int, error) {
	var a sqlArgs
	where := "i.open_time >= " + a.add(sr.Start) + " AND i.open_time < " + a.add(sr.End) + " AND NOT i.is_drill AND i.status <> 6"
	if sr.BusinessID != "" {
		where += " AND i.business_id = " + a.add(sr.BusinessID)
	}
	var total, done int
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*), count(*) FILTER (WHERE a.status = 2) FROM incident_action_item a JOIN incident_record i ON i.id = a.incident_id WHERE "+where, a.args...).Scan(&total, &done); err != nil {
		return 0, 0, fmt.Errorf("count incident action items: %w", err)
	}
	return total, done, nil
}
