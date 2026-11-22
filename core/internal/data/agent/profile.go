// Package data 运维 Agent 域仓储实现（pgx + PostgreSQL，手写参数化 SQL）。本域所有表（agent_*）只有 core 读写；
// incident 域在自己的事务里调用 EnqueueTx 时传入 txrun 句柄，这里经 basedata.PgxTx 还原为 pgx.Tx。
package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/agent"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/google/wire"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ProviderSet is 运维 Agent 域 data providers。
var ProviderSet = wire.NewSet(
	NewProfileRepo, NewTaskRepo, NewRunRepo, NewBudgetRepo,
	wire.Bind(new(biz.ProfileRepo), new(*profileRepo)),
	wire.Bind(new(biz.TaskRepo), new(*taskRepo)),
	wire.Bind(new(biz.RunRepo), new(*runRepo)),
	wire.Bind(new(biz.BudgetRepo), new(*budgetRepo)),
)

var _ biz.ProfileRepo = (*profileRepo)(nil)

// isUniqueViolation 报告是否唯一约束冲突（constraint 为空不区分约束）。
// dup: 与 data/incident 等域各有一份相同的实现（各域 data 包不能互相 import）。
func isUniqueViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation &&
		(constraint == "" || pgErr.ConstraintName == constraint)
}

func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nilTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return *t
}

func encodeJSON(v any, what string) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("encode %s: %w", what, err)
	}
	return raw, nil
}

func decodeJSON(raw []byte, dst any, what string) error {
	if len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		return fmt.Errorf("decode %s: %w", what, err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Profile
// ---------------------------------------------------------------------------

type profileRepo struct {
	data *basedata.Data
}

// NewProfileRepo new a profileRepo.
func NewProfileRepo(data *basedata.Data) *profileRepo { return &profileRepo{data: data} }

const profileColumns = `p.id::text, p.spec, p.breaker_open, p.breaker_reason, p.version, p.create_time, p.update_time,
	COALESCE(p.create_user::text, ''), COALESCE(p.update_user::text, '')`

func scanProfile(scan func(dest ...any) error) (*biz.Profile, error) {
	var p biz.Profile
	var spec []byte
	if err := scan(&p.ID, &spec, &p.BreakerOpen, &p.BreakerReason, &p.Version, &p.CreateTime, &p.UpdateTime, &p.CreateUser, &p.UpdateUser); err != nil {
		return nil, err
	}
	if err := decodeJSON(spec, &p.Spec, "agent profile spec"); err != nil {
		return nil, err
	}
	return &p, nil
}

func (r *profileRepo) query(ctx context.Context, what, tail string, args ...any) ([]*biz.Profile, error) {
	rows, err := r.data.Pool.Query(ctx, "SELECT "+profileColumns+" FROM agent_profile p WHERE p.delete_time IS NULL"+tail, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", what, err)
	}
	defer rows.Close()
	out := make([]*biz.Profile, 0, 8)
	for rows.Next() {
		p, err := scanProfile(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("%s: scan: %w", what, err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// List implements biz.ProfileRepo.
func (r *profileRepo) List(ctx context.Context, keyword string, enabled *bool, page, pageSize int64) ([]*biz.Profile, int64, error) {
	where := []string{"delete_time IS NULL"}
	args := make([]any, 0, 4)
	if keyword != "" {
		args = append(args, dbx.LikeContains(keyword))
		where = append(where, "name ILIKE $"+strconv.Itoa(len(args)))
	}
	if enabled != nil {
		args = append(args, *enabled)
		where = append(where, "enabled = $"+strconv.Itoa(len(args)))
	}
	clause := strings.Join(where, " AND ")
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM agent_profile WHERE "+clause, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count agent profiles: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}
	args = append(args, pageSize, (page-1)*pageSize)
	rows, err := r.data.Pool.Query(ctx, "SELECT "+profileColumns+" FROM agent_profile p WHERE "+strings.ReplaceAll(clause, "name ILIKE", "p.name ILIKE")+
		" ORDER BY p.priority, p.create_time LIMIT $"+strconv.Itoa(len(args)-1)+" OFFSET $"+strconv.Itoa(len(args)), args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list agent profiles: %w", err)
	}
	defer rows.Close()
	out := make([]*biz.Profile, 0, pageSize)
	for rows.Next() {
		p, err := scanProfile(rows.Scan)
		if err != nil {
			return nil, 0, fmt.Errorf("scan agent profile: %w", err)
		}
		out = append(out, p)
	}
	return out, total, rows.Err()
}

// ListEnabled implements biz.ProfileRepo.
func (r *profileRepo) ListEnabled(ctx context.Context, kind string) ([]*biz.Profile, error) {
	return r.query(ctx, "list enabled agent profiles", " AND p.enabled AND p.kind = $1 ORDER BY p.priority, p.create_time", kind)
}

// Get implements biz.ProfileRepo.
func (r *profileRepo) Get(ctx context.Context, id string) (*biz.Profile, error) {
	p, err := scanProfile(r.data.Pool.QueryRow(ctx, "SELECT "+profileColumns+" FROM agent_profile p WHERE p.id = $1 AND p.delete_time IS NULL", id).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrProfileNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get agent profile: %w", err)
	}
	return p, nil
}

// ListByIDs implements biz.ProfileRepo（含已删除的：任务 / 运行列表要显示名字）。
func (r *profileRepo) ListByIDs(ctx context.Context, ids []string) ([]*biz.Profile, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := r.data.Pool.Query(ctx, "SELECT "+profileColumns+" FROM agent_profile p WHERE p.id::text = ANY($1)", ids)
	if err != nil {
		return nil, fmt.Errorf("list agent profiles by ids: %w", err)
	}
	defer rows.Close()
	out := make([]*biz.Profile, 0, len(ids))
	for rows.Next() {
		p, err := scanProfile(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("scan agent profile: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// Create implements biz.ProfileRepo.
func (r *profileRepo) Create(ctx context.Context, p *biz.Profile) error {
	spec, err := encodeJSON(p.Spec, "agent profile spec")
	if err != nil {
		return err
	}
	if _, err := r.data.Pool.Exec(ctx,
		`INSERT INTO agent_profile (id, name, kind, agent_user_id, priority, enabled, spec, version, create_time, update_time, create_user, update_user)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9, $10, $10)`,
		p.ID, p.Spec.Name, p.Spec.Kind, p.Spec.AgentUserID, p.Spec.Priority, p.Spec.Enabled, spec, p.Version, p.CreateTime, nilIfEmpty(p.CreateUser)); err != nil {
		if isUniqueViolation(err, "uk_agent_profile_name") {
			return biz.ErrProfileNameExists
		}
		return fmt.Errorf("insert agent profile: %w", err)
	}
	return nil
}

// Update implements biz.ProfileRepo.
func (r *profileRepo) Update(ctx context.Context, p *biz.Profile, expectVersion int32) error {
	spec, err := encodeJSON(p.Spec, "agent profile spec")
	if err != nil {
		return err
	}
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE agent_profile SET name = $3, kind = $4, agent_user_id = $5, priority = $6, enabled = $7, spec = $8, version = version + 1, update_time = now(), update_user = $9
		 WHERE id = $1 AND version = $2 AND delete_time IS NULL`,
		p.ID, expectVersion, p.Spec.Name, p.Spec.Kind, p.Spec.AgentUserID, p.Spec.Priority, p.Spec.Enabled, spec, nilIfEmpty(p.UpdateUser))
	if err != nil {
		if isUniqueViolation(err, "uk_agent_profile_name") {
			return biz.ErrProfileNameExists
		}
		return fmt.Errorf("update agent profile: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return r.missingOrConflict(ctx, p.ID)
	}
	p.Version = expectVersion + 1
	return nil
}

// SetBreaker implements biz.ProfileRepo.
func (r *profileRepo) SetBreaker(ctx context.Context, id string, open bool, reason string) error {
	if _, err := r.data.Pool.Exec(ctx, "UPDATE agent_profile SET breaker_open = $2, breaker_reason = $3, update_time = now() WHERE id = $1 AND delete_time IS NULL", id, open, reason); err != nil {
		return fmt.Errorf("set agent profile breaker: %w", err)
	}
	return nil
}

// Delete implements biz.ProfileRepo.
func (r *profileRepo) Delete(ctx context.Context, id string, expectVersion int32, actor string) error {
	tag, err := r.data.Pool.Exec(ctx, "UPDATE agent_profile SET delete_time = now(), update_time = now(), update_user = $3 WHERE id = $1 AND version = $2 AND delete_time IS NULL", id, expectVersion, nilIfEmpty(actor))
	if err != nil {
		return fmt.Errorf("delete agent profile: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return r.missingOrConflict(ctx, id)
	}
	return nil
}

// missingOrConflict 0 行更新：不存在 → NotFound，否则版本冲突。
func (r *profileRepo) missingOrConflict(ctx context.Context, id string) error {
	var n int
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM agent_profile WHERE id = $1 AND delete_time IS NULL", id).Scan(&n); err != nil {
		return fmt.Errorf("check agent profile: %w", err)
	}
	if n == 0 {
		return biz.ErrProfileNotFound
	}
	return biz.ErrVersionConflict
}

// AppendHistory implements biz.ProfileRepo.
func (r *profileRepo) AppendHistory(ctx context.Context, h *biz.ProfileHistory) error {
	after, err := encodeJSON(h.After, "agent profile history")
	if err != nil {
		return err
	}
	var before any
	if h.Before != nil {
		raw, err := encodeJSON(*h.Before, "agent profile history")
		if err != nil {
			return err
		}
		before = raw
	}
	if _, err := r.data.Pool.Exec(ctx,
		"INSERT INTO agent_profile_history (id, profile_id, version, actor_id, before, after, note, create_time) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
		h.ID, h.ProfileID, h.Version, nilIfEmpty(h.ActorID), before, after, h.Note, h.CreateTime); err != nil {
		return fmt.Errorf("insert agent profile history: %w", err)
	}
	return nil
}

// ListHistory implements biz.ProfileRepo.
func (r *profileRepo) ListHistory(ctx context.Context, profileID string, page, pageSize int64) ([]*biz.ProfileHistory, int64, error) {
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM agent_profile_history WHERE profile_id = $1", profileID).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count agent profile history: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}
	rows, err := r.data.Pool.Query(ctx,
		`SELECT id::text, profile_id::text, version, COALESCE(actor_id::text, ''), before, after, note, create_time
		 FROM agent_profile_history WHERE profile_id = $1 ORDER BY create_time DESC, id DESC LIMIT $2 OFFSET $3`, profileID, pageSize, (page-1)*pageSize)
	if err != nil {
		return nil, 0, fmt.Errorf("list agent profile history: %w", err)
	}
	defer rows.Close()
	out := make([]*biz.ProfileHistory, 0, pageSize)
	for rows.Next() {
		var h biz.ProfileHistory
		var before, after []byte
		if err := rows.Scan(&h.ID, &h.ProfileID, &h.Version, &h.ActorID, &before, &after, &h.Note, &h.CreateTime); err != nil {
			return nil, 0, fmt.Errorf("scan agent profile history: %w", err)
		}
		if len(before) > 0 {
			h.Before = &biz.ProfileSpec{}
			if err := decodeJSON(before, h.Before, "agent profile history before"); err != nil {
				return nil, 0, err
			}
		}
		if err := decodeJSON(after, &h.After, "agent profile history after"); err != nil {
			return nil, 0, err
		}
		out = append(out, &h)
	}
	return out, total, rows.Err()
}
