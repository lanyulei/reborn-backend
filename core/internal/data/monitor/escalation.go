package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	"github.com/google/wire"
	"github.com/jackc/pgx/v5"
)

// escalationProviders 升级策略、升级进度、自监控事件发件箱的 data providers。
var escalationProviders = wire.NewSet(
	NewEscalationRepo, NewEngineEventRepo,
	wire.Bind(new(biz.EscalationReader), new(*escalationRepo)),
	wire.Bind(new(biz.EscalationWriter), new(*escalationRepo)),
	wire.Bind(new(biz.EscalationUsage), new(*escalationRepo)),
	wire.Bind(new(biz.EscalationProgress), new(*escalationRepo)),
	wire.Bind(new(biz.EngineEventRepo), new(*engineEventRepo)),
)

var (
	_ biz.EscalationReader   = (*escalationRepo)(nil)
	_ biz.EscalationWriter   = (*escalationRepo)(nil)
	_ biz.EscalationUsage    = (*escalationRepo)(nil)
	_ biz.EscalationProgress = (*escalationRepo)(nil)
)

const tableEscalation = "monitor_escalation"

// escalationColumns 升级策略查询列序（表别名 e）。
const escalationColumns = `e.id::text, COALESCE(e.group_id::text, ''), e.name, e.description, e.mode, e.min_severity, e.levels,
	e.enabled, e.version, e.create_time, e.update_time, COALESCE(e.update_user::text, '')`

func scanEscalation(scan func(dest ...any) error) (*biz.Escalation, error) {
	var e biz.Escalation
	s := &e.Spec
	var levels []byte
	if err := scan(&e.ID, &s.GroupID, &s.Name, &s.Description, &s.Mode, &s.MinSeverity, &levels,
		&s.Enabled, &e.Version, &e.CreateTime, &e.UpdateTime, &e.UpdateUser); err != nil {
		return nil, err
	}
	if err := decodeJSON(levels, &s.Levels, "escalation levels"); err != nil {
		return nil, err
	}
	return &e, nil
}

type escalationRepo struct {
	data *basedata.Data
}

// NewEscalationRepo new an escalationRepo.
func NewEscalationRepo(data *basedata.Data) *escalationRepo { return &escalationRepo{data: data} }

func (r *escalationRepo) query(ctx context.Context, what, tail string, args ...any) ([]*biz.Escalation, error) {
	rows, err := r.data.Pool.Query(ctx, "SELECT "+escalationColumns+" FROM monitor_escalation e WHERE e.delete_time IS NULL"+tail, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", what, err)
	}
	defer rows.Close()
	items := make([]*biz.Escalation, 0, 16)
	for rows.Next() {
		e, err := scanEscalation(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("%s: scan: %w", what, err)
		}
		items = append(items, e)
	}
	return items, rows.Err()
}

// Get implements biz.EscalationReader.
func (r *escalationRepo) Get(ctx context.Context, id string) (*biz.Escalation, error) {
	e, err := scanEscalation(r.data.Pool.QueryRow(ctx,
		"SELECT "+escalationColumns+" FROM monitor_escalation e WHERE e.id = $1 AND e.delete_time IS NULL", id).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrEscalationNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get monitor escalation: %w", err)
	}
	return e, nil
}

// ListByIDs implements biz.EscalationReader.
func (r *escalationRepo) ListByIDs(ctx context.Context, ids []string) ([]*biz.Escalation, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	return r.query(ctx, "list monitor escalations by ids", " AND e.id = ANY($1::uuid[]) ORDER BY e.id", ids)
}

// List implements biz.EscalationReader。全局策略在前，其余按名称排序。
func (r *escalationRepo) List(ctx context.Context, filter biz.EscalationFilter, page, pageSize int64) ([]*biz.Escalation, int64, error) {
	var a sqlArgs
	where := ""
	switch {
	case filter.Groups.All && !filter.IncludeGlobal:
		where += " AND e.group_id IS NOT NULL"
	case filter.Groups.All:
	case filter.IncludeGlobal:
		where += " AND (e.group_id IS NULL OR e.group_id = ANY(" + a.add(dbx.TextArray(filter.Groups.IDs)) + "::uuid[]))"
	default:
		where += " AND e.group_id = ANY(" + a.add(dbx.TextArray(filter.Groups.IDs)) + "::uuid[])"
	}
	if filter.Global != nil {
		where += " AND (e.group_id IS NULL) = " + a.add(*filter.Global)
	}
	if filter.Keyword != "" {
		p := a.add(dbx.LikeContains(filter.Keyword))
		where += " AND (e.name ILIKE " + p + " OR e.description ILIKE " + p + ")"
	}
	if filter.Enabled != nil {
		where += " AND e.enabled = " + a.add(*filter.Enabled)
	}
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM monitor_escalation e WHERE e.delete_time IS NULL"+where, a.args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count monitor escalations: %w", err)
	}
	limit, offset := a.add(pageSize), a.add((page-1)*pageSize)
	items, err := r.query(ctx, "list monitor escalations",
		where+" ORDER BY (e.group_id IS NULL) DESC, e.name, e.id LIMIT "+limit+" OFFSET "+offset, a.args...)
	return items, total, err
}

// RuleCounts implements biz.EscalationUsage（走 idx_monitor_rule_escalation）。
func (r *escalationRepo) RuleCounts(ctx context.Context, ids []string) (map[string]int32, error) {
	out := make(map[string]int32, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := r.data.Pool.Query(ctx,
		`SELECT escalation_id::text, count(*) FROM monitor_rule
		 WHERE escalation_id = ANY($1::uuid[]) AND delete_time IS NULL GROUP BY escalation_id`, ids)
	if err != nil {
		return nil, fmt.Errorf("count monitor escalation rules: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var n int32
		if err := rows.Scan(&id, &n); err != nil {
			return nil, fmt.Errorf("scan monitor escalation rule count: %w", err)
		}
		out[id] = n
	}
	return out, rows.Err()
}

func escalationWriteErr(err error, what string) error {
	if isUniqueViolation(err, "uk_monitor_escalation_name") {
		return biz.ErrEscalationNameExists
	}
	return fmt.Errorf("%s monitor escalation: %w", what, err)
}

func encodeLevels(levels []mc.EscalationLevel) ([]byte, error) {
	raw, err := json.Marshal(levels)
	if err != nil {
		return nil, fmt.Errorf("encode escalation levels: %w", err)
	}
	return raw, nil
}

// Create implements biz.EscalationWriter.
func (r *escalationRepo) Create(ctx context.Context, e *biz.Escalation, actor string) error {
	s := &e.Spec
	levels, err := encodeLevels(s.Levels)
	if err != nil {
		return err
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if s.GroupID != "" {
			if err := lockGroup(ctx, tx, s.GroupID); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO monitor_escalation (id, group_id, name, description, mode, min_severity, levels, enabled, create_user, update_user)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)`,
			e.ID, dbx.NilIfEmpty(s.GroupID), s.Name, s.Description, s.Mode, s.MinSeverity, levels, s.Enabled, dbx.NilIfEmpty(actor)); err != nil {
			return escalationWriteErr(err, "insert")
		}
		return bumpCfg(ctx, tx, mc.CfgEscalation)
	})
}

// Update implements biz.EscalationWriter。归属分组不在可改之列。
func (r *escalationRepo) Update(ctx context.Context, e *biz.Escalation, expectVersion int32, actor string) error {
	s := &e.Spec
	levels, err := encodeLevels(s.Levels)
	if err != nil {
		return err
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`UPDATE monitor_escalation SET name = $4, description = $5, mode = $6, min_severity = $7, levels = $8, enabled = $9,
			   version = version + 1, update_time = now(), update_user = $3
			 WHERE id = $1 AND version = $2 AND delete_time IS NULL`,
			e.ID, expectVersion, dbx.NilIfEmpty(actor), s.Name, s.Description, s.Mode, s.MinSeverity, levels, s.Enabled)
		if err != nil {
			return escalationWriteErr(err, "update")
		}
		if tag.RowsAffected() == 0 {
			return missingOrConflict(ctx, tx, tableEscalation, e.ID, biz.ErrEscalationNotFound)
		}
		return bumpCfg(ctx, tx, mc.CfgEscalation)
	})
}

// Delete implements biz.EscalationWriter。行锁策略行（FOR UPDATE）后在同一事务里数引用。规则保存时若对
// 所引用的策略行加共享锁（SELECT … FOR SHARE），两者即互斥；没有加锁时仍可能有一条并发保存的规则引用上
// 刚被删除的策略——monitor_rule.escalation_id 不设外键，引擎与派发都按「没有策略」处理这种引用。
func (r *escalationRepo) Delete(ctx context.Context, id string, expectVersion int32, actor string) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		var locked string
		err := tx.QueryRow(ctx, "SELECT id::text FROM monitor_escalation WHERE id = $1 AND delete_time IS NULL FOR UPDATE", id).Scan(&locked)
		if errors.Is(err, pgx.ErrNoRows) {
			return biz.ErrEscalationNotFound
		}
		if err != nil {
			return fmt.Errorf("lock monitor escalation: %w", err)
		}
		var rules int
		if err := tx.QueryRow(ctx,
			"SELECT count(*) FROM monitor_rule WHERE escalation_id = $1 AND delete_time IS NULL", id).Scan(&rules); err != nil {
			return fmt.Errorf("count monitor escalation rules: %w", err)
		}
		if rules > 0 {
			return biz.EscalationInUse(rules)
		}
		if err := softDelete(ctx, tx, tableEscalation, id, expectVersion, actor, biz.ErrEscalationNotFound); err != nil {
			return err
		}
		return bumpCfg(ctx, tx, mc.CfgEscalation)
	})
}

// TouchLevelTx implements biz.EscalationProgress。notify_count 只增不减（重复执行不会多加）。
func (r *escalationRepo) TouchLevelTx(ctx context.Context, tx txrun.Tx, alertID string, level, seq int32) error {
	if _, err := tx.Exec(ctx,
		`INSERT INTO monitor_alert_escalation (alert_id, level, notify_count, last_notify_time) VALUES ($1, $2, $3, now())
		 ON CONFLICT (alert_id, level) DO UPDATE
		   SET notify_count = GREATEST(monitor_alert_escalation.notify_count, EXCLUDED.notify_count), last_notify_time = now()`,
		alertID, level, seq); err != nil {
		return fmt.Errorf("touch monitor alert escalation: %w", err)
	}
	return nil
}
