// Package data 实现 cmdb 域 biz 仓储接口：手写参数化 SQL，PO/DO 显式转换；
// 资源/关系写操作与变更历史同事务。
package data

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var _ biz.ModelRepo = (*modelRepo)(nil)

// modelSelectColumns 类型查询列序（含未删除资源数子查询）。
const modelSelectColumns = `m.code, m.name, m.category, m.icon, m.description, m.status, m.builtin,
	(SELECT count(*) FROM cmdb_ci c WHERE c.model_code = m.code AND c.delete_time IS NULL), m.create_time, m.update_time`

func scanModel(scan func(dest ...any) error) (*biz.Model, error) {
	var m biz.Model
	var status int16
	if err := scan(&m.Code, &m.Name, &m.Category, &m.Icon, &m.Description, &status, &m.Builtin,
		&m.CICount, &m.CreateTime, &m.UpdateTime); err != nil {
		return nil, err
	}
	m.Status = int32(status)
	return &m, nil
}

// isUniqueViolation 报告是否命中指定唯一约束/索引。
func isUniqueViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation &&
		(constraint == "" || pgErr.ConstraintName == constraint)
}

// isForeignKeyViolation 报告是否外键冲突。
func isForeignKeyViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.ForeignKeyViolation
}

type modelRepo struct {
	data *basedata.Data
}

// NewModelRepo new a modelRepo.
func NewModelRepo(data *basedata.Data) *modelRepo { return &modelRepo{data: data} }

// Create implements biz.ModelRepo.
func (r *modelRepo) Create(ctx context.Context, m *biz.Model) error {
	_, err := r.data.Pool.Exec(ctx,
		`INSERT INTO cmdb_model (code, name, category, icon, description, status, builtin, create_time, update_time)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		m.Code, m.Name, m.Category, m.Icon, m.Description, m.Status, m.Builtin, m.CreateTime, m.UpdateTime)
	if err != nil {
		if isUniqueViolation(err, "cmdb_model_pkey") {
			return biz.ErrModelCodeExists
		}
		return fmt.Errorf("insert cmdb model: %w", err)
	}
	return nil
}

// Get implements biz.ModelRepo.
func (r *modelRepo) Get(ctx context.Context, code string) (*biz.Model, error) {
	row := r.data.Pool.QueryRow(ctx,
		"SELECT "+modelSelectColumns+" FROM cmdb_model m WHERE m.code = $1 AND m.delete_time IS NULL", code)
	m, err := scanModel(row.Scan)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrModelNotFound
		}
		return nil, fmt.Errorf("get cmdb model: %w", err)
	}
	return m, nil
}

// List implements biz.ModelRepo.
func (r *modelRepo) List(ctx context.Context, filter biz.ModelFilter, page, pageSize int64) ([]*biz.Model, int64, error) {
	where := []string{"m.delete_time IS NULL"}
	args := make([]any, 0, 5)
	if filter.Keyword != "" {
		args = append(args, dbx.LikeContains(filter.Keyword))
		n := strconv.Itoa(len(args))
		where = append(where, "(m.code ILIKE $"+n+" OR m.name ILIKE $"+n+")")
	}
	if filter.Category != "" {
		args = append(args, filter.Category)
		where = append(where, "m.category = $"+strconv.Itoa(len(args)))
	}
	if filter.Status != nil {
		args = append(args, *filter.Status)
		where = append(where, "m.status = $"+strconv.Itoa(len(args)))
	}
	whereClause := strings.Join(where, " AND ")
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM cmdb_model m WHERE "+whereClause, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count cmdb models: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}
	args = append(args, pageSize, (page-1)*pageSize)
	rows, err := r.data.Pool.Query(ctx,
		"SELECT "+modelSelectColumns+" FROM cmdb_model m WHERE "+whereClause+
			" ORDER BY m.create_time, m.code LIMIT $"+strconv.Itoa(len(args)-1)+" OFFSET $"+strconv.Itoa(len(args)), args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list cmdb models: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.Model, 0, pageSize)
	for rows.Next() {
		m, err := scanModel(rows.Scan)
		if err != nil {
			return nil, 0, fmt.Errorf("scan cmdb model: %w", err)
		}
		items = append(items, m)
	}
	return items, total, rows.Err()
}

// Update implements biz.ModelRepo.
func (r *modelRepo) Update(ctx context.Context, code string, upd biz.ModelUpdate) error {
	sets := make([]string, 0, 5)
	args := make([]any, 0, 6)
	add := func(column string, value any) {
		args = append(args, value)
		sets = append(sets, column+" = $"+strconv.Itoa(len(args)))
	}
	if upd.Name != nil {
		add("name", *upd.Name)
	}
	if upd.Category != nil {
		add("category", *upd.Category)
	}
	if upd.Icon != nil {
		add("icon", *upd.Icon)
	}
	if upd.Description != nil {
		add("description", *upd.Description)
	}
	if upd.Status != nil {
		add("status", *upd.Status)
	}
	if len(sets) == 0 {
		return nil
	}
	args = append(args, code)
	tag, err := r.data.Pool.Exec(ctx, "UPDATE cmdb_model SET "+strings.Join(sets, ", ")+
		", update_time = now() WHERE code = $"+strconv.Itoa(len(args))+" AND delete_time IS NULL", args...)
	if err != nil {
		return fmt.Errorf("update cmdb model: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrModelNotFound
	}
	return nil
}

// SoftDelete implements biz.ModelRepo.
func (r *modelRepo) SoftDelete(ctx context.Context, code string, at time.Time) error {
	tag, err := r.data.Pool.Exec(ctx,
		"UPDATE cmdb_model SET delete_time = $2 WHERE code = $1 AND delete_time IS NULL", code, at)
	if err != nil {
		return fmt.Errorf("soft delete cmdb model: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrModelNotFound
	}
	return nil
}
