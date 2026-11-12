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

	"github.com/jackc/pgx/v5"
)

var _ biz.RelationTypeRepo = (*relationTypeRepo)(nil)

const relationTypeSelectColumns = `t.code, t.name, t.forward_name, t.backward_name, t.src_model, t.dst_model, t.cardinality, t.on_delete,
	t.description, (SELECT count(*) FROM cmdb_relation r WHERE r.type_code = t.code), t.create_time, t.update_time`

func scanRelationType(scan func(dest ...any) error) (*biz.RelationType, error) {
	var t biz.RelationType
	if err := scan(&t.Code, &t.Name, &t.ForwardName, &t.BackwardName, &t.SrcModel, &t.DstModel, &t.Cardinality, &t.OnDelete,
		&t.Description, &t.RelationCount, &t.CreateTime, &t.UpdateTime); err != nil {
		return nil, err
	}
	return &t, nil
}

type relationTypeRepo struct {
	data *basedata.Data
}

// NewRelationTypeRepo new a relationTypeRepo.
func NewRelationTypeRepo(data *basedata.Data) *relationTypeRepo { return &relationTypeRepo{data: data} }

// Create implements biz.RelationTypeRepo.
func (r *relationTypeRepo) Create(ctx context.Context, t *biz.RelationType) error {
	_, err := r.data.Pool.Exec(ctx,
		`INSERT INTO cmdb_relation_type (code, name, forward_name, backward_name, src_model, dst_model, cardinality, on_delete, description, create_time, update_time)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		t.Code, t.Name, t.ForwardName, t.BackwardName, t.SrcModel, t.DstModel, t.Cardinality, t.OnDelete, t.Description, t.CreateTime, t.UpdateTime)
	if err != nil {
		if isUniqueViolation(err, "cmdb_relation_type_pkey") {
			return biz.ErrRelationTypeCodeExists
		}
		return fmt.Errorf("insert cmdb relation type: %w", err)
	}
	return nil
}

// Get implements biz.RelationTypeRepo.
func (r *relationTypeRepo) Get(ctx context.Context, code string) (*biz.RelationType, error) {
	row := r.data.Pool.QueryRow(ctx,
		"SELECT "+relationTypeSelectColumns+" FROM cmdb_relation_type t WHERE t.code = $1 AND t.delete_time IS NULL", code)
	t, err := scanRelationType(row.Scan)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrRelationTypeNotFound
		}
		return nil, fmt.Errorf("get cmdb relation type: %w", err)
	}
	return t, nil
}

// List implements biz.RelationTypeRepo.
func (r *relationTypeRepo) List(ctx context.Context, modelCode string) ([]*biz.RelationType, error) {
	where := "t.delete_time IS NULL"
	args := []any{}
	if modelCode != "" {
		args = append(args, modelCode)
		where += " AND (t.src_model = '*' OR t.src_model = $1 OR t.dst_model = '*' OR t.dst_model = $1)"
	}
	rows, err := r.data.Pool.Query(ctx,
		"SELECT "+relationTypeSelectColumns+" FROM cmdb_relation_type t WHERE "+where+" ORDER BY t.create_time, t.code", args...)
	if err != nil {
		return nil, fmt.Errorf("list cmdb relation types: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.RelationType, 0, 8)
	for rows.Next() {
		t, err := scanRelationType(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("scan cmdb relation type: %w", err)
		}
		items = append(items, t)
	}
	return items, rows.Err()
}

// Update implements biz.RelationTypeRepo.
func (r *relationTypeRepo) Update(ctx context.Context, code string, upd biz.RelationTypeUpdate) error {
	sets := make([]string, 0, 5)
	args := make([]any, 0, 6)
	add := func(column string, value any) {
		args = append(args, value)
		sets = append(sets, column+" = $"+strconv.Itoa(len(args)))
	}
	if upd.Name != nil {
		add("name", *upd.Name)
	}
	if upd.ForwardName != nil {
		add("forward_name", *upd.ForwardName)
	}
	if upd.BackwardName != nil {
		add("backward_name", *upd.BackwardName)
	}
	if upd.OnDelete != nil {
		add("on_delete", *upd.OnDelete)
	}
	if upd.Description != nil {
		add("description", *upd.Description)
	}
	if len(sets) == 0 {
		return nil
	}
	args = append(args, code)
	tag, err := r.data.Pool.Exec(ctx, "UPDATE cmdb_relation_type SET "+strings.Join(sets, ", ")+
		", update_time = now() WHERE code = $"+strconv.Itoa(len(args))+" AND delete_time IS NULL", args...)
	if err != nil {
		return fmt.Errorf("update cmdb relation type: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrRelationTypeNotFound
	}
	return nil
}

// SoftDelete implements biz.RelationTypeRepo.
func (r *relationTypeRepo) SoftDelete(ctx context.Context, code string, at time.Time) error {
	tag, err := r.data.Pool.Exec(ctx,
		"UPDATE cmdb_relation_type SET delete_time = $2 WHERE code = $1 AND delete_time IS NULL", code, at)
	if err != nil {
		return fmt.Errorf("soft delete cmdb relation type: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrRelationTypeNotFound
	}
	return nil
}
