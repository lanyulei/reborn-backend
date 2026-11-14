package data

import (
	"context"
	"errors"
	"fmt"

	"github.com/lanyulei/reborn/backend/cmdb/internal/biz"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.MetaRepo   = (*metaRepo)(nil)
	_ biz.SourceRepo = (*sourceRepo)(nil)
)

type metaRepo struct {
	data *Data
}

// NewMetaRepo new a metaRepo.
func NewMetaRepo(data *Data) *metaRepo { return &metaRepo{data: data} }

// Fields implements biz.MetaRepo（类型须启用且未删除；字段未废弃）。
func (r *metaRepo) Fields(ctx context.Context, modelCode string) ([]biz.ModelField, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT f.code, f.owner FROM cmdb_model_field f JOIN cmdb_model m ON m.code = f.model_code
		 WHERE f.model_code = $1 AND NOT f.deprecated AND m.status = 1 AND m.delete_time IS NULL ORDER BY f.sort, f.code`, modelCode)
	if err != nil {
		return nil, fmt.Errorf("query model fields: %w", err)
	}
	defer rows.Close()
	out := make([]biz.ModelField, 0, 8)
	for rows.Next() {
		var f biz.ModelField
		if err := rows.Scan(&f.Code, &f.Owner); err != nil {
			return nil, fmt.Errorf("scan model field: %w", err)
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

type sourceRepo struct {
	data *Data
}

// NewSourceRepo new a sourceRepo.
func NewSourceRepo(data *Data) *sourceRepo { return &sourceRepo{data: data} }

// ErrSourceNotFound 采集源不存在或已删除。
var ErrSourceNotFound = errors.New("sync source not found")

// Get implements biz.SourceRepo（含凭据密文；已删除视为不存在）。
func (r *sourceRepo) Get(ctx context.Context, id string) (*biz.Source, error) {
	var s biz.Source
	err := r.data.Pool.QueryRow(ctx,
		`SELECT id, name, provider, config, credential, env, interval_seconds, grace_period_seconds, enabled
		 FROM cmdb_sync_source WHERE id = $1 AND delete_time IS NULL`, id).
		Scan(&s.ID, &s.Name, &s.Provider, &s.Config, &s.Credential, &s.Env, &s.IntervalSeconds, &s.GracePeriodSeconds, &s.Enabled)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrSourceNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get sync source: %w", err)
	}
	return &s, nil
}
