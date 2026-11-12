package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

var _ biz.ViewRepo = (*viewRepo)(nil)

type viewRepo struct {
	data *basedata.Data
}

// NewViewRepo new a viewRepo.
func NewViewRepo(data *basedata.Data) *viewRepo { return &viewRepo{data: data} }

// Get implements biz.ViewRepo（未配置返回 nil, nil）。
func (r *viewRepo) Get(ctx context.Context, modelCode string) (*biz.ModelView, error) {
	var v biz.ModelView
	var raw []byte
	var updateUser *string
	err := r.data.Pool.QueryRow(ctx,
		"SELECT model_code, sections, update_user, update_time FROM cmdb_model_view WHERE model_code = $1", modelCode).
		Scan(&v.ModelCode, &raw, &updateUser, &v.UpdateTime)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get cmdb model view: %w", err)
	}
	if err := json.Unmarshal(raw, &v.Sections); err != nil {
		return nil, fmt.Errorf("decode cmdb model view: %w", err)
	}
	if updateUser != nil {
		v.UpdateUser = *updateUser
	}
	return &v, nil
}

// Upsert implements biz.ViewRepo.
func (r *viewRepo) Upsert(ctx context.Context, view *biz.ModelView) error {
	raw, err := json.Marshal(view.Sections)
	if err != nil {
		return fmt.Errorf("marshal cmdb model view: %w", err)
	}
	_, err = r.data.Pool.Exec(ctx,
		`INSERT INTO cmdb_model_view (model_code, sections, update_user, update_time) VALUES ($1, $2, $3, $4)
		 ON CONFLICT (model_code) DO UPDATE SET sections = EXCLUDED.sections, update_user = EXCLUDED.update_user, update_time = EXCLUDED.update_time`,
		view.ModelCode, raw, dbx.NilIfEmpty(view.UpdateUser), view.UpdateTime)
	if err != nil {
		if isForeignKeyViolation(err) {
			return biz.ErrModelNotFound
		}
		return fmt.Errorf("upsert cmdb model view: %w", err)
	}
	return nil
}
