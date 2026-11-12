package data

import (
	"context"
	"fmt"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"
)

var _ biz.ScopeRepo = (*scopeRepo)(nil)

type scopeRepo struct {
	data *basedata.Data
}

// NewScopeRepo new a scopeRepo.
func NewScopeRepo(data *basedata.Data) *scopeRepo { return &scopeRepo{data: data} }

const scopeSelectColumns = "role_id, model_codes, envs, update_user, update_time"

func (r *scopeRepo) query(ctx context.Context, where string, args ...any) ([]*biz.RoleScope, error) {
	rows, err := r.data.Pool.Query(ctx, "SELECT "+scopeSelectColumns+" FROM cmdb_role_scope "+where, args...)
	if err != nil {
		return nil, fmt.Errorf("list cmdb role scopes: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.RoleScope, 0, 8)
	for rows.Next() {
		var rs biz.RoleScope
		var updateUser *string
		if err := rows.Scan(&rs.RoleID, &rs.ModelCodes, &rs.Envs, &updateUser, &rs.UpdateTime); err != nil {
			return nil, fmt.Errorf("scan cmdb role scope: %w", err)
		}
		if updateUser != nil {
			rs.UpdateUser = *updateUser
		}
		items = append(items, &rs)
	}
	return items, rows.Err()
}

// ListByRoles implements biz.ScopeRepo.
func (r *scopeRepo) ListByRoles(ctx context.Context, roleIDs []string) ([]*biz.RoleScope, error) {
	if len(roleIDs) == 0 {
		return nil, nil
	}
	return r.query(ctx, "WHERE role_id = ANY($1::uuid[])", dbx.TextArray(roleIDs))
}

// ListAll implements biz.ScopeRepo.
func (r *scopeRepo) ListAll(ctx context.Context) ([]*biz.RoleScope, error) {
	return r.query(ctx, "")
}

// Upsert implements biz.ScopeRepo.
func (r *scopeRepo) Upsert(ctx context.Context, rs *biz.RoleScope) error {
	_, err := r.data.Pool.Exec(ctx,
		`INSERT INTO cmdb_role_scope (role_id, model_codes, envs, update_user, update_time) VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (role_id) DO UPDATE SET model_codes = EXCLUDED.model_codes, envs = EXCLUDED.envs,
		   update_user = EXCLUDED.update_user, update_time = EXCLUDED.update_time`,
		rs.RoleID, dbx.TextArray(rs.ModelCodes), dbx.TextArray(rs.Envs), dbx.NilIfEmpty(rs.UpdateUser), rs.UpdateTime)
	if err != nil {
		return fmt.Errorf("upsert cmdb role scope: %w", err)
	}
	return nil
}
