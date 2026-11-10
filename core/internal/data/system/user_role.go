package data

import (
	"context"
	"fmt"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"

	"github.com/jackc/pgx/v5"
)

// 编译期断言锁住 biz 契约。
var _ biz.UserRoleRepo = (*userRoleRepo)(nil)

// userRoleRepo 是用户-角色绑定仓储的 PostgreSQL 实现。
// 关联表 system_user_role 仅 create_time / create_user 两个审计字段，解绑即物理删除。
type userRoleRepo struct {
	data *basedata.Data
}

// NewUserRoleRepo new a userRoleRepo.
func NewUserRoleRepo(data *basedata.Data) *userRoleRepo {
	return &userRoleRepo{data: data}
}

// BindUserRoles implements biz.UserRoleRepo：事务内全量覆盖（先物理删除旧绑定，
// 再批量插入新绑定）；批量插入经 pgx.Batch 单次往返，无逐行 N+1。
func (r *userRoleRepo) BindUserRoles(ctx context.Context, userID string, roleIDs []string) error {
	return basedata.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "DELETE FROM system_user_role WHERE user_id = $1", userID); err != nil {
			return fmt.Errorf("clear user role bindings: %w", err)
		}
		if len(roleIDs) > 0 {
			batch := &pgx.Batch{}
			for _, roleID := range roleIDs {
				id, err := basedata.NewID()
				if err != nil {
					return fmt.Errorf("generate binding id: %w", err)
				}
				batch.Queue(
					"INSERT INTO system_user_role (id, user_id, role_id) VALUES ($1, $2, $3)",
					id, userID, roleID)
			}
			results := tx.SendBatch(ctx, batch)
			for range roleIDs {
				if _, err := results.Exec(); err != nil {
					_ = results.Close()
					return fmt.Errorf("insert user role binding: %w", err)
				}
			}
			if err := results.Close(); err != nil {
				return fmt.Errorf("close binding batch: %w", err)
			}
		}
		return nil
	})
}

// ListUserRoles implements biz.UserRoleRepo：关联表 join 角色表，过滤已软删角色。
func (r *userRoleRepo) ListUserRoles(ctx context.Context, userID string) ([]*biz.Role, error) {
	rows, err := r.data.Pool.Query(ctx,
		"SELECT r.id, r.name, r.code, r.status, r.biz_scope, r.create_time, r.update_time"+
			" FROM system_user_role ur JOIN system_role r ON r.id = ur.role_id AND r.delete_time IS NULL"+
			" WHERE ur.user_id = $1 ORDER BY r.code", userID)
	if err != nil {
		return nil, fmt.Errorf("list user roles: %w", err)
	}
	defer rows.Close()

	roles := make([]*biz.Role, 0)
	for rows.Next() {
		role, err := scanRole(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("scan role: %w", err)
		}
		roles = append(roles, role)
	}
	return roles, rows.Err()
}
