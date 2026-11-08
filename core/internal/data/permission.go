package data

import (
	"context"
	"fmt"

	"github.com/lanyulei/monitor/backend/core/internal/biz"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// 编译期断言锁住 biz 契约。
var (
	_ biz.PermissionPolicySource = (*permissionRepo)(nil)
	_ biz.PermissionGrantReader  = (*permissionRepo)(nil)
	_ biz.PermissionAdminRepo    = (*permissionRepo)(nil)
)

// permissionRepo 是角色接口权限仓储的 PostgreSQL 实现。
// 关联表 system_permission 仅 create_time / create_user 审计字段，物理删除；
// 承载 Casbin p 策略，subject 经 JOIN 解析为角色 code（创建后不可变）。
type permissionRepo struct {
	data *Data
}

// NewPermissionRepo new a permissionRepo.
func NewPermissionRepo(data *Data) *permissionRepo {
	return &permissionRepo{data: data}
}

// ListPolicies implements biz.PermissionPolicySource：全量加载有效策略，
// JOIN 角色表解析 code 并剔除已软删角色；不按 status 过滤——停用角色是否
// 生效由判定链路按实时状态处理，快照无需随 status 变更刷新。
func (r *permissionRepo) ListPolicies(ctx context.Context) ([]biz.PermissionPolicy, error) {
	rows, err := r.data.pool.Query(ctx,
		"SELECT r.code, p.operation FROM system_permission p"+
			" JOIN system_role r ON r.id = p.role_id AND r.delete_time IS NULL"+
			" ORDER BY r.code, p.operation")
	if err != nil {
		return nil, fmt.Errorf("list permission policies: %w", err)
	}
	defer rows.Close()

	policies := make([]biz.PermissionPolicy, 0)
	for rows.Next() {
		var p biz.PermissionPolicy
		if err := rows.Scan(&p.RoleCode, &p.Operation); err != nil {
			return nil, fmt.Errorf("scan permission policy: %w", err)
		}
		policies = append(policies, p)
	}
	return policies, rows.Err()
}

// 资源类型字面量：system_grant.resource_type 的取值域，仅 data 层读写
// 使用（biz 侧 GrantSet 以字段区分，不经手类型字符串）。
const (
	grantTypeMenu    = "menu"
	grantTypeElement = "element"
	grantTypeAPI     = "api"
)

// ListGrantsByRole implements biz.PermissionGrantReader：三类直接授权，
// 按 (resource_type, resource_code) 字典序后归入 GrantSet。
func (r *permissionRepo) ListGrantsByRole(ctx context.Context, roleID string) (biz.GrantSet, error) {
	rows, err := r.data.pool.Query(ctx,
		"SELECT resource_type, resource_code FROM system_grant WHERE role_id = $1"+
			" ORDER BY resource_type, resource_code", roleID)
	if err != nil {
		return biz.GrantSet{}, fmt.Errorf("list role grants: %w", err)
	}
	defer rows.Close()

	var grants biz.GrantSet
	for rows.Next() {
		var kind, code string
		if err := rows.Scan(&kind, &code); err != nil {
			return biz.GrantSet{}, fmt.Errorf("scan role grant: %w", err)
		}
		switch kind {
		case grantTypeMenu:
			grants.Menus = append(grants.Menus, code)
		case grantTypeElement:
			grants.Elements = append(grants.Elements, code)
		case grantTypeAPI:
			grants.Apis = append(grants.Apis, code)
		}
	}
	return grants, rows.Err()
}

// ReplaceForRole implements biz.PermissionAdminRepo：单事务内全量替换
// 直接授权（system_grant）与物化策略（system_permission），两表先物理删除
// 再批量插入，原子一致；批量插入经 pgx.Batch 单次往返。create_user 当前置
// NULL，待认证中间件注入操作者后补。
func (r *permissionRepo) ReplaceForRole(ctx context.Context, roleID string, grants biz.GrantSet, operations []string) error {
	tx, err := r.data.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, "DELETE FROM system_grant WHERE role_id = $1", roleID); err != nil {
		return fmt.Errorf("clear role grants: %w", err)
	}
	if _, err := tx.Exec(ctx, "DELETE FROM system_permission WHERE role_id = $1", roleID); err != nil {
		return fmt.Errorf("clear role permissions: %w", err)
	}

	total := len(grants.Menus) + len(grants.Elements) + len(grants.Apis) + len(operations)
	if total > 0 {
		batch := &pgx.Batch{}
		for kind, codes := range map[string][]string{
			grantTypeMenu:    grants.Menus,
			grantTypeElement: grants.Elements,
			grantTypeAPI:     grants.Apis,
		} {
			for _, code := range codes {
				if err := queueGrantInsert(batch, roleID, kind, code); err != nil {
					return err
				}
			}
		}
		for _, op := range operations {
			id, err := uuid.NewV7()
			if err != nil {
				return fmt.Errorf("generate permission id: %w", err)
			}
			batch.Queue(
				"INSERT INTO system_permission (id, role_id, operation) VALUES ($1, $2, $3)",
				id.String(), roleID, op)
		}
		results := tx.SendBatch(ctx, batch)
		for range total {
			if _, err := results.Exec(); err != nil {
				_ = results.Close()
				return fmt.Errorf("insert role authorization: %w", err)
			}
		}
		if err := results.Close(); err != nil {
			return fmt.Errorf("close authorization batch: %w", err)
		}
	}
	return tx.Commit(ctx)
}

// queueGrantInsert 生成 system_grant 行的 UUIDv7 并入队插入。
func queueGrantInsert(batch *pgx.Batch, roleID, kind, code string) error {
	id, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("generate grant id: %w", err)
	}
	batch.Queue(
		"INSERT INTO system_grant (id, role_id, resource_type, resource_code) VALUES ($1, $2, $3, $4)",
		id.String(), roleID, kind, code)
	return nil
}
