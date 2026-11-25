package data

import (
	"context"
	"fmt"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
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
	data *basedata.Data
}

// NewPermissionRepo new a permissionRepo.
func NewPermissionRepo(data *basedata.Data) *permissionRepo {
	return &permissionRepo{data: data}
}

// ListPolicies implements biz.PermissionPolicySource：全量加载有效策略，
// JOIN 角色表解析 code 并剔除已软删角色；不按 status 过滤——停用角色是否
// 生效由判定链路按实时状态处理，快照无需随 status 变更刷新。
func (r *permissionRepo) ListPolicies(ctx context.Context) ([]biz.PermissionPolicy, error) {
	rows, err := r.data.Pool.Query(ctx,
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
	rows, err := r.data.Pool.Query(ctx,
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

// ListGrantsByRoles implements biz.PermissionGrantReader：单条查询多角色
// 直接授权并集（DISTINCT 去重，按 (resource_type, resource_code) 字典序）；
// 空输入直接返回空集不查库。
func (r *permissionRepo) ListGrantsByRoles(ctx context.Context, roleIDs []string) (biz.GrantSet, error) {
	if len(roleIDs) == 0 {
		return biz.GrantSet{}, nil
	}
	rows, err := r.data.Pool.Query(ctx,
		"SELECT DISTINCT resource_type, resource_code FROM system_grant"+
			" WHERE role_id = ANY($1::uuid[]) ORDER BY resource_type, resource_code", roleIDs)
	if err != nil {
		return biz.GrantSet{}, fmt.Errorf("list roles grants: %w", err)
	}
	defer rows.Close()

	var grants biz.GrantSet
	for rows.Next() {
		var kind, code string
		if err := rows.Scan(&kind, &code); err != nil {
			return biz.GrantSet{}, fmt.Errorf("scan roles grant: %w", err)
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

// ReplaceForRole implements biz.PermissionAdminRepo：在调用方事务内全量替换
// 直接授权（system_grant）与物化策略（system_permission），两表先物理删除
// 再插入，原子一致。事务句柄由用例层传入（txrun.Tx，与通知事件直插同一
// 事务，notify 设计 §11）；插入经窄句柄 Exec（事务边界归用例层所有，不用
// pgx.Batch），每类授权一条 unnest 批量语句。
// create_user 当前置 NULL，待认证中间件注入操作者后补。
func (r *permissionRepo) ReplaceForRole(ctx context.Context, tx txrun.Tx, roleID string, grants biz.GrantSet, operations []string) error {
	// 同一角色的两次并发保存（角色此前没有授权行时，两个事务的 DELETE 都不影响任何行、随后各自插入）
	// 会撞唯一索引、后到的一方 500。事务级咨询锁把同一角色的全量替换串行化，不同角色互不阻塞。
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended('system_grant:' || $1, 0))", roleID); err != nil {
		return fmt.Errorf("lock role grants: %w", err)
	}
	if _, err := tx.Exec(ctx, "DELETE FROM system_grant WHERE role_id = $1", roleID); err != nil {
		return fmt.Errorf("clear role grants: %w", err)
	}
	if _, err := tx.Exec(ctx, "DELETE FROM system_permission WHERE role_id = $1", roleID); err != nil {
		return fmt.Errorf("clear role permissions: %w", err)
	}

	// 每类授权一条 unnest 批量插入：一次保存最多几百行，逐行往返会把持锁（上面的咨询锁）时间
	// 拉长到几百个网络往返。行内容与逐行插入完全一致（主键仍由应用侧逐行生成）。
	insert := func(kind string, codes []string) error {
		if len(codes) == 0 {
			return nil
		}
		ids, err := newIDs(len(codes))
		if err != nil {
			return fmt.Errorf("generate grant id: %w", err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO system_grant (id, role_id, resource_type, resource_code)
			 SELECT t.id::uuid, $1::uuid, $2::text, t.code
			 FROM unnest($3::text[], $4::text[]) AS t(id, code)`,
			roleID, kind, ids, codes); err != nil {
			return fmt.Errorf("insert role grant: %w", err)
		}
		return nil
	}
	if err := insert(grantTypeMenu, grants.Menus); err != nil {
		return err
	}
	if err := insert(grantTypeElement, grants.Elements); err != nil {
		return err
	}
	if err := insert(grantTypeAPI, grants.Apis); err != nil {
		return err
	}
	if len(operations) == 0 {
		return nil
	}
	ids, err := newIDs(len(operations))
	if err != nil {
		return fmt.Errorf("generate permission id: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO system_permission (id, role_id, operation)
		 SELECT t.id::uuid, $1::uuid, t.op
		 FROM unnest($2::text[], $3::text[]) AS t(id, op)`,
		roleID, ids, operations); err != nil {
		return fmt.Errorf("insert role permission: %w", err)
	}
	return nil
}
