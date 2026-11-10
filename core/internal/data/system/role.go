package data

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// 编译期断言锁住 biz 契约。
var (
	_ biz.RoleReader = (*roleRepo)(nil)
	_ biz.RoleWriter = (*roleRepo)(nil)
)

// rolePO 是 system_role 表的持久化对象；与 biz.Role 显式转换，禁止跨层直传。
type rolePO struct {
	id         string
	name       string
	code       string
	status     int16
	bizScope   []string
	createTime time.Time
	updateTime time.Time
}

// toEntity 将 PO 转换为领域模型（DO）。
func (p rolePO) toEntity() biz.Role {
	return biz.Role{
		ID:         p.id,
		Name:       p.name,
		Code:       p.code,
		Status:     int32(p.status),
		BizScope:   p.bizScope,
		CreateTime: p.createTime,
		UpdateTime: p.updateTime,
	}
}

// scanRole 按列序扫描一行角色数据。
func scanRole(scan func(dest ...any) error) (*biz.Role, error) {
	var p rolePO
	if err := scan(&p.id, &p.name, &p.code, &p.status, &p.bizScope, &p.createTime, &p.updateTime); err != nil {
		return nil, err
	}
	r := p.toEntity()
	return &r, nil
}

// roleSelectColumns 是角色查询列序，与 scanRole 一一对应。
const roleSelectColumns = "id, name, code, status, biz_scope, create_time, update_time"

// roleRepo 是角色仓储的 PostgreSQL 实现。
type roleRepo struct {
	data *basedata.Data
}

// NewRoleRepo new a roleRepo.
func NewRoleRepo(data *basedata.Data) *roleRepo {
	return &roleRepo{data: data}
}

// mapRoleUniqueViolation 将唯一约束冲突（23505）映射为业务哨兵错误。
func mapRoleUniqueViolation(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation {
		if pgErr.ConstraintName == "uk_system_role_code" {
			return biz.ErrRoleCodeExists
		}
	}
	return err
}

// GetRole implements biz.RoleReader.
func (r *roleRepo) GetRole(ctx context.Context, id string) (*biz.Role, error) {
	row := r.data.Pool.QueryRow(ctx,
		"SELECT "+roleSelectColumns+" FROM system_role WHERE id = $1 AND delete_time IS NULL", id)
	role, err := scanRole(row.Scan)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrRoleNotFound
		}
		return nil, fmt.Errorf("get role: %w", err)
	}
	return role, nil
}

// ListRoles implements biz.RoleReader：条件过滤 + 分页，两条参数化查询。
func (r *roleRepo) ListRoles(ctx context.Context, filter biz.RoleFilter, page, pageSize int64) ([]*biz.Role, int64, error) {
	where := []string{"delete_time IS NULL"}
	args := make([]any, 0, 4)
	if filter.Keyword != "" {
		args = append(args, basedata.LikeContains(filter.Keyword))
		n := strconv.Itoa(len(args))
		where = append(where, "(name ILIKE $"+n+" OR code ILIKE $"+n+")")
	}
	if filter.Status != nil {
		args = append(args, *filter.Status)
		where = append(where, "status = $"+strconv.Itoa(len(args)))
	}
	whereClause := strings.Join(where, " AND ")

	var total int64
	if err := r.data.Pool.QueryRow(ctx,
		"SELECT count(*) FROM system_role WHERE "+whereClause, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count roles: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}

	args = append(args, pageSize, (page-1)*pageSize)
	query := "SELECT " + roleSelectColumns + " FROM system_role WHERE " + whereClause +
		" ORDER BY create_time DESC, id DESC LIMIT $" + strconv.Itoa(len(args)-1) +
		" OFFSET $" + strconv.Itoa(len(args))
	rows, err := r.data.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list roles: %w", err)
	}
	defer rows.Close()

	roles := make([]*biz.Role, 0, pageSize)
	for rows.Next() {
		role, err := scanRole(rows.Scan)
		if err != nil {
			return nil, 0, fmt.Errorf("scan role: %w", err)
		}
		roles = append(roles, role)
	}
	return roles, total, rows.Err()
}

// ListRolesByIDs implements biz.RoleReader：先对输入去重再查询；
// ids 经 unnest 参数化传入，单查询无 N+1。
func (r *roleRepo) ListRolesByIDs(ctx context.Context, ids []string) ([]*biz.Role, error) {
	ids = uniqueStrings(ids)
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := r.data.Pool.Query(ctx,
		"SELECT "+roleSelectColumns+" FROM system_role"+
			" WHERE id IN (SELECT unnest($1::text[])::uuid) AND delete_time IS NULL", ids)
	if err != nil {
		return nil, fmt.Errorf("list roles by ids: %w", err)
	}
	defer rows.Close()

	roles := make([]*biz.Role, 0, len(ids))
	for rows.Next() {
		role, err := scanRole(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("scan role: %w", err)
		}
		roles = append(roles, role)
	}
	return roles, rows.Err()
}

// uniqueStrings 保序去重字符串切片。
func uniqueStrings(items []string) []string {
	if len(items) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(items))
	out := make([]string, 0, len(items))
	for _, item := range items {
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}

// CreateRole implements biz.RoleWriter：审计时间由用例统一生成并显式落库。
func (r *roleRepo) CreateRole(ctx context.Context, role *biz.Role) error {
	_, err := r.data.Pool.Exec(ctx,
		`INSERT INTO system_role (id, name, code, status, biz_scope, create_time, update_time)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		role.ID, role.Name, role.Code, role.Status, textArrayArg(role.BizScope), role.CreateTime, role.UpdateTime)
	if err != nil {
		return mapRoleUniqueViolation(err)
	}
	return nil
}

// UpdateRole implements biz.RoleWriter：动态 SET 只允许静态列名白名单；
// 无可更新字段时幂等 no-op。
func (r *roleRepo) UpdateRole(ctx context.Context, id string, upd biz.RoleUpdate) error {
	sets := make([]string, 0, 2)
	args := make([]any, 0, 3)
	add := func(column string, value any) {
		args = append(args, value)
		sets = append(sets, column+" = $"+strconv.Itoa(len(args)))
	}
	if upd.Name != nil {
		add("name", *upd.Name)
	}
	if upd.Status != nil {
		add("status", *upd.Status)
	}
	if upd.BizScope != nil {
		add("biz_scope", textArrayArg(*upd.BizScope))
	}
	if len(sets) == 0 {
		return nil
	}

	args = append(args, id)
	query := "UPDATE system_role SET " + strings.Join(sets, ", ") +
		", update_time = now() WHERE id = $" + strconv.Itoa(len(args)) + " AND delete_time IS NULL"
	tag, err := r.data.Pool.Exec(ctx, query, args...)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrRoleNotFound
	}
	return nil
}

// DeleteRole implements biz.RoleWriter：软删角色并同事务物理删除其全部用户绑定
// 与授权行（system_grant 直接授权、system_permission 物化策略）。
func (r *roleRepo) DeleteRole(ctx context.Context, id string) error {
	tx, err := r.data.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx,
		"UPDATE system_role SET delete_time = now() WHERE id = $1 AND delete_time IS NULL", id)
	if err != nil {
		return fmt.Errorf("soft delete role: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrRoleNotFound
	}
	if _, err := tx.Exec(ctx, "DELETE FROM system_user_role WHERE role_id = $1", id); err != nil {
		return fmt.Errorf("delete role bindings: %w", err)
	}
	if _, err := tx.Exec(ctx, "DELETE FROM system_grant WHERE role_id = $1", id); err != nil {
		return fmt.Errorf("delete role grants: %w", err)
	}
	if _, err := tx.Exec(ctx, "DELETE FROM system_permission WHERE role_id = $1", id); err != nil {
		return fmt.Errorf("delete role permissions: %w", err)
	}
	return tx.Commit(ctx)
}

// textArrayArg 归一化 text[] 实参：nil 切片会被 pgx 编码为 NULL（撞 NOT NULL），
// 统一落空数组。
func textArrayArg(items []string) []string {
	if items == nil {
		return []string{}
	}
	return items
}
