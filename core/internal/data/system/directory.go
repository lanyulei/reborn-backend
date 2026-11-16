package data

import (
	"context"
	"errors"
	"fmt"
	"strings"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"

	"github.com/jackc/pgx/v5"
)

// 编译期断言锁住 biz 契约。
var _ biz.DirectoryRepo = (*directoryRepo)(nil)

// directoryRepo 组织目录只读仓储的 PostgreSQL 实现。流程引擎在自己的事务里解析处理人，
// 查询经 QueryerFrom 复用该事务的连接（不额外占用连接池）。
type directoryRepo struct {
	data *basedata.Data
}

// NewDirectoryRepo new a directoryRepo.
func NewDirectoryRepo(data *basedata.Data) *directoryRepo {
	return &directoryRepo{data: data}
}

// ListUserBriefs implements biz.DirectoryRepo.
func (r *directoryRepo) ListUserBriefs(ctx context.Context, ids []string, activeOnly bool) ([]biz.UserBrief, error) {
	rows, err := basedata.QueryerFrom(ctx, r.data.Pool).Query(ctx,
		`SELECT id, username, nickname FROM "system_user"
		 WHERE id IN (SELECT unnest($1::text[])::uuid) AND (NOT $2 OR status = $3) AND delete_time IS NULL`,
		ids, activeOnly, biz.StatusEnabled)
	if err != nil {
		return nil, fmt.Errorf("list user briefs: %w", err)
	}
	briefs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (biz.UserBrief, error) {
		var b biz.UserBrief
		return b, row.Scan(&b.ID, &b.Username, &b.Nickname)
	})
	if err != nil {
		return nil, fmt.Errorf("scan user brief: %w", err)
	}
	return briefs, nil
}

// ListActiveUserIDsByRoles implements biz.DirectoryRepo：按用户 ID 排序，结果确定。
func (r *directoryRepo) ListActiveUserIDsByRoles(ctx context.Context, roleCodes []string) ([]string, error) {
	rows, err := basedata.QueryerFrom(ctx, r.data.Pool).Query(ctx,
		`SELECT DISTINCT u.id FROM system_role ro
		 JOIN system_user_role ur ON ur.role_id = ro.id
		 JOIN "system_user" u ON u.id = ur.user_id AND u.status = $2 AND u.delete_time IS NULL
		 WHERE ro.code = ANY($1) AND ro.status = $3 AND ro.delete_time IS NULL
		 ORDER BY u.id`, roleCodes, biz.StatusEnabled, biz.RoleStatusEnabled)
	if err != nil {
		return nil, fmt.Errorf("list users by roles: %w", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("scan user id: %w", err)
	}
	return ids, nil
}

// ListDeptNames implements biz.DirectoryRepo.
func (r *directoryRepo) ListDeptNames(ctx context.Context, ids []string) (map[string]string, error) {
	rows, err := basedata.QueryerFrom(ctx, r.data.Pool).Query(ctx,
		"SELECT id, name FROM system_dept WHERE id IN (SELECT unnest($1::text[])::uuid) AND delete_time IS NULL", ids)
	if err != nil {
		return nil, fmt.Errorf("list dept names: %w", err)
	}
	defer rows.Close()
	names := make(map[string]string, len(ids))
	for rows.Next() {
		var id, name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, fmt.Errorf("scan dept name: %w", err)
		}
		names[id] = name
	}
	return names, rows.Err()
}

// ListDeptChainLeaders implements biz.DirectoryRepo：先取部门的物化路径（即祖先链），
// 再一次查询取回链上全部部门的可用负责人；两次往返，与层级深度无关。
func (r *directoryRepo) ListDeptChainLeaders(ctx context.Context, deptID string) ([][]string, error) {
	var path string
	err := basedata.QueryerFrom(ctx, r.data.Pool).QueryRow(ctx,
		"SELECT path FROM system_dept WHERE id = $1 AND delete_time IS NULL", deptID).Scan(&path)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get dept path: %w", err)
	}
	ancestors := strings.Split(strings.Trim(path, "/"), "/") // 根在前、自身在后
	rows, err := basedata.QueryerFrom(ctx, r.data.Pool).Query(ctx,
		`SELECT l.dept_id, l.user_id FROM system_dept_leader l
		 JOIN system_dept d ON d.id = l.dept_id AND d.delete_time IS NULL
		 JOIN "system_user" u ON u.id = l.user_id AND u.status = $2 AND u.delete_time IS NULL
		 WHERE l.dept_id IN (SELECT unnest($1::text[])::uuid)
		 ORDER BY l.sort, l.id`, ancestors, biz.StatusEnabled)
	if err != nil {
		return nil, fmt.Errorf("list dept chain leaders: %w", err)
	}
	defer rows.Close()
	byDept := make(map[string][]string, len(ancestors))
	for rows.Next() {
		var dept, user string
		if err := rows.Scan(&dept, &user); err != nil {
			return nil, fmt.Errorf("scan dept leader: %w", err)
		}
		byDept[dept] = append(byDept[dept], user)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	chain := make([][]string, 0, len(ancestors))
	for i := len(ancestors) - 1; i >= 0; i-- { // 由近及远
		chain = append(chain, byDept[ancestors[i]])
	}
	return chain, nil
}
