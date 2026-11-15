package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/oncall"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.GroupRepo     = (*groupRepo)(nil)
	_ biz.UserDirectory = (*groupRepo)(nil)
)

// groupSelectColumns 分组查询列序；$1 为查询者用户 ID（空串 = 匿名，身份恒为 0）。
const groupSelectColumns = `g.id::text, g.name, g.description,
	(SELECT count(*) FROM oncall_group_member m WHERE m.group_id = g.id),
	(SELECT count(*) FROM oncall_schedule s WHERE s.group_id = g.id AND s.delete_time IS NULL),
	COALESCE((SELECT m.role FROM oncall_group_member m WHERE m.group_id = g.id AND m.user_id = NULLIF($1, '')::uuid), 0),
	g.create_time, g.update_time`

func scanGroup(scan func(dest ...any) error) (*biz.Group, error) {
	var g biz.Group
	if err := scan(&g.ID, &g.Name, &g.Description, &g.MemberCount, &g.ScheduleCount, &g.MyRole, &g.CreateTime, &g.UpdateTime); err != nil {
		return nil, err
	}
	return &g, nil
}

type groupRepo struct {
	data *basedata.Data
}

// NewGroupRepo new a groupRepo.
func NewGroupRepo(data *basedata.Data) *groupRepo { return &groupRepo{data: data} }

// List implements biz.GroupRepo.
func (r *groupRepo) List(ctx context.Context, filter biz.GroupFilter, page, pageSize int64) ([]*biz.Group, int64, error) {
	where := "g.delete_time IS NULL"
	args := []any{filter.ViewerID}
	if filter.Keyword != "" {
		args = append(args, dbx.LikeContains(filter.Keyword))
		where += fmt.Sprintf(" AND g.name ILIKE $%d", len(args))
	}
	if filter.Mine {
		where += " AND EXISTS (SELECT 1 FROM oncall_group_member m WHERE m.group_id = g.id AND m.user_id = NULLIF($1, '')::uuid)"
	}
	var total int64
	// $1 在不带 mine 的计数语句里未被引用，显式标注类型避免"无法推断参数类型"。
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM oncall_group g WHERE $1::text IS NOT NULL AND "+where, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count oncall groups: %w", err)
	}
	args = append(args, pageSize, (page-1)*pageSize)
	rows, err := r.data.Pool.Query(ctx, "SELECT "+groupSelectColumns+" FROM oncall_group g WHERE "+where+
		fmt.Sprintf(" ORDER BY g.create_time DESC, g.id LIMIT $%d OFFSET $%d", len(args)-1, len(args)), args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list oncall groups: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.Group, 0, pageSize)
	for rows.Next() {
		g, err := scanGroup(rows.Scan)
		if err != nil {
			return nil, 0, fmt.Errorf("scan oncall group: %w", err)
		}
		items = append(items, g)
	}
	return items, total, rows.Err()
}

// Get implements biz.GroupRepo.
func (r *groupRepo) Get(ctx context.Context, id, viewerID string) (*biz.Group, error) {
	g, err := scanGroup(r.data.Pool.QueryRow(ctx,
		"SELECT "+groupSelectColumns+" FROM oncall_group g WHERE g.id = $2 AND g.delete_time IS NULL", viewerID, id).Scan)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrGroupNotFound
		}
		return nil, fmt.Errorf("get oncall group: %w", err)
	}
	return g, nil
}

// ListMembers implements biz.GroupRepo.
func (r *groupRepo) ListMembers(ctx context.Context, groupID string) ([]*biz.Member, error) {
	return listMembers(ctx, r.data.Pool, groupID)
}

// listMembers 分组全部成员：管理员在前，其次按加入时间。
func listMembers(ctx context.Context, q querier, groupID string) ([]*biz.Member, error) {
	rows, err := q.Query(ctx,
		`SELECT m.user_id::text, COALESCE(u.username, ''), COALESCE(u.nickname, ''), m.role, `+userAvailableExpr+`,
		        EXISTS (SELECT 1 FROM oncall_schedule_member sm
		                JOIN oncall_schedule s ON s.id = sm.schedule_id AND s.delete_time IS NULL
		                WHERE s.group_id = m.group_id AND sm.user_id = m.user_id),
		        m.create_time
		 FROM oncall_group_member m LEFT JOIN "system_user" u ON u.id = m.user_id
		 WHERE m.group_id = $1 ORDER BY m.role, m.create_time, m.id`, groupID)
	if err != nil {
		return nil, fmt.Errorf("list oncall group members: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.Member, 0, 16)
	for rows.Next() {
		var m biz.Member
		if err := rows.Scan(&m.UserID, &m.Username, &m.Nickname, &m.Role, &m.Available, &m.InRotation, &m.CreateTime); err != nil {
			return nil, fmt.Errorf("scan oncall group member: %w", err)
		}
		items = append(items, &m)
	}
	return items, rows.Err()
}

// ListUserRoles implements biz.GroupRepo.
func (r *groupRepo) ListUserRoles(ctx context.Context, userID string) (map[string]int32, error) {
	rows, err := r.data.Pool.Query(ctx, "SELECT group_id::text, role FROM oncall_group_member WHERE user_id = $1", userID)
	if err != nil {
		return nil, fmt.Errorf("list oncall user roles: %w", err)
	}
	defer rows.Close()
	roles := make(map[string]int32, 4)
	for rows.Next() {
		var id string
		var role int32
		if err := rows.Scan(&id, &role); err != nil {
			return nil, fmt.Errorf("scan oncall user role: %w", err)
		}
		roles[id] = role
	}
	return roles, rows.Err()
}

// ListAdminIDs implements biz.GroupRepo.
func (r *groupRepo) ListAdminIDs(ctx context.Context, groupID string) ([]string, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT m.user_id::text FROM oncall_group_member m JOIN "system_user" u ON u.id = m.user_id
		 WHERE m.group_id = $1 AND m.role = $2 AND u.delete_time IS NULL AND u.status = 1 ORDER BY m.create_time`,
		groupID, biz.MemberRoleAdmin)
	if err != nil {
		return nil, fmt.Errorf("list oncall group admins: %w", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("scan oncall group admins: %w", err)
	}
	return ids, nil
}

// ListChangeLogs implements biz.GroupRepo.
func (r *groupRepo) ListChangeLogs(ctx context.Context, groupID, scheduleID string, page, pageSize int64) ([]*biz.ChangeLog, int64, error) {
	const where = "l.group_id = $1 AND ($2 = '' OR l.schedule_id = NULLIF($2, '')::uuid)"
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM oncall_change_log l WHERE "+where, groupID, scheduleID).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count oncall change logs: %w", err)
	}
	rows, err := r.data.Pool.Query(ctx,
		`SELECT l.id::text, l.group_id::text, COALESCE(l.schedule_id::text, ''), l.action, l.summary, l.detail,
		        COALESCE(l.create_user::text, ''), COALESCE(NULLIF(u.nickname, ''), u.username, ''), l.create_time
		 FROM oncall_change_log l LEFT JOIN "system_user" u ON u.id = l.create_user
		 WHERE `+where+` ORDER BY l.create_time DESC, l.id DESC LIMIT $3 OFFSET $4`,
		groupID, scheduleID, pageSize, (page-1)*pageSize)
	if err != nil {
		return nil, 0, fmt.Errorf("list oncall change logs: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.ChangeLog, 0, pageSize)
	for rows.Next() {
		var l biz.ChangeLog
		var raw []byte
		if err := rows.Scan(&l.ID, &l.GroupID, &l.ScheduleID, &l.Action, &l.Summary, &raw, &l.OperatorID, &l.OperatorName, &l.CreateTime); err != nil {
			return nil, 0, fmt.Errorf("scan oncall change log: %w", err)
		}
		if err := json.Unmarshal(raw, &l.Detail); err != nil {
			return nil, 0, fmt.Errorf("decode oncall change log detail: %w", err)
		}
		items = append(items, &l)
	}
	return items, total, rows.Err()
}

// Candidates implements biz.UserDirectory（只取 ID、登录名、昵称）。
func (r *groupRepo) Candidates(ctx context.Context, keyword string, limit int) ([]*biz.User, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT id::text, username, nickname FROM "system_user"
		 WHERE delete_time IS NULL AND status = 1 AND ($1 = '' OR username ILIKE $2 OR nickname ILIKE $2)
		 ORDER BY username, id LIMIT $3`, keyword, dbx.LikeContains(keyword), limit)
	if err != nil {
		return nil, fmt.Errorf("list oncall user candidates: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.User, 0, limit)
	for rows.Next() {
		u := biz.User{Available: true}
		if err := rows.Scan(&u.ID, &u.Username, &u.Nickname); err != nil {
			return nil, fmt.Errorf("scan oncall user candidate: %w", err)
		}
		items = append(items, &u)
	}
	return items, rows.Err()
}

// Lookup implements biz.UserDirectory.
func (r *groupRepo) Lookup(ctx context.Context, ids []string) (map[string]*biz.User, error) {
	return lookupUsers(ctx, r.data.Pool, ids)
}

// lookupUsers 按 ID 批量查询用户（含不可用用户；已删除的不返回）。
func lookupUsers(ctx context.Context, q querier, ids []string) (map[string]*biz.User, error) {
	out := make(map[string]*biz.User, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := q.Query(ctx,
		`SELECT u.id::text, u.username, u.nickname, COALESCE(u.phone, ''), u.status = 1
		 FROM "system_user" u WHERE u.id = ANY($1::uuid[]) AND u.delete_time IS NULL`, ids)
	if err != nil {
		return nil, fmt.Errorf("lookup oncall users: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var u biz.User
		if err := rows.Scan(&u.ID, &u.Username, &u.Nickname, &u.Phone, &u.Available); err != nil {
			return nil, fmt.Errorf("scan oncall user: %w", err)
		}
		out[u.ID] = &u
	}
	return out, rows.Err()
}
