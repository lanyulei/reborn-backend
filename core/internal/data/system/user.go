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
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// 编译期断言锁住 biz 契约：接口签名漂移时在此处立即暴露（AGENTS.md §4.1）。
var (
	_ biz.UserReader = (*userRepo)(nil)
	_ biz.UserWriter = (*userRepo)(nil)
)

// userPO 是 system_user 表的持久化对象（SQL 中因 PG 保留字须写作 "system_user"）；与 biz.User 显式转换，禁止结构体复用跨层传递。
// 可空列用指针承载，smallint 用 int16 承载，转换在 toEntity 收敛。
type userPO struct {
	id         string
	username   string
	nickname   string
	email      *string
	phone      *string
	gender     string
	isAdmin    bool
	status     int16
	avatar     string
	createTime time.Time
	updateTime time.Time
}

// toEntity 将 PO 转换为领域模型（DO）；NULL 归一为空串（DO 约定：空串=未填写）。
func (p userPO) toEntity() biz.User {
	u := biz.User{
		ID:         p.id,
		Username:   p.username,
		Nickname:   p.nickname,
		Gender:     p.gender,
		IsAdmin:    p.isAdmin,
		Status:     int32(p.status),
		Avatar:     p.avatar,
		CreateTime: p.createTime,
		UpdateTime: p.updateTime,
	}
	if p.email != nil {
		u.Email = *p.email
	}
	if p.phone != nil {
		u.Phone = *p.phone
	}
	return u
}

// scanUser 按列序扫描一行用户数据；QueryRow 与 Rows 的 Scan 签名一致，复用一份列序。
// 读路径不查询 password 列，天然不回填密码。
func scanUser(scan func(dest ...any) error) (*biz.User, error) {
	var p userPO
	if err := scan(&p.id, &p.username, &p.nickname, &p.email, &p.phone,
		&p.gender, &p.isAdmin, &p.status, &p.avatar, &p.createTime, &p.updateTime); err != nil {
		return nil, err
	}
	u := p.toEntity()
	return &u, nil
}

// userSelectColumns 是用户查询列序，与 scanUser 一一对应。
const userSelectColumns = "id, username, nickname, email, phone, gender, is_admin, status, avatar, create_time, update_time"

// userRepo 是用户仓储的 PostgreSQL 实现：手写参数化 SQL，无 ORM/反射。
type userRepo struct {
	data *basedata.Data
}

// NewUserRepo new a userRepo.
func NewUserRepo(data *basedata.Data) *userRepo {
	return &userRepo{data: data}
}

// mapUserUniqueViolation 将唯一约束冲突（23505）映射为业务哨兵错误；
// 未知冲突原样返回，保留错误链。
func mapUserUniqueViolation(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation {
		switch pgErr.ConstraintName {
		case "uk_system_user_username":
			return biz.ErrUserExists
		case "uk_system_user_email":
			return biz.ErrUserEmailExists
		case "uk_system_user_phone":
			return biz.ErrUserPhoneExists
		}
	}
	return err
}

// GetUser implements biz.UserReader.
func (r *userRepo) GetUser(ctx context.Context, id string) (*biz.User, error) {
	row := r.data.Pool.QueryRow(ctx,
		"SELECT "+userSelectColumns+" FROM \"system_user\" WHERE id = $1 AND delete_time IS NULL", id)
	u, err := scanUser(row.Scan)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrUserNotFound
		}
		return nil, fmt.Errorf("get user: %w", err)
	}
	return u, nil
}

// GetUserByUsername implements biz.UserReader：认证链路专用，是唯一带出 password
// 哈希的读路径。列序比 userSelectColumns 多 password 列，与 scanUser 的
// 「读路径不含密码」列序契约不同，故不复用，单独扫描。
func (r *userRepo) GetUserByUsername(ctx context.Context, username string) (*biz.User, error) {
	var (
		p        userPO
		password string
	)
	err := r.data.Pool.QueryRow(ctx,
		`SELECT id, username, password, nickname, email, phone, gender, is_admin, status, create_time, update_time
		 FROM "system_user" WHERE username = $1 AND delete_time IS NULL`, username).
		Scan(&p.id, &p.username, &password, &p.nickname, &p.email, &p.phone,
			&p.gender, &p.isAdmin, &p.status, &p.createTime, &p.updateTime)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrUserNotFound
		}
		return nil, fmt.Errorf("get user by username: %w", err)
	}
	u := p.toEntity()
	u.Password = password
	return &u, nil
}

// ListUsers implements biz.UserReader：条件过滤 + 分页，两条参数化查询（count + 页数据）。
func (r *userRepo) ListUsers(ctx context.Context, filter biz.UserFilter, page, pageSize int64) ([]*biz.User, int64, error) {
	where := []string{"delete_time IS NULL"}
	args := make([]any, 0, 4)
	if filter.Keyword != "" {
		args = append(args, dbx.LikeContains(filter.Keyword))
		n := strconv.Itoa(len(args))
		where = append(where, "(username ILIKE $"+n+" OR nickname ILIKE $"+n+" OR email ILIKE $"+n+" OR phone ILIKE $"+n+")")
	}
	if filter.Status != nil {
		args = append(args, *filter.Status)
		where = append(where, "status = $"+strconv.Itoa(len(args)))
	}
	whereClause := strings.Join(where, " AND ")

	var total int64
	if err := r.data.Pool.QueryRow(ctx,
		"SELECT count(*) FROM \"system_user\" WHERE "+whereClause, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count users: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}

	args = append(args, pageSize, (page-1)*pageSize)
	query := "SELECT " + userSelectColumns + " FROM \"system_user\" WHERE " + whereClause +
		" ORDER BY create_time DESC, id DESC LIMIT $" + strconv.Itoa(len(args)-1) +
		" OFFSET $" + strconv.Itoa(len(args))
	rows, err := r.data.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list users: %w", err)
	}
	defer rows.Close()

	users := make([]*biz.User, 0, pageSize)
	for rows.Next() {
		u, err := scanUser(rows.Scan)
		if err != nil {
			return nil, 0, fmt.Errorf("scan user: %w", err)
		}
		users = append(users, u)
	}
	return users, total, rows.Err()
}

// CreateUser implements biz.UserWriter：审计时间由用例统一生成并显式落库（保证出参一致），
// 空串邮箱/手机号经 NULLIF 归一为 NULL。
func (r *userRepo) CreateUser(ctx context.Context, user *biz.User) error {
	_, err := r.data.Pool.Exec(ctx,
		`INSERT INTO "system_user" (id, username, password, nickname, email, phone, gender, is_admin, status, create_time, update_time)
		 VALUES ($1, $2, $3, $4, NULLIF($5, ''), NULLIF($6, ''), $7, $8, $9, $10, $11)`,
		user.ID, user.Username, user.Password, user.Nickname, user.Email, user.Phone,
		user.Gender, user.IsAdmin, user.Status, user.CreateTime, user.UpdateTime)
	if err != nil {
		return mapUserUniqueViolation(err)
	}
	return nil
}

// UpdateUser implements biz.UserWriter：动态 SET 只允许静态列名白名单，值全部参数化；
// 无可更新字段时幂等 no-op。
func (r *userRepo) UpdateUser(ctx context.Context, id string, upd biz.UserUpdate) error {
	sets := make([]string, 0, 7)
	args := make([]any, 0, 8)
	add := func(column string, value any) {
		args = append(args, value)
		sets = append(sets, column+" = $"+strconv.Itoa(len(args)))
	}
	if upd.Nickname != nil {
		add("nickname", *upd.Nickname)
	}
	if upd.Email != nil {
		add("email", dbx.NilIfEmpty(*upd.Email))
	}
	if upd.Phone != nil {
		add("phone", dbx.NilIfEmpty(*upd.Phone))
	}
	if upd.Gender != nil {
		add("gender", *upd.Gender)
	}
	if upd.IsAdmin != nil {
		add("is_admin", *upd.IsAdmin)
	}
	if upd.Status != nil {
		add("status", *upd.Status)
	}
	if upd.PasswordHash != nil {
		add("password", *upd.PasswordHash)
	}
	if upd.Avatar != nil {
		add("avatar", *upd.Avatar)
	}
	if len(sets) == 0 {
		return nil
	}

	args = append(args, id)
	query := "UPDATE \"system_user\" SET " + strings.Join(sets, ", ") +
		", update_time = now() WHERE id = $" + strconv.Itoa(len(args)) + " AND delete_time IS NULL"
	tag, err := r.data.Pool.Exec(ctx, query, args...)
	if err != nil {
		return mapUserUniqueViolation(err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrUserNotFound
	}
	return nil
}

// DeleteUser implements biz.UserWriter：软删用户并同事务物理删除其全部角色绑定
// 与刷新令牌（堵住「软删用户仍可刷新」的口子）。
func (r *userRepo) DeleteUser(ctx context.Context, id string) error {
	tx, err := r.data.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx,
		"UPDATE \"system_user\" SET delete_time = now() WHERE id = $1 AND delete_time IS NULL", id)
	if err != nil {
		return fmt.Errorf("soft delete user: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrUserNotFound
	}
	if _, err := tx.Exec(ctx, "DELETE FROM system_user_role WHERE user_id = $1", id); err != nil {
		return fmt.Errorf("delete user role bindings: %w", err)
	}
	if _, err := tx.Exec(ctx, "DELETE FROM system_refresh_token WHERE user_id = $1", id); err != nil {
		return fmt.Errorf("delete user refresh tokens: %w", err)
	}
	return tx.Commit(ctx)
}
