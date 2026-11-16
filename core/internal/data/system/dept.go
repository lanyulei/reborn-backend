package data

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	"github.com/lanyulei/reborn/backend/kit/idgen"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// 编译期断言锁住 biz 契约。
var (
	_ biz.DeptReader     = (*deptRepo)(nil)
	_ biz.DeptWriter     = (*deptRepo)(nil)
	_ biz.DeptMover      = (*deptRepo)(nil)
	_ biz.DeptMemberRepo = (*deptRepo)(nil)
)

// deptTreeLockID 部门树结构锁（事务级 advisory lock）：新建 / 移动 / 删除经同一把
// 锁跨实例串行——路径计算、无环判定、层级与总数上限都是「先查后写」，必须互斥；
// 锁随事务结束自动释放，实例崩溃不会遗留死锁。部门结构性写低频，串行无性能顾虑。
var deptTreeLockID = dbx.LockID("system_dept_tree")

// deptPO 是 system_dept 表的持久化对象；与 biz.Dept 显式转换，禁止跨层直传。
type deptPO struct {
	id          string
	parentID    *string
	path        string
	name        string
	code        *string
	sort        int32
	status      int16
	description string
	memberCount int64
	createTime  time.Time
	updateTime  time.Time
}

// toEntity 将 PO 转换为领域模型（DO）；NULL 归一为空串。
func (p deptPO) toEntity() biz.Dept {
	d := biz.Dept{
		ID:          p.id,
		Path:        p.path,
		Name:        p.name,
		Sort:        p.sort,
		Status:      int32(p.status),
		Description: p.description,
		MemberCount: int32(min(p.memberCount, math.MaxInt32)), //nolint:gosec // 已夹紧到 int32 上限
		CreateTime:  p.createTime,
		UpdateTime:  p.updateTime,
	}
	if p.parentID != nil {
		d.ParentID = *p.parentID
	}
	if p.code != nil {
		d.Code = *p.code
	}
	return d
}

// deptSelectSQL 部门查询（含直属成员数）；列序与 scanDept 一一对应。
// 成员数经一次分组聚合左连接，避免逐部门相关子查询。
const deptSelectSQL = `SELECT d.id, d.parent_id, d.path, d.name, d.code, d.sort, d.status, d.description,
	COALESCE(m.cnt, 0), d.create_time, d.update_time
	FROM system_dept d
	LEFT JOIN (SELECT dept_id, count(*) AS cnt FROM system_user_dept GROUP BY dept_id) m ON m.dept_id = d.id
	WHERE d.delete_time IS NULL`

// scanDept 按列序扫描一行部门数据。
func scanDept(scan func(dest ...any) error) (*biz.Dept, error) {
	var p deptPO
	if err := scan(&p.id, &p.parentID, &p.path, &p.name, &p.code, &p.sort, &p.status, &p.description,
		&p.memberCount, &p.createTime, &p.updateTime); err != nil {
		return nil, err
	}
	d := p.toEntity()
	return &d, nil
}

// deptDepth 由物化路径得到层级（根部门为 1）：path 形如 /<id>/<id>/。
func deptDepth(path string) int {
	return strings.Count(path, "/") - 1
}

// deptSelfSegment 取物化路径的末段（部门自身，带结尾 '/'）：/a/b/c/ → c/。
func deptSelfSegment(path string) string {
	return path[strings.LastIndex(path[:len(path)-1], "/")+1:]
}

// deptRepo 是部门仓储的 PostgreSQL 实现。
type deptRepo struct {
	data *basedata.Data
}

// NewDeptRepo new a deptRepo.
func NewDeptRepo(data *basedata.Data) *deptRepo {
	return &deptRepo{data: data}
}

// mapDeptUniqueViolation 将唯一约束冲突（23505）映射为业务哨兵错误。
func mapDeptUniqueViolation(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation {
		switch pgErr.ConstraintName {
		case "uk_system_dept_parent_name":
			return biz.ErrDeptNameExists
		case "uk_system_dept_code":
			return biz.ErrDeptCodeExists
		}
	}
	return err
}

// lockDeptTree 在事务内获取部门树结构锁（阻塞，随事务结束释放）。
func lockDeptTree(ctx context.Context, tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", deptTreeLockID); err != nil {
		return fmt.Errorf("lock dept tree: %w", err)
	}
	return nil
}

// deptPath 查询未删除部门的物化路径；不存在返回 ErrDeptNotFound。
func deptPath(ctx context.Context, tx pgx.Tx, id string) (string, error) {
	var path string
	err := tx.QueryRow(ctx, "SELECT path FROM system_dept WHERE id = $1 AND delete_time IS NULL", id).Scan(&path)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", biz.ErrDeptNotFound
		}
		return "", fmt.Errorf("get dept path: %w", err)
	}
	return path, nil
}

// queryer 是 Pool 与 Tx 共有的查询面（负责人加载在事务内外复用）。
type queryer interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// loadDeptLeaders 批量加载部门负责人（过滤已软删用户），按配置顺序回填到 depts；
// 单查询，无逐部门 N+1。
func loadDeptLeaders(ctx context.Context, q queryer, depts []*biz.Dept) error {
	if len(depts) == 0 {
		return nil
	}
	ids := make([]string, 0, len(depts))
	byID := make(map[string]*biz.Dept, len(depts))
	for _, d := range depts {
		ids = append(ids, d.ID)
		byID[d.ID] = d
	}
	rows, err := q.Query(ctx,
		`SELECT l.dept_id, u.id, u.username, u.nickname, u.avatar, u.status
		 FROM system_dept_leader l JOIN "system_user" u ON u.id = l.user_id AND u.delete_time IS NULL
		 WHERE l.dept_id IN (SELECT unnest($1::text[])::uuid)
		 ORDER BY l.dept_id, l.sort, l.id`, ids)
	if err != nil {
		return fmt.Errorf("list dept leaders: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			deptID string
			leader biz.DeptLeader
			status int16
		)
		if err := rows.Scan(&deptID, &leader.UserID, &leader.Username, &leader.Nickname, &leader.Avatar, &status); err != nil {
			return fmt.Errorf("scan dept leader: %w", err)
		}
		leader.Status = int32(status)
		if d := byID[deptID]; d != nil {
			d.Leaders = append(d.Leaders, leader)
		}
	}
	return rows.Err()
}

// GetDept implements biz.DeptReader.
func (r *deptRepo) GetDept(ctx context.Context, id string) (*biz.Dept, error) {
	dept, err := scanDept(r.data.Pool.QueryRow(ctx, deptSelectSQL+" AND d.id = $1", id).Scan)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrDeptNotFound
		}
		return nil, fmt.Errorf("get dept: %w", err)
	}
	if err := loadDeptLeaders(ctx, r.data.Pool, []*biz.Dept{dept}); err != nil {
		return nil, err
	}
	return dept, nil
}

// ListDepts implements biz.DeptReader：两条查询（部门 + 负责人），总数受
// biz.MaxDeptCount 约束。排序：层级浅的在前（id 定长，路径长度即层级），
// 同级按 sort、创建时间升序。
func (r *deptRepo) ListDepts(ctx context.Context) ([]*biz.Dept, error) {
	rows, err := r.data.Pool.Query(ctx, deptSelectSQL+" ORDER BY length(d.path), d.sort, d.create_time, d.id")
	if err != nil {
		return nil, fmt.Errorf("list depts: %w", err)
	}
	defer rows.Close()
	depts := make([]*biz.Dept, 0, 64)
	for rows.Next() {
		dept, err := scanDept(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("scan dept: %w", err)
		}
		depts = append(depts, dept)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := loadDeptLeaders(ctx, r.data.Pool, depts); err != nil {
		return nil, err
	}
	return depts, nil
}

// CreateDept implements biz.DeptWriter：结构锁内校验上级、层级与总数后落库，
// 负责人同事务写入。
func (r *deptRepo) CreateDept(ctx context.Context, dept *biz.Dept, leaderIDs []string) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if err := lockDeptTree(ctx, tx); err != nil {
			return err
		}
		path := "/" + dept.ID + "/"
		if dept.ParentID != "" {
			parentPath, err := deptPath(ctx, tx, dept.ParentID)
			if err != nil {
				return err
			}
			if deptDepth(parentPath)+1 > biz.MaxDeptDepth {
				return biz.ErrDeptDepthExceeded
			}
			path = parentPath + dept.ID + "/"
		}
		var total int64
		if err := tx.QueryRow(ctx, "SELECT count(*) FROM system_dept WHERE delete_time IS NULL").Scan(&total); err != nil {
			return fmt.Errorf("count depts: %w", err)
		}
		if total >= biz.MaxDeptCount {
			return biz.ErrDeptLimitExceeded
		}
		if err := lockLeaderUsers(ctx, tx, leaderIDs); err != nil {
			return err
		}
		operator := basedata.OperatorArg(ctx)
		if _, err := tx.Exec(ctx,
			`INSERT INTO system_dept (id, parent_id, path, name, code, sort, status, description, create_time, update_time, create_user, update_user)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $11)`,
			dept.ID, dbx.NilIfEmpty(dept.ParentID), path, dept.Name, dbx.NilIfEmpty(dept.Code), dept.Sort, dept.Status,
			dept.Description, dept.CreateTime, dept.UpdateTime, operator); err != nil {
			return mapDeptUniqueViolation(err)
		}
		if err := replaceDeptLeaders(ctx, tx, dept.ID, leaderIDs); err != nil {
			return err
		}
		dept.Path = path
		return nil
	})
}

// UpdateDept implements biz.DeptWriter：动态 SET 只允许静态列名白名单，值全部
// 参数化；UPDATE 恒执行（至少刷新 update_time），既判存在也对部门行加锁，
// 同一部门的并发负责人替换因此串行。加锁顺序全域统一为「先用户、后部门」
// （见 lockLeaderUsers），故负责人用户的锁先于部门行的 UPDATE 获取。
func (r *deptRepo) UpdateDept(ctx context.Context, id string, upd biz.DeptUpdate) error {
	sets := make([]string, 0, 6)
	args := make([]any, 0, 8)
	add := func(column string, value any) {
		args = append(args, value)
		sets = append(sets, column+" = $"+strconv.Itoa(len(args)))
	}
	if upd.Name != nil {
		add("name", *upd.Name)
	}
	if upd.Code != nil {
		add("code", dbx.NilIfEmpty(*upd.Code))
	}
	if upd.Sort != nil {
		add("sort", *upd.Sort)
	}
	if upd.Status != nil {
		add("status", *upd.Status)
	}
	if upd.Description != nil {
		add("description", *upd.Description)
	}
	add("update_user", basedata.OperatorArg(ctx))
	args = append(args, id)
	query := "UPDATE system_dept SET " + strings.Join(sets, ", ") +
		", update_time = now() WHERE id = $" + strconv.Itoa(len(args)) + " AND delete_time IS NULL"
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if upd.LeaderIDs != nil {
			if err := lockLeaderUsers(ctx, tx, *upd.LeaderIDs); err != nil {
				return err
			}
		}
		tag, err := tx.Exec(ctx, query, args...)
		if err != nil {
			return mapDeptUniqueViolation(err)
		}
		if tag.RowsAffected() == 0 {
			return biz.ErrDeptNotFound
		}
		if upd.LeaderIDs != nil {
			return replaceDeptLeaders(ctx, tx, id, *upd.LeaderIDs)
		}
		return nil
	})
}

// lockLeaderUsers 对负责人用户加共享锁（按 id 定序）并校验全部存在；任一不存在
// 返回 ErrUserNotFound。FOR SHARE 与并发的用户软删（UPDATE）互斥：要么软删
// 先提交、此处重判为不存在，要么此处先持锁、软删随后清除新写入的负责人行——
// 不会留下指向已删除用户的负责人行。
//
// 加锁顺序全域统一为「先用户、后部门」：本函数须在触碰部门行与负责人行之前调用。
// 反过来（先改部门再锁用户）会与「改用户资料并替换其部门归属」「删除用户并清除
// 其负责人身份」形成环路等待。ids 须已去重。
func lockLeaderUsers(ctx context.Context, tx pgx.Tx, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	var n int
	if err := tx.QueryRow(ctx,
		`SELECT count(*) FROM (
			SELECT id FROM "system_user" WHERE id IN (SELECT unnest($1::text[])::uuid) AND delete_time IS NULL
			ORDER BY id FOR SHARE) s`, ids).Scan(&n); err != nil {
		return fmt.Errorf("lock leader users: %w", err)
	}
	if n != len(ids) {
		return biz.ErrUserNotFound
	}
	return nil
}

// replaceDeptLeaders 事务内整组替换部门负责人。调用方须已经 lockLeaderUsers 锁定并
// 校验过 leaderIDs，且持有部门行锁（或该部门行尚未提交）。新负责人经单语句批量插入，
// 顺序即 leaderIDs 的顺序。
func replaceDeptLeaders(ctx context.Context, tx pgx.Tx, deptID string, leaderIDs []string) error {
	if _, err := tx.Exec(ctx, "DELETE FROM system_dept_leader WHERE dept_id = $1", deptID); err != nil {
		return fmt.Errorf("clear dept leaders: %w", err)
	}
	if len(leaderIDs) == 0 {
		return nil
	}
	ids, err := newIDs(len(leaderIDs))
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO system_dept_leader (id, dept_id, user_id, sort, create_user)
		 SELECT t.id::uuid, $1, t.user_id::uuid, t.ord::int, $4
		 FROM unnest($2::text[], $3::text[]) WITH ORDINALITY AS t(id, user_id, ord)`,
		deptID, ids, leaderIDs, basedata.OperatorArg(ctx)); err != nil {
		return fmt.Errorf("insert dept leaders: %w", err)
	}
	return nil
}

// newIDs 批量生成 n 个 UUIDv7。
func newIDs(n int) ([]string, error) {
	ids := make([]string, 0, n)
	for range n {
		id, err := idgen.NewID()
		if err != nil {
			return nil, fmt.Errorf("generate id: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// MoveDept implements biz.DeptMover：结构锁内判定无环与层级后，更新上级并整体
// 改写子树路径（单语句前缀替换）。路径只含 UUID 字符与 '/'，无 LIKE 元字符。
func (r *deptRepo) MoveDept(ctx context.Context, id, parentID string) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if err := lockDeptTree(ctx, tx); err != nil {
			return err
		}
		oldPath, err := deptPath(ctx, tx, id)
		if err != nil {
			return err
		}
		// 自身的路径段取自库内既有路径而非入参：入参 UUID 可能是大写形式（等值比较
		// 不区分大小写），直接拼接会把非规范形式写进路径。
		self := deptSelfSegment(oldPath)
		newPath := "/" + self
		if parentID != "" {
			parentPath, err := deptPath(ctx, tx, parentID)
			if err != nil {
				return err
			}
			// 目标上级位于被移动子树内（含自身）即成环。
			if strings.HasPrefix(parentPath, oldPath) {
				return biz.ErrDeptMoveInvalid
			}
			newPath = parentPath + self
		}
		if newPath == oldPath {
			return nil // 上级未变化，幂等 no-op
		}
		// 子树最深层级随移动整体平移：超限则拒绝。
		var deepest int
		if err := tx.QueryRow(ctx,
			`SELECT COALESCE(max(length(path) - length(replace(path, '/', '')) - 1), 0)
			 FROM system_dept WHERE path LIKE $1 AND delete_time IS NULL`, oldPath+"%").Scan(&deepest); err != nil {
			return fmt.Errorf("measure dept subtree: %w", err)
		}
		if deepest-deptDepth(oldPath)+deptDepth(newPath) > biz.MaxDeptDepth {
			return biz.ErrDeptDepthExceeded
		}
		if _, err := tx.Exec(ctx,
			"UPDATE system_dept SET parent_id = $2, update_time = now(), update_user = $3 WHERE id = $1 AND delete_time IS NULL",
			id, dbx.NilIfEmpty(parentID), basedata.OperatorArg(ctx)); err != nil {
			return mapDeptUniqueViolation(err)
		}
		if _, err := tx.Exec(ctx,
			`UPDATE system_dept SET path = $2 || substr(path, $3)
			 WHERE path LIKE $1 AND delete_time IS NULL`,
			oldPath+"%", newPath, len(oldPath)+1); err != nil {
			return fmt.Errorf("rewrite dept subtree path: %w", err)
		}
		return nil
	})
}

// DeleteDept implements biz.DeptWriter：结构锁内先锁部门行，再判定无下级、无成员
// 后软删并清除负责人。成员写入方对部门行持键共享锁（见 lockDepts），与此处的
// FOR UPDATE 互斥：要么成员先提交、此处看见后拒绝，要么此处先删、成员写入方重判为不存在。
func (r *deptRepo) DeleteDept(ctx context.Context, id string) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if err := lockDeptTree(ctx, tx); err != nil {
			return err
		}
		var locked bool
		err := tx.QueryRow(ctx,
			"SELECT true FROM system_dept WHERE id = $1 AND delete_time IS NULL FOR UPDATE", id).Scan(&locked)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return biz.ErrDeptNotFound
			}
			return fmt.Errorf("lock dept: %w", err)
		}
		var hasChildren, hasMembers bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM system_dept WHERE parent_id = $1 AND delete_time IS NULL),
			        EXISTS (SELECT 1 FROM system_user_dept WHERE dept_id = $1)`, id).Scan(&hasChildren, &hasMembers); err != nil {
			return fmt.Errorf("check dept usage: %w", err)
		}
		if hasChildren {
			return biz.ErrDeptHasChildren
		}
		if hasMembers {
			return biz.ErrDeptHasMembers
		}
		if _, err := tx.Exec(ctx,
			"UPDATE system_dept SET delete_time = now(), delete_user = $2 WHERE id = $1",
			id, basedata.OperatorArg(ctx)); err != nil {
			return fmt.Errorf("soft delete dept: %w", err)
		}
		if _, err := tx.Exec(ctx, "DELETE FROM system_dept_leader WHERE dept_id = $1", id); err != nil {
			return fmt.Errorf("delete dept leaders: %w", err)
		}
		return nil
	})
}

// lockDepts 对给定未删除部门加键共享锁（按 id 定序）并校验全部存在；任一不存在
// 返回 ErrDeptNotFound。FOR KEY SHARE 只与删除部门时的 FOR UPDATE 互斥（成员写入
// 期间部门不会被并发删除），不与部门的普通更新 / 移动（非键列 UPDATE）冲突，
// 因此不会和「移动子树时成批改写路径」形成环路等待。ids 须已去重。
func lockDepts(ctx context.Context, tx pgx.Tx, ids []string) error {
	var n int
	if err := tx.QueryRow(ctx,
		`SELECT count(*) FROM (
			SELECT id FROM system_dept WHERE id IN (SELECT unnest($1::text[])::uuid) AND delete_time IS NULL
			ORDER BY id FOR KEY SHARE) s`, ids).Scan(&n); err != nil {
		return fmt.Errorf("lock depts: %w", err)
	}
	if n != len(ids) {
		return biz.ErrDeptNotFound
	}
	return nil
}

// lockUsers 对给定未删除用户加行锁（FOR NO KEY UPDATE：与用户软删、同一用户的
// 并发部门归属写互斥，但不阻塞引用该用户的外键写入如登录签发刷新令牌；按 id
// 定序防死锁）并校验全部存在；任一不存在返回 ErrUserNotFound。ids 须已去重。
func lockUsers(ctx context.Context, tx pgx.Tx, ids []string) error {
	var n int
	if err := tx.QueryRow(ctx,
		`SELECT count(*) FROM (
			SELECT id FROM "system_user" WHERE id IN (SELECT unnest($1::text[])::uuid) AND delete_time IS NULL
			ORDER BY id FOR NO KEY UPDATE) s`, ids).Scan(&n); err != nil {
		return fmt.Errorf("lock users: %w", err)
	}
	if n != len(ids) {
		return biz.ErrUserNotFound
	}
	return nil
}

// AddDeptMembers implements biz.DeptMemberRepo：单语句批量插入，已在部门内的
// 经 ON CONFLICT 跳过；用户尚无主部门时本部门成为主部门（用户行锁保证
// 「是否已有主部门」的判定与写入之间无并发插入）。
func (r *deptRepo) AddDeptMembers(ctx context.Context, deptID string, userIDs []string) error {
	if len(userIDs) == 0 {
		return nil
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		// 加锁顺序：先用户、后部门（与用户资料更新链路一致）。
		if err := lockUsers(ctx, tx, userIDs); err != nil {
			return err
		}
		if err := lockDepts(ctx, tx, []string{deptID}); err != nil {
			return err
		}
		var overLimit bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS (
				SELECT 1 FROM system_user_dept
				WHERE user_id IN (SELECT unnest($1::text[])::uuid)
				GROUP BY user_id
				HAVING count(*) >= $3 AND NOT bool_or(dept_id = $2))`,
			userIDs, deptID, biz.MaxUserDepts).Scan(&overLimit); err != nil {
			return fmt.Errorf("check user dept limit: %w", err)
		}
		if overLimit {
			return biz.ErrDeptLimitExceeded
		}
		ids, err := newIDs(len(userIDs))
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO system_user_dept (id, user_id, dept_id, is_primary, create_user)
			 SELECT t.id::uuid, t.user_id::uuid, $1,
			        NOT EXISTS (SELECT 1 FROM system_user_dept p WHERE p.user_id = t.user_id::uuid AND p.is_primary), $4
			 FROM unnest($2::text[], $3::text[]) AS t(id, user_id)
			 ORDER BY t.user_id
			 ON CONFLICT (user_id, dept_id) DO NOTHING`,
			deptID, ids, userIDs, basedata.OperatorArg(ctx)); err != nil {
			return fmt.Errorf("insert dept members: %w", err)
		}
		return nil
	})
}

// RemoveDeptMember implements biz.DeptMemberRepo：先锁用户行（与该用户的其他
// 部门归属写串行），删除归属；移出的是主部门时由最早加入的其余部门顶替。
func (r *deptRepo) RemoveDeptMember(ctx context.Context, deptID, userID string) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT 1 FROM "system_user" WHERE id = $1 FOR NO KEY UPDATE`, userID); err != nil {
			return fmt.Errorf("lock user: %w", err)
		}
		var wasPrimary bool
		err := tx.QueryRow(ctx,
			"DELETE FROM system_user_dept WHERE dept_id = $1 AND user_id = $2 RETURNING is_primary",
			deptID, userID).Scan(&wasPrimary)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil // 不在部门内：幂等 no-op
			}
			return fmt.Errorf("delete dept member: %w", err)
		}
		if !wasPrimary {
			return nil
		}
		return promotePrimaryDept(ctx, tx, userID)
	})
}

// promotePrimaryDept 用户失去主部门后，由最早加入的其余部门顶替（无其余部门则无事发生）。
func promotePrimaryDept(ctx context.Context, tx pgx.Tx, userID string) error {
	if _, err := tx.Exec(ctx,
		`UPDATE system_user_dept SET is_primary = true
		 WHERE id = (SELECT id FROM system_user_dept WHERE user_id = $1 ORDER BY create_time, id LIMIT 1)`,
		userID); err != nil {
		return fmt.Errorf("promote primary dept: %w", err)
	}
	return nil
}

// ListUserDepts implements biz.DeptMemberRepo：单查询批量取回，主部门在前。
func (r *deptRepo) ListUserDepts(ctx context.Context, userIDs []string) (map[string][]biz.UserDept, error) {
	if len(userIDs) == 0 {
		return nil, nil
	}
	rows, err := basedata.QueryerFrom(ctx, r.data.Pool).Query(ctx,
		`SELECT ud.user_id, d.id, d.name, ud.is_primary
		 FROM system_user_dept ud JOIN system_dept d ON d.id = ud.dept_id AND d.delete_time IS NULL
		 WHERE ud.user_id IN (SELECT unnest($1::text[])::uuid)
		 ORDER BY ud.user_id, ud.is_primary DESC, ud.create_time, ud.id`, userIDs)
	if err != nil {
		return nil, fmt.Errorf("list user depts: %w", err)
	}
	defer rows.Close()
	out := make(map[string][]biz.UserDept, len(userIDs))
	for rows.Next() {
		var (
			userID string
			dept   biz.UserDept
		)
		if err := rows.Scan(&userID, &dept.ID, &dept.Name, &dept.Primary); err != nil {
			return nil, fmt.Errorf("scan user dept: %w", err)
		}
		out[userID] = append(out[userID], dept)
	}
	return out, rows.Err()
}

// replaceUserDepts 事务内整组替换用户所属部门（调用方已持有用户行锁：更新链路
// 先执行对用户行的 UPDATE，创建链路的用户行尚未提交）。按差量改写而非清空重建，
// 保留未变化归属的加入时间（主部门顶替按加入时间排序）。入参须已规整
// （DeptIDs 去重排序、PrimaryDeptID 在其内）。
func replaceUserDepts(ctx context.Context, tx pgx.Tx, userID string, assign biz.UserDeptAssignment) error {
	if len(assign.DeptIDs) == 0 {
		if _, err := tx.Exec(ctx, "DELETE FROM system_user_dept WHERE user_id = $1", userID); err != nil {
			return fmt.Errorf("clear user depts: %w", err)
		}
		return nil
	}
	if err := lockDepts(ctx, tx, assign.DeptIDs); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		"DELETE FROM system_user_dept WHERE user_id = $1 AND dept_id NOT IN (SELECT unnest($2::text[])::uuid)",
		userID, assign.DeptIDs); err != nil {
		return fmt.Errorf("delete stale user depts: %w", err)
	}
	// 主部门唯一索引逐行即时校验：先清旧主部门标记，再置新主部门。
	if _, err := tx.Exec(ctx,
		"UPDATE system_user_dept SET is_primary = false WHERE user_id = $1 AND is_primary AND dept_id <> $2",
		userID, assign.PrimaryDeptID); err != nil {
		return fmt.Errorf("clear primary dept: %w", err)
	}
	ids, err := newIDs(len(assign.DeptIDs))
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO system_user_dept (id, user_id, dept_id, is_primary, create_user)
		 SELECT t.id::uuid, $1, t.dept_id::uuid, false, $4
		 FROM unnest($2::text[], $3::text[]) AS t(id, dept_id)
		 ON CONFLICT (user_id, dept_id) DO NOTHING`,
		userID, ids, assign.DeptIDs, basedata.OperatorArg(ctx)); err != nil {
		return fmt.Errorf("insert user depts: %w", err)
	}
	if _, err := tx.Exec(ctx,
		"UPDATE system_user_dept SET is_primary = true WHERE user_id = $1 AND dept_id = $2 AND NOT is_primary",
		userID, assign.PrimaryDeptID); err != nil {
		return fmt.Errorf("set primary dept: %w", err)
	}
	return nil
}
