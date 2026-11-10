package data

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
)

// 编译期断言锁住 biz 契约：接口签名漂移时在此处立即暴露（AGENTS.md §4.1）。
var (
	_ biz.LoginLogWriter = (*loginLogRepo)(nil)
	_ biz.LoginLogReader = (*loginLogRepo)(nil)
)

// loginLogPO 是 system_login_log 表的持久化对象；与 biz.LoginLog 显式转换，
// 禁止结构体复用跨层传递。可空列用指针承载，smallint 用 int16 承载，
// 转换在 toEntity 收敛。append-only 审计表，无软删与更新语义。
type loginLogPO struct {
	id         string
	userID     *string
	username   string
	result     int16
	ip         *string
	userAgent  *string
	createTime time.Time
}

// toEntity 将 PO 转换为领域模型（DO）；NULL 归一为空串（DO 约定：空串=未取到）。
func (p loginLogPO) toEntity() biz.LoginLog {
	l := biz.LoginLog{
		ID:         p.id,
		Username:   p.username,
		Result:     int32(p.result),
		CreateTime: p.createTime,
	}
	if p.userID != nil {
		l.UserID = *p.userID
	}
	if p.ip != nil {
		l.IP = *p.ip
	}
	if p.userAgent != nil {
		l.UserAgent = *p.userAgent
	}
	return l
}

// loginLogSelectColumns 是登录日志查询列序，与 scanLoginLog 一一对应。
const loginLogSelectColumns = "id, user_id, username, result, ip, user_agent, create_time"

// scanLoginLog 按列序扫描一行登录日志；QueryRow 与 Rows 的 Scan 签名一致，复用一份列序。
func scanLoginLog(scan func(dest ...any) error) (*biz.LoginLog, error) {
	var p loginLogPO
	if err := scan(&p.id, &p.userID, &p.username, &p.result, &p.ip, &p.userAgent,
		&p.createTime); err != nil {
		return nil, err
	}
	l := p.toEntity()
	return &l, nil
}

// loginLogRepo 是登录日志仓储的 PostgreSQL 实现：手写参数化 SQL，无 ORM/反射。
type loginLogRepo struct {
	data *basedata.Data
}

// NewLoginLogRepo new a loginLogRepo.
func NewLoginLogRepo(data *basedata.Data) *loginLogRepo {
	return &loginLogRepo{data: data}
}

// Create implements biz.LoginLogWriter：插入一条登录日志；可空列（user_id/ip/
// user_agent）空串经 nilIfEmpty 归一为 NULL。
func (r *loginLogRepo) Create(ctx context.Context, log *biz.LoginLog) error {
	_, err := r.data.Pool.Exec(ctx,
		`INSERT INTO system_login_log (id, user_id, username, result, ip, user_agent, create_time)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		log.ID, nilIfEmpty(log.UserID), log.Username, log.Result,
		nilIfEmpty(log.IP), nilIfEmpty(log.UserAgent), log.CreateTime)
	if err != nil {
		// 审计表无唯一约束与外键，违约面仅基础设施错误，原样包装上抛，
		// 由调用方（recordLogin）决定降级口径。
		return fmt.Errorf("insert login log: %w", err)
	}
	return nil
}

// ListLoginLogs implements biz.LoginLogReader：条件过滤 + 分页，两条参数化查询
// （count + 页数据）。动态 WHERE 仅拼接静态条件片段，值全部参数化。
func (r *loginLogRepo) ListLoginLogs(ctx context.Context, filter biz.LoginLogFilter, page, pageSize int64) ([]*biz.LoginLog, int64, error) {
	where := []string{"true"}
	args := make([]any, 0, 4)
	if filter.Username != "" {
		args = append(args, basedata.LikeContains(filter.Username))
		where = append(where, "username ILIKE $"+strconv.Itoa(len(args)))
	}
	if filter.Result != nil {
		args = append(args, *filter.Result)
		where = append(where, "result = $"+strconv.Itoa(len(args)))
	}
	if filter.StartTime != nil {
		args = append(args, *filter.StartTime)
		where = append(where, "create_time >= $"+strconv.Itoa(len(args)))
	}
	if filter.EndTime != nil {
		args = append(args, *filter.EndTime)
		where = append(where, "create_time <= $"+strconv.Itoa(len(args)))
	}
	whereClause := strings.Join(where, " AND ")

	var total int64
	if err := r.data.Pool.QueryRow(ctx,
		"SELECT count(*) FROM system_login_log WHERE "+whereClause, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count login logs: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}

	args = append(args, pageSize, (page-1)*pageSize)
	query := "SELECT " + loginLogSelectColumns + " FROM system_login_log WHERE " + whereClause +
		" ORDER BY create_time DESC, id DESC LIMIT $" + strconv.Itoa(len(args)-1) +
		" OFFSET $" + strconv.Itoa(len(args))
	rows, err := r.data.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list login logs: %w", err)
	}
	defer rows.Close()

	logs := make([]*biz.LoginLog, 0, pageSize)
	for rows.Next() {
		l, err := scanLoginLog(rows.Scan)
		if err != nil {
			return nil, 0, fmt.Errorf("scan login log: %w", err)
		}
		logs = append(logs, l)
	}
	return logs, total, rows.Err()
}
