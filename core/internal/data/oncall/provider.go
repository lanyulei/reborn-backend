// Package data 值班域仓储实现（pgx + PostgreSQL）。用户展示信息取自 "system_user" 的
// 只读投影（LEFT JOIN，不设外键）：用户停用、离职或被删除后历史班次仍可展示。
package data

import (
	"context"
	"errors"

	"github.com/google/wire"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/oncall"
)

// ProviderSet is 值班域 data providers。
var ProviderSet = wire.NewSet(
	NewGroupRepo, NewScheduleRepo, NewShiftRepo, NewShareRepo, NewStore, NewTokenBox,
	wire.Bind(new(biz.GroupRepo), new(*groupRepo)),
	wire.Bind(new(biz.UserDirectory), new(*groupRepo)),
	wire.Bind(new(biz.ScheduleRepo), new(*scheduleRepo)),
	wire.Bind(new(biz.ShiftRepo), new(*shiftRepo)),
	wire.Bind(new(biz.ShareRepo), new(*shareRepo)),
	wire.Bind(new(biz.Store), new(*store)),
	wire.Bind(new(biz.TokenCipher), new(*TokenBox)),
)

// querier 连接池与事务的公共查询面：读 SQL 在两处复用（事务内读取须走事务连接，
// 才能看到本事务的写入并受行锁保护）。
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// isUniqueViolation 报告是否唯一约束冲突（constraint 为空不区分约束）。
func isUniqueViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation &&
		(constraint == "" || pgErr.ConstraintName == constraint)
}

// isExclusionViolation 报告是否排他约束冲突（班次时间重叠）。
func isExclusionViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.ExclusionViolation
}

// userAvailableExpr 用户可用判定（别名 u）：存在、未删除且状态为可用。
const userAvailableExpr = `(u.id IS NOT NULL AND u.delete_time IS NULL AND u.status = 1)`
