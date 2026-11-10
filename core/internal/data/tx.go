package data

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// WithTx 在单个事务中执行 fn：Begin 失败即返回错误；fn 出错时回滚并原样透传，
// 成功则提交。域子包（internal/data/<domain>）经本包复用，收敛 Begin/Rollback/
// Commit 样板；Rollback 在已提交后返回 ErrTxClosed，统一忽略。
func WithTx(ctx context.Context, pool *pgxpool.Pool, fn func(tx pgx.Tx) error) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// LikeContains 构造 ILIKE 子串匹配实参（%keyword%），并转义关键字中的
// LIKE 元字符（\ % _）：用户输入的 `%`/`_` 按字面匹配，而非退化成
// 全表通配（`%` 命中全部行并强制全扫）。PostgreSQL LIKE 的缺省转义字符
// 即反斜杠，调用方 SQL 无需追加 ESCAPE 子句。
func LikeContains(keyword string) string {
	return "%" + likeEscaper.Replace(keyword) + "%"
}

var likeEscaper = strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
