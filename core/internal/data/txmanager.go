package data

import (
	"context"

	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TxManager 是 txrun.Manager 的 PostgreSQL 实现：biz 用例层的跨域事务
// 编排（业务写 + 通知事件直插同事务原子提交，notify 设计 §11）经此开启
// 事务；复用 WithTx 的 Begin/Rollback/Commit 样板，句柄适配为零驱动
// 依赖的窄视图供 biz 引用。
type TxManager struct {
	pool *pgxpool.Pool
}

// NewTxManager new a TxManager.
func NewTxManager(data *Data) *TxManager {
	return &TxManager{pool: data.Pool}
}

// WithinTx implements txrun.Manager：fn 出错回滚并透传，成功提交。
func (m *TxManager) WithinTx(ctx context.Context, fn func(tx txrun.Tx) error) error {
	return WithTx(ctx, m.pool, func(tx pgx.Tx) error {
		return fn(txHandle{tx: tx})
	})
}

// txHandle 将 pgx.Tx 适配为 txrun.Tx 窄视图（仅 Exec——业务写与事件
// INSERT 均为无结果集语句，无需暴露查询/批量接口）。
type txHandle struct{ tx pgx.Tx }

// Exec implements txrun.Tx。
func (h txHandle) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	tag, err := h.tx.Exec(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// QueryRow implements txrun.Tx（pgx.Row 即 txrun.Row）。
func (h txHandle) QueryRow(ctx context.Context, sql string, args ...any) txrun.Row {
	return h.tx.QueryRow(ctx, sql, args...)
}

// 编译期断言锁住 txrun 契约。
var (
	_ txrun.Tx      = txHandle{}
	_ txrun.Manager = (*TxManager)(nil)
)
