// Package txrun 提供 biz 层跨域事务编排的最小抽象：业务用例需要在同一
// 事务内组合本域业务写与通知事件直插（backend/docs/notify-design.md §11，
// 业务提交 = 事件必然落库），而 biz 禁止 import 数据库驱动（depguard），
// 事务句柄在此收敛为零驱动依赖的窄视图，由 data 层以 pgx 适配实现
// （internal/data.TxManager）；测试脚手架以内存桩满足同型。
package txrun

import "context"

// Tx 是事务句柄的窄视图：Exec（业务写 / 事件 INSERT）与 QueryRow（单行
// RETURNING，如去重门闩 upsert 返回首次 event_id）。事务边界归 Manager/用例层
// 所有，句柄不暴露提交/回滚。
type Tx interface {
	// Exec 执行单条语句，返回受影响行数。
	Exec(ctx context.Context, sql string, args ...any) (int64, error)
	// QueryRow 执行返回单行的语句（Scan 取列值）。
	QueryRow(ctx context.Context, sql string, args ...any) Row
}

// Row 单行结果视图。
type Row interface {
	Scan(dest ...any) error
}

// Manager 开启事务并在其中执行 fn：fn 出错回滚并原样透传，成功则提交。
// 事务由调用方管理（§11：业务写 + 事件写经同一 Tx 原子提交）。
type Manager interface {
	WithinTx(ctx context.Context, fn func(tx Tx) error) error
}
