// Package data 监控告警域仓储实现（pgx + PostgreSQL，手写参数化 SQL）。
//
// 表的读写分工（迁移 0069 以列级 GRANT 强制）：配置表与 monitor_notify_log 由 core 读写；运行表
// （评估任务 / 判定状态 / 告警事件 / 判定记录 / 发件箱）由评估引擎写，core 只读——例外只有
// monitor_alert 的 notify_count、last_notify_time 与 monitor_notify_outbox 的 status、attempts、
// last_error、done_time。本包不得写运行表的其他列。
//
// 配置表的写入同事务 bump monitor_cfg_version（引擎据此刷新只读缓存）；时间列一律用数据库 now()。
package data

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/wire"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	"github.com/lanyulei/reborn/backend/kit/dbx"
)

// ProviderSet is 监控告警域 data providers。
var ProviderSet = wire.NewSet(
	NewGroupRepo, NewRoleScopeRepo, NewDatasourceRepo, NewSecretBox, NewQueryClient, NewRuleRepo, NewRuleVersionRepo, NewEvalRepo,
	NewAlertRepo, NewNotifyLogRepo, NewMuteRepo, NewSubscriptionRepo, NewOutboxRepo,
	wire.Bind(new(biz.GroupReader), new(*groupRepo)),
	wire.Bind(new(biz.GroupWriter), new(*groupRepo)),
	wire.Bind(new(biz.RoleScopeRepo), new(*roleScopeRepo)),
	wire.Bind(new(biz.DatasourceReader), new(*datasourceRepo)),
	wire.Bind(new(biz.DatasourceLookup), new(*datasourceRepo)),
	wire.Bind(new(biz.DatasourceWriter), new(*datasourceRepo)),
	wire.Bind(new(biz.SecretCipher), new(*SecretBox)),
	wire.Bind(new(biz.QueryClient), new(*QueryClient)),
	wire.Bind(new(biz.RuleReader), new(*ruleRepo)),
	wire.Bind(new(biz.RuleLookup), new(*ruleRepo)),
	wire.Bind(new(biz.RuleWriter), new(*ruleRepo)),
	wire.Bind(new(biz.RuleVersionReader), new(*ruleVersionRepo)),
	wire.Bind(new(biz.EvalReader), new(*evalRepo)),
	wire.Bind(new(biz.AlertReader), new(*alertRepo)),
	wire.Bind(new(biz.AlertStats), new(*alertRepo)),
	wire.Bind(new(biz.NotifyLogReader), new(*notifyLogRepo)),
	wire.Bind(new(biz.MuteReader), new(*muteRepo)),
	wire.Bind(new(biz.MuteWriter), new(*muteRepo)),
	wire.Bind(new(biz.SubscriptionReader), new(*subscriptionRepo)),
	wire.Bind(new(biz.SubscriptionWriter), new(*subscriptionRepo)),
	wire.Bind(new(biz.OutboxRepo), new(*outboxRepo)),
	wire.Bind(new(biz.DispatchWriter), new(*outboxRepo)),
	// 第二、三期：各功能的 provider 集定义在各自的文件里。
	wire.Bind(new(biz.DatasourceWriteTargets), new(*datasourceRepo)),
	wire.Bind(new(biz.WriteURLValidator), new(*QueryClient)),
	AlertP2ProviderSet, escalationProviders, SyncProviderSet, ruleP2Providers, StatsProviders, InhibitProviderSet,
	// 告警事件对外只读视图（故障管理域消费）。
	FeedProviderSet,
)

// isUniqueViolation 报告是否唯一约束冲突（constraint 为空不区分约束）。
// dup: 与 data/cmdb、data/oncall、data/job 各有一份相同的实现。合并的去处只能是 kit/dbx（各域的 data
// 包不能互相 import），那需要改 kit 与另外三个域的文件，不在本域的改动范围内，留待统一处理。
func isUniqueViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation &&
		(constraint == "" || pgErr.ConstraintName == constraint)
}

// lockCfg 行锁配置版本行：同一张配置表的写事务以它串行化（分组的层级 / 总数上限、缺省数据源的
// 唯一性都依赖「先查后写」，必须互斥）。事务结束前须调用 bumpCfg。
func lockCfg(ctx context.Context, tx pgx.Tx, name string) error {
	var v int64
	if err := tx.QueryRow(ctx, "SELECT version FROM monitor_cfg_version WHERE name = $1 FOR UPDATE", name).Scan(&v); err != nil {
		return fmt.Errorf("lock monitor cfg version %s: %w", name, err)
	}
	return nil
}

// bumpCfg 配置版本 +1（与配置写入同事务提交；评估引擎轮询比对后刷新缓存）。
func bumpCfg(ctx context.Context, tx pgx.Tx, name string) error {
	tag, err := tx.Exec(ctx, "UPDATE monitor_cfg_version SET version = version + 1, update_time = now() WHERE name = $1", name)
	if err != nil {
		return fmt.Errorf("bump monitor cfg version %s: %w", name, err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("bump monitor cfg version %s: row missing", name)
	}
	return nil
}

// 带乐观锁的表（表名为代码内常量，不接受外部输入）。
const (
	tableRule         = "monitor_rule"
	tableMute         = "monitor_mute"
	tableSubscription = "monitor_subscription"
)

// missingOrConflict 乐观锁更新没有命中任何行时区分原因：行不存在（或已删除）返回 notFound，
// 否则是版本不一致。
func missingOrConflict(ctx context.Context, tx pgx.Tx, table, id string, notFound error) error {
	var exists bool
	if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM "+table+" WHERE id = $1 AND delete_time IS NULL)", id).Scan(&exists); err != nil {
		return fmt.Errorf("check %s exists: %w", table, err)
	}
	if !exists {
		return notFound
	}
	return biz.ErrVersionConflict
}

// sqlArgs 动态条件的实参收集器：add 追加一个实参并返回其占位符（$n）。
type sqlArgs struct {
	args []any
}

func (a *sqlArgs) add(v any) string {
	a.args = append(a.args, v)
	return fmt.Sprintf("$%d", len(a.args))
}

// groupCond 把分组过滤翻译成条件：不限时返回空串；IDs 为空时恒假（调用方通常已提前返回）。
func groupCond(a *sqlArgs, column string, f biz.GroupFilter) string {
	if f.All {
		return ""
	}
	return " AND " + column + " = ANY(" + a.add(dbx.TextArray(f.IDs)) + "::uuid[])"
}

// int32Array nil 切片落空数组（pgx 会把 nil 编码为 NULL）；字符串数组用 dbx.TextArray。
func int32Array(items []int32) []int32 {
	if items == nil {
		return []int32{}
	}
	return items
}

// collectNames 读取 (id, name) 两列的结果集。
func collectNames(rows pgx.Rows, what string) (map[string]string, error) {
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var id, name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, fmt.Errorf("scan %s: %w", what, err)
		}
		out[id] = name
	}
	return out, rows.Err()
}
