package data

import (
	"context"
	"fmt"
	"slices"

	"github.com/lanyulei/reborn/backend/cmdb/internal/biz"

	"github.com/jackc/pgx/v5"
)

// 增长观测：保留期清理落地前的第一步——只加指标，不删任何数据，不改任何清理行为。
//
// growthTables 里的表只增不删，行数会一直涨。保留期清理分三步落地：(1) 先加本指标、上线观察一个周期；
// (2) 再加清理；(3) 清理分批、选主执行。本文件只做第 (1) 步。
//
// 为什么报估算行数而不是精确的「可清理行数」：精确计数要按「终态且早于 N 天」逐行数，代价与表的大小
// 成正比——这两张表在该条件上都没有索引，每次抓取都这样查等于给库加负载。精确的可清理行数留到第 (2) 步
// 连同保留期索引一起做。估算值取 pg_class.reltuples（autovacuum / analyze 更新，滞后于真实行数；从未
// 分析过的表按 0 报）：只读系统目录，一条语句，开销与表的大小无关。它是估算值，用于观察增长趋势，
// 不是精确计数。
//
// growthTables 增长观测清单，只定义在这一处；只读，任何代码不得修改（仓储构造时拷贝一份）。
// 每张表后面写明入清单的依据（2026-11 全项目审查报告的条目）。只收普通表——分区父表的
// reltuples 不可靠，不收。
var growthTables = []string{
	// 同步任务：每个源每个周期各增一行（5 分钟周期一年约 10 万行 / 源）。保留期清理（maintenance.run_retention）
	// 已接入维护循环，行数应回落到保留期内的量并保持平稳——继续观测一个周期，确认清理确实生效
	//（数据面账号对这两张表的 DELETE 授权须到位，否则维护循环只告警、不清理）（审查 cmdb-module-migrations 优化项 O3）。
	"cmdb_sync_task",
	// 同步运行记录：每次任务执行一行，与任务同一保留期；各采集源 last_run_id 指向的最近一条始终保留。
	"cmdb_sync_run",
}

// tableRowEstimatesSQL 清单内各表的估算行数。表名经参数传入，不拼 SQL。
// 用 to_regclass 逐个解析：名字在当前库里不存在时返回 NULL、该表被跳过，不会像 '::regclass' 转换
// 那样让整条语句报错、其余表的数据点一起缺失。relkind = 'r' 只收普通表；GREATEST 把「从未分析」
// 的 -1 归成 0。
const tableRowEstimatesSQL = `SELECT c.relname::text, GREATEST(c.reltuples, 0::real)::bigint
FROM unnest($1::text[]) AS t(name)
JOIN pg_class c ON c.oid = to_regclass(t.name)::oid AND c.relkind = 'r'`

// rowQuerier TableRowEstimates 用到的唯一数据库能力（*pgxpool.Pool 满足它）；收窄成接口是为了
// 单测能注入假数据源。
type rowQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// 编译期断言锁住 biz 契约。
var _ biz.TableStats = (*tableStatsRepo)(nil)

// tableStatsRepo 增长观测数据源：growthTables 内各表的估算行数。
type tableStatsRepo struct {
	db     rowQuerier
	tables []string
}

// NewTableStatsRepo new a tableStatsRepo（观测清单取 growthTables 的副本）。
func NewTableStatsRepo(data *Data) *tableStatsRepo {
	return newTableStatsRepo(data.Pool, growthTables)
}

// newTableStatsRepo 以给定的查询面与观测清单构造（单测注入假数据源）。
func newTableStatsRepo(db rowQuerier, tables []string) *tableStatsRepo {
	return &tableStatsRepo{db: db, tables: slices.Clone(tables)}
}

// TableRowEstimates implements biz.TableStats：返回清单内各表的估算行数（表名 → 估算行数）。
// 清单为空时不查库；清单里的表在当前库不存在（或不是普通表）时不出现在结果里。
// 失败返回错误，由调用方决定是否上报。
func (r *tableStatsRepo) TableRowEstimates(ctx context.Context) (map[string]int64, error) {
	estimates := make(map[string]int64, len(r.tables))
	if len(r.tables) == 0 {
		return estimates, nil
	}
	set, err := r.db.Query(ctx, tableRowEstimatesSQL, r.tables)
	if err != nil {
		return nil, fmt.Errorf("table row estimates: %w", err)
	}
	defer set.Close()
	var table string
	var rows int64
	if _, err := pgx.ForEachRow(set, []any{&table, &rows}, func() error {
		estimates[table] = rows
		return nil
	}); err != nil {
		return nil, fmt.Errorf("table row estimates: %w", err)
	}
	return estimates, nil
}
