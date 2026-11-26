package data

import (
	"context"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"
)

// 增长观测：只加指标，本文件不删任何数据、不改任何清理行为。
//
// growthTables 里的表只增不删，或者虽然有清理但清理有上限，行数会一直涨。保留期清理分三步落地：
// (1) 先加本指标、上线观察一个周期；(2) 再加清理；(3) 清理分批、选主执行。本文件只做观测（第 (1) 步），
// 不做清理；其中知识库导入任务与已删值班表的班次两处清理已经落地，继续留在清单里是为了从指标上确认
// 清理确实在生效（行数回落到保留期内的量并保持平稳）。
//
// 为什么报估算行数而不是精确的「可清理行数」：精确计数要按「终态且早于 N 天」逐行数，代价与表的大小
// 成正比——这些表大多在该条件上没有索引，个别有的也只是供清理分批取行的局部索引，拿来做计数同样要扫过
// 绝大部分行，每次抓取都这样查等于给库加负载。精确的可清理行数留到第 (2) 步连同保留期索引一起做。
// 估算值取 pg_class.reltuples（autovacuum / analyze 更新，滞后于真实行数；从未分析过的表按 0 报）：
// 只读系统目录，一条语句，开销与表的大小无关。它是估算值，用于观察增长趋势，不是精确计数。
//
// growthTables 增长观测清单，只定义在这一处；只读，任何代码不得修改（仓储构造时拷贝一份）。
// 每张表后面写明入清单的依据（2026-11 全项目审查报告的条目）。只收普通表——分区父表的
// reltuples 不可靠，不收。
var growthTables = []string{
	// 规则版本快照：每次保存 / 启停 / 批量操作追加一行完整 RuleSpec JSON，全仓库没有清理逻辑
	// （审查 core-monitor-rules 优化项 1：只增不删）。
	"monitor_rule_version",
	// 知识库导入任务：此前成功 / 失败的终态任务永久累积（审查 core-knowledge 优化项 4）。现在由 knowledge-purge
	// 循环按保留期（knowledge.ingest_job_retention，缺省 30 天，显式 0 = 不清理）分批清理。
	"knowledge_ingest_job",
	// 值班班次：排班只补不删，已删值班表的历史班次此前留成孤儿行（审查 core-oncall-ticket 优化项 9）。现在由
	// oncall-purge 循环按保留期（oncall.deleted_schedule_shift_retention，缺省 30 天，显式 0 = 不清理）分批清理；
	// 在用值班表的班次本身不在清理范围内，长期看仍只增不减。
	"oncall_shift",
	// 作业触发记录：保留期清理排除 signal_state = 1（信号待发送）的行，这些行永不删除；告警触发器
	// 长期运行后该表可达百万行（审查 core-job 已确认 Bug 2、优化项 D：清理有上限）。
	"job_trigger_fire",
	// 不收故障 Webhook 出站箱（incident_outbox）与动作发件箱（incident_action）：审查报告说它们「每小时
	// 至多清 1000 行、只增不减」，实际清理循环只要删到了行就立即开始下一轮，直到删空才等下一个小时，
	// 不会持续增长。
}

// tableRowEstimatesSQL 清单内各表的估算行数。表名经参数传入，不拼 SQL。
// 用 to_regclass 逐个解析：名字在当前库里不存在时返回 NULL、该表被跳过，不会像 '::regclass' 转换
// 那样让整条语句报错、其余表的数据点一起缺失。relkind = 'r' 只收普通表；GREATEST 把「从未分析」
// 的 -1 归成 0。
const tableRowEstimatesSQL = `SELECT c.relname::text, GREATEST(c.reltuples, 0::real)::bigint
FROM unnest($1::text[]) AS t(name)
JOIN pg_class c ON c.oid = to_regclass(t.name)::oid AND c.relkind = 'r'`

// TableStatsRepo 增长观测数据源：growthTables 内各表的估算行数。它是 server.TableStats 的实现，
// 在组合根 wire.Bind；本包不得 import server（depguard），接口是否满足由 wire 生成时校验。
type TableStatsRepo struct {
	db     Queryer
	tables []string
}

// NewTableStatsRepo new a TableStatsRepo（观测清单取 growthTables 的副本）。
func NewTableStatsRepo(data *Data) *TableStatsRepo {
	return newTableStatsRepo(data.Pool, growthTables)
}

// newTableStatsRepo 以给定的查询面与观测清单构造（单测注入假数据源）。
func newTableStatsRepo(db Queryer, tables []string) *TableStatsRepo {
	return &TableStatsRepo{db: db, tables: slices.Clone(tables)}
}

// TableRowEstimates 返回清单内各表的估算行数（表名 → 估算行数）。清单为空时不查库；清单里的表在
// 当前库不存在（或不是普通表）时不出现在结果里。失败返回错误，由调用方决定是否上报。
func (r *TableStatsRepo) TableRowEstimates(ctx context.Context) (map[string]int64, error) {
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
