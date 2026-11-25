//go:build integration

package data

import (
	"context"
	"testing"
)

// 增长观测清单必须与库对得上：清单里的每张表都存在且是普通表——表名写错、或将来有人把分区父表
// 加进清单时，指标会静默缺失，所以这里直接失败；再断言估算查询对每张表都给出非负的估算值。
// 经 newIntegrationData 连接（会 TRUNCATE cmdb 表，与同包其它集成用例同一护栏、同一串行要求）。
func TestIntegration_TableStatsGrowthTables(t *testing.T) {
	d := newIntegrationData(t)
	ctx := context.Background()
	for _, name := range growthTables {
		// to_regclass 对不存在的名字返回 NULL（不报错）；relkind：r = 普通表，p = 分区父表。
		var kind *string
		err := d.Pool.QueryRow(ctx, `SELECT c.relkind::text FROM pg_class c WHERE c.oid = to_regclass($1::text)`, name).Scan(&kind)
		if err != nil {
			t.Fatalf("relkind(%s) error = %v", name, err)
		}
		if kind == nil {
			t.Fatalf("增长观测清单里的表 %q 在库中不存在（表名写错会让指标静默缺失；测试库须已应用全部迁移）", name)
		}
		if *kind != "r" {
			t.Fatalf("增长观测清单里的 %q 不是普通表（relkind = %s）：分区父表的 reltuples 不可靠，不收", name, *kind)
		}
	}

	got, err := NewTableStatsRepo(d).TableRowEstimates(ctx)
	if err != nil {
		t.Fatalf("TableRowEstimates() error = %v", err)
	}
	if len(got) != len(growthTables) {
		t.Fatalf("TableRowEstimates() returned %d tables, want all %d: %v", len(got), len(growthTables), got)
	}
	for _, name := range growthTables {
		if rows, ok := got[name]; !ok || rows < 0 {
			t.Fatalf("estimate(%s) = %d, present = %v; want a non-negative estimate", name, rows, ok)
		}
	}

	// 库里不存在的表名不占位，也不拖累其余表：结果里只有能解析到的普通表。
	partial := newTableStatsRepo(d.Pool, []string{"cmdb_sync_task", "no_such_table_reborn_audit"})
	got, err = partial.TableRowEstimates(ctx)
	if err != nil {
		t.Fatalf("TableRowEstimates(partial) error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("TableRowEstimates(partial) = %v, want cmdb_sync_task only", got)
	}
	if _, ok := got["cmdb_sync_task"]; !ok {
		t.Fatalf("TableRowEstimates(partial) = %v, want cmdb_sync_task", got)
	}
	// 分区父表不收：变更历史表自建表起就按月分区（迁移 0032），relkind = 'p'。
	partitioned := newTableStatsRepo(d.Pool, []string{"cmdb_ci_history"})
	got, err = partitioned.TableRowEstimates(ctx)
	if err != nil {
		t.Fatalf("TableRowEstimates(partitioned) error = %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("TableRowEstimates(partitioned) = %v, want nothing: a partitioned parent has no reliable reltuples", got)
	}
}
