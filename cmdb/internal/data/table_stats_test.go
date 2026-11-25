package data

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// fakeQueryer 记下执行的语句与实参的假数据源（实现 rowQuerier）。
type fakeQueryer struct {
	sql  string
	args []any
	rows pgx.Rows
	err  error
}

func (q *fakeQueryer) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	q.sql, q.args = sql, args
	return q.rows, q.err
}

// fakeRows 预置结果的假结果集（每行两列：表名 text、估算行数 bigint）。
type fakeRows struct {
	cells [][]any
	i     int // 当前行的下标；尚无当前行时为 -1
}

// newFakeRows 以给定的行构造结果集；游标停在第一行之前。
func newFakeRows(cells ...[]any) *fakeRows { return &fakeRows{cells: cells, i: -1} }

func (r *fakeRows) Close()                                       {}
func (r *fakeRows) Err() error                                   { return nil }
func (r *fakeRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *fakeRows) Values() ([]any, error)                       { return slices.Clone(r.cells[r.i]), nil }
func (r *fakeRows) RawValues() [][]byte                          { return nil }
func (r *fakeRows) Conn() *pgx.Conn                              { return nil }
func (r *fakeRows) TypeMap() *pgtype.Map                         { return nil }

func (r *fakeRows) Next() bool {
	r.i++
	return r.i < len(r.cells)
}

func (r *fakeRows) Scan(dest ...any) error {
	for i, d := range dest {
		switch d := d.(type) {
		case *string:
			v, ok := r.cells[r.i][i].(string)
			if !ok {
				return fmt.Errorf("fakeRows: column %d is %T, want string", i, r.cells[r.i][i])
			}
			*d = v
		case *int64:
			v, ok := r.cells[r.i][i].(int64)
			if !ok {
				return fmt.Errorf("fakeRows: column %d is %T, want int64", i, r.cells[r.i][i])
			}
			*d = v
		default:
			return fmt.Errorf("fakeRows: unsupported destination %T", d)
		}
	}
	return nil
}

// 编译期断言：假对象满足仓储依赖的接口。
var (
	_ rowQuerier = (*fakeQueryer)(nil)
	_ pgx.Rows   = (*fakeRows)(nil)

	// relationPattern 找出语句里 FROM / JOIN 后面的关系名（小写）。
	relationPattern = regexp.MustCompile(`\b(from|join)\s+([a-z_]+)`)
)

// 观测清单为空时不查库（空数组对 unnest 没有意义），直接返回空结果。
func TestTableStatsRepo_EmptyTablesDoNotQuery(t *testing.T) {
	t.Parallel()
	q := &fakeQueryer{}
	got, err := newTableStatsRepo(q, nil).TableRowEstimates(context.Background())
	if err != nil {
		t.Fatalf("TableRowEstimates() error = %v", err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("TableRowEstimates() = %v, want an empty result", got)
	}
	if q.sql != "" || len(q.args) != 0 {
		t.Fatalf("empty watch list must not query the database, got %q %v", q.sql, q.args)
	}
}

// 返回值逐表落位：每行一个表；清单里的表没有对应的行（库里不存在 / 不是普通表）时不占位，也不补 0。
func TestTableStatsRepo_MapsEachRowByTable(t *testing.T) {
	t.Parallel()
	q := &fakeQueryer{rows: newFakeRows(
		[]any{"cmdb_sync_task", int64(12)},
		[]any{"cmdb_sync_run", int64(0)},
	)}
	list := []string{"cmdb_sync_task", "cmdb_sync_run", "missing_table"}
	got, err := newTableStatsRepo(q, list).TableRowEstimates(context.Background())
	if err != nil {
		t.Fatalf("TableRowEstimates() error = %v", err)
	}
	want := map[string]int64{"cmdb_sync_task": 12, "cmdb_sync_run": 0}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("TableRowEstimates() = %v, want %v", got, want)
	}
	// 清单整份经参数传入（$1::text[]），语句里不出现任何表名。
	if !reflect.DeepEqual(q.args, []any{list}) {
		t.Fatalf("args = %#v, want %#v", q.args, []any{list})
	}
	for _, name := range list {
		if strings.Contains(q.sql, name) {
			t.Fatalf("statement must take table names as a parameter, found %q in:\n%s", name, q.sql)
		}
	}
	// 只读系统目录、只收普通表。
	stmt := strings.ToLower(q.sql)
	for _, fragment := range []string{"to_regclass(", "relkind = 'r'", "greatest(c.reltuples, 0::real)::bigint"} {
		if !strings.Contains(stmt, fragment) {
			t.Fatalf("statement lacks %q:\n%s", fragment, q.sql)
		}
	}
	// 语句里出现的关系只有两处：参数展开（unnest）与系统目录 pg_class——不碰任何业务表。
	relations := relationPattern.FindAllStringSubmatch(stmt, -1)
	if len(relations) != 2 {
		t.Fatalf("statement must read exactly unnest and pg_class:\n%s", q.sql)
	}
	for _, m := range relations {
		if m[2] != "unnest" && m[2] != "pg_class" {
			t.Fatalf("statement must only read system catalogs, found %s %s:\n%s", m[1], m[2], q.sql)
		}
	}
}

// 查询失败、行读取失败：返回错误（指标回调据此本次不上报），不返回半截结果。
func TestTableStatsRepo_Errors(t *testing.T) {
	t.Parallel()
	boom := errors.New("boom")
	cases := []struct {
		name string
		q    *fakeQueryer
		want error
	}{
		{name: "query", q: &fakeQueryer{err: boom}, want: boom},
		// 第二行的表名不是字符串：扫描失败。
		{name: "scan", q: &fakeQueryer{rows: newFakeRows([]any{"cmdb_sync_task", int64(0)}, []any{int64(1), int64(2)})}},
	}
	for _, tc := range cases {
		got, err := newTableStatsRepo(tc.q, []string{"cmdb_sync_task"}).TableRowEstimates(context.Background())
		if err == nil {
			t.Fatalf("%s: TableRowEstimates() error = nil, want a failure", tc.name)
		}
		if got != nil {
			t.Fatalf("%s: TableRowEstimates() = %v alongside error %v, want nil", tc.name, got, err)
		}
		if tc.want != nil && !errors.Is(err, tc.want) {
			t.Fatalf("%s: TableRowEstimates() error = %v, want %v", tc.name, err, tc.want)
		}
	}
}

// 观测清单定义在一处，这里锁住它的基本形态：表名是不带 schema 的普通标识符（与 to_regclass 的解析
// 方式一致），没有重复（重复项只会多出一个同样的数据点）。
func TestGrowthTables_WellFormed(t *testing.T) {
	t.Parallel()
	pattern := regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	seen := make(map[string]struct{}, len(growthTables))
	for _, name := range growthTables {
		if !pattern.MatchString(name) {
			t.Fatalf("growth table %q is not a plain lower-case identifier", name)
		}
		if _, dup := seen[name]; dup {
			t.Fatalf("growth table %q is listed twice", name)
		}
		seen[name] = struct{}{}
	}
}

// 观测清单是包内只读数据：仓储持有的是副本——改仓储手里的清单不会改到包级清单。
func TestTableStatsRepo_OwnsTableListCopy(t *testing.T) {
	t.Parallel()
	before := slices.Clone(growthTables)
	r := newTableStatsRepo(&fakeQueryer{}, growthTables)
	r.tables[0] = "tampered"
	if !reflect.DeepEqual(growthTables, before) {
		t.Fatalf("growthTables was modified: %v, want %v", growthTables, before)
	}
}
