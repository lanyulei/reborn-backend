//go:build integration

package data

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/idgen"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// 本文件为「逐行往返 → 批量语句」改写补基准对比（AGENTS §6：性能敏感改动必须附 go test -bench
// 对比结果）：同一份夹具、同一个连接池上先跑 HEAD 的旧实现、再跑当前实现，上报每个操作的耗时与
// 数据库往返次数。
//
//   - 旧实现逐字取自 git HEAD（只读）：只改了名字、把方法接收者换成入参。
//   - 不含性能阈值断言（机器差异会变成不稳定测试），只在显式 -bench 时运行。
//   - 与集成测试同一份夹具：TEST_DATABASE_SOURCE 指向一次性库，会 TRUNCATE 系统域全部表。
//     go test -tags=integration -run '^$' -bench 'BenchmarkIntegration_' -benchtime 10x ./internal/data/system/
//   - 造数后 ANALYZE：批量插入的表没有统计信息时计划不稳定（见各基准的说明）。

// tripCounter 统计客户端向数据库写 socket 的次数。pgx 一次往返恰是一次写：非管线语句逐条 flush，
// pgx.Batch 在管线里攒齐后一次 flush。所以这个数就是数据库往返次数——pgx 的 tracer 对批内每条
// 语句各报一次，区分不出「一个批里的 N 条」与「N 次单独往返」，socket 层能。
//
// dup: 与 notify/internal/data、core/internal/data/job 基准文件里的同名类型逐字相同——各模块的测试
// 文件互相 import 不到，公共测试辅助包不在本次改动范围内；要合并应提到 kit/dbx 的测试辅助里。
type tripCounter struct{ writes atomic.Int64 }

func (c *tripCounter) reset()        { c.writes.Store(0) }
func (c *tripCounter) snapshot() int { return int(c.writes.Load()) }

// dial 包住连接池原有的拨号函数：此后新建的每条连接都经过计数。
func (c *tripCounter) dial(next pgconn.DialFunc) pgconn.DialFunc {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := next(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return &tripConn{Conn: conn, trips: c}, nil
	}
}

// tripConn 只拦截 Write，其余行为原样透传。
type tripConn struct {
	net.Conn
	trips *tripCounter
}

func (c *tripConn) Write(p []byte) (int, error) {
	c.trips.writes.Add(1)
	return c.Conn.Write(p)
}

// newTripCountingData 先走集成夹具（护栏：库名须含 test；清空系统域全部表），再拿同一份连接池
// 配置另建一个「写 socket 计数」的池返回；夹具自己的池随即关闭，基准期间的往返全部经过计数器。
func newTripCountingData(tb testing.TB) (*basedata.Data, *tripCounter) {
	tb.Helper()
	fixture := newIntegrationData(tb)
	cfg := fixture.Pool.Config() // 副本：与生产连接池同一份参数（超时、池大小等）
	fixture.Pool.Close()
	trips := &tripCounter{}
	cfg.ConnConfig.DialFunc = trips.dial(cfg.ConnConfig.DialFunc)
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		// 连接串不进错误信息（%T 只报类型）。
		tb.Fatalf("create trip-counting pool failed (%T)", err)
	}
	tb.Cleanup(pool.Close)
	return &basedata.Data{Pool: pool}, trips
}

// benchVariant 被比较的一个实现：fn 是一次基准操作。
type benchVariant struct {
	name string
	fn   func(ctx context.Context) error
}

// runVariants 在同一份夹具上依次跑各实现。reset 在每次操作之前把夹具恢复原状（含预热之前），
// 处于计时区间外；往返数只累计计时区间内的。先预热一次：pgx 的语句描述缓存与服务端预备语句
// 就位后，首条语句多出的描述往返不计入。
func runVariants(b *testing.B, trips *tripCounter, reset func(tb testing.TB, ctx context.Context), variants ...benchVariant) {
	b.Helper()
	for _, v := range variants {
		b.Run(v.name, func(b *testing.B) {
			ctx := context.Background()
			if reset != nil {
				reset(b, ctx)
			}
			if err := v.fn(ctx); err != nil {
				b.Fatalf("warm-up: %v", err)
			}
			var roundTrips int64
			b.ResetTimer()
			for range b.N {
				b.StopTimer()
				if reset != nil {
					reset(b, ctx)
				}
				trips.reset()
				b.StartTimer()
				if err := v.fn(ctx); err != nil {
					b.Fatalf("%s: %v", v.name, err)
				}
				roundTrips += int64(trips.snapshot())
			}
			b.StopTimer()
			b.ReportMetric(float64(roundTrips)/float64(b.N), "roundtrips/op")
		})
	}
}

// ---- 旧实现（git show HEAD:<路径>，逐字拷贝，仅改名） ----

// replaceForRoleLegacy HEAD:core/internal/data/system/permission.go 的 ReplaceForRole：两类 DELETE
// 之后逐行 INSERT（主键逐行生成），一行一次往返。不使用接收者，故改为普通函数；其余逐字保留。
func replaceForRoleLegacy(ctx context.Context, tx txrun.Tx, roleID string, grants biz.GrantSet, operations []string) error {
	if _, err := tx.Exec(ctx, "DELETE FROM system_grant WHERE role_id = $1", roleID); err != nil {
		return fmt.Errorf("clear role grants: %w", err)
	}
	if _, err := tx.Exec(ctx, "DELETE FROM system_permission WHERE role_id = $1", roleID); err != nil {
		return fmt.Errorf("clear role permissions: %w", err)
	}

	insert := func(kind string, codes []string) error {
		for _, code := range codes {
			id, err := idgen.NewID()
			if err != nil {
				return fmt.Errorf("generate grant id: %w", err)
			}
			if _, err := tx.Exec(ctx,
				"INSERT INTO system_grant (id, role_id, resource_type, resource_code) VALUES ($1, $2, $3, $4)",
				id, roleID, kind, code); err != nil {
				return fmt.Errorf("insert role grant: %w", err)
			}
		}
		return nil
	}
	if err := insert(grantTypeMenu, grants.Menus); err != nil {
		return err
	}
	if err := insert(grantTypeElement, grants.Elements); err != nil {
		return err
	}
	if err := insert(grantTypeAPI, grants.Apis); err != nil {
		return err
	}
	for _, op := range operations {
		id, err := idgen.NewID()
		if err != nil {
			return fmt.Errorf("generate permission id: %w", err)
		}
		if _, err := tx.Exec(ctx,
			"INSERT INTO system_permission (id, role_id, operation) VALUES ($1, $2, $3)",
			id, roleID, op); err != nil {
			return fmt.Errorf("insert role permission: %w", err)
		}
	}
	return nil
}

// ---- 基准 ----

// benchCodes n 个互不重复的资源码 / 操作码。
func benchCodes(prefix string, n int) []string {
	out := make([]string, 0, n)
	for i := range n {
		out = append(out, fmt.Sprintf("%s.%04d", prefix, i))
	}
	return out
}

// BenchmarkIntegration_ReplaceForRole 角色授权全量替换：HEAD 逐行 INSERT（一行一次往返）对当前
// 每类授权一条 unnest 批量语句。一个 op = 用例层事务里的一次保存（DELETE + INSERT）——两次保存
// 之间库内状态相同，不需要恢复夹具。两档行数：小档各类取上限的 1/4（16 / 32 / 64 + 策略 64），
// 大档取各类上限（64 / 128 / 256 + 策略 256）。
func BenchmarkIntegration_ReplaceForRole(b *testing.B) {
	for _, tc := range []struct{ menus, elements, apis, ops int }{{16, 32, 64, 64}, {64, 128, 256, 256}} {
		d, trips := newTripCountingData(b)
		repo := NewPermissionRepo(d)
		txs := basedata.NewTxManager(d)
		role := &biz.Role{ID: "018f3e1a-0100-7000-8000-0000000000b2", Name: "基准", Code: "bench", Status: biz.RoleStatusEnabled}
		if err := NewRoleRepo(d).CreateRole(context.Background(), role); err != nil {
			b.Fatalf("CreateRole() error = %v", err)
		}
		grants := biz.GrantSet{Menus: benchCodes("menu", tc.menus), Elements: benchCodes("element", tc.elements), Apis: benchCodes("api", tc.apis)}
		operations := benchCodes("op", tc.ops)
		wantGrants, wantPerms := tc.menus+tc.elements+tc.apis, tc.ops
		replace := func(replace func(context.Context, txrun.Tx, string, biz.GrantSet, []string) error) func(context.Context) error {
			return func(ctx context.Context) error {
				return txs.WithinTx(ctx, func(tx txrun.Tx) error {
					return replace(ctx, tx, role.ID, grants, operations)
				})
			}
		}
		// 计时之前各跑一次并回读：两种做法写出的行数相同。
		var gotGrants, gotPerms int
		for _, v := range []struct {
			name string
			fn   func(context.Context) error
		}{{"legacy", replace(replaceForRoleLegacy)}, {"current", replace(repo.ReplaceForRole)}} {
			if err := v.fn(context.Background()); err != nil {
				b.Fatalf("%s: %v", v.name, err)
			}
			if err := d.Pool.QueryRow(context.Background(),
				`SELECT (SELECT count(*) FROM system_grant WHERE role_id = $1::uuid), (SELECT count(*) FROM system_permission WHERE role_id = $1::uuid)`,
				role.ID).Scan(&gotGrants, &gotPerms); err != nil {
				b.Fatal(err)
			}
			if gotGrants != wantGrants || gotPerms != wantPerms {
				b.Fatalf("%s: rows = %d grants, %d permissions; want %d, %d", v.name, gotGrants, gotPerms, wantGrants, wantPerms)
			}
		}
		b.Run(fmt.Sprintf("rows=%d", wantGrants+wantPerms), func(b *testing.B) {
			runVariants(b, trips, nil,
				benchVariant{"legacy=per-row-insert", replace(replaceForRoleLegacy)},
				benchVariant{"current=unnest-inserts", replace(repo.ReplaceForRole)},
			)
		})
	}
}
