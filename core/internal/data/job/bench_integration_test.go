//go:build integration

package data

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/job"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/kitconf"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// 本文件为「执行统计的 7 条聚合合并为 5 条」改写补基准对比（AGENTS §6：性能敏感改动必须附
// go test -bench 对比结果）：同一份数据上先跑 HEAD 的 Stats、再跑当前的 Stats，上报每次统计的
// 耗时与数据库往返次数。
//
//   - 旧实现逐字取自 git HEAD（只读）：只改了名字、把方法接收者换成入参。
//   - 不含性能阈值断言（机器差异会变成不稳定测试），只在显式 -bench 时运行。
//   - 与集成测试同一份护栏（testDSN）：TEST_DATABASE_SOURCE 指向一次性库。只清基准自己写、统计要读的
//     两张表（job_execution 及其子表、job_runner）。
//     go test -tags=integration -run '^$' -bench 'BenchmarkIntegration_' -benchtime 10x ./internal/data/job/

// tripCounter 统计客户端向数据库写 socket 的次数。pgx 一次往返恰是一次写：非管线语句逐条 flush，
// pgx.Batch 在管线里攒齐后一次 flush。所以这个数就是数据库往返次数——pgx 的 tracer 对批内每条
// 语句各报一次，区分不出「一个批里的 N 条」与「N 次单独往返」，socket 层能。
//
// dup: 与 notify/internal/data、core/internal/data/system 基准文件里的同名类型逐字相同——各模块的测试
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

// newTripCountingData 先按生产构造（dbx.NewPoolFromConf）建池，再拿同一份连接池配置另建一个
// 「写 socket 计数」的池返回；第一个池随即关闭，基准期间的往返全部经过计数器。
func newTripCountingData(tb testing.TB) (*basedata.Data, *tripCounter) {
	tb.Helper()
	prod, cleanup, err := basedata.NewData(&conf.Data{Database: &kitconf.Database{Source: testDSN(tb)}})
	if err != nil {
		// 连接串不进错误信息（%T 只报类型）。
		tb.Fatalf("NewData() failed (%T)", err)
	}
	defer cleanup()
	cfg := prod.Pool.Config() // 副本：与生产连接池同一份参数（超时、池大小等）
	trips := &tripCounter{}
	cfg.ConnConfig.DialFunc = trips.dial(cfg.ConnConfig.DialFunc)
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
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

// ---- 旧实现（git show HEAD:core/internal/data/job/stats.go，逐字拷贝，仅改名与接收者） ----

func statsLegacy(ctx context.Context, r *statsRepo, since time.Time, timezone string) (*biz.Stats, error) {
	out := &biz.Stats{Timezone: timezone}
	pool := r.data.Pool
	if err := pool.QueryRow(ctx,
		`SELECT count(*), count(*) FILTER (WHERE status NOT IN (1, 2, 9)), count(*) FILTER (WHERE status = 3),
		        COALESCE(avg(extract(epoch FROM finish_time - start_time)) FILTER (WHERE start_time IS NOT NULL AND finish_time IS NOT NULL), 0)::bigint,
		        COALESCE(sum(host_total), 0)
		 FROM job_execution WHERE create_time >= $1`, since).
		Scan(&out.Total, &out.Finished, &out.Success, &out.AvgDurationSeconds, &out.HostRuns); err != nil {
		return nil, fmt.Errorf("job stats totals: %w", err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT (SELECT count(*) FROM job_execution WHERE status IN (1, 2, 9)),
		        (SELECT count(*) FROM job_runner WHERE delete_time IS NULL AND enabled AND last_seen > now() - make_interval(secs => $1)),
		        (SELECT count(*) FROM job_runner WHERE delete_time IS NULL AND enabled)`, biz.RunnerOnlineWindow.Seconds()).
		Scan(&out.Active, &out.RunnersOnline, &out.RunnersTotal); err != nil {
		return nil, fmt.Errorf("job stats live counters: %w", err)
	}
	var err error
	// column 为代码内常量。
	group := func(column string) ([]biz.StatCount, error) {
		rows, err := pool.Query(ctx, "SELECT "+column+", count(*) FROM job_execution WHERE create_time >= $1 GROUP BY 1 ORDER BY 1", since)
		if err != nil {
			return nil, fmt.Errorf("job stats by %s: %w", column, err)
		}
		return pgx.CollectRows(rows, func(row pgx.CollectableRow) (biz.StatCount, error) {
			var key int32
			var c biz.StatCount
			err := row.Scan(&key, &c.Count)
			c.Key = strconv.Itoa(int(key))
			return c, err
		})
	}
	if out.ByStatus, err = group("status"); err != nil {
		return nil, err
	}
	if out.ByKind, err = group("kind"); err != nil {
		return nil, err
	}
	if out.ByTrigger, err = group("trigger"); err != nil {
		return nil, err
	}
	rows, err := pool.Query(ctx,
		`SELECT template_id::text, max(template_name), count(*), count(*) FILTER (WHERE status IN `+failedStatuses+`)
		 FROM job_execution WHERE create_time >= $1 AND template_id IS NOT NULL
		 GROUP BY template_id ORDER BY 3 DESC, 1 LIMIT 10`, since)
	if err != nil {
		return nil, fmt.Errorf("job stats top templates: %w", err)
	}
	if out.TopTemplates, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (biz.StatCount, error) {
		var c biz.StatCount
		err := row.Scan(&c.Key, &c.Name, &c.Count, &c.Failed)
		return c, err
	}); err != nil {
		return nil, fmt.Errorf("job stats top templates: %w", err)
	}
	rows, err = pool.Query(ctx,
		`SELECT to_char(create_time AT TIME ZONE $2, 'YYYY-MM-DD'), count(*), count(*) FILTER (WHERE status = 3),
		        count(*) FILTER (WHERE status IN `+failedStatuses+`)
		 FROM job_execution WHERE create_time >= $1 GROUP BY 1 ORDER BY 1`, since, timezone)
	if err != nil {
		return nil, fmt.Errorf("job stats trend: %w", err)
	}
	if out.Trend, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (biz.StatTrendPoint, error) {
		var p biz.StatTrendPoint
		err := row.Scan(&p.Day, &p.Total, &p.Success, &p.Failed)
		return p, err
	}); err != nil {
		return nil, fmt.Errorf("job stats trend: %w", err)
	}
	return out, nil
}

// ---- 基准 ----

// seedBenchExecutions 直插 n 条执行：类型、触发来源、状态按序号轮转（三组分布各有若干键），
// 全部落在最近一小时之内；已结束的带起止时间。只写统计要读的列。
func seedBenchExecutions(tb testing.TB, d *basedata.Data, n int) {
	tb.Helper()
	if _, err := d.Pool.Exec(context.Background(),
		`INSERT INTO job_execution (id, template_id, template_name, template_version, kind, trigger, creator, status, spec, timeout_s,
			host_total, host_ok, start_time, finish_time, create_time)
		 SELECT gen_random_uuid(), CASE WHEN 1 + (g % 4) = 1 THEN gen_random_uuid() END, 'bench-' || g, 1, 1 + (g % 4)::smallint,
		        1 + (g % 4)::smallint, '018f3e1a-0e00-7000-8000-00000000000a'::uuid, 1 + (g % 10)::smallint, '{"playbook":"site.yml"}'::jsonb, 600,
		        10, 10, now() - interval '5 minutes', CASE WHEN 1 + (g % 10) IN (1, 2, 9) THEN NULL ELSE now() - interval '1 minute' END,
		        now() - make_interval(mins => (g % 50)::int)
		 FROM generate_series(1, $1::int) g`, n); err != nil {
		tb.Fatalf("seed executions: %v", err)
	}
}

// BenchmarkIntegration_ExecutionStats 执行统计：HEAD 的 7 条语句（状态 / 类型 / 触发来源各一条 GROUP BY）
// 对当前 5 条（三组分布合并成一条 GROUPING SETS）。一个 op = Stats 一次（只读，不需要恢复夹具）。
func BenchmarkIntegration_ExecutionStats(b *testing.B) {
	for _, n := range []int{1_000, 100_000} {
		d, trips := newTripCountingData(b)
		// 只清基准自己写、统计要读的表；CASCADE 带上引用 job_execution 的子表。
		if _, err := d.Pool.Exec(context.Background(), "TRUNCATE TABLE job_execution, job_runner CASCADE"); err != nil {
			b.Fatalf("truncate: %v", err)
		}
		seedBenchExecutions(b, d, n)
		// 刚批量插入的表没有统计信息（relpages / reltuples 还是空的），计划会随 autovacuum 的 analyze 时机
		// 而变；先 ANALYZE，让两种写法基于真实统计信息选计划（生产库的表本来就有统计信息）。
		if _, err := d.Pool.Exec(context.Background(), "ANALYZE job_execution, job_runner"); err != nil {
			b.Fatalf("analyze: %v", err)
		}
		repo := NewStatsRepo(d)
		since := time.Now().Add(-24 * time.Hour)
		stats := func(stats func(context.Context) (*biz.Stats, error)) func(context.Context) error {
			return func(ctx context.Context) error {
				got, err := stats(ctx)
				if err != nil {
					return err
				}
				// 两边读到的是同一份数据：总数、三组分布的组数应一致且等于造数的行数 / 取值个数。
				if got.Total != int64(n) || len(got.ByStatus) != 10 || len(got.ByKind) != 4 || len(got.ByTrigger) != 4 {
					return fmt.Errorf("stats = total %d, %d statuses, %d kinds, %d triggers; want %d, 10, 4, 4",
						got.Total, len(got.ByStatus), len(got.ByKind), len(got.ByTrigger), n)
				}
				return nil
			}
		}
		b.Run(fmt.Sprintf("executions=%d", n), func(b *testing.B) {
			runVariants(b, trips, nil,
				benchVariant{"legacy=7-statements", stats(func(ctx context.Context) (*biz.Stats, error) {
					return statsLegacy(ctx, repo, since, "UTC")
				})},
				benchVariant{"current=grouping-sets", stats(func(ctx context.Context) (*biz.Stats, error) {
					return repo.Stats(ctx, since, "UTC")
				})},
			)
		})
	}
}
