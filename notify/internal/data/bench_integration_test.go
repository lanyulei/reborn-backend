//go:build integration

package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/dbx"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/notify/internal/biz"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// 本文件为「逐行往返 → 单批 / 单语句」改写（AGENTS §13.7）补基准对比（AGENTS §6：性能敏感改动
// 必须附 go test -bench 对比结果）：同一份夹具、同一个连接池上先跑 HEAD 的旧实现、再跑当前实现，
// 上报每个操作的耗时与数据库往返次数。
//
//   - 旧实现逐字取自 git HEAD（只读）：只改了名字、把方法接收者换成入参、省略日志调用；它们调用的
//     finishDispatch / Release / Claim 与 HEAD 逐字相同（落笔前已逐函数比对）。
//   - 不含性能阈值断言（机器差异会变成不稳定测试），只在显式 -bench 时运行。
//   - 与集成测试同一份夹具：TEST_DATABASE_SOURCE 指向一次性库，会 TRUNCATE 通知域全部表。
//     go test -tags=integration -run '^$' -bench 'BenchmarkIntegration_' -benchtime 10x ./internal/data/

// tripCounter 统计客户端向数据库写 socket 的次数。pgx 一次往返恰是一次写：非管线语句逐条 flush，
// pgx.Batch 在管线里攒齐后一次 flush。所以这个数就是数据库往返次数——pgx 的 tracer 对批内每条
// 语句各报一次，区分不出「一个批里的 N 条」与「N 次单独往返」，socket 层能。
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

// newTripCountingData 先走集成夹具（护栏：库名须含 test；清空通知域全部表），再拿同一份连接池
// 配置另建一个「写 socket 计数」的池返回；夹具自己的池随即关闭，基准期间的往返全部经过计数器。
func newTripCountingData(tb testing.TB) (*Data, *tripCounter) {
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
	return &Data{Pool: pool}, trips
}

// benchExec 基准夹具的一条写语句（只在计时区间外调用）。
func benchExec(tb testing.TB, d *Data, sql string, args ...any) {
	tb.Helper()
	if _, err := d.Pool.Exec(context.Background(), sql, args...); err != nil {
		tb.Fatalf("bench fixture: %v\n%s", err, sql)
	}
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

// upsertAggGroupsLegacy HEAD:notify/internal/data/agg.go 的 upsertAggGroups：循环内逐键
// tx.QueryRow，一键一次往返。SQL 与当前实现一字未动。
func upsertAggGroupsLegacy(ctx context.Context, tx pgx.Tx, incs []biz.AggIncrement) (map[string]biz.AggVerdict, error) {
	const q = `INSERT INTO notify_agg_group
		(group_key, biz_id, event_type, route_id, first_event_id, first_event_time, count, samples, window_until, suppressed_until, status, create_time, update_time)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, now() + make_interval(secs => $9), NULL, 1, now(), now())
		ON CONFLICT (group_key) DO UPDATE SET
		  count = CASE WHEN ` + reopenCond + ` THEN EXCLUDED.count ELSE notify_agg_group.count + EXCLUDED.count END,
		  samples = CASE WHEN ` + reopenCond + ` THEN EXCLUDED.samples
		                 WHEN notify_agg_group.status = 1 THEN (
		                   SELECT COALESCE(jsonb_agg(e ORDER BY ord), '[]'::jsonb) FROM (
		                     SELECT e, ord FROM jsonb_array_elements(notify_agg_group.samples || EXCLUDED.samples) WITH ORDINALITY AS t(e, ord)
		                     ORDER BY ord LIMIT 20) s)
		                 ELSE notify_agg_group.samples END,
		  route_id = CASE WHEN ` + reopenCond + ` THEN EXCLUDED.route_id ELSE notify_agg_group.route_id END,
		  first_event_id = CASE WHEN ` + reopenCond + ` THEN EXCLUDED.first_event_id ELSE notify_agg_group.first_event_id END,
		  first_event_time = CASE WHEN ` + reopenCond + ` THEN EXCLUDED.first_event_time ELSE notify_agg_group.first_event_time END,
		  window_until = CASE WHEN ` + reopenCond + ` THEN EXCLUDED.window_until
		                      WHEN notify_agg_group.status = 1 THEN LEAST(now() + make_interval(secs => $9), notify_agg_group.first_event_time + make_interval(secs => $10))
		                      ELSE notify_agg_group.window_until END,
		  status = CASE WHEN ` + reopenCond + ` THEN 1 ELSE notify_agg_group.status END,
		  suppressed_until = CASE WHEN ` + reopenCond + ` THEN NULL ELSE notify_agg_group.suppressed_until END,
		  update_time = now()
		RETURNING (status = 2 AND suppressed_until IS NOT NULL AND suppressed_until > now())`
	out := make(map[string]biz.AggVerdict, len(incs))
	for _, inc := range incs {
		samples, err := json.Marshal(inc.Samples)
		if err != nil {
			return nil, fmt.Errorf("encode agg samples: %w", err)
		}
		var suppressed bool
		if err := tx.QueryRow(ctx, q, inc.Key, inc.BizID, inc.EventType, inc.RouteID, inc.FirstEventID, inc.FirstEventTime,
			inc.Count, samples, float64(inc.WindowS), inc.MaxWindow.Seconds()).Scan(&suppressed); err != nil {
			return nil, fmt.Errorf("upsert agg group %s: %w", inc.Key, err)
		}
		out[inc.Key] = biz.AggVerdict{Suppressed: suppressed}
	}
	return out, nil
}

// reapExpiredLegacy HEAD:notify/internal/data/dispatch.go 的 ReapExpired：选行后逐行
// finishDispatch——每行一个 pgx.Batch、一次往返。方法接收者换成连接池入参，其余逐字保留。
func reapExpiredLegacy(ctx context.Context, pool *pgxpool.Pool, limit int, maxAttempts int32, backoff func(int32) time.Duration) (int, error) {
	n := 0
	err := dbx.WithTx(ctx, pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx,
			"SELECT "+claimedColumns+" FROM notify_dispatch d WHERE d.status = 2 AND d.next_retry_time < now()"+
				" ORDER BY d.next_retry_time LIMIT $1 FOR UPDATE SKIP LOCKED", limit)
		if err != nil {
			return fmt.Errorf("scan expired leases: %w", err)
		}
		var expired []*biz.ClaimedDispatch
		for rows.Next() {
			d, err := scanClaimed(rows.Scan)
			if err != nil {
				rows.Close()
				return fmt.Errorf("scan expired dispatch: %w", err)
			}
			expired = append(expired, d)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, d := range expired {
			res := biz.FinishResult{OK: false, Retryable: true, Error: "lease expired (sender crashed or stalled mid-send)", RetryAfter: backoff(d.Attempts)}
			if err := finishDispatch(ctx, tx, d.ID, d.CreateTime, d.Attempts, d.EventID, d.ChannelID, d.Recipient, res, maxAttempts); err != nil {
				return err
			}
			n++
		}
		return nil
	})
	return n, err
}

// releaseAllLegacy HEAD:notify/internal/biz/sender.go 的 releaseAll 循环体：逐行
// repo.Release——每行一条自动提交语句、一次往返。失败分支里的日志调用省略，改为直接返回。
func releaseAllLegacy(ctx context.Context, repo *dispatchRepo, claimed []*biz.ClaimedDispatch, delay time.Duration) error {
	for _, d := range claimed {
		if err := repo.Release(ctx, d, delay); err != nil && !errors.Is(err, biz.ErrStaleClaim) {
			return err
		}
	}
	return nil
}

// loadDeadContentsLegacy HEAD:notify/internal/data/dlq.go 的 Replay 里取 content 的循环：逐条
// tx.QueryRow，一条死信一次往返。
func loadDeadContentsLegacy(ctx context.Context, tx pgx.Tx, dls []*biz.DeadLetter) error {
	for _, dl := range dls {
		if err := tx.QueryRow(ctx, "SELECT content FROM notify_dispatch WHERE id = $1 AND create_time <= $2",
			dl.DispatchID, dl.CreateTime).Scan(&dl.Content); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("load dead dispatch content: %w", err)
		}
	}
	return nil
}

// ---- 基准 ----

// benchAggIncrements n 个互不相同、按键排序的组各来一条事件（同 router：批内按键有序 upsert）。
func benchAggIncrements(tb testing.TB, n int) []biz.AggIncrement {
	tb.Helper()
	first, err := idgen.NewID()
	if err != nil {
		tb.Fatal(err)
	}
	now := time.Now().UTC()
	incs := make([]biz.AggIncrement, n)
	for i := range n {
		incs[i] = biz.AggIncrement{
			Key: fmt.Sprintf("default|bench-%06d", i), BizID: "default", EventType: "host.down", RouteID: idRouteAgg,
			FirstEventID: first, FirstEventTime: now, Count: 1,
			Samples:   []biz.AggSample{{EventID: first, Title: "bench", Severity: 2, CreateTime: now}},
			WindowS:   30,
			MaxWindow: time.Hour,
		}
	}
	return incs
}

// BenchmarkIntegration_AggUpsertGroups 聚合组 upsert：HEAD 逐键 QueryRow（一键一次往返）对
// 当前一个 pgx.Batch（整批一次往返）。一个 op = 一个路由事务里 upsert N 个组；测的是稳态
// （组已存在，走累加并延长窗口的分支——预热一次后即为此形态）。
func BenchmarkIntegration_AggUpsertGroups(b *testing.B) {
	for _, n := range []int{100, 1000} {
		d, trips := newTripCountingData(b)
		incs := benchAggIncrements(b, n)
		inTx := func(upsert func(context.Context, pgx.Tx, []biz.AggIncrement) (map[string]biz.AggVerdict, error)) func(context.Context) error {
			return func(ctx context.Context) error {
				return dbx.WithTx(ctx, d.Pool, func(tx pgx.Tx) error {
					verdicts, err := upsert(ctx, tx, incs)
					if err != nil {
						return err
					}
					if len(verdicts) != n {
						return fmt.Errorf("verdicts = %d, want %d", len(verdicts), n)
					}
					return nil
				})
			}
		}
		b.Run(fmt.Sprintf("groups=%d", n), func(b *testing.B) {
			runVariants(b, trips, nil,
				benchVariant{"legacy=per-key-queryrow", inTx(upsertAggGroupsLegacy)},
				benchVariant{"current=one-pgx-batch", inTx(upsertAggGroups)},
			)
		})
	}
}

// BenchmarkIntegration_DispatchReapExpired 租约回收：HEAD 逐行 finishDispatch（每行一个批、
// 一次往返）对当前 reapDispatches（三条 unnest 语句一个批）。夹具是 N 行「租约已过期的
// sending、attempts=1」——回收后全部转 failed（不到上限，不进死信）；每个 op 之前恢复原状
// 并清掉上一轮写的投递日志。
func BenchmarkIntegration_DispatchReapExpired(b *testing.B) {
	for _, n := range []int{100, 1000} {
		d, trips := newTripCountingData(b)
		const ev = "018f3e1a-3b01-7000-8000-000000000001"
		benchExec(b, d, `INSERT INTO notify_dispatch (id, event_id, channel_id, recipient, status, attempts, next_retry_time, send_key, create_time)
			SELECT gen_random_uuid(), $1::uuid, $2::uuid, 'bench@example.com', 2, 1, now() - interval '1 minute', md5(random()::text), now()
			FROM generate_series(1, $3)`, ev, idChanHook, n)
		repo := NewDispatchRepo(d)
		backoff := func(int32) time.Duration { return 10 * time.Minute }
		reset := func(tb testing.TB, _ context.Context) {
			benchExec(tb, d, "DELETE FROM notify_delivery_log WHERE dispatch_id IN (SELECT id FROM notify_dispatch WHERE event_id = $1)", ev)
			benchExec(tb, d, `UPDATE notify_dispatch SET status = 2, attempts = 1, next_retry_time = now() - interval '1 minute', last_error = NULL
				WHERE event_id = $1`, ev)
		}
		reap := func(reap func(context.Context) (int, error)) func(context.Context) error {
			return func(ctx context.Context) error {
				got, err := reap(ctx)
				if err != nil {
					return err
				}
				if got != n {
					return fmt.Errorf("reaped = %d, want %d", got, n)
				}
				return nil
			}
		}
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			runVariants(b, trips, reset,
				benchVariant{"legacy=per-row-finish", reap(func(ctx context.Context) (int, error) {
					return reapExpiredLegacy(ctx, d.Pool, n, 6, backoff)
				})},
				benchVariant{"current=unnest-batch", reap(func(ctx context.Context) (int, error) {
					return repo.ReapExpired(ctx, n, 6, backoff)
				})},
			)
		})
	}
}

// BenchmarkIntegration_DispatchRelease 整批放回：HEAD 的 releaseAll 逐行 Release（每行一条
// 自动提交语句）对当前一条 ReleaseBatch。夹具是发送器刚声明到的一批任务；每个 op 之前恢复成
// 到期 pending 并重新 Claim。
func BenchmarkIntegration_DispatchRelease(b *testing.B) {
	for _, n := range []int{100, 1000} {
		d, trips := newTripCountingData(b)
		const ev = "018f3e1a-3b02-7000-8000-000000000001"
		benchExec(b, d, `INSERT INTO notify_dispatch (id, event_id, channel_id, recipient, status, attempts, next_retry_time, send_key, create_time)
			SELECT gen_random_uuid(), $1::uuid, $2::uuid, 'bench@example.com', 1, 0, now() - interval '1 second', md5(random()::text), now()
			FROM generate_series(1, $3)`, ev, idChanHook, n)
		repo := NewDispatchRepo(d)
		var claimed []*biz.ClaimedDispatch
		reset := func(tb testing.TB, ctx context.Context) {
			benchExec(tb, d, `UPDATE notify_dispatch SET status = 1, attempts = 0, next_retry_time = now() - interval '1 second',
				publish_state = 0, relay_claim_until = NULL WHERE event_id = $1`, ev)
			var err error
			if claimed, err = repo.Claim(ctx, n, 5*time.Minute, nil); err != nil {
				tb.Fatalf("Claim: %v", err)
			}
			if len(claimed) != n {
				tb.Fatalf("Claim = %d dispatches, want %d", len(claimed), n)
			}
		}
		const delay = 5 * time.Second
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			runVariants(b, trips, reset,
				benchVariant{"legacy=per-row-release", func(ctx context.Context) error {
					return releaseAllLegacy(ctx, repo, claimed, delay)
				}},
				benchVariant{"current=releasebatch", func(ctx context.Context) error {
					return repo.ReleaseBatch(ctx, claimed, delay)
				}},
			)
		})
	}
}

// BenchmarkIntegration_DlqLoadContents 死信重放取 content 快照：HEAD 逐条 SELECT（一条死信
// 一次往返）对当前一次查询。只读，事务提交即回滚，不需要恢复夹具。夹具：N 条带聚合快照的
// 死 dispatch 与对应的 N 条待重放死信（死信时间晚于 dispatch 创建时间，快照应被取到）。
func BenchmarkIntegration_DlqLoadContents(b *testing.B) {
	for _, n := range []int{100, 1000} {
		d, trips := newTripCountingData(b)
		const ev = "018f3e1a-3b03-7000-8000-000000000001"
		ids := make([]string, n)
		recipients := make([]string, n)
		contents := make([]string, n)
		for i := range n {
			id, err := idgen.NewID()
			if err != nil {
				b.Fatal(err)
			}
			ids[i] = id
			recipients[i] = fmt.Sprintf("bench-%d@example.com", i)
			contents[i] = `{"subject":"s","body":"b","agg":{"count":9}}`
		}
		benchExec(b, d, `INSERT INTO notify_dispatch (id, event_id, channel_id, recipient, content, status, attempts, next_retry_time, send_key, create_time)
			SELECT t.id::uuid, $1::uuid, $2::uuid, t.recipient, t.content::jsonb, 5, 6, NULL, 'k', now() - interval '10 minutes'
			FROM unnest($3::text[], $4::text[], $5::text[]) AS t(id, recipient, content)`, ev, idChanHook, ids, recipients, contents)
		benchExec(b, d, `INSERT INTO notify_dlq (id, dispatch_id, event_id, channel_id, recipient, last_error, attempts, replay_state, create_time)
			SELECT t.id::uuid, t.id::uuid, $1::uuid, $2::uuid, t.recipient, 'boom', 6, 2, now() - interval '5 minutes'
			FROM unnest($3::text[], $4::text[]) AS t(id, recipient)`, ev, idChanHook, ids, recipients)

		// 与 Replay 声明死信后手里的形态一致。
		rows, err := d.Pool.Query(context.Background(),
			`SELECT id, dispatch_id, event_id, channel_id, recipient, COALESCE(last_error, ''), attempts, create_time
			 FROM notify_dlq ORDER BY update_time, id`)
		if err != nil {
			b.Fatal(err)
		}
		var dls []*biz.DeadLetter
		for rows.Next() {
			var dl biz.DeadLetter
			if err := rows.Scan(&dl.ID, &dl.DispatchID, &dl.EventID, &dl.ChannelID, &dl.Recipient, &dl.LastError, &dl.Attempts, &dl.CreateTime); err != nil {
				b.Fatal(err)
			}
			dls = append(dls, &dl)
		}
		rows.Close()
		if err := rows.Err(); err != nil || len(dls) != n {
			b.Fatalf("dead letters = %d, %v; want %d", len(dls), err, n)
		}

		reset := func(_ testing.TB, _ context.Context) {
			for _, dl := range dls {
				dl.Content = nil
			}
		}
		inTx := func(load func(context.Context, pgx.Tx, []*biz.DeadLetter) error) func(context.Context) error {
			return func(ctx context.Context) error {
				return dbx.WithTxOptions(ctx, d.Pool, pgx.TxOptions{AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
					if err := load(ctx, tx, dls); err != nil {
						return err
					}
					loaded := 0
					for _, dl := range dls {
						if dl.Content != nil {
							loaded++
						}
					}
					if loaded != n {
						return fmt.Errorf("contents loaded = %d, want %d", loaded, n)
					}
					return nil
				})
			}
		}
		b.Run(fmt.Sprintf("deadletters=%d", n), func(b *testing.B) {
			runVariants(b, trips, reset,
				benchVariant{"legacy=per-row-select", inTx(loadDeadContentsLegacy)},
				benchVariant{"current=single-query", inTx(loadDeadContents)},
			)
		})
	}
}
