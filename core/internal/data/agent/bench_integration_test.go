//go:build integration

package data

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// 本文件为「RecentAutoOutcome 的连接改写」补基准对比（AGENTS §6）：同一份数据上先跑 HEAD 的语句、
// 再跑当前的语句，上报一次调用的耗时（一次调用一条语句，不涉及往返次数）。
//
//   - 旧实现逐字取自 git HEAD（只读）：只改了名字、把方法接收者换成入参。
//   - 不含性能阈值断言（机器差异会变成不稳定测试），只在显式 -bench 时运行。
//   - 与集成测试同一份夹具：TEST_DATABASE_SOURCE 指向一次性库，会 TRUNCATE agent 表。
//     go test -tags=integration -run '^$' -bench 'BenchmarkIntegration_' -benchtime 10x ./internal/data/agent/

// simpleVariant 被比较的一个实现：fn 是一次基准操作（只读，不需要恢复夹具、不统计往返）。
type simpleVariant struct {
	name string
	fn   func(ctx context.Context) error
}

// runSimple 在同一份夹具上依次跑各实现：先预热一次（pgx 的语句描述缓存与服务端预备语句就位后，
// 首条语句多出的描述往返不计入），再计时 b.N 次。
//
// dup: 与 core/internal/data/monitor 基准文件里的同名类型 / 函数逐字相同——各包的测试文件互相
// import 不到，公共测试辅助包不在本次改动范围内。
func runSimple(b *testing.B, variants ...simpleVariant) {
	b.Helper()
	for _, v := range variants {
		b.Run(v.name, func(b *testing.B) {
			ctx := context.Background()
			if err := v.fn(ctx); err != nil {
				b.Fatalf("warm-up: %v", err)
			}
			b.ResetTimer()
			for range b.N {
				if err := v.fn(ctx); err != nil {
					b.Fatalf("%s: %v", v.name, err)
				}
			}
		})
	}
}

func recentAutoOutcomeLegacy(ctx context.Context, r *runRepo, profileID string, window time.Duration) (int, int, error) {
	var total, failed int
	err := r.data.Pool.QueryRow(ctx,
		`SELECT count(*)::int, count(*) FILTER (WHERE p.status = 6 OR (p.verify_result ->> 'worse')::boolean)::int
		 FROM incident_proposal p
		 WHERE p.proposer_type = 3 AND p.auto AND p.status IN (5, 6) AND p.create_time >= now() - make_interval(secs => $2::float8)
		   AND jsonb_typeof(p.evidence) = 'array'
		   AND EXISTS (SELECT 1 FROM jsonb_array_elements_text(p.evidence) e JOIN agent_run r ON e = 'agent_run:' || r.id::text WHERE r.profile_id = $1)`,
		profileID, window.Seconds()).Scan(&total, &failed)
	if err != nil {
		return 0, 0, fmt.Errorf("agent auto outcome: %w", err)
	}
	return total, failed, nil
}

// ---- 基准 ----

// BenchmarkIntegration_RecentAutoOutcome 一个 Profile 的自动通过结果统计：HEAD 的连接条件
// e = 'agent_run:' || r.id::text（对 agent_run 的行逐个算表达式）对当前从证据里取出 uuid 再按主键连接。
// 夹具：该 Profile 有 runs 次运行；proposals 条自动批准、已验证的建议，每条证据数组的第一个元素指向
// 其中一次运行（全部命中，total = proposals、failed = 0）。一个 op = 一次调用（只读）。
// 这是唯一以「运行数」为规模的档位：旧写法的工作量 ∝ 证据元素数 × 该 Profile 的运行数。
func BenchmarkIntegration_RecentAutoOutcome(b *testing.B) {
	const (
		proposals = 100
		profile   = "018f3e1a-0a01-7000-8000-0000000000a1"
		incident  = "018f3e1a-0a01-7000-8000-0000000000b1"
		agentUser = "018f3e1a-0a01-7000-8000-0000000000c1"
	)
	for _, runs := range []int{1_000, 10_000} {
		d := newIntegrationAgentData(b)
		repo := NewRunRepo(d)
		ctx := context.Background()
		// 夹具只清 agent 表；建议与它所属的故障记录在 incident 域，造数前清掉上一档留下的，结束后再清一次
		//（与 TestRecentAutoOutcomeEvidenceJoinMatchesLegacy 相同的范围）。
		cleanIncident := func(tb testing.TB) {
			tb.Helper()
			if _, err := d.Pool.Exec(ctx, "DELETE FROM incident_proposal WHERE incident_id = $1::uuid", incident); err != nil {
				tb.Fatalf("clean proposals: %v", err)
			}
			if _, err := d.Pool.Exec(ctx, "DELETE FROM incident_record WHERE id = $1::uuid", incident); err != nil {
				tb.Fatalf("clean incident: %v", err)
			}
		}
		cleanIncident(b)
		b.Cleanup(func() { cleanIncident(b) })
		if _, err := d.Pool.Exec(ctx,
			`INSERT INTO agent_run (id, task_id, profile_id, incident_id, agent_user_id, conversation_id, status, start_time, end_time)
			 SELECT md5('bench-run-' || g)::uuid, gen_random_uuid(), $1::uuid, $2::uuid, $3::uuid, gen_random_uuid(), 'succeeded',
			        now() - interval '20 minutes', now() - interval '10 minutes'
			 FROM generate_series(1, $4::int) g`, profile, incident, agentUser, runs); err != nil {
			b.Fatalf("seed runs: %v", err)
		}
		if _, err := d.Pool.Exec(ctx,
			`INSERT INTO incident_record (id, number, title, severity, status, occur_time) VALUES ($1::uuid, 'INC-BENCH-OUTCOME', 'title', 2, 1, now())`,
			incident); err != nil {
			b.Fatalf("seed incident: %v", err)
		}
		if _, err := d.Pool.Exec(ctx,
			`INSERT INTO incident_proposal (id, incident_id, title, action, idem_key, status, auto, proposer_type, evidence, verify_result, expire_time)
			 SELECT gen_random_uuid(), $1::uuid, 'step', '{}'::jsonb, 'bench-idem-' || g, 5, true, 3,
			        jsonb_build_array('agent_run:' || (md5('bench-run-' || (1 + (g % $3::int)))::uuid)::text, 'alert:bench'),
			        '{"worse": false}'::jsonb, now() + interval '1 hour'
			 FROM generate_series(1, $2::int) g`, incident, proposals, runs); err != nil {
			b.Fatalf("seed proposals: %v", err)
		}
		// 批量插入的表没有统计信息时计划不稳定：先 ANALYZE（生产库的表本来就有统计信息）。
		if _, err := d.Pool.Exec(ctx, "ANALYZE agent_run, incident_proposal"); err != nil {
			b.Fatalf("analyze: %v", err)
		}
		outcome := func(recent func(context.Context) (int, int, error)) func(context.Context) error {
			return func(ctx context.Context) error {
				total, failed, err := recent(ctx)
				if err != nil {
					return err
				}
				if total != proposals || failed != 0 {
					return fmt.Errorf("outcome = %d / %d, want %d / 0", total, failed, proposals)
				}
				return nil
			}
		}
		b.Run(fmt.Sprintf("runs=%d", runs), func(b *testing.B) {
			runSimple(b,
				simpleVariant{"legacy=concat-join", outcome(func(ctx context.Context) (int, int, error) {
					return recentAutoOutcomeLegacy(ctx, repo, profile, 24*time.Hour)
				})},
				simpleVariant{"current=pk-join", outcome(func(ctx context.Context) (int, int, error) {
					return repo.RecentAutoOutcome(ctx, profile, 24*time.Hour)
				})},
			)
		})
	}
}
