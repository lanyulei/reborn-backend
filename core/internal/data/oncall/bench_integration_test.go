//go:build integration

package data

import (
	"context"
	"fmt"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/oncall"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
)

// 本文件为「OnDuty 按值班表下推范围」改写补基准对比（AGENTS §6）：同一份数据上比较取某张值班表当前班次的
// 两种读法——HEAD 的 ListRange（取全组织此刻的班次，由调用方逐行筛出目标值班表）对当前的
// ListRangeScoped（范围谓词下推为 sh.schedule_id = $n）。
//
//   - 旧读法就是现行的 ShiftRepo.ListRange，语句与 HEAD 相同（HEAD 的 OnDuty 调的是它），没有拷贝代码。
//   - 不含性能阈值断言（机器差异会变成不稳定测试），只在显式 -bench 时运行。
//   - 与集成测试同一份夹具：TEST_DATABASE_SOURCE 指向一次性库，会 TRUNCATE 值班域的表。
//     go test -tags=integration -run '^$' -bench 'BenchmarkIntegration_' -benchtime 10x ./internal/data/oncall/

// simpleVariant 被比较的一个实现：fn 是一次基准操作（只读，不需要恢复夹具、不统计往返）。
type simpleVariant struct {
	name string
	fn   func(ctx context.Context) error
}

// runSimple 在同一份夹具上依次跑各实现：先预热一次（pgx 的语句描述缓存与服务端预备语句就位后，
// 首条语句多出的描述往返不计入），再计时 b.N 次。
//
// dup: 与 core/internal/data/monitor、core/internal/data/agent 基准文件里的同名类型 / 函数逐字相同——
// 各包的测试文件互相 import 不到，公共测试辅助包不在本次改动范围内。
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

// BenchmarkIntegration_OnDutyScheduleScope 取某张值班表此刻的班次（OnDuty 的读路径）。夹具：一个分组下
// schedules 张值班表，每张此刻主 / 备各一班（共 2 × schedules 个班次与全组织此刻的班次同量级）。
// 一个 op = 一次读取：旧读法取回全部 2 × schedules 行，新读法只取回目标值班表的 2 行。
func BenchmarkIntegration_OnDutyScheduleScope(b *testing.B) {
	for _, schedules := range []int{100, 5_000} {
		e := newEnv(b)
		repo := NewShiftRepo(&basedata.Data{Pool: e.pool})
		ctx := context.Background()
		const group = "018f3e1a-0c0d-7000-8000-000000000001"
		exec := func(sql string, args ...any) {
			if _, err := e.pool.Exec(ctx, sql, args...); err != nil {
				b.Fatalf("seed: %v\n%s", err, sql)
			}
		}
		exec(`INSERT INTO oncall_group (id, name) VALUES ($1::uuid, '基准')`, group)
		// 值班表的 ID 由序号确定（md5 → uuid）；轮转规则不影响这条读路径，取合法的最小配置。
		exec(`INSERT INTO oncall_schedule (id, group_id, name, timezone, rotation_unit, rotation_interval, anchor_time, backup_enabled, horizon_days)
			SELECT md5('bench-sched-' || g)::uuid, $1::uuid, '基准' || g, 'UTC', 1, 1, now(), false, 60 FROM generate_series(1, $2::int) g`, group, schedules)
		at := time.Now()
		shift := func(role int) string {
			return fmt.Sprintf(`SELECT gen_random_uuid(), md5('bench-sched-' || g)::uuid, $1::uuid, %d, gen_random_uuid(), $3::timestamptz, $4::timestamptz
			 FROM generate_series(1, $2::int) g`, role)
		}
		exec(`INSERT INTO oncall_shift (id, schedule_id, group_id, role, user_id, start_time, end_time) `+shift(1)+` UNION ALL `+shift(2),
			group, schedules, at.Add(-time.Hour), at.Add(time.Hour))
		// 批量插入的表没有统计信息时计划不稳定：先 ANALYZE（生产库的表本来就有统计信息）。
		exec("ANALYZE oncall_schedule, oncall_shift")
		scheduleID := ""
		if err := e.pool.QueryRow(ctx, "SELECT id::text FROM oncall_schedule WHERE name = $1", "基准1").Scan(&scheduleID); err != nil {
			b.Fatalf("queried schedule: %v", err)
		}
		window := []time.Time{at, at.Add(time.Second)}
		list := func(list func(context.Context) ([]*biz.Shift, error), want int) func(context.Context) error {
			return func(ctx context.Context) error {
				shifts, err := list(ctx)
				if err != nil {
					return err
				}
				if len(shifts) != want {
					return fmt.Errorf("shifts = %d, want %d", len(shifts), want)
				}
				return nil
			}
		}
		b.Run(fmt.Sprintf("schedules=%d", schedules), func(b *testing.B) {
			runSimple(b,
				simpleVariant{"legacy=whole-org-shifts", list(func(ctx context.Context) ([]*biz.Shift, error) {
					return repo.ListRange(ctx, window[0], window[1])
				}, 2*schedules)},
				simpleVariant{"current=pushed-down-scope", list(func(ctx context.Context) ([]*biz.Shift, error) {
					return repo.ListRangeScoped(ctx, window[0], window[1], biz.ShiftScope{ScheduleID: scheduleID})
				}, 2)},
			)
		})
	}
}
