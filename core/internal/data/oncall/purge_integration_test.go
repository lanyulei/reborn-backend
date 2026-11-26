//go:build integration

package data

import (
	"context"
	"strings"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/oncall"

	"github.com/jackc/pgx/v5"
)

// shiftCounts 各值班表现有的班次数（值班表 ID → 条数，不含为 0 的）。
func (e *env) shiftCounts(t *testing.T) map[string]int {
	t.Helper()
	rows, err := e.pool.Query(context.Background(), "SELECT schedule_id::text, count(*) FROM oncall_shift GROUP BY 1")
	if err != nil {
		t.Fatalf("count shifts: %v", err)
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var id string
		var n int
		if err := rows.Scan(&id, &n); err != nil {
			t.Fatal(err)
		}
		out[id] = n
	}
	return out
}

// newShiftedSchedule 新建一张名为 name 的值班表（同一分组内未删除的值班表名称唯一），并把已排好的班次整体往前拨
// shiftBack：这样删除它之后留下的历史班次不止「进行中被截断的那一班」，足以覆盖分批。
func (e *env) newShiftedSchedule(t *testing.T, groupID, name string, shiftBack time.Duration) *biz.Schedule {
	t.Helper()
	ctx := context.Background()
	cfg := dailyConfig(time.Now().Add(-72*time.Hour), uA, uB, uC)
	cfg.Name = name
	s, err := e.schedules.Create(ctx, e.admin, groupID, cfg)
	if err != nil {
		t.Fatalf("create schedule: %v", err)
	}
	if _, err := e.pool.Exec(ctx, "UPDATE oncall_shift SET start_time = start_time - $2::interval, end_time = end_time - $2::interval WHERE schedule_id = $1",
		s.ID, shiftBack); err != nil {
		t.Fatalf("shift shifts back: %v", err)
	}
	return s
}

// deleteScheduleAgo 删除值班表，并把删除时间拨到 ago 之前。
func (e *env) deleteScheduleAgo(t *testing.T, id string, ago time.Duration) {
	t.Helper()
	ctx := context.Background()
	if err := e.schedules.Delete(ctx, e.admin, id); err != nil {
		t.Fatalf("delete schedule: %v", err)
	}
	if _, err := e.pool.Exec(ctx, "UPDATE oncall_schedule SET delete_time = now() - $2::interval WHERE id = $1", id, ago); err != nil {
		t.Fatalf("backdate delete_time: %v", err)
	}
}

// purgeShifts 直接经仓储清一批。
func (e *env) purgeShifts(t *testing.T, olderThan time.Duration, limit int) int {
	t.Helper()
	var n int
	err := e.store.WithinTx(context.Background(), func(tx biz.Tx) error {
		var err error
		if n, err = tx.PurgeDeletedScheduleShifts(context.Background(), olderThan, limit); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("purge shifts: %v", err)
	}
	return n
}

// 只删「所属值班表已删除、且删除时间早于保留期」的班次：删得早的值班表班次被清，最近才删的与仍在使用的原样保留；
// 单批不超过批大小；清完之后再清为 0；保留期 0 时用例返回 0。
func TestIntegration_PurgeDeletedScheduleShifts(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	const day = 24 * time.Hour
	g := e.group(t, uA, uB, uC)
	goneOld := e.newShiftedSchedule(t, g.ID, "早已删除", 10*day)
	goneRecent := e.newShiftedSchedule(t, g.ID, "最近删除", 10*day)
	live := e.newShiftedSchedule(t, g.ID, "在用", 10*day)
	e.deleteScheduleAgo(t, goneOld.ID, 40*day)
	e.deleteScheduleAgo(t, goneRecent.ID, 10*day)

	before := e.shiftCounts(t)
	// 拨回 10 天后删除：留下约 10 天 × 主备两个角色的历史班次。
	if before[goneOld.ID] < 10 || before[goneRecent.ID] != before[goneOld.ID] || before[live.ID] <= before[goneOld.ID] {
		t.Fatalf("unexpected shift counts before purge: %v", before)
	}

	const batch = 2
	total, statements := 0, 0
	for range 1000 {
		n := e.purgeShifts(t, 30*day, batch)
		if n > batch {
			t.Fatalf("a single statement deleted %d rows, more than the batch size %d", n, batch)
		}
		total += n
		statements++
		if n == 0 {
			break
		}
	}
	if total != before[goneOld.ID] || statements != before[goneOld.ID]/batch+1 {
		t.Fatalf("deleted %d rows in %d statements; want %d rows drained in %d statements", total, statements, before[goneOld.ID], before[goneOld.ID]/batch+1)
	}
	after := e.shiftCounts(t)
	if _, ok := after[goneOld.ID]; ok || after[goneRecent.ID] != before[goneRecent.ID] || after[live.ID] != before[live.ID] {
		t.Fatalf("shift counts after purge = %v; want only the long-deleted schedule's shifts gone (before: %v)", after, before)
	}

	// 第二次清理为 0；保留期 0（不清理）同样为 0，最近才删的那张表的班次原样保留。
	if n, err := e.worker.PurgeDeletedScheduleShifts(ctx, 30*day); err != nil || n != 0 {
		t.Fatalf("second purge = %d, %v; want 0", n, err)
	}
	if n, err := e.worker.PurgeDeletedScheduleShifts(ctx, 0); err != nil || n != 0 {
		t.Fatalf("purge with retention 0 = %d, %v; want 0", n, err)
	}

	// 在用的值班表不受影响：班次照常可读、数量不变。
	shifts, err := e.shiftRepo.ListRange(ctx, time.Now().Add(-40*day), time.Now().Add(40*day))
	if err != nil {
		t.Fatalf("list shifts: %v", err)
	}
	if len(shifts) != before[live.ID] {
		t.Fatalf("live schedule has %d readable shifts, want %d", len(shifts), before[live.ID])
	}
}

// 取行必须走迁移 0090 的 idx_oncall_shift_schedule（按值班表取班次）：关掉顺序扫描后执行计划里应当出现这个索引。
func TestIntegration_PurgeDeletedScheduleShifts_UsesScheduleIndex(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	g := e.group(t, uA, uB, uC)
	gone := e.newShiftedSchedule(t, g.ID, "早已删除", 10*24*time.Hour)
	e.deleteScheduleAgo(t, gone.ID, 40*24*time.Hour)
	live := e.newShiftedSchedule(t, g.ID, "在用", 10*24*time.Hour)
	// ANALYZE：统计信息缺失时规划器按缺省估算选计划，断言会不稳。
	if _, err := e.pool.Exec(ctx, "ANALYZE oncall_schedule, oncall_shift"); err != nil {
		t.Fatal(err)
	}

	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
		t.Fatal(err)
	}
	rows, err := tx.Query(ctx, "EXPLAIN "+purgeDeletedScheduleShiftsSQL, 30*24*time.Hour, 500)
	if err != nil {
		t.Fatalf("explain purge: %v", err)
	}
	plan, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(plan, "\n")
	t.Logf("plan:\n%s", joined)
	if !strings.Contains(joined, "idx_oncall_shift_schedule") {
		t.Fatalf("purge plan does not use idx_oncall_shift_schedule:\n%s", joined)
	}
	if got := e.shiftCounts(t); got[live.ID] == 0 || got[gone.ID] == 0 {
		t.Fatalf("EXPLAIN must not delete anything, shift counts = %v", got)
	}
}
