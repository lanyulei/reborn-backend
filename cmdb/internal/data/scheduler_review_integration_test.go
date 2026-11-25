//go:build integration

package data

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/cmdbcontract"
)

// 陈旧标记：停用中的采集源不参与；启用的、以及已删除的采集源（无论删除前是否启用）照旧。
func TestIntegration_MarkStaleSkipsDisabledSources(t *testing.T) {
	d := newIntegrationData(t)
	ctx := context.Background()
	sched := NewSchedulerRepo(d)
	next := time.Now().Add(time.Hour)
	enabled, disabled, deleted := insertSource(t, d, 0, true, next), insertSource(t, d, 0, false, next), insertSource(t, d, 0, false, next)
	if _, err := d.Pool.Exec(ctx, "UPDATE cmdb_sync_source SET delete_time = now() WHERE id = $1", deleted); err != nil {
		t.Fatal(err)
	}
	for name, sourceID := range map[string]string{"of-enabled": enabled, "of-disabled": disabled, "of-deleted": deleted} {
		// interval 600s × 3 = 30 分钟；一天前同步过的都已超期。
		if _, err := d.Pool.Exec(ctx,
			"INSERT INTO cmdb_ci (id, model_code, name, source, external_id, last_synced_at) VALUES ($1, 'cloud_host', $2, $3, $2, now() - interval '1 day')",
			mustID(t), name, cmdbcontract.SourceOf(sourceID)); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := sched.MarkStale(ctx, 3); err != nil || n != 2 {
		t.Fatalf("MarkStale = %d err=%v, want 2", n, err)
	}
	rows, err := d.Pool.Query(ctx, "SELECT name FROM cmdb_ci WHERE stale ORDER BY name")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var stale []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		stale = append(stale, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(stale, []string{"of-deleted", "of-enabled"}) {
		t.Fatalf("stale resources = %v", stale)
	}
	// 重新启用后，下一轮照常标记。
	if _, err := d.Pool.Exec(ctx, "UPDATE cmdb_sync_source SET enabled = true WHERE id = $1", disabled); err != nil {
		t.Fatal(err)
	}
	if n, err := sched.MarkStale(ctx, 3); err != nil || n != 1 {
		t.Fatalf("MarkStale after re-enable = %d err=%v, want 1", n, err)
	}
}

// 遗留暂存清理以任务租约判定在途：运行开始时间早于保留期、但任务租约未过期的暂存行不得清理；
// 租约过期后才清理。
func TestIntegration_PurgeStagingKeepsLeasedRuns(t *testing.T) {
	d := newIntegrationData(t)
	ctx := context.Background()
	sched := NewSchedulerRepo(d)
	sourceID := insertSource(t, d, 0, true, time.Now().Add(time.Hour))
	taskID, runID := mustID(t), mustID(t)
	if _, err := d.Pool.Exec(ctx, `INSERT INTO cmdb_sync_task (id, source_id, trigger, status, attempts, claimed_by, lease_until, run_id)
		VALUES ($1, $2, 'manual', 'running', 1, 'worker-a', now() + interval '5 minutes', $3)`, taskID, sourceID, runID); err != nil {
		t.Fatal(err)
	}
	// 运行已经开始 2 小时（长采集），保留期只有 1 小时。
	if _, err := d.Pool.Exec(ctx, "INSERT INTO cmdb_sync_run (id, source_id, task_id, status, start_time) VALUES ($1, $2, $3, 'running', now() - interval '2 hours')",
		runID, sourceID, taskID); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Pool.Exec(ctx, "INSERT INTO cmdb_sync_staging (run_id, model_code, external_id, name) VALUES ($1, 'cloud_host', 'i-1', 'in-flight')", runID); err != nil {
		t.Fatal(err)
	}
	if n, err := sched.PurgeStaging(ctx, time.Hour); err != nil || n != 0 {
		t.Fatalf("in-flight staging must be kept while the lease is live: purged=%d err=%v", n, err)
	}
	// 租约过期（进程已死）：按原条件清理。
	if _, err := d.Pool.Exec(ctx, "UPDATE cmdb_sync_task SET lease_until = now() - interval '1 second' WHERE id = $1", taskID); err != nil {
		t.Fatal(err)
	}
	if n, err := sched.PurgeStaging(ctx, time.Hour); err != nil || n != 1 {
		t.Fatalf("staging of an expired lease must be purged: purged=%d err=%v", n, err)
	}
}
