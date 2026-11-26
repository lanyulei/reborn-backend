//go:build integration

package data

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/cmdb/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/cmdbcontract"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// syncPair 一个同步任务与它的一次运行记录。
type syncPair struct {
	task, run string
}

// seedSyncTask 直接落一个同步任务，创建 / 开始时间都在 ago 之前；finished 为真时 finish_time 也在 ago 之前。
func seedSyncTask(t *testing.T, d *Data, sourceID, status string, finished bool, ago time.Duration, runID *string) string {
	t.Helper()
	id := mustID(t)
	if _, err := d.Pool.Exec(context.Background(),
		`INSERT INTO cmdb_sync_task (id, source_id, trigger, status, create_time, start_time, finish_time, run_id)
		 VALUES ($1, $2, 'schedule', $3, now() - $4::interval, now() - $4::interval, CASE WHEN $5 THEN now() - $4::interval END, $6)`,
		id, sourceID, status, ago, finished, runID); err != nil {
		t.Fatalf("seed sync task: %v", err)
	}
	return id
}

// seedSyncRun 直接落一条主键为 id 的运行记录；finished 为真时 finish_time 在 ago 之前，否则为 NULL（仍在进行）。
func seedSyncRun(t *testing.T, d *Data, id, sourceID, taskID, status string, finished bool, ago time.Duration) {
	t.Helper()
	if _, err := d.Pool.Exec(context.Background(),
		`INSERT INTO cmdb_sync_run (id, source_id, task_id, status, start_time, finish_time)
		 VALUES ($1, $2, $3, $4, now() - $5::interval - interval '1 minute', CASE WHEN $6 THEN now() - $5::interval END)`,
		id, sourceID, taskID, status, ago, finished); err != nil {
		t.Fatalf("seed sync run: %v", err)
	}
}

// seedSyncPair 落一个已结束的任务与它的一次运行记录（都结束于 ago 之前，任务指向这次运行）。
func seedSyncPair(t *testing.T, d *Data, sourceID, status string, ago time.Duration) syncPair {
	t.Helper()
	runID := mustID(t)
	taskID := seedSyncTask(t, d, sourceID, status, true, ago, &runID)
	seedSyncRun(t, d, runID, sourceID, taskID, status, true, ago)
	return syncPair{task: taskID, run: runID}
}

// remaining 返回 ids 里仍存在于 table 的那部分（table 只取本文件用到的两张同步表）。
func remaining(t *testing.T, d *Data, table string, ids ...string) map[string]bool {
	t.Helper()
	if table != "cmdb_sync_task" && table != "cmdb_sync_run" {
		t.Fatalf("unexpected table %q", table)
	}
	rows, err := d.Pool.Query(context.Background(), "SELECT id::text FROM "+table+" WHERE id = ANY($1::uuid[])", ids)
	if err != nil {
		t.Fatalf("query %s: %v", table, err)
	}
	got := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		got[id] = true
	}
	rows.Close()
	return got
}

func idsOf(pairs []syncPair, pick func(syncPair) string) []string {
	out := make([]string, len(pairs))
	for i, p := range pairs {
		out[i] = pick(p)
	}
	return out
}

const retentionDay = 24 * time.Hour

// 保留期清理：旧的终态任务与运行记录被删（含同一任务多次尝试留下的多条运行记录）；近期行与非终态行保留；
// 每个采集源 last_run_id 指向的最近一次运行保留，它的任务因运行记录还在也保留；任务与运行记录成对一致
// （不留「运行记录还在、任务没了」）；第二次清理为 0。
func TestIntegration_PurgeSyncRecords(t *testing.T) {
	d := newIntegrationData(t)
	ctx := context.Background()
	maint := NewMaintenanceRepo(d)
	srcA, srcB := insertSource(t, d, 0, true, time.Now().Add(time.Hour)), insertSource(t, d, 0, true, time.Now().Add(time.Hour))

	// srcA：早已结束的一批。
	gone := seedSyncPair(t, d, srcA, cmdbcontract.TaskSucceeded, 200*retentionDay)
	retried := seedSyncPair(t, d, srcA, cmdbcontract.TaskFailed, 200*retentionDay)
	// 同一任务上一次尝试的运行记录（重领时更早被置失败）：也要删。
	earlierAttempt := mustID(t)
	seedSyncRun(t, d, earlierAttempt, srcA, retried.task, cmdbcontract.RunFailed, true, 201*retentionDay)
	// srcA 最近一次运行：很旧（采集源之后没再同步过），但被 last_run_id 引用，保留；它的任务随之保留。
	lastOfA := seedSyncPair(t, d, srcA, cmdbcontract.TaskSucceeded, 190*retentionDay)
	// 近期行。
	recent := seedSyncPair(t, d, srcA, cmdbcontract.TaskSucceeded, 10*retentionDay)
	// 非终态行：早已过了保留期，但仍在 running / pending（等待被重领或领取），不删。
	runID := mustID(t)
	runningTask := seedSyncTask(t, d, srcA, cmdbcontract.TaskRunning, false, 200*retentionDay, &runID)
	seedSyncRun(t, d, runID, srcA, runningTask, cmdbcontract.RunRunning, false, 200*retentionDay)
	// 每个采集源同时只能有一个未完成的任务（uq_cmdb_sync_task_active）：排队的任务放在另一个采集源下。
	pendingTask := seedSyncTask(t, d, srcB, cmdbcontract.TaskPending, false, 200*retentionDay, nil)

	// srcB：没有被引用的旧记录，整批可删；外加一个被 last_run_id 引用的旧运行。
	goneB := seedSyncPair(t, d, srcB, cmdbcontract.TaskFailed, 200*retentionDay)
	lastOfB := seedSyncPair(t, d, srcB, cmdbcontract.TaskSucceeded, 195*retentionDay)
	for src, last := range map[string]syncPair{srcA: lastOfA, srcB: lastOfB} {
		if _, err := d.Pool.Exec(ctx, "UPDATE cmdb_sync_source SET last_run_id = $2, last_status = $3 WHERE id = $1", src, last.run, cmdbcontract.RunSucceeded); err != nil {
			t.Fatal(err)
		}
	}

	if n, err := maint.PurgeRuns(ctx, 180*retentionDay, 1000); err != nil || n != 4 {
		t.Fatalf("PurgeRuns() = %d, %v; want 4 (two old runs of srcA, the earlier attempt, one of srcB)", n, err)
	}
	if got := remaining(t, d, "cmdb_sync_run", gone.run, retried.run, earlierAttempt, lastOfA.run, recent.run, runID, goneB.run, lastOfB.run); len(got) != 4 {
		t.Fatalf("remaining runs = %v; want only the two last-run rows, the recent one and the running one", got)
	}
	// 运行记录删掉之后任务才可删；运行记录仍被引用的任务不动。
	if n, err := maint.PurgeTasks(ctx, 180*retentionDay, 1000); err != nil || n != 3 {
		t.Fatalf("PurgeTasks() = %d, %v; want 3", n, err)
	}
	keep := []syncPair{lastOfA, recent, lastOfB}
	want := map[string]bool{runningTask: true, pendingTask: true}
	for _, p := range keep {
		want[p.task] = true
	}
	all := append(idsOf([]syncPair{gone, retried, lastOfA, recent, goneB, lastOfB}, func(p syncPair) string { return p.task }), runningTask, pendingTask)
	got := remaining(t, d, "cmdb_sync_task", all...)
	if len(got) != len(want) {
		t.Fatalf("remaining tasks = %v; want %v", got, want)
	}
	for id := range want {
		if !got[id] {
			t.Errorf("task %s must be kept", id)
		}
	}
	// 成对一致：没有「运行记录还在、任务没了」。
	var orphans int
	if err := d.Pool.QueryRow(ctx, `SELECT count(*) FROM cmdb_sync_run r WHERE NOT EXISTS (SELECT 1 FROM cmdb_sync_task t WHERE t.id = r.task_id)`).Scan(&orphans); err != nil {
		t.Fatal(err)
	}
	if orphans != 0 {
		t.Fatalf("%d runs left without their task", orphans)
	}
	// 第二次清理为 0。
	if n, err := maint.PurgeRuns(ctx, 180*retentionDay, 1000); err != nil || n != 0 {
		t.Fatalf("second PurgeRuns() = %d, %v; want 0", n, err)
	}
	if n, err := maint.PurgeTasks(ctx, 180*retentionDay, 1000); err != nil || n != 0 {
		t.Fatalf("second PurgeTasks() = %d, %v; want 0", n, err)
	}
}

// 单条语句至多删一批：运行记录的积压分成多条删完之后，任务的积压再分成多条删完，每条不超过批大小，删完后返回 0；
// 运行记录还在时任务一条都删不掉。
func TestIntegration_PurgeSyncRecords_Batched(t *testing.T) {
	d := newIntegrationData(t)
	ctx := context.Background()
	maint := NewMaintenanceRepo(d)
	src := insertSource(t, d, 0, true, time.Now().Add(time.Hour))
	pairs := make([]syncPair, 7)
	for i := range pairs {
		pairs[i] = seedSyncPair(t, d, src, cmdbcontract.TaskSucceeded, 200*retentionDay)
	}
	if n, err := maint.PurgeTasks(ctx, 180*retentionDay, 3); err != nil || n != 0 {
		t.Fatalf("tasks purged while their runs still exist = %d, %v; want 0", n, err)
	}
	for _, tc := range []struct {
		table string
		purge func(context.Context, time.Duration, int) (int, error)
		ids   []string
	}{
		{"cmdb_sync_run", maint.PurgeRuns, idsOf(pairs, func(p syncPair) string { return p.run })},
		{"cmdb_sync_task", maint.PurgeTasks, idsOf(pairs, func(p syncPair) string { return p.task })},
	} {
		total := 0
		for i, want := range []int{3, 3, 1, 0} {
			n, err := tc.purge(ctx, 180*retentionDay, 3)
			if err != nil || n != want {
				t.Fatalf("%s batch %d: = %d, %v; want %d (never more than the batch size)", tc.table, i, n, err, want)
			}
			total += n
		}
		if total != len(pairs) || len(remaining(t, d, tc.table, tc.ids...)) != 0 {
			t.Fatalf("%s: deleted %d of %d; backlog must be drained across batches", tc.table, total, len(pairs))
		}
	}
}

// 维护用例一轮把保留期外的任务与运行记录都清掉（默认保留期 180 天），近期的不动。
func TestIntegration_MaintenanceRunOncePurge(t *testing.T) {
	d := newIntegrationData(t)
	ctx := context.Background()
	src := insertSource(t, d, 0, true, time.Now().Add(time.Hour))
	gone := seedSyncPair(t, d, src, cmdbcontract.TaskSucceeded, 200*retentionDay)
	recent := seedSyncPair(t, d, src, cmdbcontract.TaskSucceeded, 10*retentionDay)
	if err := biz.NewMaintenanceUsecase(NewMaintenanceRepo(d), NewMaintenanceRepo(d), slog.New(slog.NewTextHandler(io.Discard, nil))).RunOnce(ctx, time.Now().UTC()); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if len(remaining(t, d, "cmdb_sync_task", gone.task)) != 0 || len(remaining(t, d, "cmdb_sync_run", gone.run)) != 0 {
		t.Fatal("the old task / run survived RunOnce")
	}
	if len(remaining(t, d, "cmdb_sync_task", recent.task)) != 1 || len(remaining(t, d, "cmdb_sync_run", recent.run)) != 1 {
		t.Fatal("the recent task / run must be kept")
	}
}

// 取行条件必须能推出迁移 0090 的部分索引谓词：关掉顺序扫描后两条语句的执行计划里应当分别出现
// idx_cmdb_sync_run_finished / idx_cmdb_sync_task_finished。
func TestIntegration_PurgeSyncRecords_UsePartialIndexes(t *testing.T) {
	d := newIntegrationData(t)
	ctx := context.Background()
	tx, err := d.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ what, sql, index string }{
		{"runs", purgeSyncRunsSQL, "idx_cmdb_sync_run_finished"},
		{"tasks", purgeSyncTasksSQL, "idx_cmdb_sync_task_finished"},
	} {
		rows, err := tx.Query(ctx, "EXPLAIN "+tc.sql, 180*retentionDay, 500)
		if err != nil {
			t.Fatalf("explain purge %s: %v", tc.what, err)
		}
		plan, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			t.Fatal(err)
		}
		if joined := strings.Join(plan, "\n"); !strings.Contains(joined, tc.index) {
			t.Fatalf("purge %s plan does not use %s:\n%s", tc.what, tc.index, joined)
		}
	}
}

// 数据面账号是 cmdb_sync_role 的成员，两张表的 DELETE 授权来自迁移。这里以该角色实际执行清理语句，授权在不在都如实
// 断言：在则语句可执行；不在则数据库报 42501，仓储把它换成 biz.ErrPurgeDenied，维护用例据此只告警、不把这一轮
// 当作失败（见 TestMaintenance_RunOncePurge）。
func TestIntegration_PurgeSyncRecords_AsSyncRole(t *testing.T) {
	d := newIntegrationData(t)
	ctx := context.Background()
	var superuser, member bool
	if err := d.Pool.QueryRow(ctx, `SELECT rolsuper, pg_has_role(current_user, 'cmdb_sync_role', 'MEMBER') FROM pg_roles WHERE rolname = current_user`).Scan(&superuser, &member); err != nil {
		t.Fatalf("read current role: %v", err)
	}
	if !superuser && !member {
		t.Skip("the test connection cannot assume cmdb_sync_role; the privilege path is not exercised")
	}
	var canDelete bool
	if err := d.Pool.QueryRow(ctx, `SELECT has_table_privilege('cmdb_sync_role', 'cmdb_sync_run', 'DELETE') AND has_table_privilege('cmdb_sync_role', 'cmdb_sync_task', 'DELETE')`).Scan(&canDelete); err != nil {
		t.Fatal(err)
	}
	t.Logf("cmdb_sync_role has DELETE on cmdb_sync_task / cmdb_sync_run: %v", canDelete)
	// 以该角色执行一条语句：各自一个事务（语句出错后事务即中止，放在同一事务里第二条报的会是 25P02），结果一律回滚。
	execAsSyncRole := func(sql string) error {
		tx, err := d.Pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := tx.Exec(ctx, "SET LOCAL ROLE cmdb_sync_role"); err != nil {
			t.Fatalf("assume cmdb_sync_role: %v", err)
		}
		_, err = tx.Exec(ctx, sql, 180*retentionDay, 500)
		return err
	}
	runsErr, tasksErr := execAsSyncRole(purgeSyncRunsSQL), execAsSyncRole(purgeSyncTasksSQL)
	if canDelete {
		if runsErr != nil || tasksErr != nil {
			t.Fatalf("cmdb_sync_role may delete but the statements failed: runs = %v tasks = %v", runsErr, tasksErr)
		}
		return
	}
	for _, err := range []error{runsErr, tasksErr} {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != insufficientPrivilege {
			t.Fatalf("without the DELETE grant the statement must fail with %s, got %v", insufficientPrivilege, err)
		}
		if wrapped := wrapPurgeErr("purge", err); !errors.Is(wrapped, biz.ErrPurgeDenied) {
			t.Fatalf("a %s error must map to biz.ErrPurgeDenied, got %v", insufficientPrivilege, wrapped)
		}
	}
}
