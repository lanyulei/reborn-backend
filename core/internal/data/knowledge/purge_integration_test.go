//go:build integration

package data

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// seedIngestJob 直接落一行导入任务：status 与 update_time 可控（空间 / 文件不设外键，用固定 UUID）。
func seedIngestJob(t *testing.T, r *itRepos, status int16, ago time.Duration) string {
	t.Helper()
	id := mustID(t)
	if _, err := r.data.Pool.Exec(context.Background(),
		`INSERT INTO knowledge_ingest_job (id, space_id, file_id, filename, status, create_user, create_time, update_time)
		 VALUES ($1, $2, $2, 'note.txt', $3, $2, now() - $4::interval, now() - $4::interval)`, id, itOwner, status, ago); err != nil {
		t.Fatalf("seed ingest job: %v", err)
	}
	return id
}

func remainingJobs(t *testing.T, r *itRepos, ids ...string) map[string]bool {
	t.Helper()
	rows, err := r.data.Pool.Query(context.Background(), `SELECT id::text FROM knowledge_ingest_job WHERE id = ANY($1::uuid[])`, ids)
	if err != nil {
		t.Fatalf("query ingest jobs: %v", err)
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

// 保留期清理只删「已结束且早于保留期」的任务：旧的终态行被删，近期行与任何非终态行（排队 / 处理中，哪怕早已
// 过了保留期——它们还等着被领取）保留；时间以数据库时钟为准；第二次清理为 0。
func TestIngestRepo_PurgeFinished(t *testing.T) {
	r := newIT(t)
	ctx := context.Background()
	const day = 24 * time.Hour
	oldSucceeded := seedIngestJob(t, r, 3, 40*day)
	oldFailed := seedIngestJob(t, r, 4, 40*day)
	edgeExpired := seedIngestJob(t, r, 3, 30*day+time.Hour) // 刚过保留期
	recentSucceeded := seedIngestJob(t, r, 3, 30*day-time.Hour)
	recentFailed := seedIngestJob(t, r, 4, day)
	keep := []string{recentSucceeded, recentFailed}
	for _, status := range []int16{1, 2} {
		keep = append(keep, seedIngestJob(t, r, status, 40*day))
	}

	n, err := r.ingest.PurgeFinished(ctx, 30*day, 1000)
	if err != nil || n != 3 {
		t.Fatalf("PurgeFinished() = %d, %v; want 3", n, err)
	}
	got := remainingJobs(t, r, append([]string{oldSucceeded, oldFailed, edgeExpired}, keep...)...)
	if len(got) != len(keep) {
		t.Fatalf("remaining jobs = %v; want only the recent / non-terminal ones %v", got, keep)
	}
	for _, id := range keep {
		if !got[id] {
			t.Errorf("job %s must be kept", id)
		}
	}
	if n, err := r.ingest.PurgeFinished(ctx, 30*day, 1000); err != nil || n != 0 {
		t.Fatalf("second purge = %d, %v; want 0", n, err)
	}
}

// 单条语句至多删一批：积压分成多条删完，每条不超过批大小，删完后返回 0。
func TestIngestRepo_PurgeFinished_Batched(t *testing.T) {
	r := newIT(t)
	ctx := context.Background()
	ids := make([]string, 7)
	for i := range ids {
		ids[i] = seedIngestJob(t, r, 3, 40*24*time.Hour)
	}
	var total int
	for i, want := range []int{3, 3, 1, 0} {
		n, err := r.ingest.PurgeFinished(ctx, 30*24*time.Hour, 3)
		if err != nil || n != want {
			t.Fatalf("batch %d: PurgeFinished() = %d, %v; want %d (never more than the batch size)", i, n, err, want)
		}
		total += n
	}
	if total != len(ids) || len(remainingJobs(t, r, ids...)) != 0 {
		t.Fatalf("deleted %d of %d; backlog must be drained across batches", total, len(ids))
	}
}

// 取行条件必须能推出迁移 0090 的部分索引谓词，否则每批都是全表扫描：关掉顺序扫描后执行计划里应当出现
// idx_knowledge_ingest_job_finished（谓词写法一旦偏离索引谓词，这个索引就用不上了）。
func TestIngestRepo_PurgeFinished_UsesPartialIndex(t *testing.T) {
	r := newIT(t)
	ctx := context.Background()
	// 先放一批有代表性的数据并 ANALYZE：空表上规划器只能按缺省估算选计划，断言会不稳。
	for range 300 {
		seedIngestJob(t, r, 3, 40*24*time.Hour)
	}
	for range 300 {
		seedIngestJob(t, r, 1, time.Hour)
	}
	if _, err := r.data.Pool.Exec(ctx, "ANALYZE knowledge_ingest_job"); err != nil {
		t.Fatal(err)
	}
	tx, err := r.data.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
		t.Fatal(err)
	}
	rows, err := tx.Query(ctx, "EXPLAIN "+purgeFinishedIngestSQL, 30*24*time.Hour, 500)
	if err != nil {
		t.Fatalf("explain purge: %v", err)
	}
	plan, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(plan, "\n")
	t.Logf("plan:\n%s", joined)
	if !strings.Contains(joined, "idx_knowledge_ingest_job_finished") {
		t.Fatalf("purge plan does not use idx_knowledge_ingest_job_finished:\n%s", joined)
	}
}

// deleteWhileRequeueHeld 复现「清理与重试并发」的交错并返回 deleteSQL 删除的行数：
//  1. 事务 A 锁住这条过期的失败任务；
//  2. 另一个连接执行 deleteSQL：子查询的快照里它仍是「失败且已过期」，语句停在 A 持有的行锁上；
//  3. A 按 Requeue 的同一条 UPDATE 把它置回排队并提交；
//  4. deleteSQL 拿到行锁，此刻才决定这一行删还是不删。
func deleteWhileRequeueHeld(t *testing.T, r *itRepos, job, deleteSQL string) int64 {
	t.Helper()
	ctx := context.Background()
	lockTx, err := r.data.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lockTx.Rollback(ctx) }()
	var locked string
	if err := lockTx.QueryRow(ctx, "SELECT id::text FROM knowledge_ingest_job WHERE id = $1 FOR UPDATE", job).Scan(&locked); err != nil {
		t.Fatal(err)
	}
	type result struct {
		n   int64
		err error
	}
	done := make(chan result, 1)
	go func() {
		tag, err := r.data.Pool.Exec(ctx, deleteSQL, 30*24*time.Hour, 500)
		done <- result{tag.RowsAffected(), err}
	}()
	// 等 DELETE 真的阻塞在行锁上再提交，保证它是在「先选中、后被改写」的路径上。
	deadline := time.Now().Add(10 * time.Second)
	for {
		var waiting int
		if err := r.data.Pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
			WHERE wait_event_type = 'Lock' AND query LIKE 'DELETE FROM knowledge_ingest_job%'`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("delete statement never blocked on the row lock")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := lockTx.Exec(ctx, `UPDATE knowledge_ingest_job SET status = 1, attempts = 0, error = '', lease_until = NULL, update_time = now()
		WHERE id = $1 AND status = 4`, job); err != nil {
		t.Fatal(err)
	}
	if err := lockTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("delete: %v", got.err)
		}
		return got.n
	case <-time.After(10 * time.Second):
		t.Fatal("delete statement did not finish")
		return 0
	}
}

// 清理与「重试一条失败任务」并发时，用户刚重试的任务必须留下：DELETE 等到行锁后按改写后的行重检外层的终态 /
// 时间条件，不再满足就跳过。
func TestIngestRepo_PurgeFinished_KeepsConcurrentlyRequeuedJob(t *testing.T) {
	r := newIT(t)
	job := seedIngestJob(t, r, 4, 40*24*time.Hour)
	if n := deleteWhileRequeueHeld(t, r, job, purgeFinishedIngestSQL); n != 0 {
		t.Fatalf("purge deleted %d rows; want 0 (the requeued job must survive)", n)
	}
	var status int16
	if err := r.data.Pool.QueryRow(context.Background(), "SELECT status FROM knowledge_ingest_job WHERE id = $1", job).Scan(&status); err != nil {
		t.Fatalf("requeued job was deleted by the concurrent purge: %v", err)
	}
	if status != 1 {
		t.Fatalf("requeued job status = %d, want 1 (queued)", status)
	}
}

// 对照组：同一段交错下，只按子查询选中的主键删（外层不带条件）会把刚重试的任务删掉——上一条测试防的正是这个，
// 它也证明上面的夹具确实走到了「先选中、后被改写」这条路径。
func TestIngestRepo_DeleteBySelectionAloneLosesConcurrentlyRequeuedJob(t *testing.T) {
	r := newIT(t)
	job := seedIngestJob(t, r, 4, 40*24*time.Hour)
	const unguarded = `DELETE FROM knowledge_ingest_job WHERE id IN (
	   SELECT id FROM knowledge_ingest_job WHERE status IN (3, 4) AND update_time < now() - $1::interval ORDER BY update_time LIMIT $2)`
	if n := deleteWhileRequeueHeld(t, r, job, unguarded); n != 1 {
		t.Fatalf("unguarded delete removed %d rows, want 1: the interleaving no longer reproduces the race", n)
	}
	if got := remainingJobs(t, r, job); len(got) != 0 {
		t.Fatalf("unguarded delete kept the requeued job: %v", got)
	}
}
