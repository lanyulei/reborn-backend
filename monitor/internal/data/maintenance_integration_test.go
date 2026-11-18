//go:build integration

package data

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/monitor/internal/biz"

	"github.com/jackc/pgx/v5/pgconn"
)

// utcDay 数据库时钟下的当前 UTC 日期。
func (e *env) utcDay() time.Time {
	e.t.Helper()
	var now time.Time
	if err := e.admin.QueryRow(context.Background(), "SELECT now()").Scan(&now); err != nil {
		e.t.Fatal(err)
	}
	now = now.UTC()
	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
}

// resetPartitions 把判定记录的分区恢复成「只有当天至未来 2 天」（迁移后的样子），清掉此前测试留下的分区。
func (e *env) resetPartitions() {
	e.t.Helper()
	today := e.utcDay()
	keep := map[string]bool{evalRecordDefault: true}
	for i := 0; i <= 2; i++ {
		keep[partitionName(today.AddDate(0, 0, i))] = true
	}
	rows, err := e.admin.Query(context.Background(),
		"SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE 'monitor_eval_record_%'")
	if err != nil {
		e.t.Fatal(err)
	}
	var drop []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			e.t.Fatal(err)
		}
		if !keep[name] && partitionPattern.MatchString(name) {
			drop = append(drop, name)
		}
	}
	rows.Close()
	for _, name := range drop {
		e.exec("DROP TABLE " + name)
	}
}

// attached 报告某天的分区是否已挂在父表上。
func (e *env) attached(day time.Time) bool {
	e.t.Helper()
	return e.count("SELECT count(*) FROM pg_inherits WHERE inhparent = 'monitor_eval_record'::regclass AND inhrelid = to_regclass($1)",
		partitionName(day)) == 1
}

// record 以引擎身份写一条判定记录，返回它实际落在哪张表。
func (e *env) record(at time.Time) string {
	e.t.Helper()
	var table string
	if err := e.d.Pool.QueryRow(context.Background(),
		`INSERT INTO monitor_eval_record (id, rule_id, datasource_id, eval_time, status) VALUES ($1, $2, $3, $4, 1)
		 RETURNING tableoid::regclass::text`, e.id(), e.id(), e.id(), at).Scan(&table); err != nil {
		e.t.Fatalf("insert eval record: %v", err)
	}
	return table
}

func TestPartitionMaintenance(t *testing.T) {
	e := newEnv(t)
	e.resetPartitions()
	t.Cleanup(e.resetPartitions)
	repo := NewMaintenanceRepo(e.d)
	ctx := context.Background()
	today := e.utcDay()
	day := func(offset int) time.Time { return today.AddDate(0, 0, offset) }

	// 预建：当天至未来 N 天；已存在的不动；重复执行幂等。
	if e.attached(day(3)) || e.attached(day(4)) {
		t.Fatal("precondition: partitions beyond +2 days must not exist")
	}
	for range 2 {
		if err := repo.EnsurePartitions(ctx, 4); err != nil {
			t.Fatalf("EnsurePartitions() error = %v", err)
		}
	}
	for i := 0; i <= 4; i++ {
		if !e.attached(day(i)) {
			t.Fatalf("partition for day +%d missing", i)
		}
	}
	if e.attached(day(5)) {
		t.Fatal("must not create beyond the requested horizon")
	}
	if got := e.record(day(4).Add(12 * time.Hour)); got != partitionName(day(4)) {
		t.Fatalf("row landed in %s", got)
	}
	// 新分区的属主与父表一致（换登录账号后仍能维护），带区间 CHECK 与主键。
	if e.count(`SELECT count(*) FROM pg_class c JOIN pg_class p ON p.oid = 'monitor_eval_record'::regclass
		WHERE c.oid = to_regclass($1) AND c.relowner = p.relowner AND pg_get_userbyid(c.relowner) = $2`, partitionName(day(4)), engineRole) != 1 {
		t.Fatal("new partition must be owned by the engine role")
	}
	// 边界是 UTC 日界，含起点不含终点。
	if got := e.record(day(4)); got != partitionName(day(4)) {
		t.Fatalf("lower bound row landed in %s", got)
	}
	if got := e.record(day(5).Add(-time.Microsecond)); got != partitionName(day(4)) {
		t.Fatalf("last instant of the day landed in %s", got)
	}

	// 维护失效期间落进 DEFAULT 的行：建分区时迁出再挂接，行不丢。
	if got := e.record(day(6).Add(3 * time.Hour)); got != evalRecordDefault {
		t.Fatalf("row without a partition must land in DEFAULT, got %s", got)
	}
	e.record(day(6).Add(4 * time.Hour))
	e.record(day(9)) // 不在本次预建范围内：留在 DEFAULT
	if err := repo.EnsurePartitions(ctx, 6); err != nil {
		t.Fatalf("EnsurePartitions() with rows in DEFAULT: %v", err)
	}
	if !e.attached(day(6)) || e.count("SELECT count(*) FROM "+partitionName(day(6))) != 2 {
		t.Fatalf("rows must be moved out of DEFAULT into the new partition")
	}
	if e.count("SELECT count(*) FROM "+evalRecordDefault) != 1 || e.count("SELECT count(*) FROM monitor_eval_record") != 6 {
		t.Fatalf("default = %d, total = %d", e.count("SELECT count(*) FROM "+evalRecordDefault), e.count("SELECT count(*) FROM monitor_eval_record"))
	}

	// 上一轮建表后、挂接前崩溃留下的独立表：下一轮接着完成。
	if _, err := e.d.Pool.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (LIKE monitor_eval_record INCLUDING DEFAULTS INCLUDING CONSTRAINTS)", partitionName(day(7)))); err != nil {
		t.Fatal(err)
	}
	if err := repo.EnsurePartitions(ctx, 7); err != nil || !e.attached(day(7)) {
		t.Fatalf("half-created partition must be completed: %v", err)
	}

	// 清理：上界 ≤ now − 保留期 的分区整个 DROP；仍在保留期内的不动；DEFAULT 不动。
	for _, offset := range []int{-10, -8, -7} {
		if err := repo.ensurePartition(ctx, day(offset)); err != nil {
			t.Fatalf("create old partition: %v", err)
		}
		e.record(day(offset).Add(time.Hour))
	}
	dropped, err := repo.DropExpiredPartitions(ctx, 7*24*time.Hour)
	if err != nil || dropped != 2 {
		t.Fatalf("dropped = %d, err = %v", dropped, err)
	}
	if e.attached(day(-10)) || e.attached(day(-8)) || !e.attached(day(-7)) || !e.attached(today) {
		t.Fatal("wrong partitions dropped")
	}
	if e.count("SELECT count(*) FROM pg_tables WHERE tablename IN ($1, $2)", partitionName(day(-10)), partitionName(day(-8))) != 0 {
		t.Fatal("expired partitions must be dropped, not just detached")
	}
	if again, err := repo.DropExpiredPartitions(ctx, 7*24*time.Hour); err != nil || again != 0 {
		t.Fatalf("second drop = %d, %v", again, err)
	}

	// DEFAULT 中过了保留期的行分批删除，未过期的保留。
	e.record(day(-30))
	e.record(day(-31))
	if n, err := repo.PurgeDefaultPartition(ctx, 7*24*time.Hour, 1); err != nil || n != 1 {
		t.Fatalf("purge batch = %d, %v", n, err)
	}
	if n, err := repo.PurgeDefaultPartition(ctx, 7*24*time.Hour, 10); err != nil || n != 1 {
		t.Fatalf("purge rest = %d, %v", n, err)
	}
	if e.count("SELECT count(*) FROM "+evalRecordDefault) != 1 {
		t.Fatal("unexpired rows in DEFAULT must stay")
	}
}

func TestRetention(t *testing.T) {
	e := newEnv(t)
	repo := NewMaintenanceRepo(e.d)
	ctx := context.Background()
	ds, ruleID := e.datasource("http://127.0.0.1:1"), e.rule(ruleSpec{})
	const day = 24 * time.Hour
	oldResolved := e.seedAlert(ruleID, ds, "fp-old-resolved", monitorcontract.AlertResolved, 200*day)
	oldClosed := e.seedAlert(ruleID, ds, "fp-old-closed", monitorcontract.AlertClosed, 181*day)
	recent := e.seedAlert(ruleID, ds, "fp-recent", monitorcontract.AlertResolved, 10*day)
	active := e.seedAlert(ruleID, ds, "fp-active", monitorcontract.AlertFiring, 0) // 400 天前触发、至今未恢复
	for _, id := range []string{oldResolved, oldResolved, oldClosed, recent, active} {
		e.exec("INSERT INTO monitor_notify_log (id, alert_id, kind, seq, source) VALUES ($1, $2, 1, 1, 1)", e.id(), id)
	}
	outbox := func(alertID string, kind, status int32, doneAgo time.Duration) {
		e.exec(`INSERT INTO monitor_notify_outbox (id, alert_id, kind, seq, status, create_time, done_time)
			VALUES ($1, $2, $3, 1, $4::smallint, now() - interval '300 days', CASE WHEN $4::smallint = 1 THEN NULL ELSE now() - $5::interval END)`,
			e.id(), alertID, kind, status, doneAgo)
	}
	outbox(oldResolved, 1, monitorcontract.OutboxDone, 200*day)
	outbox(oldResolved, 3, monitorcontract.OutboxPending, 0) // 告警被清理时连带删除
	outbox(recent, 1, monitorcontract.OutboxDone, 200*day)
	outbox(recent, 2, monitorcontract.OutboxDropped, 190*day)
	outbox(recent, 3, monitorcontract.OutboxDone, 10*day)
	outbox(active, 1, monitorcontract.OutboxPending, 0) // 待派发的行不论多旧都不删
	outbox(active, 2, monitorcontract.OutboxDone, 170*day)

	// 分批：每次至多 limit 个告警。
	for i, want := range []int{1, 1, 0} {
		n, err := repo.PurgeAlerts(ctx, 180*day, 1)
		if err != nil || n != want {
			t.Fatalf("PurgeAlerts batch %d = %d, %v; want %d", i, n, err, want)
		}
	}
	left := func(id string) int { return e.count("SELECT count(*) FROM monitor_alert WHERE id = $1", id) }
	if left(oldResolved) != 0 || left(oldClosed) != 0 || left(recent) != 1 || left(active) != 1 {
		t.Fatal("wrong alerts purged")
	}
	if e.count("SELECT count(*) FROM monitor_notify_log WHERE alert_id IN ($1, $2)", oldResolved, oldClosed) != 0 ||
		e.count("SELECT count(*) FROM monitor_notify_log") != 2 {
		t.Fatal("notify logs of purged alerts must go with them, others must stay")
	}
	if e.count("SELECT count(*) FROM monitor_notify_outbox WHERE alert_id = $1", oldResolved) != 0 {
		t.Fatal("outbox rows of purged alerts must go with them")
	}

	for i, want := range []int{1, 1, 0} {
		n, err := repo.PurgeOutbox(ctx, 180*day, 1)
		if err != nil || n != want {
			t.Fatalf("PurgeOutbox batch %d = %d, %v; want %d", i, n, err, want)
		}
	}
	if e.count("SELECT count(*) FROM monitor_notify_outbox WHERE alert_id = $1", recent) != 1 ||
		e.count("SELECT count(*) FROM monitor_notify_outbox WHERE alert_id = $1", active) != 2 {
		t.Fatal("wrong outbox rows purged")
	}
	// 活跃告警的判定状态不受清理影响。
	if e.count("SELECT count(*) FROM monitor_eval_state WHERE alert_id = $1", active) != 1 {
		t.Fatal("state of the active alert must stay")
	}
}

// 维护用例端到端（以受限登录角色执行）：预建分区、按保留期删除过期分区与 DEFAULT 中的过期行、
// 清理过期告警 / 通知记录 / 发件箱行；保留期内的数据与活跃告警不动。
func TestMaintenanceRunOnceCleansUpExpiredData(t *testing.T) {
	e := newEnv(t)
	e.resetPartitions()
	t.Cleanup(e.resetPartitions)
	repo := NewMaintenanceRepo(e.d)
	ctx := context.Background()
	today := e.utcDay()
	day := func(offset int) time.Time { return today.AddDate(0, 0, offset) }
	const d = 24 * time.Hour

	// 过期与未过期的判定记录分区；DEFAULT 里一条过期、一条未来的行。
	for _, offset := range []int{-12, -9, -3} {
		if err := repo.ensurePartition(ctx, day(offset)); err != nil {
			t.Fatal(err)
		}
		e.record(day(offset).Add(time.Hour))
	}
	e.record(day(-40))
	e.record(day(20))
	e.exec("DROP TABLE " + partitionName(day(2))) // 预建缺口：本轮应补上
	// 告警：过期已恢复、未过期已恢复、活跃（很久以前触发）。
	ds, ruleID := e.datasource("http://127.0.0.1:1"), e.rule(ruleSpec{})
	expired := e.seedAlert(ruleID, ds, "fp-expired", monitorcontract.AlertResolved, 200*d)
	recent := e.seedAlert(ruleID, ds, "fp-recent", monitorcontract.AlertResolved, 20*d)
	active := e.seedAlert(ruleID, ds, "fp-active", monitorcontract.AlertFiring, 0)
	for _, id := range []string{expired, recent, active} {
		e.exec("INSERT INTO monitor_notify_log (id, alert_id, kind, seq, source) VALUES ($1, $2, 1, 1, 1)", e.id(), id)
		e.exec(`INSERT INTO monitor_notify_outbox (id, alert_id, kind, seq, status, create_time, done_time)
			VALUES ($1, $2, 1, 1, $3, now() - interval '300 days', now() - interval '250 days')`, e.id(), id, monitorcontract.OutboxDone)
	}

	uc := biz.NewMaintenanceUsecase(repo, repo, testLogger()).WithOptions(biz.MaintenanceOptions{
		PartitionsAhead: 2, EvalRecordRetention: 7 * d, AlertRetention: 180 * d, PurgeBatch: 2, MaxBatches: 10,
	})
	for i := range 2 { // 第二轮：幂等
		if err := uc.RunOnce(ctx); err != nil {
			t.Fatalf("RunOnce() round %d error = %v", i, err)
		}
	}
	for i := 0; i <= 2; i++ {
		if !e.attached(day(i)) {
			t.Fatalf("partition for day +%d missing", i)
		}
	}
	if e.attached(day(-12)) || e.attached(day(-9)) || !e.attached(day(-3)) {
		t.Fatal("wrong partitions dropped")
	}
	if e.count("SELECT count(*) FROM pg_tables WHERE tablename IN ($1, $2)", partitionName(day(-12)), partitionName(day(-9))) != 0 {
		t.Fatal("expired partitions must be dropped, not left behind detached")
	}
	if e.count("SELECT count(*) FROM monitor_eval_record") != 2 || e.count("SELECT count(*) FROM "+evalRecordDefault) != 1 {
		t.Fatalf("records left = %d, in DEFAULT = %d", e.count("SELECT count(*) FROM monitor_eval_record"), e.count("SELECT count(*) FROM "+evalRecordDefault))
	}
	left := func(table, id string) int { return e.count("SELECT count(*) FROM "+table+" WHERE alert_id = $1", id) }
	if e.count("SELECT count(*) FROM monitor_alert WHERE id = $1", expired) != 0 || left("monitor_notify_log", expired) != 0 || left("monitor_notify_outbox", expired) != 0 {
		t.Fatal("the expired alert and its notification rows must be purged")
	}
	if e.count("SELECT count(*) FROM monitor_alert WHERE id IN ($1, $2)", recent, active) != 2 || left("monitor_notify_log", recent) != 1 || left("monitor_notify_log", active) != 1 {
		t.Fatal("alerts within the retention and active alerts must stay with their notification log")
	}
	// 已派发且过期的发件箱行即使告警还在也清理。
	if left("monitor_notify_outbox", recent) != 0 || left("monitor_notify_outbox", active) != 0 {
		t.Fatal("dispatched outbox rows past the retention must be purged")
	}
	if e.count("SELECT count(*) FROM monitor_eval_state WHERE alert_id = $1", active) != 1 {
		t.Fatal("state of the active alert must stay")
	}
	// 新分区的属主是引擎角色而不是登录账号：换一个登录账号照样能维护。
	if n := e.count(`SELECT count(*) FROM pg_class c JOIN pg_inherits i ON i.inhrelid = c.oid
		WHERE i.inhparent = 'monitor_eval_record'::regclass AND pg_get_userbyid(c.relowner) <> $1`, engineRole); n != 0 {
		t.Fatalf("%d partitions are not owned by %s", n, engineRole)
	}
}

// E14：DETACH PARTITION CONCURRENTLY 在本表上不可用（PostgreSQL 18），所以分区清理保留 DROP。
// 这条测试钉住该结论：哪天 PG 放开了限制，它会失败，提醒重新评估。
func TestDetachConcurrentlyIsUnavailable(t *testing.T) {
	e := newEnv(t)
	e.resetPartitions()
	t.Cleanup(e.resetPartitions)
	repo := NewMaintenanceRepo(e.d)
	ctx := context.Background()
	old := e.utcDay().AddDate(0, 0, -20)
	if err := repo.ensurePartition(ctx, old); err != nil {
		t.Fatal(err)
	}
	detach := "ALTER TABLE monitor_eval_record DETACH PARTITION " + partitionName(old) + " CONCURRENTLY"
	var pgErr *pgconn.PgError
	// 1. 父表有 DEFAULT 分区：直接拒绝。
	_, err := e.d.Pool.Exec(ctx, detach)
	if !errors.As(err, &pgErr) || !strings.Contains(pgErr.Message, "default partition") {
		t.Fatalf("DETACH CONCURRENTLY with a default partition: %v", err)
	}
	// 2. 不能在事务块内执行（因而无法与 SET LOCAL lock_timeout 同用）。
	tx, err := e.d.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, detach)
	_ = tx.Rollback(ctx)
	if !errors.As(err, &pgErr) || pgErr.Code != "25001" {
		t.Fatalf("DETACH CONCURRENTLY inside a transaction block: %v", err)
	}
	if !e.attached(old) {
		t.Fatal("the partition must still be attached")
	}
}

// E14：DROP 拿不到锁时在 lock_timeout 内放弃，不长时间挡住评估写入；锁释放后下一轮完成。
func TestPartitionDropGivesUpQuicklyUnderLockContention(t *testing.T) {
	e := newEnv(t)
	e.resetPartitions()
	t.Cleanup(e.resetPartitions)
	repo := NewMaintenanceRepo(e.d)
	ctx := context.Background()
	today := e.utcDay()
	for _, offset := range []int{-20, -21} {
		if err := repo.ensurePartition(ctx, today.AddDate(0, 0, offset)); err != nil {
			t.Fatal(err)
		}
	}
	// 一个长事务读着父表（ACCESS SHARE），DROP 需要的 ACCESS EXCLUSIVE 拿不到。
	reader, err := e.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Rollback(ctx) }()
	if _, err := reader.Exec(ctx, "LOCK TABLE monitor_eval_record IN ACCESS SHARE MODE"); err != nil {
		t.Fatal(err)
	}
	// 清理等锁期间到来的评估写入：排在它后面，但清理放弃后立即继续。
	wrote := make(chan time.Duration, 1)
	go func() {
		time.Sleep(300 * time.Millisecond)
		start := time.Now()
		_, err := e.d.Pool.Exec(ctx, "INSERT INTO monitor_eval_record (id, rule_id, datasource_id, eval_time, status) VALUES ($1, $2, $3, now(), 1)",
			e.id(), e.id(), e.id())
		if err != nil {
			wrote <- -1
			return
		}
		wrote <- time.Since(start)
	}()
	start := time.Now()
	dropped, err := repo.DropExpiredPartitions(ctx, 7*24*time.Hour)
	elapsed := time.Since(start)
	var pgErr *pgconn.PgError
	if dropped != 0 || !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
		t.Fatalf("dropped = %d, err = %v; want a lock timeout", dropped, err)
	}
	// 只等一次 lock_timeout（2s）：第一个分区拿不到锁就不再尝试第二个。
	if elapsed < 1500*time.Millisecond || elapsed > 3500*time.Millisecond {
		t.Fatalf("gave up after %v, want about one lock_timeout", elapsed)
	}
	select {
	case d := <-wrote:
		if d < 0 || d > 3*time.Second {
			t.Fatalf("an evaluation write was blocked for %v (-1ns = failed)", d)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the evaluation write is still blocked")
	}
	if err := reader.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if dropped, err = repo.DropExpiredPartitions(ctx, 7*24*time.Hour); err != nil || dropped != 2 {
		t.Fatalf("after the lock was released: dropped = %d, err = %v", dropped, err)
	}
}
