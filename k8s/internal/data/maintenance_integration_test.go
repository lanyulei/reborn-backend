//go:build integration

package data

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// auditIndexes 某张审计表（父表或分区）上的索引数与其中可用的索引数（indisvalid 且 indisready）。
func (e *env) auditIndexes(table string) (total, valid int) {
	e.t.Helper()
	e.scan(`SELECT count(*), count(*) FILTER (WHERE i.indisvalid AND i.indisready) FROM pg_index i WHERE i.indrelid = to_regclass($1)`,
		[]any{table}, &total, &valid)
	return total, valid
}

// tableExists 某张表是否还存在。
func (e *env) tableExists(name string) bool {
	e.t.Helper()
	return e.count("SELECT count(*) FROM pg_class WHERE oid = to_regclass($1)", name) == 1
}

// 下线实例的清理：只删心跳超过 deadAfter 的实例登记；残留的集群归属不在这里清（租约到期后自然可被领取）。
func TestPurgeInstances(t *testing.T) {
	e := newEnv(t)
	repo := NewMaintenanceRepo(e.d)
	dead, fresh := "plane-dead-"+e.id(), "plane-live-"+e.id()
	clusterID := e.cluster(clusterSpec{})
	e.instance(dead, "http://dead:8030", 2, time.Minute)
	e.instance(fresh, "http://live:8030", 1, 0)
	e.exec(`INSERT INTO k8s_cluster_runtime (cluster_id, owner_instance, lease_until) VALUES ($1, $2, now() - interval '1 minute')`,
		clusterID, dead)

	n, err := repo.PurgeInstances(ctxb(), 30*time.Second)
	if err != nil || n != 1 {
		t.Fatalf("PurgeInstances() = %d, %v; want 1", n, err)
	}
	if e.count("SELECT count(*) FROM k8s_instance WHERE id = ANY($1::text[])", []string{dead, fresh}) != 1 ||
		e.count("SELECT count(*) FROM k8s_instance WHERE id = $1", fresh) != 1 {
		t.Fatal("only the dead instance must be removed")
	}
	// 归属行原样保留：数据面实例异常退出后，它留下的租约在到期前仍然有效。
	if e.count("SELECT count(*) FROM k8s_cluster_runtime WHERE cluster_id = $1 AND owner_instance = $2", clusterID, dead) != 1 {
		t.Fatal("PurgeInstances must not touch cluster ownership rows")
	}
	// 幂等。
	if n, err = repo.PurgeInstances(ctxb(), 30*time.Second); err != nil || n != 0 {
		t.Fatalf("second PurgeInstances() = %d, %v; want 0", n, err)
	}
	// 边界：deadAfter = 0 时心跳早于数据库当前时刻的实例全部算下线（刚写过的心跳也是）。
	if n, err = repo.PurgeInstances(ctxb(), 0); err != nil || n != 1 {
		t.Fatalf("PurgeInstances(0) = %d, %v; want 1", n, err)
	}
	if e.count("SELECT count(*) FROM k8s_instance") != 0 {
		t.Fatal("PurgeInstances(0) must remove every instance")
	}
}

// 审计分区维护：预建、幂等、UTC 月界、迁出 DEFAULT 再 ATTACH、属主与索引、接续半成品。
func TestEnsureAuditPartitions(t *testing.T) {
	e := newEnv(t)
	repo := NewMaintenanceRepo(e.d)
	ctx := ctxb()
	month := e.utcMonth()
	m := func(offset int) time.Time { return month.AddDate(0, offset, 0) }
	name := func(offset int) string { return partitionName(m(offset)) }
	for i := 0; i <= migratedAuditAhead; i++ {
		if !e.attached(m(i)) {
			t.Fatalf("precondition: prebuilt partition for month +%d is missing", i)
		}
	}
	if e.attached(m(migratedAuditAhead + 1)) {
		t.Fatalf("precondition: partition for month +%d must not exist yet", migratedAuditAhead+1)
	}

	// 幂等：已存在的分区原样保留（对象不重建）；ahead ≤ 0 什么都不做。
	before := e.count("SELECT count(*) FROM pg_inherits WHERE inhparent = $1::regclass", auditTable)
	for range 2 {
		if err := repo.EnsureAuditPartitions(ctx, migratedAuditAhead); err != nil {
			t.Fatalf("EnsureAuditPartitions(%d) error = %v", migratedAuditAhead, err)
		}
	}
	for _, ahead := range []int{0, -1} {
		if err := repo.EnsureAuditPartitions(ctx, ahead); err != nil {
			t.Fatalf("EnsureAuditPartitions(%d) error = %v", ahead, err)
		}
	}
	if after := e.count("SELECT count(*) FROM pg_inherits WHERE inhparent = $1::regclass", auditTable); after != before {
		t.Fatalf("partitions = %d after idempotent runs, want %d", after, before)
	}

	// 预建到 +4：新分区挂上父表，不越出要求的范围。
	if err := repo.EnsureAuditPartitions(ctx, 4); err != nil {
		t.Fatalf("EnsureAuditPartitions(4) error = %v", err)
	}
	for i := 0; i <= 4; i++ {
		if !e.attached(m(i)) {
			t.Fatalf("partition for month +%d missing", i)
		}
	}
	if e.attached(m(5)) {
		t.Fatal("must not create beyond the requested horizon")
	}
	// 新分区的属主对齐父表（迁移设为数据面角色）：换一个登录账号后仍能维护。
	for _, offset := range []int{3, 4} {
		if e.count(`SELECT count(*) FROM pg_class c, pg_class p WHERE c.oid = to_regclass($1) AND p.oid = $2::regclass
			AND c.relowner = p.relowner`, name(offset), auditTable) != 1 {
			t.Fatalf("partition %s must be owned by the owner of %s", name(offset), auditTable)
		}
	}
	// 索引随 ATTACH 建好且有效：每个分区的索引与父表一一对应。
	parentIndexes, _ := e.auditIndexes(auditTable)
	for _, table := range []string{name(0), name(3), name(4)} {
		total, valid := e.auditIndexes(table)
		if total != parentIndexes || valid != total {
			t.Fatalf("%s has %d indexes (%d valid), want %d valid", table, total, valid, parentIndexes)
		}
	}

	// 边界是 UTC 月界（数据库会话的 TimeZone 不是 UTC 时也不漂移），含起点不含终点。
	for at, want := range map[time.Time]string{
		m(3): name(3), m(4).Add(-time.Microsecond): name(3), m(4): name(4), time.Now(): name(0),
	} {
		if got := e.audit(at); got != want {
			t.Fatalf("audit at %s landed in %s, want %s", at.Format(time.RFC3339Nano), got, want)
		}
	}

	// 维护失效期间落进 DEFAULT 的行：建分区时迁出再挂接，行不丢，DEFAULT 里该区间不再有行。
	inRange := []time.Time{m(6).Add(3 * time.Hour), m(6).Add(10 * 24 * time.Hour), m(6).AddDate(0, 1, 0).Add(-time.Microsecond)}
	for _, at := range inRange {
		if got := e.audit(at); got != auditDefault {
			t.Fatalf("audit at %s landed in %s, want %s", at.Format(time.RFC3339Nano), got, auditDefault)
		}
	}
	e.audit(m(7)) // 不在本次预建范围内：留在 DEFAULT
	const inDefault = 4
	defaultBefore, totalBefore := e.count("SELECT count(*) FROM "+auditDefault), e.count("SELECT count(*) FROM "+auditTable)
	if defaultBefore != inDefault {
		t.Fatalf("precondition: %d rows in %s, want %d", defaultBefore, auditDefault, inDefault)
	}
	if err := repo.EnsureAuditPartitions(ctx, 6); err != nil {
		t.Fatalf("EnsureAuditPartitions(6) with rows in DEFAULT error = %v", err)
	}
	if !e.attached(m(6)) || e.attached(m(7)) {
		t.Fatal("wrong partitions after EnsureAuditPartitions(6)")
	}
	if n := e.count("SELECT count(*) FROM " + name(6)); n != len(inRange) {
		t.Fatalf("%d rows moved into %s, want %d", n, name(6), len(inRange))
	}
	if d, tot := e.count("SELECT count(*) FROM "+auditDefault), e.count("SELECT count(*) FROM "+auditTable); d != inDefault-len(inRange) || tot != totalBefore {
		t.Fatalf("rows in DEFAULT = %d (want %d), total = %d (want %d)", d, inDefault-len(inRange), tot, totalBefore)
	}
	// 迁出后：行经父表按主键仍可查，新分区的索引有效，DEFAULT 里该区间一行不剩。
	if e.count("SELECT count(*) FROM "+auditTable+" WHERE create_time >= $1 AND create_time < $2", m(6), m(7)) != len(inRange) {
		t.Fatal("moved rows must stay reachable through the parent table")
	}
	if total, valid := e.auditIndexes(name(6)); total != parentIndexes || valid != total {
		t.Fatalf("%s has %d indexes (%d valid), want %d valid", name(6), total, valid, parentIndexes)
	}
	if e.count("SELECT count(*) FROM "+auditDefault+" WHERE create_time >= $1 AND create_time < $2", m(6), m(7)) != 0 {
		t.Fatal("rows of the new range must not stay in DEFAULT")
	}

	// 上一轮建表后、挂接前（拿不到锁）留下的独立表：下一轮原样接续——同一个表对象，属主对齐，挂上父表。
	// 以数据面身份建表：属主是登录账号，正是生产里半成品的形态。
	if _, err := e.d.Pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+name(7)+" (LIKE "+auditTable+" INCLUDING DEFAULTS INCLUDING CONSTRAINTS)"); err != nil {
		t.Fatalf("create half-made partition: %v", err)
	}
	var oidBefore int
	e.scan("SELECT (to_regclass($1)::oid)::int", []any{name(7)}, &oidBefore)
	if err := repo.EnsureAuditPartitions(ctx, 7); err != nil {
		t.Fatalf("EnsureAuditPartitions(7) error = %v", err)
	}
	var oidAfter int
	e.scan("SELECT (to_regclass($1)::oid)::int", []any{name(7)}, &oidAfter)
	if !e.attached(m(7)) || oidAfter != oidBefore {
		t.Fatalf("half-made partition must be completed in place: attached = %v, oid %d → %d", e.attached(m(7)), oidBefore, oidAfter)
	}
	if e.count(`SELECT count(*) FROM pg_class c, pg_class p WHERE c.oid = to_regclass($1) AND p.oid = $2::regclass AND c.relowner = p.relowner`,
		name(7), auditTable) != 1 {
		t.Fatalf("%s must be re-owned by the owner of %s", name(7), auditTable)
	}
	if e.count(`SELECT count(*) FROM pg_constraint WHERE conrelid = to_regclass($1) AND conname = $2 AND contype = 'c'`,
		name(7), name(7)+"_range") != 1 {
		t.Fatalf("%s must carry its range CHECK constraint", name(7))
	}
	if d, n := e.count("SELECT count(*) FROM "+auditDefault), e.count("SELECT count(*) FROM "+name(7)); d != 0 || n != 1 {
		t.Fatalf("DEFAULT = %d, %s = %d; the stray row must move into the new partition", d, name(7), n)
	}
}

// 拿不到锁：DEFAULT 分区正被别的事务读着时，迁出需要的 ACCESS EXCLUSIVE 在 lock_timeout（2s）内放弃，
// 不长时间挡住审计写入；锁释放后下一轮接着完成（上一轮建好的独立表被原样接续）。
func TestEnsureAuditPartitionGivesUpQuicklyUnderLockContention(t *testing.T) {
	e := newEnv(t)
	repo := NewMaintenanceRepo(e.d)
	ctx := ctxb()
	target := e.utcMonth().AddDate(0, 3, 0)
	reader, err := e.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Rollback(ctx) }()
	if _, err := reader.Exec(ctx, "LOCK TABLE "+auditDefault+" IN ACCESS SHARE MODE"); err != nil {
		t.Fatal(err)
	}
	// 清理等锁期间到来的审计写入：路由到当月分区，不应被长挡。
	auditID := e.id()
	wrote := make(chan time.Duration, 1)
	go func() {
		time.Sleep(300 * time.Millisecond)
		start := time.Now()
		if _, err := e.d.Pool.Exec(ctx, "INSERT INTO k8s_audit (id, action, result) VALUES ($1, 'update', 1)", auditID); err != nil {
			wrote <- -1
			return
		}
		wrote <- time.Since(start)
	}()
	start := time.Now()
	err = repo.EnsureAuditPartitions(ctx, 3)
	elapsed := time.Since(start)
	if pgCode(err) != "55P03" {
		t.Fatalf("EnsureAuditPartitions() under contention error = %v; want a lock timeout (55P03)", err)
	}
	if elapsed < 1500*time.Millisecond || elapsed > 3500*time.Millisecond {
		t.Fatalf("gave up after %v, want about one lock_timeout", elapsed)
	}
	select {
	case d := <-wrote:
		if d < 0 || d > 3*time.Second {
			t.Fatalf("an audit write was blocked for %v (-1ns = failed)", d)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the audit write is still blocked")
	}
	if err := reader.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := repo.EnsureAuditPartitions(ctx, 3); err != nil || !e.attached(target) {
		t.Fatalf("after the lock was released: attached = %v, err = %v", e.attached(target), err)
	}
}

// 过期清理：过期的 Agent 令牌、补结束时间的孤儿会话恒处理；retention > 0 时再清过期的终端会话记录与
// 审计分区。该删的删、不该删的留，返回值是本轮处理的条数。
func TestPurgeExpired(t *testing.T) {
	e := newEnv(t)
	repo := NewMaintenanceRepo(e.d)
	ctx := ctxb()
	const day = 24 * time.Hour
	agent, direct := e.cluster(clusterSpec{Mode: k8scontract.AccessAgent}), e.cluster(clusterSpec{})

	// 令牌：过期（注册 / 长期）删，未过期与不过期的留。
	expiredReg := e.token(agent, k8scontract.TokenRegister, "s1", ptr(-time.Hour))
	expiredLong := e.token(agent, k8scontract.TokenLongTerm, "s2", ptr(-time.Second))
	liveReg := e.token(agent, k8scontract.TokenRegister, "s3", ptr(time.Hour))
	forever := e.token(agent, k8scontract.TokenLongTerm, "s4", nil)
	// 终端会话：实例异常退出后一直「进行中」的、刚开始的、结束已久 / 不久的、刚好在保留期边缘的。
	orphan := e.execSession(execSpec{Cluster: direct, Started: 10 * time.Hour})
	young := e.execSession(execSpec{Cluster: direct, Started: time.Hour})
	old := e.execSession(execSpec{Cluster: direct, Ended: ptr(200 * day), Records: 3})
	recent := e.execSession(execSpec{Cluster: direct, Ended: ptr(10 * day), Records: 2})
	edge := e.execSession(execSpec{Cluster: direct, Ended: ptr(149 * day), Records: 1})
	// 审计分区：上界早于保留期的（7 个月前：其上界距今 181～214 天，都多于 150 天）与仍在保留期内的
	// （4 个月前：其上界距今 89～122 天，都少于 150 天）。
	month := e.utcMonth()
	staleMonth, keptMonth := month.AddDate(0, -7, 0), month.AddDate(0, -4, 0)
	for _, m := range []time.Time{staleMonth, keptMonth} {
		if err := repo.ensurePartition(ctx, m); err != nil {
			t.Fatalf("create partition %s: %v", partitionName(m), err)
		}
		e.audit(m.Add(48 * time.Hour))
	}
	// 没有对应分区的月份：行落进 DEFAULT。
	if stray := e.audit(time.Date(2019, 1, 15, 0, 0, 0, 0, time.UTC)); stray != auditDefault {
		t.Fatalf("audit without a partition landed in %s, want %s", stray, auditDefault)
	}

	// retention = 0：只清过期令牌、给孤儿会话补结束时间；历史一律不动。
	n, err := repo.PurgeExpired(ctx, 0)
	if err != nil || n != 3 {
		t.Fatalf("PurgeExpired(0) = %d, %v; want 3 (two tokens + one orphan session)", n, err)
	}
	if e.count("SELECT count(*) FROM k8s_agent_token WHERE id = ANY($1::uuid[])", []string{liveReg, forever}) != 2 ||
		e.count("SELECT count(*) FROM k8s_agent_token WHERE id = ANY($1::uuid[])", []string{expiredReg, expiredLong}) != 0 ||
		e.count("SELECT count(*) FROM k8s_agent_token") != 2 {
		t.Fatal("only the expired tokens must be purged")
	}
	if r := e.execRow(orphan); !r.Ended || r.Reason != "instance_lost" || r.End.Before(e.dbNow().Add(-time.Minute)) {
		t.Fatalf("orphan session must be closed by the reaper: %+v", r)
	}
	if r := e.execRow(young); r.Ended {
		t.Fatalf("a young running session must be left alone: %+v", r)
	}
	if e.count("SELECT count(*) FROM k8s_exec_session") != 5 || e.count("SELECT count(*) FROM k8s_exec_record") != 6 ||
		!e.attached(staleMonth) || e.count("SELECT count(*) FROM "+auditDefault) != 1 {
		t.Fatal("PurgeExpired(0) must not touch sessions, records, audit partitions or DEFAULT")
	}
	if n, err = repo.PurgeExpired(ctx, 0); err != nil || n != 0 {
		t.Fatalf("second PurgeExpired(0) = %d, %v; want 0", n, err)
	}

	// retention = 150 天：过期的会话连同录制、上界早于保留期的审计分区被清；保留期内的不动。
	n, err = repo.PurgeExpired(ctx, 150*day)
	if err != nil || n != 2 {
		t.Fatalf("PurgeExpired(150d) = %d, %v; want 2 (one session + one partition)", n, err)
	}
	if e.count("SELECT count(*) FROM k8s_exec_session WHERE id = $1", old) != 0 ||
		e.count("SELECT count(*) FROM k8s_exec_record WHERE session_id = $1", old) != 0 {
		t.Fatal("the expired session and its records must be purged")
	}
	if e.count("SELECT count(*) FROM k8s_exec_session WHERE id = ANY($1::uuid[])", []string{recent, edge, orphan, young}) != 4 ||
		e.count("SELECT count(*) FROM k8s_exec_record") != 3 {
		t.Fatal("sessions within the retention must stay with their records")
	}
	if e.attached(staleMonth) || e.tableExists(partitionName(staleMonth)) {
		t.Fatal("the expired partition must be dropped, not just detached")
	}
	if !e.attached(keptMonth) || !e.attached(month) || !e.attached(month.AddDate(0, 1, 0)) {
		t.Fatal("partitions within the retention must stay")
	}
	// DEFAULT 分区不动（现状）：PurgeExpired 只按月分区整体 DROP，DEFAULT 里没有对应分区的陈旧行不清。
	if e.count("SELECT count(*) FROM pg_inherits WHERE inhrelid = to_regclass($1)", auditDefault) != 1 {
		t.Fatal("the DEFAULT partition must stay")
	}
	if e.count("SELECT count(*) FROM "+auditDefault) != 1 {
		t.Fatal("DEFAULT is never purged row by row (current behaviour)")
	}
	if n, err = repo.PurgeExpired(ctx, 150*day); err != nil || n != 0 {
		t.Fatalf("third PurgeExpired(150d) = %d, %v; want 0", n, err)
	}
}

// 拿不到锁：父表正被别的事务读着时，DROP 需要的 ACCESS EXCLUSIVE 在 lock_timeout 内放弃，本轮不清；
// 锁释放后下一轮完成。
func TestPurgeExpiredGivesUpQuicklyUnderLockContention(t *testing.T) {
	e := newEnv(t)
	repo := NewMaintenanceRepo(e.d)
	ctx := ctxb()
	old := e.utcMonth().AddDate(0, -8, 0)
	if err := repo.ensurePartition(ctx, old); err != nil {
		t.Fatal(err)
	}
	e.audit(old.Add(time.Hour))
	reader, err := e.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Rollback(ctx) }()
	if _, err := reader.Exec(ctx, "LOCK TABLE "+auditTable+" IN ACCESS SHARE MODE"); err != nil {
		t.Fatal(err)
	}
	auditID := e.id()
	wrote := make(chan time.Duration, 1)
	go func() {
		time.Sleep(300 * time.Millisecond)
		start := time.Now()
		if _, err := e.d.Pool.Exec(ctx, "INSERT INTO k8s_audit (id, action, result) VALUES ($1, 'update', 1)", auditID); err != nil {
			wrote <- -1
			return
		}
		wrote <- time.Since(start)
	}()
	start := time.Now()
	n, err := repo.PurgeExpired(ctx, 30*24*time.Hour)
	elapsed := time.Since(start)
	if n != 0 || pgCode(err) != "55P03" {
		t.Fatalf("PurgeExpired() under contention = %d, %v; want 0 and a lock timeout (55P03)", n, err)
	}
	if elapsed < 1500*time.Millisecond || elapsed > 3500*time.Millisecond {
		t.Fatalf("gave up after %v, want about one lock_timeout", elapsed)
	}
	select {
	case d := <-wrote:
		if d < 0 || d > 3*time.Second {
			t.Fatalf("an audit write was blocked for %v (-1ns = failed)", d)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the audit write is still blocked")
	}
	if err := reader.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if n, err = repo.PurgeExpired(ctx, 30*24*time.Hour); err != nil || n != 1 || e.attached(old) {
		t.Fatalf("after the lock was released: dropped = %d, err = %v", n, err)
	}
}

// ---- 清理 DELETE 分批 ----

// 改成分批之前的清理语句原文（基准）：分批清理的最终状态必须与它们在同样数据上的结果一致。
const (
	singleTokens  = `DELETE FROM k8s_agent_token WHERE expire_time IS NOT NULL AND expire_time < now()`
	singleRecords = `DELETE FROM k8s_exec_record WHERE session_id IN (
	     SELECT id FROM k8s_exec_session WHERE end_time IS NOT NULL AND end_time < now() - make_interval(secs => $1))`
	singleSessions = "DELETE FROM k8s_exec_session WHERE end_time IS NOT NULL AND end_time < now() - make_interval(secs => $1)"
)

// purgeState 行级清理涉及的三张表里剩下哪些行（排序后的清单，与清理顺序无关）。
type purgeState struct {
	tokens, sessions, records []string
}

func (e *env) purgeState() purgeState {
	e.t.Helper()
	list := func(sql string) []string {
		rows, err := e.admin.Query(ctxb(), sql)
		if err != nil {
			e.t.Fatalf("list rows: %v", err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				e.t.Fatalf("list rows: %v", err)
			}
			out = append(out, id)
		}
		if err := rows.Err(); err != nil {
			e.t.Fatalf("list rows: %v", err)
		}
		return out
	}
	return purgeState{
		tokens:   list("SELECT id::text FROM k8s_agent_token ORDER BY 1"),
		sessions: list("SELECT id::text FROM k8s_exec_session ORDER BY 1"),
		records:  list("SELECT session_id::text || '/' || seq FROM k8s_exec_record ORDER BY 1"),
	}
}

// orphans 没有会话行的录制块（永远清不掉的那种）。
func (e *env) orphans() int {
	return e.count(`SELECT count(*) FROM k8s_exec_record r LEFT JOIN k8s_exec_session s ON s.id = r.session_id WHERE s.id IS NULL`)
}

// seedPurge 重建一份清理数据集（先清空三张表）：expired 条结束于 endedAgo 之前的会话（各带 2 块录制）、
// 5 条刚结束不久的会话、一枚过期与一枚不过期的令牌。同一个 tag 重建出的会话 / 录制 ID 相同。
func (e *env) seedPurge(clusterID, tag string, expired int, endedAgo time.Duration) (expiredToken, liveToken string) {
	e.t.Helper()
	e.exec("TRUNCATE TABLE k8s_exec_session, k8s_exec_record, k8s_agent_token CASCADE")
	e.execSessionsBulk(clusterID, tag+":expired", expired, 2, endedAgo)
	e.execSessionsBulk(clusterID, tag+":live", 5, 2, time.Hour)
	return e.token(clusterID, k8scontract.TokenRegister, "expired", ptr(-time.Hour)),
		e.token(clusterID, k8scontract.TokenLongTerm, "live", nil)
}

// onlyLive 清理之后三张表里只剩保留期内的数据，且没有孤儿录制。
func (e *env) onlyLive(liveToken string) bool {
	e.t.Helper()
	st := e.purgeState()
	return len(st.sessions) == 5 && len(st.records) == 10 && sameSet(st.tokens, []string{liveToken}) && e.orphans() == 0
}

// testBudget 需要跑完全部分批的测试用的时间预算：缺省的 purgeBudget（1 分钟）是给生产里的积压用的，
// 别让共享测试库上的偶发卡顿把一轮提前截断，否则「批数」断言会误报。
const testBudget = 10 * time.Minute

// 清理 DELETE 分批：最终状态与单条无界 DELETE（改之前的语句原文）一致；单条语句影响行数不超过批大小，
// 行数多时确实分成多批；没有过期行时不再发出 DELETE。
func TestPurgeExpiredDeleteBatches(t *testing.T) {
	e := newEnv(t)
	repo := NewMaintenanceRepo(e.d)
	repo.budget = testBudget
	ctx := ctxb()
	const retention = 150 * 24 * time.Hour
	clusterID := e.cluster(clusterSpec{})
	const expired = purgeBatch*2 + 7 // 两整批多一点

	// 基准：改之前的单条无界 DELETE。
	_, liveToken := e.seedPurge(clusterID, "it", expired, 200*24*time.Hour)
	for _, step := range []struct {
		sql  string
		args []any
	}{{singleTokens, nil}, {singleRecords, []any{retention.Seconds()}}, {singleSessions, []any{retention.Seconds()}}} {
		if _, err := e.d.Pool.Exec(ctx, step.sql, step.args...); err != nil {
			t.Fatalf("single-statement purge %.40q: %v", step.sql, err)
		}
	}
	want := e.purgeState()
	if len(want.sessions) != 5 || len(want.records) != 10 || !sameSet(want.tokens, []string{liveToken}) {
		t.Fatalf("precondition: reference purge left %d sessions / %d records", len(want.sessions), len(want.records))
	}

	// 分批清理：同一份数据（同 tag → 同 ID；令牌 ID 是随机生成的，按本次签发的那一枚核对）。
	_, liveToken = e.seedPurge(clusterID, "it", expired, 200*24*time.Hour)
	e.statements()
	n, err := repo.PurgeExpired(ctx, retention)
	stmts := e.statements()
	if err != nil || n != expired+1 { // 过期的会话 + 过期的令牌
		t.Fatalf("PurgeExpired() = %d, %v; want %d", n, err, expired+1)
	}
	// 1. 最终状态与单条删除一致。
	got := e.purgeState()
	if !slices.Equal(got.sessions, want.sessions) || !slices.Equal(got.records, want.records) || !sameSet(got.tokens, []string{liveToken}) || e.orphans() != 0 {
		t.Fatalf("batched purge left a different state: %d/%d/%d rows, want %d/%d/%d",
			len(got.sessions), len(got.records), len(got.tokens), len(want.sessions), len(want.records), len(want.tokens))
	}
	// 2. 单条语句影响行数不超过批大小；行数多时分成多批，最后一批不足一批即停。
	sessions, records, tokens := deletedRows(stmts, "k8s_exec_session"), deletedRows(stmts, "k8s_exec_record"), deletedRows(stmts, "k8s_agent_token")
	if !slices.Equal(sessions, []int64{purgeBatch, purgeBatch, 7}) ||
		!slices.Equal(records, []int64{purgeBatch, purgeBatch, purgeBatch, purgeBatch, 14}) || !slices.Equal(tokens, []int64{1}) {
		t.Fatalf("rows per DELETE — sessions %v, records %v, tokens %v", sessions, records, tokens)
	}
	for _, s := range stmts {
		if strings.HasPrefix(s.sql, "DELETE FROM ") {
			t.Logf("%s… rows=%d took=%v", s.sql[:min(len(s.sql), 48)], s.rows, s.took)
		}
	}
	// 3. 没有过期行时不再发出删除。
	e.statements()
	if n, err = repo.PurgeExpired(ctx, retention); err != nil || n != 0 {
		t.Fatalf("PurgeExpired() with nothing expired = %d, %v; want 0", n, err)
	}
	if again := e.statements(); len(deletedRows(again, "k8s_exec_session"))+len(deletedRows(again, "k8s_exec_record"))+len(deletedRows(again, "k8s_agent_token")) != 0 {
		t.Fatalf("DELETEs issued while nothing was expired: %+v", again)
	}
	// 批大小可配：循环与批大小无关（10 条过期会话 → 4 / 4 / 2；20 块录制恰好是 5 整批，之后再发一条删到 0 行才知道清完）。
	repo.batch = 4
	e.seedPurge(clusterID, "small", 10, 200*24*time.Hour)
	e.statements()
	if n, err = repo.PurgeExpired(ctx, retention); err != nil || n != 11 {
		t.Fatalf("PurgeExpired() with batch 4 = %d, %v; want 11", n, err)
	}
	stmts = e.statements()
	sessions, records = deletedRows(stmts, "k8s_exec_session"), deletedRows(stmts, "k8s_exec_record")
	if !slices.Equal(sessions, []int64{4, 4, 2}) || !slices.Equal(records, []int64{4, 4, 4, 4, 4, 0}) {
		t.Fatalf("rows per DELETE with batch 4 — sessions %v, records %v", sessions, records)
	}
	repo.batch = purgeBatch
}

// 分批清理的停止条件：上下文结束——停下并返回上下文错误，没删的留给下一轮；本轮时间预算用尽——停下且不报错，
// 录制没清完时会话一个不动（否则会留下永远清不掉的孤儿录制），下一轮接着删。
func TestPurgeExpiredStopsWhenInterrupted(t *testing.T) {
	e := newEnv(t)
	repo := NewMaintenanceRepo(e.d)
	repo.budget = testBudget
	ctx := ctxb()
	const retention = 150 * 24 * time.Hour
	clusterID := e.cluster(clusterSpec{})
	const expired = purgeBatch*2 + 7
	const live = 5

	// 上下文在第一批会话删满之后结束。
	_, liveToken := e.seedPurge(clusterID, "cancel", expired, 200*24*time.Hour)
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	e.onStatement(func(s stmt) {
		if strings.HasPrefix(s.sql, "DELETE FROM k8s_exec_session ") && s.rows == int64(purgeBatch) {
			cancel()
		}
	})
	e.statements()
	_, err := repo.PurgeExpired(cctx, retention)
	e.onStatement(nil)
	// 每条 DELETE 的耗时与影响行数（排查偶发慢语句时直接看得到）。
	for _, s := range e.statements() {
		if strings.HasPrefix(s.sql, "DELETE FROM ") {
			t.Logf("%s… rows=%d took=%v failed=%v", s.sql[:min(len(s.sql), 48)], s.rows, s.took, s.fails)
		}
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("PurgeExpired() with a cancelled context error = %v, want context.Canceled", err)
	}
	if n := e.count("SELECT count(*) FROM k8s_exec_session"); n != purgeBatch+7+live {
		t.Fatalf("%d sessions after the first batch was interrupted, want %d", n, purgeBatch+7+live)
	}
	// 下一轮（新的上下文）删完剩余。
	if n, err := repo.PurgeExpired(ctx, retention); err != nil || n != purgeBatch+7 || !e.onlyLive(liveToken) {
		t.Fatalf("PurgeExpired() after the interruption = %d, %v; only live rows left = %v", n, err, e.onlyLive(liveToken))
	}

	// 本轮时间预算用尽：录制只删了一整批就停下，不报错；会话因此一个不动。
	_, liveToken = e.seedPurge(clusterID, "budget", expired, 200*24*time.Hour)
	repo.budget = time.Nanosecond
	e.statements()
	n, err := repo.PurgeExpired(ctx, retention)
	stmts := e.statements()
	repo.budget = testBudget
	if err != nil || n != 1 { // 只有那枚过期令牌
		t.Fatalf("PurgeExpired() with an exhausted budget = %d, %v; want 1", n, err)
	}
	if records, sessions := deletedRows(stmts, "k8s_exec_record"), deletedRows(stmts, "k8s_exec_session"); !slices.Equal(records, []int64{purgeBatch}) || len(sessions) != 0 {
		t.Fatalf("DELETEs with an exhausted budget — records %v, sessions %v; want one full record batch and no session DELETE", records, sessions)
	}
	if n := e.count("SELECT count(*) FROM k8s_exec_session"); n != expired+live {
		t.Fatalf("%d sessions after an interrupted round; sessions must stay until their records are gone", n)
	}
	// 剩下的录制 = 过期会话的（少了一整批）+ 保留期内会话的。
	if n := e.count("SELECT count(*) FROM k8s_exec_record"); n != 2*expired+2*live-purgeBatch {
		t.Fatalf("%d records after an interrupted round, want %d", n, 2*expired+2*live-purgeBatch)
	}
	// 下一轮（预算恢复）删完，没有留下孤儿录制。
	if n, err := repo.PurgeExpired(ctx, retention); err != nil || n != expired || !e.onlyLive(liveToken) {
		t.Fatalf("PurgeExpired() after the budget was restored = %d, %v; only live rows left = %v", n, err, e.onlyLive(liveToken))
	}
}

// 刚越过保留期、但还没越过「删会话的余量」的会话：录制本轮就删，会话多留一轮（purgeSessionMargin）。
// 这样被删掉的会话，它的录制一定已经删过——不会出现「会话没了、录制还在」、而且再也清不掉的孤儿录制。
func TestPurgeExpiredKeepsSessionsInsideTheMargin(t *testing.T) {
	e := newEnv(t)
	repo := NewMaintenanceRepo(e.d)
	ctx := ctxb()
	const retention = 150 * 24 * time.Hour
	direct := e.cluster(clusterSpec{})
	deep := e.execSession(execSpec{Cluster: direct, Ended: ptr(retention + time.Hour), Records: 2})
	band := e.execSession(execSpec{Cluster: direct, Ended: ptr(retention + purgeSessionMargin/2), Records: 2})
	session := func(id string) (bool, int) {
		return e.count("SELECT count(*) FROM k8s_exec_session WHERE id = $1", id) == 1,
			e.count("SELECT count(*) FROM k8s_exec_record WHERE session_id = $1", id)
	}

	if _, err := repo.PurgeExpired(ctx, retention); err != nil {
		t.Fatalf("PurgeExpired() error = %v", err)
	}
	if deepSession, deepRecords := session(deep); deepSession || deepRecords != 0 {
		t.Fatalf("a session well past the retention must be deleted together with its records: session row left = %v, records left = %d", deepSession, deepRecords)
	}
	if bandSession, bandRecords := session(band); !bandSession || bandRecords != 0 {
		t.Fatalf("a session inside the margin keeps its row but loses its records this round: session %v, records %d", bandSession, bandRecords)
	}
	if e.count(`SELECT count(*) FROM k8s_exec_record r WHERE NOT EXISTS (SELECT 1 FROM k8s_exec_session s WHERE s.id = r.session_id)`) != 0 {
		t.Fatal("no record may outlive its session")
	}

	// 下一轮：越过余量后会话被删。
	e.exec(`UPDATE k8s_exec_session SET end_time = now() - make_interval(secs => $1) WHERE id = $2::uuid`, (retention + 2*purgeSessionMargin).Seconds(), band)
	if _, err := repo.PurgeExpired(ctx, retention); err != nil {
		t.Fatalf("PurgeExpired() error = %v", err)
	}
	if bandSession, _ := session(band); bandSession {
		t.Fatal("the session must be deleted once it is past the margin")
	}
}
