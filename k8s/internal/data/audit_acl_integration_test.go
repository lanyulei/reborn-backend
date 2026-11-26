//go:build integration

package data

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// 审计表只增不改的数据库约束（迁移 0092_audit_append_only）。k8s_audit 的属主是数据面角色 k8s_plane_role
// （迁移 0072 为了分区维护而设），它自己对父表与月分区的 UPDATE / DELETE / TRUNCATE 被收回，INSERT / SELECT 与属主身份
// 不变；DEFAULT 分区保留 DELETE——新月份的分区建好时要把此前落进去的行搬过去（见 maintenance.go 的 ensurePartition）。
// core 的角色只有 SELECT / INSERT。这里在真实库上断言每条越权语句都被数据库拒绝（42501）：代码约束之外的最后一道防线。

const (
	// insufficientPrivilege 权限不足的 SQLSTATE。
	insufficientPrivilege = "42501"
	// coreRole core 服务读写审计用的 DB 角色（迁移 0072：只授 SELECT, INSERT）。
	coreRole = "k8s_core_role"
	// coreMemberRole 集成测试专用的 coreRole 成员（做法同 newEnv 的 memberRole）：生产里 core 用自己的登录账号连库。
	coreMemberRole = "k8s_core_it_member"
)

// planeIdentities 数据面的两种会话角色：成员（生产里数据面的登录账号，也是夹具连接池的缺省角色）与属主角色本身。
func planeIdentities() []string { return []string{memberRole, planeRole} }

// auditWrites 对一张审计表改写 / 删除 / 清空的语句，键是「语句 表」。与表里有没有行无关：权限检查在执行之前。
func auditWrites(table string) map[string]string {
	q := pgx.Identifier{table}.Sanitize()
	return map[string]string{
		"UPDATE " + table:   "UPDATE " + q + " SET username = 'x'",
		"DELETE " + table:   "DELETE FROM " + q,
		"TRUNCATE " + table: "TRUNCATE " + q,
	}
}

// auditInsertSelect 对父表写入与读取的语句：写入经父表路由到当月分区，不需要分区上的授权。
func (e *env) auditInsertSelect() map[string]string {
	return map[string]string{
		"INSERT INTO k8s_audit": fmt.Sprintf(`INSERT INTO k8s_audit (id, action, result) VALUES ('%s', 'update', 1)`, e.id()),
		"SELECT FROM k8s_audit": `SELECT id::text, username, before_obj, after_obj, patch FROM k8s_audit
			WHERE create_time <= now() + interval '1 second' ORDER BY create_time DESC, id DESC LIMIT 10`,
	}
}

// stmtCodes 在 pool 的一条独占连接上开一个回滚的事务（role 非空时先把会话角色切到它），逐条执行语句并返回各自的错误
// （成功为 nil）。每条语句包在保存点里：被拒绝的语句不会毒化后面语句的执行。事务一律回滚，不留数据——
// 「会被数据库拒绝」与「不该成功却成功了」的语句都经它执行，后者（例如误放行的 TRUNCATE）也不会真的改到数据。
func stmtCodes(t *testing.T, pool *pgxpool.Pool, role string, stmts map[string]string) map[string]error {
	t.Helper()
	ctx := ctxb()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	tx, err := conn.Conn().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if role != "" {
		if _, err := tx.Exec(ctx, "SET LOCAL ROLE "+pgx.Identifier{role}.Sanitize()); err != nil {
			t.Fatalf("SET LOCAL ROLE %s: %v", role, err)
		}
	}
	out := make(map[string]error, len(stmts))
	for _, name := range slices.Sorted(maps.Keys(stmts)) {
		if _, err := tx.Exec(ctx, "SAVEPOINT probe"); err != nil {
			t.Fatalf("savepoint before %s: %v", name, err)
		}
		_, execErr := tx.Exec(ctx, stmts[name])
		if _, err := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT probe"); err != nil {
			t.Fatalf("rollback to savepoint after %s: %v", name, err)
		}
		out[name] = execErr
	}
	return out
}

// asPlane 以数据面连接池、会话角色 who 执行一批语句（见 stmtCodes）。
func (e *env) asPlane(who string, stmts map[string]string) map[string]error {
	return stmtCodes(e.t, e.d.Pool, who, stmts)
}

// wantDenied 断言 got 里的每条语句都被数据库以权限不足（42501）拒绝；没被拒绝的按会话角色汇总报出。
func (e *env) wantDenied(who string, got map[string]error) {
	e.t.Helper()
	var ran, other []string
	for _, name := range slices.Sorted(maps.Keys(got)) {
		err := got[name]
		switch {
		case pgCode(err) == insufficientPrivilege:
		case err == nil:
			ran = append(ran, name)
		default:
			other = append(other, fmt.Sprintf("%s: %v", name, err))
		}
	}
	if len(ran) > 0 {
		e.t.Errorf("[%s] executed, want permission denied (%s): %s", who, insufficientPrivilege, strings.Join(ran, "; "))
	}
	if len(other) > 0 {
		e.t.Errorf("[%s] want permission denied (%s), got: %s", who, insufficientPrivilege, strings.Join(other, "; "))
	}
}

// wantOK 断言 got 里的每条语句都能执行。
func (e *env) wantOK(who string, got map[string]error) {
	e.t.Helper()
	for _, name := range slices.Sorted(maps.Keys(got)) {
		if err := got[name]; err != nil {
			e.t.Errorf("%s [%s]: unexpected error: %v", name, who, err)
		}
	}
}

// auditPartitionNames 挂在 k8s_audit 上的全部分区名（含 DEFAULT），取自系统目录。
func (e *env) auditPartitionNames() []string {
	e.t.Helper()
	rows, err := e.admin.Query(ctxb(), `SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
		WHERE i.inhparent = $1::regclass ORDER BY 1`, auditTable)
	if err != nil {
		e.t.Fatalf("list audit partitions: %v", err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			e.t.Fatalf("list audit partitions: %v", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		e.t.Fatalf("list audit partitions: %v", err)
	}
	return names
}

// monthlyPartitionNames 挂在 k8s_audit 上的月分区名（不含 DEFAULT）。
func (e *env) monthlyPartitionNames() []string {
	e.t.Helper()
	return slices.DeleteFunc(e.auditPartitionNames(), func(name string) bool { return name == auditDefault })
}

// hasPrivilege role 对某张表是否持有某项权限（has_table_privilege：含经成员关系继承到的）。
func (e *env) hasPrivilege(role, table, priv string) bool {
	e.t.Helper()
	return e.count("SELECT count(*) FROM (SELECT has_table_privilege($1, $2, $3) AS has) p WHERE has", role, table, priv) == 1
}

// asCoreRole 以 coreRole 成员的会话角色建连接池（做法同 newEnv 的数据面连接池；成员角色是测试专用的，用完即删）。
func (e *env) asCoreRole() *pgxpool.Pool {
	e.t.Helper()
	ensureMemberRole(ctxb(), e.t, e.admin, coreMemberRole, coreRole)
	pool := newPoolAs(e.t, coreMemberRole, func(cfg *pgxpool.Config) { cfg.MaxConns = 4 })
	e.t.Cleanup(pool.Close)
	var who string
	if err := pool.QueryRow(ctxb(), "SELECT current_user").Scan(&who); err != nil || who != coreMemberRole {
		e.t.Fatalf("core role pool runs as %q (%v), want %s", who, err, coreMemberRole)
	}
	return pool
}

// cleanAuditRows 测试结束时清掉本用例写入的审计行。分区不动，由 newEnv 的清理恢复成迁移预建的样子。
func (e *env) cleanAuditRows() {
	e.t.Cleanup(func() {
		if err := e.try("TRUNCATE TABLE " + auditTable); err != nil {
			e.t.Errorf("clean up audit rows: %v", err)
		}
	})
}

// 数据面角色：对父表与每个已挂接的月分区，UPDATE / DELETE / TRUNCATE 一律被数据库拒绝（42501）；INSERT / SELECT 正常；
// 经父表写入的行照常落到对应分区。会话角色各试两种：成员（生产里数据面的登录账号）与属主角色本身。
func TestAuditAppendOnlyForPlaneRole(t *testing.T) {
	e := newEnv(t)
	e.cleanAuditRows()
	month := e.utcMonth()

	// 写入与读取：数据面的真实写入语句；经父表写入的行落到对应月份的分区。
	clusterID := e.cluster(clusterSpec{})
	if err := NewAuditRepo(e.d).Insert(ctxb(), &biz.AuditRecord{
		ClusterID: clusterID, Action: k8scontract.ActionExec, Result: k8scontract.AuditOK,
	}); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}
	if n := e.count("SELECT count(*) FROM "+auditTable+" WHERE cluster_id = $1::uuid AND action = 'exec'", clusterID); n != 1 {
		t.Fatalf("%d audit rows reachable through the parent, want 1", n)
	}
	for i := 0; i <= migratedAuditAhead; i++ {
		at, want := month.AddDate(0, i, 0), partitionName(month.AddDate(0, i, 0))
		if got := e.audit(at.Add(12 * time.Hour)); got != want {
			t.Fatalf("audit for month +%d landed in %s, want %s", i, got, want)
		}
	}
	total := e.count("SELECT count(*) FROM " + auditTable)

	// 改写 / 删除 / 清空：父表与每个月分区。upsert 要有列的 UPDATE 权限，同样被拒——不能以插入的形式改写已有的行。
	stmts := auditWrites(auditTable)
	for _, p := range e.monthlyPartitionNames() {
		maps.Copy(stmts, auditWrites(p))
	}
	stmts["upsert k8s_audit"] = fmt.Sprintf(`INSERT INTO k8s_audit (id, action, result) VALUES ('%s', 'update', 1)
		ON CONFLICT (id, create_time) DO UPDATE SET username = 'x'`, e.id())
	for _, who := range planeIdentities() {
		e.wantDenied(who, e.asPlane(who, stmts))
		e.wantOK(who, e.asPlane(who, e.auditInsertSelect()))
	}
	// 行一条不少、一个没改。
	if n := e.count("SELECT count(*) FROM " + auditTable); n != total {
		t.Fatalf("%d audit rows after the denied statements, want %d", n, total)
	}
	if n := e.count("SELECT count(*) FROM " + auditTable + " WHERE username <> ''"); n != 0 {
		t.Fatalf("%d audit rows were rewritten", n)
	}

	// 目录视角：属主角色在这些表上恰好只剩 SELECT / INSERT（DEFAULT 另有搬移行用的 DELETE）。
	for _, tb := range append([]string{auditTable}, e.auditPartitionNames()...) {
		for _, priv := range []string{"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"} {
			want := priv == "SELECT" || priv == "INSERT" || (priv == "DELETE" && tb == auditDefault)
			if got := e.hasPrivilege(planeRole, tb, priv); got != want {
				t.Errorf("%s: %s has %s = %v, want %v", tb, planeRole, priv, got, want)
			}
		}
	}
}

// DEFAULT 分区的例外：保留 DELETE——新月份的分区建好时要把此前落进去的行搬过去（ensurePartition）；UPDATE / TRUNCATE
// 同样被拒；落在 DEFAULT 里的行经父表也删不掉、改不了。
func TestAuditAppendOnlyDefaultPartitionKeepsDelete(t *testing.T) {
	e := newEnv(t)
	e.cleanAuditRows()
	// 没有对应分区的月份：行落进 DEFAULT。
	month := e.utcMonth().AddDate(0, 9, 0)
	lo, hi := month.Format(partitionBoundLayout), month.AddDate(0, 1, 0).Format(partitionBoundLayout)
	id := e.id()
	if table := e.insertAuditRow(id, month.Add(12*time.Hour)); table != auditDefault {
		t.Fatalf("audit without a partition landed in %s, want %s", table, auditDefault)
	}
	for _, who := range planeIdentities() {
		e.wantDenied(who, e.asPlane(who, map[string]string{
			"UPDATE k8s_audit_default":   "UPDATE " + auditDefault + " SET username = 'x'",
			"TRUNCATE k8s_audit_default": "TRUNCATE " + auditDefault,
			"DELETE FROM k8s_audit":      "DELETE FROM " + auditTable + " WHERE create_time >= '" + lo + "'",
			"UPDATE k8s_audit":           "UPDATE " + auditTable + " SET username = 'x' WHERE create_time >= '" + lo + "'",
		}))
		// 直接往 DEFAULT 写入也不变：分区约束要求行的 create_time 落在没有月分区覆盖的区间里。
		e.wantOK(who, e.asPlane(who, map[string]string{
			"SELECT FROM k8s_audit_default": "SELECT count(*) FROM " + auditDefault,
			"INSERT INTO k8s_audit_default": fmt.Sprintf("INSERT INTO %s (id, action, result, create_time) VALUES ('%s', 'update', 1, '%s')",
				auditDefault, e.id(), lo),
		}))
	}
	// DELETE 真的可用，且就是搬移语句的读出半边（maintenance.go：DELETE … RETURNING 再插到新分区）。
	var moved int
	if err := e.d.Pool.QueryRow(ctxb(), fmt.Sprintf(
		`WITH moved AS (DELETE FROM %s WHERE create_time >= '%s' AND create_time < '%s' RETURNING id) SELECT count(*) FROM moved`,
		auditDefault, lo, hi)).Scan(&moved); err != nil {
		t.Fatalf("move the row out of %s as the data plane role: %v", auditDefault, err)
	}
	if moved != 1 || e.count("SELECT count(*) FROM "+auditTable+" WHERE id = $1::uuid", id) != 0 {
		t.Fatalf("rows moved out of %s = %d, the row is still reachable = %v; want 1 and gone",
			auditDefault, moved, e.count("SELECT count(*) FROM "+auditTable+" WHERE id = $1::uuid", id) == 1)
	}
}

// 新建的月分区同样受限：数据面经真实的 EnsureAuditPartitions 建分区（完整路径，以及接续上一轮留下的半成品）之后，
// 新分区的权限与迁移预建的分区一致，DEFAULT 里的行照常搬进新分区，搬过去的行在月分区里不可改、不可删。
func TestAuditAppendOnlyCoversNewPartitions(t *testing.T) {
	e := newEnv(t)
	e.cleanAuditRows()
	repo := NewMaintenanceRepo(e.d)
	ctx := ctxb()
	month := e.utcMonth()
	// +5 走「建表 → 迁出 → 挂接」的完整路径；+6 在维护跑之前先以数据面身份建好独立表（上一轮建表后、挂接前中断的
	// 半成品——属主是登录账号），这一轮接续。
	fresh, resumed := month.AddDate(0, 5, 0), month.AddDate(0, 6, 0)
	if e.attached(fresh) || e.attached(resumed) || e.tableExists(partitionName(resumed)) {
		t.Fatalf("precondition: months +%d and +%d must have no partition yet", 5, 6)
	}
	// 维护失效期间落进 DEFAULT 的行：两个月各两条（区间的起点附近与终点），另有一条不在本次范围内、应留在 DEFAULT。
	const perMonth = 2
	var movedIDs []string
	for _, m := range []time.Time{fresh, resumed} {
		for _, at := range []time.Time{m.Add(3 * time.Hour), m.AddDate(0, 1, 0).Add(-time.Microsecond)} {
			id := e.id()
			if table := e.insertAuditRow(id, at); table != auditDefault {
				t.Fatalf("audit at %s landed in %s, want %s", at.Format(time.RFC3339Nano), table, auditDefault)
			}
			movedIDs = append(movedIDs, id)
		}
	}
	stray := e.id()
	if table := e.insertAuditRow(stray, fresh.AddDate(0, 2, 0)); table != auditDefault {
		t.Fatalf("audit out of range landed in %s, want %s", table, auditDefault)
	}
	totalBefore := e.count("SELECT count(*) FROM " + auditTable)
	if _, err := e.d.Pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+partitionName(resumed)+
		" (LIKE "+auditTable+" INCLUDING DEFAULTS INCLUDING CONSTRAINTS)"); err != nil {
		t.Fatalf("create the half-made partition table: %v", err)
	}
	if err := repo.EnsureAuditPartitions(ctx, 6); err != nil {
		t.Fatalf("EnsureAuditPartitions(6) error = %v", err)
	}

	// 功能：两个分区都挂上了，属主对齐父表，各自区间里的行从 DEFAULT 搬了过来，总量不变，经父表仍可查。
	for _, m := range []time.Time{fresh, resumed} {
		name := partitionName(m)
		if !e.attached(m) {
			t.Fatalf("%s must be attached", name)
		}
		if n := e.count("SELECT count(*) FROM " + name); n != perMonth {
			t.Fatalf("%d rows in %s, want %d", n, name, perMonth)
		}
		if n := e.count("SELECT count(*) FROM "+name+" WHERE create_time >= $1 AND create_time < $2", m, m.AddDate(0, 1, 0)); n != perMonth {
			t.Fatalf("%d rows of this month's range in %s, want %d", n, name, perMonth)
		}
		if e.count(`SELECT count(*) FROM pg_class c, pg_class p WHERE c.oid = to_regclass($1) AND p.oid = $2::regclass
			AND c.relowner = p.relowner`, name, auditTable) != 1 {
			t.Fatalf("%s must be owned by the owner of %s", name, auditTable)
		}
	}
	if d, tot := e.count("SELECT count(*) FROM "+auditDefault), e.count("SELECT count(*) FROM "+auditTable); d != 1 || tot != totalBefore {
		t.Fatalf("rows in DEFAULT = %d (want 1), total = %d (want %d)", d, tot, totalBefore)
	}
	for _, id := range movedIDs {
		if e.count("SELECT count(*) FROM "+auditTable+" WHERE id = $1::uuid", id) != 1 {
			t.Fatalf("audit %s must stay reachable through the parent table", id)
		}
	}
	if got := e.audit(fresh.Add(48 * time.Hour)); got != partitionName(fresh) {
		t.Fatalf("an audit for the new month landed in %s, want %s", got, partitionName(fresh))
	}

	// 权限：新分区与迁移预建的分区一样只增不改——直接对新分区改 / 删 / 清空，或经父表改 / 删搬过去的行，都被拒绝。
	// 迁移 0092 只处理了它执行时已存在的分区，新分区的 ACL 得靠分区维护自己保证；默认权限帮不上忙——带 IN SCHEMA 的
	// REVOKE 建不出条目，而且默认权限只按建表时的角色匹配，覆盖不到「登录账号建表 → 属主对齐」这条路径。
	// 条目数一并打出，排查时不用再查目录。
	e.t.Logf("pg_default_acl entries for %s: %d", planeRole,
		e.count("SELECT count(*) FROM pg_default_acl WHERE defaclrole = $1::regrole", planeRole))
	quoted := make([]string, len(movedIDs))
	for i, id := range movedIDs {
		quoted[i] = "'" + id + "'"
	}
	byID := " WHERE id IN (" + strings.Join(quoted, ", ") + ")"
	stmts := map[string]string{
		"UPDATE k8s_audit (moved rows)": "UPDATE " + auditTable + " SET username = 'x'" + byID,
		"DELETE k8s_audit (moved rows)": "DELETE FROM " + auditTable + byID,
	}
	for _, m := range []time.Time{fresh, resumed} {
		maps.Copy(stmts, auditWrites(partitionName(m)))
	}
	for _, who := range planeIdentities() {
		e.wantDenied(who, e.asPlane(who, stmts))
	}
	for _, tb := range []string{partitionName(fresh), partitionName(resumed)} {
		for _, priv := range []string{"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"} {
			want := priv == "SELECT" || priv == "INSERT"
			if got := e.hasPrivilege(planeRole, tb, priv); got != want {
				t.Errorf("%s (new partition): %s has %s = %v, want %v", tb, planeRole, priv, got, want)
			}
		}
	}
	// 收尾全量扫描：挂在父表上的每一个月分区——迁移预建的、这次新建的、接续的——对数据面角色都是只增不改。
	sweep := map[string]string{}
	for _, p := range e.monthlyPartitionNames() {
		maps.Copy(sweep, auditWrites(p))
	}
	for _, who := range planeIdentities() {
		e.wantDenied(who, e.asPlane(who, sweep))
	}
	if n := e.count("SELECT count(*) FROM " + auditTable + " WHERE username <> ''"); n != 0 {
		t.Fatalf("%d audit rows were rewritten", n)
	}
}

// core 角色：对父表仍然只有 SELECT / INSERT——追加审计（集群与授权的配置变更）与列表 / 详情查询可用，新分区出现后
// 也不需要分区上的授权；UPDATE / DELETE / TRUNCATE 被拒，直接对各分区的写语句同样被拒。
func TestAuditAppendOnlyForCoreRole(t *testing.T) {
	e := newEnv(t)
	e.cleanAuditRows()
	core := e.asCoreRole()
	ctx := ctxb()
	if err := NewMaintenanceRepo(e.d).EnsureAuditPartitions(ctx, 5); err != nil {
		t.Fatalf("EnsureAuditPartitions(5) error = %v", err)
	}
	clusterID, user, auditID := e.cluster(clusterSpec{}), e.id(), e.id()

	// 追加审计（core/internal/data/k8s/provider.go 的 insertAudit：与配置变更同事务）。
	tx, err := core.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `INSERT INTO k8s_audit (id, user_id, username, client_ip, request_id, cluster_id, cluster_name, resource, name,
		action, result, patch) VALUES ($1, $2::uuid, 'ops', '10.0.0.9', 'req-0001', $3::uuid, 'c', 'cluster', 'c-1', 'update', 1, $4::jsonb)`,
		auditID, user, clusterID, `{"enabled":false}`); err != nil {
		t.Fatalf("insert an audit as the core role: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	// 列表与详情（core/internal/data/k8s/audit.go 的语句形态）；行经父表写入，落在当月分区。
	var total int
	if err := core.QueryRow(ctx, "SELECT count(*) FROM "+auditTable+
		" WHERE create_time >= now() - interval '1 day' AND create_time <= now() + interval '1 day' AND cluster_id = $1::uuid", clusterID).Scan(&total); err != nil {
		t.Fatalf("list audits as the core role: %v", err)
	}
	if total != 1 {
		t.Fatalf("the core role sees %d audit rows of the cluster, want 1", total)
	}
	var got string
	if err := core.QueryRow(ctx, "SELECT id::text FROM "+auditTable+" WHERE id = $1 LIMIT 1", auditID).Scan(&got); err != nil || got != auditID {
		t.Fatalf("get the audit as the core role = %q, %v; want %s", got, err, auditID)
	}
	if n := e.count("SELECT count(*) FROM "+partitionName(e.utcMonth())+" WHERE id = $1::uuid", auditID); n != 1 {
		t.Fatalf("%d rows of the core audit in the current-month partition, want 1", n)
	}

	// 父表与每个分区（含 DEFAULT 与新分区）上的改写 / 删除 / 清空。
	stmts := auditWrites(auditTable)
	for _, p := range e.auditPartitionNames() {
		maps.Copy(stmts, auditWrites(p))
	}
	e.wantDenied(coreMemberRole, stmtCodes(t, core, "", stmts))
	e.wantOK(coreMemberRole, stmtCodes(t, core, "", e.auditInsertSelect()))
	for _, priv := range []string{"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"} {
		want := priv == "SELECT" || priv == "INSERT"
		if got := e.hasPrivilege(coreRole, auditTable, priv); got != want {
			t.Errorf("%s: %s has %s = %v, want %v", auditTable, coreRole, priv, got, want)
		}
	}
}
