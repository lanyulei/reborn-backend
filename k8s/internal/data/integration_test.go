//go:build integration

// k8s 数据面的连库集成测试（真实 PostgreSQL，跑的是 data 层实际执行的 SQL）。
//
// 用法（库须已应用 core/migrations 的全部迁移；库名须含 "test"）：
//
//	TEST_DATABASE_SOURCE='postgres://…/reborn_test?…' \
//	go test -tags=integration -p 1 -count=1 ./internal/data/...
//
// 每个测试开头 TRUNCATE k8s_* 表（配置版本表只重置版本号），审计分区恢复成迁移预建的样子（当月至未来
// 2 个月 + DEFAULT）；测试里新建的分区在测试结束时 DROP。包内测试串行；与其它模块共用测试库时务必带
// -p 1，并行会互相清掉对方的数据。未设置 TEST_DATABASE_SOURCE 时整体跳过。
//
// 仓储全部跑在 k8s_plane_role 成员的连接上（见 newEnv）：超出迁移 0072 / 0073 / 0074 / 0077 / 0078
// 授权范围的语句被数据库直接拒绝，所以这里的通过意味着「数据面的 DB 角色确实能执行这些 SQL」。
// 审计表只增不改的权限矩阵（迁移 0092）见 audit_acl_integration_test.go。

package data

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"
	"github.com/lanyulei/reborn/backend/kit/secretbox"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// planeRole 数据面的 DB 角色（NOLOGIN 组角色，迁移 0072）；memberRole 是它的成员——集成测试的
	// 全部仓储都跑在「会话角色 = memberRole」的连接上，它自身没有任何授权。
	planeRole  = "k8s_plane_role"
	memberRole = "k8s_plane_it_member"
	// migratedAuditAhead 迁移 0072 预建的审计月分区数（当月至未来 2 个月）。
	migratedAuditAhead = 2
	// planeStatementTimeout 数据面连接池的语句超时（毫秒），与 kit/dbx 的缺省值一致。
	planeStatementTimeout = "120000"
	// testMasterKey 集成测试里密封凭据用的测试主密钥，与任何真实配置无关。
	testMasterKey = "k8s-integration-master-key"
)

// env 一次集成测试的环境：admin 是 TEST_DATABASE_SOURCE 的原始账号（建配置数据、改时间、断言——相当于
// core 与迁移的合体）；d 是数据面视角的连接池（会话角色 = planeRole 的成员）。
type env struct {
	t     *testing.T
	admin *pgxpool.Pool
	d     *Data
	trace *queryTracer
}

// newEnv 连接测试库并清空 k8s_* 表。未配置 TEST_DATABASE_SOURCE 时跳过；库名须含 "test"。
func newEnv(t *testing.T) *env {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_SOURCE")
	if dsn == "" {
		t.Skip("TEST_DATABASE_SOURCE not set, skipping data integration tests")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse TEST_DATABASE_SOURCE: %v", err)
	}
	if db := cfg.ConnConfig.Database; !strings.Contains(db, "test") {
		t.Fatalf("TEST_DATABASE_SOURCE 指向库 %q：库名须含 \"test\"", db)
	}
	ctx := context.Background()
	admin := newPoolAs(t, "", func(cfg *pgxpool.Config) { cfg.MaxConns = 8 })
	t.Cleanup(admin.Close)

	// 角色是集群级对象：不存在才建，用完即删。清理顺序为后进先出——先关数据面连接池，再删角色，最后关 admin。
	ensureMemberRole(ctx, t, admin, memberRole, planeRole)

	trace := &queryTracer{}
	plane := newPoolAs(t, memberRole, func(cfg *pgxpool.Config) {
		cfg.MaxConns = 16
		// 语句超时与生产连接池一致。
		cfg.ConnConfig.RuntimeParams["statement_timeout"] = planeStatementTimeout
		cfg.ConnConfig.Tracer = trace
	})
	t.Cleanup(plane.Close)
	var who string
	if err := plane.QueryRow(ctx, "SELECT current_user").Scan(&who); err != nil || who != memberRole {
		t.Fatalf("data plane pool runs as %q (%v), want %s", who, err, memberRole)
	}

	e := &env{t: t, admin: admin, d: &Data{Pool: plane}, trace: trace}
	e.exec(`TRUNCATE TABLE k8s_cluster, k8s_agent_token, k8s_role_scope, k8s_helm_repo, k8s_instance, k8s_cluster_runtime, k8s_audit,
		k8s_exec_session, k8s_exec_record, k8s_exec_ticket, k8s_notify_outbox, k8s_event, k8s_inspection_run, k8s_inspection_finding,
		k8s_kube_token, k8s_change, k8s_setting CASCADE`)
	e.exec("UPDATE k8s_cfg_version SET version = 1")
	t.Cleanup(func() {
		if err := e.resetAuditPartitions(); err != nil {
			t.Errorf("restore audit partitions: %v", err)
		}
	})
	if err := e.resetAuditPartitions(); err != nil {
		t.Fatalf("restore audit partitions: %v", err)
	}
	return e
}

func ctxb() context.Context { return context.Background() }

// newPoolAs 按 TEST_DATABASE_SOURCE 建连接池；role 非空时在启动参数里设置会话角色：该连接池的每个会话都以 role 的
// 权限执行（生产里各服务用自己的登录账号连库，账号是组角色的成员）。adjust 可再调整配置。连接类错误会带上连接参数：
// 落进测试输出之前去掉口令。
func newPoolAs(t *testing.T, role string, adjust func(*pgxpool.Config)) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(os.Getenv("TEST_DATABASE_SOURCE"))
	if err != nil {
		t.Fatalf("parse TEST_DATABASE_SOURCE: %v", err)
	}
	if role != "" {
		cfg.ConnConfig.RuntimeParams["role"] = role
	}
	if adjust != nil {
		adjust(cfg)
	}
	pool, err := pgxpool.NewWithConfig(ctxb(), cfg)
	if err != nil {
		msg := err.Error()
		if pwd := cfg.ConnConfig.Password; pwd != "" {
			msg = strings.ReplaceAll(msg, pwd, "***")
		}
		t.Fatalf("connect (session role %q): %s", role, msg)
	}
	return pool
}

// ensureMemberRole 建一个测试专用的成员角色：member 加入 NOLOGIN 组角色 group，自身没有任何授权——会话角色为 member 的
// 连接就只有 group 的权限。角色是集群级对象：不存在才建（上一次运行异常退出可能留下），用完即删；之后注册的清理
// （关连接池）先于它执行（后进先出：先关连接池，再删角色）。
func ensureMemberRole(ctx context.Context, t *testing.T, admin *pgxpool.Pool, member, group string) {
	t.Helper()
	if _, err := admin.Exec(ctx, `DO $$ BEGIN
		IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '`+member+`') THEN
			CREATE ROLE `+member+` NOLOGIN IN ROLE `+group+`;
		END IF; END $$`); err != nil {
		t.Fatalf("create member role %s: %v", member, err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(context.Background(), "DROP ROLE IF EXISTS "+member); err != nil {
			t.Errorf("drop member role %s: %v", member, err)
		}
	})
}

// exec 以 admin 身份执行，出错即失败。
func (e *env) exec(sql string, args ...any) {
	e.t.Helper()
	if err := e.try(sql, args...); err != nil {
		e.t.Fatalf("exec %.60q: %v", sql, err)
	}
}

// try 以 admin 身份执行并返回错误（断言「会被数据库拒绝」的语句用）。
func (e *env) try(sql string, args ...any) error {
	e.t.Helper()
	_, err := e.admin.Exec(context.Background(), sql, args...)
	return err
}

// scan 读一行到 dest。
func (e *env) scan(sql string, args []any, dest ...any) {
	e.t.Helper()
	if err := e.admin.QueryRow(context.Background(), sql, args...).Scan(dest...); err != nil {
		e.t.Fatalf("query %.60q: %v", sql, err)
	}
}

// count 返回单值整数查询的结果。
func (e *env) count(sql string, args ...any) int {
	e.t.Helper()
	var n int
	e.scan(sql, args, &n)
	return n
}

func (e *env) id() string {
	e.t.Helper()
	id, err := idgen.NewID()
	if err != nil {
		e.t.Fatal(err)
	}
	return id
}

// dbNow 数据库时钟的当前时间。
func (e *env) dbNow() time.Time {
	e.t.Helper()
	var now time.Time
	e.scan("SELECT now()", nil, &now)
	return now
}

// utcMonth 数据库时钟下的当前 UTC 月（第一天）。
func (e *env) utcMonth() time.Time {
	e.t.Helper()
	now := e.dbNow().UTC()
	return time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
}

// attached 报告某个月的审计分区是否已挂在父表上。
func (e *env) attached(month time.Time) bool {
	e.t.Helper()
	return e.count("SELECT count(*) FROM pg_inherits WHERE inhparent = $1::regclass AND inhrelid = to_regclass($2)",
		auditTable, partitionName(month)) == 1
}

// resetAuditPartitions 把审计分区恢复成「迁移预建的样子」：先 DROP 测试里新建的月分区与中途崩溃留下的
// 同名独立表（只动本服务命名规则内的对象），再补齐缺的预建分区（库若是在更早的月份迁移的）。
// 测试开头与结尾各跑一次，失败不抛出——它也用在清理路径上。
func (e *env) resetAuditPartitions() error {
	keep := map[string]bool{auditDefault: true}
	month := e.utcMonth()
	for i := 0; i <= migratedAuditAhead; i++ {
		keep[partitionName(month.AddDate(0, i, 0))] = true
	}
	rows, err := e.admin.Query(context.Background(),
		`SELECT tablename FROM pg_tables WHERE schemaname = current_schema() AND tablename LIKE 'k8s\_audit\_%'`)
	if err != nil {
		return fmt.Errorf("list audit partitions: %w", err)
	}
	defer rows.Close()
	var drop []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("list audit partitions: %w", err)
		}
		if !keep[name] && partitionPattern.MatchString(name) {
			drop = append(drop, name)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("list audit partitions: %w", err)
	}
	rows.Close()
	for _, name := range drop {
		if _, err := e.admin.Exec(context.Background(), "DROP TABLE IF EXISTS "+pgx.Identifier{name}.Sanitize()); err != nil {
			return fmt.Errorf("drop %s: %w", name, err)
		}
	}
	if err := NewMaintenanceRepo(e.d).EnsureAuditPartitions(context.Background(), migratedAuditAhead); err != nil {
		return fmt.Errorf("prebuilt audit partitions: %w", err)
	}
	return nil
}

// statements 返回数据面连接池到目前为止执行过的语句，并清空记录。
func (e *env) statements() []stmt {
	e.t.Helper()
	e.trace.mu.Lock()
	defer e.trace.mu.Unlock()
	out := e.trace.stmts
	e.trace.stmts = nil
	return out
}

// onStatement 注册钩子：数据面连接池上每条语句结束时装入它的记录（测试据此在某一批之后取消上下文）。
func (e *env) onStatement(fn func(stmt)) {
	e.t.Helper()
	e.trace.mu.Lock()
	defer e.trace.mu.Unlock()
	e.trace.hook = fn
}

// deletedRows 从语句记录里挑出对某张表的 DELETE，返回各自影响的行数。
func deletedRows(stmts []stmt, table string) []int64 {
	prefix := "DELETE FROM " + table + " "
	var out []int64
	for _, s := range stmts {
		if strings.HasPrefix(s.sql, prefix) {
			out = append(out, s.rows)
		}
	}
	return out
}

// stmt 数据面连接池上执行过的一条语句、它影响的行数（非 DML 为 0）与耗时。
type stmt struct {
	sql   string
	rows  int64
	took  time.Duration
	fails bool // 以错误结束
}

// queryTracer 记录数据面连接池执行过的每条语句。「分批删除」「没有过期行时不发 DELETE」这类性质只能从
// 驱动层观察：库里没有现成的逐条语句统计，仓储的返回值也只有总数。
type queryTracer struct {
	mu    sync.Mutex
	stmts []stmt
	hook  func(stmt)
}

// traceInfo TraceQueryStart 交给 TraceQueryEnd 的语句文本与起始时刻的 ctx 值（包内私有类型，不会与其它 ctx 值冲突）。
type (
	sqlKey    struct{}
	traceInfo struct {
		sql   string
		start time.Time
	}
)

// TraceQueryStart implements pgx.QueryTracer.
func (t *queryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, sqlKey{}, traceInfo{sql: data.SQL, start: time.Now()})
}

// TraceQueryEnd implements pgx.QueryTracer.
func (t *queryTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	info, _ := ctx.Value(sqlKey{}).(traceInfo)
	s := stmt{sql: info.sql, took: time.Since(info.start), fails: data.Err != nil}
	if data.Err == nil {
		s.rows = data.CommandTag.RowsAffected()
	}
	t.mu.Lock()
	t.stmts = append(t.stmts, s)
	hook := t.hook
	t.mu.Unlock()
	// 不持锁调用：钩子会反过来读记录、改钩子。
	if hook != nil {
		hook(s)
	}
}

// pgCode 返回错误的 SQLSTATE；不是 PostgreSQL 报的错返回空串。
func pgCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

// seal 用测试主密钥密封一段明文（core 写入 k8s_cluster.credential / k8s_helm_repo.secret 的做法）。
func seal(t *testing.T, plaintext string) []byte {
	t.Helper()
	box, err := secretbox.New(testMasterKey)
	if err != nil {
		t.Fatal(err)
	}
	out, err := box.Seal(plaintext)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// secretBox 测试主密钥对应的解密器。
func secretBox(t *testing.T) *SecretBox {
	t.Helper()
	box, err := secretbox.New(testMasterKey)
	if err != nil {
		t.Fatal(err)
	}
	return &SecretBox{box: box}
}

// clusterSpec 建集群的可调字段；零值 = 一个启用的直连集群。
type clusterSpec struct {
	Mode         int32 // 接入方式，0 = 直连
	Disabled     bool
	Deleted      bool
	ExecDisabled bool
	APIServer    string // 缺省 https://<名称>.example.com:6443
	ProxyURL     string
	Credential   []byte   // 凭据密文；nil = 不保存
	Reasons      []string // 上报到通知的 Warning 事件原因
}

// cluster 建一个集群（core 的写入），返回 ID。
func (e *env) cluster(spec clusterSpec) string {
	e.t.Helper()
	id := e.id()
	name := "c-" + id[len(id)-12:]
	if spec.Mode == 0 {
		spec.Mode = k8scontract.AccessDirect
	}
	// CHECK：Agent 接入的集群必须填 Agent 部署位置（集群内 = 1），直连的为 0。
	location := 0
	if spec.Mode == k8scontract.AccessAgent {
		location = 1
	}
	if spec.APIServer == "" {
		spec.APIServer = "https://" + name + ".example.com:6443"
	}
	if spec.Reasons == nil {
		spec.Reasons = []string{} // text[] NOT NULL：nil 切片会被编码成 NULL
	}
	e.exec(`INSERT INTO k8s_cluster (id, name, display_name, env, access_mode, agent_location, api_server, proxy_url, credential,
		exec_disabled, event_reasons, enabled, delete_time)
		VALUES ($1, $2, $2, 'test', $3, $4, $5, $6, $7, $8, $9::text[], NOT $10, CASE WHEN $11 THEN now() ELSE NULL END)`,
		id, name, spec.Mode, location, spec.APIServer, spec.ProxyURL, spec.Credential, spec.ExecDisabled, spec.Reasons, spec.Disabled, spec.Deleted)
	return id
}

// scope 给一个新建角色在集群的某命名空间上授一个档位（core 的写入），返回角色 ID。
func (e *env) scope(clusterID, namespace string, level int32) string {
	e.t.Helper()
	role := e.id()
	e.exec(`INSERT INTO k8s_role_scope (role_id, cluster_id, namespace, level) VALUES ($1, $2, $3, $4)`, role, clusterID, namespace, level)
	return role
}

// token 签发一枚 Agent 令牌并落库（库里只存密钥的 SHA-256），返回令牌 ID。expiresIn 为 nil = 不过期，
// 负数 = 已经过去；时间以数据库时钟计。
func (e *env) token(clusterID string, kind int32, secret string, expiresIn *time.Duration) string {
	e.t.Helper()
	id := e.id()
	var secs any
	if expiresIn != nil {
		secs = expiresIn.Seconds()
	}
	e.exec(`INSERT INTO k8s_agent_token (id, cluster_id, kind, token_hash, expire_time)
		VALUES ($1, $2, $3, $4, CASE WHEN $5::float8 IS NULL THEN NULL ELSE now() + make_interval(secs => $5) END)`,
		id, clusterID, kind, k8scontract.HashSecret(secret), secs)
	return id
}

// instance 登记一个数据面实例，心跳停在 ago 之前。
func (e *env) instance(id, addr string, clusters int, heartbeatAgo time.Duration) {
	e.t.Helper()
	e.exec(`INSERT INTO k8s_instance (id, addr, clusters, heartbeat_time) VALUES ($1, $2, $3, now() - $4::interval)`,
		id, addr, clusters, heartbeatAgo)
}

// execSpec 终端会话记录的可调字段；零值 = 一条已结束（container_exit）的会话。
type execSpec struct {
	Cluster   string
	Kind      int32          // 缺省容器终端
	Started   time.Duration  // 开始于多久之前
	Ended     *time.Duration // 结束于多久之前；nil = 仍在进行
	Reason    string         // 缺省 container_exit
	Records   int            // 附带的录制块数
	Truncated bool           // 录制不完整标记
}

// execSession 建一条终端会话记录（数据面的写入），返回 ID。
func (e *env) execSession(spec execSpec) string {
	e.t.Helper()
	id := e.id()
	if spec.Kind == 0 {
		spec.Kind = k8scontract.ExecKindShell
	}
	if spec.Reason == "" {
		spec.Reason = k8scontract.ExecEndContainerExit
	}
	var ended any
	if spec.Ended != nil {
		ended = spec.Ended.Seconds()
	}
	e.exec(`INSERT INTO k8s_exec_session (id, user_id, username, client_ip, cluster_id, cluster_name, namespace, pod, container, kind, start_time,
		end_time, end_reason, bytes_in, bytes_out, recorded, record_truncated)
		VALUES ($1, $2, 'ops', '10.0.0.9', $3, 'c', 'default', 'web-1', 'app', $4, now() - $5::interval,
		CASE WHEN $6::float8 IS NULL THEN NULL ELSE now() - make_interval(secs => $6) END, $7, 10, 20, $8, $9)`,
		id, e.id(), spec.Cluster, spec.Kind, spec.Started, ended, spec.Reason, spec.Records > 0, spec.Truncated)
	for seq := 1; seq <= spec.Records; seq++ {
		e.exec(`INSERT INTO k8s_exec_record (session_id, seq, data) VALUES ($1, $2, $3)`, id, seq, []byte(fmt.Sprintf("[%d,\"o\",\"aGk=\"]\n", seq)))
	}
	return id
}

// execSessionsBulk 一次建 count 条已结束的会话，挂在专用的 clusterID 下（不与别的数据混）。ID 由 tag 与序号
// 确定：同一个 tag 可以原样重建出同一批行，用来对比两种清理方式的最终状态。每条会话带 records 块录制；
// tag 记在 container 列里，录制只挂到这一批会话上。
func (e *env) execSessionsBulk(clusterID, tag string, count, records int, endedAgo time.Duration) {
	e.t.Helper()
	e.exec(`INSERT INTO k8s_exec_session (id, user_id, cluster_id, namespace, pod, container, start_time, end_time, end_reason)
		SELECT md5($1 || g::text)::uuid, md5($1 || 'u' || g::text)::uuid, $2::uuid, 'default', 'web-1', $1,
		       now() - make_interval(secs => $3) - interval '1 hour', now() - make_interval(secs => $3), 'container_exit'
		FROM generate_series(1, $4::int) g`, tag, clusterID, endedAgo.Seconds(), count)
	if records > 0 {
		e.exec(`INSERT INTO k8s_exec_record (session_id, seq, data)
			SELECT s.id, g, '\x00ff'::bytea FROM k8s_exec_session s, generate_series(1, $1::int) g
			WHERE s.cluster_id = $2::uuid AND s.container = $3`, records, clusterID, tag)
	}
}

// audit 以数据面身份写一条审计，返回它实际落在哪张表。
func (e *env) audit(at time.Time) string {
	e.t.Helper()
	return e.insertAuditRow(e.id(), at)
}

// insertAuditRow 以数据面身份经父表写入一条指定 ID 与时间的审计，返回它实际落在哪张表。
func (e *env) insertAuditRow(id string, at time.Time) string {
	e.t.Helper()
	var table string
	if err := e.d.Pool.QueryRow(ctxb(),
		`INSERT INTO k8s_audit (id, action, result, create_time) VALUES ($1, 'update', 1, $2) RETURNING tableoid::regclass::text`,
		id, at).Scan(&table); err != nil {
		e.t.Fatalf("insert audit: %v", err)
	}
	return table
}
