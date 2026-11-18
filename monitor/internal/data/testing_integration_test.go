//go:build integration

package data

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/monitor/internal/biz"
	"github.com/lanyulei/reborn/backend/monitor/internal/conf"

	"github.com/jackc/pgx/v5/pgxpool"
)

// engineRole 引擎的 DB 角色（NOLOGIN 组角色）；loginRole 是部署时的引擎登录账号的替身——一个真实的
// LOGIN 角色，仅仅是 engineRole 的成员，自身没有任何授权、不是超级用户。集成测试的全部仓储都以它
// 登录（密码认证，不是超级用户会话里 SET ROLE）：任何超出迁移 0069 授权范围的读写都会直接报
// permission denied，分区维护也以「成员登录账号」的身份验证。
const (
	engineRole = "monitor_engine_role"
	loginRole  = "monitor_engine_it_login"
)

// env 一次集成测试的环境：admin 是 TEST_DATABASE_SOURCE 的原始账号（建配置数据、改时间、断言）；
// d 是引擎视角的连接池（以 loginRole 登录）。
type env struct {
	t     *testing.T
	admin *pgxpool.Pool
	d     *Data
}

// testLogger 缺省丢弃日志；排查失败时设 MONITOR_IT_LOG=1 把引擎日志打到标准错误。
func testLogger() *slog.Logger {
	if os.Getenv("MONITOR_IT_LOG") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// dropLoginRole 删除登录角色。它建的分区表属主都已对齐到 engineRole；万一上次测试中途崩溃留下它
// 名下的表，先移交再删。
func dropLoginRole(ctx context.Context, admin *pgxpool.Pool) error {
	var exists bool
	if err := admin.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)", loginRole).Scan(&exists); err != nil || !exists {
		return err
	}
	if _, err := admin.Exec(ctx, "REASSIGN OWNED BY "+loginRole+" TO "+engineRole); err != nil {
		return err
	}
	_, err := admin.Exec(ctx, "DROP ROLE "+loginRole)
	return err
}

// newEnv 连接测试库并清空 monitor 运行数据与配置数据（保留根分组与配置版本行）。
// 未配置 TEST_DATABASE_SOURCE 时跳过；库名须含 "test"。
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
	cfg.MaxConns = 8
	admin, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(admin.Close)

	// 角色是集群级对象：每个测试新建（随机口令，只存在于本进程内存），用完即删。清理顺序为后进先出——
	// 先关引擎连接池，再删角色，最后关 admin。
	secret := make([]byte, 18)
	if _, err := rand.Read(secret); err != nil {
		t.Fatal(err)
	}
	password := hex.EncodeToString(secret)
	if err := dropLoginRole(ctx, admin); err != nil {
		t.Fatalf("drop stale login role: %v", err)
	}
	// 口令是本进程生成的十六进制串，直接拼进语句是安全的（CREATE ROLE 不接受绑定参数）。
	if _, err := admin.Exec(ctx, "CREATE ROLE "+loginRole+" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD '"+password+"' IN ROLE "+engineRole); err != nil {
		t.Fatalf("create login role: %v", err)
	}
	t.Cleanup(func() {
		if err := dropLoginRole(context.Background(), admin); err != nil {
			t.Errorf("drop login role: %v", err)
		}
	})

	engineCfg, _ := pgxpool.ParseConfig(dsn)
	engineCfg.MaxConns = 16
	engineCfg.ConnConfig.User, engineCfg.ConnConfig.Password = loginRole, password
	engine, err := pgxpool.NewWithConfig(ctx, engineCfg)
	if err != nil {
		t.Fatalf("connect as the engine login role: %v", err)
	}
	t.Cleanup(engine.Close)
	var (
		session, current string
		super            bool
	)
	if err := engine.QueryRow(ctx, "SELECT session_user, current_user, rolsuper FROM pg_roles WHERE rolname = session_user").
		Scan(&session, &current, &super); err != nil || session != loginRole || current != loginRole || super {
		t.Fatalf("engine pool is logged in as %q / %q (superuser = %v, err = %v), want %s", session, current, super, err, loginRole)
	}

	e := &env{t: t, admin: admin, d: &Data{Pool: engine}}
	e.exec(`TRUNCATE TABLE monitor_notify_log, monitor_notify_outbox, monitor_eval_record, monitor_alert, monitor_eval_state,
		monitor_eval_task, monitor_mute, monitor_subscription, monitor_role_scope, monitor_rule_version, monitor_rule, monitor_datasource,
		monitor_escalation, monitor_inhibit, monitor_record_rule, monitor_record_task, monitor_ci_index, monitor_alert_event,
		monitor_alert_cmd, monitor_alert_escalation, monitor_alert_action, monitor_engine_instance, monitor_engine_event CASCADE`)
	e.exec("UPDATE monitor_calendar SET config = '{}'")
	e.exec("DELETE FROM monitor_rule_group WHERE id <> $1", monitorcontract.RootGroupID)
	e.exec("UPDATE monitor_cfg_version SET version = 1")
	return e
}

func (e *env) exec(sql string, args ...any) {
	e.t.Helper()
	if _, err := e.admin.Exec(context.Background(), sql, args...); err != nil {
		e.t.Fatalf("exec %.60q: %v", sql, err)
	}
}

// count 返回单值整数查询的结果。
func (e *env) count(sql string, args ...any) int {
	e.t.Helper()
	var n int
	if err := e.admin.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		e.t.Fatalf("count %.60q: %v", sql, err)
	}
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

// datasource 建一个启用的无认证数据源，返回 ID。
func (e *env) datasource(url string) string {
	e.t.Helper()
	id := e.id()
	e.exec("INSERT INTO monitor_datasource (id, name, url, timeout_ms) VALUES ($1, $2, $3, 2000)", id, "ds-"+id[len(id)-12:], url)
	e.exec("UPDATE monitor_cfg_version SET version = version + 1 WHERE name = $1", monitorcontract.CfgDatasource)
	return id
}

// ruleSpec 测试规则的可调字段；零值 = 一条 `up == 0` 警告查询、立即触发、全部数据源。
type ruleSpec struct {
	GroupID       string
	Queries       []monitorcontract.Query
	Labels        map[string]string
	Annotations   map[string]string
	DatasourceIDs []string // 空 = 全部启用的数据源
	NoInhibit     bool
	ForS          int
	RecoverS      int
	NodataKeepS   int
	RecoveryExpr  string
	NoRecovery    bool
	RepeatM       int
	MaxNotify     int
	Disabled      bool
}

// rule 建一条规则，返回 ID。
func (e *env) rule(spec ruleSpec) string {
	e.t.Helper()
	id := e.id()
	if spec.GroupID == "" {
		spec.GroupID = monitorcontract.RootGroupID
	}
	if spec.Queries == nil {
		spec.Queries = []monitorcontract.Query{{Expr: "up == 0", Severity: monitorcontract.SeverityWarning}}
	}
	mustJSON := func(v any, empty string) string {
		b, err := json.Marshal(v)
		if err != nil {
			e.t.Fatal(err)
		}
		if string(b) == "null" {
			return empty
		}
		return string(b)
	}
	scope := monitorcontract.DatasourceScopeAll
	if len(spec.DatasourceIDs) > 0 {
		scope = monitorcontract.DatasourceScopeSpecified
	}
	if spec.DatasourceIDs == nil {
		spec.DatasourceIDs = []string{}
	}
	e.exec(`INSERT INTO monitor_rule (id, group_id, name, enabled, labels, datasource_scope, datasource_ids, queries, level_inhibit,
		  eval_interval_s, for_s, recover_s, nodata_keep_s, annotations, recovery_expr, notify_recovery, repeat_interval_m, max_notify)
		VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7::uuid[], $8::jsonb, $9, 15, $10, $11, $12, $13::jsonb, $14, $15, $16, $17)`,
		id, spec.GroupID, "rule-"+id[len(id)-12:], !spec.Disabled, mustJSON(spec.Labels, "{}"), scope, spec.DatasourceIDs,
		mustJSON(spec.Queries, "[]"), !spec.NoInhibit, spec.ForS, spec.RecoverS, spec.NodataKeepS, mustJSON(spec.Annotations, "{}"),
		spec.RecoveryExpr, !spec.NoRecovery, spec.RepeatM, spec.MaxNotify)
	return id
}

// countObserver 只数评估次数（按结果）。
type countObserver struct {
	evals  atomic.Int64
	failed atomic.Int64
}

func (o *countObserver) ObserveEval(_ context.Context, result string, _, _ time.Duration) {
	o.evals.Add(1)
	if result == biz.ResultFailed || result == biz.ResultLeaseLost {
		o.failed.Add(1)
	}
}
func (*countObserver) AddOutbox(context.Context, int32, int)                     {}
func (*countObserver) AddMutesTruncated(context.Context)                         {}
func (*countObserver) AddTruncated(context.Context, string)                      {}
func (*countObserver) AddInhibitCycles(context.Context, int)                     {}
func (*countObserver) ObserveRecord(context.Context, string, time.Duration, int) {}
func (*countObserver) AddCommand(context.Context, string)                        {}
func (*countObserver) AddEngineEvents(context.Context, string, int)              {}

// engine 一个「引擎进程」：配置缓存 + 评估器 + 规划器，全部接真实仓储。每次调用等价于重启一个新进程。
type engine struct {
	e         *env
	cache     *biz.ConfigCache
	evaluator *biz.EvaluatorUsecase
	planner   *biz.PlannerUsecase
	inhibits  *biz.InhibitCache
	stats     *biz.InstanceStats
	commands  *biz.CommandUsecase
	records   *biz.RecordUsecase
	watch     *biz.WatchUsecase
	token     string
	obs       *countObserver
}

func (e *env) engine(masterKey string) *engine {
	e.t.Helper()
	box, err := NewSecretBox(&conf.Notify{Master: &conf.Notify_Master{Key: masterKey}})
	if err != nil {
		e.t.Fatal(err)
	}
	cfg := NewConfigRepo(e.d)
	obs := &countObserver{}
	cache, cleanup := biz.NewConfigCache(cfg, box, obs, testLogger())
	e.t.Cleanup(cleanup)
	// 抑制快照每次都重读：测试里改了活跃告警 / 抑制规则后下一轮评估即生效。
	inhibits := biz.NewInhibitCache(cfg, cache, obs, testLogger()).WithOptions(biz.InhibitOptions{Refresh: time.Nanosecond})
	stats := biz.NewInstanceStats()
	return &engine{
		e: e, cache: cache, token: "it-" + e.id()[24:], obs: obs, inhibits: inhibits, stats: stats,
		evaluator: biz.NewEvaluatorUsecase(NewTaskRepo(e.d), cfg, NewRoundRepo(e.d), cache, inhibits, stats, obs, testLogger()),
		planner:   biz.NewPlannerUsecase(NewPlannerRepo(e.d), cache, obs, testLogger()),
		commands:  biz.NewCommandUsecase(NewCommandRepo(e.d), obs, testLogger()),
		records:   biz.NewRecordUsecase(NewRecordRepo(e.d), cache, stats, obs, testLogger()),
		watch:     biz.NewWatchUsecase(NewSelfmonRepo(e.d), obs, testLogger()),
	}
}

// materialize 跑一轮任务物化。
func (g *engine) materialize() {
	g.e.t.Helper()
	if _, err := g.planner.Materialize(context.Background()); err != nil {
		g.e.t.Fatalf("Materialize() error = %v", err)
	}
}

// evalDue 把全部任务置为到期并评估，直到没有可领取的任务；返回评估的任务数。
func (g *engine) evalDue() int {
	g.e.t.Helper()
	g.e.exec("UPDATE monitor_eval_task SET next_eval_time = now() - interval '1 second' WHERE lease_owner IS NULL")
	total := 0
	for {
		n, err := g.evaluator.RunOnce(context.Background(), g.token)
		if err != nil {
			g.e.t.Fatalf("RunOnce() error = %v", err)
		}
		if n == 0 {
			return total
		}
		total += n
	}
}

// due 把全部未被领取的任务置为到期（新任务的首次评估时刻带抖动）。
func (e *env) due() {
	e.t.Helper()
	e.exec("UPDATE monitor_eval_task SET next_eval_time = now() - interval '1 second' WHERE lease_owner IS NULL")
}

// age 把运行数据的时间整体前移 d（等价于时间过去了 d），用来越过持续时长 / 留观时长 / 重复间隔。
func (e *env) age(d time.Duration) {
	e.t.Helper()
	e.exec(`UPDATE monitor_eval_state SET active_since = active_since - $1::interval, last_seen = last_seen - $1::interval,
		recover_since = recover_since - $1::interval`, d)
	e.exec(`UPDATE monitor_alert SET first_fire_time = first_fire_time - $1::interval, last_fire_time = last_fire_time - $1::interval,
		last_notify_time = last_notify_time - $1::interval`, d)
	e.exec("UPDATE monitor_alert_escalation SET last_notify_time = last_notify_time - $1::interval", d)
	e.exec("UPDATE monitor_notify_outbox SET done_time = done_time - $1::interval WHERE done_time IS NOT NULL", d)
	// 任务行记录的上一轮时刻一并前移：状态机拿它与状态行的 last_seen 比较。
	e.exec("UPDATE monitor_eval_task SET last_eval_time = last_eval_time - $1::interval", d)
}

// asCore 以 core 的 DB 角色（monitor_core_role）执行一条语句：模拟 core 的写入时只能用到它被授权的表与列，
// 写错了列会直接报 permission denied。
func (e *env) asCore(sql string, args ...any) {
	e.t.Helper()
	ctx := context.Background()
	tx, err := e.admin.Begin(ctx)
	if err != nil {
		e.t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SET LOCAL ROLE monitor_core_role"); err != nil {
		e.t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, sql, args...); err != nil {
		e.t.Fatalf("as core: exec %.60q: %v", sql, err)
	}
	if err := tx.Commit(ctx); err != nil {
		e.t.Fatal(err)
	}
}

// dispatch 模拟 core 的 monitor-notify：派发全部待派发的发件箱行并回写进度（只动 core 被授权的列）——
// 触发 / 重复通知计入事件的 notify_count；升级通知（kind 4）计入该级的进度（monitor_alert_escalation，
// notify_count = GREATEST(原值, 级内序号)），不动事件的 notify_count。
func (e *env) dispatch() {
	e.t.Helper()
	e.asCore(`WITH done AS (
		  UPDATE monitor_notify_outbox SET status = $1, done_time = now() WHERE status = $2 RETURNING alert_id, kind, level, seq),
		esc AS (
		  INSERT INTO monitor_alert_escalation (alert_id, level, notify_count, last_notify_time)
		  SELECT alert_id, level, max(seq % $5), now() FROM done WHERE kind = $4 GROUP BY alert_id, level
		  ON CONFLICT (alert_id, level) DO UPDATE
		    SET notify_count = GREATEST(monitor_alert_escalation.notify_count, EXCLUDED.notify_count), last_notify_time = now())
		UPDATE monitor_alert a SET notify_count = a.notify_count + c.n, last_notify_time = now()
		FROM (SELECT alert_id, count(*) AS n FROM done WHERE kind IN ($6, $3) GROUP BY alert_id) c WHERE a.id = c.alert_id`,
		monitorcontract.OutboxDone, monitorcontract.OutboxPending, monitorcontract.NotifyKindRepeat,
		monitorcontract.NotifyKindEscalation, monitorcontract.EscalationSeqBase, monitorcontract.NotifyKindFiring)
}

// outbox 列出某告警的发件箱 "kind/seq/status"（按写入顺序）。
func (e *env) outbox(alertID string) []string {
	e.t.Helper()
	rows, err := e.admin.Query(context.Background(),
		"SELECT kind, seq, status FROM monitor_notify_outbox WHERE alert_id = $1 ORDER BY create_time, kind, seq", alertID)
	if err != nil {
		e.t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var kind, seq, status int
		if err := rows.Scan(&kind, &seq, &status); err != nil {
			e.t.Fatal(err)
		}
		out = append(out, fmt.Sprintf("%d/%d/%d", kind, seq, status))
	}
	return out
}

type alertRow struct {
	ID, Fingerprint, RuleName, DatasourceName, Expr, GroupKey string
	Severity, Status, NotifyCount                             int
	Value, RecoveryValue                                      *float64
	Labels, Annotations                                       map[string]string
	Muted                                                     bool
	MuteID                                                    *string
	FirstFire, LastFire                                       time.Time
	Resolve                                                   *time.Time
}

// alerts 某规则的全部告警（按首次触发时间）。
func (e *env) alerts(ruleID string) []alertRow {
	e.t.Helper()
	rows, err := e.admin.Query(context.Background(),
		`SELECT id, fingerprint, rule_name, datasource_name, expr, group_key, severity, status, notify_count, value, recovery_value,
		        labels, annotations, muted, mute_id::text, first_fire_time, last_fire_time, resolve_time
		 FROM monitor_alert WHERE rule_id = $1 ORDER BY first_fire_time, id`, ruleID)
	if err != nil {
		e.t.Fatal(err)
	}
	defer rows.Close()
	var out []alertRow
	for rows.Next() {
		var a alertRow
		if err := rows.Scan(&a.ID, &a.Fingerprint, &a.RuleName, &a.DatasourceName, &a.Expr, &a.GroupKey, &a.Severity, &a.Status,
			&a.NotifyCount, &a.Value, &a.RecoveryValue, &a.Labels, &a.Annotations, &a.Muted, &a.MuteID, &a.FirstFire, &a.LastFire, &a.Resolve); err != nil {
			e.t.Fatal(err)
		}
		out = append(out, a)
	}
	return out
}

// oneAlert 返回规则唯一的告警。
func (e *env) oneAlert(ruleID string) alertRow {
	e.t.Helper()
	all := e.alerts(ruleID)
	if len(all) != 1 {
		e.t.Fatalf("want exactly 1 alert, got %d", len(all))
	}
	return all[0]
}

type recordRow struct {
	Status, Series, Anomalies, Recovered int
	Error                                string
	Detail                               monitorcontract.EvalDetail
}

// lastRecord 某规则最近一条判定记录。
func (e *env) lastRecord(ruleID string) recordRow {
	e.t.Helper()
	var (
		r   recordRow
		raw []byte
	)
	if err := e.admin.QueryRow(context.Background(),
		`SELECT status, series, anomalies, recovered, error, detail FROM monitor_eval_record
		 WHERE rule_id = $1 ORDER BY eval_time DESC, id DESC LIMIT 1`, ruleID).
		Scan(&r.Status, &r.Series, &r.Anomalies, &r.Recovered, &r.Error, &raw); err != nil {
		e.t.Fatalf("read last eval record: %v", err)
	}
	if err := json.Unmarshal(raw, &r.Detail); err != nil {
		e.t.Fatal(err)
	}
	return r
}

type taskRow struct {
	Status, FiringCount int
	Error               string
	Owner               *string
	NextIn              float64 // next_eval_time − now()，秒
}

func (e *env) task(ruleID, datasourceID string) taskRow {
	e.t.Helper()
	var r taskRow
	if err := e.admin.QueryRow(context.Background(),
		`SELECT last_status, firing_count, last_error, lease_owner, extract(epoch FROM next_eval_time - now())::float8
		 FROM monitor_eval_task WHERE rule_id = $1 AND datasource_id = $2`, ruleID, datasourceID).
		Scan(&r.Status, &r.FiringCount, &r.Error, &r.Owner, &r.NextIn); err != nil {
		e.t.Fatalf("read task: %v", err)
	}
	return r
}

func equalStrings(a, b []string) bool { return slices.Equal(a, b) }
