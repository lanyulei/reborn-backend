//go:build integration

package data

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	datanotify "github.com/lanyulei/reborn/backend/core/internal/data/notify"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/kitconf"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	kerrors "github.com/go-kratos/kratos/v3/errors"
	"github.com/jackc/pgx/v5/pgxpool"
)

// 固定的测试用户与角色（监控域不读其他域的表，这些只是合法的 UUID）。
const (
	uAdmin = "018f3e1a-0d00-7000-8000-000000000001"
	uAlice = "018f3e1a-0d00-7000-8000-00000000000a"
	uBob   = "018f3e1a-0d00-7000-8000-00000000000b"
	uCarol = "018f3e1a-0d00-7000-8000-00000000000c"
	roleA  = "018f3e1a-0d01-7000-8000-00000000000a"
	roleB  = "018f3e1a-0d01-7000-8000-00000000000b"
)

type env struct {
	t       *testing.T
	d       *basedata.Data
	pool    *pgxpool.Pool
	groups  *groupRepo
	scopes  *roleScopeRepo
	sources *datasourceRepo
	rules   *ruleRepo
	vers    *ruleVersionRepo
	evals   *evalRepo
	alerts  *alertRepo
	logs    *notifyLogRepo
	mutes   *muteRepo
	subs    *subscriptionRepo
	outbox  *outboxRepo
	org     *testfake.MonitorOrg
}

// testDSN / openData 接收 testing.TB：集成基准（bench_integration_test.go）共用同一份护栏与建池。
func testDSN(t testing.TB) string {
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
	return dsn
}

func openData(t testing.TB, dsn string) *basedata.Data {
	t.Helper()
	d, cleanup, err := basedata.NewData(&conf.Data{Database: &kitconf.Database{Source: dsn}})
	if err != nil {
		t.Fatalf("NewData() error = %v", err)
	}
	t.Cleanup(cleanup)
	return d
}

func reposOf(t *testing.T, d *basedata.Data) *env {
	return &env{
		t: t, d: d, pool: d.Pool,
		groups: NewGroupRepo(d), scopes: NewRoleScopeRepo(d), sources: NewDatasourceRepo(d), rules: NewRuleRepo(d),
		vers: NewRuleVersionRepo(d), evals: NewEvalRepo(d), alerts: NewAlertRepo(d), logs: NewNotifyLogRepo(d),
		mutes: NewMuteRepo(d), subs: NewSubscriptionRepo(d), outbox: NewOutboxRepo(d),
		org: &testfake.MonitorOrg{
			Admins: []string{uAdmin}, UserRoles: map[string][]string{uAlice: {roleA}, uBob: {roleB}},
			Users: map[string]string{uAdmin: "管理员", uAlice: "Alice", uBob: "Bob", uCarol: "Carol"},
		},
	}
}

// newEnv 真实连接池上的全部仓储；未配置 TEST_DATABASE_SOURCE 时跳过。会清空监控域的表
// （根分组与配置版本行保留）。
func newEnv(t *testing.T) *env {
	t.Helper()
	d := openData(t, testDSN(t))
	ctx := context.Background()
	if _, err := d.Pool.Exec(ctx, `TRUNCATE TABLE monitor_notify_log, monitor_notify_outbox, monitor_alert, monitor_eval_state,
		monitor_eval_task, monitor_eval_record, monitor_subscription, monitor_mute, monitor_rule_version, monitor_rule,
		monitor_role_scope, monitor_datasource, notify_event, notify_dedup,
		monitor_escalation, monitor_inhibit, monitor_record_rule, monitor_record_task, monitor_alert_view, monitor_alert_event,
		monitor_alert_cmd, monitor_alert_escalation, monitor_alert_action, monitor_engine_instance, monitor_engine_event,
		monitor_ci_index`); err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
	// 工作日历快照恒一行：复位为「尚未物化」。
	if _, err := d.Pool.Exec(ctx, "UPDATE monitor_calendar SET config = '{}' WHERE id = 1"); err != nil {
		t.Fatalf("reset calendar: %v", err)
	}
	if _, err := d.Pool.Exec(ctx, "DELETE FROM monitor_rule_group WHERE id <> $1", mc.RootGroupID); err != nil {
		t.Fatalf("reset rule groups: %v", err)
	}
	return reposOf(t, d)
}

func newID(t *testing.T) string {
	t.Helper()
	id, err := idgen.NewID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func wantErr(t *testing.T, what string, err error, want *kerrors.Error) {
	t.Helper()
	if !kerrors.Is(err, want) {
		t.Fatalf("%s: err = %v, want %s", what, err, want.Reason)
	}
}

func (e *env) cfgVersion(name string) int64 {
	e.t.Helper()
	var v int64
	if err := e.pool.QueryRow(context.Background(), "SELECT version FROM monitor_cfg_version WHERE name = $1", name).Scan(&v); err != nil {
		e.t.Fatalf("read cfg version: %v", err)
	}
	return v
}

func (e *env) count(sql string, args ...any) int {
	e.t.Helper()
	var n int
	if err := e.pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		e.t.Fatalf("count %q: %v", sql, err)
	}
	return n
}

func (e *env) group(parentID, name string) *biz.RuleGroup {
	e.t.Helper()
	g := &biz.RuleGroup{ID: newID(e.t), ParentID: parentID, Name: name}
	if err := e.groups.Create(context.Background(), g, uAdmin); err != nil {
		e.t.Fatalf("create group %s: %v", name, err)
	}
	return g
}

func (e *env) datasource(name string, isDefault bool) *biz.Datasource {
	e.t.Helper()
	d := &biz.Datasource{
		ID: newID(e.t), Name: name, Type: mc.DatasourceTypePrometheus, URL: "http://" + name + ":8428", AuthType: mc.AuthNone,
		Headers: map[string]string{"X-Scope": "a"}, TimeoutMS: 5000, IsDefault: isDefault, Enabled: true,
	}
	if err := e.sources.Create(context.Background(), d, nil, uAdmin); err != nil {
		e.t.Fatalf("create datasource %s: %v", name, err)
	}
	return d
}

func ruleSpec(groupID, name string) biz.RuleSpec {
	return biz.RuleSpec{
		GroupID: groupID, Name: name, Enabled: true, Labels: map[string]string{"team": "infra"},
		DatasourceScope: mc.DatasourceScopeAll, DatasourceIDs: []string{}, LevelInhibit: true, EvalIntervalS: 60, ForS: 60,
		Queries:     []mc.Query{{Expr: "cpu > 85", Severity: mc.SeverityWarning}, {Expr: "cpu > 95", Severity: mc.SeverityCritical}},
		Effective:   mc.Effective{Timezone: "Asia/Shanghai", Windows: []mc.TimeWindow{{Days: []int32{1, 2}, Start: "09:00", End: "18:00"}}},
		Annotations: map[string]string{"summary": "{{ $value }}"}, RecoveryExpr: "cpu",
		Notify:         mc.NotifyTargets{UserIDs: []string{uAlice}, ScheduleIDs: []string{}, DeptIDs: []string{}},
		NotifyRecovery: true, RepeatIntervalM: 30, MaxNotify: 5, GroupKeyLabels: []string{"instance"},
	}
}

func (e *env) rule(groupID, name string) *biz.Rule {
	e.t.Helper()
	r := &biz.Rule{ID: newID(e.t), Spec: ruleSpec(groupID, name)}
	if err := e.rules.Create(context.Background(), r, biz.RuleChange{Action: mc.RuleActionCreate, Summary: "新建", Operator: uAlice}); err != nil {
		e.t.Fatalf("create rule %s: %v", name, err)
	}
	return r
}

// alertRow 以评估引擎的身份直接写一条告警事件（core 的仓储没有这个写路径）。
type alertRow struct {
	id, ruleID, ruleName, groupID string
	severity, status              int32
	labels                        string
	fired                         time.Time
	muted                         bool
}

func (e *env) alert(a alertRow) string {
	e.t.Helper()
	if a.id == "" {
		a.id = newID(e.t)
	}
	if a.labels == "" {
		a.labels = "{}"
	}
	if a.fired.IsZero() {
		a.fired = time.Now().Add(-5 * time.Minute)
	}
	if a.status == 0 {
		a.status = mc.AlertFiring
	}
	if a.severity == 0 {
		a.severity = mc.SeverityCritical
	}
	var resolve any
	if a.status != mc.AlertFiring {
		resolve = a.fired.Add(time.Minute)
	}
	// 指纹只需在活跃事件之间唯一：取事件 ID 的后 32 位十六进制。
	fp := strings.ReplaceAll(a.id, "-", "")
	if _, err := e.pool.Exec(context.Background(),
		`INSERT INTO monitor_alert (id, fingerprint, rule_id, rule_name, group_id, datasource_id, datasource_name, severity, status,
		   labels, annotations, expr, value, first_fire_time, last_fire_time, resolve_time, muted, group_key)
		 VALUES ($1, $2, $3, $4, $5, $6, 'vm', $7, $8, $9::jsonb, '{"summary":"CPU 95%"}', 'cpu > 90', 95.5, $10, $10, $11, $12, $13)`,
		a.id, fp, a.ruleID, a.ruleName, a.groupID, uCarol, a.severity, a.status, a.labels, a.fired, resolve, a.muted,
		"monitor:rule:"+a.ruleID); err != nil {
		e.t.Fatalf("seed alert: %v", err)
	}
	return a.id
}

func (e *env) outboxRow(alertID string, kind, seq int32) string {
	e.t.Helper()
	id := newID(e.t)
	if _, err := e.pool.Exec(context.Background(),
		"INSERT INTO monitor_notify_outbox (id, alert_id, kind, seq) VALUES ($1, $2, $3, $4)", id, alertID, kind, seq); err != nil {
		e.t.Fatalf("seed outbox: %v", err)
	}
	return id
}

type outboxState struct {
	status, attempts int32
	lastError        string
	done             *time.Time
}

func (e *env) outboxState(id string) outboxState {
	e.t.Helper()
	var s outboxState
	if err := e.pool.QueryRow(context.Background(),
		"SELECT status, attempts, last_error, done_time FROM monitor_notify_outbox WHERE id = $1", id).
		Scan(&s.status, &s.attempts, &s.lastError, &s.done); err != nil {
		e.t.Fatalf("read outbox: %v", err)
	}
	return s
}

func TestGroupRepo(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	before := e.cfgVersion(mc.CfgRuleGroup)
	infra := e.group("", "基础设施")
	host := e.group(infra.ID, "主机")
	if host.Path != "/"+infra.ID+"/"+host.ID+"/" || e.cfgVersion(mc.CfgRuleGroup) != before+2 {
		t.Fatalf("path = %q cfg = %d (was %d)", host.Path, e.cfgVersion(mc.CfgRuleGroup), before)
	}
	// 同一上级下重名；顶级分组之间同样不能重名（NULLS NOT DISTINCT）。
	wantErr(t, "duplicate child", e.groups.Create(ctx, &biz.RuleGroup{ID: newID(t), ParentID: infra.ID, Name: "主机"}, uAdmin), biz.ErrGroupNameExists)
	wantErr(t, "duplicate top", e.groups.Create(ctx, &biz.RuleGroup{ID: newID(t), Name: "基础设施"}, uAdmin), biz.ErrGroupNameExists)
	wantErr(t, "unknown parent", e.groups.Create(ctx, &biz.RuleGroup{ID: newID(t), ParentID: newID(t), Name: "x"}, uAdmin), biz.ErrGroupNotFound)
	if e.cfgVersion(mc.CfgRuleGroup) != before+2 {
		t.Fatalf("failed writes must not bump the cfg version")
	}
	// 层级上限。
	parent := host.ID
	for depth := 3; depth <= biz.MaxGroupDepth; depth++ {
		parent = e.group(parent, fmt.Sprintf("l%d", depth)).ID
	}
	wantErr(t, "depth limit", e.groups.Create(ctx, &biz.RuleGroup{ID: newID(t), ParentID: parent, Name: "too-deep"}, uAdmin), biz.ErrGroupInvalid)

	e.rule(host.ID, "r1")
	all, err := e.groups.ListAll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	byID := map[string]*biz.RuleGroup{}
	for i, g := range all {
		byID[g.ID] = g
		if i > 0 && len(all[i-1].Path) > len(g.Path) {
			t.Fatalf("parents must come first")
		}
	}
	if byID[host.ID].ParentID != infra.ID || byID[mc.RootGroupID] == nil {
		t.Fatalf("tree = %+v / %+v", byID[infra.ID], byID[host.ID])
	}
	// 分组树不带计数（每次范围解析、每轮派发都要读它）；直属规则数另取，已删除的规则不算。
	gone := e.rule(host.ID, "r-deleted")
	if err := e.rules.Delete(ctx, gone, biz.RuleChange{Action: mc.RuleActionDelete, Operator: uAlice}); err != nil {
		t.Fatal(err)
	}
	counts, err := e.groups.RuleCounts(ctx)
	if err != nil || len(counts) != 1 || counts[host.ID] != 1 {
		t.Fatalf("RuleCounts = %v err=%v, want only host with 1 rule", counts, err)
	}

	wantErr(t, "delete with children", e.groups.Delete(ctx, infra.ID, uAdmin), biz.ErrGroupInUse)
	wantErr(t, "delete with rules", e.groups.Delete(ctx, host.ID, uAdmin), biz.ErrGroupInUse)
	wantErr(t, "delete unknown", e.groups.Delete(ctx, newID(t), uAdmin), biz.ErrGroupNotFound)

	// 改名；与兄弟重名被拒。
	db := e.group(infra.ID, "数据库")
	wantErr(t, "rename onto sibling", e.groups.Update(ctx, &biz.RuleGroup{ID: db.ID, Name: "主机"}, uAdmin), biz.ErrGroupNameExists)
	if err := e.groups.Update(ctx, &biz.RuleGroup{ID: db.ID, Name: "DB", Description: "d", Sort: 3}, uAdmin); err != nil {
		t.Fatal(err)
	}

	// 角色授权：整组替换；分组删除时一并清除；已删除的分组不能再授权。
	if err := e.scopes.Replace(ctx, db.ID, []*biz.RoleScope{{RoleID: roleA, Level: 2}, {RoleID: roleB, Level: 1}}, uAdmin); err != nil {
		t.Fatal(err)
	}
	if err := e.scopes.Replace(ctx, db.ID, []*biz.RoleScope{{RoleID: roleB, Level: 2}}, uAdmin); err != nil {
		t.Fatal(err)
	}
	got, err := e.scopes.ListByGroups(ctx, []string{db.ID, infra.ID})
	if err != nil || len(got) != 1 || got[0].RoleID != roleB || got[0].Level != 2 || got[0].UpdateUser != uAdmin {
		t.Fatalf("role scopes after replace = %+v err=%v", got, err)
	}
	if byRole, _ := e.scopes.ListByRoles(ctx, []string{roleA, roleB}); len(byRole) != 1 {
		t.Fatalf("ListByRoles = %d", len(byRole))
	}
	if err := e.groups.Delete(ctx, db.ID, uAdmin); err != nil {
		t.Fatal(err)
	}
	if n := e.count("SELECT count(*) FROM monitor_role_scope WHERE group_id = $1", db.ID); n != 0 {
		t.Fatalf("grants of a deleted group must be removed: %d", n)
	}
	wantErr(t, "grant on deleted group", e.scopes.Replace(ctx, db.ID, []*biz.RoleScope{{RoleID: roleA, Level: 1}}, uAdmin), biz.ErrGroupNotFound)
	// 软删后同名可重建。
	e.group(infra.ID, "DB")
}

// 规则的写入与分组 / 数据源的删除互斥：并发下不会出现「规则指向已删除的分组」。
func TestRuleWrite_RacesWithGroupDelete(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	for round := range 20 {
		g := e.group("", fmt.Sprintf("race-%d", round))
		var wg sync.WaitGroup
		var createErr, deleteErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			r := &biz.Rule{ID: newID(t), Spec: ruleSpec(g.ID, "r")}
			createErr = e.rules.Create(ctx, r, biz.RuleChange{Action: mc.RuleActionCreate, Operator: uAlice})
		}()
		go func() {
			defer wg.Done()
			deleteErr = e.groups.Delete(ctx, g.ID, uAdmin)
		}()
		wg.Wait()
		// 恰好一方成功：规则先建成则分组删不掉；分组先删掉则规则建不成。
		switch {
		case createErr == nil && kerrors.Is(deleteErr, biz.ErrGroupInUse):
		case deleteErr == nil && kerrors.Is(createErr, biz.ErrGroupNotFound):
		default:
			t.Fatalf("round %d: create = %v, delete = %v", round, createErr, deleteErr)
		}
	}
	if n := e.count(`SELECT count(*) FROM monitor_rule r JOIN monitor_rule_group g ON g.id = r.group_id
		WHERE r.delete_time IS NULL AND g.delete_time IS NOT NULL`); n != 0 {
		t.Fatalf("%d rules point at deleted groups", n)
	}
}

func TestDatasourceRepo(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	box, err := NewSecretBox(&conf.Notify{Master: &kitconf.Master{Key: "integration-test-master-key"}})
	if err != nil {
		t.Fatal(err)
	}
	before := e.cfgVersion(mc.CfgDatasource)
	sealed, err := box.Seal("tok-plain-123")
	if err != nil {
		t.Fatal(err)
	}
	d := &biz.Datasource{
		ID: newID(t), Name: "vm", Type: mc.DatasourceTypePrometheus, URL: "http://vm:8428", AuthType: mc.AuthBearer,
		Headers: map[string]string{"X-Scope": "a"}, TimeoutMS: 5000, Description: "生产", IsDefault: true, Enabled: true,
	}
	if err := e.sources.Create(ctx, d, sealed, uAdmin); err != nil {
		t.Fatal(err)
	}
	got, err := e.sources.Get(ctx, d.ID)
	if err != nil || !got.HasSecret || got.Version != 1 || got.Headers["X-Scope"] != "a" || !got.IsDefault || got.UpdateUser != uAdmin ||
		got.HealthStatus != mc.HealthUnknown || got.Type != mc.DatasourceTypePrometheus {
		t.Fatalf("get = %+v err=%v", got, err)
	}
	// 凭据落库的是密文，能用同一主密钥解开；没有任何读路径带出它。
	stored, err := e.sources.GetSecret(ctx, d.ID)
	if err != nil || strings.Contains(string(stored), "tok-plain-123") {
		t.Fatalf("secret must be stored sealed: err=%v", err)
	}
	if plain, err := box.Open(stored); err != nil || plain != "tok-plain-123" {
		t.Fatalf("open sealed secret: %v", err)
	}
	// 主密钥未配置：Seal / Open 返回领域错误。
	off, _ := NewSecretBox(&conf.Notify{})
	if _, err := off.Seal("x"); !kerrors.Is(err, biz.ErrSecretUnavailable) {
		t.Fatalf("Seal without master key: %v", err)
	}
	if _, err := off.Open(stored); !kerrors.Is(err, biz.ErrSecretUnavailable) {
		t.Fatalf("Open without master key: %v", err)
	}
	wrong, _ := NewSecretBox(&conf.Notify{Master: &kitconf.Master{Key: "another-key"}})
	if _, err := wrong.Open(stored); err == nil || strings.Contains(err.Error(), "tok-plain") {
		t.Fatalf("Open with a wrong key must fail without leaking: %v", err)
	}

	dup := *d
	dup.ID = newID(t)
	wantErr(t, "duplicate name", e.sources.Create(ctx, &dup, nil, uAdmin), biz.ErrDatasourceNameExists)
	if e.cfgVersion(mc.CfgDatasource) != before+1 {
		t.Fatalf("cfg version = %d, want %d", e.cfgVersion(mc.CfgDatasource), before+1)
	}

	// 乐观锁；不动凭据 / 换凭据 / 清凭据。
	upd := *d
	upd.Description, upd.Enabled = "改", false
	wantErr(t, "stale version", e.sources.Update(ctx, &upd, biz.SecretChange{}, 9, uAlice), biz.ErrVersionConflict)
	missing := upd
	missing.ID = newID(t)
	wantErr(t, "unknown id", e.sources.Update(ctx, &missing, biz.SecretChange{}, 1, uAlice), biz.ErrDatasourceNotFound)
	if err := e.sources.Update(ctx, &upd, biz.SecretChange{}, 1, uAlice); err != nil {
		t.Fatal(err)
	}
	if got, _ := e.sources.Get(ctx, d.ID); got.Version != 2 || !got.HasSecret || got.Enabled || got.UpdateUser != uAlice {
		t.Fatalf("after update = %+v", got)
	}
	resealed, _ := box.Seal("tok-new")
	if err := e.sources.Update(ctx, &upd, biz.SecretChange{Set: true, Sealed: resealed}, 2, uAlice); err != nil {
		t.Fatal(err)
	}
	if stored, _ := e.sources.GetSecret(ctx, d.ID); string(stored) != string(resealed) {
		t.Fatalf("secret was not replaced")
	}
	if err := e.sources.Update(ctx, &upd, biz.SecretChange{Set: true}, 3, uAlice); err != nil {
		t.Fatal(err)
	}
	if got, _ := e.sources.Get(ctx, d.ID); got.HasSecret || got.Version != 4 {
		t.Fatalf("secret was not cleared: %+v", got)
	}
	// 探活三列归引擎：core 的更新不会覆盖它们。
	if _, err := e.pool.Exec(ctx, "UPDATE monitor_datasource SET health_status = 2, health_error = 'down', health_time = now() WHERE id = $1", d.ID); err != nil {
		t.Fatal(err)
	}
	if err := e.sources.Update(ctx, &upd, biz.SecretChange{}, 4, uAlice); err != nil {
		t.Fatal(err)
	}
	if got, _ := e.sources.Get(ctx, d.ID); got.HealthStatus != mc.HealthFailed || got.HealthError != "down" || got.HealthTime == nil {
		t.Fatalf("health columns must survive a core update: %+v", got)
	}

	// 列表过滤与选项。
	other := e.datasource("prom", false)
	enabled := true
	failed := mc.HealthFailed
	for _, c := range []struct {
		f    biz.DatasourceFilter
		want int64
	}{
		{biz.DatasourceFilter{}, 2},
		{biz.DatasourceFilter{Keyword: "PRO"}, 1},
		{biz.DatasourceFilter{Enabled: &enabled}, 1},
		{biz.DatasourceFilter{HealthStatus: &failed}, 1},
		{biz.DatasourceFilter{Keyword: "100%"}, 0},
	} {
		if _, total, err := e.sources.List(ctx, c.f, 1, 10); err != nil || total != c.want {
			t.Errorf("List(%+v) = %d, want %d (err=%v)", c.f, total, c.want, err)
		}
	}
	if opts, _ := e.sources.ListOptions(ctx, true); len(opts) != 1 || opts[0].ID != other.ID {
		t.Fatalf("enabled-only options = %d", len(opts))
	}
	if briefs, _ := e.sources.Briefs(ctx, []string{d.ID, other.ID, newID(t)}); len(briefs) != 2 || briefs[d.ID].Enabled || !briefs[other.ID].Enabled {
		t.Fatalf("briefs = %+v", briefs)
	}

	// 删除：被规则明确指定时拒绝并带上规则名；范围为全部数据源的规则不算。
	g := e.group("", "g")
	e.rule(g.ID, "global-rule")
	spec := ruleSpec(g.ID, "pinned")
	spec.DatasourceScope, spec.DatasourceIDs = mc.DatasourceScopeSpecified, []string{d.ID}
	pinned := &biz.Rule{ID: newID(t), Spec: spec}
	if err := e.rules.Create(ctx, pinned, biz.RuleChange{Action: mc.RuleActionCreate, Operator: uAlice}); err != nil {
		t.Fatal(err)
	}
	if got, _ := e.sources.Get(ctx, d.ID); got.RuleCount != 1 {
		t.Fatalf("rule_count = %d, want 1 (global-scope rules do not count)", got.RuleCount)
	}
	err = e.sources.Delete(ctx, d.ID, uAdmin)
	wantErr(t, "delete referenced", err, biz.ErrDatasourceInUse)
	if !strings.Contains(err.Error(), "pinned") || strings.Contains(err.Error(), "global-rule") {
		t.Fatalf("in-use message = %v", err)
	}
	if err := e.sources.Delete(ctx, other.ID, uAdmin); err != nil {
		t.Fatalf("delete unreferenced: %v", err)
	}
	if err := e.rules.Delete(ctx, pinned, biz.RuleChange{Action: mc.RuleActionDelete, Operator: uAlice}); err != nil {
		t.Fatal(err)
	}
	reseal, _ := box.Seal("x")
	if err := e.sources.Update(ctx, &upd, biz.SecretChange{Set: true, Sealed: reseal}, 5, uAlice); err != nil {
		t.Fatal(err)
	}
	if err := e.sources.Delete(ctx, d.ID, uAdmin); err != nil {
		t.Fatalf("delete after the rule is gone: %v", err)
	}
	_, err = e.sources.Get(ctx, d.ID)
	wantErr(t, "deleted", err, biz.ErrDatasourceNotFound)
	// 已删除的数据源不留可解密的凭据。
	if n := e.count("SELECT count(*) FROM monitor_datasource WHERE id = $1 AND secret IS NOT NULL", d.ID); n != 0 {
		t.Fatalf("a deleted datasource must not keep its secret")
	}
	// 规则引用已删除的数据源：写事务内拒绝。
	spec.Name = "dangling"
	err = e.rules.Create(ctx, &biz.Rule{ID: newID(t), Spec: spec}, biz.RuleChange{Action: mc.RuleActionCreate, Operator: uAlice})
	wantErr(t, "rule pointing at a deleted datasource", err, biz.ErrRuleInvalid)
}

// 缺省数据源唯一：顺序与并发下都至多一个，且被取消缺省的那一行版本 +1。
func TestDatasourceRepo_SingleDefault(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	first := e.datasource("d1", true)
	second := e.datasource("d2", true)
	if got, _ := e.sources.Get(ctx, first.ID); got.IsDefault || got.Version != 2 {
		t.Fatalf("previous default = %+v", got)
	}
	// 用旧版本把缺省标记抢回去：版本冲突，且不会顺手把现任缺省清掉。
	back := *first
	back.IsDefault = true
	wantErr(t, "stale version must not steal the default", e.sources.Update(ctx, &back, biz.SecretChange{}, 1, uAdmin), biz.ErrVersionConflict)
	if got, _ := e.sources.Get(ctx, second.ID); !got.IsDefault {
		t.Fatalf("a rejected update must not clear the current default")
	}
	if err := e.sources.Update(ctx, &back, biz.SecretChange{}, 2, uAdmin); err != nil {
		t.Fatal(err)
	}
	// 并发新建 12 个缺省数据源。
	var wg sync.WaitGroup
	errs := make([]error, 12)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d := &biz.Datasource{
				ID: newID(t), Name: fmt.Sprintf("c%d", i), Type: mc.DatasourceTypePrometheus, URL: "http://c:1", AuthType: mc.AuthNone,
				TimeoutMS: 5000, IsDefault: true, Enabled: true,
			}
			errs[i] = e.sources.Create(ctx, d, nil, uAdmin)
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent create %d: %v", i, err)
		}
	}
	if n := e.count("SELECT count(*) FROM monitor_datasource WHERE is_default AND delete_time IS NULL"); n != 1 {
		t.Fatalf("default datasources = %d, want exactly 1", n)
	}
	// 删除缺省数据源后可以再设一个。
	var current string
	if err := e.pool.QueryRow(ctx, "SELECT id::text FROM monitor_datasource WHERE is_default AND delete_time IS NULL").Scan(&current); err != nil {
		t.Fatal(err)
	}
	if err := e.sources.Delete(ctx, current, uAdmin); err != nil {
		t.Fatal(err)
	}
	e.datasource("after-delete", true)
	if n := e.count("SELECT count(*) FROM monitor_datasource WHERE is_default AND delete_time IS NULL"); n != 1 {
		t.Fatalf("default datasources after delete = %d", n)
	}
}

func TestRuleRepo(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	g := e.group("", "g")
	r := e.rule(g.ID, "cpu")
	if r.Version != 1 || r.SpecTime.IsZero() || r.CreateTime.IsZero() {
		t.Fatalf("create must fill version / times: %+v", r)
	}
	// 往返：全部字段原样读回（jsonb、数组、时间窗）。
	got, err := e.rules.Get(ctx, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	want := ruleSpec(g.ID, "cpu")
	s := got.Spec
	if s.Name != want.Name || !slices.Equal(s.Queries, want.Queries) || s.Labels["team"] != "infra" || s.Effective.Timezone != "Asia/Shanghai" ||
		len(s.Effective.Windows) != 1 || !slices.Equal(s.Effective.Windows[0].Days, []int32{1, 2}) || s.Annotations["summary"] != "{{ $value }}" ||
		s.RecoveryExpr != "cpu" || !slices.Equal(s.Notify.UserIDs, []string{uAlice}) || !s.NotifyRecovery || s.RepeatIntervalM != 30 ||
		s.MaxNotify != 5 || !slices.Equal(s.GroupKeyLabels, []string{"instance"}) || !s.LevelInhibit || s.ForS != 60 || got.UpdateUser != uAlice {
		t.Fatalf("round trip = %+v", s)
	}
	// 每次写入同事务留一条版本快照。
	if v, err := e.vers.Get(ctx, r.ID, 1); err != nil || v.Action != mc.RuleActionCreate || v.Summary != "新建" || v.OperatorID != uAlice ||
		v.Spec.Name != "cpu" || len(v.Spec.Queries) != 2 || v.Spec.GroupID != g.ID {
		t.Fatalf("version 1 = %+v err=%v", v, err)
	}
	dupErr := e.rules.Create(ctx, &biz.Rule{ID: newID(t), Spec: ruleSpec(g.ID, "cpu")}, biz.RuleChange{Action: mc.RuleActionCreate})
	wantErr(t, "duplicate name", dupErr, biz.ErrRuleNameExists)
	wantErr(t, "unknown group", e.rules.Create(ctx, &biz.Rule{ID: newID(t), Spec: ruleSpec(newID(t), "x")}, biz.RuleChange{Action: mc.RuleActionCreate}), biz.ErrGroupNotFound)
	if n := e.count("SELECT count(*) FROM monitor_rule_version"); n != 1 {
		t.Fatalf("failed creates must not leave version rows: %d", n)
	}

	// 更新：乐观锁；失败不写规则、不留版本。
	next := *got
	next.Spec.Name = "cpu-high"
	wantErr(t, "stale version", e.rules.Update(ctx, &next, 7, biz.RuleChange{Action: mc.RuleActionUpdate}), biz.ErrVersionConflict)
	ghost := next
	ghost.ID = newID(t)
	wantErr(t, "unknown rule", e.rules.Update(ctx, &ghost, 1, biz.RuleChange{Action: mc.RuleActionUpdate}), biz.ErrRuleNotFound)
	if n := e.count("SELECT count(*) FROM monitor_rule_version"); n != 1 {
		t.Fatalf("failed updates must not leave version rows: %d", n)
	}
	// spec_time 只在 BumpSpecTime 时推进。
	specTime := got.SpecTime
	if err := e.rules.Update(ctx, &next, 1, biz.RuleChange{Action: mc.RuleActionUpdate, Summary: "改名", Operator: uBob}); err != nil {
		t.Fatal(err)
	}
	if next.Version != 2 || !next.SpecTime.Equal(specTime) {
		t.Fatalf("rename: version %d spec_time %v (was %v)", next.Version, next.SpecTime, specTime)
	}
	next.Spec.Queries = []mc.Query{{Expr: "cpu > 99", Severity: mc.SeverityFatal}}
	if err := e.rules.Update(ctx, &next, 2, biz.RuleChange{Action: mc.RuleActionUpdate, BumpSpecTime: true, Operator: uBob}); err != nil {
		t.Fatal(err)
	}
	if next.Version != 3 || !next.SpecTime.After(specTime) {
		t.Fatalf("query change must bump spec_time: %v -> %v", specTime, next.SpecTime)
	}
	// 两个并发的更新带着同一个版本号：恰好一个成功。
	a, b := next, next
	a.Spec.Description, b.Spec.Description = "from-a", "from-b"
	var wg sync.WaitGroup
	results := make([]error, 2)
	for i, r := range []*biz.Rule{&a, &b} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = e.rules.Update(ctx, r, 3, biz.RuleChange{Action: mc.RuleActionUpdate, Operator: uBob})
		}()
	}
	wg.Wait()
	won := 0
	for _, err := range results {
		if err == nil {
			won++
		} else if !kerrors.Is(err, biz.ErrVersionConflict) {
			t.Fatalf("concurrent update: %v", err)
		}
	}
	if won != 1 {
		t.Fatalf("concurrent updates with the same version: %d succeeded, want 1", won)
	}
	// 改名撞上同分组的另一条。
	other := e.rule(g.ID, "mem")
	clash := *other
	clash.Spec.Name = "cpu-high"
	wantErr(t, "rename onto existing", e.rules.Update(ctx, &clash, 1, biz.RuleChange{Action: mc.RuleActionUpdate}), biz.ErrRuleNameExists)

	// 版本列表：倒序、不含快照。
	versions, total, err := e.vers.List(ctx, r.ID, 1, 2)
	if err != nil || total != 4 || len(versions) != 2 || versions[0].Version != 4 || versions[0].Spec != nil {
		t.Fatalf("versions = %d/%d err=%v", len(versions), total, err)
	}
	_, err = e.vers.Get(ctx, r.ID, 99)
	wantErr(t, "unknown version", err, biz.ErrRuleVersionNotFound)

	// 删除：软删 + delete 版本；之后读不到，名称可复用，历史仍在。
	current, _ := e.rules.Get(ctx, r.ID)
	stale := *current
	stale.Version = 1
	wantErr(t, "delete with stale version", e.rules.Delete(ctx, &stale, biz.RuleChange{Action: mc.RuleActionDelete}), biz.ErrVersionConflict)
	if err := e.rules.Delete(ctx, current, biz.RuleChange{Action: mc.RuleActionDelete, Summary: "删除", Operator: uAlice}); err != nil {
		t.Fatal(err)
	}
	_, err = e.rules.Get(ctx, r.ID)
	wantErr(t, "deleted", err, biz.ErrRuleNotFound)
	if v, err := e.vers.Get(ctx, r.ID, 5); err != nil || v.Action != mc.RuleActionDelete || v.Spec.Name != "cpu-high" {
		t.Fatalf("delete version = %+v err=%v", v, err)
	}
	e.rule(g.ID, "cpu-high")
}

func TestRuleRepo_ListFilters(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	ga, gb := e.group("", "a"), e.group("", "b")
	ds := e.datasource("vm", false)
	mk := func(group, name string, mutate func(s *biz.RuleSpec)) *biz.Rule {
		s := ruleSpec(group, name)
		if mutate != nil {
			mutate(&s)
		}
		r := &biz.Rule{ID: newID(t), Spec: s}
		if err := e.rules.Create(ctx, r, biz.RuleChange{Action: mc.RuleActionCreate, Operator: uAlice}); err != nil {
			t.Fatal(err)
		}
		return r
	}
	r1 := mk(ga.ID, "cpu-usage", nil)
	r2 := mk(ga.ID, "disk", func(s *biz.RuleSpec) {
		s.Enabled, s.Description = false, "磁盘 100%_满"
		s.Queries = []mc.Query{{Expr: "disk_free < 10", Severity: mc.SeverityFatal}}
		s.Labels = map[string]string{"team": "db", "env": "prod"}
		s.DatasourceScope, s.DatasourceIDs = mc.DatasourceScopeSpecified, []string{ds.ID}
	})
	r3 := mk(gb.ID, "other-team", nil)
	// 评估引擎的产物：r1 有一个任务报错、一条活跃告警。
	if _, err := e.pool.Exec(ctx, "INSERT INTO monitor_eval_task (rule_id, datasource_id, last_status, last_error, firing_count) VALUES ($1, $2, 4, 'boom', 2)", r1.ID, ds.ID); err != nil {
		t.Fatal(err)
	}
	e.alert(alertRow{ruleID: r1.ID, ruleName: "cpu-usage", groupID: ga.ID})

	yes, no := true, false
	sevFatal, errStatus := mc.SeverityFatal, mc.EvalError
	both := biz.GroupFilter{IDs: []string{ga.ID, gb.ID}}
	onlyA := biz.GroupFilter{IDs: []string{ga.ID}}
	for _, c := range []struct {
		name string
		f    biz.RuleFilter
		want []string
	}{
		{"all (admin)", biz.RuleFilter{Groups: biz.GroupFilter{All: true}}, []string{r1.ID, r2.ID, r3.ID}},
		{"scoped to group a", biz.RuleFilter{Groups: onlyA}, []string{r1.ID, r2.ID}},
		{"empty id set", biz.RuleFilter{Groups: biz.GroupFilter{IDs: []string{}}}, nil},
		{"keyword name (case-insensitive)", biz.RuleFilter{Groups: both, Keyword: "USAGE"}, []string{r1.ID}},
		{"keyword also searches promql", biz.RuleFilter{Groups: both, Keyword: "cpu >"}, []string{r1.ID, r3.ID}},
		{"keyword promql", biz.RuleFilter{Groups: both, Keyword: "disk_free"}, []string{r2.ID}},
		{"keyword is literal, not a LIKE pattern", biz.RuleFilter{Groups: both, Keyword: "100%_"}, []string{r2.ID}},
		{"keyword wildcard does not match all", biz.RuleFilter{Groups: both, Keyword: "%"}, []string{r2.ID}},
		{"severity", biz.RuleFilter{Groups: both, Severity: &sevFatal}, []string{r2.ID}},
		{"datasource: specified or global", biz.RuleFilter{Groups: onlyA, DatasourceID: ds.ID}, []string{r1.ID, r2.ID}},
		{"enabled", biz.RuleFilter{Groups: both, Enabled: &no}, []string{r2.ID}},
		{"firing", biz.RuleFilter{Groups: both, Firing: &yes}, []string{r1.ID}},
		{"not firing", biz.RuleFilter{Groups: onlyA, Firing: &no}, []string{r2.ID}},
		{"last status", biz.RuleFilter{Groups: both, LastStatus: &errStatus}, []string{r1.ID}},
		{"labels", biz.RuleFilter{Groups: both, Labels: map[string]string{"team": "db", "env": "prod"}}, []string{r2.ID}},
		{"labels mismatch", biz.RuleFilter{Groups: both, Labels: map[string]string{"team": "db", "env": "test"}}, nil},
	} {
		items, total, err := e.rules.List(ctx, c.f, 1, 50)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		got := make([]string, 0, len(items))
		for _, r := range items {
			got = append(got, r.ID)
		}
		slices.Sort(got)
		want := slices.Clone(c.want)
		slices.Sort(want)
		if int(total) != len(want) || !slices.Equal(got, want) {
			t.Errorf("%s: got %d %v, want %v", c.name, total, got, want)
		}
	}
	// 分页。
	if items, total, _ := e.rules.List(ctx, biz.RuleFilter{Groups: biz.GroupFilter{All: true}}, 2, 2); total != 3 || len(items) != 1 {
		t.Fatalf("page 2 = %d items / total %d", len(items), total)
	}
	// 查找类。
	if names, _ := e.rules.Names(ctx, []string{r1.ID, newID(t)}); len(names) != 1 || names[r1.ID] != "cpu-usage" {
		t.Fatalf("Names = %v", names)
	}
	if found, _ := e.rules.FindByNames(ctx, ga.ID, []string{"disk", "other-team", "nope"}); len(found) != 1 || found["disk"] != r2.ID {
		t.Fatalf("FindByNames = %v", found)
	}
	if byIDs, _ := e.rules.ListByIDs(ctx, []string{r1.ID, r3.ID, newID(t)}); len(byIDs) != 2 {
		t.Fatalf("ListByIDs = %d", len(byIDs))
	}
	if byGroups, _ := e.rules.ListByGroups(ctx, onlyA, 1); len(byGroups) != 1 {
		t.Fatalf("ListByGroups limit = %d", len(byGroups))
	}
	if byGroups, _ := e.rules.ListByGroups(ctx, biz.GroupFilter{}, 10); len(byGroups) != 0 {
		t.Fatalf("ListByGroups with an empty filter must return nothing")
	}
	// 评估任务与判定记录。
	tasks, err := e.evals.ListTasks(ctx, []string{r1.ID, r2.ID})
	if err != nil || len(tasks[r1.ID]) != 1 || tasks[r1.ID][0].LastError != "boom" || tasks[r1.ID][0].FiringCount != 2 || len(tasks[r2.ID]) != 0 {
		t.Fatalf("tasks = %+v err=%v", tasks, err)
	}
	for i := range 3 {
		if _, err := e.pool.Exec(ctx,
			`INSERT INTO monitor_eval_record (id, rule_id, datasource_id, eval_time, status, series, anomalies, detail)
			 VALUES ($1, $2, $3, now() - make_interval(mins => $4), $5, 3, 1,
			         '{"queries":[{"index":0,"severity":3,"series":3}],"points":[{"fingerprint":"f","labels":{"a":"b"},"severity":3,"value":"NaN","step":"pending"}],"truncated":true}')`,
			newID(t), r1.ID, ds.ID, i, 1+i%2); err != nil {
			t.Fatal(err)
		}
	}
	since := time.Now().Add(-time.Hour)
	okStatus := mc.EvalOK
	recs, total, err := e.evals.ListRecords(ctx, biz.EvalRecordFilter{RuleID: r1.ID, Start: &since}, 1, 2)
	if err != nil || total != 3 || len(recs) != 2 || !recs[0].EvalTime.After(recs[1].EvalTime) || !recs[0].Detail.Truncated ||
		recs[0].Detail.Points[0].Value != "NaN" || recs[0].Detail.Queries[0].Series != 3 {
		t.Fatalf("records = %d/%d err=%v", len(recs), total, err)
	}
	if _, total, _ := e.evals.ListRecords(ctx, biz.EvalRecordFilter{RuleID: r1.ID, Start: &since, Status: &okStatus, DatasourceID: ds.ID}, 1, 10); total != 2 {
		t.Fatalf("records by status = %d", total)
	}
	future := time.Now().Add(time.Hour)
	if _, total, _ := e.evals.ListRecords(ctx, biz.EvalRecordFilter{RuleID: r1.ID, Start: &future}, 1, 10); total != 0 {
		t.Fatalf("records after now = %d", total)
	}
}

func TestAlertRepo(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	ga, gb := e.group("", "a"), e.group("", "b")
	ruleA, ruleB := newID(t), newID(t)
	now := time.Now()
	a1 := e.alert(alertRow{ruleID: ruleA, ruleName: "cpu-high", groupID: ga.ID, severity: 3, labels: `{"instance":"h1","env":"prod"}`, fired: now.Add(-time.Minute)})
	a2 := e.alert(alertRow{ruleID: ruleA, ruleName: "cpu-high", groupID: ga.ID, severity: 2, labels: `{"instance":"h2","env":"test"}`, fired: now.Add(-2 * time.Minute), muted: true})
	a3 := e.alert(alertRow{ruleID: ruleA, ruleName: "cpu-high", groupID: ga.ID, severity: 3, status: mc.AlertResolved, labels: `{"instance":"h1","env":"prod"}`, fired: now.Add(-3 * time.Hour)})
	a4 := e.alert(alertRow{ruleID: ruleA, ruleName: "cpu-high", groupID: ga.ID, severity: 3, status: mc.AlertClosed, labels: `{"instance":"h9"}`, fired: now.Add(-40 * 24 * time.Hour)})
	b1 := e.alert(alertRow{ruleID: ruleB, ruleName: "disk-full", groupID: gb.ID, severity: 4, labels: `{"instance":"b1","env":"prod"}`, fired: now.Add(-time.Minute)})
	if _, err := e.pool.Exec(ctx,
		`INSERT INTO monitor_notify_log (id, alert_id, kind, seq, source, recipients) VALUES ($1, $2, 1, 1, 1, $3::jsonb)`,
		newID(t), a1, `["`+uAlice+`","$group:ops"]`); err != nil {
		t.Fatal(err)
	}

	list := func(f biz.AlertFilter) []string {
		t.Helper()
		items, total, err := e.alerts.List(ctx, f, 1, 50)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if int(total) != len(items) {
			t.Fatalf("total %d != %d", total, len(items))
		}
		out := make([]string, 0, len(items))
		for _, a := range items {
			out = append(out, a.ID)
		}
		return out
	}
	set := func(ids ...string) []string { slices.Sort(ids); return ids }
	sortedList := func(f biz.AlertFilter) []string { return set(list(f)...) }
	all := biz.GroupFilter{All: true}
	onlyA := biz.GroupFilter{IDs: []string{ga.ID}}
	since := now.Add(-7 * 24 * time.Hour)
	yes := true
	for _, c := range []struct {
		name string
		f    biz.AlertFilter
		want []string
	}{
		{"everything", biz.AlertFilter{Groups: all}, set(a1, a2, a3, a4, b1)},
		{"scoped", biz.AlertFilter{Groups: onlyA}, set(a1, a2, a3, a4)},
		{"scoped to nothing", biz.AlertFilter{Groups: biz.GroupFilter{IDs: []string{}}}, []string{}},
		{"statuses", biz.AlertFilter{Groups: all, Statuses: []int32{mc.AlertResolved, mc.AlertClosed}}, set(a3, a4)},
		{"severities", biz.AlertFilter{Groups: all, Severities: []int32{2, 4}}, set(a2, b1)},
		{"muted", biz.AlertFilter{Groups: all, Muted: &yes}, set(a2)},
		{"rule", biz.AlertFilter{Groups: all, RuleIDs: []string{ruleB}}, set(b1)},
		{"label containment", biz.AlertFilter{Groups: all, Labels: map[string]string{"env": "prod", "instance": "h1"}}, set(a1, a3)},
		{"label value is exact", biz.AlertFilter{Groups: all, Labels: map[string]string{"instance": "h"}}, []string{}},
		{"keyword on rule name", biz.AlertFilter{Groups: all, Keyword: "DISK"}, set(b1)},
		{"keyword does not search labels", biz.AlertFilter{Groups: all, Keyword: "h2"}, []string{}},
		{"default history window", biz.AlertFilter{Groups: onlyA, HistorySince: &since}, set(a1, a2, a3)},
		{"recipient", biz.AlertFilter{Groups: all, RecipientID: uAlice}, set(a1)},
		{"recipient never notified", biz.AlertFilter{Groups: all, RecipientID: uBob}, []string{}},
		{"fingerprint", biz.AlertFilter{Groups: all, Fingerprint: strings.ReplaceAll(a3, "-", "")}, set(a3)},
	} {
		if got := sortedList(c.f); !slices.Equal(got, c.want) {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
	from, to := now.Add(-4*time.Hour), now.Add(-2*time.Hour)
	if got := sortedList(biz.AlertFilter{Groups: all, FireStart: &from, FireEnd: &to}); !slices.Equal(got, set(a3)) {
		t.Errorf("time range: %v", got)
	}
	// 排序：最近触发在前。
	if got := list(biz.AlertFilter{Groups: onlyA, Statuses: []int32{mc.AlertFiring}}); !slices.Equal(got, []string{a1, a2}) {
		t.Errorf("order = %v", got)
	}
	if scanned, err := e.alerts.Scan(ctx, biz.AlertFilter{Groups: all}, 2); err != nil || len(scanned) != 2 {
		t.Fatalf("Scan limit = %d err=%v", len(scanned), err)
	}
	// 标签过滤走 GIN（jsonb_path_ops）索引：计划里不能是对 labels 的顺序过滤。
	if _, err := e.pool.Exec(ctx, "SET enable_seqscan = off"); err != nil {
		t.Fatal(err)
	}
	var plan string
	rows, err := e.pool.Query(ctx, `EXPLAIN SELECT id FROM monitor_alert a WHERE a.labels @> '{"env":"prod"}'::jsonb`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var line string
		_ = rows.Scan(&line)
		plan += line + "\n"
	}
	rows.Close()
	if !strings.Contains(plan, "idx_monitor_alert_labels") {
		t.Errorf("label containment must be able to use the GIN index:\n%s", plan)
	}

	got, err := e.alerts.Get(ctx, a1)
	if err != nil || got.Labels["instance"] != "h1" || got.Annotations["summary"] != "CPU 95%" || got.Value == nil || *got.Value != 95.5 ||
		got.RecoveryValue != nil || got.ResolveTime != nil || got.GroupKey != "monitor:rule:"+ruleA || got.MuteID != "" {
		t.Fatalf("Get = %+v err=%v", got, err)
	}
	_, err = e.alerts.Get(ctx, newID(t))
	wantErr(t, "unknown alert", err, biz.ErrAlertNotFound)

	// 聚合：只统计传入过滤条件内的。
	firing := biz.AlertFilter{Groups: all, Statuses: []int32{mc.AlertFiring}}
	agg := func(by biz.AggregateBy, f biz.AlertFilter) (map[string]*biz.AlertAggregate, int64) {
		t.Helper()
		items, total, err := e.alerts.Aggregate(ctx, by, f, 200)
		if err != nil {
			t.Fatalf("Aggregate(%+v): %v", by, err)
		}
		out := map[string]*biz.AlertAggregate{}
		for _, it := range items {
			out[it.Key] = it
		}
		return out, total
	}
	byRule, total := agg(biz.AggregateBy{Kind: biz.AggregateByRule}, firing)
	if total != 3 || byRule[ruleA].Count != 2 || byRule[ruleA].Name != "cpu-high" || byRule[ruleA].MaxSeverity != 3 ||
		byRule[ruleA].SeverityCounts != [4]int32{0, 1, 1, 0} || byRule[ruleB].SeverityCounts != [4]int32{0, 0, 0, 1} {
		t.Fatalf("by rule = %+v / %+v total=%d", byRule[ruleA], byRule[ruleB], total)
	}
	bySev, _ := agg(biz.AggregateBy{Kind: biz.AggregateBySeverity}, firing)
	if bySev["3"].Count != 1 || bySev["2"].Count != 1 || bySev["4"].Count != 1 {
		t.Fatalf("by severity = %v", bySev)
	}
	byGroup, _ := agg(biz.AggregateBy{Kind: biz.AggregateByGroup}, firing)
	if byGroup[ga.ID].Count != 2 || byGroup[gb.ID].Count != 1 {
		t.Fatalf("by group = %v", byGroup)
	}
	byLabel, total := agg(biz.AggregateBy{Kind: biz.AggregateByLabel, Label: "env"}, firing)
	if total != 3 || byLabel["prod"].Count != 2 || byLabel["test"].Count != 1 {
		t.Fatalf("by label = %v", byLabel)
	}
	// 标签名是实参，不拼进 SQL：注入式的名字只会得到「没有这个标签」。
	byLabel, _ = agg(biz.AggregateBy{Kind: biz.AggregateByLabel, Label: `env') FROM monitor_alert; --`}, firing)
	if len(byLabel) != 1 || byLabel[""].Count != 3 {
		t.Fatalf("hostile label name = %v", byLabel)
	}
	if scoped, total := agg(biz.AggregateBy{Kind: biz.AggregateByRule}, biz.AlertFilter{Groups: onlyA, Statuses: []int32{mc.AlertFiring}}); total != 2 || len(scoped) != 1 {
		t.Fatalf("scoped aggregate = %v total=%d", scoped, total)
	}
	if _, _, err := e.alerts.Aggregate(ctx, biz.AggregateBy{Kind: "nope"}, firing, 10); err == nil {
		t.Fatal("unknown dimension must be rejected")
	}
	muteID := newID(t)
	if _, err := e.pool.Exec(ctx, "UPDATE monitor_alert SET mute_id = $2 WHERE id = $1", a2, muteID); err != nil {
		t.Fatal(err)
	}
	if counts, _ := e.alerts.MutedCounts(ctx, []string{muteID, newID(t)}); len(counts) != 1 || counts[muteID] != 1 {
		t.Fatalf("MutedCounts = %v", counts)
	}

	// 通知记录读取。
	if ok, _ := e.logs.IsRecipient(ctx, a1, uAlice); !ok {
		t.Fatal("alice is a recipient")
	}
	if ok, _ := e.logs.IsRecipient(ctx, a1, uBob); ok {
		t.Fatal("bob is not a recipient")
	}
	if ok, _ := e.logs.IsRecipient(ctx, b1, uAlice); ok {
		t.Fatal("alice was not notified about b1")
	}
	logs, err := e.logs.ListByAlert(ctx, a1, 10)
	if err != nil || len(logs) != 1 || !slices.Equal(logs[0].Recipients, []string{uAlice, "$group:ops"}) || logs[0].SubscriptionID != "" {
		t.Fatalf("logs = %+v err=%v", logs, err)
	}
}

func TestMuteRepo(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	ga, gb := e.group("", "a"), e.group("", "b")
	before := e.cfgVersion(mc.CfgMute)
	start, end := time.Now().Add(-time.Hour).Truncate(time.Microsecond), time.Now().Add(time.Hour).Truncate(time.Microsecond)
	mk := func(group, name string, mutate func(s *biz.MuteSpec)) *biz.Mute {
		s := biz.MuteSpec{
			GroupID: group, Name: name, Reason: "变更", RuleIDs: []string{newID(t)}, Severities: []int32{3, 4},
			Matchers: []mc.Matcher{{Label: "env", Op: mc.OpIn, Values: []string{"a", "b"}}, {Label: "i", Op: mc.OpRegex, Value: `10\..*`}},
			Mode:     mc.MuteModeNotify, TimeType: mc.MuteTimeOnce, StartTime: &start, EndTime: &end, Enabled: true,
		}
		if mutate != nil {
			mutate(&s)
		}
		m := &biz.Mute{ID: newID(t), Spec: s}
		if err := e.mutes.Create(ctx, m, uAlice); err != nil {
			t.Fatalf("create mute %s: %v", name, err)
		}
		return m
	}
	m1 := mk(ga.ID, "发布窗口", nil)
	global := mk("", "全局", func(s *biz.MuteSpec) { s.Mode = mc.MuteModeEvent })
	periodic := mk(gb.ID, "夜间", func(s *biz.MuteSpec) {
		s.TimeType, s.StartTime, s.EndTime = mc.MuteTimePeriodic, nil, nil
		s.Periodic = &mc.Effective{Timezone: "Asia/Shanghai", Windows: []mc.TimeWindow{{Days: []int32{6, 7}, Start: "22:00", End: "06:00"}}}
	})
	expired := mk(ga.ID, "已过期", func(s *biz.MuteSpec) {
		a, b := time.Now().Add(-3*time.Hour), time.Now().Add(-2*time.Hour)
		s.StartTime, s.EndTime = &a, &b
	})
	disabled := mk(ga.ID, "停用", func(s *biz.MuteSpec) { s.Enabled = false })
	if e.cfgVersion(mc.CfgMute) != before+5 {
		t.Fatalf("cfg version = %d, want %d", e.cfgVersion(mc.CfgMute), before+5)
	}
	wantErr(t, "unknown group", e.mutes.Create(ctx, &biz.Mute{ID: newID(t), Spec: func() biz.MuteSpec { s := m1.Spec; s.GroupID = newID(t); return s }()}, uAlice), biz.ErrGroupNotFound)

	// 往返。
	got, err := e.mutes.Get(ctx, m1.ID)
	if err != nil || got.Version != 1 || got.Spec.GroupID != ga.ID || !slices.Equal(got.Spec.Severities, []int32{3, 4}) || len(got.Spec.Matchers) != 2 ||
		got.Spec.Matchers[1].Value != `10\..*` || !slices.Equal(got.Spec.Matchers[0].Values, []string{"a", "b"}) || got.Spec.Periodic != nil ||
		!got.Spec.StartTime.Equal(start) || !got.Spec.EndTime.Equal(end) || got.CreateUser != uAlice || len(got.Spec.RuleIDs) != 1 {
		t.Fatalf("round trip = %+v err=%v", got, err)
	}
	if g, _ := e.mutes.Get(ctx, global.ID); g.Spec.GroupID != "" || g.Spec.Mode != mc.MuteModeEvent {
		t.Fatalf("global mute = %+v", g.Spec)
	}
	if p, _ := e.mutes.Get(ctx, periodic.ID); p.Spec.Periodic == nil || p.Spec.Periodic.Windows[0].Start != "22:00" || p.Spec.StartTime != nil {
		t.Fatalf("periodic mute = %+v", p.Spec)
	}

	ids := func(f biz.MuteFilter) []string {
		t.Helper()
		items, total, err := e.mutes.List(ctx, f, 1, 50)
		if err != nil || int(total) != len(items) {
			t.Fatalf("List(%+v): total=%d items=%d err=%v", f, total, len(items), err)
		}
		out := make([]string, 0, len(items))
		for _, m := range items {
			out = append(out, m.ID)
		}
		slices.Sort(out)
		return out
	}
	set := func(v ...string) []string { slices.Sort(v); return v }
	onlyA := biz.GroupFilter{IDs: []string{ga.ID}}
	event := mc.MuteModeEvent
	for _, c := range []struct {
		name string
		f    biz.MuteFilter
		want []string
	}{
		{"admin with global", biz.MuteFilter{Groups: biz.GroupFilter{All: true}, IncludeGlobal: true}, set(m1.ID, global.ID, periodic.ID, expired.ID, disabled.ID)},
		{"scoped + global", biz.MuteFilter{Groups: onlyA, IncludeGlobal: true}, set(m1.ID, global.ID, expired.ID, disabled.ID)},
		{"scoped without global", biz.MuteFilter{Groups: onlyA}, set(m1.ID, expired.ID, disabled.ID)},
		{"empty scope still sees global when asked", biz.MuteFilter{Groups: biz.GroupFilter{IDs: []string{}}, IncludeGlobal: true}, set(global.ID)},
		{"keyword", biz.MuteFilter{Groups: biz.GroupFilter{All: true}, IncludeGlobal: true, Keyword: "窗口"}, set(m1.ID)},
		{"mode", biz.MuteFilter{Groups: biz.GroupFilter{All: true}, IncludeGlobal: true, Mode: &event}, set(global.ID)},
		{"disabled", biz.MuteFilter{Groups: onlyA, State: biz.MuteStateDisabled}, set(disabled.ID)},
		{"expired", biz.MuteFilter{Groups: onlyA, State: biz.MuteStateExpired}, set(expired.ID)},
		{"live", biz.MuteFilter{Groups: onlyA, Live: true}, set(m1.ID)},
	} {
		if got := ids(c.f); !slices.Equal(got, c.want) {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}

	// 更新：乐观锁 + cfg bump；失败不 bump。
	upd := *got
	upd.Spec.Name, upd.Spec.GroupID = "改名", gb.ID
	v := e.cfgVersion(mc.CfgMute)
	wantErr(t, "stale version", e.mutes.Update(ctx, &upd, 5, uBob), biz.ErrVersionConflict)
	wantErr(t, "unknown id", e.mutes.Update(ctx, &biz.Mute{ID: newID(t), Spec: upd.Spec}, 1, uBob), biz.ErrMuteNotFound)
	if e.cfgVersion(mc.CfgMute) != v {
		t.Fatal("failed updates must not bump the cfg version")
	}
	if err := e.mutes.Update(ctx, &upd, 1, uBob); err != nil {
		t.Fatal(err)
	}
	if after, _ := e.mutes.Get(ctx, m1.ID); after.Version != 2 || after.Spec.Name != "改名" || after.Spec.GroupID != gb.ID || after.UpdateUser != uBob || e.cfgVersion(mc.CfgMute) != v+1 {
		t.Fatalf("after update = %+v", after)
	}
	wantErr(t, "delete with stale version", e.mutes.Delete(ctx, m1.ID, 1, uBob), biz.ErrVersionConflict)
	if err := e.mutes.Delete(ctx, m1.ID, 2, uBob); err != nil {
		t.Fatal(err)
	}
	_, err = e.mutes.Get(ctx, m1.ID)
	wantErr(t, "deleted", err, biz.ErrMuteNotFound)
	if e.cfgVersion(mc.CfgMute) != v+2 {
		t.Fatal("delete must bump the cfg version")
	}
	// 名称查询含已删除的（历史事件仍要展示命中的屏蔽规则）。
	if names, _ := e.mutes.Names(ctx, []string{m1.ID, global.ID}); names[m1.ID] != "改名" || names[global.ID] != "全局" {
		t.Fatalf("Names = %v", names)
	}
}

func TestSubscriptionRepo(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	infra := e.group("", "infra")
	host := e.group(infra.ID, "host")
	app := e.group("", "app")
	ruleID, dsID := newID(t), newID(t)
	mk := func(owner, name string, groups []string, mutate func(s *biz.SubscriptionSpec)) *biz.Subscription {
		s := biz.SubscriptionSpec{
			Name: name, GroupIDs: groups, RuleIDs: []string{}, DatasourceIDs: []string{}, Severities: []int32{3, 4}, OnFiring: true,
			Matchers:  []mc.Matcher{{Label: "env", Op: mc.OpEqual, Value: "prod"}},
			Receivers: mc.Receivers{Self: true, UserIDs: []string{uCarol}, ScheduleIDs: []string{}, Groups: []string{"$group:ops"}}, Enabled: true,
		}
		if mutate != nil {
			mutate(&s)
		}
		sub := &biz.Subscription{ID: newID(t), Spec: s, OwnerID: owner}
		if err := e.subs.Create(ctx, sub); err != nil {
			t.Fatalf("create subscription %s: %v", name, err)
		}
		return sub
	}
	s1 := mk(uAlice, "主机", []string{host.ID}, nil)
	s2 := mk(uAlice, "基础设施全部", []string{infra.ID}, func(s *biz.SubscriptionSpec) { s.OnResolved, s.FollowRepeat = true, true })
	s3 := mk(uBob, "应用", []string{app.ID}, func(s *biz.SubscriptionSpec) { s.RuleIDs, s.DatasourceIDs = []string{ruleID}, []string{dsID} })
	s4 := mk(uBob, "停用", []string{host.ID}, func(s *biz.SubscriptionSpec) { s.Enabled = false })
	s5 := mk(uBob, "只订恢复", []string{host.ID}, func(s *biz.SubscriptionSpec) { s.OnFiring, s.OnResolved, s.Severities = false, true, []int32{1} })

	got, err := e.subs.Get(ctx, s1.ID)
	if err != nil || got.OwnerID != uAlice || got.Version != 1 || !slices.Equal(got.Spec.GroupIDs, []string{host.ID}) ||
		!slices.Equal(got.Spec.Severities, []int32{3, 4}) || !got.Spec.Receivers.Self || !slices.Equal(got.Spec.Receivers.Groups, []string{"$group:ops"}) ||
		!slices.Equal(got.Spec.Receivers.UserIDs, []string{uCarol}) || got.Spec.Matchers[0].Value != "prod" || !got.Spec.OnFiring || got.Spec.OnResolved {
		t.Fatalf("round trip = %+v err=%v", got, err)
	}

	ids := func(items []*biz.Subscription) []string {
		out := make([]string, 0, len(items))
		for _, s := range items {
			out = append(out, s.ID)
		}
		slices.Sort(out)
		return out
	}
	set := func(v ...string) []string { slices.Sort(v); return v }
	list := func(f biz.SubscriptionFilter) []string {
		t.Helper()
		items, total, err := e.subs.List(ctx, f, 1, 50)
		if err != nil || int(total) != len(items) {
			t.Fatalf("List: total=%d items=%d err=%v", total, len(items), err)
		}
		return ids(items)
	}
	enabled := true
	for _, c := range []struct {
		name string
		f    biz.SubscriptionFilter
		want []string
	}{
		{"mine", biz.SubscriptionFilter{OwnerID: uAlice}, set(s1.ID, s2.ID)},
		{"admin all", biz.SubscriptionFilter{ViewerID: uAdmin, Groups: biz.GroupFilter{All: true}}, set(s1.ID, s2.ID, s3.ID, s4.ID, s5.ID)},
		{"all within readable groups", biz.SubscriptionFilter{ViewerID: uCarol, Groups: biz.GroupFilter{IDs: []string{host.ID}}}, set(s1.ID, s4.ID, s5.ID)},
		{"own subscriptions are always visible", biz.SubscriptionFilter{ViewerID: uBob, Groups: biz.GroupFilter{IDs: []string{}}}, set(s3.ID, s4.ID, s5.ID)},
		{"no scope, no own", biz.SubscriptionFilter{ViewerID: uCarol, Groups: biz.GroupFilter{IDs: []string{}}}, []string{}},
		{"covering a group or its ancestors", biz.SubscriptionFilter{ViewerID: uAdmin, Groups: biz.GroupFilter{All: true}, CoverGroupIDs: []string{infra.ID, host.ID}}, set(s1.ID, s2.ID, s4.ID, s5.ID)},
		{"keyword", biz.SubscriptionFilter{ViewerID: uAdmin, Groups: biz.GroupFilter{All: true}, Keyword: "全部"}, set(s2.ID)},
		{"enabled", biz.SubscriptionFilter{OwnerID: uBob, Enabled: &enabled}, set(s3.ID, s5.ID)},
	} {
		if got := list(c.f); !slices.Equal(got, c.want) {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}

	// 派发预筛：分组（事件所在分组及其上级）、规则、数据源、级别、事件类型、是否跟随重复。
	cand := func(c biz.SubscriptionCandidate) []string {
		t.Helper()
		items, total, err := e.subs.ListCandidates(ctx, c, 100)
		if err != nil || total != len(items) {
			t.Fatalf("ListCandidates: total = %d for %d items, err = %v", total, len(items), err)
		}
		return ids(items)
	}
	hostChain := []string{infra.ID, host.ID}
	other := newID(t)
	for _, c := range []struct {
		name string
		c    biz.SubscriptionCandidate
		want []string
	}{
		{"firing on host", biz.SubscriptionCandidate{GroupIDs: hostChain, RuleID: other, DatasourceID: other, Severity: 3}, set(s1.ID, s2.ID)},
		{"resolved on host", biz.SubscriptionCandidate{GroupIDs: hostChain, RuleID: other, DatasourceID: other, Severity: 3, Resolved: true}, set(s2.ID)},
		{"repeat: followers only", biz.SubscriptionCandidate{GroupIDs: hostChain, RuleID: other, DatasourceID: other, Severity: 4, RepeatOnly: true}, set(s2.ID)},
		{"severity not subscribed", biz.SubscriptionCandidate{GroupIDs: hostChain, RuleID: other, DatasourceID: other, Severity: 2}, []string{}},
		{"resolved with severity 1", biz.SubscriptionCandidate{GroupIDs: hostChain, RuleID: other, DatasourceID: other, Severity: 1, Resolved: true}, set(s5.ID)},
		{"sibling group", biz.SubscriptionCandidate{GroupIDs: []string{infra.ID, other}, RuleID: other, DatasourceID: other, Severity: 3}, set(s2.ID)},
		{"rule + datasource limited: match", biz.SubscriptionCandidate{GroupIDs: []string{app.ID}, RuleID: ruleID, DatasourceID: dsID, Severity: 3}, set(s3.ID)},
		{"rule limited: other rule", biz.SubscriptionCandidate{GroupIDs: []string{app.ID}, RuleID: other, DatasourceID: dsID, Severity: 3}, []string{}},
		{"datasource limited: other datasource", biz.SubscriptionCandidate{GroupIDs: []string{app.ID}, RuleID: ruleID, DatasourceID: other, Severity: 3}, []string{}},
	} {
		if got := cand(c.c); !slices.Equal(got, c.want) {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
	// 超过上限：只返回前 limit 条，但总数如实给出（派发据此告警，而不是静默截断）。
	limited, total, err := e.subs.ListCandidates(ctx, biz.SubscriptionCandidate{GroupIDs: hostChain, RuleID: other, DatasourceID: other, Severity: 3}, 1)
	if err != nil || len(limited) != 1 || total != 2 {
		t.Fatalf("candidate limit: items = %d total = %d err = %v, want 1 of 2", len(limited), total, err)
	}

	// 更新 / 删除：乐观锁；所有者不变。
	upd := *got
	upd.Spec.Name, upd.Spec.Severities = "改名", []int32{1}
	wantErr(t, "stale version", e.subs.Update(ctx, &upd, 3, uBob), biz.ErrVersionConflict)
	wantErr(t, "unknown id", e.subs.Update(ctx, &biz.Subscription{ID: newID(t), Spec: upd.Spec}, 1, uBob), biz.ErrSubscriptionNotFound)
	if err := e.subs.Update(ctx, &upd, 1, uBob); err != nil {
		t.Fatal(err)
	}
	if after, _ := e.subs.Get(ctx, s1.ID); after.Version != 2 || after.Spec.Name != "改名" || after.OwnerID != uAlice || !slices.Equal(after.Spec.Severities, []int32{1}) {
		t.Fatalf("after update = %+v", after)
	}
	wantErr(t, "delete with stale version", e.subs.Delete(ctx, s1.ID, 1, uAlice), biz.ErrVersionConflict)
	if err := e.subs.Delete(ctx, s1.ID, 2, uAlice); err != nil {
		t.Fatal(err)
	}
	_, err = e.subs.Get(ctx, s1.ID)
	wantErr(t, "deleted", err, biz.ErrSubscriptionNotFound)

	// 命中统计：最近一次命中不受 since 限制，次数只算 since 之后的。
	alertID := newID(t)
	for i, age := range []time.Duration{time.Hour, 3 * 24 * time.Hour, 10 * 24 * time.Hour} {
		if _, err := e.pool.Exec(ctx,
			`INSERT INTO monitor_notify_log (id, alert_id, kind, seq, source, subscription_id, recipients, create_time)
			 VALUES ($1, $2, 1, $3, 2, $4, '[]', now() - make_interval(secs => $5))`,
			newID(t), alertID, i+1, s2.ID, age.Seconds()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.pool.Exec(ctx,
		`INSERT INTO monitor_notify_log (id, alert_id, kind, seq, source, subscription_id, recipients, create_time)
		 VALUES ($1, $2, 1, 1, 2, $3, '[]', now() - interval '30 days')`, newID(t), alertID, s3.ID); err != nil {
		t.Fatal(err)
	}
	hits, err := e.logs.SubscriptionHits(ctx, []string{s2.ID, s3.ID, s4.ID}, time.Now().Add(-7*24*time.Hour))
	if err != nil || len(hits) != 2 || hits[s2.ID].Count != 2 || time.Since(hits[s2.ID].LastTime) > 2*time.Hour ||
		hits[s3.ID].Count != 0 || hits[s3.ID].LastTime.IsZero() {
		t.Fatalf("hits = %+v err=%v", hits, err)
	}
	// 时间线里带出订阅名（含已删除的订阅）。
	if _, err := e.pool.Exec(ctx, "UPDATE monitor_subscription SET delete_time = now() WHERE id = $1", s2.ID); err != nil {
		t.Fatal(err)
	}
	if logs, _ := e.logs.ListByAlert(ctx, alertID, 2); len(logs) != 2 || logs[0].SubscriptionName == "" {
		t.Fatalf("timeline = %+v", logs)
	}
}

// ---------------------------------------------------------------------------
// 通知派发
// ---------------------------------------------------------------------------

// flakyWriter 包住真实的 NotificationWriter：failures > 0 时先把事件写进事务再报错，
// 用来验证「事件 + 通知记录 + 发件箱」同事务回滚。
type flakyWriter struct {
	inner    biznotify.NotificationWriter
	mu       sync.Mutex
	failures int
}

func (w *flakyWriter) NotifyEvent(ctx context.Context, tx txrun.Tx, ev biznotify.Event) (*biznotify.Event, error) {
	out, err := w.inner.NotifyEvent(ctx, tx, ev)
	if err != nil {
		return nil, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failures > 0 {
		w.failures--
		return nil, errors.New("notify pipeline unavailable")
	}
	return out, nil
}

func (w *flakyWriter) failNext(n int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.failures = n
}

type dispatchEnv struct {
	*env
	uc     *biz.DispatchUsecase
	writer *flakyWriter
	group  *biz.RuleGroup
	rule   *biz.Rule
}

func newDispatchEnv(t *testing.T) *dispatchEnv {
	t.Helper()
	e := newEnv(t)
	g := e.group("", "team-a")
	if err := e.scopes.Replace(context.Background(), g.ID, []*biz.RoleScope{{RoleID: roleA, Level: mc.ScopeManage}}, uAdmin); err != nil {
		t.Fatal(err)
	}
	writer := &flakyWriter{inner: biznotify.NewNotificationUsecase(datanotify.NewEventRepo(e.d), nil)}
	d := &dispatchEnv{env: e, writer: writer, group: g, rule: e.rule(g.ID, "cpu-high")}
	d.uc = d.newUsecase(e, writer)
	return d
}

// newUsecase 以给定仓储（可以来自另一个连接池 / 另一个数据库角色）与通知事件写入方装配派发用例。
func (d *dispatchEnv) newUsecase(e *env, events biz.EventWriter) *biz.DispatchUsecase {
	d.t.Helper()
	uc, err := biz.NewDispatchUsecase(basedata.NewTxManager(e.d), e.outbox, e.outbox, e.alerts, e.rules, e.subs,
		biz.NewScopeUsecase(d.org, e.scopes, e.groups), d.org, d.org, d.org, events, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	if err != nil {
		d.t.Fatalf("NewDispatchUsecase() error = %v", err)
	}
	return uc
}

// mustFail 跑一轮，要求恰有一行失败：单行失败不作为 error 返回（后台循环不因它退避），只在该行上记一次。
func (d *dispatchEnv) mustFail(outboxID string, wantDone int) {
	d.t.Helper()
	before := d.outboxState(outboxID).attempts
	n, err := d.run()
	if err != nil || n != wantDone {
		d.t.Fatalf("Run() = %d, %v; want %d processed and no loop error for a single failed row", n, err, wantDone)
	}
	if got := d.outboxState(outboxID).attempts; got != before+1 {
		d.t.Fatalf("outbox %s attempts = %d, want %d", outboxID, got, before+1)
	}
}

func (d *dispatchEnv) firingAlert() string {
	return d.alert(alertRow{ruleID: d.rule.ID, ruleName: "cpu-high", groupID: d.group.ID, labels: `{"instance":"h1","env":"prod"}`})
}

func (d *dispatchEnv) run() (int, error) { return d.uc.Run(context.Background()) }

func (d *dispatchEnv) mustRun(want int) {
	d.t.Helper()
	n, err := d.run()
	if err != nil || n != want {
		d.t.Fatalf("Run() = %d, %v; want %d, nil", n, err, want)
	}
}

type alertProgress struct {
	count int32
	last  *time.Time
}

func (d *dispatchEnv) progress(alertID string) alertProgress {
	d.t.Helper()
	var p alertProgress
	if err := d.pool.QueryRow(context.Background(), "SELECT notify_count, last_notify_time FROM monitor_alert WHERE id = $1", alertID).
		Scan(&p.count, &p.last); err != nil {
		d.t.Fatal(err)
	}
	return p
}

// expireBackoff 把失败时间拨回过去，让退避期立刻结束。
func (d *dispatchEnv) expireBackoff(outboxID string) {
	d.t.Helper()
	if _, err := d.pool.Exec(context.Background(), "UPDATE monitor_notify_outbox SET done_time = now() - interval '2 hours' WHERE id = $1", outboxID); err != nil {
		d.t.Fatal(err)
	}
}

// 派发一行：通知事件、通知记录、通知计数、发件箱状态在同一事务里落库；内置模板能渲染出内容。
func TestDispatch_WritesEverythingInOneTx(t *testing.T) {
	d := newDispatchEnv(t)
	ctx := context.Background()
	alertID := d.firingAlert()
	sub := &biz.Subscription{ID: newID(t), OwnerID: uAlice, Spec: biz.SubscriptionSpec{
		Name: "关注", GroupIDs: []string{d.group.ID}, Severities: []int32{3}, OnFiring: true, OnResolved: true, Enabled: true,
		Matchers:  []mc.Matcher{{Label: "env", Op: mc.OpEqual, Value: "prod"}},
		Receivers: mc.Receivers{Self: true, UserIDs: []string{uBob}, Groups: []string{"$group:ops"}},
	}}
	if err := d.subs.Create(ctx, sub); err != nil {
		t.Fatal(err)
	}
	ob := d.outboxRow(alertID, mc.NotifyKindFiring, 1)
	d.mustRun(1)

	if s := d.outboxState(ob); s.status != mc.OutboxDone || s.done == nil || s.attempts != 0 || s.lastError != "" {
		t.Fatalf("outbox = %+v", s)
	}
	if p := d.progress(alertID); p.count != 1 || p.last == nil {
		t.Fatalf("alert progress = %+v", p)
	}
	// 规则事件发给 alice；订阅事件减去已通知的 alice，剩 bob 与群。
	type event struct {
		typ, title, dedup, group string
		recipients               []string
		severity                 int32
	}
	rows, err := d.pool.Query(ctx, `SELECT event_type, title, dedup_key, group_key, severity,
		ARRAY(SELECT jsonb_array_elements_text(COALESCE(recipients, '[]'::jsonb))) FROM notify_event ORDER BY event_type`)
	if err != nil {
		t.Fatal(err)
	}
	var events []event
	for rows.Next() {
		var ev event
		if err := rows.Scan(&ev.typ, &ev.title, &ev.dedup, &ev.group, &ev.severity, &ev.recipients); err != nil {
			t.Fatal(err)
		}
		events = append(events, ev)
	}
	rows.Close()
	if len(events) != 2 || events[0].typ != mc.EventAlertFiring || events[1].typ != mc.EventSubscriptionFiring {
		t.Fatalf("events = %+v", events)
	}
	if !slices.Equal(events[0].recipients, []string{uAlice}) || !slices.Equal(events[1].recipients, []string{uBob, "$group:ops"}) {
		t.Fatalf("recipients = %v / %v", events[0].recipients, events[1].recipients)
	}
	bare := strings.ReplaceAll(alertID, "-", "")
	if events[0].dedup != "monitor:"+bare+":"+bare+":1:1" || events[0].group != "monitor:rule:"+d.rule.ID || events[0].severity != 3 ||
		!strings.HasPrefix(events[1].dedup, events[0].dedup+":s:") || len(events[1].dedup) > 128 {
		t.Fatalf("keys = %q / %q / %q", events[0].dedup, events[1].dedup, events[0].group)
	}
	if n := d.count("SELECT count(*) FROM monitor_notify_log WHERE alert_id = $1", alertID); n != 2 {
		t.Fatalf("notify logs = %d", n)
	}
	logs, _ := d.logs.ListByAlert(ctx, alertID, 10)
	if logs[0].Source != mc.NotifySourceRule || len(logs[0].NotifyEventIDs) != 1 || logs[1].SubscriptionID != sub.ID || logs[1].SubscriptionName != "关注" {
		t.Fatalf("logs = %+v / %+v", logs[0], logs[1])
	}
	// 日志里记的事件 ID 确实是落库的事件。
	if n := d.count("SELECT count(*) FROM notify_event WHERE id = ANY($1::uuid[])", []string{logs[0].NotifyEventIDs[0], logs[1].NotifyEventIDs[0]}); n != 2 {
		t.Fatalf("notify_event_ids must reference real events: %d", n)
	}
	// 载荷的键与迁移内置的模板一致：数值与时间是已格式化的文本。
	if n := d.count(`SELECT count(*) FROM notify_event WHERE jsonb_typeof(payload->'value') = 'string' AND payload->>'value' = '95.5'
		AND jsonb_typeof(payload->'first_fire_time') = 'string' AND payload->>'duration' <> '' AND payload->>'alert_url' = $1
		AND payload->>'group_name' = 'team-a' AND payload->'labels'->>'env' = 'prod' AND (payload->>'notify_seq')::int = 1`,
		"/monitor/alert/"+alertID); n != 2 {
		t.Fatalf("payload shape mismatch on %d of 2 events", 2-n)
	}
	if n := d.count(`SELECT count(*) FROM notify_event WHERE event_type = $1 AND payload->>'subscription_name' = '关注'`, mc.EventSubscriptionFiring); n != 1 {
		t.Fatal("subscription event must carry the subscription name")
	}
	// 内置模板：0069 的四个事件类型 + 0076 的升级通知与四个自监控事件，每个类型一份。
	seeded := append([]string{mc.EventAlertFiring, mc.EventAlertResolved, mc.EventSubscriptionFiring, mc.EventSubscriptionResolved, mc.EventAlertEscalated},
		mc.EngineEventTypes()...)
	if n := d.count(`SELECT count(*) FROM notify_template WHERE biz_id = $1 AND event_type = ANY($2::text[]) AND delete_time IS NULL`, mc.NotifyBizID, seeded); n != len(seeded) {
		t.Fatalf("seeded monitor templates = %d, want %d", n, len(seeded))
	}
}

// 事件写入之后才失败：整行回滚——没有事件、没有去重门闩、没有通知记录、计数不动，发件箱仍待派发并记一次失败；
// 退避期内不会被再次领取，退避期过后重试成功且只通知一次。
func TestDispatch_RollsBackAtomically(t *testing.T) {
	d := newDispatchEnv(t)
	alertID := d.firingAlert()
	ob := d.outboxRow(alertID, mc.NotifyKindFiring, 1)
	d.writer.failNext(1)
	d.mustFail(ob, 0)
	for table, where := range map[string]string{
		"notify_event": "TRUE", "notify_dedup": "TRUE", "monitor_notify_log": "TRUE", "monitor_alert": "notify_count > 0 OR last_notify_time IS NOT NULL",
	} {
		if c := d.count("SELECT count(*) FROM " + table + " WHERE " + where); c != 0 {
			t.Errorf("%s must be untouched after a rolled-back dispatch: %d rows", table, c)
		}
	}
	s := d.outboxState(ob)
	if s.status != mc.OutboxPending || s.attempts != 1 || !strings.Contains(s.lastError, "notify pipeline unavailable") || s.done == nil {
		t.Fatalf("failed row = %+v, want pending / attempts 1 / failure time recorded", s)
	}
	// 退避期内：不领取。
	d.mustRun(0)
	if d.outboxState(ob).attempts != 1 {
		t.Fatal("a row in backoff must not be retried")
	}
	d.expireBackoff(ob)
	d.mustRun(1)
	if s := d.outboxState(ob); s.status != mc.OutboxDone || s.attempts != 1 {
		t.Fatalf("after retry = %+v", s)
	}
	if c := d.count("SELECT count(*) FROM notify_event"); c != 1 {
		t.Fatalf("exactly one event after the retry: %d", c)
	}
	if p := d.progress(alertID); p.count != 1 {
		t.Fatalf("notify_count = %d", p.count)
	}
}

// 一行有问题不拖累同批其它行。
func TestDispatch_PoisonedRowDoesNotBlockOthers(t *testing.T) {
	d := newDispatchEnv(t)
	bad, good := d.firingAlert(), d.firingAlert()
	obBad := d.outboxRow(bad, mc.NotifyKindFiring, 1)
	obGood := d.outboxRow(good, mc.NotifyKindFiring, 1)
	d.writer.failNext(1)
	d.mustFail(obBad, 1)
	if d.outboxState(obBad).status != mc.OutboxPending || d.outboxState(obGood).status != mc.OutboxDone {
		t.Fatalf("bad = %+v good = %+v", d.outboxState(obBad), d.outboxState(obGood))
	}
	if d.progress(bad).count != 0 || d.progress(good).count != 1 {
		t.Fatal("only the healthy row counts as notified")
	}
}

// 失败次数用尽才丢弃；退避间隔逐次拉长且有上限。
func TestDispatch_BackoffAndDropAfterMaxAttempts(t *testing.T) {
	d := newDispatchEnv(t)
	ctx := context.Background()
	alertID := d.firingAlert()
	ob := d.outboxRow(alertID, mc.NotifyKindFiring, 1)
	claimable := func() bool {
		t.Helper()
		var ok bool
		err := basedata.NewTxManager(d.d).WithinTx(ctx, func(tx txrun.Tx) error {
			item, err := d.outbox.ClaimTx(ctx, tx, nil)
			ok = item != nil
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}
	setFailedAgo := func(attempts int, ago time.Duration) {
		t.Helper()
		if _, err := d.pool.Exec(ctx, "UPDATE monitor_notify_outbox SET attempts = $2, done_time = now() - make_interval(secs => $3) WHERE id = $1",
			ob, attempts, ago.Seconds()); err != nil {
			t.Fatal(err)
		}
	}
	// 15s × 2^(n-1)，封顶 30 分钟。
	for _, c := range []struct {
		attempts int
		ago      time.Duration
		want     bool
	}{
		{1, 10 * time.Second, false},
		{1, 16 * time.Second, true},
		{3, 50 * time.Second, false},
		{3, 61 * time.Second, true},
		{6, 7 * time.Minute, false},
		{6, 9 * time.Minute, true},
		{9, 29 * time.Minute, false},
		{9, 31 * time.Minute, true},
	} {
		setFailedAgo(c.attempts, c.ago)
		if got := claimable(); got != c.want {
			t.Errorf("attempts=%d failed %v ago: claimable = %v, want %v", c.attempts, c.ago, got, c.want)
		}
	}
	setFailedAgo(0, 0)
	if _, err := d.pool.Exec(ctx, "UPDATE monitor_notify_outbox SET done_time = NULL WHERE id = $1", ob); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= mc.OutboxMaxAttempts; i++ {
		d.writer.failNext(1)
		d.mustFail(ob, 0)
		s := d.outboxState(ob)
		want := mc.OutboxPending
		if i == mc.OutboxMaxAttempts {
			want = mc.OutboxDropped
		}
		if s.attempts != int32(i) || s.status != want {
			t.Fatalf("after attempt %d: %+v", i, s)
		}
		d.expireBackoff(ob)
	}
	d.mustRun(0)
	if c := d.count("SELECT count(*) FROM notify_event"); c != 0 {
		t.Fatalf("a row that never dispatched must leave no event: %d", c)
	}
	if p := d.progress(alertID); p.count != 0 || p.last != nil {
		t.Fatalf("a dropped row must not touch notify_count: %+v", p)
	}
}

// 引擎从不写 notify_count / last_notify_time：只有触发 / 重复通知确实派发时回写；恢复通知与丢弃不动。
func TestDispatch_NotifyCountSemantics(t *testing.T) {
	d := newDispatchEnv(t)
	ctx := context.Background()
	alertID := d.firingAlert()
	d.outboxRow(alertID, mc.NotifyKindFiring, 1)
	d.mustRun(1)
	first := d.progress(alertID)
	if first.count != 1 || first.last == nil {
		t.Fatalf("after firing = %+v", first)
	}
	time.Sleep(5 * time.Millisecond)
	d.outboxRow(alertID, mc.NotifyKindRepeat, 2)
	d.mustRun(1)
	second := d.progress(alertID)
	if second.count != 2 || !second.last.After(*first.last) {
		t.Fatalf("after repeat = %+v", second)
	}
	// 恢复通知：派发，但不计数、不改最近通知时间。
	time.Sleep(5 * time.Millisecond)
	if _, err := d.pool.Exec(ctx, "UPDATE monitor_alert SET status = 2, resolve_time = now(), recovery_value = 41.7 WHERE id = $1", alertID); err != nil {
		t.Fatal(err)
	}
	resolved := d.outboxRow(alertID, mc.NotifyKindResolved, 3)
	d.mustRun(1)
	third := d.progress(alertID)
	if d.outboxState(resolved).status != mc.OutboxDone || third.count != 2 || !third.last.Equal(*second.last) {
		t.Fatalf("after resolved = %+v (was %+v)", third, second)
	}
	if n := d.count(`SELECT count(*) FROM notify_event WHERE event_type = $1 AND payload->>'status' = 'resolved'
		AND payload->>'recovery_value' = '41.7' AND (payload->>'notify_seq')::int = 3`, mc.EventAlertResolved); n != 1 {
		t.Fatalf("resolved event payload mismatch")
	}
	// 迟到的重复通知（事件已恢复）：丢弃，不计数。
	stale := d.outboxRow(alertID, mc.NotifyKindRepeat, 3)
	d.mustRun(1)
	if s := d.outboxState(stale); s.status != mc.OutboxDropped || !strings.Contains(s.lastError, "不再重复通知") {
		t.Fatalf("stale repeat = %+v", s)
	}
	if p := d.progress(alertID); p.count != 2 || !p.last.Equal(*second.last) {
		t.Fatalf("a dropped repeat must not touch the alert: %+v", p)
	}
	// 被屏蔽的事件：丢弃，不计数、不写事件。
	muted := d.alert(alertRow{ruleID: d.rule.ID, ruleName: "cpu-high", groupID: d.group.ID, muted: true})
	ob := d.outboxRow(muted, mc.NotifyKindFiring, 1)
	d.mustRun(1)
	if s := d.outboxState(ob); s.status != mc.OutboxDropped || !strings.Contains(s.lastError, "屏蔽") || s.done == nil {
		t.Fatalf("muted row = %+v", s)
	}
	if p := d.progress(muted); p.count != 0 || p.last != nil {
		t.Fatalf("dropped row touched the alert: %+v", p)
	}
	// 没发出过触发通知的事件不发恢复通知。
	never := d.alert(alertRow{ruleID: d.rule.ID, ruleName: "cpu-high", groupID: d.group.ID, status: mc.AlertResolved})
	ob = d.outboxRow(never, mc.NotifyKindResolved, 1)
	d.mustRun(1)
	if s := d.outboxState(ob); s.status != mc.OutboxDropped {
		t.Fatalf("recovery without a prior firing notification = %+v", s)
	}
	// 规则被删除：丢弃。
	gone := d.rule
	orphan := d.firingAlert()
	if err := d.rules.Delete(ctx, gone, biz.RuleChange{Action: mc.RuleActionDelete, Operator: uAlice}); err != nil {
		t.Fatal(err)
	}
	ob = d.outboxRow(orphan, mc.NotifyKindFiring, 1)
	d.mustRun(1)
	if s := d.outboxState(ob); s.status != mc.OutboxDropped || !strings.Contains(s.lastError, "规则已删除") {
		t.Fatalf("orphan row = %+v", s)
	}
}

// rearmSQL 评估引擎在屏蔽解除后重新置为待派发的语句（backend/monitor/internal/data/round.go 的
// queueOutbox；这里原样执行，验证派发端与它配合）。
const rearmSQL = `INSERT INTO monitor_notify_outbox (id, alert_id, kind, seq, status)
	 SELECT v.id, v.alert_id, v.kind, v.seq, $1 FROM unnest($2::uuid[], $3::uuid[], $4::int4[], $5::int4[]) AS v(id, alert_id, kind, seq)
	 ON CONFLICT (alert_id, kind, seq) DO UPDATE SET status = $1, attempts = 0, last_error = '', create_time = now(), done_time = NULL
	 WHERE monitor_notify_outbox.status = $6`

func (d *dispatchEnv) rearm(alertID string, kind, seq int32) {
	d.t.Helper()
	if _, err := d.pool.Exec(context.Background(), rearmSQL, mc.OutboxPending, []string{newID(d.t)}, []string{alertID},
		[]int32{kind}, []int32{seq}, mc.OutboxDropped); err != nil {
		d.t.Fatalf("re-arm outbox: %v", err)
	}
}

// 被丢弃的行由引擎重新置为待派发后按普通待派发行处理：立即可领取（上一世的失败次数与退避不带过来）、
// 正常派发、去重键不与先前撞车。
func TestDispatch_RearmedRow(t *testing.T) {
	d := newDispatchEnv(t)
	ctx := context.Background()
	alertID := d.firingAlert()
	ob := d.outboxRow(alertID, mc.NotifyKindFiring, 1)
	// 第一世：失败几次，随后事件被屏蔽，行被丢弃。
	for range 3 {
		d.writer.failNext(1)
		d.mustFail(ob, 0)
		d.expireBackoff(ob)
	}
	if _, err := d.pool.Exec(ctx, "UPDATE monitor_alert SET muted = true WHERE id = $1", alertID); err != nil {
		t.Fatal(err)
	}
	d.mustRun(1)
	if s := d.outboxState(ob); s.status != mc.OutboxDropped || s.attempts != 3 {
		t.Fatalf("first life = %+v", s)
	}
	if c := d.count("SELECT count(*) FROM notify_event") + d.count("SELECT count(*) FROM notify_dedup"); c != 0 {
		t.Fatalf("a dropped row must leave neither events nor dedup latches: %d", c)
	}
	// 对未丢弃的行，引擎的语句是空操作。
	done := d.firingAlert()
	obDone := d.outboxRow(done, mc.NotifyKindFiring, 1)
	d.mustRun(1)
	d.rearm(done, mc.NotifyKindFiring, 1)
	if s := d.outboxState(obDone); s.status != mc.OutboxDone {
		t.Fatalf("re-arm must not touch a dispatched row: %+v", s)
	}

	// 屏蔽解除，引擎重新置为待派发。
	if _, err := d.pool.Exec(ctx, "UPDATE monitor_alert SET muted = false, mute_id = NULL WHERE id = $1", alertID); err != nil {
		t.Fatal(err)
	}
	d.rearm(alertID, mc.NotifyKindFiring, 1)
	if s := d.outboxState(ob); s.status != mc.OutboxPending || s.attempts != 0 || s.lastError != "" || s.done != nil {
		t.Fatalf("re-armed row = %+v", s)
	}
	d.mustRun(1) // 不必等任何退避
	if s := d.outboxState(ob); s.status != mc.OutboxDone {
		t.Fatalf("re-armed row after dispatch = %+v", s)
	}
	if p := d.progress(alertID); p.count != 1 || p.last == nil {
		t.Fatalf("re-armed dispatch must count: %+v", p)
	}
	if c := d.count("SELECT count(*) FROM notify_event WHERE payload->>'alert_id' = $1", alertID); c != 1 {
		t.Fatalf("events for the re-armed row = %d, want 1", c)
	}
	// 重新置位后再失败：次数从头计，不会因为上一世的三次而提前丢弃。
	if _, err := d.pool.Exec(ctx, "UPDATE monitor_alert SET muted = true WHERE id = $1", alertID); err != nil {
		t.Fatal(err)
	}
	ob2 := d.outboxRow(alertID, mc.NotifyKindRepeat, 2)
	d.mustRun(1)
	if _, err := d.pool.Exec(ctx, "UPDATE monitor_alert SET muted = false WHERE id = $1", alertID); err != nil {
		t.Fatal(err)
	}
	d.rearm(alertID, mc.NotifyKindRepeat, 2)
	d.writer.failNext(1)
	d.mustFail(ob2, 0)
	if s := d.outboxState(ob2); s.status != mc.OutboxPending || s.attempts != 1 {
		t.Fatalf("failure after re-arm = %+v", s)
	}
	d.expireBackoff(ob2)
	d.mustRun(1)
	if p := d.progress(alertID); p.count != 2 {
		t.Fatalf("notify_count after the re-armed repeat = %d", p.count)
	}
}

// 同一事件的行按入箱顺序派发：前一条未终结（哪怕在退避中）时后一条不会被领取。
func TestDispatch_OrderWithinAlert(t *testing.T) {
	d := newDispatchEnv(t)
	ctx := context.Background()
	alertID := d.firingAlert()
	first := d.outboxRow(alertID, mc.NotifyKindFiring, 1)
	if _, err := d.pool.Exec(ctx, "UPDATE monitor_alert SET status = 2, resolve_time = now() WHERE id = $1", alertID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	second := d.outboxRow(alertID, mc.NotifyKindResolved, 2)
	d.writer.failNext(1)
	d.mustFail(first, 0)
	d.mustRun(0) // 第一条在退避中，第二条被它挡住
	if d.outboxState(second).status != mc.OutboxPending {
		t.Fatal("the later row must wait for the earlier one")
	}
	d.expireBackoff(first)
	d.mustRun(2)
	var kinds []int32
	rows, _ := d.pool.Query(ctx, "SELECT kind FROM monitor_notify_log WHERE alert_id = $1 ORDER BY create_time, id", alertID)
	for rows.Next() {
		var k int32
		_ = rows.Scan(&k)
		kinds = append(kinds, k)
	}
	rows.Close()
	if !slices.Equal(kinds, []int32{mc.NotifyKindFiring, mc.NotifyKindResolved}) {
		t.Fatalf("dispatch order = %v", kinds)
	}
}

// 行锁领取：一个事务持有某行时，另一个事务领不到它（SKIP LOCKED），只能领到别的行。
func TestOutbox_ClaimSkipsLockedRows(t *testing.T) {
	d := newDispatchEnv(t)
	ctx := context.Background()
	a1, a2 := d.firingAlert(), d.firingAlert()
	ob1 := d.outboxRow(a1, mc.NotifyKindFiring, 1)
	time.Sleep(2 * time.Millisecond)
	ob2 := d.outboxRow(a2, mc.NotifyKindFiring, 1)
	txm := basedata.NewTxManager(d.d)
	holding, release := make(chan struct{}), make(chan struct{})
	var firstID string
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = txm.WithinTx(ctx, func(tx txrun.Tx) error {
			item, err := d.outbox.ClaimTx(ctx, tx, nil)
			if err != nil || item == nil {
				t.Errorf("first claim = %v, %v", item, err)
				close(holding)
				return err
			}
			firstID = item.ID
			close(holding)
			<-release
			return nil
		})
	}()
	<-holding
	secondID, thirdNil := "", false
	if err := txm.WithinTx(ctx, func(tx txrun.Tx) error {
		item, err := d.outbox.ClaimTx(ctx, tx, nil)
		if err != nil || item == nil {
			return fmt.Errorf("second claim = %v, %v", item, err)
		}
		secondID = item.ID
		// 两行都被占着：第三个领取者什么都拿不到，也不会阻塞。
		return txm.WithinTx(ctx, func(tx2 txrun.Tx) error {
			item, err := d.outbox.ClaimTx(ctx, tx2, nil)
			thirdNil = item == nil
			return err
		})
	}); err != nil {
		t.Fatal(err)
	}
	close(release)
	wg.Wait()
	if firstID != ob1 || secondID != ob2 || !thirdNil {
		t.Fatalf("claims = %s / %s / third nil=%v; want %s / %s / true", firstID, secondID, thirdNil, ob1, ob2)
	}
	// exclude 生效。
	_ = txm.WithinTx(ctx, func(tx txrun.Tx) error {
		item, err := d.outbox.ClaimTx(ctx, tx, []string{ob1})
		if err != nil || item == nil || item.ID != ob2 {
			t.Errorf("claim with exclude = %v, %v", item, err)
		}
		return nil
	})
}

// 多个派发实例并发：每一行恰好被处理一次——每个事件一条通知事件、一条通知记录，计数为 1。
func TestDispatch_ConcurrentWorkersProcessEachRowOnce(t *testing.T) {
	d := newDispatchEnv(t)
	ctx := context.Background()
	const alerts = 150
	ids := make([]string, alerts)
	for i := range ids {
		ids[i] = d.alert(alertRow{ruleID: d.rule.ID, ruleName: "cpu-high", groupID: d.group.ID, labels: fmt.Sprintf(`{"i":"%d"}`, i)})
		d.outboxRow(ids[i], mc.NotifyKindFiring, 1)
	}
	// 每个实例一个用例对象（各自的匹配器缓存），共用同一个库。
	const workers = 6
	var wg sync.WaitGroup
	processed := make([]int, workers)
	errs := make([]error, workers)
	for w := range workers {
		uc := d.newUsecase(d.env, d.writer)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				n, err := uc.Run(ctx)
				processed[w] += n
				if err != nil {
					errs[w] = err
					return
				}
				if n == 0 {
					return
				}
			}
		}()
	}
	wg.Wait()
	total := 0
	for w := range workers {
		if errs[w] != nil {
			t.Fatalf("worker %d: %v", w, errs[w])
		}
		total += processed[w]
	}
	if total != alerts {
		t.Fatalf("rows processed across workers = %d, want %d (a row was handled twice or skipped)", total, alerts)
	}
	for what, sql := range map[string]string{
		"outbox rows done":         "SELECT count(*) FROM monitor_notify_outbox WHERE status = 2 AND attempts = 0",
		"notify events":            "SELECT count(*) FROM notify_event",
		"distinct dedup keys":      "SELECT count(DISTINCT dedup_key) FROM notify_event",
		"notify logs":              "SELECT count(*) FROM monitor_notify_log",
		"alerts with one log":      "SELECT count(*) FROM (SELECT alert_id FROM monitor_notify_log GROUP BY alert_id HAVING count(*) = 1) x",
		"alerts with notify_count": "SELECT count(*) FROM monitor_alert WHERE notify_count = 1 AND last_notify_time IS NOT NULL",
	} {
		if n := d.count(sql); n != alerts {
			t.Errorf("%s = %d, want %d", what, n, alerts)
		}
	}
}

// core 登录账号经 monitor_core_role 获得的权限足以跑完本包的全部语句，且运行表的其他列写不了。
// 这里用超级用户连接 SET ROLE 成该角色；真实的登录账号见 TestCoreLoginRole。
func TestCoreRoleGrants(t *testing.T) {
	admin := newDispatchEnv(t)
	ctx := context.Background()
	dsn := testDSN(t)
	// 以 monitor_core_role 的身份建连接池（测试账号是超级用户，SET ROLE 后按该角色的权限检查）。
	d := openData(t, dsn+" options='-c role=monitor_core_role'")
	var role string
	if err := d.Pool.QueryRow(ctx, "SELECT current_user").Scan(&role); err != nil || role != "monitor_core_role" {
		t.Fatalf("current_user = %q err=%v", role, err)
	}
	exerciseAsCoreRole(t, admin, d)
}

// exerciseAsCoreRole 用连接池 d（其数据库身份只有 monitor_core_role 的权限）跑一遍本包的语句：配置表
// 全权、运行表只读加获准的列；不在授权内的写入必须被数据库拒绝。admin 是超级用户连接，代替评估引擎
// 写运行表。
func exerciseAsCoreRole(t *testing.T, admin *dispatchEnv, d *basedata.Data) {
	t.Helper()
	ctx := context.Background()
	e := reposOf(t, d)

	// 配置表：全权。
	g := e.group("", "as-core-role")
	ds := e.datasource("as-core-role", true)
	r := e.rule(g.ID, "as-core-role")
	// 分组：轻量树读取、规则计数、改名、删除（含删除前对屏蔽规则 / 订阅引用的检查）。
	tmp := e.group(g.ID, "tmp")
	if err := e.groups.Update(ctx, &biz.RuleGroup{ID: tmp.ID, Name: "tmp-2"}, uAdmin); err != nil {
		t.Fatalf("group update: %v", err)
	}
	from, to := time.Now().Add(-time.Hour), time.Now().Add(time.Hour)
	ref := &biz.Mute{ID: newID(t), Spec: biz.MuteSpec{GroupID: tmp.ID, Name: "ref", Reason: "r", Mode: 1, TimeType: 1, StartTime: &from, EndTime: &to, Enabled: true}}
	if err := e.mutes.Create(ctx, ref, uAlice); err != nil {
		t.Fatalf("mute on tmp: %v", err)
	}
	if err := e.groups.Delete(ctx, tmp.ID, uAdmin); kerrors.FromError(err).Metadata["mute_count"] != "1" {
		t.Fatalf("deleting a referenced group as the core role: %v, want MONITOR_GROUP_IN_USE with mute_count=1", err)
	}
	if err := e.mutes.Delete(ctx, ref.ID, 1, uAlice); err != nil {
		t.Fatalf("mute delete: %v", err)
	}
	if all, err := e.groups.ListAll(ctx); err != nil || len(all) < 3 {
		t.Fatalf("group tree: %d err=%v", len(all), err)
	}
	if counts, err := e.groups.RuleCounts(ctx); err != nil || counts[g.ID] != 1 {
		t.Fatalf("rule counts: %v err=%v", counts, err)
	}
	if err := e.groups.Delete(ctx, tmp.ID, uAdmin); err != nil {
		t.Fatalf("group delete: %v", err)
	}
	// 数据源：读、改（含凭据）、取凭据、选项与引用查询。
	if err := e.sources.Update(ctx, &biz.Datasource{ID: ds.ID, Name: "as-core-role", URL: ds.URL, AuthType: mc.AuthBearer, TimeoutMS: 5000, Enabled: true},
		biz.SecretChange{Set: true, Sealed: []byte("sealed")}, 1, uAdmin); err != nil {
		t.Fatalf("datasource update: %v", err)
	}
	if sealed, err := e.sources.GetSecret(ctx, ds.ID); err != nil || string(sealed) != "sealed" {
		t.Fatalf("datasource secret: %v", err)
	}
	if _, _, err := e.sources.List(ctx, biz.DatasourceFilter{Keyword: "core"}, 1, 10); err != nil {
		t.Fatalf("datasource list: %v", err)
	}
	if _, err := e.sources.ListOptions(ctx, true); err != nil {
		t.Fatalf("datasource options: %v", err)
	}
	if _, err := e.sources.Briefs(ctx, []string{ds.ID}); err != nil {
		t.Fatalf("datasource briefs: %v", err)
	}
	// 规则：版本、按名称 / 分组查找。
	if _, _, err := e.vers.List(ctx, r.ID, 1, 10); err != nil {
		t.Fatalf("rule versions: %v", err)
	}
	if _, err := e.vers.Get(ctx, r.ID, 1); err != nil {
		t.Fatalf("rule version: %v", err)
	}
	if _, err := e.rules.FindByNames(ctx, g.ID, []string{"as-core-role"}); err != nil {
		t.Fatalf("rules by name: %v", err)
	}
	if _, err := e.rules.ListByGroups(ctx, biz.GroupFilter{IDs: []string{g.ID}}, 10); err != nil {
		t.Fatalf("rules by group: %v", err)
	}
	if err := e.scopes.Replace(ctx, g.ID, []*biz.RoleScope{{RoleID: roleB, Level: 1}}, uAdmin); err != nil {
		t.Fatalf("role scope: %v", err)
	}
	r.Spec.Description = "x"
	if err := e.rules.Update(ctx, r, 1, biz.RuleChange{Action: mc.RuleActionUpdate, BumpSpecTime: true, Operator: uAlice}); err != nil {
		t.Fatalf("rule update: %v", err)
	}
	start, end := time.Now(), time.Now().Add(time.Hour)
	m := &biz.Mute{ID: newID(t), Spec: biz.MuteSpec{GroupID: g.ID, Name: "m", Reason: "r", Mode: 1, TimeType: 1, StartTime: &start, EndTime: &end, Enabled: true}}
	if err := e.mutes.Create(ctx, m, uAlice); err != nil {
		t.Fatalf("mute: %v", err)
	}
	m.Spec.Reason = "r2"
	if err := e.mutes.Update(ctx, m, 1, uAlice); err != nil {
		t.Fatalf("mute update: %v", err)
	}
	if _, _, err := e.mutes.List(ctx, biz.MuteFilter{Groups: biz.GroupFilter{All: true}, IncludeGlobal: true, Live: true}, 1, 10); err != nil {
		t.Fatalf("mute list: %v", err)
	}
	if _, err := e.mutes.Names(ctx, []string{m.ID}); err != nil {
		t.Fatalf("mute names: %v", err)
	}
	if err := e.mutes.Delete(ctx, m.ID, 2, uAlice); err != nil {
		t.Fatalf("mute delete: %v", err)
	}
	sub := &biz.Subscription{ID: newID(t), OwnerID: uAlice, Spec: biz.SubscriptionSpec{
		Name: "s", GroupIDs: []string{g.ID}, Severities: []int32{3}, OnFiring: true, Receivers: mc.Receivers{Self: true}, Enabled: true,
	}}
	if err := e.subs.Create(ctx, sub); err != nil {
		t.Fatalf("subscription: %v", err)
	}
	sub.Spec.Name = "s2"
	if err := e.subs.Update(ctx, sub, 1, uAlice); err != nil {
		t.Fatalf("subscription update: %v", err)
	}
	if _, _, err := e.subs.List(ctx, biz.SubscriptionFilter{ViewerID: uAlice, Groups: biz.GroupFilter{IDs: []string{g.ID}}, CoverGroupIDs: []string{g.ID}}, 1, 10); err != nil {
		t.Fatalf("subscription list: %v", err)
	}
	if items, total, err := e.subs.ListCandidates(ctx, biz.SubscriptionCandidate{GroupIDs: []string{g.ID}, RuleID: r.ID, DatasourceID: ds.ID, Severity: 3}, 1); err != nil || len(items) != 1 || total != 1 {
		t.Fatalf("subscription candidates: %d / %d err=%v", len(items), total, err)
	}
	if _, err := e.logs.SubscriptionHits(ctx, []string{sub.ID}, time.Now().Add(-time.Hour)); err != nil {
		t.Fatalf("subscription hits: %v", err)
	}
	if err := e.sources.Delete(ctx, ds.ID, uAdmin); err != nil {
		t.Fatalf("datasource delete: %v", err)
	}

	// 运行表：只读 + 获准的列。引擎的行由超级用户连接写入。
	alertID := admin.alert(alertRow{ruleID: r.ID, ruleName: "as-core-role", groupID: g.ID})
	ob := admin.outboxRow(alertID, mc.NotifyKindFiring, 1)
	if _, err := e.alerts.Get(ctx, alertID); err != nil {
		t.Fatalf("read alert: %v", err)
	}
	if items, err := e.alerts.Scan(ctx, biz.AlertFilter{Groups: biz.GroupFilter{IDs: []string{g.ID}}, FireStart: &from, FireEnd: &to, Labels: map[string]string{"a": "b"}}, 10); err != nil || len(items) != 0 {
		t.Fatalf("scan alerts: %d err=%v", len(items), err)
	}
	if _, err := e.alerts.MutedCounts(ctx, []string{m.ID}); err != nil {
		t.Fatalf("muted counts: %v", err)
	}
	if _, err := e.logs.ListByAlert(ctx, alertID, 10); err != nil {
		t.Fatalf("notify logs: %v", err)
	}
	if _, _, err := e.alerts.List(ctx, biz.AlertFilter{Groups: biz.GroupFilter{All: true}, RecipientID: uAlice}, 1, 10); err != nil {
		t.Fatalf("list alerts: %v", err)
	}
	if _, _, err := e.alerts.Aggregate(ctx, biz.AggregateBy{Kind: biz.AggregateByRule}, biz.AlertFilter{Groups: biz.GroupFilter{All: true}}, 10); err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	if _, err := e.evals.ListTasks(ctx, []string{r.ID}); err != nil {
		t.Fatalf("eval tasks: %v", err)
	}
	since := time.Now().Add(-time.Hour)
	if _, _, err := e.evals.ListRecords(ctx, biz.EvalRecordFilter{RuleID: r.ID, Start: &since}, 1, 10); err != nil {
		t.Fatalf("eval records: %v", err)
	}
	if _, _, err := e.rules.List(ctx, biz.RuleFilter{Groups: biz.GroupFilter{All: true}, Firing: &[]bool{true}[0], LastStatus: &[]int32{1}[0]}, 1, 10); err != nil {
		t.Fatalf("rule list touching run tables: %v", err)
	}
	// 发件箱：领取（行锁）、记失败、终结；通知记录与通知计数。
	txm := basedata.NewTxManager(d)
	if err := txm.WithinTx(ctx, func(tx txrun.Tx) error {
		item, err := e.outbox.ClaimTx(ctx, tx, nil)
		if err != nil || item == nil || item.ID != ob {
			return fmt.Errorf("claim = %v, %v", item, err)
		}
		if err := e.outbox.InsertLogTx(ctx, tx, &biz.NotifyLog{ID: newID(t), AlertID: alertID, Kind: 1, Seq: 1, Source: 1, Recipients: []string{uAlice}}); err != nil {
			return err
		}
		if err := e.outbox.TouchAlertTx(ctx, tx, alertID, 1); err != nil {
			return err
		}
		return errors.New("rollback on purpose")
	}); err == nil || !strings.Contains(err.Error(), "on purpose") {
		t.Fatalf("dispatch statements as core role: %v", err)
	}
	if dropped, err := e.outbox.Fail(ctx, ob, "boom"); err != nil || dropped {
		t.Fatalf("Fail as core role: %v", err)
	}
	if _, err := admin.pool.Exec(ctx, "UPDATE monitor_notify_outbox SET done_time = now() - interval '1 hour' WHERE id = $1", ob); err != nil {
		t.Fatal(err)
	}
	if err := txm.WithinTx(ctx, func(tx txrun.Tx) error {
		return e.outbox.FinishTx(ctx, tx, ob, mc.OutboxDone, "")
	}); err != nil {
		t.Fatalf("FinishTx as core role: %v", err)
	}
	if _, err := e.logs.IsRecipient(ctx, alertID, uAlice); err != nil {
		t.Fatalf("read notify log: %v", err)
	}

	// 反过来：不在授权内的写入必须被数据库拒绝（证明上面跑的确实受列级权限约束）。
	for what, sql := range map[string]string{
		"alert.status":            "UPDATE monitor_alert SET status = 3, resolve_time = now() WHERE id = '" + alertID + "'",
		"alert.muted":             "UPDATE monitor_alert SET muted = true WHERE id = '" + alertID + "'",
		"alert insert":            "INSERT INTO monitor_alert (id, fingerprint, rule_id, rule_name, group_id, datasource_id, datasource_name, severity, first_fire_time, last_fire_time) SELECT id, 'x', rule_id, 'x', group_id, datasource_id, 'x', 1, now(), now() FROM monitor_alert LIMIT 1",
		"alert delete":            "DELETE FROM monitor_alert",
		"outbox.create_time":      "UPDATE monitor_notify_outbox SET create_time = now()",
		"outbox.seq":              "UPDATE monitor_notify_outbox SET seq = 9",
		"outbox insert":           "INSERT INTO monitor_notify_outbox (id, alert_id, kind) SELECT gen_random_uuid(), alert_id, 2 FROM monitor_notify_outbox LIMIT 1",
		"outbox delete":           "DELETE FROM monitor_notify_outbox",
		"eval task update":        "UPDATE monitor_eval_task SET last_status = 1",
		"eval state delete":       "DELETE FROM monitor_eval_state",
		"eval record insert":      "INSERT INTO monitor_eval_record (id, rule_id, datasource_id, eval_time, status) VALUES (gen_random_uuid(), gen_random_uuid(), gen_random_uuid(), now(), 1)",
		"other domain's table":    `SELECT count(*) FROM "system_user"`,
		"notify_event (not ours)": "SELECT count(*) FROM notify_event",
	} {
		_, err := d.Pool.Exec(ctx, sql)
		var pgErr interface{ SQLState() string }
		if err == nil || !errors.As(err, &pgErr) || pgErr.SQLState() != "42501" {
			t.Errorf("%s: err = %v, want insufficient_privilege (42501)", what, err)
		}
	}
}
