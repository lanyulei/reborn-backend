//go:build integration

package data

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

const p2InhibitTables = "monitor_inhibit, monitor_record_rule, monitor_record_task"

// newInhibitEnv 清空本文件用到的第二期表（它们引用规则分组，必须先于 newEnv 的分组清理），测试结束再清一次。
func newInhibitEnv(t *testing.T) (*env, *inhibitRepo, *recordRuleRepo) {
	t.Helper()
	d := openData(t, testDSN(t))
	clean := func() {
		if _, err := d.Pool.Exec(context.Background(), "TRUNCATE TABLE "+p2InhibitTables); err != nil {
			t.Errorf("truncate p2 tables: %v", err)
		}
	}
	clean()
	e := newEnv(t)
	t.Cleanup(clean)
	return e, NewInhibitRepo(e.d), NewRecordRuleRepo(e.d)
}

func (e *env) writableDatasource(name, writeURL string) *biz.Datasource {
	e.t.Helper()
	d := &biz.Datasource{
		ID: newID(e.t), Name: name, Type: mc.DatasourceTypePrometheus, URL: "http://" + name + ":8428", AuthType: mc.AuthNone,
		TimeoutMS: 5000, Enabled: true, RemoteWriteURL: writeURL,
	}
	if err := e.sources.Create(context.Background(), d, nil, uAdmin); err != nil {
		e.t.Fatalf("create datasource %s: %v", name, err)
	}
	return d
}

func TestInhibitRepo(t *testing.T) {
	e, repo, _ := newInhibitEnv(t)
	ctx := context.Background()
	g := e.group("", "infra")
	sub := e.group(g.ID, "host")
	rule := e.rule(sub.ID, "host-down")
	v0 := e.cfgVersion(mc.CfgInhibit)

	spec := biz.InhibitSpec{
		GroupID: g.ID, Name: "主机失联", Description: "d", Enabled: true, EqualLabels: []string{"ident"},
		Source: mc.InhibitSelector{RuleIDs: []string{rule.ID}, Severities: []int32{4}, Matchers: []mc.Matcher{{Label: "alertname", Op: mc.OpEqual, Value: "HostDown"}}},
	}
	in := &biz.Inhibit{ID: newID(t), Spec: spec}
	if err := repo.Create(ctx, in, uAlice); err != nil {
		t.Fatal(err)
	}
	global := &biz.Inhibit{ID: newID(t), Spec: biz.InhibitSpec{Name: "全局", Source: spec.Source, Target: mc.InhibitSelector{Severities: []int32{1}}}}
	if err := repo.Create(ctx, global, uAdmin); err != nil {
		t.Fatal(err)
	}
	if got := e.cfgVersion(mc.CfgInhibit); got != v0+2 {
		t.Fatalf("cfg version = %d, want %d", got, v0+2)
	}
	wantErr(t, "missing group", repo.Create(ctx, &biz.Inhibit{ID: newID(t), Spec: biz.InhibitSpec{GroupID: newID(t), Name: "x", Source: spec.Source, EqualLabels: []string{"a"}}}, uAlice), biz.ErrGroupNotFound)
	if got := e.cfgVersion(mc.CfgInhibit); got != v0+2 {
		t.Fatal("a failed write must not bump the cfg version")
	}

	got, err := repo.Get(ctx, in.ID)
	if err != nil || got.Version != 1 || got.CreateUser != uAlice || got.Spec.GroupID != g.ID || !slices.Equal(got.Spec.EqualLabels, []string{"ident"}) ||
		!slices.Equal(got.Spec.Source.RuleIDs, []string{rule.ID}) || len(got.Spec.Source.Matchers) != 1 || !got.Spec.Target.Empty() {
		t.Fatalf("get = %+v err=%v", got, err)
	}
	if gg, _ := repo.Get(ctx, global.ID); gg.Spec.GroupID != "" || gg.Spec.Enabled {
		t.Fatalf("global = %+v", gg)
	}

	// 列表：分组过滤、是否含全局、关键字、启用状态。
	on := true
	for _, c := range []struct {
		name string
		f    biz.InhibitFilter
		want int64
	}{
		{"all", biz.InhibitFilter{Groups: biz.GroupFilter{All: true}, IncludeGlobal: true}, 2},
		{"all without global", biz.InhibitFilter{Groups: biz.GroupFilter{All: true}}, 1},
		{"group + global", biz.InhibitFilter{Groups: biz.GroupFilter{IDs: []string{g.ID}}, IncludeGlobal: true}, 2},
		{"other group + global", biz.InhibitFilter{Groups: biz.GroupFilter{IDs: []string{sub.ID}}, IncludeGlobal: true}, 1},
		{"other group only", biz.InhibitFilter{Groups: biz.GroupFilter{IDs: []string{sub.ID}}}, 0},
		{"keyword", biz.InhibitFilter{Groups: biz.GroupFilter{All: true}, IncludeGlobal: true, Keyword: "失联"}, 1},
		{"enabled", biz.InhibitFilter{Groups: biz.GroupFilter{All: true}, IncludeGlobal: true, Enabled: &on}, 1},
	} {
		items, total, err := repo.List(ctx, c.f, 1, 10)
		if err != nil || total != c.want || int64(len(items)) != c.want {
			t.Fatalf("list %s = %d/%d err=%v, want %d", c.name, len(items), total, err, c.want)
		}
	}
	if items, total, _ := repo.List(ctx, biz.InhibitFilter{Groups: biz.GroupFilter{All: true}, IncludeGlobal: true}, 2, 1); total != 2 || len(items) != 1 {
		t.Fatalf("paging = %d/%d", len(items), total)
	}
	if enabled, err := repo.ListEnabled(ctx, 10); err != nil || len(enabled) != 1 || enabled[0].ID != in.ID {
		t.Fatalf("list enabled = %+v err=%v", enabled, err)
	}

	// 抑制计数：只算告警中且被该规则抑制的事件。
	a1 := e.alert(alertRow{ruleID: rule.ID, ruleName: "cpu", groupID: sub.ID})
	a2 := e.alert(alertRow{ruleID: rule.ID, ruleName: "cpu", groupID: sub.ID})
	a3 := e.alert(alertRow{ruleID: rule.ID, ruleName: "cpu", groupID: sub.ID, status: mc.AlertResolved})
	if _, err := e.pool.Exec(ctx, "UPDATE monitor_alert SET inhibited = true, inhibit_kind = 1, inhibit_id = $1 WHERE id = ANY($2::uuid[])", in.ID, []string{a1, a2, a3}); err != nil {
		t.Fatal(err)
	}
	counts, err := repo.InhibitedCounts(ctx, []string{in.ID, global.ID})
	if err != nil || counts[in.ID] != 2 || len(counts) != 1 {
		t.Fatalf("counts = %v err=%v", counts, err)
	}

	// 更新：乐观锁、换分组、cfg bump。
	upd := &biz.Inhibit{ID: in.ID, Spec: spec}
	upd.Spec.GroupID, upd.Spec.Name, upd.Spec.Target = sub.ID, "改名", mc.InhibitSelector{Severities: []int32{2}}
	wantErr(t, "stale version", repo.Update(ctx, upd, 5, uBob), biz.ErrVersionConflict)
	wantErr(t, "missing", repo.Update(ctx, &biz.Inhibit{ID: newID(t), Spec: spec}, 1, uBob), biz.ErrInhibitNotFound)
	if err := repo.Update(ctx, upd, 1, uBob); err != nil {
		t.Fatal(err)
	}
	if got, _ := repo.Get(ctx, in.ID); got.Version != 2 || got.UpdateUser != uBob || got.CreateUser != uAlice || got.Spec.GroupID != sub.ID ||
		!slices.Equal(got.Spec.Target.Severities, []int32{2}) || !got.UpdateTime.After(got.CreateTime.Add(-time.Second)) {
		t.Fatalf("after update = %+v", got)
	}
	if got := e.cfgVersion(mc.CfgInhibit); got != v0+3 {
		t.Fatalf("cfg version = %d, want %d", got, v0+3)
	}

	// 删除：软删、名称仍可查、cfg bump；作用分组在有抑制规则时不可物理消失（外键）而软删不受限。
	wantErr(t, "delete stale", repo.Delete(ctx, in.ID, 1, uBob), biz.ErrVersionConflict)
	if err := repo.Delete(ctx, in.ID, 2, uBob); err != nil {
		t.Fatal(err)
	}
	_, err = repo.Get(ctx, in.ID)
	wantErr(t, "deleted", err, biz.ErrInhibitNotFound)
	wantErr(t, "delete twice", repo.Delete(ctx, in.ID, 3, uBob), biz.ErrInhibitNotFound)
	names, err := repo.InhibitNames(ctx, []string{in.ID, global.ID})
	if err != nil || names[in.ID] != "改名" || names[global.ID] != "全局" {
		t.Fatalf("names = %v err=%v", names, err)
	}
	if got := e.cfgVersion(mc.CfgInhibit); got != v0+4 {
		t.Fatalf("cfg version = %d, want %d", got, v0+4)
	}
}

func TestRecordRuleRepo(t *testing.T) {
	e, _, repo := newInhibitEnv(t)
	ctx := context.Background()
	g := e.group("", "infra")
	g2 := e.group("", "app")
	ds := e.writableDatasource("vm-w", "http://vminsert:8480/insert/0/prometheus/api/v1/write")
	ro := e.writableDatasource("vm-ro", "")
	spec := biz.RecordRuleSpec{
		GroupID: g.ID, Name: "job:req:rate5m", DatasourceID: ds.ID, Expr: "sum(rate(x[5m])) by (job)", IntervalS: 60,
		Labels: map[string]string{"team": "ops"}, Enabled: true,
	}
	r := &biz.RecordRule{ID: newID(t), Spec: spec}
	if err := repo.Create(ctx, r, uAlice); err != nil || r.Version != 1 {
		t.Fatalf("create err=%v version=%d", err, r.Version)
	}
	wantErr(t, "duplicate", repo.Create(ctx, &biz.RecordRule{ID: newID(t), Spec: spec}, uAlice), biz.ErrRecordRuleNameExists)
	other := spec
	other.GroupID = g2.ID
	r2 := &biz.RecordRule{ID: newID(t), Spec: other}
	if err := repo.Create(ctx, r2, uAlice); err != nil {
		t.Fatalf("same name in another group: %v", err)
	}
	bad := spec
	bad.Name, bad.GroupID = "x:y", newID(t)
	wantErr(t, "missing group", repo.Create(ctx, &biz.RecordRule{ID: newID(t), Spec: bad}, uAlice), biz.ErrGroupNotFound)
	bad = spec
	bad.Name, bad.DatasourceID = "x:y", newID(t)
	wantErr(t, "missing datasource", repo.Create(ctx, &biz.RecordRule{ID: newID(t), Spec: bad}, uAlice), biz.ErrRecordRuleInvalid)
	bad.DatasourceID = ro.ID
	wantErr(t, "no write url", repo.Create(ctx, &biz.RecordRule{ID: newID(t), Spec: bad}, uAlice), biz.ErrRecordRuleNoWriteURL)
	bad.Enabled = false
	if err := repo.Create(ctx, &biz.RecordRule{ID: newID(t), Spec: bad}, uAlice); err != nil {
		t.Fatalf("a disabled rule may sit on a datasource without a write url: %v", err)
	}

	got, err := repo.Get(ctx, r.ID)
	if err != nil || got.Task != nil || got.Spec.Labels["team"] != "ops" || got.UpdateUser != uAlice || got.Spec.IntervalS != 60 {
		t.Fatalf("get = %+v err=%v", got, err)
	}
	if id, err := repo.FindByName(ctx, g.ID, ds.ID, "job:req:rate5m"); err != nil || id != r.ID {
		t.Fatalf("find = %q err=%v", id, err)
	}
	if id, _ := repo.FindByName(ctx, g.ID, ds.ID, "nope"); id != "" {
		t.Fatalf("find missing = %q", id)
	}

	// 任务行由引擎写：LEFT JOIN 读出。
	if _, err := e.pool.Exec(ctx,
		`INSERT INTO monitor_record_task (record_rule_id, last_eval_time, last_status, last_error, last_duration_ms, last_series)
		 VALUES ($1, now(), $2, 'http: Bad Gateway', 12, 7)`, r.ID, mc.RecordWriteError); err != nil {
		t.Fatal(err)
	}
	got, _ = repo.Get(ctx, r.ID)
	if got.Task == nil || got.Task.LastStatus != mc.RecordWriteError || got.Task.LastSeries != 7 || got.Task.LastEvalTime == nil || got.Task.NextEvalTime == nil {
		t.Fatalf("task = %+v", got.Task)
	}
	writeErr, never, on := mc.RecordWriteError, mc.RecordNever, true
	for _, c := range []struct {
		name string
		f    biz.RecordRuleFilter
		want int64
	}{
		{"all", biz.RecordRuleFilter{Groups: biz.GroupFilter{All: true}}, 3},
		{"group", biz.RecordRuleFilter{Groups: biz.GroupFilter{IDs: []string{g2.ID}}}, 1},
		{"datasource", biz.RecordRuleFilter{Groups: biz.GroupFilter{All: true}, DatasourceID: ro.ID}, 1},
		{"keyword expr", biz.RecordRuleFilter{Groups: biz.GroupFilter{All: true}, Keyword: "rate(x"}, 3},
		{"keyword name", biz.RecordRuleFilter{Groups: biz.GroupFilter{All: true}, Keyword: "x:y"}, 1},
		{"enabled", biz.RecordRuleFilter{Groups: biz.GroupFilter{All: true}, Enabled: &on}, 2},
		{"status write error", biz.RecordRuleFilter{Groups: biz.GroupFilter{All: true}, LastStatus: &writeErr}, 1},
		{"status never", biz.RecordRuleFilter{Groups: biz.GroupFilter{All: true}, LastStatus: &never}, 2},
	} {
		items, total, err := repo.List(ctx, c.f, 1, 10)
		if err != nil || total != c.want || int64(len(items)) != c.want {
			t.Fatalf("list %s = %d/%d err=%v, want %d", c.name, len(items), total, err, c.want)
		}
	}

	// 更新：乐观锁、重名、停用后状态过滤按 0。
	upd := &biz.RecordRule{ID: r.ID, Spec: spec}
	upd.Spec.Enabled, upd.Spec.Description = false, "停一下"
	wantErr(t, "stale", repo.Update(ctx, upd, 9, uBob), biz.ErrVersionConflict)
	wantErr(t, "missing", repo.Update(ctx, &biz.RecordRule{ID: newID(t), Spec: spec}, 1, uBob), biz.ErrRecordRuleNotFound)
	if err := repo.Update(ctx, upd, 1, uBob); err != nil || upd.Version != 2 {
		t.Fatalf("update err=%v version=%d", err, upd.Version)
	}
	if _, total, _ := repo.List(ctx, biz.RecordRuleFilter{Groups: biz.GroupFilter{All: true}, LastStatus: &writeErr}, 1, 10); total != 0 {
		t.Fatal("a disabled rule must count as never-run")
	}
	clash := &biz.RecordRule{ID: r2.ID, Spec: spec}
	wantErr(t, "rename into a taken name", repo.Update(ctx, clash, 1, uBob), biz.ErrRecordRuleNameExists)

	// 数据源：仍被记录规则使用时不能删；仍有启用的记录规则时不能清除写入地址。
	wantErr(t, "delete datasource in use", e.sources.Delete(ctx, ds.ID, uAdmin), biz.ErrDatasourceInUse)
	cur, _ := e.sources.Get(ctx, ds.ID)
	cleared := *cur
	cleared.RemoteWriteURL = ""
	err = e.sources.Update(ctx, &cleared, biz.SecretChange{}, cur.Version, uAdmin)
	wantErr(t, "clear write url with enabled record rules", err, biz.ErrDatasourceInUse)
	if msg := kerrors.FromError(err).Message; !strings.Contains(msg, "record rule") || !strings.Contains(msg, "job:req:rate5m") {
		t.Fatalf("message = %q", msg)
	}
	// 停用剩下那条启用的规则后可以清除；此后启用被拒绝。
	off := &biz.RecordRule{ID: r2.ID, Spec: other}
	off.Spec.Enabled = false
	if err := repo.Update(ctx, off, 1, uBob); err != nil {
		t.Fatal(err)
	}
	if err := e.sources.Update(ctx, &cleared, biz.SecretChange{}, cur.Version, uAdmin); err != nil {
		t.Fatalf("clear write url once no enabled record rule uses it: %v", err)
	}
	upd.Spec.Enabled = true
	wantErr(t, "enable after the write url is gone", repo.Update(ctx, upd, 2, uBob), biz.ErrRecordRuleNoWriteURL)
	targets, err := e.sources.WriteTargets(ctx, []string{ds.ID, ro.ID, newID(t)})
	if err != nil || len(targets) != 2 || targets[ds.ID].HasWriteURL || !targets[ds.ID].Enabled || targets[ds.ID].Name != "vm-w" {
		t.Fatalf("write targets = %+v err=%v", targets, err)
	}

	// 删除：软删后名称可复用、数据源可删。
	wantErr(t, "delete stale", repo.Delete(ctx, r.ID, 1, uBob), biz.ErrVersionConflict)
	if err := repo.Delete(ctx, r.ID, 2, uBob); err != nil {
		t.Fatal(err)
	}
	_, err = repo.Get(ctx, r.ID)
	wantErr(t, "deleted", err, biz.ErrRecordRuleNotFound)
	again := spec
	again.Enabled = false
	if err := repo.Create(ctx, &biz.RecordRule{ID: newID(t), Spec: again}, uAlice); err != nil {
		t.Fatalf("name must be reusable after delete: %v", err)
	}
}

// p2LoginPool 新建一个只属于 monitor_core_role 的 LOGIN 角色并以它连接测试库；测试结束时删除。
// dup: 与 loginPool（monitor_round2_integration_test.go）同一做法，只是角色名按并行开发的约定带 monp2_f_ 前缀。
func p2LoginPool(t *testing.T, e *env) *basedata.Data {
	t.Helper()
	ctx := context.Background()
	buf := make([]byte, 12)
	if _, err := rand.Read(buf); err != nil {
		t.Fatal(err)
	}
	role, password := "monp2_f_"+hex.EncodeToString(buf[:4]), hex.EncodeToString(buf[4:])+"Aa1"
	if _, err := e.pool.Exec(ctx, fmt.Sprintf("CREATE ROLE %s LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS PASSWORD '%s' IN ROLE monitor_core_role", role, password)); err != nil {
		t.Fatalf("create login role: %v", err)
	}
	cfg := e.pool.Config().ConnConfig
	d := openData(t, fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", cfg.Host, cfg.Port, role, password, cfg.Database))
	// 后注册的先执行：先关连接（openData 的 cleanup 更晚注册、更早执行），再删角色。
	t.Cleanup(func() {
		d.Pool.Close()
		if _, err := e.pool.Exec(context.Background(), "DROP ROLE IF EXISTS "+role); err != nil {
			t.Errorf("drop login role %s: %v", role, err)
		}
	})
	var current string
	var super bool
	if err := d.Pool.QueryRow(ctx, "SELECT current_user::text, (SELECT rolsuper FROM pg_roles WHERE rolname = current_user)").Scan(&current, &super); err != nil || current != role || super {
		t.Fatalf("connected as %s super=%v err=%v", current, super, err)
	}
	return d
}

// 以真实的受限登录账号（只是 monitor_core_role 的成员）跑通抑制规则、记录规则与数据源新增语句的全部读写。
func TestInhibitRecord_CoreLoginRole(t *testing.T) {
	admin, _, _ := newInhibitEnv(t)
	ctx := context.Background()
	d := p2LoginPool(t, admin)
	e := reposOf(t, d)
	inhibits, records := NewInhibitRepo(d), NewRecordRuleRepo(d)

	g := e.group("", "as-core-role")
	rule := e.rule(g.ID, "r")
	ds := e.writableDatasource("as-core-role", "http://vminsert:8480/write")
	in := &biz.Inhibit{ID: newID(t), Spec: biz.InhibitSpec{
		GroupID: g.ID, Name: "n", Enabled: true, EqualLabels: []string{"ident"},
		Source: mc.InhibitSelector{RuleIDs: []string{rule.ID}},
	}}
	if err := inhibits.Create(ctx, in, uAlice); err != nil {
		t.Fatalf("create inhibit: %v", err)
	}
	in.Spec.Name = "n2"
	if err := inhibits.Update(ctx, in, 1, uAlice); err != nil {
		t.Fatalf("update inhibit: %v", err)
	}
	if _, _, err := inhibits.List(ctx, biz.InhibitFilter{Groups: biz.GroupFilter{IDs: []string{g.ID}}, IncludeGlobal: true, Keyword: "n"}, 1, 10); err != nil {
		t.Fatalf("list inhibits: %v", err)
	}
	if _, err := inhibits.ListEnabled(ctx, 10); err != nil {
		t.Fatalf("list enabled: %v", err)
	}
	if _, err := inhibits.InhibitedCounts(ctx, []string{in.ID}); err != nil {
		t.Fatalf("inhibited counts: %v", err)
	}
	if _, err := inhibits.InhibitNames(ctx, []string{in.ID}); err != nil {
		t.Fatalf("names: %v", err)
	}
	if err := inhibits.Delete(ctx, in.ID, 2, uAlice); err != nil {
		t.Fatalf("delete inhibit: %v", err)
	}

	r := &biz.RecordRule{ID: newID(t), Spec: biz.RecordRuleSpec{GroupID: g.ID, Name: "a:b", DatasourceID: ds.ID, Expr: "up", IntervalS: 60, Enabled: true}}
	if err := records.Create(ctx, r, uAlice); err != nil {
		t.Fatalf("create record rule: %v", err)
	}
	status := mc.RecordOK
	if _, _, err := records.List(ctx, biz.RecordRuleFilter{Groups: biz.GroupFilter{IDs: []string{g.ID}}, LastStatus: &status, Keyword: "a"}, 1, 10); err != nil {
		t.Fatalf("list record rules (joins monitor_record_task): %v", err)
	}
	if _, err := records.FindByName(ctx, g.ID, ds.ID, "a:b"); err != nil {
		t.Fatalf("find: %v", err)
	}
	if _, err := e.sources.WriteTargets(ctx, []string{ds.ID}); err != nil {
		t.Fatalf("write targets: %v", err)
	}
	wantErr(t, "delete datasource in use", e.sources.Delete(ctx, ds.ID, uAdmin), biz.ErrDatasourceInUse)
	cur, _ := e.sources.Get(ctx, ds.ID)
	cleared := *cur
	cleared.RemoteWriteURL = ""
	wantErr(t, "clear write url", e.sources.Update(ctx, &cleared, biz.SecretChange{}, cur.Version, uAdmin), biz.ErrDatasourceInUse)
	r.Spec.Enabled = false
	if err := records.Update(ctx, r, 1, uAlice); err != nil {
		t.Fatalf("update record rule: %v", err)
	}
	if _, err := records.Get(ctx, r.ID); err != nil {
		t.Fatalf("get: %v", err)
	}
	if err := records.Delete(ctx, r.ID, 2, uAlice); err != nil {
		t.Fatalf("delete record rule: %v", err)
	}
	if err := e.sources.Delete(ctx, ds.ID, uAdmin); err != nil {
		t.Fatalf("delete datasource: %v", err)
	}
	// 任务表归引擎：受限账号写不了。
	if _, err := d.Pool.Exec(ctx, "INSERT INTO monitor_record_task (record_rule_id) VALUES ($1)", r.ID); err == nil {
		t.Fatal("core role must not be able to write monitor_record_task")
	}
}
