//go:build integration

package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/monitor/internal/biz"
	"github.com/lanyulei/reborn/backend/monitor/internal/promtest"

	"github.com/jackc/pgx/v5/pgconn"
)

// 第二、三期的集成测试。全部仓储以受限登录角色运行（newEnv）：新语句超出 0076 的授权会直接报 42501。
// 配置数据经 admin 写入；模拟 core 的运行期写入一律经 asCore（monitor_core_role 的授权范围）。

func (e *env) bump(name string) {
	e.t.Helper()
	e.exec("UPDATE monitor_cfg_version SET version = version + 1 WHERE name = $1", name)
}

func (e *env) set(ruleID, assignments string, args ...any) {
	e.t.Helper()
	e.exec("UPDATE monitor_rule SET "+assignments+", update_time = now() WHERE id = $1", append([]any{ruleID}, args...)...)
}

func (e *env) ci(name, ip, env, status string, upstream ...string) string {
	e.t.Helper()
	id := e.id()
	if upstream == nil {
		upstream = []string{}
	}
	e.exec(`INSERT INTO monitor_ci_index (ci_id, model_code, name, env, status, owner, tags, attrs, upstream_ids)
		VALUES ($1, 'host', $2, $3, $4, 'alice', '{core}', jsonb_build_object('ip', jsonb_build_array($5::text)), $6::uuid[])`,
		id, name, env, status, ip, upstream)
	e.bump(monitorcontract.CfgCIIndex)
	return id
}

func (e *env) inhibit(sourceKind, targetKind string, equal ...string) string {
	e.t.Helper()
	id := e.id()
	sel := func(kind string) string {
		return fmt.Sprintf(`{"matchers":[{"label":"kind","op":"==","value":%q}]}`, kind)
	}
	e.exec(`INSERT INTO monitor_inhibit (id, name, source, target, equal_labels) VALUES ($1, $2, $3::jsonb, $4::jsonb, $5)`,
		id, "inh-"+id[len(id)-12:], sel(sourceKind), sel(targetKind), equal)
	e.bump(monitorcontract.CfgInhibit)
	return id
}

// escalation 建一条两级策略：15 分钟未认领（每 15 分钟重复、至多 3 次）、60 分钟未恢复（只通知一次）。
func (e *env) escalation(mode int32) string {
	e.t.Helper()
	id := e.id()
	e.exec(`INSERT INTO monitor_escalation (id, name, mode, levels) VALUES ($1, $2, $3, $4::jsonb)`, id, "esc-"+id[len(id)-12:], mode,
		`[{"trigger":1,"after_m":15,"targets":{"user_ids":["018f3e1a-0000-7000-8000-0000000000a1"]},"repeat_interval_m":15,"max_notify":3},
		  {"trigger":2,"after_m":60,"targets":{"user_ids":["018f3e1a-0000-7000-8000-0000000000a2"]},"repeat_interval_m":0,"max_notify":0}]`)
	e.bump(monitorcontract.CfgEscalation)
	return id
}

// timeline 某告警的时间线类型（按写入顺序）。
func (e *env) timeline(alertID string) string {
	e.t.Helper()
	rows, err := e.admin.Query(context.Background(),
		"SELECT type FROM monitor_alert_event WHERE alert_id = $1 ORDER BY create_time, id", alertID)
	if err != nil {
		e.t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var typ string
		if err := rows.Scan(&typ); err != nil {
			e.t.Fatal(err)
		}
		out = append(out, typ)
	}
	return strings.Join(out, ",")
}

// actions 某告警的事件动作 "kind/status"（按 kind）。
func (e *env) actions(alertID string) string {
	e.t.Helper()
	var out *string
	if err := e.admin.QueryRow(context.Background(),
		"SELECT string_agg(kind || '/' || status, ',' ORDER BY kind) FROM monitor_alert_action WHERE alert_id = $1", alertID).Scan(&out); err != nil {
		e.t.Fatal(err)
	}
	if out == nil {
		return ""
	}
	return *out
}

type p2Alert struct {
	ID                              string
	Status, Level, InhibitKind      int
	Inhibited, Muted                bool
	MuteID, InhibitID, InhibitAlert *string
	CI, ClosedBy                    *string
	CloseReason                     string
	Labels                          map[string]string
}

// p2Alerts 某规则的告警（按首次触发时间）及第二期的列。
func (e *env) p2Alerts(ruleID string) []p2Alert {
	e.t.Helper()
	rows, err := e.admin.Query(context.Background(),
		`SELECT id::text, status, escalation_level, inhibit_kind, inhibited, muted, mute_id::text, inhibit_id::text, inhibit_alert_id::text,
		        ci_id::text, closed_by::text, close_reason, labels
		 FROM monitor_alert WHERE rule_id = $1 ORDER BY first_fire_time, id`, ruleID)
	if err != nil {
		e.t.Fatal(err)
	}
	defer rows.Close()
	var out []p2Alert
	for rows.Next() {
		var a p2Alert
		if err := rows.Scan(&a.ID, &a.Status, &a.Level, &a.InhibitKind, &a.Inhibited, &a.Muted, &a.MuteID, &a.InhibitID, &a.InhibitAlert,
			&a.CI, &a.ClosedBy, &a.CloseReason, &a.Labels); err != nil {
			e.t.Fatal(err)
		}
		out = append(out, a)
	}
	return out
}

func (e *env) p2Alert(ruleID string) p2Alert {
	e.t.Helper()
	all := e.p2Alerts(ruleID)
	if len(all) != 1 {
		e.t.Fatalf("want exactly 1 alert, got %d", len(all))
	}
	return all[0]
}

func str(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// —— 缓存与失效 ——

func TestP2CachesLoadAndInvalidate(t *testing.T) {
	e := newEnv(t)
	prom := promtest.New(t)
	e.datasource(prom.URL)
	g := e.engine("")
	ctx := context.Background()
	refresh := func() {
		t.Helper()
		time.Sleep(2100 * time.Millisecond) // 版本轮询的限频
		if err := g.cache.Refresh(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.cache.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if g.cache.CIs().Len() != 0 || g.cache.Calendar() != nil || g.cache.Inhibitor().Len() != 0 {
		t.Fatal("empty tables must load as empty caches")
	}

	host := e.ci("host-1", "10.0.0.1", "prod", "running")
	vm := e.ci("vm-1", "10.0.0.2", "prod", "maintenance", host)
	gone := e.ci("old-1", "10.0.0.3", "prod", "running")
	e.exec("UPDATE monitor_ci_index SET valid = false WHERE ci_id = $1", gone)
	inh := e.inhibit("down", "cpu", "instance")
	esc := e.escalation(monitorcontract.EscalationModeTakeover)
	e.exec(`UPDATE monitor_calendar SET config = '{"timezone":"Asia/Shanghai","weekdays":[1,2,3,4,5],"segments":[{"start":"09:00","end":"18:00"}],"holidays":[],"workdays":[]}'`)
	e.bump(monitorcontract.CfgCalendar)
	refresh()

	cis := g.cache.CIs()
	if cis.Len() != 2 || cis.Get(gone) != nil {
		t.Fatalf("ci index len = %d", cis.Len())
	}
	if c := cis.Get(vm); c == nil || c.Name != "vm-1" || c.ModelCode != "host" || c.Env != "prod" || c.Owner != "alice" || !c.Maintenance() ||
		!slices.Equal(c.Tags, []string{"core"}) || !slices.Equal(c.Attrs["ip"], []string{"10.0.0.2"}) || !slices.Equal(c.UpstreamIDs, []string{host}) {
		t.Fatalf("ci = %+v", c)
	}
	if found, _ := cis.Lookup(monitorcontract.CIMatch{}, map[string]string{"instance": "10.0.0.1:9100"}); found == nil || found.ID != host {
		t.Fatalf("lookup by instance → ip = %+v", found)
	}
	monday := time.Date(2026, 11, 16, 2, 0, 0, 0, time.UTC) // 周一 10:00 上海
	if cal := g.cache.Calendar(); cal == nil || !cal.Working(monday) || cal.Working(monday.Add(10*time.Hour)) {
		t.Fatalf("calendar = %v", g.cache.Calendar())
	}
	if g.cache.Inhibitor().Len() != 1 {
		t.Fatalf("inhibit rules = %d", g.cache.Inhibitor().Len())
	}
	if p := g.cache.Escalation(esc); p == nil || p.Mode != monitorcontract.EscalationModeTakeover || len(p.Levels) != 2 ||
		p.Levels[0].AfterM != 15 || p.Levels[0].MaxNotify != 3 || p.Levels[1].Trigger != monitorcontract.EscalationTriggerUnresolved {
		t.Fatalf("escalation = %+v", p)
	}

	// 失效：core 改表并在同事务里给版本号加一（这里分两条语句写，效果相同）。
	e.exec("UPDATE monitor_ci_index SET status = 'running', env = 'staging', sync_time = now() WHERE ci_id = $1", vm)
	e.exec("UPDATE monitor_ci_index SET valid = false, sync_time = now() WHERE ci_id = $1", host)
	e.bump(monitorcontract.CfgCIIndex)
	e.exec("UPDATE monitor_inhibit SET enabled = false WHERE id = $1", inh)
	e.bump(monitorcontract.CfgInhibit)
	e.exec("UPDATE monitor_escalation SET delete_time = now() WHERE id = $1", esc)
	e.bump(monitorcontract.CfgEscalation)
	e.exec("UPDATE monitor_calendar SET config = '{}'")
	e.bump(monitorcontract.CfgCalendar)
	refresh()
	cis = g.cache.CIs()
	if cis.Len() != 1 || cis.Get(host) != nil || cis.Get(vm).Maintenance() || cis.Get(vm).Env != "staging" {
		t.Fatalf("ci index after the incremental reload: len = %d, vm = %+v", cis.Len(), cis.Get(vm))
	}
	if g.cache.Inhibitor().Len() != 0 || g.cache.Escalation(esc) != nil || g.cache.Calendar() != nil {
		t.Fatal("disabled inhibit rule, deleted escalation and emptied calendar must leave the cache")
	}

	// 抑制规则挂在分组上：分组被删除后规则不再加载（分组版本变化触发重读）。
	child := e.id()
	root := monitorcontract.RootGroupID
	e.exec("INSERT INTO monitor_rule_group (id, parent_id, path, name) VALUES ($1, $2, $3, 'child')", child, root, "/"+root+"/"+child+"/")
	scoped := e.inhibit("a", "b", "host")
	e.exec("UPDATE monitor_inhibit SET group_id = $2 WHERE id = $1", scoped, child)
	e.bump(monitorcontract.CfgInhibit)
	refresh()
	if g.cache.Inhibitor().Len() != 1 {
		t.Fatalf("inhibit rules = %d", g.cache.Inhibitor().Len())
	}
	e.exec("UPDATE monitor_rule_group SET delete_time = now() WHERE id = $1", child)
	e.bump(monitorcontract.CfgRuleGroup)
	refresh()
	if g.cache.Inhibitor().Len() != 0 {
		t.Fatal("an inhibit rule whose group was deleted must not be loaded")
	}
}

// —— 流水线端到端：改写 / 富化 / 丢弃 / 范围 / 维护态 / 工作日历 ——

func TestP2PipelineEndToEnd(t *testing.T) {
	e := newEnv(t)
	prom := promtest.New(t)
	e.datasource(prom.URL)
	prod := e.ci("web-1", "10.0.0.1", "prod", "maintenance")
	e.ci("web-2", "10.0.0.2", "dev", "running")
	ruleID := e.rule(ruleSpec{Annotations: map[string]string{"summary": "{{ $labels.host }} / {{ $labels.ci_env }}"}})
	e.set(ruleID, `relabel = '[{"action":"replace","source_labels":["instance"],"regex":"(.+):\\d+","target_label":"host","replacement":"$1"}]',
		drop_expr = '{{ if eq $labels.ci_env "dev" }}true{{ end }}',
		ci_match = '{"label":"host","by":"attr","attr":"ip","enrich_tags":["core"]}',
		resource_scope = '{"enabled":true,"model_codes":["host"]}',
		actions = '{"ticket":{"enabled":true,"template_id":"018f3e1a-0000-7000-8000-0000000000b1","agent_user_id":"018f3e1a-0000-7000-8000-0000000000b2"},"workflow":{},"ai_summary":true}'`)
	prom.Set(upDown, promtest.S(0, "instance", "10.0.0.1:9100"), promtest.S(0, "instance", "10.0.0.2:9100"), promtest.S(0, "instance", "10.0.0.9:9100"))
	g := e.engine("")
	g.materialize()
	g.evalDue()

	a := e.p2Alert(ruleID)
	want := map[string]string{
		"instance": "10.0.0.1:9100", "host": "10.0.0.1", "ci_env": "prod", "ci_owner": "alice", "ci_model": "host", "ci_tag_core": "true",
	}
	if fmt.Sprint(a.Labels) != fmt.Sprint(want) || str(a.CI) != prod {
		t.Fatalf("labels = %v, ci = %s", a.Labels, str(a.CI))
	}
	// 维护态：只屏蔽通知，muted = true 而 mute_id 为空；不入箱、不入队事件动作。
	if !a.Muted || a.MuteID != nil || a.Inhibited || len(e.outbox(a.ID)) != 0 || e.actions(a.ID) != "" || e.timeline(a.ID) != "fire,mute" {
		t.Fatalf("alert = %+v, outbox = %v, actions = %q, timeline = %s", a, e.outbox(a.ID), e.actions(a.ID), e.timeline(a.ID))
	}
	if got := e.oneAlert(ruleID).Annotations["summary"]; got != "10.0.0.1 / prod" {
		t.Fatalf("annotation = %q", got)
	}
	rec := e.lastRecord(ruleID)
	var got []string
	for _, p := range rec.Detail.Points {
		got = append(got, p.Step)
		if p.Step == monitorcontract.StepMutedMaintenance && p.CIID != prod {
			t.Fatalf("point = %+v", p)
		}
	}
	slices.Sort(got)
	if !slices.Equal(got, []string{"dropped", "muted_maintenance", "out_of_scope"}) || rec.Anomalies != 1 || rec.Series != 3 {
		t.Fatalf("steps = %v, record = %+v", got, rec)
	}

	// 维护结束：按一次新通知处理，事件动作这时才入队（快照了规则当时的配置）。
	e.exec("UPDATE monitor_ci_index SET status = 'running', sync_time = now() WHERE ci_id = $1", prod)
	e.bump(monitorcontract.CfgCIIndex)
	time.Sleep(2100 * time.Millisecond)
	g.evalDue()
	a = e.p2Alert(ruleID)
	if a.Muted || !equalStrings(e.outbox(a.ID), []string{"1/1/1"}) || e.actions(a.ID) != "1/1,3/1" || e.timeline(a.ID) != "fire,mute,unmute" {
		t.Fatalf("alert = %+v, outbox = %v, actions = %q, timeline = %s", a, e.outbox(a.ID), e.actions(a.ID), e.timeline(a.ID))
	}
	var payload monitorcontract.ActionPayload
	var raw []byte
	if err := e.admin.QueryRow(context.Background(), "SELECT payload FROM monitor_alert_action WHERE alert_id = $1 AND kind = 1", a.ID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &payload); err != nil || payload.TemplateID != "018f3e1a-0000-7000-8000-0000000000b1" || payload.AgentUserID != "018f3e1a-0000-7000-8000-0000000000b2" {
		t.Fatalf("payload = %+v, %v", payload, err)
	}
	// 重复评估不会再写动作行；core 改了状态的行不被覆盖。
	e.asCore("UPDATE monitor_alert_action SET status = 2, done_time = now() WHERE alert_id = $1 AND kind = 1", a.ID)
	e.dispatch()
	g.evalDue()
	if e.actions(a.ID) != "1/2,3/1" {
		t.Fatalf("actions = %q", e.actions(a.ID))
	}
	// 恢复：时间线记恢复；开了自动建单的规则入队 ticket_note。
	prom.Set(upDown)
	g.evalDue()
	a = e.p2Alert(ruleID)
	if a.Status != int(monitorcontract.AlertResolved) || e.actions(a.ID) != "1/2,3/1,4/1" || e.timeline(a.ID) != "fire,mute,unmute,resolve" {
		t.Fatalf("alert = %+v, actions = %q, timeline = %s", a, e.actions(a.ID), e.timeline(a.ID))
	}

	// 生效时间「仅工作时间」：日历说现在不上班（全周不工作日以外的写法：只把今天之外的某一天设为工作日）。
	e.set(ruleID, `effective = '{"timezone":"","windows":[],"mode":"work_time"}'`)
	weekday := e.count("SELECT extract(dow FROM now() AT TIME ZONE 'Asia/Shanghai')::int")
	e.exec(`UPDATE monitor_calendar SET config = jsonb_build_object('timezone', 'Asia/Shanghai', 'weekdays', jsonb_build_array($1::int),
		'segments', '[{"start":"00:00","end":"24:00"}]'::jsonb, 'holidays', '[]'::jsonb, 'workdays', '[]'::jsonb)`, (weekday+3)%7)
	e.bump(monitorcontract.CfgCalendar)
	time.Sleep(2100 * time.Millisecond)
	prom.Set(upDown, promtest.S(0, "instance", "10.0.0.1:9100"))
	g.evalDue()
	if rec := e.lastRecord(ruleID); rec.Status != int(monitorcontract.EvalInactive) {
		t.Fatalf("off hours: status = %d", rec.Status)
	}
	e.set(ruleID, `effective = '{"timezone":"","windows":[],"mode":"off_time"}'`)
	g.evalDue()
	if rec := e.lastRecord(ruleID); rec.Status != int(monitorcontract.EvalFiring) {
		t.Fatalf("off_time rule during off hours: status = %d", rec.Status)
	}
}

// —— 抑制端到端：跨规则、留档、解除后通知 ——

func TestP2InhibitEndToEnd(t *testing.T) {
	e := newEnv(t)
	prom := promtest.New(t)
	e.datasource(prom.URL)
	const srcExpr, dstExpr = "up == 0", "cpu > 90"
	source := e.rule(ruleSpec{Labels: map[string]string{"kind": "down"}})
	target := e.rule(ruleSpec{Labels: map[string]string{"kind": "cpu"}, Queries: []monitorcontract.Query{{Expr: dstExpr, Severity: monitorcontract.SeverityCritical}}})
	inh := e.inhibit("down", "cpu", "instance")
	prom.Set(srcExpr, promtest.S(0, "instance", "a"))
	g := e.engine("")
	g.materialize()
	g.evalDue() // 源告警触发
	src := e.p2Alert(source)
	prom.Set(dstExpr, promtest.S(95, "instance", "a"), promtest.S(96, "instance", "b"))
	g.evalDue()

	var hit, free p2Alert
	for _, a := range e.p2Alerts(target) {
		if a.Labels["instance"] == "a" {
			hit = a
		} else {
			free = a
		}
	}
	// 被抑制的事件留档并标注被谁抑制；相等标签不同的不受影响。
	if !hit.Inhibited || hit.InhibitKind != int(monitorcontract.InhibitRule) || str(hit.InhibitID) != inh || str(hit.InhibitAlert) != src.ID ||
		len(e.outbox(hit.ID)) != 0 || e.timeline(hit.ID) != "fire,inhibit" {
		t.Fatalf("inhibited alert = %+v, outbox = %v, timeline = %s", hit, e.outbox(hit.ID), e.timeline(hit.ID))
	}
	if free.Inhibited || !equalStrings(e.outbox(free.ID), []string{"1/1/1"}) || src.Inhibited {
		t.Fatalf("unrelated alert = %+v, source = %+v", free, src)
	}
	rec := e.lastRecord(target)
	found := false
	for _, p := range rec.Detail.Points {
		if p.Step == monitorcontract.StepInhibitedRule && p.InhibitAlertID == src.ID {
			found = true
		}
	}
	if !found {
		t.Fatalf("eval record points = %+v", rec.Detail.Points)
	}
	// 持续被抑制：不通知，时间线不增长。
	e.dispatch()
	g.evalDue()
	g.evalDue()
	if len(e.outbox(hit.ID)) != 0 || e.timeline(hit.ID) != "fire,inhibit" {
		t.Fatalf("outbox = %v, timeline = %s", e.outbox(hit.ID), e.timeline(hit.ID))
	}
	// 源告警恢复 → 抑制解除 → 目标按一次新通知处理。
	prom.Set(srcExpr)
	g.evalDue()
	g.evalDue() // 两条规则的任务同批评估：再评一轮，保证目标看到源已恢复之后的快照
	after := e.p2Alerts(target)
	for _, a := range after {
		if a.ID == hit.ID {
			hit = a
		}
	}
	if hit.Inhibited || hit.InhibitKind != 0 || hit.InhibitID != nil || hit.InhibitAlert != nil ||
		!equalStrings(e.outbox(hit.ID), []string{"1/1/1"}) || e.timeline(hit.ID) != "fire,inhibit,uninhibit" {
		t.Fatalf("after the lift: alert = %+v, outbox = %v, timeline = %s", hit, e.outbox(hit.ID), e.timeline(hit.ID))
	}
}

// 抖动的抑制只在时间线里留下有上限的行数。
func TestP2TimelineIsCappedPerAlert(t *testing.T) {
	e := newEnv(t)
	ds := e.datasource("http://127.0.0.1:1")
	ruleID := e.rule(ruleSpec{})
	e.engine("").materialize()
	alertID := e.seedAlert(ruleID, ds, "fp-flap", monitorcontract.AlertFiring, 0)
	rounds := NewRoundRepo(e.d)
	tasks := NewTaskRepo(e.d)
	ctx := context.Background()
	write := func(events []biz.AlertEventInsert) {
		t.Helper()
		e.due()
		claimed, err := tasks.Claim(ctx, "cap-test", 1, time.Minute, time.Second, time.Second)
		if err != nil || len(claimed) != 1 {
			t.Fatalf("Claim() = %d, %v", len(claimed), err)
		}
		err = rounds.Commit(ctx, claimed[0], false, func([]biz.SeriesState) (*biz.Round, error) {
			return &biz.Round{Now: time.Now(), Interval: time.Minute, Status: monitorcontract.EvalFiring, Transition: &biz.Transition{Events: events, FiringCount: 1}}, nil
		})
		if err != nil {
			t.Fatalf("Commit() error = %v", err)
		}
	}
	batch := func(n int, types ...string) []biz.AlertEventInsert {
		out := make([]biz.AlertEventInsert, 0, n)
		for i := range n {
			out = append(out, biz.AlertEventInsert{ID: e.id(), AlertID: alertID, Type: types[i%len(types)]})
		}
		return out
	}
	for range 5 {
		write(batch(50, monitorcontract.AlertEventInhibit, monitorcontract.AlertEventUninhibit, monitorcontract.AlertEventMute, monitorcontract.AlertEventUnmute))
	}
	if n := e.count("SELECT count(*) FROM monitor_alert_event WHERE alert_id = $1", alertID); n != maxFlapEvents {
		t.Fatalf("flap events = %d, want the cap %d", n, maxFlapEvents)
	}
	// 封顶之后：会抖动的几类不再写；触发 / 恢复照写。
	write(append(batch(3, monitorcontract.AlertEventEscalate, monitorcontract.AlertEventInhibit), biz.AlertEventInsert{ID: e.id(), AlertID: alertID, Type: monitorcontract.AlertEventResolve, Severity: 2}))
	if n := e.count("SELECT count(*) FROM monitor_alert_event WHERE alert_id = $1", alertID); n != maxFlapEvents+1 {
		t.Fatalf("events = %d", n)
	}
	if e.count(`SELECT count(*) FROM monitor_alert_event WHERE alert_id = $1 AND type = 'resolve' AND detail = '{"severity":2}'`, alertID) != 1 {
		t.Fatal("the resolve row must be written with its detail")
	}
}

// —— 升级端到端：core 的写入经它被授权的列 ——

func TestP2EscalationEndToEnd(t *testing.T) {
	e := newEnv(t)
	prom := promtest.New(t)
	e.datasource(prom.URL)
	esc := e.escalation(monitorcontract.EscalationModeStack)
	ruleID := e.rule(ruleSpec{RepeatM: 30})
	e.set(ruleID, "escalation_id = $2", esc)
	prom.Set(upDown, promtest.S(0, "instance", "a"))
	g := e.engine("")
	g.materialize()
	round := func(minutes int) {
		t.Helper()
		e.age(time.Duration(minutes) * time.Minute)
		g.evalDue()
	}
	round(0)
	a := e.p2Alert(ruleID)
	e.dispatch()
	round(10)
	if !equalStrings(e.outbox(a.ID), []string{"1/1/2"}) || e.p2Alert(ruleID).Level != 0 {
		t.Fatalf("before the first level: outbox = %v", e.outbox(a.ID))
	}
	// 15 分钟未认领：升到第 1 级，发件箱 kind 4、level 1、seq = 100001。
	round(6)
	if got := e.outbox(a.ID); !equalStrings(got, []string{"1/1/2", "4/100001/1"}) {
		t.Fatalf("outbox = %v", got)
	}
	if e.count("SELECT count(*) FROM monitor_notify_outbox WHERE alert_id = $1 AND kind = 4 AND level = 1 AND seq = 100001", a.ID) != 1 ||
		e.p2Alert(ruleID).Level != 1 || e.timeline(a.ID) != "fire,escalate" {
		t.Fatalf("level = %d, timeline = %s", e.p2Alert(ruleID).Level, e.timeline(a.ID))
	}
	// 在途期间重复评估不追加；core 派发后回写该级的进度（不动事件的 notify_count）。
	g.evalDue()
	if len(e.outbox(a.ID)) != 2 {
		t.Fatalf("appended while pending: %v", e.outbox(a.ID))
	}
	e.dispatch()
	if e.count("SELECT notify_count FROM monitor_alert_escalation WHERE alert_id = $1 AND level = 1", a.ID) != 1 || e.oneAlert(ruleID).NotifyCount != 1 {
		t.Fatal("core's write-back must land in monitor_alert_escalation only")
	}
	// 该级每 15 分钟重复；规则自己的重复通知（30 分钟）照常（叠加）。
	round(15)
	if got := e.outbox(a.ID); !equalStrings(got, []string{"1/1/2", "4/100001/2", "2/2/1", "4/100002/1"}) && !equalStrings(got, []string{"1/1/2", "4/100001/2", "4/100002/1", "2/2/1"}) {
		t.Fatalf("outbox = %v", got)
	}
	e.dispatch()
	// 认领（core 写 ack 两列）：停止规则的重复通知与「未认领」类的级；「未恢复」类的第 2 级到点照常升级。
	e.asCore("UPDATE monitor_alert SET ack_user = $2, ack_time = now() WHERE id = $1", a.ID, e.id())
	round(31)
	if got := e.outbox(a.ID); len(got) != 5 || got[4] != "4/200001/1" {
		t.Fatalf("after ack: outbox = %v", got)
	}
	if e.p2Alert(ruleID).Level != 2 || e.timeline(a.ID) != "fire,escalate,escalate" {
		t.Fatalf("level = %d, timeline = %s", e.p2Alert(ruleID).Level, e.timeline(a.ID))
	}
	// 第 2 级的通知被 core 丢弃（派发失败次数用尽）：该级没有重复间隔，过 10 分钟把同一行重新置为待派发。
	e.asCore("UPDATE monitor_notify_outbox SET status = 3, attempts = 5, last_error = 'boom', done_time = now() WHERE alert_id = $1 AND kind = 4 AND level = 2", a.ID)
	round(5)
	if e.count("SELECT status FROM monitor_notify_outbox WHERE alert_id = $1 AND kind = 4 AND level = 2", a.ID) != int(monitorcontract.OutboxDropped) {
		t.Fatal("re-armed too early")
	}
	round(6)
	if e.count("SELECT count(*) FROM monitor_notify_outbox WHERE alert_id = $1 AND kind = 4 AND level = 2 AND status = 1 AND attempts = 0 AND last_error = '' AND done_time IS NULL", a.ID) != 1 ||
		len(e.outbox(a.ID)) != 5 || e.timeline(a.ID) != "fire,escalate,escalate" {
		t.Fatalf("dropped escalation row was not re-armed in place: outbox = %v", e.outbox(a.ID))
	}
	e.dispatch()
	// 取消认领：第 1 级接着数（第 3 次，达到上限）；规则的重复通知补一条。
	e.asCore("UPDATE monitor_alert SET ack_user = NULL, ack_time = NULL WHERE id = $1", a.ID)
	round(1)
	got := e.outbox(a.ID)
	slices.Sort(got)
	if !equalStrings(got, []string{"1/1/2", "2/2/2", "2/3/1", "4/100001/2", "4/100002/2", "4/100003/1", "4/200001/2"}) {
		t.Fatalf("after un-ack: outbox = %v", got)
	}
	e.dispatch()
	round(20)
	if n := e.count("SELECT count(*) FROM monitor_notify_outbox WHERE alert_id = $1 AND kind = 4 AND level = 1", a.ID); n != 3 {
		t.Fatalf("level 1 exceeded max_notify: %d rows", n)
	}
	// 策略被停用：不再升级；告警行上「到达过的最高级」保留。
	e.exec("UPDATE monitor_escalation SET enabled = false WHERE id = $1", esc)
	e.bump(monitorcontract.CfgEscalation)
	time.Sleep(2100 * time.Millisecond)
	before := e.count("SELECT count(*) FROM monitor_notify_outbox WHERE alert_id = $1 AND kind = 4", a.ID)
	round(120)
	if e.count("SELECT count(*) FROM monitor_notify_outbox WHERE alert_id = $1 AND kind = 4", a.ID) != before || e.p2Alert(ruleID).Level != 2 {
		t.Fatalf("a disabled policy still escalated, or the reached level was lost (level = %d)", e.p2Alert(ruleID).Level)
	}
	// 恢复：停止一切；恢复通知照发。
	e.dispatch()
	prom.Set(upDown)
	g.evalDue()
	out := e.outbox(a.ID)
	if e.p2Alert(ruleID).Status != int(monitorcontract.AlertResolved) || !strings.HasPrefix(out[len(out)-1], "3/") {
		t.Fatalf("outbox = %v", out)
	}
}

// —— 手动关闭：两个实例并发消费指令 ——

func (e *env) closeCmd(alertID, operator, reason string) string {
	e.t.Helper()
	id := e.id()
	e.asCore("INSERT INTO monitor_alert_cmd (id, alert_id, kind, operator, reason) VALUES ($1, $2, 1, $3, $4)", id, alertID, operator, reason)
	return id
}

func TestP2CloseCommandUnderTwoInstances(t *testing.T) {
	e := newEnv(t)
	prom := promtest.New(t)
	e.datasource(prom.URL)
	const alerts = 30
	ruleID := e.rule(ruleSpec{ForS: 60})
	samples := make([]promtest.Sample, alerts)
	for i := range samples {
		samples[i] = promtest.S(0, "instance", fmt.Sprintf("h%02d", i))
	}
	prom.Set(upDown, samples...)
	a, b := e.engine(""), e.engine("")
	a.materialize()
	a.evalDue()
	e.age(61 * time.Second)
	a.evalDue()
	e.dispatch()
	before := e.p2Alerts(ruleID)
	if len(before) != alerts {
		t.Fatalf("alerts = %d", len(before))
	}
	operator := e.id()
	ticketed := before[0].ID
	e.asCore("UPDATE monitor_alert SET ticket_id = $2 WHERE id = $1", ticketed, e.id())
	for _, al := range before {
		e.closeCmd(al.ID, operator, "机器已下线")
	}
	// 一条指向不存在的事件、一条指向已恢复的事件：置未执行。
	ghost := e.closeCmd(e.id(), operator, "x")
	resolved := e.seedAlert(ruleID, e.datasource("http://127.0.0.1:1"), "fp-resolved", monitorcontract.AlertResolved, time.Hour)
	stale := e.closeCmd(resolved, operator, "x")

	// 两个实例同时消费，同时评估还在跑。
	var wg sync.WaitGroup
	ctx := context.Background()
	for _, g := range []*engine{a, b} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 20 {
				if _, err := g.commands.RunOnce(ctx); err != nil {
					t.Errorf("commands.RunOnce() error = %v", err)
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 3 {
			a.evalDue()
		}
	}()
	wg.Wait()

	if n := e.count("SELECT count(*) FROM monitor_alert_cmd WHERE status = 1"); n != 0 {
		t.Fatalf("%d commands still pending", n)
	}
	if n := e.count("SELECT count(*) FROM monitor_alert_cmd WHERE status = 2 AND done_time IS NOT NULL"); n != alerts {
		t.Fatalf("done commands = %d, want %d", n, alerts)
	}
	if e.count("SELECT count(*) FROM monitor_alert_cmd WHERE id = $1 AND status = 3 AND result = 'alert not found'", ghost) != 1 ||
		e.count("SELECT count(*) FROM monitor_alert_cmd WHERE id = $1 AND status = 3 AND result = 'alert is not firing'", stale) != 1 {
		t.Fatal("commands for missing / inactive alerts must be rejected with a reason")
	}
	// 每个事件恰好关闭一次：状态、操作人、原因、时间线各一条；不发恢复通知。
	for _, al := range before {
		if e.count(`SELECT count(*) FROM monitor_alert WHERE id = $1 AND status = 3 AND closed_by = $2 AND close_reason = '机器已下线' AND resolve_time IS NOT NULL`,
			al.ID, operator) != 1 {
			t.Fatalf("alert %s was not closed properly", al.ID)
		}
		if n := e.count(`SELECT count(*) FROM monitor_alert_event WHERE alert_id = $1 AND type = 'close' AND operator = $2
			AND detail = '{"cause":"manual","text":"机器已下线"}'`, al.ID, operator); n != 1 {
			t.Fatalf("alert %s has %d close events", al.ID, n)
		}
		if n := e.count("SELECT count(*) FROM monitor_notify_outbox WHERE alert_id = $1 AND kind = 3", al.ID); n != 0 {
			t.Fatalf("a manually closed alert got a recovery notification")
		}
	}
	// 已关联工单的事件入队一条 ticket_note，其余没有。
	if e.actions(ticketed) != "4/1" || e.count("SELECT count(*) FROM monitor_alert_action") != 1 {
		t.Fatalf("actions = %q, total = %d", e.actions(ticketed), e.count("SELECT count(*) FROM monitor_alert_action"))
	}
	// 条件仍成立：状态行被删，序列重新进入待触发并重新走持续时长——期间没有新事件。
	a.evalDue()
	if n := e.count("SELECT count(*) FROM monitor_eval_state WHERE rule_id = $1 AND state = 1 AND alert_id IS NULL", ruleID); n != alerts {
		t.Fatalf("pending states = %d, want %d", n, alerts)
	}
	if n := e.count("SELECT count(*) FROM monitor_alert WHERE rule_id = $1 AND status = 1", ruleID); n != 0 {
		t.Fatalf("%d alerts re-fired without going through for_s", n)
	}
	e.age(61 * time.Second)
	a.evalDue()
	if n := e.count("SELECT count(*) FROM monitor_alert WHERE rule_id = $1 AND status = 1", ruleID); n != alerts {
		t.Fatalf("new alerts after for_s = %d, want %d", n, alerts)
	}
	// 幂等：没有待处理的指令时什么都不做；同一事件的第二条指令被拒。
	second := e.closeCmd(before[1].ID, operator, "again")
	if _, err := b.commands.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if e.count("SELECT count(*) FROM monitor_alert_cmd WHERE id = $1 AND status = 3", second) != 1 ||
		e.count("SELECT count(*) FROM monitor_alert_event WHERE alert_id = $1 AND type = 'close'", before[1].ID) != 1 {
		t.Fatal("closing an already closed alert must be rejected without side effects")
	}
}

// 关闭与一轮评估的提交在任务行上串行：评估先持有任务行锁时，关闭等它提交后再执行。
func TestP2CloseCommandSerialisesWithTheEvaluation(t *testing.T) {
	e := newEnv(t)
	prom := promtest.New(t)
	e.datasource(prom.URL)
	ruleID := e.rule(ruleSpec{})
	prom.Set(upDown, promtest.S(0, "instance", "a"))
	g := e.engine("")
	g.materialize()
	g.evalDue()
	alert := e.p2Alert(ruleID)
	cmd := e.closeCmd(alert.ID, e.id(), "r")
	ctx := context.Background()
	// 另一个事务持有任务行锁（相当于一轮评估正在提交）。
	tx, err := e.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "SELECT 1 FROM monitor_eval_task WHERE rule_id = $1 FOR UPDATE", ruleID); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := NewCommandRepo(e.d).CloseAlert(ctx, biz.AlertCommand{ID: cmd, AlertID: alert.ID, Kind: monitorcontract.AlertCmdClose}, func() (string, error) { return e.id(), nil })
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("CloseAlert returned while the task row was locked: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	// 持锁期间别的实例看这条指令：SKIP LOCKED，立即返回「已被处理」，不排队。
	if _, err := NewCommandRepo(e.d).CloseAlert(ctx, biz.AlertCommand{ID: cmd, AlertID: alert.ID}, func() (string, error) { return e.id(), nil }); !errors.Is(err, biz.ErrCommandGone) {
		t.Fatalf("a second consumer must skip the locked command, got %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("CloseAlert() error = %v", err)
	}
	if e.p2Alert(ruleID).Status != int(monitorcontract.AlertClosed) || e.count("SELECT count(*) FROM monitor_eval_state WHERE rule_id = $1", ruleID) != 0 {
		t.Fatal("the alert must be closed and its state row deleted")
	}
}

// —— 自监控：两个规划器实例下每次状态翻转恰好一条事件 ——

func (e *env) engineEvents() []string {
	e.t.Helper()
	rows, err := e.admin.Query(context.Background(), "SELECT type FROM monitor_engine_event ORDER BY create_time, id")
	if err != nil {
		e.t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var typ string
		if err := rows.Scan(&typ); err != nil {
			e.t.Fatal(err)
		}
		out = append(out, strings.TrimPrefix(typ, "monitor.engine."))
	}
	return out
}

func TestP2EngineEventsExactlyOnceUnderTwoPlanners(t *testing.T) {
	e := newEnv(t)
	bad, good := promtest.New(t), promtest.New(t)
	bad.SetDown(true)
	dsBad, dsGood := e.datasource(bad.URL), e.datasource(good.URL)
	const failing = "bad("
	okRule := e.rule(ruleSpec{DatasourceIDs: []string{dsBad}})
	badRule := e.rule(ruleSpec{DatasourceIDs: []string{dsGood}, Queries: []monitorcontract.Query{{Expr: failing, Severity: 2}}})
	good.SetError(failing, 422, "bad_data", "parse error")
	a, b := e.engine(""), e.engine("")
	ctx := context.Background()
	opts := biz.WatchOptions{DatasourceDownAfter: 5 * time.Minute, RuleFailingAfter: 10 * time.Minute, RuleFailingMinFailures: 3}
	a.watch.WithOptions(opts)
	b.watch.WithOptions(opts)
	both := func(f func(g *engine)) {
		t.Helper()
		var wg sync.WaitGroup
		for _, g := range []*engine{a, b} {
			wg.Add(1)
			go func() { defer wg.Done(); f(g) }()
		}
		wg.Wait()
	}
	probe := func() {
		both(func(g *engine) {
			if _, err := g.planner.Probe(ctx); err != nil {
				t.Errorf("Probe() error = %v", err)
			}
		})
	}
	watch := func() {
		both(func(g *engine) {
			for range 3 {
				if _, err := g.watch.RunOnce(ctx); err != nil {
					t.Errorf("watch.RunOnce() error = %v", err)
				}
			}
		})
	}
	a.materialize()
	probe()
	a.evalDue()
	a.evalDue()
	a.evalDue()
	// health_since 记状态自何时起保持不变；再探一次不动它。
	var since1, since2 time.Time
	read := func(dst *time.Time) {
		if err := e.admin.QueryRow(ctx, "SELECT health_since FROM monitor_datasource WHERE id = $1", dsBad).Scan(dst); err != nil {
			t.Fatal(err)
		}
	}
	read(&since1)
	probe()
	read(&since2)
	if since1.IsZero() || !since1.Equal(since2) {
		t.Fatalf("health_since must stay put while the status does not change: %v → %v", since1, since2)
	}
	if e.count("SELECT error_count FROM monitor_eval_task WHERE rule_id = $1", badRule) != 3 ||
		e.count("SELECT count(*) FROM monitor_eval_task WHERE rule_id = $1 AND error_since IS NOT NULL", badRule) != 1 {
		t.Fatal("consecutive query errors must be tracked on the task row")
	}
	// 还没到阈值：什么都不写。
	watch()
	if got := e.engineEvents(); len(got) != 0 {
		t.Fatalf("events before the thresholds: %v", got)
	}
	// 时间过去 11 分钟：数据源持续不可用、规则持续评估失败——两个实例各检测多遍，各写一条。
	e.exec("UPDATE monitor_datasource SET health_since = health_since - interval '11 minutes'")
	e.exec("UPDATE monitor_eval_task SET error_since = error_since - interval '11 minutes'")
	watch()
	watch()
	got := e.engineEvents()
	slices.Sort(got)
	// 坏数据源上的那条规则也在报查询错误，但那是数据源的事件，不另报规则。
	if !slices.Equal(got, []string{"datasource_down", "rule_failing"}) {
		t.Fatalf("events = %v", got)
	}
	var detail monitorcontract.EngineEventPayload
	var raw []byte
	var objectID string
	if err := e.admin.QueryRow(ctx, "SELECT detail, object_id::text FROM monitor_engine_event WHERE type = $1", monitorcontract.EventEngineRuleFailing).Scan(&raw, &objectID); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &detail); err != nil || objectID != badRule || detail.ObjectType != "rule" || detail.ObjectID != badRule ||
		detail.Failures != 3 || !strings.Contains(detail.Error, "parse error") || detail.DatasourceName == "" || detail.Since == "" || detail.Duration != "11 分钟" {
		t.Fatalf("rule_failing detail = %+v (%v)", detail, err)
	}
	// 故障持续：继续探活、评估、检测，不再产生新事件。
	probe()
	a.evalDue()
	watch()
	if got := e.engineEvents(); len(got) != 2 {
		t.Fatalf("events while the outage continues: %v", got)
	}
	// 恢复：两个实例同时探活，恢复事件恰好一条；规则修好后评估成功的那一轮写恢复事件，恰好一条。
	bad.SetDown(false)
	good.Set(failing)
	probe()
	probe()
	a.evalDue()
	b.evalDue()
	got = e.engineEvents()
	slices.Sort(got)
	if !slices.Equal(got, []string{"datasource_down", "datasource_recovered", "rule_failing", "rule_recovered"}) {
		t.Fatalf("events after recovery = %v", got)
	}
	if e.count("SELECT count(*) FROM monitor_eval_task WHERE error_since IS NOT NULL OR error_count <> 0") != 0 {
		t.Fatal("a successful round must clear the error streak")
	}
	if err := e.admin.QueryRow(ctx, "SELECT detail FROM monitor_engine_event WHERE type = $1", monitorcontract.EventEngineDatasourceUp).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var up monitorcontract.EngineEventPayload // 新变量：缺省的键不会覆盖上一次解码留下的值
	if err := json.Unmarshal(raw, &up); err != nil || up.ObjectID != dsBad || up.ObjectType != "datasource" || up.Error != "" || up.Duration == "" {
		t.Fatalf("datasource_recovered detail = %+v", up)
	}
	// 第二次故障是一次新的故障：新的起始时刻，新的一对事件。
	bad.SetDown(true)
	probe()
	e.exec("UPDATE monitor_datasource SET health_since = health_since - interval '6 minutes' WHERE id = $1", dsBad)
	watch()
	if n := e.count("SELECT count(*) FROM monitor_engine_event WHERE type = $1", monitorcontract.EventEngineDatasourceDown); n != 2 {
		t.Fatalf("datasource_down events = %d", n)
	}
	// 停用期间留在行上的旧状态不算数：重新启用后、第一次探活之前不上报。
	e.exec("DELETE FROM monitor_engine_event")
	e.exec("UPDATE monitor_datasource SET health_time = now() - interval '1 hour', health_since = now() - interval '2 hours' WHERE id = $1", dsBad)
	watch()
	if got := e.engineEvents(); len(got) != 0 {
		t.Fatalf("a stale probe result produced events: %v", got)
	}
	// 短暂的故障（没到阈值就恢复）：没有 down，也就没有 recovered。
	e.exec("DELETE FROM monitor_engine_event")
	bad.SetDown(false)
	probe()
	bad.SetDown(true)
	probe()
	bad.SetDown(false)
	probe()
	if got := e.engineEvents(); len(got) != 0 {
		t.Fatalf("a blip produced events: %v", got)
	}
	_ = okRule
}

// —— 心跳 ——

func TestP2HeartbeatAndPruning(t *testing.T) {
	e := newEnv(t)
	repo := NewSelfmonRepo(e.d)
	ctx := context.Background()
	beat := func(id string, tasks int32) {
		t.Helper()
		if err := repo.Beat(ctx, &biz.InstanceBeat{
			Instance: id, Roles: "planner,evaluator", Version: "v1", Leader: id == "host-a", TaskCount: tasks,
			EvalLag: 1500 * time.Millisecond, EvalCount: int64(tasks) * 10, FailCount: 2,
		}); err != nil {
			t.Fatal(err)
		}
	}
	beat("host-a", 5)
	beat("host-b", 7)
	if e.count(`SELECT count(*) FROM monitor_engine_instance WHERE instance = 'host-a' AND roles = 'planner,evaluator' AND version = 'v1'
		AND leader AND task_count = 5 AND eval_lag_ms = 1500 AND eval_count = 50 AND fail_count = 2`) != 1 {
		t.Fatal("heartbeat row not written as expected")
	}
	// 再次心跳：更新同一行，start_time 不变、last_seen 前进。
	e.exec("UPDATE monitor_engine_instance SET start_time = now() - interval '1 hour', last_seen = now() - interval '5 minutes' WHERE instance = 'host-a'")
	beat("host-a", 9)
	if e.count(`SELECT count(*) FROM monitor_engine_instance WHERE instance = 'host-a' AND task_count = 9
		AND start_time < now() - interval '59 minutes' AND last_seen > now() - interval '1 minute'`) != 1 ||
		e.count("SELECT count(*) FROM monitor_engine_instance") != 2 {
		t.Fatal("a repeated heartbeat must update the same row and keep start_time")
	}
	// 过期的行由自监控检测清理。
	e.exec("UPDATE monitor_engine_instance SET last_seen = now() - interval '2 hours' WHERE instance = 'host-b'")
	g := e.engine("")
	if _, err := g.watch.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if e.count("SELECT count(*) FROM monitor_engine_instance WHERE instance = 'host-b'") != 0 || e.count("SELECT count(*) FROM monitor_engine_instance") != 1 {
		t.Fatal("stale instance rows must be pruned, live ones kept")
	}
	// 用例：评估的统计进心跳；正常停机删除自己的行。
	info := biz.InstanceInfo{ID: "host-c", Roles: "evaluator", Version: "v2"}
	hb, cleanup := biz.NewHeartbeatUsecase(repo, info, g.stats, testLogger())
	if err := hb.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if e.count("SELECT count(*) FROM monitor_engine_instance WHERE instance = 'host-c' AND roles = 'evaluator' AND version = 'v2' AND NOT leader") != 1 {
		t.Fatal("usecase heartbeat not written")
	}
	cleanup()
	if e.count("SELECT count(*) FROM monitor_engine_instance WHERE instance = 'host-c'") != 0 {
		t.Fatal("a clean shutdown must remove the instance row")
	}
}

// —— 记录规则 ——

func (e *env) recordRule(datasourceID, name, expr string, labels string) string {
	e.t.Helper()
	id := e.id()
	e.exec(`INSERT INTO monitor_record_rule (id, group_id, name, datasource_id, expr, interval_s, labels) VALUES ($1, $2, $3, $4, $5, 60, $6::jsonb)`,
		id, monitorcontract.RootGroupID, name, datasourceID, expr, labels)
	return id
}

func (g *engine) recordDue() int {
	g.e.t.Helper()
	g.e.exec("UPDATE monitor_record_task SET next_eval_time = now() - interval '1 second' WHERE lease_owner IS NULL")
	total := 0
	for {
		n, err := g.records.RunOnce(context.Background(), g.token+"-r")
		if err != nil {
			g.e.t.Fatalf("records.RunOnce() error = %v", err)
		}
		if n == 0 {
			return total
		}
		total += n
	}
}

func TestP2RecordRuleEndToEnd(t *testing.T) {
	e := newEnv(t)
	prom, recv := promtest.New(t), promtest.NewWriteReceiver(t)
	ds := e.datasource(prom.URL)
	e.exec("UPDATE monitor_datasource SET remote_write_url = $2 WHERE id = $1", ds, recv.URL)
	e.bump(monitorcontract.CfgDatasource)
	const expr = "sum by (instance) (rate(http_requests_total[5m]))"
	id := e.recordRule(ds, "instance:http_requests:rate5m", expr, `{"source":"reborn"}`)
	disabled := e.recordRule(ds, "unused:metric", "vector(1)", `{}`)
	e.exec("UPDATE monitor_record_rule SET enabled = false WHERE id = $1", disabled)
	prom.Set(expr, promtest.S(1.5, "__name__", "x", "instance", "a"), promtest.S(2.5, "instance", "b"))
	g := e.engine("")
	g.materialize()
	g.materialize() // 幂等
	if e.count("SELECT count(*) FROM monitor_record_task") != 1 ||
		e.count("SELECT count(*) FROM monitor_record_task WHERE record_rule_id = $1 AND last_status = 0 AND next_eval_time <= now() + interval '30 seconds'", id) != 1 {
		t.Fatal("exactly the enabled record rule must get a task, with a jittered first run")
	}
	if n := g.recordDue(); n != 1 {
		t.Fatalf("ran %d record tasks", n)
	}
	series := recv.Series()
	if len(recv.Problems()) != 0 || len(series) != 2 {
		t.Fatalf("problems = %v, series = %+v", recv.Problems(), series)
	}
	slices.SortFunc(series, func(a, b promtest.WrittenSeries) int {
		return strings.Compare(a.Labels["instance"], b.Labels["instance"])
	})
	want := map[string]string{"__name__": "instance:http_requests:rate5m", "instance": "a", "source": "reborn"}
	if fmt.Sprint(series[0].Labels) != fmt.Sprint(want) || series[0].Value != 1.5 || series[1].Value != 2.5 ||
		time.Since(time.UnixMilli(series[0].TimestampMS)).Abs() > time.Minute {
		t.Fatalf("series = %+v", series)
	}
	task := func() (status, written int, errText string, nextIn float64, owner *string) {
		if err := e.admin.QueryRow(context.Background(),
			`SELECT last_status, last_series, last_error, extract(epoch FROM next_eval_time - now())::float8, lease_owner
			 FROM monitor_record_task WHERE record_rule_id = $1`, id).Scan(&status, &written, &errText, &nextIn, &owner); err != nil {
			t.Fatal(err)
		}
		return
	}
	if status, written, errText, nextIn, owner := task(); status != int(monitorcontract.RecordOK) || written != 2 || errText != "" || owner != nil || nextIn < 55 || nextIn > 62 {
		t.Fatalf("task = %d / %d / %q / %.1f / %v", status, written, errText, nextIn, owner)
	}
	// 写回失败：记状态与原因（不含响应体）；下一轮成功后清掉。
	recv.SetStatus(503)
	g.recordDue()
	if status, written, errText, _, _ := task(); status != int(monitorcontract.RecordWriteError) || written != 0 || !strings.Contains(errText, "503") {
		t.Fatalf("task = %d / %d / %q", status, written, errText)
	}
	recv.SetStatus(0)
	// 部分结果：整轮不写。
	prom.SetPartial(true)
	before := len(recv.Requests())
	g.recordDue()
	if status, _, errText, _, _ := task(); status != int(monitorcontract.RecordQueryError) || !strings.HasPrefix(errText, "incomplete result: ") || len(recv.Requests()) != before {
		t.Fatalf("task = %d / %q, requests %d → %d", status, errText, before, len(recv.Requests()))
	}
	prom.SetPartial(false)
	// 无数据。
	prom.Set(expr)
	g.recordDue()
	if status, _, _, _, _ := task(); status != int(monitorcontract.RecordNoData) {
		t.Fatalf("status = %d", status)
	}
	// 规则被修改：提前到立即执行；规则停用：任务被清理。
	e.exec("UPDATE monitor_record_rule SET expr = 'vector(2)', update_time = now() + interval '1 second' WHERE id = $1", id)
	g.materialize()
	if _, _, _, nextIn, _ := task(); nextIn > 0.5 {
		t.Fatalf("an updated record rule must be hastened, next in %.1fs", nextIn)
	}
	e.exec("UPDATE monitor_record_rule SET delete_time = now() WHERE id = $1", id)
	g.materialize()
	if e.count("SELECT count(*) FROM monitor_record_task") != 0 {
		t.Fatal("the task of a deleted record rule must be removed")
	}
}

// 记录规则任务与评估任务共用同一套租约：互斥领取、围栏、租约过期接管。
func TestP2RecordTaskLeaseAndFencing(t *testing.T) {
	e := newEnv(t)
	ds := e.datasource("http://127.0.0.1:1")
	const rules = 25
	for i := range rules {
		e.recordRule(ds, fmt.Sprintf("m:%02d", i), "vector(1)", `{}`)
	}
	g := e.engine("")
	g.materialize()
	e.exec("UPDATE monitor_record_task SET next_eval_time = now() - interval '1 second'")
	repo := NewRecordRepo(e.d)
	ctx := context.Background()
	var (
		mu      sync.Mutex
		claimed = map[string]string{}
		wg      sync.WaitGroup
	)
	for _, token := range []string{"rec-a", "rec-b", "rec-c"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				tasks, err := repo.Claim(ctx, token, 4, time.Minute, 30*time.Second, time.Second)
				if err != nil {
					t.Errorf("Claim() error = %v", err)
					return
				}
				if len(tasks) == 0 {
					return
				}
				mu.Lock()
				for _, task := range tasks {
					if prev, dup := claimed[task.RuleID]; dup {
						t.Errorf("record task %s claimed by both %s and %s", task.RuleID, prev, token)
					}
					if task.DatasourceID != ds {
						t.Errorf("task carries datasource %q", task.DatasourceID)
					}
					claimed[task.RuleID] = token
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(claimed) != rules {
		t.Fatalf("claimed %d of %d record tasks", len(claimed), rules)
	}
	var one string
	for id := range claimed {
		one = id
		break
	}
	owner := claimed[one]
	task := &biz.Task{RuleID: one, DatasourceID: ds, Token: "someone-else"}
	res := &biz.RecordResult{Status: monitorcontract.RecordOK, Series: 3, Interval: time.Minute}
	if err := repo.Finish(ctx, task, res); !errors.Is(err, biz.ErrLeaseLost) {
		t.Fatalf("a foreign token must be fenced, got %v", err)
	}
	// 租约过期：别人可以接管，原属主的写回被拒。
	e.exec("UPDATE monitor_record_task SET lease_until = now() - interval '1 second' WHERE record_rule_id = $1", one)
	taken, err := repo.Claim(ctx, "rec-new", 50, time.Minute, 30*time.Second, time.Second)
	if err != nil || len(taken) != 1 || taken[0].RuleID != one {
		t.Fatalf("takeover claim = %+v, %v", taken, err)
	}
	task.Token = owner
	if err := repo.Finish(ctx, task, res); !errors.Is(err, biz.ErrLeaseLost) {
		t.Fatalf("the expired owner must be fenced, got %v", err)
	}
	if err := repo.Finish(ctx, taken[0], res); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}
	if e.count("SELECT count(*) FROM monitor_record_task WHERE record_rule_id = $1 AND lease_owner IS NULL AND last_status = 1 AND last_series = 3", one) != 1 {
		t.Fatal("write-back not recorded")
	}
	// 归还：属主围栏。
	var other *biz.Task
	for id, token := range claimed {
		if id != one {
			other = &biz.Task{RuleID: id, DatasourceID: ds, Token: token}
			break
		}
	}
	if err := repo.Release(ctx, "not-the-owner", []*biz.Task{other}, 0); err != nil {
		t.Fatal(err)
	}
	if e.count("SELECT count(*) FROM monitor_record_task WHERE record_rule_id = $1 AND lease_owner IS NOT NULL", other.RuleID) != 1 {
		t.Fatal("a foreign release must not clear the lease")
	}
	if err := repo.Release(ctx, other.Token, []*biz.Task{other}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if e.count("SELECT count(*) FROM monitor_record_task WHERE record_rule_id = $1 AND lease_owner IS NULL AND next_eval_time > now() + interval '4 seconds'", other.RuleID) != 1 {
		t.Fatal("release must clear the lease and postpone the task")
	}
}

// —— 维护：过期告警连带第二期的附属行；自监控事件与指令 ——

func TestP2RetentionCoversTheNewTables(t *testing.T) {
	e := newEnv(t)
	repo := NewMaintenanceRepo(e.d)
	ctx := context.Background()
	ds, ruleID := e.datasource("http://127.0.0.1:1"), e.rule(ruleSpec{})
	const day = 24 * time.Hour
	old := e.seedAlert(ruleID, ds, "fp-old", monitorcontract.AlertResolved, 200*day)
	recent := e.seedAlert(ruleID, ds, "fp-recent", monitorcontract.AlertResolved, 10*day)
	for _, id := range []string{old, recent} {
		e.exec("INSERT INTO monitor_alert_event (id, alert_id, type) VALUES ($1, $2, 'fire')", e.id(), id)
		e.exec("INSERT INTO monitor_alert_action (id, alert_id, kind) VALUES ($1, $2, 1)", e.id(), id)
		e.exec("INSERT INTO monitor_alert_cmd (id, alert_id, kind, status, done_time) VALUES ($1, $2, 1, 2, now() - interval '5 days')", e.id(), id)
		e.exec("INSERT INTO monitor_alert_escalation (alert_id, level, notify_count) VALUES ($1, 1, 2)", id)
	}
	if n, err := repo.PurgeAlerts(ctx, 180*day, 100); err != nil || n != 1 {
		t.Fatalf("PurgeAlerts() = %d, %v", n, err)
	}
	for _, table := range []string{"monitor_alert_event", "monitor_alert_action", "monitor_alert_cmd", "monitor_alert_escalation"} {
		if e.count("SELECT count(*) FROM "+table+" WHERE alert_id = $1", old) != 0 || e.count("SELECT count(*) FROM "+table+" WHERE alert_id = $1", recent) != 1 {
			t.Fatalf("%s: rows of the purged alert must go, others must stay", table)
		}
	}
	event := func(status int, doneAgo time.Duration) {
		e.exec(`INSERT INTO monitor_engine_event (id, type, object_id, dedup_key, status, done_time)
			VALUES ($1::uuid, 'monitor.engine.datasource_down', $2, $1::uuid::text, $3::smallint, CASE WHEN $3::smallint = 1 THEN NULL ELSE now() - $4::interval END)`,
			e.id(), ds, status, doneAgo)
	}
	event(2, 200*day)
	event(3, 190*day)
	event(2, 10*day)
	event(1, 0) // 待派发的不论多旧都不删
	e.exec("UPDATE monitor_engine_event SET create_time = now() - interval '300 days'")
	if n, err := repo.PurgeEngineEvents(ctx, 180*day, 100); err != nil || n != 2 || e.count("SELECT count(*) FROM monitor_engine_event") != 2 {
		t.Fatalf("PurgeEngineEvents() = %d, %v", n, err)
	}
	cmd := func(status int, doneAgo time.Duration) {
		e.exec(`INSERT INTO monitor_alert_cmd (id, alert_id, kind, status, done_time)
			VALUES ($1, $2, 1, $3::smallint, CASE WHEN $3::smallint = 1 THEN NULL ELSE now() - $4::interval END)`, e.id(), e.id(), status, doneAgo)
	}
	cmd(2, 200*day)
	cmd(3, 181*day)
	cmd(1, 0)
	for i, want := range []int{1, 1, 0} {
		if n, err := repo.PurgeCommands(ctx, 180*day, 1); err != nil || n != want {
			t.Fatalf("PurgeCommands batch %d = %d, %v; want %d", i, n, err, want)
		}
	}
}

// 规则被停用 / 删除时关闭的告警：时间线记一条关闭（原因 = rule）。
func TestP2MaterializeRecordsCloseEvents(t *testing.T) {
	e := newEnv(t)
	prom := promtest.New(t)
	e.datasource(prom.URL)
	ruleID := e.rule(ruleSpec{})
	prom.Set(upDown, promtest.S(0, "instance", "a"))
	g := e.engine("")
	g.materialize()
	g.evalDue()
	a := e.p2Alert(ruleID)
	e.exec("UPDATE monitor_rule SET enabled = false WHERE id = $1", ruleID)
	g.materialize()
	if e.p2Alert(ruleID).Status != int(monitorcontract.AlertClosed) || e.timeline(a.ID) != "fire,close" ||
		e.count(`SELECT count(*) FROM monitor_alert_event WHERE alert_id = $1 AND type = 'close' AND operator IS NULL AND detail = '{"cause":"rule"}'`, a.ID) != 1 {
		t.Fatalf("timeline = %s", e.timeline(a.ID))
	}
}

// 第二期的授权边界：引擎能做的与不能做的（以受限登录角色实测）。
func TestP2EngineRoleGrants(t *testing.T) {
	e := newEnv(t)
	ds := e.datasource("http://127.0.0.1:1")
	ruleID := e.rule(ruleSpec{})
	alert := e.seedAlert(ruleID, ds, "fp-grants", monitorcontract.AlertFiring, 0)
	e.exec("INSERT INTO monitor_alert_cmd (id, alert_id, kind) VALUES ($1, $2, 1)", e.id(), alert)
	e.exec("INSERT INTO monitor_alert_escalation (alert_id, level) VALUES ($1, 1)", alert)
	e.exec("INSERT INTO monitor_alert_action (id, alert_id, kind) VALUES ($1, $2, 1)", e.id(), alert)
	ctx := context.Background()
	for name, stmt := range map[string]string{
		"write escalation policy":    "UPDATE monitor_escalation SET enabled = false",
		"write inhibit rule":         "DELETE FROM monitor_inhibit",
		"write record rule":          "UPDATE monitor_record_rule SET enabled = false",
		"write ci index":             "UPDATE monitor_ci_index SET valid = false",
		"write calendar":             "UPDATE monitor_calendar SET config = '{}'",
		"read alert views":           "SELECT count(*) FROM monitor_alert_view",
		"insert a command":           "INSERT INTO monitor_alert_cmd (id, alert_id, kind) VALUES (gen_random_uuid(), gen_random_uuid(), 1)",
		"rewrite a command":          "UPDATE monitor_alert_cmd SET reason = 'x'",
		"write escalation progress":  "UPDATE monitor_alert_escalation SET notify_count = 9",
		"insert escalation progress": "INSERT INTO monitor_alert_escalation (alert_id, level) VALUES (gen_random_uuid(), 1)",
		"update an action":           "UPDATE monitor_alert_action SET status = 2",
		"update a timeline row":      "UPDATE monitor_alert_event SET type = 'ack'",
		"update an engine event":     "UPDATE monitor_engine_event SET status = 2",
		"write datasource write url": "UPDATE monitor_datasource SET remote_write_url = 'http://x'",
	} {
		_, err := e.d.Pool.Exec(ctx, stmt)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
			t.Errorf("%s: want permission denied (42501), got %v", name, err)
		}
	}
	for name, stmt := range map[string]string{
		"health_since":             "UPDATE monitor_datasource SET health_since = now()",
		"command status":           "UPDATE monitor_alert_cmd SET status = 3, result = 'x', done_time = now()",
		"delete commands":          "DELETE FROM monitor_alert_cmd",
		"delete escalation rows":   "DELETE FROM monitor_alert_escalation",
		"engine alert columns":     "UPDATE monitor_alert SET inhibited = true, inhibit_kind = 2, inhibit_alert_id = gen_random_uuid(), ci_id = gen_random_uuid(), escalation_level = 2, closed_by = gen_random_uuid(), close_reason = 'x'",
		"task error streak":        "UPDATE monitor_eval_task SET error_since = now(), error_count = 1",
		"delete timeline / action": "WITH a AS (DELETE FROM monitor_alert_event) DELETE FROM monitor_alert_action",
	} {
		if _, err := e.d.Pool.Exec(ctx, stmt); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}
