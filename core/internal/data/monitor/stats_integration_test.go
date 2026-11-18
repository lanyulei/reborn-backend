//go:build integration

package data

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// statsP2Tables 统计 / 引擎状态 / 聚合视图的测试会写的第二期表。
const statsP2Tables = "monitor_alert_event, monitor_alert_view, monitor_engine_instance, monitor_record_task, monitor_record_rule"

// newStatsEnv 第一期的环境 + 清空本文件用到的第二期表。记录规则引用规则分组（外键），所以要先于第一期的
// 环境重置（它会删除分组）清掉，测试结束时再清一次，不给后面的测试留下挡住分组删除的行。
func newStatsEnv(t *testing.T) *env {
	t.Helper()
	d := openData(t, testDSN(t))
	truncate := func() error {
		_, err := d.Pool.Exec(context.Background(), "TRUNCATE TABLE "+statsP2Tables)
		return err
	}
	if err := truncate(); err != nil {
		t.Fatalf("truncate p2 tables: %v", err)
	}
	e := newEnv(t)
	t.Cleanup(func() {
		if err := truncate(); err != nil {
			t.Errorf("truncate p2 tables: %v", err)
		}
	})
	return e
}

func (e *env) exec(sql string, args ...any) {
	e.t.Helper()
	if _, err := e.pool.Exec(context.Background(), sql, args...); err != nil {
		e.t.Fatalf("exec %q: %v", sql, err)
	}
}

// notifyLog 以派发用例的身份写一条通知记录。
func (e *env) notifyLog(alertID string, source int32, at time.Time, recipients ...string) {
	e.t.Helper()
	kind, level := mc.NotifyKindFiring, 0
	var sub any
	switch source {
	case mc.NotifySourceSubscription:
		sub = newID(e.t)
	case mc.NotifySourceEscalation:
		kind, level = mc.NotifyKindEscalation, 1
	}
	list := "["
	for i, r := range recipients {
		if i > 0 {
			list += ","
		}
		list += `"` + r + `"`
	}
	e.exec(`INSERT INTO monitor_notify_log (id, alert_id, kind, seq, source, subscription_id, recipients, create_time, level)
		VALUES ($1, $2, $3, 1, $4, $5, $6::jsonb, $7, $8)`, newID(e.t), alertID, kind, source, sub, list+"]", at, level)
}

func TestStatsRepo(t *testing.T) {
	e := newStatsEnv(t)
	ctx := context.Background()
	st := NewStatsRepo(e.d)
	host, db := e.group("", "主机"), e.group("", "数据库")
	other := e.group("", "别的团队")
	r1, r2 := e.rule(host.ID, "CPU"), e.rule(db.ID, "慢查询")
	loc, _ := time.LoadLocation("Asia/Shanghai")
	now := time.Now().In(loc)
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, loc)
	fire := today.Add(-36 * time.Hour) // 前天中午（上海）
	ci := newID(t)

	a1 := e.alert(alertRow{ruleID: r1.ID, ruleName: "CPU", groupID: host.ID, severity: mc.SeverityFatal, fired: today.Add(time.Second), labels: `{"instance":"web-1"}`})
	a2 := e.alert(alertRow{ruleID: r1.ID, ruleName: "CPU", groupID: host.ID, severity: mc.SeverityWarning, fired: fire, labels: `{"instance":"web-1"}`})
	a3 := e.alert(alertRow{ruleID: r1.ID, ruleName: "CPU 旧名", groupID: host.ID, severity: mc.SeverityWarning, status: mc.AlertResolved, fired: fire.Add(-time.Hour), labels: `{"instance":"web-2"}`})
	a4 := e.alert(alertRow{ruleID: r2.ID, ruleName: "慢查询", groupID: db.ID, severity: mc.SeverityCritical, status: mc.AlertClosed, fired: fire})
	a5 := e.alert(alertRow{ruleID: r2.ID, ruleName: "慢查询", groupID: db.ID, severity: mc.SeverityCritical, fired: fire, muted: true})
	foreign := e.alert(alertRow{ruleID: newID(t), ruleName: "别人的", groupID: other.ID, fired: fire, labels: `{"instance":"web-1"}`})
	// 引擎 / core 的回写：认领、抑制、关联资源、通知计数、恢复时间。
	e.exec("UPDATE monitor_alert SET ack_user = $2, ack_time = first_fire_time + interval '3 minutes', ci_id = $3, notify_count = 2 WHERE id = $1", a2, uAlice, ci)
	e.exec("UPDATE monitor_alert SET resolve_time = first_fire_time + interval '10 minutes', ack_user = $2, ack_time = first_fire_time + interval '5 minutes' WHERE id = $1", a3, uBob)
	e.exec("UPDATE monitor_alert SET resolve_time = $2 WHERE id = $1", a4, today.Add(time.Minute))
	e.exec("UPDATE monitor_alert SET inhibited = true, inhibit_kind = 1 WHERE id = $1", a1)
	e.exec("UPDATE monitor_alert SET ci_id = $2, notify_count = 1 WHERE id = $1", foreign, ci)
	e.notifyLog(a2, mc.NotifySourceRule, fire, uAlice)
	e.notifyLog(a2, mc.NotifySourceRule, fire.Add(time.Hour), uAlice)
	e.notifyLog(a2, mc.NotifySourceEscalation, fire.Add(2*time.Hour), uBob)
	e.notifyLog(a4, mc.NotifySourceSubscription, fire, uCarol) // 只有订阅通知：也算通知过
	e.notifyLog(foreign, mc.NotifySourceRule, fire, uAlice)
	// a3 曾被屏蔽后解除；时间线里别的类型不影响。
	e.exec("INSERT INTO monitor_alert_event (id, alert_id, type) VALUES ($1, $2, 'mute'), ($3, $2, 'unmute'), ($4, $5, 'fire'), ($6, $7, 'inhibit')",
		newID(t), a3, newID(t), newID(t), a2, newID(t), foreign)

	mine := biz.GroupFilter{IDs: []string{host.ID, db.ID}}
	all := biz.GroupFilter{All: true}
	rng := func(g biz.GroupFilter) biz.StatsRange {
		return biz.StatsRange{Groups: g, Start: today.Add(-72 * time.Hour), End: today.Add(24 * time.Hour)}
	}

	o, err := st.Overview(ctx, mine, today, today.Add(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if want := (biz.AlertOverview{Firing: 3, FiringBySeverity: [4]int32{0, 1, 1, 1}, Unacked: 0, Muted: 1, Inhibited: 1, TodayFired: 1, TodayClosed: 1}); *o != want {
		t.Fatalf("Overview = %+v, want %+v", *o, want)
	}
	if o, err = st.Overview(ctx, all, today, today.Add(24*time.Hour)); err != nil || o.Firing != 4 || o.Unacked != 1 {
		t.Fatalf("Overview(all) = %+v, %v", o, err)
	}

	cells, err := st.Trend(ctx, rng(mine), biz.StatsBySeverity, "Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	d2 := fire.Format(time.DateOnly)
	want := []biz.StatsCell{{Day: d2, Key: "2", Count: 2}, {Day: d2, Key: "3", Count: 2}, {Day: today.Format(time.DateOnly), Key: "4", Count: 1}}
	if !slices.Equal(cells, want) {
		t.Fatalf("Trend = %+v, want %+v", cells, want)
	}
	// 时区决定分桶：前天中午（上海）在 Pacific/Kiritimati（UTC+14）是前天晚上，今天 00:00:01 则是同一天的下午。
	if cells, err = st.Trend(ctx, rng(mine), biz.StatsByGroup, "UTC"); err != nil || len(cells) != 3 || cells[2].Day != today.UTC().Format(time.DateOnly) {
		t.Fatalf("Trend(group, UTC) = %+v, %v", cells, err)
	}
	// 范围是 [start, end)：恰在 end 的事件不计。
	exact := biz.StatsRange{Groups: mine, Start: fire.Add(-time.Hour), End: fire}
	if cells, _ = st.Trend(ctx, exact, biz.StatsBySeverity, "UTC"); len(cells) != 1 || cells[0].Count != 1 {
		t.Fatalf("half-open range = %+v", cells)
	}

	rows, err := st.Efficiency(ctx, rng(mine), biz.StatsByGroup)
	if err != nil || len(rows) != 2 {
		t.Fatalf("Efficiency = %+v, %v", rows, err)
	}
	byKey := map[string]biz.EfficiencyRow{}
	for _, r := range rows {
		byKey[r.Key] = r
	}
	if h := byKey[host.ID]; h.Alerts != 3 || h.Acked != 2 || h.Resolved != 1 || h.AckSumS != 480 || h.ResolveSumS != 600 {
		t.Fatalf("host efficiency = %+v", h)
	}
	if d := byKey[db.ID]; d.Alerts != 2 || d.Acked != 0 || d.Resolved != 0 || d.ResolveSumS != 0 {
		t.Fatalf("db efficiency (closed is not resolved) = %+v", d)
	}

	top, total, err := st.Top(ctx, rng(mine), biz.TopBy{Kind: biz.TopByRule}, 10)
	if err != nil || total != 5 || len(top) != 2 {
		t.Fatalf("Top(rule) = %+v, %d, %v", top, total, err)
	}
	// 名称取范围内最近一条事件上的（a1 的 "CPU"，不是更早的「CPU 旧名」）。
	if top[0].Key != r1.ID || top[0].Name != "CPU" || top[0].GroupID != host.ID || top[0].Count != 3 || top[0].Firing != 2 || top[0].MaxSeverity != 4 {
		t.Fatalf("Top(rule)[0] = %+v", top[0])
	}
	if top, total, err = st.Top(ctx, rng(mine), biz.TopBy{Kind: biz.TopByRule}, 1); err != nil || total != 5 || len(top) != 1 {
		t.Fatalf("Top(rule, limit 1) = %+v, %d, %v", top, total, err)
	}
	if top, total, err = st.Top(ctx, rng(mine), biz.TopBy{Kind: biz.TopByCI}, 10); err != nil || total != 1 || len(top) != 1 || top[0].Key != ci {
		t.Fatalf("Top(ci) = %+v, %d, %v", top, total, err)
	}
	if top, total, err = st.Top(ctx, rng(all), biz.TopBy{Kind: biz.TopByCI}, 10); err != nil || total != 2 || top[0].Count != 2 {
		t.Fatalf("Top(ci, all) = %+v, %d, %v", top, total, err)
	}
	top, total, err = st.Top(ctx, rng(mine), biz.TopBy{Kind: biz.TopByLabel, Label: "instance"}, 10)
	if err != nil || total != 3 || len(top) != 2 || top[0].Key != "web-1" || top[0].Count != 2 || top[1].Key != "web-2" {
		t.Fatalf("Top(label) = %+v, %d, %v", top, total, err)
	}
	// 标签名是实参：带引号的名字只是匹配不到，不会拼进 SQL。
	if top, total, err = st.Top(ctx, rng(mine), biz.TopBy{Kind: biz.TopByLabel, Label: `x' OR '1'='1`}, 10); err != nil || total != 0 || len(top) != 0 {
		t.Fatalf("Top(odd label) = %+v, %d, %v", top, total, err)
	}

	n, err := st.Noise(ctx, rng(mine))
	if err != nil {
		t.Fatal(err)
	}
	if want := (biz.AlertNoise{Alerts: 5, Muted: 2, Inhibited: 1, Notified: 2, RuleNotifications: 2, SubscriptionNotifications: 1, EscalationNotifications: 1}); *n != want {
		t.Fatalf("Noise = %+v, want %+v", *n, want)
	}
	if n, err = st.Noise(ctx, rng(all)); err != nil || n.Alerts != 6 || n.Inhibited != 2 || n.Notified != 3 || n.RuleNotifications != 3 {
		t.Fatalf("Noise(all) = %+v, %v", n, err)
	}
	_ = a5
}

func TestEngineRepo(t *testing.T) {
	e := newStatsEnv(t)
	ctx := context.Background()
	repo := NewEngineRepo(e.d)
	host, other := e.group("", "主机"), e.group("", "别的团队")
	ds, bad := e.datasource("vm", false), e.datasource("vm-down", false)
	r1, r2, off := e.rule(host.ID, "CPU"), e.rule(other.ID, "别人的"), e.rule(host.ID, "停用的")
	e.exec("UPDATE monitor_rule SET enabled = false WHERE id = $1", off.ID)
	e.exec("UPDATE monitor_datasource SET health_status = $2, health_error = 'dial timeout', health_since = now() - interval '10 minutes' WHERE id = $1", bad.ID, mc.HealthFailed)
	task := func(ruleID, dsID, next string, status, errors int) {
		e.exec(`INSERT INTO monitor_eval_task (rule_id, datasource_id, next_eval_time, last_status, last_error, error_count, error_since)
			VALUES ($1, $2, now() + $3::interval, $4, 'boom', $5, CASE WHEN $5 > 0 THEN now() - interval '5 minutes' END)`, ruleID, dsID, next, status, errors)
	}
	task(r1.ID, ds.ID, "30 seconds", int(mc.EvalOK), 0)
	task(r1.ID, bad.ID, "-10 minutes", int(mc.EvalError), 4) // 逾期（规则周期 60s）且持续失败
	task(r2.ID, bad.ID, "-90 seconds", int(mc.EvalError), 3) // 到期未评估，但没超过两个周期
	task(off.ID, bad.ID, "-1 hour", int(mc.EvalError), 9)    // 停用规则的任务不算
	e.exec(`INSERT INTO monitor_record_rule (id, group_id, name, datasource_id, expr) VALUES ($1, $2, 'job:up:sum', $3, 'sum(up)'), ($4, $2, 'job:down:sum', $3, 'sum(up == 0)')`,
		r1.ID, host.ID, ds.ID, r2.ID)
	e.exec(`INSERT INTO monitor_record_task (record_rule_id, last_status) VALUES ($1, $2), ($3, $4)`, r1.ID, mc.RecordWriteError, r2.ID, mc.RecordOK)
	a := e.alert(alertRow{ruleID: r1.ID, ruleName: "CPU", groupID: host.ID})
	e.outboxRow(a, mc.NotifyKindFiring, 1)
	e.exec(`INSERT INTO monitor_engine_instance (instance, roles, version, leader, last_seen, task_count, eval_lag_ms, eval_count, fail_count) VALUES
		('b-alive', 'evaluator', 'v1', false, now() - interval '10 seconds', 5, 120, 1000, 3),
		('a-alive', 'planner, evaluator', 'v1', true, now(), 7, 80, 2000, 0),
		('c-dead', 'planner', 'v0', true, now() - interval '5 minutes', 0, 0, 10, 0),
		('d-gone', 'evaluator', 'v0', false, now() - interval '2 days', 0, 0, 10, 0)`)

	ins, err := repo.Instances(ctx, biz.EngineAliveWindow, biz.EngineStaleWindow)
	if err != nil || len(ins) != 3 {
		t.Fatalf("Instances = %+v, %v", ins, err)
	}
	if ins[0].Instance != "a-alive" || !ins[0].Alive || !ins[0].Leader || !slices.Equal(ins[0].Roles, []string{"planner", "evaluator"}) ||
		ins[1].Instance != "b-alive" || ins[1].EvalLagMS != 120 || ins[2].Instance != "c-dead" || ins[2].Alive || ins[2].Leader {
		t.Fatalf("Instances = %+v / %+v / %+v", ins[0], ins[1], ins[2])
	}
	s, err := repo.Summary(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if s.TaskTotal != 3 || s.TaskOverdue != 1 || s.TaskError != 2 || s.MaxLagMS < 590_000 || s.MaxLagMS > 700_000 ||
		s.RecordTaskTotal != 2 || s.RecordTaskError != 1 || s.NotifyPending != 1 {
		t.Fatalf("Summary = %+v", s)
	}
	dss, err := repo.Datasources(ctx, 50)
	if err != nil || len(dss) != 1 || dss[0].ID != bad.ID || dss[0].Error != "dial timeout" || dss[0].Since == nil || dss[0].RuleCount != 3 {
		t.Fatalf("Datasources = %+v, %v", dss, err)
	}
	rules, err := repo.Rules(ctx, biz.GroupFilter{IDs: []string{host.ID}}, biz.EngineFailingThreshold, 50)
	if err != nil || len(rules) != 1 || rules[0].RuleID != r1.ID || rules[0].DatasourceName != "vm-down" || rules[0].Failures != 4 ||
		rules[0].Since == nil || rules[0].GroupID != host.ID || rules[0].Error != "boom" {
		t.Fatalf("Rules(host) = %+v, %v", rules, err)
	}
	if rules, err = repo.Rules(ctx, biz.GroupFilter{All: true}, biz.EngineFailingThreshold, 50); err != nil || len(rules) != 2 {
		t.Fatalf("Rules(all) = %+v, %v", rules, err)
	}
	if rules, err = repo.Rules(ctx, biz.GroupFilter{All: true}, 4, 50); err != nil || len(rules) != 1 {
		t.Fatalf("Rules(threshold 4) = %+v, %v", rules, err)
	}
	if rules, err = repo.Rules(ctx, biz.GroupFilter{}, 1, 50); err != nil || len(rules) != 0 {
		t.Fatalf("Rules(empty scope) = %+v, %v", rules, err)
	}
}

func TestAlertViewRepo(t *testing.T) {
	e := newStatsEnv(t)
	ctx := context.Background()
	repo := NewAlertViewRepo(e.d)
	yes := true
	mk := func(owner, name string, public bool) *biz.SavedView {
		return &biz.SavedView{ID: newID(t), OwnerID: owner, Spec: biz.SavedViewSpec{
			Name: name, By: "label:instance", IsPublic: public,
			Filters: mc.AlertViewFilter{GroupID: mc.RootGroupID, Severities: []int32{3, 4}, Muted: &yes, Labels: []string{"env=prod"}},
		}}
	}
	mine := mk(uAlice, "b-mine", false)
	if err := repo.Create(ctx, mine, 2); err != nil || mine.CreateTime.IsZero() {
		t.Fatalf("Create = %v", err)
	}
	wantErr(t, "duplicate name", repo.Create(ctx, mk(uAlice, "b-mine", false), 2), biz.ErrAlertViewNameExists)
	if err := repo.Create(ctx, mk(uBob, "b-mine", false), 2); err != nil {
		t.Fatalf("same name, other owner: %v", err)
	}
	pub := mk(uAdmin, "z-public", true)
	if err := repo.Create(ctx, pub, 0); err != nil {
		t.Fatalf("public view ignores the personal cap: %v", err)
	}
	if err := repo.Create(ctx, mk(uAlice, "a-mine", false), 2); err != nil {
		t.Fatal(err)
	}
	wantErr(t, "personal cap", repo.Create(ctx, mk(uAlice, "c-mine", false), 2), biz.ErrAlertViewInvalid)

	got, err := repo.Get(ctx, mine.ID)
	if err != nil || got.Spec.Name != "b-mine" || got.Spec.By != "label:instance" || got.OwnerID != uAlice ||
		got.Spec.Filters.GroupID != mc.RootGroupID || !slices.Equal(got.Spec.Filters.Severities, []int32{3, 4}) ||
		got.Spec.Filters.Muted == nil || !*got.Spec.Filters.Muted || got.Spec.Filters.Acked != nil {
		t.Fatalf("Get = %+v, %v", got, err)
	}
	_, err = repo.Get(ctx, newID(t))
	wantErr(t, "get missing", err, biz.ErrAlertViewNotFound)

	list, err := repo.ListVisible(ctx, uAlice, 200)
	if err != nil || len(list) != 3 || list[0].ID != pub.ID || list[1].Spec.Name != "a-mine" || list[2].ID != mine.ID {
		t.Fatalf("ListVisible = %+v, %v", list, err)
	}
	if list, _ = repo.ListVisible(ctx, uAlice, 1); len(list) != 1 {
		t.Fatalf("ListVisible(limit) = %d", len(list))
	}

	// 围栏：所有者或公共标记对不上就不写。
	upd := *mine
	upd.Spec.Name, upd.Spec.Filters = "renamed", mc.AlertViewFilter{}
	wantErr(t, "update with a stale public flag", repo.Update(ctx, &upd, true), biz.ErrAlertViewNotFound)
	stolen := upd
	stolen.OwnerID = uBob
	wantErr(t, "update as another owner", repo.Update(ctx, &stolen, false), biz.ErrAlertViewNotFound)
	if err := repo.Update(ctx, &upd, false); err != nil || !upd.UpdateTime.After(mine.CreateTime.Add(-time.Second)) {
		t.Fatalf("Update = %v", err)
	}
	if got, _ = repo.Get(ctx, mine.ID); got.Spec.Name != "renamed" || got.Spec.Filters.GroupID != "" || len(got.Spec.Filters.Severities) != 0 {
		t.Fatalf("after update = %+v", got)
	}
	upd.Spec.Name = "a-mine"
	wantErr(t, "rename onto own view", repo.Update(ctx, &upd, false), biz.ErrAlertViewNameExists)
	wantErr(t, "delete as another owner", repo.Delete(ctx, mine.ID, uBob, false), biz.ErrAlertViewNotFound)
	wantErr(t, "delete with a stale public flag", repo.Delete(ctx, mine.ID, uAlice, true), biz.ErrAlertViewNotFound)
	if err := repo.Delete(ctx, mine.ID, uAlice, false); err != nil {
		t.Fatal(err)
	}
	wantErr(t, "delete twice", repo.Delete(ctx, mine.ID, uAlice, false), biz.ErrAlertViewNotFound)
}

// 个人视图的数量上限在并发下不会被冲破（同一所有者的新建以 advisory lock 串行）。
func TestAlertViewRepo_ConcurrentCap(t *testing.T) {
	e := newStatsEnv(t)
	repo := NewAlertViewRepo(e.d)
	const limit, workers = 5, 20
	var wg sync.WaitGroup
	errs := make([]error, workers)
	for i := range workers {
		wg.Go(func() {
			errs[i] = repo.Create(context.Background(), &biz.SavedView{
				ID: newID(t), OwnerID: uAlice, Spec: biz.SavedViewSpec{Name: fmt.Sprintf("v-%d", i), By: "rule"},
			}, limit)
		})
	}
	wg.Wait()
	ok := 0
	for _, err := range errs {
		if err == nil {
			ok++
		} else {
			wantErr(t, "over the cap", err, biz.ErrAlertViewInvalid)
		}
	}
	if n := e.count("SELECT count(*) FROM monitor_alert_view WHERE owner_id = $1", uAlice); ok != limit || n != limit {
		t.Fatalf("created %d views (%d rows), want exactly %d", ok, n, limit)
	}
}

func TestShiftCountRepo(t *testing.T) {
	e := newStatsEnv(t)
	ctx := context.Background()
	repo := NewShiftCountRepo(e.d)
	g := e.group("", "主机")
	base := time.Now().Add(-48 * time.Hour).Truncate(time.Hour)
	a1 := e.alert(alertRow{ruleID: newID(t), ruleName: "r", groupID: g.ID})
	a2 := e.alert(alertRow{ruleID: newID(t), ruleName: "r", groupID: g.ID})
	e.notifyLog(a1, mc.NotifySourceRule, base.Add(time.Hour), uAlice)
	e.notifyLog(a1, mc.NotifySourceSubscription, base.Add(2*time.Hour), uAlice, uBob) // 同一事件：alice 只计一次
	e.notifyLog(a2, mc.NotifySourceEscalation, base.Add(3*time.Hour), uAlice)
	e.notifyLog(a2, mc.NotifySourceRule, base.Add(10*time.Hour), uAlice) // 窗口之外
	e.notifyLog(a2, mc.NotifySourceRule, base.Add(4*time.Hour), "$group:ops")
	e.notifyLog(a2, mc.NotifySourceRule, base.Add(8*time.Hour), uBob) // 恰在窗口终点：不计
	got, err := repo.CountNotified(ctx, []biz.ShiftWindow{
		{UserID: uAlice, Start: base, End: base.Add(8 * time.Hour)},
		{UserID: uBob, Start: base, End: base.Add(8 * time.Hour)},
		{UserID: uCarol, Start: base, End: base.Add(8 * time.Hour)},
		{UserID: uAlice, Start: base.Add(8 * time.Hour), End: base.Add(16 * time.Hour)},
		{UserID: uAlice, Start: base.Add(time.Hour), End: base.Add(2 * time.Hour)}, // 含起点
	})
	if err != nil || !slices.Equal(got, []int32{2, 1, 0, 1, 1}) {
		t.Fatalf("CountNotified = %v, %v", got, err)
	}
	if got, err = repo.CountNotified(ctx, nil); err != nil || len(got) != 0 {
		t.Fatalf("CountNotified(nil) = %v, %v", got, err)
	}
}

// statsLoginPool 新建一个只是 monitor_core_role 成员的 LOGIN 角色并以它连接（测试结束时删除）。
func statsLoginPool(t *testing.T, e *env) *basedata.Data {
	t.Helper()
	buf := make([]byte, 12)
	if _, err := rand.Read(buf); err != nil {
		t.Fatal(err)
	}
	role, password := "monp2_e_"+hex.EncodeToString(buf[:4]), hex.EncodeToString(buf[4:])+"Aa1"
	// 标识符与口令都是本函数生成的十六进制串，不含需要转义的字符。
	e.exec(fmt.Sprintf("CREATE ROLE %s LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS PASSWORD '%s' IN ROLE monitor_core_role", role, password))
	t.Cleanup(func() {
		if _, err := e.pool.Exec(context.Background(), "DROP ROLE IF EXISTS "+role); err != nil {
			t.Errorf("drop login role %s: %v", role, err)
		}
	})
	cfg := e.pool.Config().ConnConfig
	sslmode := "disable"
	if cfg.TLSConfig != nil {
		sslmode = "require"
	}
	return openData(t, fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s", cfg.Host, cfg.Port, role, password, cfg.Database, sslmode))
}

// 统计 / 引擎状态 / 聚合视图 / 班次告警数的全部语句在 core 的受限账号（只是 monitor_core_role 的成员）下可执行。
func TestStatsAsCoreLoginRole(t *testing.T) {
	e := newStatsEnv(t)
	ctx := context.Background()
	g := e.group("", "主机")
	a := e.alert(alertRow{ruleID: newID(t), ruleName: "r", groupID: g.ID})
	e.notifyLog(a, mc.NotifySourceRule, time.Now(), uAlice)
	d := statsLoginPool(t, e)
	var super bool
	if err := d.Pool.QueryRow(ctx, "SELECT rolsuper FROM pg_roles WHERE rolname = current_user").Scan(&super); err != nil || super {
		t.Fatalf("login role must not be a superuser (super=%v, err=%v)", super, err)
	}
	st, eng, views, shifts := NewStatsRepo(d), NewEngineRepo(d), NewAlertViewRepo(d), NewShiftCountRepo(d)
	r := biz.StatsRange{Groups: biz.GroupFilter{IDs: []string{g.ID}}, Start: time.Now().Add(-time.Hour), End: time.Now().Add(time.Hour)}
	steps := map[string]func() error{
		"overview":   func() error { _, err := st.Overview(ctx, r.Groups, r.Start, r.End); return err },
		"trend":      func() error { _, err := st.Trend(ctx, r, biz.StatsByGroup, "Asia/Shanghai"); return err },
		"efficiency": func() error { _, err := st.Efficiency(ctx, r, biz.StatsBySeverity); return err },
		"top rule":   func() error { _, _, err := st.Top(ctx, r, biz.TopBy{Kind: biz.TopByRule}, 5); return err },
		"top label":  func() error { _, _, err := st.Top(ctx, r, biz.TopBy{Kind: biz.TopByLabel, Label: "x"}, 5); return err },
		"noise": func() error {
			n, err := st.Noise(ctx, r)
			if err == nil && (n.Alerts != 1 || n.RuleNotifications != 1) {
				err = fmt.Errorf("noise = %+v", n)
			}
			return err
		},
		"instances":   func() error { _, err := eng.Instances(ctx, time.Minute, time.Hour); return err },
		"summary":     func() error { _, err := eng.Summary(ctx); return err },
		"datasources": func() error { _, err := eng.Datasources(ctx, 5); return err },
		"rules":       func() error { _, err := eng.Rules(ctx, biz.GroupFilter{All: true}, 3, 5); return err },
		"shift count": func() error {
			n, err := shifts.CountNotified(ctx, []biz.ShiftWindow{{UserID: uAlice, Start: r.Start, End: r.End}})
			if err == nil && n[0] != 1 {
				err = fmt.Errorf("count = %v", n)
			}
			return err
		},
		"views": func() error {
			v := &biz.SavedView{ID: newID(t), OwnerID: uAlice, Spec: biz.SavedViewSpec{Name: "v", By: "rule"}}
			if err := views.Create(ctx, v, 50); err != nil {
				return err
			}
			if _, err := views.ListVisible(ctx, uAlice, 10); err != nil {
				return err
			}
			if err := views.Update(ctx, v, false); err != nil {
				return err
			}
			return views.Delete(ctx, v.ID, uAlice, false)
		},
	}
	for name, run := range steps {
		if err := run(); err != nil {
			t.Errorf("%s as the core login role: %v", name, err)
		}
	}
}
