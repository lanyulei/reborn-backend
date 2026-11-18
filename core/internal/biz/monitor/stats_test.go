package biz_test

import (
	"context"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// fakeCIIndexE CMDB 资源索引的桩（由其他负责方实现）。
type fakeCIIndexE struct {
	items    map[string]*biz.CIEntry
	valid    int32
	lastSync *time.Time
}

func (f *fakeCIIndexE) Briefs(_ context.Context, ids []string) (map[string]*biz.CIEntry, error) {
	out := map[string]*biz.CIEntry{}
	for _, id := range ids {
		if ci, ok := f.items[id]; ok {
			out[id] = ci
		}
	}
	return out, nil
}

func (f *fakeCIIndexE) ListValid(context.Context, int) ([]*biz.CIEntry, error) { return nil, nil }

func (f *fakeCIIndexE) Summary(context.Context) (int32, *time.Time, error) {
	return f.valid, f.lastSync, nil
}

// statsEnv 统计 / 引擎 / 视图 / 班次用例（建在第一期的 env 之上）。
type statsEnv struct {
	*env
	repo   *testfake.MonitorStats
	cis    *fakeCIIndexE
	stats  *biz.StatsUsecase
	engine *testfake.MonitorEngine
	status *biz.EngineUsecase
	views  *biz.AlertViewUsecase
}

func newStatsEnv(t *testing.T) *statsEnv {
	t.Helper()
	e := newEnv(t)
	repo := e.store.Stats()
	cis := &fakeCIIndexE{items: map[string]*biz.CIEntry{}}
	engine := &testfake.MonitorEngine{}
	return &statsEnv{
		env: e, repo: repo, cis: cis, engine: engine,
		stats:  biz.NewStatsUsecase(e.scopes, repo, repo, e.store.Rules(), cis),
		status: biz.NewEngineUsecase(e.scopes, engine, engine, cis),
		views:  biz.NewAlertViewUsecase(e.scopes, testfake.NewMonitorViews(), testfake.NewMonitorViews(), e.org),
	}
}

// alertSeq 测试事件的序号（并行测试共用）。
var alertSeq atomic.Int64

// seedAlert 预置一条事件；mutate 可改任意字段。
func (e *statsEnv) seedAlert(groupID string, fire time.Time, mutate func(a *biz.Alert)) *biz.Alert {
	a := &biz.Alert{
		ID: fmt.Sprintf("al-%04d", alertSeq.Add(1)), RuleID: "rule-" + groupID, RuleName: "规则 " + groupID, GroupID: groupID,
		DatasourceID: dsVM, Severity: mc.SeverityWarning, Status: mc.AlertFiring, FirstFireTime: fire, LastFireTime: fire,
		Labels: map[string]string{},
	}
	if mutate != nil {
		mutate(a)
	}
	e.store.SeedAlert(a)
	return a
}

func resolved(after time.Duration) func(a *biz.Alert) {
	return func(a *biz.Alert) {
		t := a.FirstFireTime.Add(after)
		a.Status, a.ResolveTime = mc.AlertResolved, &t
	}
}

func lastDays(n int) biz.StatsQuery {
	now := time.Now()
	return biz.StatsQuery{Start: now.AddDate(0, 0, -n), End: now.Add(time.Minute)}
}

func TestStats_ScopeIsolation(t *testing.T) {
	t.Parallel()
	e := newStatsEnv(t)
	ctx := context.Background()
	now := time.Now()
	e.seedAlert(gHost, now.Add(-time.Hour), nil)
	e.seedAlert(gDB, now.Add(-2*time.Hour), func(a *biz.Alert) { a.Severity = mc.SeverityFatal })
	e.seedAlert(gApp, now.Add(-3*time.Hour), nil)
	e.seedAlert(gApp, now.Add(-4*time.Hour), nil)
	e.seedAlert(gApp, now.Add(-5*time.Hour), nil)

	// 各用户看到的事件数：alice / bob = infra 的 2 条，carol = app 的 3 条，dave = host 的 1 条，管理员 5 条，
	// 没有任何范围的人 0 条。
	for user, want := range map[string]int32{uAlice: 2, uBob: 2, uCarol: 3, uDave: 1, uAdmin: 5, "u-nobody": 0} {
		o, err := e.stats.Overview(ctx, user, "", "")
		if err != nil || o.Firing != want {
			t.Fatalf("Overview(%s) firing = %+v, %v; want %d", user, o, err, want)
		}
		tr, err := e.stats.Trend(ctx, user, lastDays(2), "")
		if err != nil {
			t.Fatal(err)
		}
		var total int32
		for _, n := range tr.Totals {
			total += n
		}
		if total != want {
			t.Fatalf("Trend(%s) total = %d, want %d", user, total, want)
		}
		_, sum, err := e.stats.Efficiency(ctx, user, lastDays(2))
		if err != nil || sum.Alerts != want {
			t.Fatalf("Efficiency(%s) = %+v, %v; want %d", user, sum, err, want)
		}
		q := biz.TopQuery{StatsQuery: lastDays(2)}
		q.By = biz.TopByRule
		_, topTotal, err := e.stats.Top(ctx, user, q)
		if err != nil || topTotal != int64(want) {
			t.Fatalf("Top(%s) total = %d, %v; want %d", user, topTotal, err, want)
		}
		n, err := e.stats.Noise(ctx, user, lastDays(2))
		if err != nil || n.Alerts != want {
			t.Fatalf("Noise(%s) = %+v, %v; want %d", user, n, err, want)
		}
	}
	// 指定范围外的分组：与没有范围一样得到空结果，而且不查询。
	before := e.repo.Calls()
	q := lastDays(2)
	q.GroupID = gApp
	if n, err := e.stats.Noise(ctx, uAlice, q); err != nil || n.Alerts != 0 {
		t.Fatalf("Noise(alice, app) = %+v, %v", n, err)
	}
	if o, err := e.stats.Overview(ctx, uAlice, gApp, ""); err != nil || o.Firing != 0 {
		t.Fatalf("Overview(alice, app) = %+v, %v", o, err)
	}
	tq := biz.TopQuery{StatsQuery: q}
	tq.By = biz.TopByRule
	if items, total, err := e.stats.Top(ctx, uAlice, tq); err != nil || total != 0 || len(items) != 0 {
		t.Fatalf("Top(alice, app) = %v, %d, %v", items, total, err)
	}
	if e.repo.Calls() != before {
		t.Fatalf("out-of-scope statistics must not query the store")
	}
	// 指定下级分组：只统计它。
	q.GroupID = gHost
	if n, err := e.stats.Noise(ctx, uAlice, q); err != nil || n.Alerts != 1 {
		t.Fatalf("Noise(alice, host) = %+v, %v", n, err)
	}
	// dave 只对 host 有范围：指定上级 infra 时只统计 host。
	q.GroupID = gInfra
	if n, err := e.stats.Noise(ctx, uDave, q); err != nil || n.Alerts != 1 {
		t.Fatalf("Noise(dave, infra) = %+v, %v", n, err)
	}
}

func TestStats_Validation(t *testing.T) {
	t.Parallel()
	e := newStatsEnv(t)
	ctx := context.Background()
	now := time.Now()
	tooLong := biz.StatsQuery{Start: now.Add(-90*24*time.Hour - time.Second), End: now}
	backwards := biz.StatsQuery{Start: now, End: now}
	for name, q := range map[string]biz.StatsQuery{"too long": tooLong, "empty": backwards} {
		_, err := e.stats.Trend(ctx, uAdmin, q, "")
		wantErr(t, "Trend "+name, err, biz.ErrTimeRangeInvalid)
		_, _, err = e.stats.Efficiency(ctx, uAdmin, q)
		wantErr(t, "Efficiency "+name, err, biz.ErrTimeRangeInvalid)
		_, err = e.stats.Noise(ctx, uAdmin, q)
		wantErr(t, "Noise "+name, err, biz.ErrTimeRangeInvalid)
		tq := biz.TopQuery{StatsQuery: q}
		tq.By = biz.TopByCI
		_, _, err = e.stats.Top(ctx, uAdmin, tq)
		wantErr(t, "Top "+name, err, biz.ErrTimeRangeInvalid)
	}
	// 恰好 90 天可以。
	if _, err := e.stats.Noise(ctx, uAdmin, biz.StatsQuery{Start: now.Add(-90 * 24 * time.Hour), End: now}); err != nil {
		t.Fatalf("90 days: %v", err)
	}
	for _, tz := range []string{"Mars/Phobos", "Local"} {
		_, err := e.stats.Overview(ctx, uAdmin, "", tz)
		wantErr(t, "Overview tz "+tz, err, biz.ErrQueryInvalid)
		_, err = e.stats.Trend(ctx, uAdmin, lastDays(1), tz)
		wantErr(t, "Trend tz "+tz, err, biz.ErrQueryInvalid)
	}
	bad := lastDays(1)
	bad.By = "rule"
	_, err := e.stats.Trend(ctx, uAdmin, bad, "")
	wantErr(t, "Trend by", err, biz.ErrQueryInvalid)
	_, _, err = e.stats.Efficiency(ctx, uAdmin, bad)
	wantErr(t, "Efficiency by", err, biz.ErrQueryInvalid)
	for _, by := range []string{"", "severity", "label:", "label:1x"} {
		tq := biz.TopQuery{StatsQuery: lastDays(1)}
		tq.By = by
		_, _, err := e.stats.Top(ctx, uAdmin, tq)
		wantErr(t, "Top by "+by, err, biz.ErrQueryInvalid)
	}
}

func TestStats_Overview(t *testing.T) {
	t.Parallel()
	e := newStatsEnv(t)
	ctx := context.Background()
	loc, _ := time.LoadLocation("Asia/Shanghai")
	now := time.Now().In(loc)
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, loc)
	yesterday := today.Add(-12 * time.Hour)
	who := "u-x"
	ack := today
	e.seedAlert(gHost, today, func(a *biz.Alert) { a.Severity = mc.SeverityFatal })
	e.seedAlert(gHost, today, func(a *biz.Alert) { a.AckUser, a.AckTime = who, &ack })
	e.seedAlert(gHost, yesterday, func(a *biz.Alert) { a.Muted = true })
	e.seedAlert(gHost, yesterday, func(a *biz.Alert) { a.Inhibited, a.InhibitKind = true, mc.InhibitRule })
	// 昨天触发、今天恢复 / 关闭；昨天触发昨天恢复的不计入今日。
	e.seedAlert(gDB, yesterday, func(a *biz.Alert) { a.Status, a.ResolveTime = mc.AlertResolved, &today })
	e.seedAlert(gDB, yesterday, func(a *biz.Alert) { a.Status, a.ResolveTime = mc.AlertClosed, &today })
	e.seedAlert(gDB, yesterday, resolved(time.Minute))
	o, err := e.stats.Overview(ctx, uAlice, "", "")
	if err != nil {
		t.Fatal(err)
	}
	want := biz.AlertOverview{
		Firing: 4, FiringBySeverity: [4]int32{0, 3, 0, 1}, Unacked: 1, Muted: 1, Inhibited: 1,
		TodayFired: 2, TodayResolved: 1, TodayClosed: 1,
	}
	if *o != want {
		t.Fatalf("Overview = %+v, want %+v", *o, want)
	}
}

func TestStats_TrendBuckets(t *testing.T) {
	t.Parallel()
	e := newStatsEnv(t)
	ctx := context.Background()
	loc, _ := time.LoadLocation("Asia/Shanghai")
	day := func(d int, hour int) time.Time { return time.Date(2026, 11, d, hour, 0, 0, 0, loc) }
	e.seedAlert(gHost, day(10, 0), nil) // 10 日 00:00（UTC 还是 9 日）
	e.seedAlert(gHost, day(10, 23), func(a *biz.Alert) { a.Severity = mc.SeverityFatal })
	e.seedAlert(gDB, day(12, 9), nil)
	e.seedAlert(gInfra, day(12, 10), nil)
	e.seedAlert(gApp, day(12, 11), nil)
	q := biz.StatsQuery{Start: day(10, 0), End: day(13, 0)}
	tr, err := e.stats.Trend(ctx, uAdmin, q, "Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(tr.Days, []string{"2026-11-10", "2026-11-11", "2026-11-12"}) || !slices.Equal(tr.Totals, []int32{2, 0, 3}) {
		t.Fatalf("days = %v totals = %v", tr.Days, tr.Totals)
	}
	if len(tr.Series) != 4 || tr.Series[1].Key != "2" || tr.Series[1].Name != "警告" || !slices.Equal(tr.Series[1].Counts, []int32{1, 0, 3}) ||
		!slices.Equal(tr.Series[3].Counts, []int32{1, 0, 0}) || tr.Series[3].Total != 1 {
		t.Fatalf("severity series = %+v / %+v", tr.Series[1], tr.Series[3])
	}
	// 同一批事件按 UTC 分桶：10 日 00:00（上海）落在 9 日。
	utc, err := e.stats.Trend(ctx, uAdmin, q, "UTC")
	if err != nil || utc.Days[0] != "2026-11-09" || utc.Totals[0] != 1 {
		t.Fatalf("utc trend = %+v, %v", utc, err)
	}
	// 按分组、未指定分组：归到可见的顶级分组（管理员：infra / app）。
	q.By = biz.StatsByGroup
	tr, err = e.stats.Trend(ctx, uAdmin, q, "Asia/Shanghai")
	if err != nil || len(tr.Series) != 2 || tr.Series[0].Key != gInfra || tr.Series[0].Total != 4 || tr.Series[0].Name != "基础设施" ||
		tr.Series[1].Key != gApp || tr.Series[1].Total != 1 {
		t.Fatalf("group series = %+v, %v", tr.Series, err)
	}
	// dave 只能读 host：他的顶级可见分组是 host，显示完整名称。
	tr, err = e.stats.Trend(ctx, uDave, q, "Asia/Shanghai")
	if err != nil || len(tr.Series) != 1 || tr.Series[0].Key != gHost || tr.Series[0].Total != 2 || tr.Series[0].Name != "基础设施 / 主机" {
		t.Fatalf("dave group series = %+v, %v", tr.Series, err)
	}
	// 指定分组：按它的直接下级拆分，直接在它里面的事件归它自己。
	q.GroupID = gInfra
	tr, err = e.stats.Trend(ctx, uAlice, q, "Asia/Shanghai")
	if err != nil || len(tr.Series) != 3 {
		t.Fatalf("infra children = %+v, %v", tr.Series, err)
	}
	got := map[string]string{}
	for _, s := range tr.Series {
		got[s.Key] = fmt.Sprintf("%s:%d", s.Name, s.Total)
	}
	if got[gHost] != "主机:2" || got[gDB] != "数据库:1" || got[gInfra] != "基础设施:1" {
		t.Fatalf("infra children = %v", got)
	}
}

func TestStats_GroupRollUp(t *testing.T) {
	t.Parallel()
	e := newStatsEnv(t)
	ctx := context.Background()
	now := time.Now()
	// app 下 12 个子分组，第 i 个有 i 条事件：留下最多的 10 个，其余并入「其他」（1 + 2 = 3 条）。
	for i := 1; i <= 12; i++ {
		id := fmt.Sprintf("g-app-%02d", i)
		e.store.SeedGroup(id, gApp, fmt.Sprintf("子系统 %02d", i))
		for range i {
			e.seedAlert(id, now.Add(-time.Hour), resolved(time.Duration(i)*time.Minute))
		}
	}
	q := lastDays(1)
	q.GroupID, q.By = gApp, biz.StatsByGroup
	tr, err := e.stats.Trend(ctx, uCarol, q, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(tr.Series) != 11 || tr.Series[0].Key != "g-app-12" || tr.Series[0].Total != 12 || tr.Series[9].Key != "g-app-03" ||
		tr.Series[10].Key != biz.StatsOtherKey || tr.Series[10].Total != 3 || tr.Series[10].Name != "其他" {
		t.Fatalf("series = %d, first %+v, last %+v", len(tr.Series), tr.Series[0], tr.Series[len(tr.Series)-1])
	}
	items, total, err := e.stats.Efficiency(ctx, uCarol, q)
	if err != nil {
		t.Fatal(err)
	}
	// 「其他」行：1 条恢复用 1 分钟 + 2 条各用 2 分钟 → 平均 100 秒。
	other := items[len(items)-1]
	if len(items) != 11 || other.Key != biz.StatsOtherKey || other.Alerts != 3 || other.Resolved != 3 || other.MTTRS != 100 {
		t.Fatalf("efficiency other = %+v (%d items)", other, len(items))
	}
	if total.Key != biz.StatsTotalKey || total.Alerts != 78 || total.Resolved != 78 || total.Acked != 0 || total.MTTAS != 0 {
		t.Fatalf("total = %+v", total)
	}
}

func TestStats_Efficiency(t *testing.T) {
	t.Parallel()
	e := newStatsEnv(t)
	ctx := context.Background()
	fire := time.Now().Add(-6 * time.Hour)
	ackAt := func(d time.Duration) func(a *biz.Alert) {
		return func(a *biz.Alert) {
			at := a.FirstFireTime.Add(d)
			a.AckUser, a.AckTime = uAlice, &at
		}
	}
	e.seedAlert(gHost, fire, ackAt(2*time.Minute))
	e.seedAlert(gHost, fire, func(a *biz.Alert) { ackAt(4 * time.Minute)(a); resolved(10 * time.Minute)(a) })
	e.seedAlert(gHost, fire, func(a *biz.Alert) { resolved(30 * time.Minute)(a); a.Severity = mc.SeverityFatal })
	// 手动关闭的不算恢复。
	e.seedAlert(gHost, fire, func(a *biz.Alert) { resolved(time.Hour)(a); a.Status = mc.AlertClosed })
	q := lastDays(1)
	q.By = biz.StatsBySeverity
	items, total, err := e.stats.Efficiency(ctx, uAlice, q)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 4 || items[0].Alerts != 0 || items[0].Name != "提示" {
		t.Fatalf("items = %+v", items)
	}
	if w := items[1]; w.Alerts != 3 || w.Acked != 2 || w.Resolved != 1 || w.MTTAS != 180 || w.MTTRS != 600 {
		t.Fatalf("warning row = %+v", w)
	}
	if f := items[3]; f.Alerts != 1 || f.Acked != 0 || f.MTTAS != 0 || f.MTTRS != 1800 {
		t.Fatalf("fatal row = %+v", f)
	}
	if total.Alerts != 4 || total.Acked != 2 || total.Resolved != 2 || total.MTTAS != 180 || total.MTTRS != 1200 || total.Name != "合计" {
		t.Fatalf("total = %+v", total)
	}
}

func TestStats_Top(t *testing.T) {
	t.Parallel()
	e := newStatsEnv(t)
	ctx := context.Background()
	now := time.Now()
	rule := e.mustRule(t, uAlice, spec(gHost, "CPU 高"))
	for i := range 3 {
		e.seedAlert(gHost, now.Add(-time.Duration(i+1)*time.Hour), func(a *biz.Alert) {
			a.RuleID, a.RuleName, a.CIID = rule.ID, "CPU 高（旧名）", "ci-1"
			a.Labels = map[string]string{"instance": "web-1"}
			if i > 0 {
				resolved(time.Minute)(a)
			}
		})
	}
	e.seedAlert(gDB, now.Add(-time.Hour), func(a *biz.Alert) {
		a.RuleID, a.RuleName, a.CIID, a.Severity = "rule-gone", "已删除的规则", "ci-2", mc.SeverityFatal
		a.Labels = map[string]string{"instance": "db-1"}
	})
	e.seedAlert(gDB, now.Add(-time.Hour), func(a *biz.Alert) { a.RuleID, a.RuleName = "rule-gone", "已删除的规则" })
	e.cis.items["ci-1"] = &biz.CIEntry{ID: "ci-1", Name: "web-1", Valid: true}
	e.cis.items["ci-2"] = &biz.CIEntry{ID: "ci-2", Name: "db-1", Valid: false}

	q := biz.TopQuery{StatsQuery: lastDays(1)}
	q.By = biz.TopByRule
	items, total, err := e.stats.Top(ctx, uAlice, q)
	if err != nil || total != 5 || len(items) != 2 {
		t.Fatalf("top rule = %+v, %d, %v", items, total, err)
	}
	if r := items[0]; r.Key != rule.ID || r.Name != "CPU 高" || !r.Exists || r.Count != 3 || r.Firing != 1 || r.GroupName != "基础设施 / 主机" {
		t.Fatalf("top rule[0] = %+v", r)
	}
	if r := items[1]; r.Name != "已删除的规则" || r.Exists || r.MaxSeverity != mc.SeverityFatal || r.GroupName != "基础设施 / 数据库" {
		t.Fatalf("top rule[1] = %+v", r)
	}
	q.By = biz.TopByCI
	items, total, err = e.stats.Top(ctx, uAlice, q)
	if err != nil || total != 4 || len(items) != 2 || items[0].Name != "web-1" || !items[0].Exists || items[1].Name != "db-1" || items[1].Exists ||
		items[0].GroupName != "" {
		t.Fatalf("top ci = %+v, %d, %v", items, total, err)
	}
	q.By, q.Limit = "label:instance", 1
	items, total, err = e.stats.Top(ctx, uAlice, q)
	if err != nil || total != 4 || len(items) != 1 || items[0].Key != "web-1" || items[0].Name != "web-1" || !items[0].Exists {
		t.Fatalf("top label = %+v, %d, %v", items, total, err)
	}
	// limit 夹到上限；缺省 10。
	q.Limit = 1000
	if items, _, err = e.stats.Top(ctx, uAlice, q); err != nil || len(items) != 2 {
		t.Fatalf("top label clamp = %+v, %v", items, err)
	}
}

func TestStats_Noise(t *testing.T) {
	t.Parallel()
	e := newStatsEnv(t)
	ctx := context.Background()
	fire := time.Now().Add(-time.Hour)
	notified := e.seedAlert(gHost, fire, func(a *biz.Alert) { a.NotifyCount = 2 })
	for i, src := range []int32{mc.NotifySourceRule, mc.NotifySourceRule, mc.NotifySourceSubscription, mc.NotifySourceEscalation} {
		e.store.SeedNotifyLog(&biz.NotifyLog{ID: fmt.Sprintf("l-%d", i), AlertID: notified.ID, Source: src, CreateTime: fire})
	}
	// 只有订阅通知（规则没有通知计数）的事件也算通知过。
	subOnly := e.seedAlert(gHost, fire, nil)
	e.store.SeedNotifyLog(&biz.NotifyLog{ID: "l-sub", AlertID: subOnly.ID, Source: mc.NotifySourceSubscription, CreateTime: fire})
	e.seedAlert(gHost, fire, func(a *biz.Alert) { a.Muted = true })
	// 屏蔽过、后来解除并通知了：既算被屏蔽过，也算通知过。
	unmuted := e.seedAlert(gHost, fire, func(a *biz.Alert) { a.NotifyCount = 1 })
	e.repo.MarkTimeline(unmuted.ID, true, false)
	e.seedAlert(gHost, fire, func(a *biz.Alert) { a.Inhibited, a.InhibitKind = true, mc.InhibitTopology })
	// 别的分组的事件与通知不计。
	other := e.seedAlert(gApp, fire, func(a *biz.Alert) { a.NotifyCount = 1 })
	e.store.SeedNotifyLog(&biz.NotifyLog{ID: "l-app", AlertID: other.ID, Source: mc.NotifySourceRule, CreateTime: fire})
	n, err := e.stats.Noise(ctx, uAlice, lastDays(1))
	if err != nil {
		t.Fatal(err)
	}
	want := biz.AlertNoise{Alerts: 5, Muted: 2, Inhibited: 1, Notified: 3, RuleNotifications: 2, SubscriptionNotifications: 2, EscalationNotifications: 1}
	if *n != want {
		t.Fatalf("Noise = %+v, want %+v", *n, want)
	}
}

func TestEngine_Status(t *testing.T) {
	t.Parallel()
	e := newStatsEnv(t)
	ctx := context.Background()
	sync := time.Now().Add(-time.Minute)
	e.cis.valid, e.cis.lastSync = 42, &sync
	since := time.Now().Add(-time.Hour)
	e.engine.RuleItems = []*biz.EngineRuleIssue{
		{RuleID: "r1", RuleName: "主机规则", GroupID: gHost, Failures: 5, Since: &since},
		{RuleID: "r2", RuleName: "应用规则", GroupID: gApp, Failures: 9, Since: &since},
		{RuleID: "r3", RuleName: "偶发", GroupID: gHost, Failures: 2},
	}
	e.engine.DatasourceItems = []*biz.EngineDatasourceIssue{{ID: dsVM, Name: "vm", Error: "timeout", RuleCount: 3}}
	e.engine.SummaryValue = biz.EngineSummary{TaskTotal: 100, TaskOverdue: 5, NotifyPending: 7}
	e.engine.InstanceItems = []*biz.EngineInstance{
		{Instance: "a", Roles: []string{"planner", "evaluator"}, Leader: true, Alive: true},
		{Instance: "b", Roles: []string{"evaluator"}},
	}
	st, err := e.status.Status(ctx, uAlice)
	if err != nil {
		t.Fatal(err)
	}
	if !st.Healthy || len(st.Instances) != 2 || st.Summary.NotifyPending != 7 || st.CICount != 42 || st.CISyncTime == nil || len(st.Datasources) != 1 {
		t.Fatalf("status = %+v", st)
	}
	// 持续失败的规则只返回范围内、且达到阈值的。
	if len(st.Rules) != 1 || st.Rules[0].RuleID != "r1" || st.Rules[0].GroupName != "基础设施 / 主机" {
		t.Fatalf("alice rules = %+v", st.Rules)
	}
	if st, _ = e.status.Status(ctx, uCarol); len(st.Rules) != 1 || st.Rules[0].RuleID != "r2" {
		t.Fatalf("carol rules = %+v", st.Rules)
	}
	if st, _ = e.status.Status(ctx, uAdmin); len(st.Rules) != 2 {
		t.Fatalf("admin rules = %+v", st.Rules)
	}
	if st, _ = e.status.Status(ctx, "u-nobody"); len(st.Rules) != 0 || st.Rules == nil {
		t.Fatalf("nobody rules = %+v", st.Rules)
	}
	// 不健康的三种情形：积压超过 5%、没有选主实例、没有存活的评估实例。
	e.engine.SummaryValue.TaskOverdue = 6
	if st, _ = e.status.Status(ctx, uAdmin); st.Healthy {
		t.Fatal("backlog over 5% must be unhealthy")
	}
	e.engine.SummaryValue.TaskOverdue = 0
	e.engine.InstanceItems[0].Leader = false
	if st, _ = e.status.Status(ctx, uAdmin); st.Healthy {
		t.Fatal("no leader must be unhealthy")
	}
	e.engine.InstanceItems[0].Leader, e.engine.InstanceItems[0].Roles = true, []string{"planner"}
	if st, _ = e.status.Status(ctx, uAdmin); st.Healthy {
		t.Fatal("no alive evaluator must be unhealthy")
	}
	if got := biz.SplitEngineRoles(" planner, evaluator ,,"); !slices.Equal(got, []string{"planner", "evaluator"}) {
		t.Fatalf("SplitEngineRoles = %v", got)
	}
}
