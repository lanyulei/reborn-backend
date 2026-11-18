//go:build integration

package data

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/monitor/internal/biz"
	"github.com/lanyulei/reborn/backend/monitor/internal/promtest"
)

// 第二轮评审（docs/monitor-review-round2.md，R 项）的回归测试：凡是要靠真实 PostgreSQL 才能证明的行为。

// ageOutbox 把发件箱行的时间整体前移 d（等价于时间过去了 d）。
func (e *env) ageOutbox(d time.Duration) {
	e.t.Helper()
	e.exec("UPDATE monitor_notify_outbox SET create_time = create_time - $1::interval, done_time = done_time - $1::interval", d)
}

// R1：首次通知行被 core 丢弃（派发失败次数用尽）→ 引擎按重复通知间隔把同一行重新置为待派发 → core 派发
// → 之后的重复通知与恢复通知与普通事件一样。节奏有上限：不是每轮都重新激活，也不新增行；首次通知还在
// 排队（含 core 失败重试中）的事件不动。core 的动作全部经它被授权的列模拟。
func TestDroppedFirstNotificationIsRetriedOnABoundedCadence(t *testing.T) {
	e := newEnv(t)
	prom := promtest.New(t)
	e.datasource(prom.URL)
	ruleID := e.rule(ruleSpec{RepeatM: 5})
	prom.Set(upDown, promtest.S(0, "instance", "a"))
	g := e.engine("")
	g.materialize()
	g.evalDue()
	a := e.oneAlert(ruleID)
	first := e.outboxRow(a.ID, monitorcontract.NotifyKindFiring, 1)
	row := func() outboxRow {
		t.Helper()
		return e.outboxRow(a.ID, monitorcontract.NotifyKindFiring, 1)
	}
	rows := func() int { return e.count("SELECT count(*) FROM monitor_notify_outbox WHERE alert_id = $1", a.ID) }

	// 首次通知还在排队，core 已经失败过 3 次（待派发行的 done_time 是 core 记的「最近一次失败时间」）：
	// 过去多久引擎都不碰它。
	e.exec("UPDATE monitor_notify_outbox SET attempts = 3, last_error = 'boom', done_time = now() WHERE id = $1", first.ID)
	e.ageOutbox(2 * time.Hour)
	e.age(2 * time.Hour)
	for range 3 {
		g.evalDue()
	}
	if got := row(); got.ID != first.ID || got.Status != int(monitorcontract.OutboxPending) || got.Attempts != 3 || got.LastError != "boom" || got.Done == nil {
		t.Fatalf("a pending first notification must be left alone: %+v", got)
	}

	// core 用尽次数丢弃。此后连续多轮评估：没到间隔，一次都不重新激活。
	e.drop(first.ID)
	for range 5 {
		g.evalDue()
		if got := row(); got.Status != int(monitorcontract.OutboxDropped) || got.Attempts != int(monitorcontract.OutboxMaxAttempts) {
			t.Fatalf("re-armed right after the drop: %+v", got)
		}
	}
	e.ageOutbox(4 * time.Minute)
	g.evalDue()
	if got := row(); got.Status != int(monitorcontract.OutboxDropped) {
		t.Fatalf("re-armed before the repeat interval (5m) elapsed: %+v", got)
	}
	// 距丢弃满一个重复间隔：原地重新置为待派发（失败次数清零），不新增行。
	e.ageOutbox(61 * time.Second)
	g.evalDue()
	if got := row(); got.ID != first.ID || got.Status != int(monitorcontract.OutboxPending) || got.Attempts != 0 || got.LastError != "" || got.Done != nil {
		t.Fatalf("the dropped first notification must be re-armed in place: %+v", got)
	}
	if rows() != 1 {
		t.Fatalf("re-arming must not add rows: %v", e.outbox(a.ID))
	}
	// 重新激活后是待派发：后续各轮不再动它（失败次数不被再次清零）。
	e.exec("UPDATE monitor_notify_outbox SET attempts = 2, done_time = now() WHERE id = $1", first.ID)
	e.ageOutbox(time.Hour)
	for range 3 {
		g.evalDue()
	}
	if got := row(); got.Status != int(monitorcontract.OutboxPending) || got.Attempts != 2 {
		t.Fatalf("a re-armed row is pending and must be left alone: %+v", got)
	}
	// core 再次丢弃：又要等满一个间隔，而不是下一轮就重新激活。
	e.drop(first.ID)
	for range 3 {
		g.evalDue()
	}
	if got := row(); got.Status != int(monitorcontract.OutboxDropped) {
		t.Fatalf("re-armed again without waiting: %+v", got)
	}
	e.ageOutbox(5*time.Minute + time.Second)
	g.evalDue()
	if got := row(); got.Status != int(monitorcontract.OutboxPending) || got.Attempts != 0 || rows() != 1 {
		t.Fatalf("second retry: %+v, rows = %d", got, rows())
	}

	// 这一次 core 派发成功：通知计数回写。之后与普通事件一样。
	e.dispatch()
	if cur := e.oneAlert(ruleID); cur.NotifyCount != 1 {
		t.Fatalf("notify count = %d", cur.NotifyCount)
	}
	g.evalDue()
	if got := e.outbox(a.ID); !equalStrings(got, []string{"1/1/2"}) {
		t.Fatalf("outbox = %v", got)
	}
	e.age(6 * time.Minute)
	g.evalDue()
	if got := e.outbox(a.ID); !equalStrings(got, []string{"1/1/2", "2/2/1"}) {
		t.Fatalf("repeat notification after the retried first one: %v", got)
	}
	e.dispatch()
	prom.Set(upDown)
	g.evalDue()
	if cur := e.oneAlert(ruleID); cur.Status != int(monitorcontract.AlertResolved) || cur.NotifyCount != 2 {
		t.Fatalf("alert = %+v", cur)
	}
	if got := e.outbox(a.ID); !equalStrings(got, []string{"1/1/2", "2/2/2", "3/3/1"}) {
		t.Fatalf("recovery notification: %v", got)
	}
}

// R1：规则没有配置重复通知时，首次通知按固定间隔（10 分钟）重试；送达后不再有通知。
func TestDroppedFirstNotificationRetryWithoutRepeatInterval(t *testing.T) {
	e := newEnv(t)
	prom := promtest.New(t)
	e.datasource(prom.URL)
	ruleID := e.rule(ruleSpec{})
	prom.Set(upDown, promtest.S(0, "instance", "a"))
	g := e.engine("")
	g.materialize()
	g.evalDue()
	a := e.oneAlert(ruleID)
	first := e.outboxRow(a.ID, monitorcontract.NotifyKindFiring, 1)
	e.drop(first.ID)
	e.ageOutbox(9 * time.Minute)
	g.evalDue()
	if got := e.outboxRow(a.ID, monitorcontract.NotifyKindFiring, 1); got.Status != int(monitorcontract.OutboxDropped) {
		t.Fatalf("re-armed before 10 minutes: %+v", got)
	}
	e.ageOutbox(61 * time.Second)
	g.evalDue()
	if got := e.outboxRow(a.ID, monitorcontract.NotifyKindFiring, 1); got.ID != first.ID || got.Status != int(monitorcontract.OutboxPending) || got.Attempts != 0 {
		t.Fatalf("must be re-armed after 10 minutes: %+v", got)
	}
	e.dispatch()
	e.age(time.Hour)
	g.evalDue()
	if got := e.outbox(a.ID); !equalStrings(got, []string{"1/1/2"}) || e.oneAlert(ruleID).NotifyCount != 1 {
		t.Fatalf("outbox = %v", got)
	}
}

// R3：被顺延的任务在任务行上留下标记（无属主 + 已过期的 lease_until）；标记随归还保留、随领取带回，
// 评估完成才清除。带着标记的那一轮让待触发 / 留观计时重新起算——与查询错误、不在生效时间的轮次同一口径。
func TestPostponedTaskCarriesTheMarkAndRestartsTimers(t *testing.T) {
	e := newEnv(t)
	prom := promtest.New(t)
	ds := e.datasource(prom.URL)
	ruleID := e.rule(ruleSpec{ForS: 60, RecoverS: 120})
	g := e.engine("")
	g.materialize()
	repo := NewTaskRepo(e.d)
	ctx := context.Background()
	marked := func() bool {
		t.Helper()
		return e.count(`SELECT count(*) FROM monitor_eval_task WHERE rule_id = $1 AND lease_owner IS NULL AND lease_until IS NOT NULL
			AND lease_until <= now()`, ruleID) == 1
	}
	// postpone 以评估器的方式顺延一轮：领取、置标记、归还。
	postpone := func(token string) {
		t.Helper()
		task := claimAfter(t, e, repo, token)
		task.Postponed = true
		if err := repo.Release(ctx, token, []*biz.Task{task}, 5*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	sinceAgo := func(column string) float64 {
		t.Helper()
		var ago float64
		if err := e.admin.QueryRow(ctx,
			"SELECT extract(epoch FROM now() - "+column+")::float8 FROM monitor_eval_state WHERE rule_id = $1", ruleID).Scan(&ago); err != nil {
			t.Fatal(err)
		}
		return ago
	}

	prom.Set(upDown, promtest.S(0, "instance", "a"))
	g.evalDue()
	if marked() {
		t.Fatalf("a completed round must leave no mark")
	}
	// 普通归还不留标记；租约过期被接管（属主非空）也不是顺延。
	plain := claimAfter(t, e, repo, "plain")
	if plain.Postponed {
		t.Fatalf("task = %+v", plain)
	}
	if err := repo.Release(ctx, "plain", []*biz.Task{plain}, 0); err != nil || marked() {
		t.Fatalf("plain release: err = %v, marked = %v", err, marked())
	}
	claimAfter(t, e, repo, "crashed")
	e.exec("UPDATE monitor_eval_task SET lease_until = now() - interval '1 second'")
	if taken := claimAfter(t, e, repo, "takeover"); taken.Postponed {
		t.Fatalf("an expired lease is not a postponed round: %+v", taken)
	} else if err := repo.Release(ctx, "takeover", []*biz.Task{taken}, 0); err != nil {
		t.Fatal(err)
	}

	// 顺延：标记落库，任务 5 秒后可再领。
	postpone("busy-1")
	if got := e.task(ruleID, ds); !marked() || got.Owner != nil || got.NextIn < 3 || got.NextIn > 6 {
		t.Fatalf("postponed task = %+v, marked = %v", got, marked())
	}
	// 再被领取但没评估就归还（停机移交）：标记原样保留。
	handed := claimAfter(t, e, repo, "stopping")
	if !handed.Postponed {
		t.Fatalf("the mark must come back with the claim: %+v", handed)
	}
	if err := repo.Release(ctx, "stopping", []*biz.Task{handed}, 0); err != nil || !marked() {
		t.Fatalf("hand-back must keep the mark: err = %v, marked = %v", err, marked())
	}

	// 其间过去了 61s（超过 for_s）。带着标记的这一轮不触发，持续时长从本轮重新起算；标记清除。
	e.age(61 * time.Second)
	g.evalDue()
	if len(e.alerts(ruleID)) != 0 {
		t.Fatalf("for_s was bypassed across a postponed round")
	}
	if ago := sinceAgo("active_since"); ago > 5 {
		t.Fatalf("active_since must restart at the first observed round after the postponed one, is %.0fs old", ago)
	}
	if marked() {
		t.Fatalf("a completed round must clear the mark")
	}
	e.age(61 * time.Second)
	g.evalDue()
	a := e.oneAlert(ruleID)

	// 留观同理：进入留观后顺延一轮，其间过去了 125s（超过 recover_s）——不恢复，留观从本轮重新起算。
	prom.Set(upDown)
	g.evalDue()
	e.age(125 * time.Second)
	postpone("busy-2")
	g.evalDue()
	if cur := e.oneAlert(ruleID); cur.Status != int(monitorcontract.AlertFiring) || cur.ID != a.ID {
		t.Fatalf("the postponed round was counted as observed-healthy time: %+v", cur)
	}
	if ago := sinceAgo("recover_since"); ago > 5 {
		t.Fatalf("recover_since must restart after the postponed round, is %.0fs old", ago)
	}
	e.age(121 * time.Second)
	g.evalDue()
	if cur := e.oneAlert(ruleID); cur.Status != int(monitorcontract.AlertResolved) {
		t.Fatalf("alert must resolve after a full observed recover_s: %+v", cur)
	}
}

// R2：健康但慢的数据源（每次查询 1.6s，超时 2s）在并发槽排满时不产生查询错误。20 条规则同时到期、
// 8 个并发槽：晚拿到槽的查询有完整的查询超时；等槽超出预算的任务顺延（带标记归还），不落判定记录。
// 没有任务还攥着租约，耗时都在「等槽预算 + 查询超时」之内。
func TestSlowDatasourceWithSaturatedSlotsProducesNoQueryErrors(t *testing.T) {
	const rules = 20
	e := newEnv(t)
	prom := promtest.New(t)
	e.datasource(prom.URL) // timeout_ms = 2000
	for range rules {
		e.rule(ruleSpec{})
	}
	prom.Set(upDown, promtest.S(0, "instance", "a"))
	prom.SetDelay(1600 * time.Millisecond)
	g := e.engine("")
	g.evaluator.WithOptions(biz.EvaluatorOptions{Batch: rules})
	g.materialize()
	e.due()
	start := time.Now()
	n, err := g.evaluator.RunOnce(context.Background(), g.token)
	if err != nil || n != rules {
		t.Fatalf("RunOnce() = %d, %v", n, err)
	}
	elapsed := time.Since(start)
	errors := func() int {
		return e.count("SELECT count(*) FROM monitor_eval_record WHERE status = $1", monitorcontract.EvalError)
	}
	if errors() != 0 || g.obs.failed.Load() != 0 {
		t.Fatalf("query error rounds = %d, failed = %d: slot wait was charged to the query timeout", errors(), g.obs.failed.Load())
	}
	done := e.count("SELECT count(*) FROM monitor_eval_task WHERE last_status = $1 AND lease_until IS NULL", monitorcontract.EvalFiring)
	postponed := e.count("SELECT count(*) FROM monitor_eval_task WHERE last_status = $1 AND lease_owner IS NULL AND lease_until IS NOT NULL", monitorcontract.EvalNever)
	if done != 16 || postponed != rules-16 {
		t.Fatalf("completed = %d, postponed = %d, want 16 / %d", done, postponed, rules-16)
	}
	if got := prom.MaxInFlight(); got != 8 {
		t.Fatalf("max in-flight queries = %d, want 8", got)
	}
	// 租约占用有上限：没有任务还被占着，单个任务与整批都在 2 × 查询超时（等槽预算 + 查询）附近，
	// 远小于租约 max(60s, 3 × 2s + 10s)。
	if held := e.count("SELECT count(*) FROM monitor_eval_task WHERE lease_owner IS NOT NULL"); held != 0 {
		t.Fatalf("%d tasks still hold a lease", held)
	}
	if longest := e.count("SELECT max(last_duration_ms) FROM monitor_eval_task"); longest > 5000 || elapsed > 6*time.Second {
		t.Fatalf("longest round = %dms, batch = %v", longest, elapsed)
	}
	// 顺延的任务随后照常评估完，仍然没有查询错误。
	prom.SetDelay(0)
	g.evalDue()
	if got := e.count("SELECT count(*) FROM monitor_eval_task WHERE last_status = $1 AND lease_until IS NULL", monitorcontract.EvalFiring); got != rules || errors() != 0 {
		t.Fatalf("firing tasks = %d, query error rounds = %d", got, errors())
	}
}

// R4 / R6：标签超限的序列删减 / 截断后照常落库，提示写进任务与判定记录；部分结果在判定记录明细里的
// 键名是 queries[].partial（core 以同一个结构读出）。
func TestLabelLimitsAndPartialFlagAreStored(t *testing.T) {
	e := newEnv(t)
	prom := promtest.New(t)
	ds := e.datasource(prom.URL)
	ruleID := e.rule(ruleSpec{})
	kv := []string{"instance", "a", "msg", strings.Repeat("错", 2000)}
	for i := range 80 {
		kv = append(kv, fmt.Sprintf("z%03d", i), "v") // 名称排在 instance / msg 之后：被删掉的是它们里的一部分
	}
	prom.Set(upDown, promtest.S(0, kv...))
	prom.SetPartial(true)
	g := e.engine("")
	g.materialize()
	g.evalDue()
	a := e.oneAlert(ruleID)
	if len(a.Labels) != 64 || a.Labels["instance"] != "a" || a.Labels["z061"] != "v" || a.Labels["z062"] != "" ||
		len(a.Labels["msg"]) > 1024 || !strings.HasSuffix(a.Labels["msg"], "...[truncated]") {
		t.Fatalf("stored labels = %d, msg = %d bytes", len(a.Labels), len(a.Labels["msg"]))
	}
	const want = "incomplete result: query 0 returned a partial result; series not returned were left unchanged; " +
		"label limit: 1 series exceeded the label limits (at most 64 labels per series, 128 bytes per name, 1024 bytes per value); " +
		"extra labels were dropped and long values truncated"
	if rec := e.lastRecord(ruleID); rec.Error != want || rec.Status != int(monitorcontract.EvalFiring) {
		t.Fatalf("record = status %d error %q", rec.Status, rec.Error)
	}
	if got := e.task(ruleID, ds); got.Error != want {
		t.Fatalf("task error = %q", got.Error)
	}
	if n := e.count(`SELECT count(*) FROM monitor_eval_record WHERE rule_id = $1 AND detail->'queries'->0->>'partial' = 'true'
		AND detail->'queries'->0 ? 'series' AND NOT detail->'queries'->0 ? 'truncated'`, ruleID); n != 1 {
		t.Fatalf("detail.queries[0].partial must be stored as a JSON true under the key \"partial\"")
	}
	// 同一条序列再评估：删减是确定的，指纹不变，不会新建事件。
	g.evalDue()
	if got := e.oneAlert(ruleID); got.ID != a.ID {
		t.Fatalf("label limiting changed the fingerprint between rounds")
	}
}
