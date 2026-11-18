//go:build integration

package data

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/loop"
	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/monitor/internal/biz"
	"github.com/lanyulei/reborn/backend/monitor/internal/promtest"

	"github.com/jackc/pgx/v5/pgconn"
)

// 第一轮评审（docs/monitor-review-round1.md，E 项）的回归测试：凡是要靠真实 PostgreSQL 才能证明的行为。

// outboxRow 发件箱一行的完整状态。
type outboxRow struct {
	ID        string
	Status    int
	Attempts  int
	LastError string
	Done      *time.Time
}

func (e *env) outboxRow(alertID string, kind, seq int32) outboxRow {
	e.t.Helper()
	var r outboxRow
	if err := e.admin.QueryRow(context.Background(),
		"SELECT id::text, status, attempts, last_error, done_time FROM monitor_notify_outbox WHERE alert_id = $1 AND kind = $2 AND seq = $3",
		alertID, kind, seq).Scan(&r.ID, &r.Status, &r.Attempts, &r.LastError, &r.Done); err != nil {
		e.t.Fatalf("read outbox row %d/%d: %v", kind, seq, err)
	}
	return r
}

// drop 模拟 core 把一条发件箱行丢弃（派发失败次数用尽，或派发时事件被屏蔽）。
func (e *env) drop(id string) {
	e.t.Helper()
	e.exec("UPDATE monitor_notify_outbox SET status = $2, attempts = $3, last_error = 'dispatch failed', done_time = now() WHERE id = $1",
		id, monitorcontract.OutboxDropped, monitorcontract.OutboxMaxAttempts)
}

// E6：重复通知行被 core 丢弃后，下一次到期时引擎写出同一个键 (alert, 2, seq)——真实执行
// ON CONFLICT … DO UPDATE … WHERE status = 已丢弃 分支，把那一行重新置为待派发。
func TestDroppedRepeatNotificationIsRearmed(t *testing.T) {
	e := newEnv(t)
	prom := promtest.New(t)
	e.datasource(prom.URL)
	ruleID := e.rule(ruleSpec{RepeatM: 5})
	prom.Set(upDown, promtest.S(0, "instance", "a"))
	g := e.engine("")
	g.materialize()
	g.evalDue()
	a := e.oneAlert(ruleID)
	e.dispatch()
	e.age(6 * time.Minute)
	g.evalDue()
	if got := e.outbox(a.ID); !equalStrings(got, []string{"1/1/2", "2/2/1"}) {
		t.Fatalf("outbox = %v", got)
	}
	repeat := e.outboxRow(a.ID, monitorcontract.NotifyKindRepeat, 2)
	e.drop(repeat.ID)

	// 通知计数没有前进（仍是 1）：到期后写出的还是 2/2，命中唯一键冲突。
	g.evalDue()
	got := e.outboxRow(a.ID, monitorcontract.NotifyKindRepeat, 2)
	if got.ID != repeat.ID || got.Status != int(monitorcontract.OutboxPending) || got.Attempts != 0 || got.LastError != "" || got.Done != nil {
		t.Fatalf("the dropped repeat row must be re-armed in place: %+v", got)
	}
	if e.count("SELECT count(*) FROM monitor_notify_outbox WHERE alert_id = $1", a.ID) != 2 {
		t.Fatalf("re-arming must not add rows: %v", e.outbox(a.ID))
	}
	// 待派发期间再评估：冲突分支的 WHERE 不成立，行原样不动。
	e.exec("UPDATE monitor_notify_outbox SET attempts = 3 WHERE id = $1", repeat.ID)
	g.evalDue()
	if got = e.outboxRow(a.ID, monitorcontract.NotifyKindRepeat, 2); got.Status != int(monitorcontract.OutboxPending) || got.Attempts != 3 {
		t.Fatalf("a pending row must be left alone: %+v", got)
	}
	// 派发后重复通知照常继续（seq 3），已派发的行永远不会被重新激活。
	e.dispatch()
	if cur := e.oneAlert(ruleID); cur.NotifyCount != 2 {
		t.Fatalf("notify count = %d", cur.NotifyCount)
	}
	e.age(6 * time.Minute)
	g.evalDue()
	if got := e.outbox(a.ID); !equalStrings(got, []string{"1/1/2", "2/2/2", "2/3/1"}) {
		t.Fatalf("outbox = %v", got)
	}
}

// 重新激活只作用于已丢弃的行：直接以 Rearm 写一条键相同的行，已派发的不变、已丢弃的变回待派发。
func TestRearmOnlyRevivesDroppedRows(t *testing.T) {
	e := newEnv(t)
	prom := promtest.New(t)
	ds := e.datasource(prom.URL)
	ruleID := e.rule(ruleSpec{})
	prom.Set(upDown, promtest.S(0, "instance", "a"))
	g := e.engine("")
	g.materialize()
	g.evalDue()
	a := e.oneAlert(ruleID)
	first := e.outboxRow(a.ID, monitorcontract.NotifyKindFiring, 1)
	rounds, tasks := NewRoundRepo(e.d), NewTaskRepo(e.d)
	rearm := func() {
		t.Helper()
		task := claimAfter(t, e, tasks, "rearm-"+e.id()[30:])
		if task.DatasourceID != ds {
			t.Fatalf("task = %+v", task)
		}
		err := rounds.Commit(context.Background(), task, false, func([]biz.SeriesState) (*biz.Round, error) {
			return &biz.Round{
				Now: time.Now(), Interval: 15 * time.Second, Status: monitorcontract.EvalFiring,
				Transition: &biz.Transition{FiringCount: 1, Outbox: []biz.OutboxInsert{
					{ID: e.id(), AlertID: a.ID, Kind: monitorcontract.NotifyKindFiring, Seq: 1, Rearm: true},
				}},
			}, nil
		})
		if err != nil {
			t.Fatalf("Commit() error = %v", err)
		}
	}
	e.dispatch() // 已派发
	rearm()
	if got := e.outboxRow(a.ID, monitorcontract.NotifyKindFiring, 1); got.ID != first.ID || got.Status != int(monitorcontract.OutboxDone) || got.Done == nil {
		t.Fatalf("a dispatched row must never be re-armed: %+v", got)
	}
	e.drop(first.ID)
	rearm()
	if got := e.outboxRow(a.ID, monitorcontract.NotifyKindFiring, 1); got.ID != first.ID || got.Status != int(monitorcontract.OutboxPending) || got.Attempts != 0 || got.Done != nil {
		t.Fatalf("a dropped row must be re-armed: %+v", got)
	}
}

// E5：屏蔽解除的那一轮与 core 的派发并发。core 的派发事务锁着那条待派发的行、正按「事件被屏蔽」把它
// 丢弃；引擎的重新激活 upsert 必须等它提交，然后把这一行重新置为待派发——通知不丢。
func TestUnmuteRearmWaitsForTheDispatcher(t *testing.T) {
	e := newEnv(t)
	prom := promtest.New(t)
	e.datasource(prom.URL)
	ruleID := e.rule(ruleSpec{})
	prom.Set(upDown, promtest.S(0, "instance", "a"))
	e.engine("").materialize()
	e.engine("").evalDue()
	a := e.oneAlert(ruleID)
	first := e.outboxRow(a.ID, monitorcontract.NotifyKindFiring, 1)

	// 首次通知还没派发，屏蔽规则生效：事件标记为已屏蔽，那一行仍是待派发。
	muteID := e.id()
	e.exec(`INSERT INTO monitor_mute (id, name, reason, mode, time_type, start_time, end_time)
		VALUES ($1, 'm', 'maintenance', 1, 1, now() - interval '1 hour', now() + interval '1 hour')`, muteID)
	e.exec("UPDATE monitor_cfg_version SET version = version + 1 WHERE name = $1", monitorcontract.CfgMute)
	e.engine("").evalDue()
	if cur := e.oneAlert(ruleID); !cur.Muted || !equalStrings(e.outbox(a.ID), []string{"1/1/1"}) {
		t.Fatalf("precondition: muted alert with a pending row, got muted = %v, outbox = %v", cur.Muted, e.outbox(a.ID))
	}

	// 屏蔽解除。与此同时 core 的派发事务已经领到那一行（行锁），它读到的事件还是「已屏蔽」，决定丢弃。
	e.exec("UPDATE monitor_mute SET delete_time = now() WHERE id = $1", muteID)
	e.exec("UPDATE monitor_cfg_version SET version = version + 1 WHERE name = $1", monitorcontract.CfgMute)
	ctx := context.Background()
	core, err := e.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = core.Rollback(ctx) }()
	var locked string
	if err := core.QueryRow(ctx, "SELECT id::text FROM monitor_notify_outbox WHERE id = $1 FOR UPDATE SKIP LOCKED", first.ID).Scan(&locked); err != nil {
		t.Fatalf("core could not lock the outbox row: %v", err)
	}
	if _, err := core.Exec(ctx, "UPDATE monitor_notify_outbox SET status = $2, last_error = 'alert muted', done_time = now() WHERE id = $1",
		first.ID, monitorcontract.OutboxDropped); err != nil {
		t.Fatal(err)
	}

	g := e.engine("")
	e.due()
	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	go func() {
		n, err := g.evaluator.RunOnce(ctx, g.token)
		done <- result{n, err}
	}()
	select {
	case r := <-done:
		t.Fatalf("the engine did not wait for the dispatcher's row lock (n = %d, err = %v): the upsert was skipped", r.n, r.err)
	case <-time.After(700 * time.Millisecond):
	}
	if err := core.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-done:
		if r.err != nil || r.n != 1 {
			t.Fatalf("RunOnce() = %d, %v", r.n, r.err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the engine round did not finish after the dispatcher committed")
	}
	if g.obs.failed.Load() != 0 {
		t.Fatalf("the round failed")
	}
	got := e.outboxRow(a.ID, monitorcontract.NotifyKindFiring, 1)
	if got.ID != first.ID || got.Status != int(monitorcontract.OutboxPending) || got.LastError != "" || got.Done != nil {
		t.Fatalf("the notification was lost: %+v", got)
	}
	if cur := e.oneAlert(ruleID); cur.Muted || cur.MuteID != nil {
		t.Fatalf("alert must be unmuted: %+v", cur)
	}
	// 之后正常派发；下一轮不再重复写。
	e.dispatch()
	e.engine("").evalDue()
	if cur := e.oneAlert(ruleID); cur.NotifyCount != 1 || !equalStrings(e.outbox(a.ID), []string{"1/1/2"}) {
		t.Fatalf("notify count = %d, outbox = %v", cur.NotifyCount, e.outbox(a.ID))
	}
}

// E2 / E8：2000 序列截断。被挤出结果的序列既不恢复也不删除状态；截断保留哪些序列只取决于标签，
// 与上游返回的顺序无关（每轮顺序都打乱）。
func TestTruncatedResultNeverResolvesOrDeletesState(t *testing.T) {
	e := newEnv(t)
	prom := promtest.New(t)
	ds := e.datasource(prom.URL)
	ruleID := e.rule(ruleSpec{RecoveryExpr: `up{instance="{{ $labels.instance }}"}`})
	pendingRule := e.rule(ruleSpec{ForS: 3600, Queries: []monitorcontract.Query{{Expr: "slow == 1", Severity: monitorcontract.SeverityInfo}}})
	g := e.engine("")
	g.materialize()
	const limit = 2000
	series := func(prefix string, n int) []promtest.Sample {
		out := make([]promtest.Sample, n)
		for i := range out {
			out[i] = promtest.S(1, "instance", fmt.Sprintf("%s-%04d", prefix, i), "job", "node")
		}
		return out
	}
	// 每次调用换一种确定的排列（不需要随机数）：按与长度互素的步长跳着取。
	calls := 0
	shuffled := func(parts ...[]promtest.Sample) []promtest.Sample {
		var all []promtest.Sample
		for _, p := range parts {
			all = append(all, p...)
		}
		calls++
		step := []int{7, 11, 37, 17, 19, 23, 29, 31}[calls%8] // 均与 1500、2600 互素
		out := make([]promtest.Sample, len(all))
		for i := range all {
			out[i] = all[(i*step+calls)%len(all)]
		}
		return out
	}
	count := func(sql string, args ...any) int { t.Helper(); return e.count(sql, args...) }

	// 第 1 轮（完整）：1500 条触发；另一条规则的 1500 条停在待触发。
	old := series("m", 1500)
	prom.Set(upDown, shuffled(old)...)
	prom.Set("slow == 1", shuffled(old)...)
	g.evalDue()
	e.dispatch()
	if count("SELECT count(*) FROM monitor_alert WHERE rule_id = $1 AND status = 1", ruleID) != 1500 ||
		count("SELECT count(*) FROM monitor_eval_state WHERE rule_id = $1 AND state = 1", pendingRule) != 1500 {
		t.Fatalf("precondition failed")
	}
	if got := e.task(ruleID, ds); got.Error != "" {
		t.Fatalf("a complete round must not be annotated: %+v", got)
	}

	// 第 2-4 轮：又冒出 1100 条标签排序更靠前的序列（共 2600 > 2000）——原来的 1500 条里有 600 条被挤出结果。
	fresh := series("a", 1100)
	for i := range 3 {
		prom.Set(upDown, shuffled(old, fresh)...)
		prom.Set("slow == 1", shuffled(old, fresh)...)
		g.evalDue()
		tag := fmt.Sprintf("truncated round %d", i+1)
		if n := count("SELECT count(*) FROM monitor_alert WHERE rule_id = $1 AND status <> 1", ruleID); n != 0 {
			t.Fatalf("%s: %d alerts were resolved by a truncated result", tag, n)
		}
		if n := count("SELECT count(*) FROM monitor_notify_outbox WHERE kind = 3"); n != 0 {
			t.Fatalf("%s: %d recovery notifications were queued", tag, n)
		}
		// 确定性：每轮顺序都不同，留下的恒是 a-0000…a-1099 + m-0000…m-0899，所以活跃告警恒为 2600。
		if n := count("SELECT count(*) FROM monitor_alert WHERE rule_id = $1 AND status = 1", ruleID); n != 2600 {
			t.Fatalf("%s: active alerts = %d, want 2600 (1500 old + the same 1100 new every round)", tag, n)
		}
		if n := count("SELECT count(*) FROM monitor_eval_state WHERE rule_id = $1 AND state = 2", ruleID); n != 2600 {
			t.Fatalf("%s: firing states = %d (unseen series must stay firing, not enter recovery)", tag, n)
		}
		if n := count(`SELECT count(*) FROM monitor_eval_state WHERE rule_id = $1 AND labels->>'instance' >= 'm-0900'`, ruleID); n != 600 {
			t.Fatalf("%s: states of the series pushed out of the result = %d, want 600", tag, n)
		}
		// 待触发的同理：被挤出结果的 600 条不删除。
		if n := count("SELECT count(*) FROM monitor_eval_state WHERE rule_id = $1 AND state = 1", pendingRule); n != 2600 {
			t.Fatalf("%s: pending states = %d, want 2600 (unseen pending series must not be deleted)", tag, n)
		}
		rec, task := e.lastRecord(ruleID), e.task(ruleID, ds)
		if !rec.Detail.Queries[0].Truncated || rec.Series != limit || rec.Recovered != 0 || rec.Status != int(monitorcontract.EvalFiring) ||
			!strings.HasPrefix(rec.Error, "incomplete result: query 0 truncated to 2000 series") || task.Error != rec.Error ||
			task.Status != int(monitorcontract.EvalFiring) || task.FiringCount != 2600 {
			t.Fatalf("%s: record = %+v, task = %+v", tag, rec, task)
		}
	}
	if prom.Hits(`up{instance="m-0900"}`) != 0 {
		t.Fatalf("recovery value queries must not run on a truncated round")
	}

	// 结果重新完整，且只剩 10 条：其余 2590 条这时才恢复；待触发的 2590 条这时才删除。
	prom.Set(upDown, old[:10]...)
	prom.Set("slow == 1", old[:10]...)
	g.evalDue()
	if count("SELECT count(*) FROM monitor_alert WHERE rule_id = $1 AND status = 2", ruleID) != 2590 ||
		count("SELECT count(*) FROM monitor_alert WHERE rule_id = $1 AND status = 1", ruleID) != 10 ||
		count("SELECT count(*) FROM monitor_eval_state WHERE rule_id = $1", pendingRule) != 10 {
		t.Fatalf("a complete round must resolve what is really gone")
	}
	if got := e.task(ruleID, ds); got.Error != "" || got.FiringCount != 10 {
		t.Fatalf("task after a complete round = %+v", got)
	}
}

// E3：部分结果（VictoriaMetrics isPartial）与截断同样处理。
func TestPartialResultNeverResolves(t *testing.T) {
	e := newEnv(t)
	prom := promtest.New(t)
	ds := e.datasource(prom.URL)
	ruleID := e.rule(ruleSpec{})
	g := e.engine("")
	g.materialize()
	prom.Set(upDown, promtest.S(0, "instance", "a"), promtest.S(0, "instance", "b"))
	g.evalDue()
	e.dispatch()
	// 一个存储节点不可用：只返回了 a，并声明结果不完整。
	prom.Set(upDown, promtest.S(0, "instance", "a"))
	prom.SetPartial(true)
	g.evalDue()
	g.evalDue()
	if e.count("SELECT count(*) FROM monitor_alert WHERE rule_id = $1 AND status = 1", ruleID) != 2 ||
		e.count("SELECT count(*) FROM monitor_eval_state WHERE state = 2") != 2 || e.count("SELECT count(*) FROM monitor_notify_outbox WHERE kind = 3") != 0 {
		t.Fatalf("a partial result resolved or moved an unseen series")
	}
	rec := e.lastRecord(ruleID)
	if !rec.Detail.Queries[0].Partial || rec.Error != "incomplete result: query 0 returned a partial result; series not returned were left unchanged" ||
		e.task(ruleID, ds).Error != rec.Error {
		t.Fatalf("record = %+v", rec)
	}
	prom.SetPartial(false)
	g.evalDue()
	if e.count("SELECT count(*) FROM monitor_alert WHERE rule_id = $1 AND status = 2", ruleID) != 1 {
		t.Fatalf("a complete result must resolve the series that is really gone")
	}
}

// E9：待触发 / 留观计时跨过查询错误的轮次后从头起算（落库的 active_since / recover_since 被重置）。
func TestTimersRestartAfterUnevaluatedRounds(t *testing.T) {
	e := newEnv(t)
	prom := promtest.New(t)
	e.datasource(prom.URL)
	ruleID := e.rule(ruleSpec{ForS: 60, RecoverS: 120})
	g := e.engine("")
	g.materialize()
	up := func() { prom.Set(upDown, promtest.S(0, "instance", "a")) }
	sinceAgo := func(column string) float64 {
		t.Helper()
		var ago float64
		if err := e.admin.QueryRow(context.Background(),
			"SELECT extract(epoch FROM now() - "+column+")::float8 FROM monitor_eval_state WHERE rule_id = $1", ruleID).Scan(&ago); err != nil {
			t.Fatal(err)
		}
		return ago
	}
	up()
	g.evalDue()
	// 数据源故障一轮；其间过去了 61s（超过 for_s）。
	prom.SetError(upDown, 503, "unavailable", "down")
	e.age(30 * time.Second)
	g.evalDue()
	e.age(31 * time.Second)
	up()
	g.evalDue()
	if len(e.alerts(ruleID)) != 0 {
		t.Fatalf("for_s was bypassed: the outage was counted as continuously abnormal")
	}
	if ago := sinceAgo("active_since"); ago > 5 {
		t.Fatalf("active_since must restart at the first observed round after the outage, is %.0fs old", ago)
	}
	// 连续观察满 for_s 才触发。
	e.age(61 * time.Second)
	g.evalDue()
	a := e.oneAlert(ruleID)
	e.dispatch()

	// 进入留观后数据源故障；其间过去了 130s（超过 recover_s）。
	prom.Set(upDown)
	g.evalDue()
	prom.SetError(upDown, 503, "unavailable", "down")
	e.age(65 * time.Second)
	g.evalDue()
	e.age(65 * time.Second)
	prom.Set(upDown)
	g.evalDue()
	if cur := e.oneAlert(ruleID); cur.Status != 1 || cur.ID != a.ID {
		t.Fatalf("the outage was counted as observed-healthy time: %+v", cur)
	}
	if ago := sinceAgo("recover_since"); ago > 5 {
		t.Fatalf("recover_since must restart after the outage, is %.0fs old", ago)
	}
	e.age(121 * time.Second)
	g.evalDue()
	if cur := e.oneAlert(ruleID); cur.Status != 2 {
		t.Fatalf("alert must resolve after a full observed recover_s: %+v", cur)
	}
}

// E7 / E10：新任务的首次评估时刻带抖动；规则被修改后，物化把它的任务提前到立即评估（只提前一次）。
func TestMaterializeJittersNewTasksAndHastensUpdatedRules(t *testing.T) {
	e := newEnv(t)
	prom := promtest.New(t)
	ds := e.datasource(prom.URL)
	for range 60 {
		e.rule(ruleSpec{})
	}
	slow := e.rule(ruleSpec{})
	e.exec("UPDATE monitor_rule SET eval_interval_s = 3600 WHERE id = $1", slow)
	g := e.engine("")
	g.materialize()
	var lo, hi float64
	var distinct int
	if err := e.admin.QueryRow(context.Background(),
		`SELECT min(d), max(d), count(DISTINCT d) FROM (
		   SELECT extract(epoch FROM t.next_eval_time - t.create_time)::float8 AS d FROM monitor_eval_task t WHERE t.rule_id <> $1) x`, slow).
		Scan(&lo, &hi, &distinct); err != nil {
		t.Fatal(err)
	}
	if lo < 0 || hi > 15 || hi-lo < 5 || distinct < 50 {
		t.Fatalf("first evaluations must be spread over the 15s interval: min = %.2f, max = %.2f, distinct = %d", lo, hi, distinct)
	}
	// 长周期规则的抖动封顶 30s（不让新规则等太久才有第一次结果）。
	if in := e.task(slow, ds).NextIn; in < 0 || in > 30.5 {
		t.Fatalf("first evaluation of an hourly rule is %.1fs away, want within 30s", in)
	}

	// 评估一轮后，长周期规则的下一次评估在一小时之后。
	g.evalDue()
	if in := e.task(slow, ds).NextIn; in < 3590 {
		t.Fatalf("next evaluation in %.0fs", in)
	}
	repo := NewPlannerRepo(e.d)
	materialize := func() biz.MaterializeResult {
		t.Helper()
		res, err := repo.Materialize(context.Background(), 500)
		if err != nil {
			t.Fatal(err)
		}
		return res
	}
	if res := materialize(); res.Hastened != 0 {
		t.Fatalf("nothing was modified, hastened = %d", res.Hastened)
	}
	// 规则被修改（core 写 update_time）：物化把任务提前到现在，不必等旧的 next_eval_time。
	e.exec("UPDATE monitor_rule SET for_s = 30, update_time = now(), version = version + 1 WHERE id = $1", slow)
	if res := materialize(); res.Hastened != 1 {
		t.Fatalf("hastened = %d, want 1", res.Hastened)
	}
	if in := e.task(slow, ds).NextIn; in > 0.5 {
		t.Fatalf("the modified rule is still %.0fs away from its next evaluation", in)
	}
	// 领取并评估（不经 evalDue：验证任务确实已到期）。
	if n, err := g.evaluator.RunOnce(context.Background(), g.token); err != nil || n != 1 {
		t.Fatalf("RunOnce() = %d, %v", n, err)
	}
	// 只提前一次：评估过后回到正常周期。
	if res := materialize(); res.Hastened != 0 || e.task(slow, ds).NextIn < 3590 {
		t.Fatalf("hastened = %d, next in %.0fs", res.Hastened, e.task(slow, ds).NextIn)
	}
	// 已停用 / 已删除的规则不提前（它们的任务由清理分支删除）。
	e.exec("UPDATE monitor_rule SET enabled = false, update_time = now() WHERE id = $1", slow)
	if res := materialize(); res.Hastened != 0 || res.Removed != 1 {
		t.Fatalf("res = %+v", res)
	}
}

// E7 / E17：两个评估器实例（各自独立的进程内状态与属主令牌，真实的轮询循环）稳态下都有产出，
// 没有任务被重复评估，也没有任务被饿死。
//
// 复现联调中的现象所需的条件：任务数少（一个实例一次轮询就能全部领走），两个实例的轮询节拍错开
// （这里是半个周期）。实例 a 评估完一个任务后，该任务的下一次到期时刻 = 那一刻 + 评估周期，恰好落在
// a 自己的轮询节拍之前几十毫秒——没有每轮抖动时，之后每一轮都是 a 先轮询到它，b 永远领不到。
// 每轮的下一次评估时刻加上抖动后，到期时刻在一个轮询周期内散开，谁的节拍先到谁领。
func TestTwoEvaluatorInstancesShareTheWork(t *testing.T) {
	if testing.Short() {
		t.Skip("runs for about three evaluation intervals")
	}
	e := newEnv(t)
	prom := promtest.New(t)
	e.datasource(prom.URL)
	const rules = 12
	for range rules {
		e.rule(ruleSpec{ForS: 3600}) // 15s 周期；只评估，不触发
	}
	prom.Set(upDown, promtest.S(0, "instance", "a"))
	a, b := e.engine(""), e.engine("")
	a.materialize()
	e.due()

	// 与生产一致：每个实例若干条循环、1s 轮询。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var loops []*loop.Loop
	start := func(g *engine, name string) {
		for i := range 2 {
			token := fmt.Sprintf("%s-%d", name, i)
			l := loop.New(token, time.Second, testLogger(), nil, func(ctx context.Context) (int, error) { return g.evaluator.RunOnce(ctx, token) })
			loops = append(loops, l)
			go func() { _ = l.Start(ctx) }()
		}
	}
	stop := func() {
		cancel()
		for _, l := range loops {
			_ = l.Stop(context.Background())
		}
	}
	defer stop()
	start(a, "instance-a")
	// 第一轮：实例 a 独自评估全部任务。
	wait := time.Now().Add(10 * time.Second)
	for a.obs.evals.Load() < rules && time.Now().Before(wait) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := a.obs.evals.Load(); got != rules {
		t.Fatalf("instance a evaluated %d of %d tasks in the first round", got, rules)
	}
	// 实例 b 的轮询节拍比 a 晚半个周期。
	time.Sleep(500 * time.Millisecond)
	start(b, "instance-b")

	// 再跑三个评估周期（每轮至多 15s + 1s 抖动 + 等下一个节拍）：每个任务再被评估 3 次，第 4 次不早于 60s。
	time.Sleep(53 * time.Second)
	stop()
	na, nb := a.obs.evals.Load()-rules, b.obs.evals.Load()
	total := na + nb
	t.Logf("evaluations after the first round: instance a = %d, instance b = %d", na, nb)
	if a.obs.failed.Load()+b.obs.failed.Load() != 0 {
		t.Fatalf("failed or fenced rounds: a = %d, b = %d", a.obs.failed.Load(), b.obs.failed.Load())
	}
	if total != 3*rules {
		t.Fatalf("total evaluations = %d, want %d (every task exactly once per interval)", total, 3*rules)
	}
	if min(na, nb)*5 < total {
		t.Fatalf("one instance did (almost) all the work: a = %d, b = %d", na, nb)
	}
	// 判定记录数与评估数一致（没有重复写回）；每个任务都被评估了 4 次。
	if n := e.count("SELECT count(*) FROM monitor_eval_record"); int64(n) != total+rules {
		t.Fatalf("eval records = %d, evaluations = %d", n, total+rules)
	}
	if n := e.count("SELECT count(*) FROM (SELECT rule_id FROM monitor_eval_record GROUP BY rule_id HAVING count(*) <> 4) x"); n != 0 {
		t.Fatalf("%d rules were not evaluated exactly 4 times", n)
	}
}

// E1：带攻击模板的规则照常触发，事务不被卡住；该项附加信息落成 <template error>。
func TestMaliciousAnnotationTemplateCannotStallARound(t *testing.T) {
	e := newEnv(t)
	prom := promtest.New(t)
	ds := e.datasource(prom.URL)
	ruleID := e.rule(ruleSpec{Annotations: map[string]string{
		"assign":    `{{$labels = 9000000000000000000}}{{range $labels}}{{end}}`,
		"redeclare": `{{$labels := 9000000000000000000}}{{range $labels}}{{end}}`,
		"summary":   "{{ $labels.instance }} is down",
	}})
	prom.Set(upDown, promtest.S(0, "instance", "a"))
	g := e.engine("")
	g.materialize()
	e.due()
	done := make(chan error, 1)
	go func() {
		_, err := g.evaluator.RunOnce(context.Background(), g.token)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the evaluation round is stuck rendering a template")
	}
	a := e.oneAlert(ruleID)
	if a.Annotations["assign"] != "<template error>" || a.Annotations["redeclare"] != "<template error>" || a.Annotations["summary"] != "a is down" {
		t.Fatalf("annotations = %v", a.Annotations)
	}
	if got := e.task(ruleID, ds); got.Owner != nil || got.Status != int(monitorcontract.EvalFiring) {
		t.Fatalf("task = %+v", got)
	}
	// 没有任何会话卡在事务里。
	if n := e.count("SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND state = 'idle in transaction'"); n != 0 {
		t.Fatalf("%d sessions are idle in transaction", n)
	}
}

// E11：标量结果不触发任何事件，记为查询错误。
func TestScalarResultIsAQueryError(t *testing.T) {
	e := newEnv(t)
	prom := promtest.New(t)
	ds := e.datasource(prom.URL)
	ruleID := e.rule(ruleSpec{Queries: []monitorcontract.Query{{Expr: "time()", Severity: monitorcontract.SeverityWarning}}})
	prom.SetScalar("time()", 1.7e9)
	g := e.engine("")
	g.materialize()
	g.evalDue()
	if len(e.alerts(ruleID)) != 0 || e.count("SELECT count(*) FROM monitor_eval_state") != 0 {
		t.Fatalf("a scalar result fired an alert")
	}
	if got := e.task(ruleID, ds); got.Status != int(monitorcontract.EvalError) || got.Error != "query 0: query must return an instant vector, got a scalar" {
		t.Fatalf("task = %+v", got)
	}
}

// 引擎登录账号是一个真实的、受限的 LOGIN 角色：不是超级用户，自身没有任何授权，权限全部来自
// monitor_engine_role 的成员关系。全部集成测试都以它登录运行。
func TestEngineRunsAsARestrictedLoginRole(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	var (
		session            string
		login, super, mine bool
		direct             int
	)
	if err := e.d.Pool.QueryRow(ctx,
		`SELECT session_user, r.rolcanlogin, r.rolsuper OR r.rolcreaterole OR r.rolcreatedb OR r.rolbypassrls OR r.rolreplication,
		        pg_has_role(session_user, $1, 'MEMBER'),
		        (SELECT count(*) FROM information_schema.role_table_grants g WHERE g.grantee = session_user)
		 FROM pg_roles r WHERE r.rolname = session_user`, engineRole).Scan(&session, &login, &super, &mine, &direct); err != nil {
		t.Fatal(err)
	}
	if session != loginRole || !login || super || !mine || direct != 0 {
		t.Fatalf("session = %s, login = %v, privileged = %v, member = %v, direct grants = %d", session, login, super, mine, direct)
	}
	if n := e.count("SELECT count(*) FROM pg_auth_members m JOIN pg_roles r ON r.oid = m.member WHERE r.rolname = $1", loginRole); n != 1 {
		t.Fatalf("the login role is a member of %d roles, want only %s", n, engineRole)
	}
	// 以它完成一整套工作：物化、评估、探活、分区维护、清理。
	prom := promtest.New(t)
	ds := e.datasource(prom.URL)
	ruleID := e.rule(ruleSpec{})
	prom.Set(upDown, promtest.S(0, "instance", "a"))
	g := e.engine("")
	g.materialize()
	if g.evalDue() != 1 || e.oneAlert(ruleID).Status != 1 {
		t.Fatalf("evaluation as the login role failed")
	}
	if _, err := g.planner.Probe(ctx); err != nil || e.count("SELECT count(*) FROM monitor_datasource WHERE id = $1 AND health_status = 1", ds) != 1 {
		t.Fatalf("probe as the login role failed: %v", err)
	}
	// 它不能脱离成员关系给自己加权限。
	for name, stmt := range map[string]string{
		"become superuser":   "ALTER ROLE " + loginRole + " SUPERUSER",
		"create a role":      "CREATE ROLE monitor_engine_it_evil",
		"set role to others": "SET ROLE postgres",
	} {
		_, err := e.d.Pool.Exec(ctx, stmt)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || (pgErr.Code != "42501" && pgErr.Code != "0LP01") {
			t.Errorf("%s: want a permission error, got %v", name, err)
		}
	}
}
