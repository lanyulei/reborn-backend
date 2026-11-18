//go:build integration

package data

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/kit/secretbox"
	"github.com/lanyulei/reborn/backend/monitor/internal/biz"
	"github.com/lanyulei/reborn/backend/monitor/internal/promtest"

	"github.com/jackc/pgx/v5/pgconn"
)

const upDown = "up == 0"

// seedAlert 直接插入一条告警及其判定状态（status = 1 时），返回告警 ID。
func (e *env) seedAlert(ruleID, datasourceID, fingerprint string, status int32, resolvedAgo time.Duration) string {
	e.t.Helper()
	id := e.id()
	e.exec(`INSERT INTO monitor_alert (id, fingerprint, rule_id, rule_name, group_id, datasource_id, datasource_name, severity, status,
		  first_fire_time, last_fire_time, resolve_time)
		VALUES ($1, $2, $3, 'r', $4, $5, 'd', 2, $6::smallint, now() - interval '400 days', now() - interval '400 days',
		  CASE WHEN $6::smallint = 1 THEN NULL ELSE now() - $7::interval END)`,
		id, fingerprint, ruleID, monitorcontract.RootGroupID, datasourceID, status, resolvedAgo)
	if status == monitorcontract.AlertFiring {
		e.exec(`INSERT INTO monitor_eval_state (rule_id, datasource_id, fingerprint, state, severity, active_since, last_seen, alert_id)
			VALUES ($1, $2, $3, 2, 2, now(), now(), $4)`, ruleID, datasourceID, fingerprint, id)
	}
	return id
}

func TestMaterialize(t *testing.T) {
	e := newEnv(t)
	g := e.engine("")
	ds1, ds2, ds3 := e.datasource("http://127.0.0.1:1"), e.datasource("http://127.0.0.1:2"), e.datasource("http://127.0.0.1:3")
	e.exec("UPDATE monitor_datasource SET enabled = false WHERE id = $1", ds3)
	ruleAll := e.rule(ruleSpec{})
	ruleOne := e.rule(ruleSpec{DatasourceIDs: []string{ds1, ds3}})
	e.rule(ruleSpec{Disabled: true})
	tasks := func() int { return e.count("SELECT count(*) FROM monitor_eval_task") }
	has := func(ruleID, datasourceID string) bool {
		return e.count("SELECT count(*) FROM monitor_eval_task WHERE rule_id = $1 AND datasource_id = $2", ruleID, datasourceID) == 1
	}

	g.materialize()
	if tasks() != 3 || !has(ruleAll, ds1) || !has(ruleAll, ds2) || !has(ruleOne, ds1) {
		t.Fatalf("tasks = %d, want ruleAll×{ds1,ds2} + ruleOne×ds1", tasks())
	}
	// 幂等：再跑不新增，也不重置已有任务。
	e.exec("UPDATE monitor_eval_task SET last_status = 2, firing_count = 7")
	g.materialize()
	if tasks() != 3 || e.count("SELECT count(*) FROM monitor_eval_task WHERE last_status = 2 AND firing_count = 7") != 3 {
		t.Fatalf("materialize must be idempotent")
	}

	// 数据源停用：任务、判定状态一并删除，活跃告警关闭（不发恢复通知）；其他任务与历史告警不动。
	closing := e.seedAlert(ruleAll, ds2, "fp-closing", monitorcontract.AlertFiring, 0)
	history := e.seedAlert(ruleAll, ds2, "fp-history", monitorcontract.AlertResolved, time.Hour)
	keep := e.seedAlert(ruleOne, ds1, "fp-keep", monitorcontract.AlertFiring, 0)
	e.exec("UPDATE monitor_datasource SET enabled = false WHERE id = $1", ds2)
	g.materialize()
	if has(ruleAll, ds2) || tasks() != 2 {
		t.Fatalf("task of the disabled datasource must be removed")
	}
	if e.count("SELECT count(*) FROM monitor_eval_state WHERE rule_id = $1 AND datasource_id = $2", ruleAll, ds2) != 0 {
		t.Fatalf("eval states of the removed task must be deleted")
	}
	if e.count("SELECT count(*) FROM monitor_alert WHERE id = $1 AND status = 3 AND resolve_time > now() - interval '1 minute'", closing) != 1 {
		t.Fatalf("active alert of the removed task must be closed")
	}
	if e.count("SELECT count(*) FROM monitor_alert WHERE id = $1 AND status = 2 AND resolve_time < now() - interval '30 minutes'", history) != 1 {
		t.Fatalf("non-active alerts must not be touched")
	}
	if e.count("SELECT count(*) FROM monitor_alert WHERE id = $1 AND status = 1", keep) != 1 ||
		e.count("SELECT count(*) FROM monitor_eval_state WHERE alert_id = $1", keep) != 1 {
		t.Fatalf("unrelated task must be left alone")
	}
	if e.count("SELECT count(*) FROM monitor_notify_outbox") != 0 {
		t.Fatalf("closing alerts must not notify")
	}

	// 重新启用：scope = all 的规则自动纳入。
	e.exec("UPDATE monitor_datasource SET enabled = true WHERE id = $1", ds2)
	g.materialize()
	if !has(ruleAll, ds2) || tasks() != 3 {
		t.Fatalf("re-enabled datasource must get its task back")
	}
	// 范围变更：ds1 移出、ds2 移入。
	e.exec("UPDATE monitor_rule SET datasource_ids = $2::uuid[] WHERE id = $1", ruleOne, []string{ds2})
	g.materialize()
	if has(ruleOne, ds1) || !has(ruleOne, ds2) || e.count("SELECT count(*) FROM monitor_alert WHERE id = $1 AND status = 3", keep) != 1 {
		t.Fatalf("scope change must move the task and close the alert")
	}
	// 规则停用 / 软删除 / 数据源软删除。
	e.exec("UPDATE monitor_rule SET enabled = false WHERE id = $1", ruleOne)
	g.materialize()
	if has(ruleOne, ds2) || tasks() != 2 {
		t.Fatalf("disabled rule must lose its tasks")
	}
	e.exec("UPDATE monitor_datasource SET delete_time = now() WHERE id = $1", ds1)
	g.materialize()
	if has(ruleAll, ds1) || tasks() != 1 {
		t.Fatalf("deleted datasource must lose its tasks")
	}
	e.exec("UPDATE monitor_rule SET delete_time = now() WHERE id = $1", ruleAll)
	g.materialize()
	if tasks() != 0 {
		t.Fatalf("deleted rule must lose its tasks, left %d", tasks())
	}

	// 清理分批：每轮至多删 limit 个，删满一批时用例返回 > 0 让循环立即再跑。
	for range 3 {
		e.rule(ruleSpec{})
	}
	g.materialize()
	e.exec("UPDATE monitor_rule SET enabled = false")
	res, err := NewPlannerRepo(e.d).Materialize(context.Background(), 2)
	if err != nil || res.Removed != 2 || tasks() != 1 {
		t.Fatalf("res = %+v, err = %v, left = %d", res, err, tasks())
	}
}

func TestClaimIsExclusive(t *testing.T) {
	e := newEnv(t)
	ds := e.datasource("http://127.0.0.1:1")
	const rules = 40
	for range rules {
		e.rule(ruleSpec{})
	}
	e.engine("").materialize()
	e.due()
	repo := NewTaskRepo(e.d)
	ctx := context.Background()

	var (
		mu      sync.Mutex
		claimed = map[string]string{}
		wg      sync.WaitGroup
	)
	for _, token := range []string{"evaluator-a", "evaluator-b", "evaluator-c"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				tasks, err := repo.Claim(ctx, token, 7, time.Minute, 30*time.Second, time.Second)
				if err != nil {
					t.Errorf("Claim() error = %v", err)
					return
				}
				if len(tasks) == 0 {
					return
				}
				mu.Lock()
				for _, task := range tasks {
					if other, dup := claimed[task.RuleID]; dup {
						t.Errorf("task %s claimed by both %s and %s", task.RuleID, other, token)
					}
					if task.Token != token || task.DatasourceID != ds {
						t.Errorf("task = %+v", task)
					}
					claimed[task.RuleID] = token
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(claimed) != rules {
		t.Fatalf("claimed %d tasks, want %d", len(claimed), rules)
	}
	// 租约窗口内不可再领。
	if again, err := repo.Claim(ctx, "evaluator-d", 100, time.Minute, 30*time.Second, time.Second); err != nil || len(again) != 0 {
		t.Fatalf("leased tasks must not be claimable: %d, %v", len(again), err)
	}
	if n := e.count("SELECT count(*) FROM monitor_eval_task WHERE lease_until BETWEEN now() + interval '55 seconds' AND now() + interval '61 seconds'"); n != rules {
		t.Fatalf("lease = max(60s, 3 × 2s + margin) expected on all tasks, got %d", n)
	}
}

func TestClaimLeaseExpiryOrderAndDuration(t *testing.T) {
	e := newEnv(t)
	ds := e.datasource("http://127.0.0.1:1")
	early, late := e.rule(ruleSpec{}), e.rule(ruleSpec{})
	e.engine("").materialize()
	e.exec("UPDATE monitor_eval_task SET next_eval_time = now() - interval '10 seconds' WHERE rule_id = $1", early)
	e.exec("UPDATE monitor_eval_task SET next_eval_time = now() + interval '1 hour' WHERE rule_id = $1", late)
	repo := NewTaskRepo(e.d)
	ctx := context.Background()

	// 只领到期的；Lag = 领取时刻 − 应评估时刻；租约 = max(lease, 3 × min(数据源超时, 上限) + 余量)。
	// 领取同时带回任务行记录的上一轮结果与时刻。
	e.exec("UPDATE monitor_eval_task SET last_status = 4, last_eval_time = now() - interval '1 minute' WHERE rule_id = $1", early)
	tasks, err := repo.Claim(ctx, "a", 10, time.Second, 30*time.Second, time.Second)
	if err != nil || len(tasks) != 1 || tasks[0].RuleID != early {
		t.Fatalf("tasks = %+v, err = %v", tasks, err)
	}
	if lag := tasks[0].Lag; lag < 9*time.Second || lag > 20*time.Second {
		t.Fatalf("lag = %v", lag)
	}
	if got := tasks[0]; got.LastStatus != monitorcontract.EvalError || time.Since(got.LastEval) < 50*time.Second || time.Since(got.LastEval) > 2*time.Minute {
		t.Fatalf("claimed task must carry the previous round: %+v", got)
	}
	if e.count("SELECT count(*) FROM monitor_eval_task WHERE rule_id = $1 AND lease_until BETWEEN now() + interval '6 seconds' AND now() + interval '7100 milliseconds'", early) != 1 {
		t.Fatalf("lease must be 3 × datasource timeout + margin (7s) when that exceeds the configured floor")
	}
	// 数据源超时超过上限：按上限算。
	e.exec("UPDATE monitor_datasource SET timeout_ms = 30000 WHERE id = $1", ds)
	e.exec("UPDATE monitor_eval_task SET lease_owner = NULL, lease_until = NULL")
	if _, err := repo.Claim(ctx, "a", 10, time.Second, 10*time.Second, 0); err != nil {
		t.Fatal(err)
	}
	if e.count("SELECT count(*) FROM monitor_eval_task WHERE rule_id = $1 AND lease_until BETWEEN now() + interval '29 seconds' AND now() + interval '30100 milliseconds'", early) != 1 {
		t.Fatalf("lease must use the clamped query timeout (3 × 10s)")
	}
	// 从未评估过的任务：上一轮时刻为零值。
	e.exec("UPDATE monitor_eval_task SET next_eval_time = now() - interval '1 second' WHERE rule_id = $1", late)
	if never, err := repo.Claim(ctx, "n", 10, time.Second, 10*time.Second, 0); err != nil || len(never) != 1 || never[0].LastStatus != monitorcontract.EvalNever || !never[0].LastEval.IsZero() {
		t.Fatalf("never-evaluated task = %+v, %v", never, err)
	}

	// 租约未过期：他人领不到；过期后可被接管，且原属主的归还不再生效。
	if got, _ := repo.Claim(ctx, "b", 10, time.Minute, 30*time.Second, 0); len(got) != 0 {
		t.Fatalf("live lease was stolen")
	}
	e.exec("UPDATE monitor_eval_task SET lease_until = now() - interval '1 second' WHERE rule_id = $1", early)
	taken, err := repo.Claim(ctx, "b", 10, time.Minute, 30*time.Second, 0)
	if err != nil || len(taken) != 1 || taken[0].Token != "b" {
		t.Fatalf("expired lease must be taken over: %+v, %v", taken, err)
	}
	if err := repo.Release(ctx, "a", tasks, 0); err != nil {
		t.Fatal(err)
	}
	if owner := e.task(early, ds).Owner; owner == nil || *owner != "b" {
		t.Fatalf("stale owner's release must not touch the new lease, owner = %v", owner)
	}
	// 现属主：归还后立刻可领，且 next_eval_time 至少推迟 delay。
	if err := repo.Release(ctx, "b", taken, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	if got := e.task(early, ds); got.Owner != nil || got.NextIn < 25 || got.NextIn > 31 {
		t.Fatalf("released task = %+v", got)
	}
}

func TestFencing(t *testing.T) {
	e := newEnv(t)
	ds := e.datasource("http://127.0.0.1:1")
	ruleID := e.rule(ruleSpec{})
	e.engine("").materialize()
	e.due()
	tasks, rounds := NewTaskRepo(e.d), NewRoundRepo(e.d)
	ctx := context.Background()
	claim := func(token string) *biz.Task {
		t.Helper()
		got, err := tasks.Claim(ctx, token, 1, time.Minute, 30*time.Second, 0)
		if err != nil || len(got) != 1 {
			t.Fatalf("Claim(%s) = %+v, %v", token, got, err)
		}
		return got[0]
	}
	stale := claim("stale")
	e.exec("UPDATE monitor_eval_task SET lease_until = now() - interval '1 second'")
	fresh := claim("fresh")

	now := time.Now()
	firing := func(called *bool) func([]biz.SeriesState) (*biz.Round, error) {
		return func(prev []biz.SeriesState) (*biz.Round, error) {
			*called = true
			alertID, outboxID := e.id(), e.id()
			return &biz.Round{
				Now: now, Interval: 15 * time.Second, Status: monitorcontract.EvalFiring, Exprs: []string{upDown},
				Alert: biz.AlertMeta{RuleName: "r", GroupID: monitorcontract.RootGroupID, DatasourceName: "d"},
				Transition: &biz.Transition{
					FiringCount:  1,
					StateInserts: []biz.StateInsert{{Fingerprint: "fp-fence", State: monitorcontract.StateFiring, Severity: 2, AlertID: alertID}},
					AlertInserts: []biz.AlertInsert{{ID: alertID, Fingerprint: "fp-fence", Severity: 2, Labels: map[string]string{"a": "b"}}},
					Outbox:       []biz.OutboxInsert{{ID: outboxID, AlertID: alertID, Kind: monitorcontract.NotifyKindFiring, Seq: 1}},
				},
			}, nil
		}
	}
	written := func() int {
		return e.count(`SELECT (SELECT count(*) FROM monitor_eval_state) + (SELECT count(*) FROM monitor_alert)
			+ (SELECT count(*) FROM monitor_notify_outbox) + (SELECT count(*) FROM monitor_eval_record)`)
	}

	// 过期属主：状态事务在围栏处被拒，状态机根本不执行。
	var called bool
	if err := rounds.Commit(ctx, stale, false, firing(&called)); !errors.Is(err, biz.ErrLeaseLost) || called {
		t.Fatalf("stale Commit: err = %v, decide called = %v", err, called)
	}
	// 过期属主：只写判定记录的路径同样被拒，且已写的判定记录随事务回滚。
	errRound := &biz.Round{Now: now, Interval: 15 * time.Second, Status: monitorcontract.EvalError, Error: "boom"}
	if err := rounds.Record(ctx, stale, errRound); !errors.Is(err, biz.ErrLeaseLost) {
		t.Fatalf("stale Record: err = %v", err)
	}
	if written() != 0 {
		t.Fatalf("a rejected write-back must leave nothing behind, rows = %d", written())
	}
	if got := e.task(ruleID, ds); got.Owner == nil || *got.Owner != "fresh" || got.Status != 0 {
		t.Fatalf("task must still belong to the new owner untouched: %+v", got)
	}

	// 状态机报错：整轮回滚，租约仍在。
	if err := rounds.Commit(ctx, fresh, false, func([]biz.SeriesState) (*biz.Round, error) { return nil, errors.New("decide failed") }); err == nil {
		t.Fatal("want decide error")
	}
	if written() != 0 || e.task(ruleID, ds).Owner == nil {
		t.Fatalf("failed decide must roll back and keep the lease")
	}

	// 现属主：写入成功，任务写回并释放租约。
	if err := rounds.Commit(ctx, fresh, false, firing(&called)); err != nil || !called {
		t.Fatalf("fresh Commit: %v", err)
	}
	if written() != 4 {
		t.Fatalf("rows = %d, want state + alert + outbox + record", written())
	}
	got := e.task(ruleID, ds)
	if got.Owner != nil || got.Status != int(monitorcontract.EvalFiring) || got.FiringCount != 1 || got.NextIn < 10 || got.NextIn > 16.1 {
		t.Fatalf("task after write-back = %+v", got)
	}
	// 租约已释放：同一令牌的第二次写回被拒（一次领取只能写回一次）。
	if err := rounds.Commit(ctx, fresh, false, firing(&called)); !errors.Is(err, biz.ErrLeaseLost) {
		t.Fatalf("second write-back with a released lease: %v", err)
	}
	// 只写判定记录的一轮不改 firing_count。
	again := claimAfter(t, e, tasks, "third")
	if err := rounds.Record(ctx, again, errRound); err != nil {
		t.Fatal(err)
	}
	if got := e.task(ruleID, ds); got.Status != int(monitorcontract.EvalError) || got.Error != "boom" || got.FiringCount != 1 {
		t.Fatalf("task after an error round = %+v", got)
	}

	// 任务被规划器删除后，持有旧租约的评估器写回同样被拒。
	last := claimAfter(t, e, tasks, "fourth")
	e.exec("DELETE FROM monitor_eval_task")
	if err := rounds.Commit(ctx, last, false, firing(&called)); !errors.Is(err, biz.ErrLeaseLost) {
		t.Fatalf("write-back for a deleted task: %v", err)
	}
}

func claimAfter(t *testing.T, e *env, repo *taskRepo, token string) *biz.Task {
	t.Helper()
	e.exec("UPDATE monitor_eval_task SET next_eval_time = now() - interval '1 second'")
	got, err := repo.Claim(context.Background(), token, 1, time.Minute, 30*time.Second, 0)
	if err != nil || len(got) != 1 {
		t.Fatalf("Claim(%s) = %+v, %v", token, got, err)
	}
	return got[0]
}

func TestEndToEnd(t *testing.T) {
	e := newEnv(t)
	prom := promtest.New(t)
	ds := e.datasource(prom.URL)
	ruleID := e.rule(ruleSpec{
		ForS: 60, RepeatM: 5, Labels: map[string]string{"team": "ops"},
		Annotations:  map[string]string{"summary": "{{ $labels.instance }} is down ({{ $value }})", "broken": "{{ nope }}"},
		RecoveryExpr: `up{instance="{{ $labels.instance }}"}`,
	})
	g := e.engine("")
	g.materialize()
	states := func() int { return e.count("SELECT count(*) FROM monitor_eval_state WHERE rule_id = $1", ruleID) }

	// 1. 序列出现 → 待触发。
	prom.Set(upDown, promtest.S(0, "__name__", "up", "instance", "a"))
	if n := g.evalDue(); n != 1 {
		t.Fatalf("evaluated %d tasks", n)
	}
	if states() != 1 || len(e.alerts(ruleID)) != 0 {
		t.Fatalf("round 1: states = %d, alerts = %d", states(), len(e.alerts(ruleID)))
	}
	rec := e.lastRecord(ruleID)
	if rec.Status != int(monitorcontract.EvalOK) || rec.Series != 1 || rec.Anomalies != 1 || rec.Detail.Points[0].Step != monitorcontract.StepPending ||
		len(rec.Detail.Queries) != 1 || rec.Detail.Queries[0].Series != 1 || rec.Detail.Queries[0].Severity != 2 {
		t.Fatalf("round 1 record = %+v", rec)
	}
	if task := e.task(ruleID, ds); task.Status != int(monitorcontract.EvalOK) || task.Owner != nil || task.NextIn < 10 || task.NextIn > 16.1 {
		t.Fatalf("round 1 task = %+v", task)
	}

	// 2. 持续满 for_s → 告警中：事件 + 发件箱（触发）。
	e.age(61 * time.Second)
	g.evalDue()
	a := e.oneAlert(ruleID)
	wantFP := monitorcontract.Fingerprint(ruleID, ds, map[string]string{"instance": "a", "team": "ops"}, 0)
	if a.Status != 1 || a.Severity != 2 || a.Fingerprint != wantFP || a.Expr != upDown || a.Value == nil || *a.Value != 0 || a.Resolve != nil {
		t.Fatalf("alert = %+v", a)
	}
	if len(a.Labels) != 2 || a.Labels["team"] != "ops" || a.Labels["instance"] != "a" {
		t.Fatalf("labels = %v (must drop __name__ and merge rule labels)", a.Labels)
	}
	if a.Annotations["summary"] != "a is down (0)" || a.Annotations["broken"] != "<template error>" {
		t.Fatalf("annotations = %v", a.Annotations)
	}
	if !strings.HasPrefix(a.RuleName, "rule-") || !strings.HasPrefix(a.DatasourceName, "ds-") || a.GroupKey != monitorcontract.GroupKey(ruleID, a.Labels, nil) {
		t.Fatalf("alert meta = %+v", a)
	}
	if got := e.outbox(a.ID); !equalStrings(got, []string{"1/1/1"}) {
		t.Fatalf("outbox = %v", got)
	}
	if task := e.task(ruleID, ds); task.Status != int(monitorcontract.EvalFiring) || task.FiringCount != 1 {
		t.Fatalf("round 2 task = %+v", task)
	}
	if rec = e.lastRecord(ruleID); rec.Detail.Points[0].Step != monitorcontract.StepFired || rec.Detail.Points[0].AlertID != a.ID {
		t.Fatalf("round 2 record = %+v", rec)
	}
	if e.count("SELECT count(*) FROM monitor_eval_state WHERE alert_id = $1 AND state = 2", a.ID) != 1 {
		t.Fatalf("state row must point at the alert")
	}

	// 3. 仍在告警、core 尚未派发：不重复写首次通知，也不发重复通知。
	g.evalDue()
	if got := e.outbox(a.ID); !equalStrings(got, []string{"1/1/1"}) {
		t.Fatalf("outbox = %v", got)
	}
	// 4. 派发后到达重复间隔 → 重复通知（seq 2）；未派发前再评估不重复写。
	e.dispatch()
	e.age(6 * time.Minute)
	prom.Set(upDown, promtest.S(0.5, "__name__", "up", "instance", "a"))
	g.evalDue()
	g.evalDue()
	if got := e.outbox(a.ID); !equalStrings(got, []string{"1/1/2", "2/2/1"}) {
		t.Fatalf("outbox = %v", got)
	}
	if cur := e.oneAlert(ruleID); *cur.Value != 0.5 || !cur.LastFire.After(cur.FirstFire) || cur.ID != a.ID {
		t.Fatalf("alert must be refreshed in place: %+v", cur)
	}
	e.dispatch()

	// 5. 序列消失 → 恢复：恢复取值 + 发件箱（恢复，seq = 已发送数 + 1）。
	prom.Set(upDown)
	prom.Set(`up{instance="a"}`, promtest.S(1, "instance", "a"))
	g.evalDue()
	cur := e.oneAlert(ruleID)
	if cur.Status != 2 || cur.Resolve == nil || cur.RecoveryValue == nil || *cur.RecoveryValue != 1 || cur.NotifyCount != 2 {
		t.Fatalf("resolved alert = %+v", cur)
	}
	if got := e.outbox(a.ID); !equalStrings(got, []string{"1/1/2", "2/2/2", "3/3/1"}) {
		t.Fatalf("outbox = %v", got)
	}
	if states() != 0 {
		t.Fatalf("state row must be deleted on resolve")
	}
	if rec = e.lastRecord(ruleID); rec.Status != int(monitorcontract.EvalNoData) || rec.Recovered != 1 || rec.Detail.Points[0].Step != monitorcontract.StepResolved {
		t.Fatalf("resolve record = %+v", rec)
	}
	if task := e.task(ruleID, ds); task.Status != int(monitorcontract.EvalNoData) || task.FiringCount != 0 {
		t.Fatalf("resolve task = %+v", task)
	}

	// 6. 再次触发是一条新事件。
	prom.Set(upDown, promtest.S(0, "__name__", "up", "instance", "a"))
	g.evalDue()
	e.age(61 * time.Second)
	g.evalDue()
	all := e.alerts(ruleID)
	if len(all) != 2 || all[0].Status != 2 || all[1].Status != 1 || all[1].ID == a.ID || all[1].Fingerprint != wantFP {
		t.Fatalf("alerts = %+v", all)
	}

	// 7. 查询出错：记一条查询错误，不改任何状态、不判恢复。
	prom.SetError(upDown, 422, "execution", "boom")
	g.evalDue()
	if rec = e.lastRecord(ruleID); rec.Status != int(monitorcontract.EvalError) || !strings.Contains(rec.Error, "boom") || rec.Detail.Queries[0].Error == "" {
		t.Fatalf("error record = %+v", rec)
	}
	if task := e.task(ruleID, ds); task.Status != int(monitorcontract.EvalError) || task.FiringCount != 1 || !strings.Contains(task.Error, "boom") {
		t.Fatalf("error task = %+v", task)
	}
	if all = e.alerts(ruleID); all[1].Status != 1 || states() != 1 {
		t.Fatalf("a query error must not resolve anything")
	}
	if n := e.count("SELECT count(*) FROM monitor_eval_record WHERE rule_id = $1", ruleID); n < 9 {
		t.Fatalf("every round must leave an eval record, got %d", n)
	}
}

func TestCrashSafety(t *testing.T) {
	e := newEnv(t)
	prom := promtest.New(t)
	ds := e.datasource(prom.URL)
	ruleID := e.rule(ruleSpec{ForS: 60})
	prom.Set(upDown, promtest.S(0, "instance", "a"))

	// 进程 1：评估一轮（待触发）后被杀。
	p1 := e.engine("")
	p1.materialize()
	p1.evalDue()
	var since time.Time
	if err := e.admin.QueryRow(context.Background(), "SELECT active_since FROM monitor_eval_state").Scan(&since); err != nil {
		t.Fatal(err)
	}

	// 进程 2 接手：计时延续（active_since 不变），满时长后触发。
	e.age(30 * time.Second)
	p2 := e.engine("")
	p2.evalDue()
	if len(e.alerts(ruleID)) != 0 || e.count("SELECT count(*) FROM monitor_eval_state WHERE active_since = $1::timestamptz - interval '30 seconds'", since) != 1 {
		t.Fatalf("pending timer must continue across restarts")
	}
	e.age(31 * time.Second)
	p2.evalDue()
	a := e.oneAlert(ruleID)
	if got := e.outbox(a.ID); !equalStrings(got, []string{"1/1/1"}) {
		t.Fatalf("outbox = %v", got)
	}

	// 进程 3 领到任务后、写回之前被杀：租约期内无人能评估；到期后进程 4 接手。
	if got, err := NewTaskRepo(e.d).Claim(context.Background(), "killed", 10, time.Minute, 30*time.Second, 0); err != nil || len(got) != 0 {
		t.Fatalf("task is not due yet: %v %v", got, err)
	}
	e.exec("UPDATE monitor_eval_task SET next_eval_time = now() - interval '1 second'")
	if got, err := NewTaskRepo(e.d).Claim(context.Background(), "killed", 10, time.Minute, 30*time.Second, 0); err != nil || len(got) != 1 {
		t.Fatalf("claim: %v %v", got, err)
	}
	p4 := e.engine("")
	if n, err := p4.evaluator.RunOnce(context.Background(), p4.token); err != nil || n != 0 {
		t.Fatalf("a leased task must not be evaluated by another instance: %d, %v", n, err)
	}
	e.exec("UPDATE monitor_eval_task SET lease_until = now() - interval '1 second'")
	if n, err := p4.evaluator.RunOnce(context.Background(), p4.token); err != nil || n != 1 {
		t.Fatalf("expired lease must be taken over: %d, %v", n, err)
	}
	// 状态延续：还是同一个事件，没有第二条首次通知。
	cur := e.oneAlert(ruleID)
	if cur.ID != a.ID || cur.Status != 1 || !equalStrings(e.outbox(a.ID), []string{"1/1/1"}) {
		t.Fatalf("after takeover: alert = %+v, outbox = %v", cur, e.outbox(a.ID))
	}
	if task := e.task(ruleID, ds); task.Owner != nil || task.FiringCount != 1 {
		t.Fatalf("task = %+v", task)
	}
	// 再换两个进程各评估一轮：仍然只有一条首次通知。
	e.engine("").evalDue()
	e.engine("").evalDue()
	if len(e.alerts(ruleID)) != 1 || e.count("SELECT count(*) FROM monitor_notify_outbox") != 1 {
		t.Fatalf("duplicate alert or notification after repeated restarts")
	}
}

func TestAlertInsertFallsBackToExistingActiveAlert(t *testing.T) {
	e := newEnv(t)
	prom := promtest.New(t)
	ds := e.datasource(prom.URL)
	ruleID := e.rule(ruleSpec{})
	prom.Set(upDown, promtest.S(3, "instance", "a"))
	g := e.engine("")
	g.materialize()
	// 同指纹已有活跃事件但判定状态行不在了（例如状态被误删）：部分唯一索引兜底，改为更新已有事件。
	fp := monitorcontract.Fingerprint(ruleID, ds, map[string]string{"instance": "a"}, 0)
	existing := e.seedAlert(ruleID, ds, fp, monitorcontract.AlertFiring, 0)
	e.exec("DELETE FROM monitor_eval_state")

	g.evalDue()
	a := e.oneAlert(ruleID)
	if a.ID != existing || a.Status != 1 || a.Value == nil || *a.Value != 3 {
		t.Fatalf("alert = %+v, want the existing one updated", a)
	}
	if e.count("SELECT count(*) FROM monitor_eval_state WHERE alert_id = $1 AND state = 2", existing) != 1 {
		t.Fatalf("state row must point at the existing alert")
	}
	if got := e.outbox(existing); !equalStrings(got, []string{"1/1/1"}) {
		t.Fatalf("outbox must reference the existing alert: %v", got)
	}
	if e.count("SELECT count(*) FROM monitor_notify_outbox") != 1 {
		t.Fatalf("no outbox row may reference the discarded id")
	}
}

func TestLargeRule(t *testing.T) {
	e := newEnv(t)
	prom := promtest.New(t)
	ds := e.datasource(prom.URL)
	ruleID := e.rule(ruleSpec{Annotations: map[string]string{"summary": "{{ $labels.instance }} = {{ $value }}"}})
	g := e.engine("")
	g.materialize()
	const total, limit = 2100, 2000 // 超过 max_series 缺省值：截断并标记
	series := make([]promtest.Sample, total)
	for i := range series {
		series[i] = promtest.S(float64(i), "instance", fmt.Sprintf("10.0.%d.%d", i/250, i%250), "job", "node")
	}
	prom.Set(upDown, series...)
	round := func(name string) {
		t.Helper()
		start := time.Now()
		g.evalDue()
		t.Logf("%s: %v", name, time.Since(start))
	}

	round("2000 series fire")
	if n := e.count("SELECT count(*) FROM monitor_alert WHERE rule_id = $1 AND status = 1", ruleID); n != limit {
		t.Fatalf("alerts = %d", n)
	}
	if e.count("SELECT count(*) FROM monitor_eval_state WHERE state = 2 AND alert_id IS NOT NULL") != limit ||
		e.count("SELECT count(*) FROM monitor_notify_outbox WHERE kind = 1 AND seq = 1") != limit {
		t.Fatalf("states / outbox rows do not match")
	}
	if e.count("SELECT count(*) FROM monitor_eval_state s JOIN monitor_alert a ON a.id = s.alert_id AND a.fingerprint = s.fingerprint AND a.labels = s.labels") != limit {
		t.Fatalf("state rows must reference their own alert")
	}
	rec := e.lastRecord(ruleID)
	if rec.Series != limit || rec.Anomalies != limit || !rec.Detail.Queries[0].Truncated || len(rec.Detail.Points) != monitorcontract.MaxEvalPoints || !rec.Detail.Truncated {
		t.Fatalf("record = series %d anomalies %d queries %+v points %d truncated %v",
			rec.Series, rec.Anomalies, rec.Detail.Queries, len(rec.Detail.Points), rec.Detail.Truncated)
	}

	// E2：截断被标注在任务与判定记录上，评估结果照常给出。
	if got := e.task(ruleID, ds); got.Status != int(monitorcontract.EvalFiring) || !strings.HasPrefix(got.Error, "incomplete result: query 0 truncated to 2000 series") ||
		rec.Error != got.Error || rec.Status != int(monitorcontract.EvalFiring) {
		t.Fatalf("task = %+v, record error = %q", got, rec.Error)
	}

	// E4：什么都没变的一轮不写任何告警行（行版本原样），也不新增发件箱行。
	versions := func() string {
		t.Helper()
		var sum string
		if err := e.admin.QueryRow(context.Background(),
			"SELECT md5(string_agg(id::text || xmin::text || ctid::text, ',' ORDER BY id)) FROM monitor_alert").Scan(&sum); err != nil {
			t.Fatal(err)
		}
		return sum
	}
	before := versions()
	round("2000 series steady")
	if versions() != before {
		t.Fatalf("an unchanged round rewrote alert rows")
	}
	if e.count("SELECT count(*) FROM monitor_alert WHERE last_fire_time > first_fire_time") != 0 || e.count("SELECT count(*) FROM monitor_notify_outbox") != limit {
		t.Fatalf("steady round must leave alerts untouched and add no outbox rows")
	}
	if e.count("SELECT count(*) FROM monitor_eval_state WHERE last_seen > active_since") != limit {
		t.Fatalf("the state rows are still refreshed every round")
	}
	// 心跳：距上次写入超过 max(60s, 评估周期) 后，last_fire_time 才被刷新一次。
	e.age(61 * time.Second)
	round("2000 series heartbeat")
	if e.count("SELECT count(*) FROM monitor_alert WHERE status = 1 AND last_fire_time > first_fire_time + interval '60 seconds'") != limit {
		t.Fatalf("the heartbeat must refresh last_fire_time on every active alert")
	}
	before = versions()
	round("2000 series steady again")
	if versions() != before {
		t.Fatalf("the round after the heartbeat rewrote alert rows")
	}
	e.dispatch()
	prom.Set(upDown)
	round("2000 series resolve")
	if e.count("SELECT count(*) FROM monitor_alert WHERE status = 2 AND resolve_time IS NOT NULL") != limit ||
		e.count("SELECT count(*) FROM monitor_eval_state") != 0 || e.count("SELECT count(*) FROM monitor_notify_outbox WHERE kind = 3 AND seq = 2") != limit {
		t.Fatalf("resolve round incomplete")
	}
}

func TestMutesFromDatabase(t *testing.T) {
	e := newEnv(t)
	prom := promtest.New(t)
	ds := e.datasource(prom.URL)
	root := monitorcontract.RootGroupID
	child := e.id()
	e.exec("INSERT INTO monitor_rule_group (id, parent_id, path, name) VALUES ($1, $2, $3, 'child')", child, root, "/"+root+"/"+child+"/")
	ruleID := e.rule(ruleSpec{GroupID: child})
	prom.Set(upDown, promtest.S(0, "instance", "a1"), promtest.S(0, "instance", "b1"))
	e.engine("").materialize()
	mute := func(mode int32, group *string, extra string, args ...any) string {
		t.Helper()
		id := e.id()
		e.exec(`INSERT INTO monitor_mute (id, group_id, name, reason, mode, time_type, start_time, end_time, matchers)
			VALUES ($1, $2, 'm', 'maintenance', $3, 1, now() - interval '1 hour', now() + interval '1 hour', '[{"label":"instance","op":"=~","value":"a.*"}]')`,
			id, group, mode)
		if extra != "" {
			e.exec("UPDATE monitor_mute SET "+extra+" WHERE id = $1", append([]any{id}, args...)...)
		}
		e.exec("UPDATE monitor_cfg_version SET version = version + 1 WHERE name = $1", monitorcontract.CfgMute)
		return id
	}
	// 不该命中的屏蔽规则：已过期 / 已停用 / 已删除 / 其他数据源 / 其他级别 / 其他规则——都比真正命中的更强（屏蔽事件）。
	mute(monitorcontract.MuteModeEvent, nil, "start_time = now() - interval '2 hours', end_time = now() - interval '1 minute'")
	mute(monitorcontract.MuteModeEvent, nil, "enabled = false")
	mute(monitorcontract.MuteModeEvent, nil, "delete_time = now()")
	mute(monitorcontract.MuteModeEvent, nil, "datasource_ids = $2::uuid[]", []string{e.id()})
	mute(monitorcontract.MuteModeEvent, nil, "severities = '{4}'")
	mute(monitorcontract.MuteModeEvent, nil, "rule_ids = $2::uuid[]", []string{e.id()})
	// 命中：挂在上级分组（含下级）、限定本数据源与本级别，只屏蔽通知。
	notifyMute := mute(monitorcontract.MuteModeNotify, &root, "datasource_ids = $2::uuid[], severities = '{2,3}', rule_ids = $3::uuid[]", []string{ds}, []string{ruleID})

	e.engine("").evalDue()
	all := e.alerts(ruleID)
	if len(all) != 2 {
		t.Fatalf("alerts = %d", len(all))
	}
	var muted, plain alertRow
	for _, a := range all {
		if a.Labels["instance"] == "a1" {
			muted = a
		} else {
			plain = a
		}
	}
	if !muted.Muted || muted.MuteID == nil || *muted.MuteID != notifyMute || len(e.outbox(muted.ID)) != 0 {
		t.Fatalf("muted alert = %+v, outbox = %v", muted, e.outbox(muted.ID))
	}
	if plain.Muted || plain.MuteID != nil || !equalStrings(e.outbox(plain.ID), []string{"1/1/1"}) {
		t.Fatalf("plain alert = %+v, outbox = %v", plain, e.outbox(plain.ID))
	}

	// 屏蔽解除（规则被删除）而告警仍在：下一轮按一次新通知处理。
	e.exec("UPDATE monitor_mute SET delete_time = now() WHERE id = $1", notifyMute)
	e.exec("UPDATE monitor_cfg_version SET version = version + 1 WHERE name = $1", monitorcontract.CfgMute)
	e.engine("").evalDue()
	if cur := e.alerts(ruleID); cur[0].Muted || cur[1].Muted || !equalStrings(e.outbox(muted.ID), []string{"1/1/1"}) {
		t.Fatalf("after the mute ended: outbox = %v", e.outbox(muted.ID))
	}

	// 屏蔽事件：新序列命中后不建事件，状态停在待触发，判定记录写明原因。
	eventMute := mute(monitorcontract.MuteModeEvent, &child, "matchers = '[{\"label\":\"instance\",\"op\":\"in\",\"values\":[\"c1\"]}]'")
	prom.Set(upDown, promtest.S(0, "instance", "a1"), promtest.S(0, "instance", "b1"), promtest.S(0, "instance", "c1"))
	e.engine("").evalDue()
	if len(e.alerts(ruleID)) != 2 || e.count(`SELECT count(*) FROM monitor_eval_state WHERE state = 1 AND alert_id IS NULL AND labels @> '{"instance":"c1"}'`) != 1 {
		t.Fatalf("mute-event must keep the series pending without an alert")
	}
	var found bool
	for _, p := range e.lastRecord(ruleID).Detail.Points {
		if p.Step == monitorcontract.StepMutedEvent && p.MuteID == eventMute && p.Labels["instance"] == "c1" {
			found = true
		}
	}
	if !found {
		t.Fatalf("eval record must explain the muted event: %+v", e.lastRecord(ruleID).Detail.Points)
	}
	// 作用分组被删除的屏蔽规则不再命中任何事件。
	e.exec("UPDATE monitor_rule SET group_id = $1", root)
	e.exec("UPDATE monitor_rule_group SET delete_time = now() WHERE id = $1", child)
	e.exec("UPDATE monitor_cfg_version SET version = version + 1 WHERE name = $1", monitorcontract.CfgRuleGroup)
	e.engine("").evalDue()
	if len(e.alerts(ruleID)) != 3 {
		t.Fatalf("a mute bound to a deleted group must stop matching, alerts = %d", len(e.alerts(ruleID)))
	}
}

func TestDatasourceSecret(t *testing.T) {
	e := newEnv(t)
	secured, open := promtest.New(t), promtest.New(t)
	box, err := secretbox.New("integration-master-key")
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := box.Seal("tok-123")
	if err != nil {
		t.Fatal(err)
	}
	dsSecured, dsOpen := e.datasource(secured.URL), e.datasource(open.URL)
	e.exec("UPDATE monitor_datasource SET auth_type = 3, secret = $2, headers = '{\"X-Scope-OrgID\":\"t1\"}' WHERE id = $1", dsSecured, sealed)
	ruleID := e.rule(ruleSpec{})
	secured.Set(upDown, promtest.S(0, "instance", "a"))
	open.Set(upDown, promtest.S(0, "instance", "a"))

	// 主密钥未配置：需要凭据的数据源本轮失败（原因明确且不含凭据），其余数据源照常。
	noKey := e.engine("")
	noKey.materialize()
	noKey.evalDue()
	if got := e.task(ruleID, dsSecured); got.Status != int(monitorcontract.EvalError) || got.Error != "datasource credential unavailable: master key missing or mismatched" {
		t.Fatalf("secured task without key = %+v", got)
	}
	if got := e.task(ruleID, dsOpen); got.Status != int(monitorcontract.EvalFiring) || secured.TotalHits() != 0 {
		t.Fatalf("open task = %+v, secured hits = %d", got, secured.TotalHits())
	}
	if _, err := noKey.planner.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	if e.count("SELECT count(*) FROM monitor_datasource WHERE id = $1 AND health_status = 2 AND health_error LIKE 'datasource credential unavailable%'", dsSecured) != 1 ||
		e.count("SELECT count(*) FROM monitor_datasource WHERE id = $1 AND health_status = 1 AND health_error = ''", dsOpen) != 1 {
		t.Fatalf("probe must report the undecryptable datasource as failed and the other as healthy")
	}

	// 配置了主密钥：凭据解密后随请求发出。
	withKey := e.engine("integration-master-key")
	withKey.evalDue()
	if got := e.task(ruleID, dsSecured); got.Status != int(monitorcontract.EvalFiring) {
		t.Fatalf("secured task with key = %+v", got)
	}
	if h := secured.LastHeader(); h.Get("Authorization") != "Bearer tok-123" || h.Get("X-Scope-OrgID") != "t1" {
		t.Fatalf("request headers = %v", h)
	}
	if e.count("SELECT count(*) FROM monitor_eval_record WHERE error LIKE '%tok-123%' OR detail::text LIKE '%tok-123%'") != 0 {
		t.Fatalf("secret leaked into eval records")
	}
}

func TestHealthProbeWritesOnlyHealthColumns(t *testing.T) {
	e := newEnv(t)
	up, down := promtest.New(t), promtest.New(t)
	down.SetDown(true)
	dsUp, dsDown, dsOff := e.datasource(up.URL), e.datasource(down.URL), e.datasource(up.URL)
	e.exec("UPDATE monitor_datasource SET enabled = false WHERE id = $1", dsOff)
	e.exec("UPDATE monitor_datasource SET update_time = now() - interval '1 day', version = 5")
	g := e.engine("")
	if _, err := g.planner.Probe(context.Background()); err != nil {
		t.Fatalf("Probe() error = %v", err)
	}
	if e.count("SELECT count(*) FROM monitor_datasource WHERE id = $1 AND health_status = 1 AND health_error = '' AND health_time > now() - interval '1 minute'", dsUp) != 1 {
		t.Fatalf("healthy datasource not recorded")
	}
	if e.count("SELECT count(*) FROM monitor_datasource WHERE id = $1 AND health_status = 2 AND health_error LIKE '%503%' AND health_time IS NOT NULL", dsDown) != 1 {
		t.Fatalf("failed datasource not recorded")
	}
	if e.count("SELECT count(*) FROM monitor_datasource WHERE id = $1 AND health_status = 0 AND health_time IS NULL", dsOff) != 1 {
		t.Fatalf("disabled datasource must not be probed")
	}
	// 只动 health 三列：乐观锁版本与更新时间不变（否则会让管理端的并发保存失败、让各实例重建查询客户端）。
	if e.count("SELECT count(*) FROM monitor_datasource WHERE version = 5 AND update_time < now() - interval '23 hours'") != 3 {
		t.Fatalf("probe must not touch version / update_time")
	}
	// 恢复后翻回正常并清空错误。
	down.SetDown(false)
	if _, err := g.planner.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	if e.count("SELECT count(*) FROM monitor_datasource WHERE id = $1 AND health_status = 1 AND health_error = ''", dsDown) != 1 {
		t.Fatalf("recovered datasource must flip back to healthy")
	}
}

// TestEngineRoleIsRestricted 确认集成测试确实跑在受限角色上：引擎不该有的权限一律被数据库拒绝。
func TestEngineRoleIsRestricted(t *testing.T) {
	e := newEnv(t)
	ds := e.datasource("http://127.0.0.1:1")
	ruleID := e.rule(ruleSpec{})
	ctx := context.Background()
	for name, stmt := range map[string]string{
		"update datasource config":  "UPDATE monitor_datasource SET name = 'x' WHERE id = '" + ds + "'",
		"update datasource version": "UPDATE monitor_datasource SET version = version + 1",
		"update rule":               "UPDATE monitor_rule SET enabled = false WHERE id = '" + ruleID + "'",
		"delete rule":               "DELETE FROM monitor_rule",
		"insert mute":               "INSERT INTO monitor_mute (id, name, reason) VALUES (gen_random_uuid(), 'm', 'r')",
		"bump cfg version":          "UPDATE monitor_cfg_version SET version = version + 1",
		"read subscriptions":        "SELECT count(*) FROM monitor_subscription",
		"read role scopes":          "SELECT count(*) FROM monitor_role_scope",
		"read rule versions":        "SELECT count(*) FROM monitor_rule_version",
		"insert notify log":         "INSERT INTO monitor_notify_log (id, alert_id, kind, seq, source) VALUES (gen_random_uuid(), gen_random_uuid(), 1, 1, 1)",
		"read notify events":        "SELECT count(*) FROM notify_event",
		// 加引号：PG 16 起裸写的 system_user 是 SQL 函数。
		"read users":                 `SELECT count(*) FROM "system_user"`,
		"truncate alerts":            "TRUNCATE monitor_alert",
		"update notify log":          "UPDATE monitor_notify_log SET note = 'x'",
		"read another domain (cmdb)": "SELECT count(*) FROM cmdb_ci",
	} {
		_, err := e.d.Pool.Exec(ctx, stmt)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
			t.Errorf("%s: want permission denied (42501), got %v", name, err)
		}
	}
	// 被授权的：health 三列、运行表、通知记录的清理。
	for name, stmt := range map[string]string{
		"write health columns": "UPDATE monitor_datasource SET health_status = 1, health_error = '', health_time = now()",
		"delete notify log":    "DELETE FROM monitor_notify_log",
		"read rule":            "SELECT count(*) FROM monitor_rule",
	} {
		if _, err := e.d.Pool.Exec(ctx, stmt); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}
