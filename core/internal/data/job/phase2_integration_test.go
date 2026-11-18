//go:build integration

package data

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/job"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func (e *env) base() *biz.Template {
	return e.template("tpl-"+newID(e.t)[30:], e.project("p-"+newID(e.t)[30:]), e.credential("c-"+newID(e.t)[30:]), false)
}

func (e *env) setStatus(id string, status int32) {
	e.t.Helper()
	finish := "NULL"
	if !jc.Active(status) {
		finish = "now()"
	}
	if _, err := e.pool.Exec(context.Background(),
		"UPDATE job_execution SET status = $2, finish_time = "+finish+", start_time = COALESCE(start_time, now() - interval '90 seconds') WHERE id = $1", id, status); err != nil {
		e.t.Fatal(err)
	}
}

// 快速执行与切片的落库：无模板、文件内容、分组；审批的记账与落账；待审批的互斥与取消。
func TestAdhocSlicesAndApproval(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	tpl := e.template("excl", e.project("p"), e.credential("c"), true)

	// 快速执行（文件）：无模板，内容入 job_execution_payload。
	adhoc := &biz.Execution{
		ID: newID(t), Kind: jc.KindFile, TemplateName: "文件：a.conf → /etc", Trigger: jc.TriggerManual, Creator: uAlice, Status: jc.StatusQueued,
		Spec: jc.Spec{
			Kind: jc.KindFile, Adhoc: &jc.Adhoc{FileName: "a.conf", Dest: "/etc", Mode: "0644", FileSize: 3}, CredentialID: tpl.CredentialID,
			Target: &jc.Target{ModelCode: "host", Mode: jc.TargetModeFilter}, ExtraVars: map[string]any{}, Options: jc.Options{Forks: 1, TimeoutS: 60, Port: 22},
		},
		TimeoutS: 60, RiskHits: []jc.RiskHit{{RuleID: newID(t), Name: "r", Action: jc.RiskWarn, Match: "x"}},
	}
	group := newID(t)
	slice := func(no int32, status int32) biz.NewExecution {
		return biz.NewExecution{
			Execution: &biz.Execution{
				ID: newID(t), Kind: jc.KindPlaybook, TemplateID: tpl.ID, TemplateName: "excl", TemplateVersion: 1, Trigger: jc.TriggerSchedule,
				TriggerRef: newID(t), Creator: uAlice, Status: status, Spec: jc.Spec{Options: jc.Options{Port: 22}, ExtraVars: map[string]any{}},
				TimeoutS: 60, GroupID: group, SliceNo: no, SliceTotal: 2, ApprovalReason: "r",
			},
			Hosts: []biz.Host{{CIID: newID(t), Name: "h", Address: "10.0.0." + string(rune('0'+no))}},
		}
	}
	err := e.txs.WithinTx(ctx, func(tx biz.Tx) error {
		if _, _, err := e.executions.CreateTx(ctx, tx, []biz.NewExecution{{Execution: adhoc, Hosts: []biz.Host{{CIID: newID(t), Name: "h", Address: "10.0.0.9"}}, Payload: []byte("abc")}}); err != nil {
			return err
		}
		id, created, err := e.executions.CreateTx(ctx, tx, []biz.NewExecution{slice(1, jc.StatusPending), slice(2, jc.StatusPending)})
		if err != nil || !created || id == "" {
			t.Errorf("create slices = %q, %v, %v", id, created, err)
		}
		return e.executions.SetApprovalTx(ctx, tx, group, "018f3e1a-0e0e-7000-8000-000000000001")
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := e.executions.Get(ctx, adhoc.ID)
	if err != nil || got.Kind != jc.KindFile || got.TemplateID != "" || got.Spec.Adhoc.FileName != "a.conf" || got.Spec.Target.ModelCode != "host" ||
		len(got.RiskHits) != 1 || got.RiskHits[0].Match != "x" {
		t.Fatalf("adhoc execution = %+v, %v", got, err)
	}
	var payload []byte
	if err := e.pool.QueryRow(ctx, "SELECT content FROM job_execution_payload WHERE execution_id = $1", adhoc.ID).Scan(&payload); err != nil || string(payload) != "abc" {
		t.Fatalf("payload = %q, %v", payload, err)
	}
	items, total, err := e.executions.List(ctx, biz.ExecutionFilter{GroupID: group}, 1, 10)
	if err != nil || total != 2 || items[0].ApprovalInstanceID == "" || items[0].SliceTotal != 2 || items[0].Trigger != jc.TriggerSchedule {
		t.Fatalf("group list = %+v, %d, %v", items, total, err)
	}
	if _, n, _ := e.executions.List(ctx, biz.ExecutionFilter{Kinds: []int32{jc.KindFile}}, 1, 10); n != 1 {
		t.Fatalf("kind filter total = %d", n)
	}
	if _, n, _ := e.executions.List(ctx, biz.ExecutionFilter{Triggers: []int32{jc.TriggerSchedule}, TriggerRef: items[0].TriggerRef}, 1, 10); n != 1 {
		t.Fatalf("trigger filter total = %d", n)
	}

	// 审批通过：整组 → 排队；重复落账不再命中。
	var n int64
	err = e.txs.WithinTx(ctx, func(tx biz.Tx) error {
		var err error
		n, err = e.executions.ResolveApprovalTx(ctx, tx, group, true, "")
		return err
	})
	if err != nil || n != 2 {
		t.Fatalf("approve = %d, %v", n, err)
	}
	_ = e.txs.WithinTx(ctx, func(tx biz.Tx) error {
		n, err = e.executions.ResolveApprovalTx(ctx, tx, group, false, "late")
		return err
	})
	if n != 0 {
		t.Fatalf("resolving twice touched %d rows", n)
	}

	// 另一组：驳回 → 审批未通过并记原因；互斥位释放。
	if _, err := e.pool.Exec(ctx, "UPDATE job_execution SET status = 6, finish_time = now() WHERE group_id = $1", group); err != nil {
		t.Fatal(err)
	}
	group = newID(t)
	a, b := slice(1, jc.StatusPending), slice(2, jc.StatusPending)
	if err := e.txs.WithinTx(ctx, func(tx biz.Tx) error {
		_, _, err := e.executions.CreateTx(ctx, tx, []biz.NewExecution{a, b})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	// 取消待审批的一片：整组取消。
	if err := e.executions.Cancel(ctx, a.Execution.ID, uBob); err != nil {
		t.Fatal(err)
	}
	if x, _ := e.executions.Get(ctx, b.Execution.ID); x.Status != jc.StatusCanceled || x.CancelUser != uBob || x.FinishTime == nil {
		t.Fatalf("sibling slice after withdraw = %+v", x)
	}
	group = newID(t)
	c := slice(1, jc.StatusPending)
	c.Execution.SliceNo, c.Execution.SliceTotal, c.Execution.GroupID, c.Execution.Exclusive = 0, 0, "", true
	if err := e.txs.WithinTx(ctx, func(tx biz.Tx) error {
		_, _, err := e.executions.CreateTx(ctx, tx, []biz.NewExecution{c})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	// 待审批的互斥执行占着互斥位。
	if _, _, _, err := e.execution(tpl, uBob, ""); !is(err, biz.ErrExecutionConflict) {
		t.Fatalf("pending exclusive slot not held: %v", err)
	}
	if err := e.txs.WithinTx(ctx, func(tx biz.Tx) error {
		_, err := e.executions.ResolveApprovalTx(ctx, tx, c.Execution.ID, false, "审批被驳回")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if x, _ := e.executions.Get(ctx, c.Execution.ID); x.Status != jc.StatusRejected || x.Error != "审批被驳回" || x.FinishTime == nil {
		t.Fatalf("rejected = %+v", x)
	}
	if _, _, _, err := e.execution(tpl, uBob, ""); err != nil {
		t.Fatalf("slot not released after rejection: %v", err)
	}
}

// 脚本库、高危规则、全局策略的仓储。
func TestScriptRulePolicyRepos(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	s := &biz.Script{ID: newID(t), Name: "clean", Interpreter: "bash", Content: "echo 你好\n", Description: "d"}
	if err := e.scripts.Write(ctx, biz.OpCreate, s, 0, uAlice); err != nil {
		t.Fatal(err)
	}
	if err := e.scripts.Write(ctx, biz.OpCreate, &biz.Script{ID: newID(t), Name: "clean", Interpreter: "sh", Content: "x"}, 0, ""); !is(err, biz.ErrScriptNameExists) {
		t.Fatalf("duplicate: %v", err)
	}
	items, total, err := e.scripts.List(ctx, biz.ScriptFilter{Keyword: "CLEAN", Interpreter: "bash"}, 1, 10)
	if err != nil || total != 1 || items[0].Content != "" || int(items[0].Size) != len(s.Content) {
		t.Fatalf("List() = %+v, %d, %v", items, total, err)
	}
	s.Content = "echo v2\n"
	if err := e.scripts.Write(ctx, biz.OpUpdate, s, 1, uBob); err != nil {
		t.Fatal(err)
	}
	if err := e.scripts.Write(ctx, biz.OpUpdate, s, 1, uBob); !is(err, biz.ErrVersionConflict) {
		t.Fatalf("stale: %v", err)
	}
	if got, _ := e.scripts.Get(ctx, s.ID); got.Content != "echo v2\n" || got.Version != 2 || got.UpdateUser != uBob {
		t.Fatalf("Get() = %+v", got)
	}
	if err := e.scripts.Write(ctx, biz.OpDelete, s, 0, uBob); err != nil {
		t.Fatal(err)
	}
	if _, err := e.scripts.Get(ctx, s.ID); !is(err, biz.ErrScriptNotFound) {
		t.Fatalf("get deleted: %v", err)
	}

	// 迁移内置的规则已被 TRUNCATE；规则按动作、名称排序。
	for i, r := range []*biz.RiskRule{{Name: "w", Pattern: "a", Action: jc.RiskWarn}, {Name: "b", Pattern: "b", Action: jc.RiskBlock, Enabled: true}} {
		r.ID = newID(t)
		if err := e.rules.Write(ctx, biz.OpCreate, r, 0, uAlice); err != nil {
			t.Fatalf("rule %d: %v", i, err)
		}
	}
	rules, err := e.rules.List(ctx)
	if err != nil || len(rules) != 2 || rules[0].Name != "b" || !rules[0].Enabled || rules[1].Enabled {
		t.Fatalf("rules = %+v, %v", rules, err)
	}
	rules[1].Enabled = true
	if err := e.rules.Write(ctx, biz.OpUpdate, rules[1], 1, uBob); err != nil {
		t.Fatal(err)
	}
	if err := e.rules.Write(ctx, biz.OpUpdate, rules[1], 1, uBob); !is(err, biz.ErrVersionConflict) {
		t.Fatalf("stale rule: %v", err)
	}
	if err := e.rules.Write(ctx, biz.OpDelete, &biz.RiskRule{ID: newID(t)}, 0, ""); !is(err, biz.ErrRiskRuleNotFound) {
		t.Fatalf("delete missing: %v", err)
	}

	p, err := e.policies.Get(ctx)
	if err != nil || p.Version != 1 || len(p.ProtectedEnvs) != 0 {
		t.Fatalf("policy = %+v, %v", p, err)
	}
	p.ProtectedEnvs, p.ApprovalWorkflowKey, p.AdhocApproval, p.MaxHosts = []string{"prod"}, "job-approval", true, 50
	if err := e.policies.Update(ctx, p, uAlice); err != nil {
		t.Fatal(err)
	}
	if err := e.policies.Update(ctx, p, uAlice); !is(err, biz.ErrVersionConflict) {
		t.Fatalf("stale policy: %v", err)
	}
	if got, _ := e.policies.Get(ctx); got.Version != 2 || got.ProtectedEnvs[0] != "prod" || !got.AdhocApproval || got.HostLimit() != 50 || got.UpdateUser != uAlice {
		t.Fatalf("policy after update = %+v", got)
	}
}

// 定时作业：到期队列、条件推进（并发只推进一次）、停用后不再到期、编辑换负责人。
func TestScheduleQueue(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	tpl := e.base()
	past := time.Now().Add(-time.Minute).Truncate(time.Microsecond)
	s := &biz.Schedule{
		ID: newID(t), Name: "nightly", TemplateID: tpl.ID, Cron: "0 3 * * *", Timezone: "Asia/Shanghai",
		Params: map[string]any{"version": "1"}, Misfire: jc.MisfireSkip, Enabled: true, NextTime: &past,
	}
	if err := e.schedules.Write(ctx, biz.OpCreate, s, 0, uAlice); err != nil {
		t.Fatal(err)
	}
	future := time.Now().Add(time.Hour)
	later := &biz.Schedule{ID: newID(t), Name: "later", TemplateID: tpl.ID, Cron: "0 4 * * *", Timezone: "UTC", Misfire: jc.MisfireRunOnce, Enabled: true, NextTime: &future}
	if err := e.schedules.Write(ctx, biz.OpCreate, later, 0, uAlice); err != nil {
		t.Fatal(err)
	}
	if err := e.schedules.Write(ctx, biz.OpCreate, &biz.Schedule{ID: newID(t), Name: "nightly", TemplateID: tpl.ID, Cron: "* * * * *", Timezone: "UTC", Misfire: 1}, 0, uAlice); !is(err, biz.ErrScheduleNameExists) {
		t.Fatalf("duplicate: %v", err)
	}
	due, now, err := e.schedules.Due(ctx, 10)
	if err != nil || len(due) != 1 || due[0].ID != s.ID || due[0].Owner != uAlice || due[0].TemplateName != tpl.Name || due[0].Params["version"] != "1" || now.IsZero() {
		t.Fatalf("Due() = %+v, %v", due, err)
	}
	// 八个并发的推进：只有一个成功。
	var wins atomic.Int32
	var wg sync.WaitGroup
	next := time.Now().Add(24 * time.Hour).Truncate(time.Microsecond)
	_, execID, _, err := e.execution(tpl, uAlice, "")
	if err != nil {
		t.Fatal(err)
	}
	for range 8 {
		wg.Go(func() {
			ok, err := e.schedules.Advance(ctx, s.ID, biz.ScheduleRun{Fired: *due[0].NextTime, Next: next, ExecutionID: execID})
			if err != nil {
				t.Errorf("Advance: %v", err)
			}
			if ok {
				wins.Add(1)
			}
		})
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("%d concurrent advances won, want exactly 1", wins.Load())
	}
	got, _ := e.schedules.Get(ctx, s.ID)
	if got.LastExecutionID != execID || got.LastTime == nil || !got.NextTime.Equal(next) || got.LastError != "" {
		t.Fatalf("after advance = %+v", got)
	}
	if due, _, _ := e.schedules.Due(ctx, 10); len(due) != 0 {
		t.Fatalf("still due after advance: %d", len(due))
	}
	// 触发失败只记原因，保留上一次的执行。
	if ok, err := e.schedules.Advance(ctx, s.ID, biz.ScheduleRun{Fired: next, Next: next.Add(time.Hour), Error: "template disabled"}); err != nil || !ok {
		t.Fatalf("advance with error = %v, %v", ok, err)
	}
	if got, _ := e.schedules.Get(ctx, s.ID); got.LastError != "template disabled" || got.LastExecutionID != execID {
		t.Fatalf("after failed fire = %+v", got)
	}
	// 编辑：负责人换成编辑人，错误清空，停用后 next_time 为空且不再到期。
	got, _ = e.schedules.Get(ctx, s.ID)
	got.Enabled, got.NextTime = false, nil
	if err := e.schedules.Write(ctx, biz.OpUpdate, got, got.Version, uBob); err != nil {
		t.Fatal(err)
	}
	if after, _ := e.schedules.Get(ctx, s.ID); after.Owner != uBob || after.LastError != "" || after.NextTime != nil || after.Version != 2 {
		t.Fatalf("after edit = %+v", after)
	}
	off := false
	if _, total, _ := e.schedules.List(ctx, biz.ScheduleFilter{Enabled: &off, TemplateID: tpl.ID, Keyword: "night"}, 1, 10); total != 1 {
		t.Fatalf("List(filter) total = %d", total)
	}
	names, err := e.triggers.SourceNames(ctx, []string{s.ID}, nil)
	if err != nil || names[s.ID] != "nightly" {
		t.Fatalf("SourceNames = %v, %v", names, err)
	}
}

// 触发器：令牌只写、去重门闩、冷却（含并发）、触发记录、信号队列（等全部切片结束）。
func TestTriggerFiresAndSignals(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	tpl := e.base()
	api := &biz.Trigger{
		ID: newID(t), Name: "deploy-hook", Type: jc.TriggerTypeAPI, TemplateID: tpl.ID, Params: map[string]any{"version": "1"},
		PassParams: true, Signal: "job.done", CooldownS: 0, Enabled: true,
	}
	if err := e.triggers.Write(ctx, biz.OpCreate, api, 0, uAlice, &biz.TriggerToken{Hash: []byte("hash-1"), Sealed: []byte("sealed-1")}); err != nil {
		t.Fatal(err)
	}
	alert := &biz.Trigger{
		ID: newID(t), Name: "heal", Type: jc.TriggerTypeAlert, TemplateID: tpl.ID, CooldownS: 600, Enabled: true,
		Alert: jc.AlertMatch{RuleIDs: []string{newID(t)}, Severities: []int32{3}, Labels: map[string]string{"job": "node"}, HostLabel: "instance"},
	}
	if err := e.triggers.Write(ctx, biz.OpCreate, alert, 0, uAlice, nil); err != nil {
		t.Fatal(err)
	}
	got, err := e.triggers.Get(ctx, alert.ID)
	if err != nil || got.Alert.HostLabel != "instance" || got.Alert.Labels["job"] != "node" || got.Owner != uAlice || got.TemplateName != tpl.Name {
		t.Fatalf("Get(alert) = %+v, %v", got, err)
	}
	if tk, err := e.triggers.Token(ctx, api.ID); err != nil || string(tk.Hash) != "hash-1" || string(tk.Sealed) != "sealed-1" {
		t.Fatalf("Token = %+v, %v", tk, err)
	}
	if _, err := e.triggers.Token(ctx, alert.ID); !is(err, biz.ErrTriggerNotFound) {
		t.Fatalf("alert trigger has a token: %v", err)
	}
	if err := e.triggers.SetToken(ctx, alert.ID, biz.TriggerToken{Hash: []byte("x")}, ""); !is(err, biz.ErrTriggerNotFound) {
		t.Fatalf("SetToken on alert trigger: %v", err)
	}
	if err := e.triggers.SetToken(ctx, api.ID, biz.TriggerToken{Hash: []byte("hash-2")}, uBob); err != nil {
		t.Fatal(err)
	}
	if tk, _ := e.triggers.Token(ctx, api.ID); string(tk.Hash) != "hash-2" || tk.Sealed != nil {
		t.Fatalf("token after reset = %+v", tk)
	}
	if list, err := e.triggers.EnabledAlertTriggers(ctx); err != nil || len(list) != 1 || list[0].ID != alert.ID {
		t.Fatalf("EnabledAlertTriggers = %+v, %v", list, err)
	}
	typ := jc.TriggerTypeAPI
	if _, total, _ := e.triggers.List(ctx, biz.TriggerFilter{Type: &typ, Keyword: "DEPLOY"}, 1, 10); total != 1 {
		t.Fatalf("List(type) total = %d", total)
	}

	// 去重：同一 ref 第二次返回先前那条。
	instance := newID(t)
	f1 := &biz.TriggerFire{ID: newID(t), TriggerID: api.ID, Ref: "job-1", Summary: "发布", WorkflowInstanceID: instance, Signal: "job.done", SignalState: jc.SignalPending}
	if existing, fresh, err := e.fires.Begin(ctx, f1, 0); err != nil || !fresh || existing != nil || f1.Result != jc.FireSubmitted {
		t.Fatalf("Begin = %+v, %v, %v (result %d)", existing, fresh, err, f1.Result)
	}
	dup := &biz.TriggerFire{ID: newID(t), TriggerID: api.ID, Ref: "job-1"}
	if existing, fresh, err := e.fires.Begin(ctx, dup, 0); err != nil || fresh || existing == nil || existing.ID != f1.ID {
		t.Fatalf("duplicate Begin = %+v, %v, %v", existing, fresh, err)
	}
	if _, _, err := e.fires.Begin(ctx, &biz.TriggerFire{ID: newID(t), TriggerID: newID(t), Ref: "x"}, 0); !is(err, biz.ErrTriggerNotFound) {
		t.Fatalf("Begin on missing trigger: %v", err)
	}

	// 冷却：并发的八次不同事件，只有一次是「已提交」，其余记为跳过。
	var submitted, skipped atomic.Int32
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			f := &biz.TriggerFire{ID: newID(t), TriggerID: alert.ID, Ref: fmt.Sprintf("alert-%d", i)}
			if _, fresh, err := e.fires.Begin(ctx, f, alert.CooldownS); err != nil || !fresh {
				t.Errorf("Begin: fresh %v, %v", fresh, err)
				return
			}
			if f.Result == jc.FireSubmitted {
				submitted.Add(1)
			} else if f.Result == jc.FireSkipped && strings.Contains(f.Reason, "冷却") {
				skipped.Add(1)
			}
		})
	}
	wg.Wait()
	if submitted.Load() != 1 || skipped.Load() != 7 {
		t.Fatalf("cooldown: %d submitted, %d skipped; want 1 and 7", submitted.Load(), skipped.Load())
	}
	// 调用方已定为「跳过」的记录原样落库，不占冷却。
	pre := &biz.TriggerFire{ID: newID(t), TriggerID: api.ID, Ref: "no-host", Result: jc.FireSkipped, Reason: "no host"}
	if _, fresh, err := e.fires.Begin(ctx, pre, 0); err != nil || !fresh {
		t.Fatal(err)
	}

	// 信号：作业（两片）都结束才到期；取最差的状态。
	group := newID(t)
	mk := func(no int32) string {
		x := &biz.Execution{
			ID: newID(t), Kind: jc.KindPlaybook, TemplateID: tpl.ID, TemplateName: tpl.Name, TemplateVersion: 1, Trigger: jc.TriggerWorkflow,
			TriggerRef: api.ID, Creator: uAlice, Status: jc.StatusQueued, Spec: jc.Spec{Options: jc.Options{Port: 22}, ExtraVars: map[string]any{}},
			TimeoutS: 60, GroupID: group, SliceNo: no, SliceTotal: 2,
		}
		if err := e.txs.WithinTx(ctx, func(tx biz.Tx) error {
			_, _, err := e.executions.CreateTx(ctx, tx, []biz.NewExecution{{Execution: x, Hosts: []biz.Host{{CIID: newID(t), Name: "h", Address: "10.1.0." + string(rune('0'+no))}}}})
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return x.ID
	}
	x1, x2 := mk(1), mk(2)
	f1.ExecutionID = x1
	if err := e.fires.Complete(ctx, f1); err != nil {
		t.Fatal(err)
	}
	if due, err := e.fires.Due(ctx, 10); err != nil || len(due) != 0 {
		t.Fatalf("signal due while running: %+v, %v", due, err)
	}
	e.setStatus(x1, jc.StatusSuccess)
	if due, _ := e.fires.Due(ctx, 10); len(due) != 0 {
		t.Fatalf("signal due while a slice is still queued: %+v", due)
	}
	e.setStatus(x2, jc.StatusPartial)
	due, err := e.fires.Due(ctx, 10)
	if err != nil || len(due) != 1 || due[0].FireID != f1.ID || due[0].InstanceID != instance || due[0].Signal != "job.done" ||
		due[0].Status != jc.StatusPartial || due[0].Success || due[0].ExecutionID != x1 {
		t.Fatalf("Due() = %+v, %v", due[0], err)
	}
	// 还没人等：稍后重试；再次到期前不出现。
	if err := e.fires.Mark(ctx, f1.ID, jc.SignalPending, time.Hour); err != nil {
		t.Fatal(err)
	}
	if due, _ := e.fires.Due(ctx, 10); len(due) != 0 {
		t.Fatalf("signal due before its retry time: %+v", due)
	}
	if _, err := e.pool.Exec(ctx, "UPDATE job_trigger_fire SET signal_next_time = now() WHERE id = $1", f1.ID); err != nil {
		t.Fatal(err)
	}
	if due, _ := e.fires.Due(ctx, 10); len(due) != 1 || due[0].Attempts != 1 {
		t.Fatalf("retry not due: %+v", due)
	}
	if err := e.fires.Mark(ctx, f1.ID, jc.SignalSent, 0); err != nil {
		t.Fatal(err)
	}
	if due, _ := e.fires.Due(ctx, 10); len(due) != 0 {
		t.Fatalf("sent signal still due: %+v", due)
	}
	res := jc.FireSkipped
	fires, total, err := e.fires.List(ctx, biz.FireFilter{TriggerID: alert.ID, Result: &res}, 1, 20)
	if err != nil || total != 7 || fires[0].TriggerName != "heal" {
		t.Fatalf("List fires = %d, %v", total, err)
	}
	if all, n, _ := e.fires.List(ctx, biz.FireFilter{TriggerID: api.ID}, 1, 20); n != 2 || all[1].SignalState != jc.SignalSent || all[1].ExecutionID != x1 {
		t.Fatalf("api fires = %+v", all)
	}
	// 删除触发器：令牌抹掉，名称仍可查。
	if err := e.triggers.Write(ctx, biz.OpDelete, api, 0, uBob, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := e.triggers.Token(ctx, api.ID); !is(err, biz.ErrTriggerNotFound) {
		t.Fatalf("token survives deletion: %v", err)
	}
	if names, _ := e.triggers.SourceNames(ctx, nil, []string{api.ID}); names[api.ID] != "deploy-hook" {
		t.Fatalf("deleted trigger name = %v", names)
	}
}

// 执行结束通知的领取（并发只领一次、未结束的不领）与统计。
func TestNotifyQueueAndStats(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	tpl := e.base()
	ids := make([]string, 0, 5)
	for i := range 5 {
		_, id, _, err := e.execution(tpl, uAlice, "")
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
		_ = i
	}
	e.setStatus(ids[0], jc.StatusSuccess)
	e.setStatus(ids[1], jc.StatusFailed)
	e.setStatus(ids[2], jc.StatusRejected)
	e.setStatus(ids[3], jc.StatusPending) // 未结束：不通知
	// ids[4] 仍在排队。
	var claimed sync.Map
	var wg sync.WaitGroup
	for range 6 {
		wg.Go(func() {
			for {
				var got *biz.FinishedExecution
				if err := e.txs.WithinTx(ctx, func(tx biz.Tx) error {
					var err error
					got, err = e.stats.ClaimTx(ctx, tx)
					return err
				}); err != nil {
					t.Errorf("ClaimTx: %v", err)
					return
				}
				if got == nil {
					return
				}
				if _, dup := claimed.LoadOrStore(got.ID, got); dup {
					t.Errorf("execution %s claimed twice", got.ID)
				}
			}
		})
	}
	wg.Wait()
	n := 0
	claimed.Range(func(_, v any) bool {
		n++
		f := v.(*biz.FinishedExecution)
		if f.Creator != uAlice || f.Name != tpl.Name || f.HostTotal != 2 || f.FinishTime.IsZero() {
			t.Errorf("claimed = %+v", f)
		}
		return true
	})
	if n != 3 {
		t.Fatalf("claimed %d executions, want the 3 finished ones", n)
	}
	// 回滚的领取不算已通知。
	e.setStatus(ids[4], jc.StatusTimeout)
	_ = e.txs.WithinTx(ctx, func(tx biz.Tx) error {
		if got, err := e.stats.ClaimTx(ctx, tx); err != nil || got == nil || got.ID != ids[4] {
			t.Errorf("claim before rollback = %+v, %v", got, err)
		}
		return context.Canceled
	})
	var again *biz.FinishedExecution
	if err := e.txs.WithinTx(ctx, func(tx biz.Tx) error {
		var err error
		again, err = e.stats.ClaimTx(ctx, tx)
		return err
	}); err != nil || again == nil || again.ID != ids[4] {
		t.Fatalf("rolled-back claim was lost: %+v, %v", again, err)
	}

	if err := e.runners.Create(ctx, &biz.Runner{ID: newID(t), Name: "r", Enabled: true}, []byte("h"), ""); err != nil {
		t.Fatal(err)
	}
	st, err := e.stats.Stats(ctx, time.Now().Add(-time.Hour), "Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	if st.Total != 5 || st.Finished != 4 || st.Success != 1 || st.HostRuns != 10 || st.Active != 1 || st.RunnersTotal != 1 || st.RunnersOnline != 0 ||
		st.AvgDurationSeconds < 80 || len(st.Trend) != 1 || st.Trend[0].Total != 5 || st.Trend[0].Success != 1 || st.Trend[0].Failed != 2 ||
		len(st.TopTemplates) != 1 || st.TopTemplates[0].Count != 5 || st.TopTemplates[0].Failed != 2 || st.TopTemplates[0].Name != tpl.Name {
		t.Fatalf("Stats() = %+v", st)
	}
	byStatus := map[string]int64{}
	for _, c := range st.ByStatus {
		byStatus[c.Key] = c.Count
	}
	if byStatus["3"] != 1 || byStatus["4"] != 1 || byStatus["7"] != 1 || byStatus["9"] != 1 || byStatus["10"] != 1 || len(st.ByKind) != 1 || st.ByKind[0].Key != "1" {
		t.Fatalf("ByStatus = %v, ByKind = %+v", byStatus, st.ByKind)
	}
	if empty, err := e.stats.Stats(ctx, time.Now().Add(time.Hour), "UTC"); err != nil || empty.Total != 0 || empty.Active != 1 {
		t.Fatalf("future window = %+v, %v", empty, err)
	}
}

// 0075 的授权：core 角色能做二期 / 三期的全部写入，但仍不能改执行内容；引擎角色看不到新的配置表。
func TestPhase2Grants(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	roles := map[string]string{"job_core_itest2": "job_core_role", "job_engine_itest2": "job_engine_role"}
	for login, group := range roles {
		if _, err := e.pool.Exec(ctx, "DROP ROLE IF EXISTS "+login); err != nil {
			t.Fatal(err)
		}
		if _, err := e.pool.Exec(ctx, "CREATE ROLE "+login+" NOLOGIN IN ROLE "+group); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _, _ = e.pool.Exec(context.Background(), "DROP ROLE IF EXISTS "+login) })
	}
	as := func(role string) *pgxpool.Pool {
		cfg, err := pgxpool.ParseConfig(testDSN(t))
		if err != nil {
			t.Fatal(err)
		}
		cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
			_, err := conn.Exec(ctx, "SET ROLE "+role)
			return err
		}
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		return pool
	}
	core := reposOf(t, &basedata.Data{Pool: as("job_core_itest2")})
	tpl := core.base()
	pending := &biz.Execution{
		ID: newID(t), Kind: jc.KindFile, TemplateName: "f", Trigger: jc.TriggerManual, Creator: uAlice, Status: jc.StatusPending,
		Spec: jc.Spec{Kind: jc.KindFile, Options: jc.Options{Port: 22}, ExtraVars: map[string]any{}}, TimeoutS: 60,
	}
	if err := core.txs.WithinTx(ctx, func(tx biz.Tx) error {
		if _, _, err := core.executions.CreateTx(ctx, tx, []biz.NewExecution{{Execution: pending, Hosts: []biz.Host{{CIID: newID(t), Name: "h", Address: "10.2.0.1"}}, Payload: []byte("x")}}); err != nil {
			return err
		}
		if err := core.executions.SetApprovalTx(ctx, tx, pending.ID, newID(t)); err != nil {
			return err
		}
		_, err := core.executions.ResolveApprovalTx(ctx, tx, pending.ID, false, "no")
		return err
	}); err != nil {
		t.Fatalf("core role cannot run the approval path: %v", err)
	}
	if err := core.txs.WithinTx(ctx, func(tx biz.Tx) error {
		_, err := core.stats.ClaimTx(ctx, tx)
		return err
	}); err != nil {
		t.Fatalf("core role cannot claim a notification: %v", err)
	}
	next := time.Now()
	sch := &biz.Schedule{ID: newID(t), Name: "s", TemplateID: tpl.ID, Cron: "* * * * *", Timezone: "UTC", Misfire: 1, Enabled: true, NextTime: &next}
	trg := &biz.Trigger{ID: newID(t), Name: "t", Type: jc.TriggerTypeAPI, TemplateID: tpl.ID, Enabled: true}
	for name, err := range map[string]error{
		"script":   core.scripts.Write(ctx, biz.OpCreate, &biz.Script{ID: newID(t), Name: "s", Interpreter: "sh", Content: "x"}, 0, ""),
		"rule":     core.rules.Write(ctx, biz.OpCreate, &biz.RiskRule{ID: newID(t), Name: "r", Pattern: "x", Action: 1}, 0, ""),
		"schedule": core.schedules.Write(ctx, biz.OpCreate, sch, 0, uAlice),
		"trigger":  core.triggers.Write(ctx, biz.OpCreate, trg, 0, uAlice, &biz.TriggerToken{Hash: []byte("h")}),
	} {
		if err != nil {
			t.Errorf("core role cannot write %s: %v", name, err)
		}
	}
	if _, err := core.schedules.Advance(ctx, sch.ID, biz.ScheduleRun{Fired: next, Next: next.Add(time.Minute)}); err != nil {
		t.Errorf("core role cannot advance a schedule: %v", err)
	}
	if _, _, err := core.fires.Begin(ctx, &biz.TriggerFire{ID: newID(t), TriggerID: trg.ID, Ref: "r"}, 0); err != nil {
		t.Errorf("core role cannot record a fire: %v", err)
	}
	p, _ := core.policies.Get(ctx)
	if err := core.policies.Update(ctx, p, uAlice); err != nil {
		t.Errorf("core role cannot update the policy: %v", err)
	}
	denied := func(pool *pgxpool.Pool, what, stmt string) {
		t.Helper()
		if _, err := pool.Exec(ctx, stmt); err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Errorf("%s: expected permission denied, got %v", what, err)
		}
	}
	denied(core.pool, "core rewrites spec", "UPDATE job_execution SET spec = '{}' WHERE id = '"+pending.ID+"'")
	denied(core.pool, "core rewrites payload", "UPDATE job_execution_payload SET content = 'y'")
	denied(core.pool, "core adds a policy row", "INSERT INTO job_policy (id) VALUES (2)")
	engine := as("job_engine_itest2")
	var got []byte
	if err := engine.QueryRow(ctx, "SELECT content FROM job_execution_payload WHERE execution_id = $1", pending.ID).Scan(&got); err != nil || string(got) != "x" {
		t.Errorf("engine role cannot read the payload: %q, %v", got, err)
	}
	for _, table := range []string{"job_script", "job_risk_rule", "job_policy", "job_schedule", "job_trigger", "job_trigger_fire", "job_template"} {
		denied(engine, "engine reads "+table, "SELECT count(*) FROM "+table)
	}
	denied(engine, "engine writes payload", "INSERT INTO job_execution_payload (execution_id, content) VALUES ('"+pending.ID+"', 'z')")
}

// 过期清理：只删已结束且过期的文件内容、过期且信号不在待发送的触发记录；模板版本随定时作业 / 触发器落库。
func TestRetentionAndTemplatePin(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	tpl := e.base()
	mk := func(status int32, age string) string {
		x := &biz.Execution{
			ID: newID(t), Kind: jc.KindFile, TemplateName: "f", Trigger: jc.TriggerManual, Creator: uAlice, Status: jc.StatusQueued,
			Spec: jc.Spec{Kind: jc.KindFile, Options: jc.Options{Port: 22}, ExtraVars: map[string]any{}}, TimeoutS: 60,
		}
		if err := e.txs.WithinTx(ctx, func(tx biz.Tx) error {
			_, _, err := e.executions.CreateTx(ctx, tx, []biz.NewExecution{{Execution: x, Hosts: []biz.Host{{CIID: newID(t), Name: "h", Address: "10.3.0.1"}}, Payload: []byte("x")}})
			return err
		}); err != nil {
			t.Fatal(err)
		}
		e.setStatus(x.ID, status)
		if age != "" {
			if _, err := e.pool.Exec(ctx, "UPDATE job_execution SET finish_time = now() - $2::interval WHERE id = $1", x.ID, age); err != nil {
				t.Fatal(err)
			}
		}
		return x.ID
	}
	old, recent, running := mk(jc.StatusSuccess, "40 days"), mk(jc.StatusSuccess, "1 day"), mk(jc.StatusRunning, "")
	trg := &biz.Trigger{ID: newID(t), Name: "t", Type: jc.TriggerTypeAPI, TemplateID: tpl.ID, TemplateVersion: 7, Enabled: true}
	if err := e.triggers.Write(ctx, biz.OpCreate, trg, 0, uAlice, &biz.TriggerToken{Hash: []byte("h")}); err != nil {
		t.Fatal(err)
	}
	for ref, state := range map[string]int32{"old-done": jc.SignalNone, "old-waiting": jc.SignalPending, "new": jc.SignalNone} {
		f := &biz.TriggerFire{ID: newID(t), TriggerID: trg.ID, Ref: ref, Result: jc.FireSkipped, SignalState: state, Signal: "s"}
		if _, _, err := e.fires.Begin(ctx, f, 0); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.pool.Exec(ctx, "UPDATE job_trigger_fire SET create_time = now() - interval '200 days' WHERE ref LIKE 'old-%'"); err != nil {
		t.Fatal(err)
	}
	n, err := e.stats.Purge(ctx, 30*24*time.Hour, 180*24*time.Hour, 100)
	if err != nil || n != 1 {
		t.Fatalf("Purge() = %d, %v", n, err)
	}
	left := map[string]bool{}
	rows, _ := e.pool.Query(ctx, "SELECT execution_id::text FROM job_execution_payload")
	for rows.Next() {
		var id string
		_ = rows.Scan(&id)
		left[id] = true
	}
	rows.Close()
	if left[old] || !left[recent] || !left[running] {
		t.Fatalf("payloads after purge: old=%v recent=%v running=%v", left[old], left[recent], left[running])
	}
	var refs string
	if err := e.pool.QueryRow(ctx, "SELECT string_agg(ref, ',' ORDER BY ref) FROM job_trigger_fire").Scan(&refs); err != nil || refs != "new,old-waiting" {
		t.Fatalf("fires after purge = %q, %v", refs, err)
	}
	if n, err := e.stats.Purge(ctx, 30*24*time.Hour, 180*24*time.Hour, 100); err != nil || n != 0 {
		t.Fatalf("second Purge() = %d, %v", n, err)
	}

	// 模板版本落库；重置令牌的人成为负责人。
	if got, _ := e.triggers.Get(ctx, trg.ID); got.TemplateVersion != 7 || got.Owner != uAlice {
		t.Fatalf("trigger = %+v", got)
	}
	if err := e.triggers.SetToken(ctx, trg.ID, biz.TriggerToken{Hash: []byte("h2")}, uBob); err != nil {
		t.Fatal(err)
	}
	if got, _ := e.triggers.Get(ctx, trg.ID); got.Owner != uBob {
		t.Fatalf("owner after token reset = %s", got.Owner)
	}
	next := time.Now().Add(time.Hour)
	sch := &biz.Schedule{ID: newID(t), Name: "s", TemplateID: tpl.ID, TemplateVersion: 3, Cron: "* * * * *", Timezone: "UTC", Misfire: 1, Enabled: true, NextTime: &next}
	if err := e.schedules.Write(ctx, biz.OpCreate, sch, 0, uAlice); err != nil {
		t.Fatal(err)
	}
	sch.TemplateVersion = 4
	if err := e.schedules.Write(ctx, biz.OpUpdate, sch, 1, uBob); err != nil {
		t.Fatal(err)
	}
	if got, _ := e.schedules.Get(ctx, sch.ID); got.TemplateVersion != 4 || got.Owner != uBob {
		t.Fatalf("schedule = %+v", got)
	}
}

// 结果未知（节点失联）的执行先观察 5 分钟：期间不通知、不回发流程信号——节点恢复后会有真实的结果。
func TestUnknownStatusIsHeldBack(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	tpl := e.base()
	_, id, _, err := e.execution(tpl, uAlice, "")
	if err != nil {
		t.Fatal(err)
	}
	e.setStatus(id, jc.StatusUnknown)
	trg := &biz.Trigger{ID: newID(t), Name: "t", Type: jc.TriggerTypeAPI, TemplateID: tpl.ID, Signal: "s", Enabled: true}
	if err := e.triggers.Write(ctx, biz.OpCreate, trg, 0, uAlice, &biz.TriggerToken{Hash: []byte("h")}); err != nil {
		t.Fatal(err)
	}
	f := &biz.TriggerFire{ID: newID(t), TriggerID: trg.ID, Ref: "r", WorkflowInstanceID: newID(t), Signal: "s", SignalState: jc.SignalPending}
	if _, _, err := e.fires.Begin(ctx, f, 0); err != nil {
		t.Fatal(err)
	}
	f.ExecutionID = id
	if err := e.fires.Complete(ctx, f); err != nil {
		t.Fatal(err)
	}
	claim := func() *biz.FinishedExecution {
		var got *biz.FinishedExecution
		if err := e.txs.WithinTx(ctx, func(tx biz.Tx) error {
			var err error
			got, err = e.stats.ClaimTx(ctx, tx)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return got
	}
	if got := claim(); got != nil {
		t.Fatalf("fresh unknown execution was notified: %+v", got)
	}
	if due, err := e.fires.Due(ctx, 10); err != nil || len(due) != 0 {
		t.Fatalf("signal due for a fresh unknown execution: %+v, %v", due, err)
	}
	if _, err := e.pool.Exec(ctx, "UPDATE job_execution SET finish_time = now() - interval '6 minutes' WHERE id = $1", id); err != nil {
		t.Fatal(err)
	}
	if due, _ := e.fires.Due(ctx, 10); len(due) != 1 || due[0].Status != jc.StatusUnknown || due[0].Success {
		t.Fatalf("signal not due after the grace period: %+v", due)
	}
	if got := claim(); got == nil || got.ID != id || got.Status != jc.StatusUnknown {
		t.Fatalf("unknown execution not notified after the grace period: %+v", got)
	}
}
