//go:build integration

package data

import (
	"context"
	"testing"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/ticket"
	bizworkflow "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"
)

// syncChildGraph 子流程：一个审批环节，金额必填。
const syncChildGraph = `{"nodes":[
	{"id":"start","type":"start"},
	{"id":"c1","type":"userTask","name":"子流程审批","assignee":{"rules":[{"kind":"user","userIds":["` + uLead + `"]}]},
	 "formPermissions":{"amount":"required"}},
	{"id":"end","type":"end"}],
	"edges":[{"id":"e1","source":"start","target":"c1"},{"id":"e2","source":"c1","target":"end"}],"settings":{}}`

// syncParentGraph 父流程：并行（子流程 ‖ 会审，会审可改原因）→ 按金额分支：大额走主管，否则直接结束。
const syncParentGraph = `{"nodes":[
	{"id":"start","type":"start"},
	{"id":"fork","type":"parallelGateway","join":"join"},
	{"id":"sub","type":"subProcess","name":"子流程","subProcess":{"definitionKey":"sync_child"}},
	{"id":"p1","type":"userTask","name":"会审","assignee":{"rules":[{"kind":"user","userIds":["` + uLead + `","` + uOther + `"]}]},
	 "formPermissions":{"reason":"editable"}},
	{"id":"join","type":"parallelGateway"},
	{"id":"gw","type":"exclusiveGateway"},
	{"id":"big","type":"userTask","name":"大额审批","assignee":{"rules":[{"kind":"user","userIds":["` + uSuper + `"]}]}},
	{"id":"merge","type":"exclusiveGateway"},
	{"id":"end","type":"end"}],
	"edges":[{"id":"e1","source":"start","target":"fork"},{"id":"e2","source":"fork","target":"sub"},{"id":"e3","source":"fork","target":"p1"},
	{"id":"e4","source":"sub","target":"join"},{"id":"e5","source":"p1","target":"join"},{"id":"e6","source":"join","target":"gw"},
	{"id":"e7","source":"gw","target":"big","condition":"form.amount > 5000","priority":1},
	{"id":"e8","source":"gw","target":"merge","isDefault":true},
	{"id":"e9","source":"big","target":"merge"},{"id":"e10","source":"merge","target":"end"}],"settings":{}}`

// 父子实例各有一份流程变量，工单数据只有一份：在任何一个实例上改了表单，树上其它实例在自己的
// 下一条命令里都按工单校准——子流程里改的金额决定父流程的分支，父流程上改的原因子流程也看得到。
// 同时在父、子流程上都有待办的人，字段权限取两处的并集，一次就能改两处允许的字段。
func TestIntegration_FormEditsReachTheWholeTree(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.publish("sync_child", syncChildGraph)
	e.publish("sync_parent", syncParentGraph)
	tpl := e.template("树同步", "sync_parent")
	tk, err := e.uc.Create(ctx, biz.CreateParams{
		TemplateID: tpl.ID, Title: "t", Priority: 2, Creator: uInit, Data: map[string]any{"reason": "old", "amount": float64(100)},
	})
	if err != nil {
		t.Fatal(err)
	}
	variable := func(where, name string) string {
		t.Helper()
		var v *string
		if err := e.d.Pool.QueryRow(ctx, "SELECT variables->>$2 FROM workflow_instance WHERE root_id = $1 AND "+where, tk.InstanceID, name).Scan(&v); err != nil {
			t.Fatalf("read variable %s: %v", name, err)
		}
		if v == nil {
			return ""
		}
		return *v
	}
	const root, child = "parent_instance_id IS NULL", "parent_instance_id IS NOT NULL"

	// 只在父流程上有待办的人改原因：锁的是父实例。
	if err := e.uc.UpdateForm(ctx, tk.ID, uOther, map[string]any{"reason": "new"}); err != nil {
		t.Fatalf("UpdateForm(root handler) error = %v", err)
	}
	if got := variable(root, "reason"); got != "new" {
		t.Fatalf("root reason = %q, want new", got)
	}
	// 父、子流程上都有待办的人：权限是两处的并集。
	d, err := e.uc.Get(ctx, tk.ID, uLead)
	if err != nil || !d.CanEditForm || d.Permissions["amount"] != "required" || d.Permissions["reason"] != "editable" {
		t.Fatalf("Get(handler on both) = perms %v canEdit %v, err %v", d.Permissions, d.CanEditForm, err)
	}
	if err := e.uc.UpdateForm(ctx, tk.ID, uLead, map[string]any{"amount": float64(9000), "reason": "newer"}); err != nil {
		t.Fatalf("UpdateForm(handler on both) error = %v", err)
	}
	// 同意子流程的任务：子实例在这条命令里按工单校准（无论上一步锁的是哪一个实例）。
	todo, _, _, err := e.rt.ListTasks(ctx, uLead, bizworkflow.TaskViewTodo, "", biz.BusinessType, 1, 10)
	if err != nil || len(todo) != 2 {
		t.Fatalf("todo(handler on both) = %d, err %v; want 2", len(todo), err)
	}
	for _, item := range todo {
		if item.InstanceID == tk.InstanceID {
			continue
		}
		if _, err := e.rt.Approve(ctx, item.TaskID, uLead, ""); err != nil {
			t.Fatalf("Approve(child task) error = %v", err)
		}
	}
	if r, a := variable(child, "reason"), variable(child, "amount"); r != "newer" || a != "9000" {
		t.Fatalf("child variables = reason %q amount %q, want newer / 9000", r, a)
	}
	// 子流程结束 → 唤醒父流程；会审通过后按金额走大额分支。
	if _, err := e.jobs.RunDue(ctx); err != nil {
		t.Fatalf("RunDue() error = %v", err)
	}
	for _, item := range todo {
		if item.InstanceID != tk.InstanceID {
			continue
		}
		if _, err := e.rt.Approve(ctx, item.TaskID, uLead, ""); err != nil {
			t.Fatalf("Approve(root task) error = %v", err)
		}
	}
	if r, a := variable(root, "reason"), variable(root, "amount"); r != "newer" || a != "9000" {
		t.Fatalf("root variables = reason %q amount %q, want newer / 9000", r, a)
	}
	big, _, _, err := e.rt.ListTasks(ctx, uSuper, bizworkflow.TaskViewTodo, "", biz.BusinessType, 1, 10)
	if err != nil || len(big) != 1 || big[0].NodeName != "大额审批" {
		t.Fatalf("todo(supervisor) = %+v, err %v; want the big-amount approval (the gateway must see the amount edited in the child)", big, err)
	}
}

// 幂等重放先于资源解析：首次提单成功后资源已不可访问，重试仍拿回原来那张工单。
func TestIntegration_ReplayBeforeResources(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	tpl := e.template("重放", flowKey)
	if _, err := e.d.Pool.Exec(ctx, "UPDATE ticket_template SET ci_link = 1 WHERE id = $1", tpl.ID); err != nil {
		t.Fatal(err)
	}
	p := biz.CreateParams{
		TemplateID: tpl.ID, Title: "t", Priority: 2, Creator: uInit, Data: map[string]any{"reason": "r"},
		ResourceIDs: []string{ciOK1}, RequestID: "replay-1",
	}
	first, err := e.uc.Create(ctx, p)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	p.ResourceIDs = []string{ciHidden} // 重试时这项资源已解析不出来
	again, err := e.uc.Create(ctx, p)
	if err != nil || again.ID != first.ID {
		t.Fatalf("Create(replay) = %v, err %v; want the original ticket %s", again, err, first.ID)
	}
}
