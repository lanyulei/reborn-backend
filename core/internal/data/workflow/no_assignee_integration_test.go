//go:build integration

package data

import (
	"context"
	"slices"
	"strings"
	"testing"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"

	"github.com/go-kratos/kratos/v3/errors"
)

// blockedTask 解析不出处理人的人工任务：规则指向已停用的 uGone。policy 是 onEmpty 的原文（含前导
// 逗号；空 = 缺省）。
func blockedTask(id, policy string) string {
	return `{"id":"` + id + `","type":"userTask","name":"` + id + `","assignee":{"rules":[{"kind":"user","userIds":["` + uGone + `"]}]` + policy + `}}`
}

// todoOf 用户在某个实例上待处理任务的 ID。
func (e *env) todoOf(instanceID, user string) string {
	e.t.Helper()
	for _, item := range e.todo(user) {
		if item.InstanceID == instanceID {
			return item.TaskID
		}
	}
	e.t.Fatalf("no pending task of %s on instance %s", user, instanceID)
	return ""
}

// approversOf 实例详情里停在某个节点上、当前可处理的用户。
func approversOf(detail *biz.InstanceDetail, node string) []string {
	var users []string
	for _, t := range detail.State.Tasks {
		if t.NodeID != node || t.Status != biz.TaskPending {
			continue
		}
		for _, a := range t.Assignees {
			if a.Status == biz.AssigneePending {
				users = append(users, a.UserID)
			}
		}
	}
	return users
}

// 人工任务解析不出处理人、且没有明确选择跳过（没写 onEmpty 的缺省、onEmpty = block，或转交流程管理员而没有
// 可用的管理员）时流程被拦住，不会「自动通过」：发起失败且什么都不落库；同意不生效，上一个任务仍待处理；
// 由定时、信号驱动的推进失败后可以重来（定时作业直接转死信并通知流程管理员，管理员的「跳过等待节点」也
// 过不去）。组织数据修好后流程继续。选了转交流程管理员却没有流程管理员的草稿在发布这一步就拦住；此前
// 已发布的版本照常能执行，运行时同样被拦住。
func TestIntegration_NoAssigneeBlocked(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	start := func(key string) (*biz.Instance, error) {
		return e.rt.Start(ctx, biz.StartParams{DefinitionKey: key, Title: "t-" + key, Initiator: uInit})
	}
	detailOf := func(instanceID string) *biz.InstanceDetail {
		t.Helper()
		d, err := e.rt.Get(ctx, instanceID, uInit, false)
		if err != nil {
			t.Fatalf("Get() error = %v", err)
		}
		return d
	}
	// wantBlocked 断言 err 是「人工任务解析不出处理人被拦住」，说明里带节点名。
	wantBlocked := func(err error, node string) {
		t.Helper()
		var ke *errors.Error
		if !errors.Is(err, biz.ErrNoAssignee) || !errors.As(err, &ke) || !strings.Contains(ke.Message, node) {
			t.Fatalf("error = %v, want WORKFLOW_NO_ASSIGNEE naming node %s", err, node)
		}
	}
	const block = `,"onEmpty":"block"`

	// 发布这一步：选了转交流程管理员却没有流程管理员。
	orphan := `{"nodes":[{"id":"start","type":"start"},` + blockedTask("a", `,"onEmpty":"admin"`) + `,{"id":"end","type":"end"}],
		"edges":[{"id":"e1","source":"start","target":"a"},{"id":"e2","source":"a","target":"end"}],"settings":{}}`
	d, err := e.defs.Create(ctx, &biz.Definition{Key: "orphan_admin", Name: "流程-orphan_admin"})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	rev, issues, err := e.defs.SaveDraft(ctx, d.ID, []byte(orphan), d.Revision)
	if err != nil || len(issues) != 1 || issues[0].Code != "ADMINS_REQUIRED" || issues[0].NodeID != "a" {
		t.Fatalf("SaveDraft() = rev %d, issues %+v, err %v; want one ADMINS_REQUIRED issue on node a", rev, issues, err)
	}
	if _, err := e.defs.Publish(ctx, d.ID, rev); !errors.Is(err, biz.ErrDefinitionInvalid) {
		t.Fatalf("Publish() error = %v, want ErrDefinitionInvalid", err)
	}
	if _, err := e.defs.Simulate(ctx, d.ID, []byte(orphan), biz.Instance{Initiator: uInit}, e.dir, usersAdapter{e.dir}, nil, nil); !errors.Is(err, biz.ErrDefinitionInvalid) {
		t.Fatalf("Simulate() error = %v, want ErrDefinitionInvalid", err)
	}
	// 补上流程管理员就能发布；处理人为空时兜底到流程管理员。
	fixed := strings.Replace(orphan, `"settings":{}`, `"settings":{"admins":["`+uBoss+`"]}`, 1)
	if rev, issues, err = e.defs.SaveDraft(ctx, d.ID, []byte(fixed), rev); err != nil || len(issues) != 0 {
		t.Fatalf("SaveDraft(fixed) = rev %d, issues %+v, err %v", rev, issues, err)
	}
	if _, err := e.defs.Publish(ctx, d.ID, rev); err != nil {
		t.Fatalf("Publish(fixed) error = %v", err)
	}
	handed, err := start("orphan_admin")
	if err != nil {
		t.Fatalf("Start(orphan_admin) error = %v", err)
	}
	if got := approversOf(detailOf(handed.ID), "a"); !slices.Equal(got, []string{uBoss}) {
		t.Fatalf("assignees of node a = %v, want the workflow admin", got)
	}

	// 这条规则之前发布的版本（转交流程管理员却没有流程管理员）：详情页提示问题、原样再发布会被拦住，
	// 但版本照常能执行——运行时处理人为空就拦在那里。
	legacy := e.publish("legacy_admin", line(blockedTask("a", `,"onEmpty":"admin"`)))
	for _, q := range []string{
		`UPDATE workflow_definition_version SET graph = graph #- '{settings,admins}' WHERE definition_id = $1`,
		`UPDATE workflow_definition SET draft = draft #- '{settings,admins}' WHERE id = $1`,
	} {
		if _, err := e.d.Pool.Exec(ctx, q, legacy.ID); err != nil {
			t.Fatalf("strip the workflow admins like a version published before this rule existed: %v", err)
		}
	}
	if _, issues, err := e.defs.Get(ctx, legacy.ID); err != nil ||
		!slices.ContainsFunc(issues, func(i biz.Issue) bool { return i.Code == "ADMINS_REQUIRED" }) {
		t.Fatalf("Get(legacy) = issues %+v, err %v; want the missing workflow admins reported", issues, err)
	}
	if _, err := start("legacy_admin"); err != nil {
		wantBlocked(err, `"a"`)
	} else {
		t.Fatal("Start(legacy_admin) succeeded; want the flow held at the first task")
	}

	// 发起：第一个节点就找不到处理人（没写 onEmpty，取缺省）——发起失败，什么都不落库。
	events := e.count("SELECT count(*) FROM notify_event")
	e.publish("blocked_start", line(`{"id":"a","type":"userTask","assignee":{"rules":[{"kind":"user","userIds":["`+uGone+`"]}]}}`))
	if _, err := start("blocked_start"); err != nil {
		wantBlocked(err, `"a"`)
	} else {
		t.Fatal("Start(blocked_start) succeeded; want the flow held at the first task")
	}
	if n := e.count("SELECT count(*) FROM workflow_instance WHERE definition_key = 'blocked_start'"); n != 0 {
		t.Fatalf("a blocked start left %d instances behind", n)
	}
	if n := e.count("SELECT count(*) FROM notify_event"); n != events {
		t.Fatalf("a blocked start emitted %d notifications", n-events)
	}

	// 同意：下一个节点找不到处理人——这次同意不生效，上一个任务仍在待办里，也不留同意 / 跳过的记录。
	e.publish("blocked_approve", line(task("a", uOps1), blockedTask("b", block)))
	in, err := start("blocked_approve")
	if err != nil {
		t.Fatalf("Start(blocked_approve) error = %v", err)
	}
	if _, err := e.rt.Approve(ctx, e.todoOf(in.ID, uOps1), uOps1, "ok"); err != nil {
		wantBlocked(err, `"b"`)
	} else {
		t.Fatal("Approve() succeeded; want it to fail because the next task has no assignee")
	}
	if len(e.todo(uOps1)) != 1 {
		t.Fatalf("todo(ops1) = %d items after a failed approval, want 1", len(e.todo(uOps1)))
	}
	if n := e.count("SELECT count(*) FROM workflow_history WHERE instance_id = $1 AND action IN ('approve', 'skip')", in.ID); n != 0 {
		t.Fatalf("a failed approval left %d approve / skip entries behind", n)
	}

	// 下一个节点是子流程、子流程的第一个任务就被拦住：父流程那一次同意同样失败（同一事务回滚），
	// 不会留下发起了一半的子流程。
	e.publish("blocked_child", line(blockedTask("c", block)))
	e.publish("blocked_parent", line(task("a", uOps2), `{"id":"sub","type":"subProcess","subProcess":{"definitionKey":"blocked_child"}}`))
	if in, err = start("blocked_parent"); err != nil {
		t.Fatalf("Start(blocked_parent) error = %v", err)
	}
	if _, err := e.rt.Approve(ctx, e.todoOf(in.ID, uOps2), uOps2, "ok"); err != nil {
		wantBlocked(err, `"c"`)
	} else {
		t.Fatal("Approve() succeeded; want it to fail because the sub-process is held at its first task")
	}
	if n := e.count("SELECT count(*) FROM workflow_instance WHERE parent_instance_id = $1", in.ID); n != 0 {
		t.Fatalf("a failed approval left %d sub-process instances behind", n)
	}
	if len(e.todo(uOps2)) != 1 {
		t.Fatalf("todo(ops2) = %d items after a failed approval, want 1", len(e.todo(uOps2)))
	}

	// 模拟运行：标出被拦住的节点，走到它为止。
	graph := line(task("a", uOps1), blockedTask("b", block))
	res, err := e.defs.Simulate(ctx, e.publish("sim_blocked", graph).ID, []byte(graph), biz.Instance{Initiator: uInit}, e.dir,
		usersAdapter{e.dir}, nil, nil)
	if err != nil {
		t.Fatalf("Simulate() error = %v", err)
	}
	if last := res.Steps[len(res.Steps)-1]; res.Status != biz.InstanceRunning || last.NodeID != "b" || !last.Blocked || last.Skipped {
		t.Fatalf("Simulate() = %+v; want the flow held at b", res.Steps)
	}

	// 定时到期后才走到被拦的节点：作业一次即转死信（记历史、通知流程管理员），不反复重试；管理员的
	// 「跳过等待节点」也过不去。
	e.publish("blocked_timer", line(task("a", uOps1),
		`{"id":"wait","type":"timer","timer":{"mode":"delay","duration":{"value":1,"unit":"minute"}}}`, blockedTask("b", block)))
	if in, err = start("blocked_timer"); err != nil {
		t.Fatalf("Start(blocked_timer) error = %v", err)
	}
	if _, err := e.rt.Approve(ctx, e.todoOf(in.ID, uOps1), uOps1, "ok"); err != nil {
		t.Fatalf("Approve(blocked_timer) error = %v", err)
	}
	e.due(in.ID)
	e.run()
	detail := detailOf(in.ID)
	if len(detail.State.Jobs) != 1 || detail.State.Jobs[0].Status != biz.JobDead || detail.State.Jobs[0].Attempts != 1 {
		t.Fatalf("after the timer fired: jobs = %+v; want the timer job dead-lettered at once", detail.State.Jobs)
	}
	job := detail.State.Jobs[0]
	if !strings.Contains(job.LastError, `"b"`) {
		t.Fatalf("last_error = %q, want it to name the blocked node", job.LastError)
	}
	if n := e.count("SELECT count(*) FROM workflow_history WHERE instance_id = $1 AND action = 'serviceFailed'", in.ID); n != 1 {
		t.Fatalf("dead-letter history entries = %d, want 1", n)
	}
	if n := e.count("SELECT count(*) FROM notify_event WHERE event_type = $1", biz.EventServiceFailed); n != 1 {
		t.Fatalf("notifications to the workflow admins = %d, want 1", n)
	}
	if _, err := e.rt.SkipJob(ctx, e.jobs, in.ID, job.ID, uBoss, "skip it"); err != nil {
		wantBlocked(err, `"b"`)
	} else {
		t.Fatal("SkipJob() succeeded; want the flow held at the blocked task")
	}
	timerIn := in

	// 等信号的节点之后才是被拦的节点：信号投递同样失败，信号不被消费。
	e.publish("blocked_signal", line(task("a", uOps2),
		`{"id":"wait","type":"timer","timer":{"mode":"signal","signal":"paid"}}`, blockedTask("b", block)))
	signalIn, err := start("blocked_signal")
	if err != nil {
		t.Fatalf("Start(blocked_signal) error = %v", err)
	}
	if _, err := e.rt.Approve(ctx, e.todoOf(signalIn.ID, uOps2), uOps2, "ok"); err != nil {
		t.Fatalf("Approve(blocked_signal) error = %v", err)
	}
	if _, err := e.rt.Signal(ctx, signalIn.ID, "paid", nil); err != nil {
		wantBlocked(err, `"b"`)
	} else {
		t.Fatal("Signal() succeeded; want the flow held at the blocked task")
	}

	// 组织数据修好（uGone 恢复可用）：定时作业重试、信号重新投递，两个流程都继续走到 b。
	if _, err := e.d.Pool.Exec(ctx, `UPDATE "system_user" SET status = 1 WHERE id = $1`, uGone); err != nil {
		t.Fatalf("make the assignee available again: %v", err)
	}
	if err := e.rt.RetryJob(ctx, e.jobs, timerIn.ID, job.ID); err != nil {
		t.Fatalf("RetryJob() error = %v", err)
	}
	e.run()
	if _, err := e.rt.Signal(ctx, signalIn.ID, "paid", nil); err != nil {
		t.Fatalf("Signal(after the fix) error = %v", err)
	}
	for _, id := range []string{timerIn.ID, signalIn.ID} {
		if got := approversOf(detailOf(id), "b"); !slices.Equal(got, []string{uGone}) {
			t.Fatalf("assignees of node b on %s = %v, want the assignee that became available", id, got)
		}
	}
}
