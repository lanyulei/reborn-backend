//go:build integration

package data

import (
	"bytes"
	"context"
	"slices"
	"testing"

	topbiz "github.com/lanyulei/reborn/backend/core/internal/biz"
	bizjob "github.com/lanyulei/reborn/backend/core/internal/biz/job"
	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	bizsystem "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	datanotify "github.com/lanyulei/reborn/backend/core/internal/data/notify"
	datasystem "github.com/lanyulei/reborn/backend/core/internal/data/system"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"

	"github.com/go-kratos/kratos/v3/errors"
)

// 迁移 0095 预置的流程标识，以及新装机默认接线引用的两个容器平台模板。
var (
	builtinFlowKeys = []string{
		"builtin_approval_admin", "builtin_approval_leader", "builtin_request_admin",
		"builtin_request_leader", "builtin_task_handle", "builtin_event_handle",
	}
	builtinNamespaceTemplateID  = "018f3e1a-0ce4-7000-8000-000000000011"
	builtinPermissionTemplateID = "018f3e1a-0ce4-7000-8000-000000000012"
)

// 迁移 0095 预置的内置流程在当前代码下合法、可发起、能在设计器里编辑，并且不能删除。
func TestIntegration_BuiltinFlows(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	if _, err := e.d.Pool.Exec(ctx, testfake.BuiltinSeedSQL(t)); err != nil {
		t.Fatalf("seed the built-in flows: %v", err)
	}
	// uBoss 是超管：内置流程的审批人都是「系统管理员」规则。
	if _, err := e.d.Pool.Exec(ctx, `UPDATE "system_user" SET is_admin = true WHERE id = $1`, uBoss); err != nil {
		t.Fatal(err)
	}
	admins := bizsystem.NewAdminDirectory(datasystem.NewDirectoryRepo(e.d))

	for _, key := range builtinFlowKeys {
		items, _, err := e.defs.List(ctx, biz.DefinitionFilter{Keyword: key}, 1, 10)
		if err != nil {
			t.Fatalf("List(%s) error = %v", key, err)
		}
		var d *biz.Definition
		for _, it := range items {
			if it.Key == key {
				d = it
			}
		}
		if d == nil || !d.Builtin || d.Status != biz.DefinitionEnabled || d.PublishedVersion != 1 {
			t.Fatalf("%s: definition = %+v; want built-in, enabled and published", key, d)
		}
		v, err := e.defs.GetVersion(ctx, d.ID, 1)
		if err != nil {
			t.Fatalf("%s: GetVersion() error = %v", key, err)
		}
		g, err := biz.ParseGraph(v.Graph)
		if err != nil {
			t.Fatalf("%s: the published graph is not valid JSON: %v", key, err)
		}
		if issues := g.Validate(); len(issues) > 0 {
			t.Fatalf("%s: the published graph fails validation: %+v", key, issues)
		}
		// 随图保存的设计器结构：没有它，内置流程就不能可视化编辑。
		if !bytes.Contains(v.Graph, []byte(`"design":`)) {
			t.Fatalf("%s: the published graph carries no designer structure", key)
		}
		// 模拟运行：每个审批 / 办理节点都落到超管，没有被跳过、也没有被拦住。
		res, err := e.defs.Simulate(ctx, d.ID, v.Graph, biz.Instance{Initiator: uInit}, e.dir, usersAdapter{e.dir}, nil, admins)
		if err != nil {
			t.Fatalf("%s: Simulate() error = %v", key, err)
		}
		if res.Status != biz.InstanceCompleted {
			t.Fatalf("%s: simulated status = %d, steps %+v; want completed", key, res.Status, res.Steps)
		}
		for _, st := range res.Steps {
			if st.Type != "userTask" {
				continue
			}
			if st.Blocked || st.Skipped || !slices.Contains(st.Assignees, uBoss) {
				t.Fatalf("%s: step %s = %+v; want the super admin among the assignees", key, st.NodeID, st)
			}
		}
		// 内置流程可以修改、停用，不能删除。
		if err := e.defs.Delete(ctx, d.ID); !errors.Is(err, biz.ErrDefinitionBuiltin) {
			t.Fatalf("%s: Delete() error = %v, want ErrDefinitionBuiltin", key, err)
		}
	}

	// 真实发起：审批落在超管的待办里，同意后结束。夹具的运行时没有接系统管理员查询，这里按组合根另建一个。
	instRepo := NewInstanceRepo(e.d)
	rt := biz.NewRuntimeUsecase(e.defs, instRepo, instRepo, instRepo, e.dir, usersAdapter{e.dir}, e.cfg, biz.NewCalendarUsecase(e.cfg),
		basedata.NewTxManager(e.d), biznotify.NewNotificationUsecase(datanotify.NewEventRepo(e.d), nil), nil, nil, nil, admins)
	in, err := rt.Start(ctx, biz.StartParams{DefinitionKey: "builtin_approval_admin", Title: "开通账号", Initiator: uInit})
	if err != nil {
		t.Fatalf("Start(builtin_approval_admin) error = %v", err)
	}
	todo := e.todo(uBoss)
	if len(todo) != 1 || todo[0].InstanceID != in.ID {
		t.Fatalf("todo(boss) = %+v; want the approval of %s", todo, in.ID)
	}
	if _, err := rt.Approve(ctx, todo[0].TaskID, uBoss, "同意"); err != nil {
		t.Fatalf("Approve() error = %v", err)
	}
	if status := e.count("SELECT status FROM workflow_instance WHERE id = $1", in.ID); status != int(biz.InstanceCompleted) {
		t.Fatalf("status after approval = %d, want completed", status)
	}
	// 没有可用的超管：缺省拦住，不会自动通过。
	if _, err := e.d.Pool.Exec(ctx, `UPDATE "system_user" SET is_admin = false WHERE id = $1`, uBoss); err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Start(ctx, biz.StartParams{DefinitionKey: "builtin_approval_admin", Title: "再开一个", Initiator: uInit}); !errors.Is(err, biz.ErrNoAssignee) {
		t.Fatalf("Start() without a super admin error = %v, want ErrNoAssignee", err)
	}
}

// approvalRecorder 记录审批结果的执行仓储桩。
type approvalRecorder struct {
	bizjob.ExecutionWriter
	got []bool
	why []string
}

func (r *approvalRecorder) ResolveApprovalTx(_ context.Context, _ bizjob.Tx, _ string, approved bool, reason string) (int64, error) {
	r.got, r.why = append(r.got, approved), append(r.why, reason)
	return 1, nil
}

// 作业审批：「流程走完」不等于有人审批过。审批环节被自动跳过、又没有人点过同意时不批准；流程本来没有
// 审批环节、或确实有人同意（包括只有一名管理员时自己同意）时照常批准。
func TestIntegration_JobApprovalNeedsAHumanApproval(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	txs := basedata.NewTxManager(e.d)
	evidence := topbiz.NewJobApprovalEvidence(NewInstanceRepo(e.d))
	approvals := topbiz.NewJobApprovals(e.rt, e.defs, evidence)
	e.publish("job_skip", line(`{"id":"approve","type":"userTask","name":"审批","assignee":{"rules":[{"kind":"user","userIds":["`+uGone+`"]}],"onEmpty":"skip"}}`))
	e.publish("job_auto", line())
	e.publish("job_review", line(task("approve", uBoss)))
	start := func(key, businessID, initiator string) bizjob.ApprovalOutcome {
		t.Helper()
		var out bizjob.ApprovalOutcome
		if err := txs.WithinTx(ctx, func(tx txrun.Tx) error {
			var err error
			out, err = approvals.StartTx(ctx, tx, bizjob.ApprovalStart{DefinitionKey: key, BusinessID: businessID, Initiator: initiator})
			return err
		}); err != nil {
			t.Fatalf("StartTx(%s) error = %v", key, err)
		}
		return out
	}

	// 审批环节被跳过：发起即结束，但没有人审批过，不算通过。
	if out := start("job_skip", "exec-skip", uInit); !out.Finished || out.Approved || out.Reason != bizjob.MsgApprovalUnreviewed {
		t.Fatalf("a skipped approval: outcome = %+v; want finished, not approved, with the reason", out)
	}
	// 流程本来就没有审批环节：维持原行为。
	if out := start("job_auto", "exec-auto", uInit); !out.Finished || !out.Approved || out.Reason != "" {
		t.Fatalf("a flow without approvals: outcome = %+v; want finished and approved", out)
	}
	// 需要人审批：先等审批；别人同意后通过。
	if out := start("job_review", "exec-others", uInit); out.Finished {
		t.Fatalf("a flow waiting for approval: outcome = %+v; want not finished", out)
	}
	e.approveTodo(uBoss)
	// 只有提交人自己（他也是审批人）：没有被跳过的环节，同样算审批过。
	if out := start("job_review", "exec-self", uBoss); out.Finished {
		t.Fatalf("a flow waiting for approval: outcome = %+v; want not finished", out)
	}
	e.approveTodo(uBoss)

	// 流程结束时作业侧落结果走的同一口径。
	rec := &approvalRecorder{}
	syncer := bizjob.NewApprovalSyncer(rec, evidence)
	for _, c := range []struct {
		id, submitter string
	}{
		{"exec-skip", uInit}, {"exec-auto", uInit}, {"exec-others", uInit}, {"exec-self", uBoss},
	} {
		if err := txs.WithinTx(ctx, func(tx txrun.Tx) error {
			return syncer.Resolve(ctx, tx, c.id, true, c.submitter, "")
		}); err != nil {
			t.Fatalf("Resolve(%s) error = %v", c.id, err)
		}
	}
	if want := []bool{false, true, true, true}; !slices.Equal(rec.got, want) {
		t.Fatalf("approved = %v, reasons %q; want %v", rec.got, rec.why, want)
	}
	if rec.why[0] != bizjob.MsgApprovalUnreviewed || rec.why[1] != "" {
		t.Fatalf("reasons = %q; want the unreviewed reason first and the default elsewhere", rec.why)
	}
}

// 迁移 0095 的默认接线只对「库里只有预置的超管一个用户」的新装机生效：存量库不动，管理员自己的选择不被覆盖，
// 重复执行不重复改。
func TestIntegration_BuiltinWiringOnlyOnFreshInstall(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	seed := testfake.BuiltinSeedSQL(t)
	t.Cleanup(func() {
		_, _ = e.d.Pool.Exec(ctx, `UPDATE job_policy SET approval_workflow_key = '' WHERE approval_workflow_key = 'builtin_approval_admin'`)
		_, _ = e.d.Pool.Exec(ctx, `DELETE FROM k8s_setting WHERE name IN ('namespace_request_template_id', 'permission_request_template_id')`)
	})
	apply := func() {
		t.Helper()
		if _, err := e.d.Pool.Exec(ctx, seed); err != nil {
			t.Fatalf("apply the built-in seed migration: %v", err)
		}
	}
	keepUsers := func(n int) {
		t.Helper()
		if _, err := e.d.Pool.Exec(ctx, `TRUNCATE TABLE system_user_role, system_dept_leader, system_user_dept CASCADE`); err != nil {
			t.Fatal(err)
		}
		if _, err := e.d.Pool.Exec(ctx, `DELETE FROM "system_user" WHERE id NOT IN (SELECT id FROM "system_user" ORDER BY id LIMIT $1)`, n); err != nil {
			t.Fatal(err)
		}
	}
	reset := func() {
		t.Helper()
		if _, err := e.d.Pool.Exec(ctx, `UPDATE job_policy SET approval_workflow_key = ''`); err != nil {
			t.Fatal(err)
		}
		if _, err := e.d.Pool.Exec(ctx, `DELETE FROM k8s_setting WHERE name IN ('namespace_request_template_id', 'permission_request_template_id')`); err != nil {
			t.Fatal(err)
		}
	}
	state := func() (key, namespace, permission string, version int) {
		t.Helper()
		if err := e.d.Pool.QueryRow(ctx, `SELECT approval_workflow_key, version FROM job_policy WHERE id = 1`).Scan(&key, &version); err != nil {
			t.Fatal(err)
		}
		rows, err := e.d.Pool.Query(ctx, `SELECT name, value FROM k8s_setting WHERE name IN ('namespace_request_template_id', 'permission_request_template_id')`)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var name, value string
			if err := rows.Scan(&name, &value); err != nil {
				t.Fatal(err)
			}
			if name == "namespace_request_template_id" {
				namespace = value
			} else {
				permission = value
			}
		}
		return key, namespace, permission, version
	}

	// 存量库（已经有别的用户）：什么都不动。
	reset()
	keepUsers(6)
	apply()
	if key, ns, pe, _ := state(); key != "" || ns != "" || pe != "" {
		t.Fatalf("an existing installation was rewired: job approval %q, namespace %q, permission %q", key, ns, pe)
	}

	// 新装机：作业审批与 K8s 自助申请指向内置的流程 / 模板。
	reset()
	keepUsers(0)
	apply()
	key, ns, pe, version := state()
	if key != "builtin_approval_admin" || ns != builtinNamespaceTemplateID || pe != builtinPermissionTemplateID {
		t.Fatalf("a fresh installation was not wired: job approval %q, namespace %q, permission %q", key, ns, pe)
	}
	apply()
	if key2, _, _, version2 := state(); key2 != key || version2 != version {
		t.Fatalf("re-applying changed the wiring: job approval %q (version %d → %d)", key2, version, version2)
	}

	// 管理员已经选了别的流程 / 模板：不覆盖。
	reset()
	if _, err := e.d.Pool.Exec(ctx, `UPDATE job_policy SET approval_workflow_key = 'my_approval'`); err != nil {
		t.Fatal(err)
	}
	if _, err := e.d.Pool.Exec(ctx, `INSERT INTO k8s_setting (name, value) VALUES ('namespace_request_template_id', '00000000-0000-7000-8000-000000000000')`); err != nil {
		t.Fatal(err)
	}
	apply()
	if key, ns, _, _ := state(); key != "my_approval" || ns != "00000000-0000-7000-8000-000000000000" {
		t.Fatalf("the administrator's own choice was overwritten: job approval %q, namespace %q", key, ns)
	}
}

// 接口创建的流程 / 模板一律不是内置的；内置前缀保留给系统。
func TestIntegration_UserCreatedDataIsNotBuiltin(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	d := e.publish("user_flow", line(task("approve", uBoss)))
	if d.Builtin {
		t.Fatal("a definition created through the API is marked built-in")
	}
	if err := e.defs.Delete(ctx, d.ID); err != nil {
		t.Fatalf("Delete(a user-created definition) error = %v", err)
	}
	if _, err := e.defs.Create(ctx, &biz.Definition{Key: "builtin_mine", Name: "占用内置前缀"}); !errors.Is(err, biz.ErrDefinitionInvalid) {
		t.Fatalf("Create(builtin_ key) error = %v, want ErrDefinitionInvalid", err)
	}
}
