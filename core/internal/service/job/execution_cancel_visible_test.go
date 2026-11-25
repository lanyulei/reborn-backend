package service

import (
	"context"
	"testing"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/job/v1"
	bizjob "github.com/lanyulei/reborn/backend/core/internal/biz/job"
	bizsystem "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"

	jwtx "github.com/go-kratos/kratos/contrib/middleware/jwt/v3"
	kerrors "github.com/go-kratos/kratos/v3/errors"
	"github.com/golang-jwt/jwt/v5"
)

// 取消执行的响应原本总是带着完整的执行内容（命令 / 脚本、参数、目标）：调用者按「执行记录」的可见性口径
// 无权查看这条执行时（只有「执行」按钮权限、且不是自己提交或审批的），不回显内容。能否取消本身不变，
// 仍只由「取消执行」的接口权限决定。
func TestCancelJobExecutionHidesDetailWhenNotVisible(t *testing.T) {
	t.Parallel()
	const (
		alice = "018f3e1a-0000-7000-8000-0000000000a1" // 只有「执行」按钮权限：只能看自己提交的
		bob   = "018f3e1a-0000-7000-8000-0000000000b2" // 同上
		carol = "018f3e1a-0000-7000-8000-0000000000c3" // 持有「执行记录」权限：能看全部
	)
	approvals := &testfake.JobApprovals{Approvers: map[string][]string{"inst-1": {bob}}}
	js := testfake.NewJobStore()
	svc := NewExecutionService(bizjob.NewExecutionUsecase(
		js.Executions(), js.Executions(), js.Executions(), js.Templates(), js.Projects(), js.Credentials(), js.Runners(), nil,
		&testfake.MonitorOrg{},
		bizjob.NewExecutionDeps(testfake.NewTxManager(), js.Policies(), js.RiskRules(), js.Scripts(), &testfake.JobFiles{}, approvals,
			testfake.JobSources{}, nil, testfake.JobRuns{OwnOnly: map[string]bool{alice: true, bob: true}}),
	))
	queued := seedExecution(t, js, alice, jc.StatusQueued, "")
	queued2 := seedExecution(t, js, alice, jc.StatusQueued, "")
	queued3 := seedExecution(t, js, alice, jc.StatusQueued, "")
	finished := seedExecution(t, js, alice, jc.StatusSuccess, "")
	pending := seedExecution(t, js, alice, jc.StatusPending, "inst-1")

	// 别人排队中的执行：照常可以取消（权限由接口授权决定），但响应不带执行内容。
	out, err := svc.CancelJobExecution(userCtx(bob), &v1.CancelJobExecutionRequest{Id: queued})
	if err != nil || out.Execution.Status != jc.StatusCanceled || !out.Execution.CancelRequested || out.Execution.Detail != nil {
		t.Fatalf("cancel someone else's execution: %v, %+v; want canceled without detail", err, out)
	}
	// 别人已结束的执行：409「已结束」（与原先一致），不因可见性变成 404。
	if _, err := svc.CancelJobExecution(userCtx(bob), &v1.CancelJobExecutionRequest{Id: finished}); !kerrors.Is(err, bizjob.ErrExecutionFinished) {
		t.Fatalf("cancel someone else's finished execution: err = %v, want finished", err)
	}

	// 本人提交的：可以取消，响应带完整的执行内容。
	if out, err = svc.CancelJobExecution(userCtx(alice), &v1.CancelJobExecutionRequest{Id: queued2}); err != nil || out.Execution.Status != jc.StatusCanceled ||
		out.Execution.Detail == nil || out.Execution.Detail.Playbook != "site.yml" {
		t.Fatalf("cancel own execution: %v, %+v", err, out)
	}
	// 审批人要能看到自己正在审批的内容，取消待审批的执行同样带内容。
	if out, err = svc.CancelJobExecution(userCtx(bob), &v1.CancelJobExecutionRequest{Id: pending}); err != nil || out.Execution.Status != jc.StatusCanceled ||
		out.Execution.Detail == nil {
		t.Fatalf("cancel as approver: %v, %+v", err, out)
	}
	// 待审批的执行被取消时照常撤回审批流程；排队中的不需要。
	if len(approvals.Cancelled) != 1 || approvals.Cancelled[0] != "inst-1" {
		t.Fatalf("approval instances withdrawn = %v, want [inst-1]", approvals.Cancelled)
	}
	// 持有「执行记录」权限的人：能取消任何人的执行并看到内容；已结束的照常 409。
	if out, err = svc.CancelJobExecution(userCtx(carol), &v1.CancelJobExecutionRequest{Id: queued3}); err != nil || out.Execution.Status != jc.StatusCanceled ||
		out.Execution.Detail == nil {
		t.Fatalf("cancel with view-all permission: %v, %+v", err, out)
	}
	if _, err := svc.CancelJobExecution(userCtx(carol), &v1.CancelJobExecutionRequest{Id: finished}); !kerrors.Is(err, bizjob.ErrExecutionFinished) {
		t.Fatalf("cancel finished with view-all permission: err = %v, want finished", err)
	}
}

// userCtx 模拟鉴权中间件注入 claims 之后的请求 ctx（接口经 UserIDFromContext 取身份）。
func userCtx(userID string) context.Context {
	return jwtx.NewContext(context.Background(), &bizsystem.AccessClaims{
		RegisteredClaims: jwt.RegisteredClaims{Subject: userID},
	})
}

// seedExecution 以 core 的身份直接建一条执行（这里只关心取消接口的可见性判定，不经提交用例）。
func seedExecution(t *testing.T, js *testfake.JobStore, creator string, status int32, instanceID string) string {
	t.Helper()
	id, err := idgen.NewID()
	if err != nil {
		t.Fatal(err)
	}
	e := &bizjob.Execution{
		ID: id, Creator: creator, Status: status, Kind: jc.KindPlaybook,
		TemplateID: "018f3e1a-0000-7000-8000-0000000000d4", TemplateName: "发布", TemplateVersion: 1,
		ApprovalInstanceID: instanceID, CreateTime: time.Now().UTC(),
	}
	e.Spec.Playbook = "site.yml"
	if _, _, err := js.Executions().CreateTx(context.Background(), nil, []bizjob.NewExecution{{Execution: e}}); err != nil {
		t.Fatalf("seed execution %s: %v", id, err)
	}
	return id
}
