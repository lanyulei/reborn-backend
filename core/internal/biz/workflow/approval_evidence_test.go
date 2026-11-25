package biz

import (
	"context"
	"slices"
	"testing"

	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
)

// 审批证据按流转历史统计：审批环节因处理人为空被跳过时记的是 skip + SkipReasonNoAssignee，有人同意时记的是
// 带操作人的 approve。这两条是引擎（engine.go）与仓储查询（data/workflow）之间的契约，这里把它们钉住——
// 引擎改了写法而查询没跟上，上层就会把「没人审批过」当成「没有审批环节」。
func TestApprovalEvidenceHistoryContract(t *testing.T) {
	t.Parallel()
	h := start(t, linearGraph(`{"id":"a","type":"userTask","assignee":{"rules":[{"kind":"role","roleCodes":["nobody"]}]}}`), nil, &dirStub{})
	h.wantStatus(InstanceCompleted)
	i := slices.IndexFunc(h.s.Histories, func(x *History) bool { return x.Action == ActionSkip })
	if i < 0 || h.s.Histories[i].Detail["reason"] != SkipReasonNoAssignee || h.s.Histories[i].NodeID != "a" {
		t.Fatalf("a skipped approval must be recorded as skip/%q at the node; histories = %+v", SkipReasonNoAssignee, h.s.Histories)
	}

	h = start(t, linearGraph(task("a", "any", "u1")), nil, nil)
	h.approve("a", "u1")
	i = slices.IndexFunc(h.s.Histories, func(x *History) bool { return x.Action == ActionApprove })
	if i < 0 || h.s.Histories[i].Operator != "u1" {
		t.Fatalf("a human approval must be recorded as approve with its operator; histories = %+v", h.s.Histories)
	}
}

// evidenceLookup 实现了审批证据可选能力的实例查询桩。
type evidenceLookup struct {
	InstanceLookup // 只为满足接口；本测试不会调用它的方法
	ev             ApprovalEvidence
	err            error
	got            []string
}

func (l *evidenceLookup) ApprovalEvidenceTx(_ context.Context, _ txrun.Tx, businessType, businessID, except string) (ApprovalEvidence, error) {
	l.got = []string{businessType, businessID, except}
	return l.ev, l.err
}

func TestRuntimeApprovalEvidenceOf(t *testing.T) {
	t.Parallel()
	l := &evidenceLookup{ev: ApprovalEvidence{SkippedApprovals: 2, HumanApprovals: 1}}
	rt := &RuntimeUsecase{lookup: l}
	ev, err := rt.ApprovalEvidenceOf(context.Background(), nil, "ticket", "T-1", "u-init")
	if err != nil || ev != l.ev {
		t.Fatalf("ApprovalEvidenceOf() = %+v, %v; want %+v", ev, err, l.ev)
	}
	if want := []string{"ticket", "T-1", "u-init"}; !slices.Equal(l.got, want) {
		t.Fatalf("forwarded = %v, want %v", l.got, want)
	}
	// 仓储没有该能力：返回错误而不是零值——零值会被上层当成「流程里没有审批环节」而放行。
	l.err = context.DeadlineExceeded
	if _, err = rt.ApprovalEvidenceOf(context.Background(), nil, "ticket", "T-1", ""); err == nil {
		t.Fatal("a failing read must return its error")
	}
	rt.lookup = struct{ InstanceLookup }{}
	if _, err = rt.ApprovalEvidenceOf(context.Background(), nil, "ticket", "T-1", ""); err == nil {
		t.Fatal("a store without the capability must be reported, not answered with zero evidence")
	}
}
