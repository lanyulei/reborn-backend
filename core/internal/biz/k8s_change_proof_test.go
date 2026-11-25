package biz

import (
	"context"
	"errors"
	"testing"

	bizk8s "github.com/lanyulei/reborn/backend/core/internal/biz/k8s"
	bizticket "github.com/lanyulei/reborn/backend/core/internal/biz/ticket"
	bizworkflow "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
)

// proofLookup 流程实例查询的桩：只提供审批证据这一项可选能力。
type proofLookup struct {
	bizworkflow.InstanceLookup // 只为满足接口；本测试不会调用它的方法
	ev                         bizworkflow.ApprovalEvidence
	err                        error
	businessType, businessID   string
	except                     string
}

func (l *proofLookup) ApprovalEvidenceTx(_ context.Context, _ txrun.Tx, businessType, businessID, except string) (bizworkflow.ApprovalEvidence, error) {
	l.businessType, l.businessID, l.except = businessType, businessID, except
	return l.ev, l.err
}

// 变更审批的证据取自工单所挂的流程：按「工单」这一业务类型查，发起人不计入「由人同意」；
// 读不出来、或流程用例没有装配，都返回错误——上层据此不批准，而不是把零值当成「流程里没有审批环节」。
func TestK8sTicketsProof(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	l := &proofLookup{ev: bizworkflow.ApprovalEvidence{SkippedApprovals: 2, HumanApprovals: 1}}
	rt := bizworkflow.NewRuntimeUsecase(nil, nil, nil, l, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	tickets := k8sTickets{runtime: rt}

	got, err := tickets.Proof(ctx, nil, "ticket-1", "u-requester")
	if err != nil || got != (bizk8s.ApprovalEvidence{Skipped: 2, Approved: 1}) {
		t.Fatalf("Proof() = %+v, %v", got, err)
	}
	if l.businessType != bizticket.BusinessType || l.businessID != "ticket-1" || l.except != "u-requester" {
		t.Fatalf("looked up %q/%q except %q", l.businessType, l.businessID, l.except)
	}

	l.err = errors.New("database is down")
	if _, err := tickets.Proof(ctx, nil, "ticket-1", "u-requester"); err == nil {
		t.Fatal("a failing read must be reported")
	}
	if _, err := (k8sTickets{}).Proof(ctx, nil, "ticket-1", "u-requester"); err == nil {
		t.Fatal("a missing workflow runtime must be reported, not answered with zero evidence")
	}
}
