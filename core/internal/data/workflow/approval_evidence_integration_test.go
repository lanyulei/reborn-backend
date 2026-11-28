//go:build integration

package data

import (
	"context"
	"testing"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
)

// 审批证据按业务对象取整棵流程树的流转历史：被跳过的审批环节、由人同意（可排除发起人）；没有流程实例时为零值。
func TestIntegration_ApprovalEvidence(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	graph := func(rules, onEmpty string) string {
		return `{"nodes":[{"id":"s","type":"start"},{"id":"a","type":"userTask","name":"审批","assignee":{"rules":[` + rules + `]` + onEmpty + `}},{"id":"e","type":"end"}],
			"edges":[{"id":"e1","source":"s","target":"a"},{"id":"e2","source":"a","target":"e"}],"settings":{}}`
	}
	e.publish("ev-approve", graph(`{"kind":"role","roleCodes":["ops"]}`, ""))
	e.publish("ev-skip", graph(`{"kind":"user","userIds":["`+uGone+`"]}`, `,"onEmpty":"skip"`)) // 处理人已停用且明确选了跳过 → 节点被跳过
	repo, txs := NewInstanceRepo(e.d), basedata.NewTxManager(e.d)
	read := func(businessID, except string) biz.ApprovalEvidence {
		t.Helper()
		var ev biz.ApprovalEvidence
		if err := txs.WithinTx(ctx, func(tx txrun.Tx) error {
			var err error
			ev, err = repo.ApprovalEvidenceTx(ctx, tx, "ticket", businessID, except)
			return err
		}); err != nil {
			t.Fatalf("ApprovalEvidenceTx(%s) error = %v", businessID, err)
		}
		return ev
	}

	if in, err := e.rt.Start(ctx, biz.StartParams{DefinitionKey: "ev-approve", BusinessType: "ticket", BusinessID: "T-ev-1", Title: "有人审批", Initiator: uInit}); err != nil || in.Status != biz.InstanceRunning {
		t.Fatalf("Start() = %+v, %v", in, err)
	}
	if ev := read("T-ev-1", uInit); ev != (biz.ApprovalEvidence{}) {
		t.Fatalf("before any approval: %+v, want zero evidence", ev)
	}
	todo := e.todo(uOps1)
	if len(todo) != 1 {
		t.Fatalf("todo = %+v", todo)
	}
	if _, err := e.rt.Approve(ctx, todo[0].TaskID, uOps1, "同意"); err != nil {
		t.Fatal(err)
	}
	if ev := read("T-ev-1", uInit); ev != (biz.ApprovalEvidence{HumanApprovals: 1}) {
		t.Fatalf("after a human approval: %+v", ev)
	}
	if ev := read("T-ev-1", uOps1); ev != (biz.ApprovalEvidence{}) {
		t.Fatalf("the excluded approver must not count: %+v", ev)
	}

	if in, err := e.rt.Start(ctx, biz.StartParams{DefinitionKey: "ev-skip", BusinessType: "ticket", BusinessID: "T-ev-2", Title: "无人审批", Initiator: uInit}); err != nil || in.Status != biz.InstanceCompleted {
		t.Fatalf("Start(skip) = %+v, %v", in, err)
	}
	if ev := read("T-ev-2", uInit); ev != (biz.ApprovalEvidence{SkippedApprovals: 1}) {
		t.Fatalf("skipped approval: %+v", ev)
	}
	// 同一业务对象再发起一次（上一次已结束）：只看最近这一次，上一次被跳过的环节不算在内，也不会因为
	// 查到两个根流程而报错。
	if in, err := e.rt.Start(ctx, biz.StartParams{DefinitionKey: "ev-approve", BusinessType: "ticket", BusinessID: "T-ev-2", Title: "再次发起", Initiator: uInit}); err != nil || in.Status != biz.InstanceRunning {
		t.Fatalf("Start(again) = %+v, %v", in, err)
	}
	if ev := read("T-ev-2", uInit); ev != (biz.ApprovalEvidence{}) {
		t.Fatalf("a re-started flow must only count its own history: %+v", ev)
	}
	if ev := read("T-none", ""); ev != (biz.ApprovalEvidence{}) {
		t.Fatalf("unknown business object: %+v, want zero evidence", ev)
	}
}
