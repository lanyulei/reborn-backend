package biz

import (
	"context"
	"errors"
	"testing"

	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"
)

// brokenTemplates 读模板时数据库故障：提交执行以瞬时错误（非业务拒绝）失败。
type brokenTemplates struct{ fakeTemplates }

func (brokenTemplates) Get(context.Context, string) (*Template, error) {
	return nil, errors.New("db down")
}

// 「信号待发送」只在 Complete 时落库：登记之后中断的记录上读到的是「无需发送」，续做时按登记时的条件
// 恢复为待发送（条件：触发器配了信号、调用方带了流程实例）——带幂等键的接口触发器续做完照常回发信号。
// 没配信号 / 没带流程实例的（告警触发器恒如此）不受影响。
func TestFireResumeRestoresPendingSignal(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	trg := &Trigger{ID: "g", Type: jc.TriggerTypeAPI, TemplateID: "t", Owner: "u", Signal: "job.done"}
	broken := NewExecutionUsecase(nil, nil, nil, brokenTemplates{}, nil, nil, nil, nil, noNames{}, ExecutionDeps{Runs: testAllow{}})

	// 续做时提交仍是瞬时失败：不写回（下次重试再续做）；记录在内存里已恢复为待发送，提交成功后 Complete 会照此落库。
	stuck := &TriggerFire{
		ID: "018f3e1a-0000-7000-8000-0000000000f1", TriggerID: "g", Ref: "req-1", Result: jc.FireSubmitted,
		Signal: "job.done", WorkflowInstanceID: "018f3e1a-0000-7000-8000-0000000000aa", SignalState: jc.SignalNone,
	}
	fires := &resumeFires{existing: stuck}
	uc := &TriggerUsecase{fires: fires, execs: broken}
	if out, err := uc.fire(ctx, trg, &TriggerFire{TriggerID: "g", Ref: "req-1"}, nil, nil); err == nil || out != nil || fires.completed != nil {
		t.Fatalf("a transient submit failure must surface and leave the record for the next retry: out=%+v err=%v completed=%+v", out, err, fires.completed)
	}
	if stuck.SignalState != jc.SignalPending {
		t.Fatalf("resumed record must carry the pending signal again, got state %d", stuck.SignalState)
	}

	// 没带流程实例 / 没配信号：不凭空生出信号。
	for _, f := range []*TriggerFire{
		{ID: "018f3e1a-0000-7000-8000-0000000000f2", TriggerID: "g", Ref: "req-2", Result: jc.FireSubmitted, Signal: "job.done"},
		{ID: "018f3e1a-0000-7000-8000-0000000000f3", TriggerID: "g", Ref: "alert-1", Result: jc.FireSubmitted, WorkflowInstanceID: "018f3e1a-0000-7000-8000-0000000000aa"},
	} {
		fires.existing = f
		if _, err := uc.fire(ctx, trg, &TriggerFire{TriggerID: "g", Ref: f.Ref}, nil, nil); err == nil {
			t.Fatal("expected the transient failure")
		}
		if f.SignalState != jc.SignalNone {
			t.Fatalf("%s: no signal was registered, state must stay none: %d", f.Ref, f.SignalState)
		}
	}

	// 续做时提交被业务规则拒绝：落为失败、信号作废（与此前一致）。
	tpl := &Template{ID: "t", Name: "x", Enabled: false}
	rejecting := NewExecutionUsecase(nil, nil, nil, fakeTemplates{tpl}, nil, nil, nil, nil, noNames{}, ExecutionDeps{Runs: testAllow{}})
	stuck.SignalState = jc.SignalNone
	fires.existing, fires.completed = stuck, nil
	uc = &TriggerUsecase{fires: fires, execs: rejecting}
	if out, err := uc.fire(ctx, trg, &TriggerFire{TriggerID: "g", Ref: "req-1"}, nil, nil); err != nil || out != stuck || fires.completed != stuck {
		t.Fatalf("rejected resume: out=%+v err=%v", out, err)
	}
	if stuck.Result != jc.FireFailed || stuck.SignalState != jc.SignalNone {
		t.Fatalf("a rejected submit must fail the record and drop the signal: %+v", stuck)
	}
}
