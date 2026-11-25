//go:build integration

package data

import (
	"context"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/job"
	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"
)

// 触发记录的「信号待发送」只在 Complete（已有执行）时落库；「已提交」却始终没有执行的记录一天后由保留期
// 清理删除——包括历史上带着「待发送」、此前永不清理的那些。
func TestFireSignalPendingOnlyAfterComplete(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	tpl := e.base()
	trg := &biz.Trigger{ID: newID(t), Name: "hook", Type: jc.TriggerTypeAPI, TemplateID: tpl.ID, Signal: "job.done", Enabled: true}
	if err := e.triggers.Write(ctx, biz.OpCreate, trg, 0, uAlice, &biz.TriggerToken{Hash: []byte("h")}); err != nil {
		t.Fatal(err)
	}
	state := func(id string) (signalState int32, hasNext bool) {
		t.Helper()
		if err := e.pool.QueryRow(ctx, "SELECT signal_state, signal_next_time IS NOT NULL FROM job_trigger_fire WHERE id = $1", id).Scan(&signalState, &hasNext); err != nil {
			t.Fatal(err)
		}
		return signalState, hasNext
	}
	instance := newID(t)
	begin := func(ref string) *biz.TriggerFire {
		t.Helper()
		f := &biz.TriggerFire{ID: newID(t), TriggerID: trg.ID, Ref: ref, WorkflowInstanceID: instance, Signal: "job.done", SignalState: jc.SignalPending}
		if _, fresh, err := e.fires.Begin(ctx, f, 0); err != nil || !fresh || f.Result != jc.FireSubmitted {
			t.Fatalf("Begin(%s): fresh %v, result %d, %v", ref, fresh, f.Result, err)
		}
		return f
	}

	// 登记：库里是「无需发送」，内存里的记录仍是待发送（Complete 用它）。
	done := begin("done")
	if s, next := state(done.ID); s != jc.SignalNone || next || done.SignalState != jc.SignalPending {
		t.Fatalf("after Begin: db state %d (next %v), in-memory state %d", s, next, done.SignalState)
	}
	// 中断后再遇到同一幂等键：拿回的记录带着信号名与流程实例，用例据此恢复为待发送。
	existing, fresh, err := e.fires.Begin(ctx, &biz.TriggerFire{ID: newID(t), TriggerID: trg.ID, Ref: "done"}, 0)
	if err != nil || fresh || existing == nil || existing.ID != done.ID || existing.SignalState != jc.SignalNone ||
		existing.Signal != "job.done" || existing.WorkflowInstanceID != instance || existing.ExecutionID != "" {
		t.Fatalf("resume Begin = %+v, %v, %v", existing, fresh, err)
	}
	// 写回执行：此时才置为待发送并立即到期。
	done.ExecutionID = newID(t)
	if err := e.fires.Complete(ctx, done); err != nil {
		t.Fatal(err)
	}
	if s, next := state(done.ID); s != jc.SignalPending || !next {
		t.Fatalf("after Complete: state %d, next %v", s, next)
	}

	// 滞留记录的清理：没有执行、登记超过一天的删除（含历史上预置了待发送的）；一天之内的与已有执行的保留。
	stranded, legacy := begin("stranded"), begin("legacy")
	begin("inflight")
	if _, err := e.pool.Exec(ctx, "UPDATE job_trigger_fire SET create_time = now() - interval '2 days' WHERE id = ANY($1::uuid[])",
		[]string{stranded.ID, legacy.ID, done.ID}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(ctx, "UPDATE job_trigger_fire SET signal_state = 1 WHERE id = $1", legacy.ID); err != nil {
		t.Fatal(err)
	}
	if n, err := e.stats.Purge(ctx, 30*24*time.Hour, 180*24*time.Hour, 100); err != nil || n != 2 {
		t.Fatalf("Purge() = %d, %v; want the two stranded fires", n, err)
	}
	var refs string
	if err := e.pool.QueryRow(ctx, "SELECT string_agg(ref, ',' ORDER BY ref) FROM job_trigger_fire WHERE trigger_id = $1", trg.ID).Scan(&refs); err != nil || refs != "done,inflight" {
		t.Fatalf("fires after purge = %q, %v", refs, err)
	}
}
