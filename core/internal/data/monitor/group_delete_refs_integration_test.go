//go:build integration

package data

import (
	"context"
	"testing"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// 删除分组时的占用检查覆盖记录规则 / 升级策略 / 抑制规则（monitor 模块评审 core-monitor-config P2）。
// 此前漏检：分组删掉后其下的记录规则成了非超级管理员再也看不到、管不了的「孤儿」，却仍被引擎照常执行。
func TestGroupRepo_DeleteRefusedWhileRecordRuleEscalationInhibitRefer(t *testing.T) {
	// 升级策略表以外键引用分组：清分组（newEnv）之前与测试结束之后都清一遍，同 newEscEnv。
	pre := openData(t, testDSN(t))
	truncateEscalationTables(t, pre.Pool)
	t.Cleanup(func() { truncateEscalationTables(t, pre.Pool) })
	e, inhibits, records := newInhibitEnv(t)
	ctx := context.Background()
	esc := NewEscalationRepo(e.d)
	g := e.group("", "configs")
	ds := e.writableDatasource("vm-w", "http://vminsert:8480/insert/0/prometheus/api/v1/write")

	record := &biz.RecordRule{ID: newID(t), Spec: biz.RecordRuleSpec{
		GroupID: g.ID, Name: "job:up:sum", DatasourceID: ds.ID, Expr: "sum(job:up)", IntervalS: 60, Enabled: true,
	}}
	if err := records.Create(ctx, record, uAlice); err != nil {
		t.Fatalf("create record rule: %v", err)
	}
	policy := escalation(g.ID, "升级")
	policy.ID = newID(t)
	if err := esc.Create(ctx, policy, uAlice); err != nil {
		t.Fatalf("create escalation: %v", err)
	}
	inhibit := &biz.Inhibit{ID: newID(t), Spec: biz.InhibitSpec{
		GroupID: g.ID, Name: "同源抑制", Enabled: true, Source: mc.InhibitSelector{Severities: []int32{4}}, Target: mc.InhibitSelector{Severities: []int32{2}},
	}}
	if err := inhibits.Create(ctx, inhibit, uAlice); err != nil {
		t.Fatalf("create inhibit: %v", err)
	}

	wantCounts := func(what, recordRules, escalations, inhibitsN string) {
		t.Helper()
		err := e.groups.Delete(ctx, g.ID, uAdmin)
		wantErr(t, what, err, biz.ErrGroupInUse)
		ke := kerrors.FromError(err)
		if ke.Code != 409 || ke.Metadata["record_rule_count"] != recordRules || ke.Metadata["escalation_count"] != escalations ||
			ke.Metadata["inhibit_count"] != inhibitsN {
			t.Fatalf("%s: %+v, want 409 with record_rule_count=%s escalation_count=%s inhibit_count=%s",
				what, ke, recordRules, escalations, inhibitsN)
		}
		if n := e.count("SELECT count(*) FROM monitor_rule_group WHERE id = $1 AND delete_time IS NULL", g.ID); n != 1 {
			t.Fatalf("%s: a refused delete must leave the group in place", what)
		}
	}
	// 只看记录规则时同样被拦：这是本条修复之前漏掉的那一种。
	wantCounts("record rule, escalation and inhibit", "1", "1", "1")
	// 已删除的记录规则 / 升级策略 / 抑制规则不算引用。
	if err := records.Delete(ctx, record.ID, 1, uAlice); err != nil {
		t.Fatalf("delete record rule: %v", err)
	}
	wantCounts("escalation and inhibit left", "0", "1", "1")
	if err := esc.Delete(ctx, policy.ID, 1, uAlice); err != nil {
		t.Fatalf("delete escalation: %v", err)
	}
	wantCounts("only the inhibit is left", "0", "0", "1")
	if err := inhibits.Delete(ctx, inhibit.ID, 1, uAlice); err != nil {
		t.Fatalf("delete inhibit: %v", err)
	}
	if err := e.groups.Delete(ctx, g.ID, uAdmin); err != nil {
		t.Fatalf("delete once unreferenced: %v", err)
	}
}
