//go:build integration

package data

import (
	"context"
	"strings"
	"testing"
	"time"

	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// 恢复通知可以越过排在它前面、退避中的升级通知（monitor 模块评审 dispatch P3-3）：恢复行存在即事件已恢复，
// 那条升级通知领取后只会被丢弃，不该在退避期里把恢复通知一并拖住。触发通知不在例外之内
// （见 TestDispatch_OrderWithinAlert：触发通知在退避中时恢复通知照旧等它）。
func TestDispatch_ResolvedPassesAnEscalationRowInBackoff(t *testing.T) {
	d := newDispatchEnv(t)
	ctx := context.Background()
	alertID := d.firingAlert()
	// 更早入箱的升级通知（第 1 级的第 1 次），刚失败过一次：退避期内不会被领取。
	esc := newID(t)
	if _, err := d.pool.Exec(ctx, `INSERT INTO monitor_notify_outbox (id, alert_id, kind, seq, level, status, attempts, done_time)
		VALUES ($1, $2, 4, $3, 1, 1, 1, now())`, esc, alertID, mc.EscalationOutboxSeq(1, 1)); err != nil {
		t.Fatal(err)
	}
	// 事件恢复并入箱恢复通知（此前发出过触发通知，所以这条恢复通知应当发出）。
	if _, err := d.pool.Exec(ctx, "UPDATE monitor_alert SET status = 2, resolve_time = now(), notify_count = 1 WHERE id = $1", alertID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	resolved := d.outboxRow(alertID, mc.NotifyKindResolved, 2)

	d.mustRun(1) // 恢复通知不等退避中的升级通知
	if s := d.outboxState(resolved); s.status != mc.OutboxDone {
		t.Fatalf("resolved row = %+v, want dispatched without waiting for the escalation row", s)
	}
	if s := d.outboxState(esc); s.status != mc.OutboxPending || s.attempts != 1 {
		t.Fatalf("escalation row = %+v, want still pending in backoff", s)
	}
	// 退避期满：升级通知被领取，事件已不在告警中 → 丢弃，不发。
	d.expireBackoff(esc)
	d.mustRun(1)
	if s := d.outboxState(esc); s.status != mc.OutboxDropped || !strings.Contains(s.lastError, "不再发升级通知") {
		t.Fatalf("escalation row after the backoff = %+v", s)
	}
	if n := d.count("SELECT count(*) FROM monitor_notify_log WHERE alert_id = $1 AND kind = 4", alertID); n != 0 {
		t.Fatalf("escalation notifications sent for a resolved alert: %d", n)
	}
	if n := d.count("SELECT count(*) FROM monitor_notify_log WHERE alert_id = $1 AND kind = 3", alertID); n != 1 {
		t.Fatalf("the recovery notification must be delivered: %d", n)
	}
}
