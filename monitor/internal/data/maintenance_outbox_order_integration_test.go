//go:build integration

package data

import (
	"context"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// 仍有待派发通知行的过期事件不清理：core 的派发事务是「发件箱行 → 告警行」，这里是「告警行 → 发件箱行」，
// 两边同时命中同一事件会互等锁。回归 monitor 模块评审 data-contracts P3-2。
func TestPurgeAlerts_SkipsAlertsThatStillHavePendingOutboxRows(t *testing.T) {
	e := newEnv(t)
	repo := NewMaintenanceRepo(e.d)
	ctx := context.Background()
	ds, ruleID := e.datasource("http://127.0.0.1:1"), e.rule(ruleSpec{})
	const day = 24 * time.Hour
	blocked := e.seedAlert(ruleID, ds, "fp-blocked", monitorcontract.AlertResolved, 200*day)
	settled := e.seedAlert(ruleID, ds, "fp-settled", monitorcontract.AlertResolved, 190*day)
	for _, id := range []string{blocked, settled} {
		e.exec("INSERT INTO monitor_notify_outbox (id, alert_id, kind, seq, status) VALUES ($1, $2, 3, 1, 1)", e.id(), id)
	}
	left := func(id string) int { return e.count("SELECT count(*) FROM monitor_alert WHERE id = $1", id) }

	// 两条都还挂着待派发行：一条都不清理（成批扫描照常越过它们，不报错、不死等）。
	if n, err := repo.PurgeAlerts(ctx, 180*day, 100); err != nil || n != 0 {
		t.Fatalf("PurgeAlerts() = %d, %v; want 0 while both alerts have pending rows", n, err)
	}
	if left(blocked) != 1 || left(settled) != 1 {
		t.Fatal("alerts with pending outbox rows must not be purged")
	}

	// core 把其中一条的发件箱行终结之后，这个事件才可清理；另一条继续留着，待派发行原样不动。
	e.exec("UPDATE monitor_notify_outbox SET status = 2, done_time = now() WHERE alert_id = $1 AND status = 1", settled)
	if n, err := repo.PurgeAlerts(ctx, 180*day, 100); err != nil || n != 1 {
		t.Fatalf("PurgeAlerts() = %d, %v; want 1 once a row is settled", n, err)
	}
	if left(settled) != 0 || left(blocked) != 1 {
		t.Fatal("only the alert without pending outbox rows may be purged")
	}
	if e.count("SELECT count(*) FROM monitor_notify_outbox WHERE alert_id = $1 AND status = 1", blocked) != 1 {
		t.Fatal("the pending outbox row of the retained alert must stay")
	}
	if e.count("SELECT count(*) FROM monitor_notify_outbox WHERE alert_id = $1", settled) != 0 {
		t.Fatal("the settled outbox row must go with its alert")
	}

	// 最后一条也终结：清理收尾。
	e.exec("UPDATE monitor_notify_outbox SET status = 2, done_time = now() WHERE alert_id = $1 AND status = 1", blocked)
	if n, err := repo.PurgeAlerts(ctx, 180*day, 100); err != nil || n != 1 || left(blocked) != 0 {
		t.Fatalf("PurgeAlerts() = %d, %v; left = %d; want the last alert purged", n, err, left(blocked))
	}
}
