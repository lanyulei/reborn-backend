//go:build integration

package data

import (
	"context"
	"slices"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// 告警详情的通知记录与时间线条数超过上限时保留的是最新的一段，仍按时间升序返回（monitor 模块评审
// core-monitor-alert P2）：持续告警的事件每轮重复提醒都追加记录，此前取「最早 N 条」会让最新通知永远看不到。
func TestAlertDetail_ListByAlertKeepsTheLatest(t *testing.T) {
	d := newDispatchEnv(t)
	ctx := context.Background()
	alertID := d.firingAlert()

	seedLog := func(age time.Duration) string {
		t.Helper()
		id := newID(t)
		if _, err := d.pool.Exec(ctx, `INSERT INTO monitor_notify_log (id, alert_id, kind, seq, source, recipients, create_time)
			VALUES ($1, $2, 1, 1, 1, '[]', now() - make_interval(secs => $3))`, id, alertID, age.Seconds()); err != nil {
			t.Fatalf("seed notify log: %v", err)
		}
		return id
	}
	oldest, middle, newest := seedLog(3*time.Hour), seedLog(2*time.Hour), seedLog(time.Hour)

	logs, err := d.logs.ListByAlert(ctx, alertID, 10)
	if err != nil || !slices.Equal(logIDs(logs), []string{oldest, middle, newest}) {
		t.Fatalf("ListByAlert(10) = %v, %v; want everything in ascending order", logIDs(logs), err)
	}
	logs, err = d.logs.ListByAlert(ctx, alertID, 2)
	if err != nil || !slices.Equal(logIDs(logs), []string{middle, newest}) {
		t.Fatalf("ListByAlert(2) = %v, %v; want the latest two, still ascending", logIDs(logs), err)
	}

	// 时间线条目同一口径。
	ops := NewAlertOpsRepo(d.d)
	var events []string
	for range 3 {
		ev := &biz.AlertEvent{ID: newID(t), AlertID: alertID, Type: mc.AlertEventComment, Operator: uAlice, Detail: mc.AlertEventDetail{Text: "备注"}}
		if err := ops.Append(ctx, ev); err != nil {
			t.Fatalf("Append: %v", err)
		}
		events = append(events, ev.ID)
		time.Sleep(2 * time.Millisecond)
	}
	got, err := ops.ListByAlert(ctx, alertID, 1)
	if err != nil || len(got) != 1 || got[0].ID != events[2] {
		t.Fatalf("ListByAlert(events, 1) = %+v, %v; want only the latest event %s", got, err, events[2])
	}
	got, err = ops.ListByAlert(ctx, alertID, 2)
	if err != nil || len(got) != 2 || got[0].ID != events[1] || got[1].ID != events[2] {
		t.Fatalf("ListByAlert(events, 2) = %+v, %v; want the latest two in ascending order", got, err)
	}
}

func logIDs(logs []*biz.NotifyLog) []string {
	out := make([]string, 0, len(logs))
	for _, l := range logs {
		out = append(out, l.ID)
	}
	return out
}
