package biz

import (
	"context"
	"slices"
	"testing"
	"time"

	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"
)

// 告警的 first_fire_time 是评估轮次的开始时刻、各任务各自提交：一轮评估慢时，「开始得早、提交得晚」的告警会
// 落在已经越过的水位线之前。按 (first_fire_time, id) 递增的游标读不到它——此前这样的告警永久漏掉，
// 自愈作业静默不触发。每轮从水位线之前 alertCommitLag 的位置重读（靠 seen 去重），迟到提交的告警不丢。
func TestRunAlertsPicksUpLateCommittedAlerts(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)
	trg := &Trigger{ID: "g1", Type: jc.TriggerTypeAlert, TemplateID: "t", Owner: "u", Alert: jc.AlertMatch{RuleIDs: []string{"r1"}}, UpdateTime: now.Add(-time.Hour)}
	events := alertEventsAt(now.Add(-3*time.Minute), 5, "r1")
	feed := &alertFeedFake{events: slices.Clone(events)}
	fires := &alertFires{}
	uc := newAlertTriggerUsecase(&alertSecrets{triggers: []*Trigger{trg}}, feed, fires, now)

	if _, err := uc.RunAlerts(context.Background()); err != nil {
		t.Fatalf("RunAlerts() error = %v", err)
	}
	if fires.count("g1") != 5 || uc.alertMark.ID != events[4].ID {
		t.Fatalf("first round fired = %d, mark = %+v; want 5 and the last event", fires.count("g1"), uc.alertMark)
	}

	// 迟到提交：上一轮读取时还没落库的告警，时间戳落在 events[1] 与 events[2] 之间（早于水位线）。
	late := &AlertEvent{
		ID: "018f3e1a-0000-7000-8000-00000000ff", RuleID: "r1", RuleName: "cpu", Severity: 3,
		FirstFireTime: events[1].FirstFireTime.Add(500 * time.Millisecond),
	}
	feed.events = slices.Concat(feed.events[:2], []*AlertEvent{late}, feed.events[2:])
	if _, err := uc.RunAlerts(context.Background()); err != nil {
		t.Fatalf("RunAlerts() error = %v", err)
	}
	if fires.count("g1") != 6 || fires.begun[len(fires.begun)-1] != "g1/"+late.ID {
		t.Fatalf("second round fired = %d (%v); want exactly the late alert added", fires.count("g1"), fires.begun)
	}
	// 再跑一轮：没有新事件，也不重复触发。
	if _, err := uc.RunAlerts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fires.count("g1") != 6 {
		t.Fatalf("third round fired = %d, want no new firings", fires.count("g1"))
	}
}
