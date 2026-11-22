package biz

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"
)

// alertSecrets 只提供启用的告警触发器。
type alertSecrets struct{ triggers []*Trigger }

func (a *alertSecrets) SetToken(context.Context, string, TriggerToken, string) error { return nil }
func (a *alertSecrets) Token(context.Context, string) (TriggerToken, error) {
	return TriggerToken{}, ErrTriggerNotFound
}

func (a *alertSecrets) EnabledAlertTriggers(context.Context) ([]*Trigger, error) {
	return a.triggers, nil
}

// alertFeedFake 按 (首次触发时间, ID) 排好序的事件，按游标翻页；记录每次读取的页数。
type alertFeedFake struct {
	events []*AlertEvent
	reads  int
}

func (f *alertFeedFake) FiringAfter(_ context.Context, after AlertCursor, limit int) ([]*AlertEvent, error) {
	f.reads++
	var out []*AlertEvent
	for _, a := range f.events {
		if c := a.FirstFireTime.Compare(after.FirstFireTime); c < 0 || (c == 0 && a.ID <= after.ID) {
			continue
		}
		out = append(out, a)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

// alertFires 触发记录：按 (触发器, ref) 去重登记；failRefs 里的 ref 登记时报错（模拟数据库故障）。
type alertFires struct {
	begun    []string
	failRefs map[string]bool
}

func (f *alertFires) List(context.Context, FireFilter, int64, int64) ([]*TriggerFire, int64, error) {
	return nil, 0, nil
}

func (f *alertFires) Begin(_ context.Context, fire *TriggerFire, _ int32) (*TriggerFire, bool, error) {
	if f.failRefs[fire.Ref] {
		return nil, false, errors.New("db down")
	}
	key := fire.TriggerID + "/" + fire.Ref
	if slices.Contains(f.begun, key) {
		return fire, false, nil
	}
	f.begun = append(f.begun, key)
	return fire, true, nil
}

func (f *alertFires) Complete(context.Context, *TriggerFire) error { return nil }

func (f *alertFires) count(triggerID string) int {
	n := 0
	for _, k := range f.begun {
		if strings.HasPrefix(k, triggerID+"/") {
			n++
		}
	}
	return n
}

func alertEventsAt(start time.Time, n int, ruleID string) []*AlertEvent {
	out := make([]*AlertEvent, 0, n)
	for i := range n {
		out = append(out, &AlertEvent{
			ID: fmt.Sprintf("018f3e1a-0000-7000-8000-%012d", i+1), RuleID: ruleID, RuleName: "cpu", Severity: 3,
			FirstFireTime: start.Add(time.Duration(i) * time.Second),
		})
	}
	return out
}

func newAlertTriggerUsecase(secrets *alertSecrets, feed *alertFeedFake, fires *alertFires, now time.Time) *TriggerUsecase {
	tpl := &Template{ID: "t", Name: "x", Enabled: false} // 停用：提交被业务规则拒绝 → 记为失败，但登记成立
	execs := NewExecutionUsecase(nil, nil, nil, fakeTemplates{tpl}, nil, nil, nil, nil, noNames{}, ExecutionDeps{Runs: testAllow{}})
	uc := NewTriggerUsecase(TriggerDeps{Secrets: secrets, Fires: fires, Alerts: feed}, fakeTemplates{tpl}, nil, execs, noNames{})
	uc.now = func() time.Time { return now }
	return uc
}

// 告警风暴：回看窗口内的事件多于一页时按水位线翻页，全部处理且每条只触发一次；下一轮不重读已处理的。
func TestRunAlertsPagesThroughABacklog(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)
	trg := &Trigger{ID: "g1", Type: jc.TriggerTypeAlert, TemplateID: "t", Owner: "u", Alert: jc.AlertMatch{RuleIDs: []string{"r1"}}, UpdateTime: now.Add(-time.Hour)}
	feed := &alertFeedFake{events: alertEventsAt(now.Add(-8*time.Minute), 2*alertBatch+50, "r1")}
	fires := &alertFires{}
	uc := newAlertTriggerUsecase(&alertSecrets{triggers: []*Trigger{trg}}, feed, fires, now)

	if _, err := uc.RunAlerts(context.Background()); err != nil {
		t.Fatalf("RunAlerts() error = %v", err)
	}
	if got := fires.count("g1"); got != 2*alertBatch+50 {
		t.Fatalf("fired %d events, want all %d", got, 2*alertBatch+50)
	}
	if feed.reads != 3 {
		t.Fatalf("feed reads = %d, want 3 pages", feed.reads)
	}
	last := feed.events[len(feed.events)-1]
	if uc.alertMark.ID != last.ID || !uc.alertMark.FirstFireTime.Equal(last.FirstFireTime) {
		t.Fatalf("mark = %+v, want the last event", uc.alertMark)
	}
	// 第二轮：从水位线之后读，没有新事件，一次读取即结束，不重复触发。
	feed.reads = 0
	if _, err := uc.RunAlerts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if feed.reads != 1 || fires.count("g1") != 2*alertBatch+50 {
		t.Fatalf("second round reads = %d, fired = %d; want 1 read and no new firings", feed.reads, fires.count("g1"))
	}
	// 翻满 alertMaxPages 页仍有积压：本轮停下（记日志），水位线留在读到的位置，下一轮接着读。
	huge := &alertFeedFake{events: alertEventsAt(now.Add(-9*time.Minute), (alertMaxPages+1)*alertBatch, "r1")}
	fires2 := &alertFires{}
	uc2 := newAlertTriggerUsecase(&alertSecrets{triggers: []*Trigger{trg}}, huge, fires2, now)
	if _, err := uc2.RunAlerts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if huge.reads != alertMaxPages || fires2.count("g1") != alertMaxPages*alertBatch {
		t.Fatalf("capped round: reads = %d, fired = %d", huge.reads, fires2.count("g1"))
	}
	if _, err := uc2.RunAlerts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fires2.count("g1") != len(huge.events) {
		t.Fatalf("after the next round fired = %d, want all %d", fires2.count("g1"), len(huge.events))
	}
}

// 一条事件对某个触发器处理失败：其余照常处理，水位线停在失败事件之前；故障恢复后的下一轮只补发那一条。
func TestRunAlertsFailureHoldsTheMark(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)
	trg := &Trigger{ID: "g1", Type: jc.TriggerTypeAlert, TemplateID: "t", Owner: "u", Alert: jc.AlertMatch{RuleIDs: []string{"r1"}}, UpdateTime: now.Add(-time.Hour)}
	events := alertEventsAt(now.Add(-5*time.Minute), 30, "r1")
	feed := &alertFeedFake{events: events}
	fires := &alertFires{failRefs: map[string]bool{events[10].ID: true}}
	uc := newAlertTriggerUsecase(&alertSecrets{triggers: []*Trigger{trg}}, feed, fires, now)

	if _, err := uc.RunAlerts(context.Background()); err == nil {
		t.Fatal("RunAlerts() must report the failure")
	}
	if fires.count("g1") != 29 {
		t.Fatalf("fired = %d, want 29 (all but the failing one)", fires.count("g1"))
	}
	if uc.alertMark.ID != events[9].ID {
		t.Fatalf("mark = %+v, want to stay before the failed event %s", uc.alertMark, events[10].ID)
	}
	fires.failRefs = nil
	if _, err := uc.RunAlerts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fires.count("g1") != 30 || fires.begun[29] != "g1/"+events[10].ID {
		t.Fatalf("retry round fired = %d, last = %s; want exactly the failed event", fires.count("g1"), fires.begun[len(fires.begun)-1])
	}
	if uc.alertMark.ID != events[29].ID {
		t.Fatalf("mark = %+v, want the last event", uc.alertMark)
	}
}

// 触发器启用前就在告警的事件不处理；启用后仅改名之类的编辑（UpdateTime 推后）不丢掉编辑前后积压的事件；
// 停用再启用重新按编辑时间起算。
func TestRunAlertsTriggerSinceSurvivesCosmeticEdits(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)
	events := alertEventsAt(now.Add(-6*time.Minute), 12, "r1") // 每秒一条：-6:00 … -5:49
	enabledAt := events[3].FirstFireTime                       // 前 3 条在启用之前
	trg := &Trigger{ID: "g1", Type: jc.TriggerTypeAlert, TemplateID: "t", Owner: "u", Alert: jc.AlertMatch{RuleIDs: []string{"r1"}}, UpdateTime: enabledAt}
	secrets := &alertSecrets{triggers: []*Trigger{trg}}
	feed := &alertFeedFake{events: events[:6]}
	fires := &alertFires{}
	uc := newAlertTriggerUsecase(secrets, feed, fires, now)
	if _, err := uc.RunAlerts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fires.count("g1") != 3 || fires.begun[0] != "g1/"+events[3].ID {
		t.Fatalf("first round fired %v, want events 3..5 only", fires.begun)
	}
	// 改名：UpdateTime 推到最后一条事件之后，同时又来了 6 条新事件（其中 4 条早于编辑时间）。
	edited := *trg
	edited.Name, edited.UpdateTime = "renamed", events[9].FirstFireTime
	secrets.triggers = []*Trigger{&edited}
	feed.events = events
	if _, err := uc.RunAlerts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fires.count("g1") != 9 {
		t.Fatalf("after a cosmetic edit fired = %d, want 9 (nothing skipped)", fires.count("g1"))
	}
	// 停用一轮（从启用列表消失）再启用：按新的编辑时间起算，期间积压的不补。
	secrets.triggers = nil
	if _, err := uc.RunAlerts(context.Background()); err != nil {
		t.Fatal(err)
	}
	reenabled := *trg
	reenabled.UpdateTime = now
	secrets.triggers = []*Trigger{&reenabled}
	feed.events = append(events, alertEventsAt(now.Add(-time.Minute), 2, "r1")[0:0]...) // 没有新事件
	if _, err := uc.RunAlerts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fires.count("g1") != 9 {
		t.Fatalf("after re-enabling fired = %d, want still 9 (no backfill)", fires.count("g1"))
	}
	if _, ok := uc.triggerSince["g1"]; !ok || !uc.triggerSince["g1"].Equal(now) {
		t.Fatalf("triggerSince = %v, want reset to the re-enable time", uc.triggerSince["g1"])
	}
}

type signalQueueFake struct {
	due   []*PendingSignal
	marks map[string]int32
}

func (q *signalQueueFake) Due(context.Context, int) ([]*PendingSignal, error) { return q.due, nil }
func (q *signalQueueFake) Mark(_ context.Context, fireID string, state int32, _ time.Duration) error {
	q.marks[fireID] = state
	return nil
}

type signalsFake struct {
	delivered, gone map[string]bool
	err             map[string]error
}

func (s signalsFake) Signal(_ context.Context, instanceID, _ string, _ map[string]any) (bool, bool, error) {
	return s.delivered[instanceID], s.gone[instanceID], s.err[instanceID]
}

// 流程信号：「还没有节点在等」与数据库故障不消耗放弃额度（无论试了多少次都继续等），自登记起超过
// signalMaxAge 才放弃；送达 / 实例已结束 / 被流程拒绝立即终结。
func TestRunSignalsGivesUpByAgeNotAttempts(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)
	recent, old := now.Add(-2*time.Hour), now.Add(-signalMaxAge-time.Minute)
	q := &signalQueueFake{marks: map[string]int32{}, due: []*PendingSignal{
		{FireID: "waiting-many", InstanceID: "i1", Signal: "done", Attempts: 1000, FiredAt: recent},
		{FireID: "waiting-old", InstanceID: "i2", Signal: "done", Attempts: 3, FiredAt: old},
		{FireID: "delivered", InstanceID: "i3", Signal: "done", Attempts: 1000, FiredAt: old},
		{FireID: "gone", InstanceID: "i4", Signal: "done", FiredAt: recent},
		{FireID: "rejected", InstanceID: "i5", Signal: "done", FiredAt: recent},
		{FireID: "db-error", InstanceID: "i6", Signal: "done", Attempts: 1000, FiredAt: old},
	}}
	uc := &TriggerUsecase{signals: q, workflow: signalsFake{
		delivered: map[string]bool{"i3": true}, gone: map[string]bool{"i4": true},
		err: map[string]error{"i5": detail(ErrTriggerInvalid, "variable clash"), "i6": errors.New("db down")},
	}, now: func() time.Time { return now }}
	if _, err := uc.RunSignals(context.Background()); err == nil {
		t.Fatal("RunSignals() must surface the infrastructure error")
	}
	want := map[string]int32{
		"waiting-many": jc.SignalPending, "waiting-old": jc.SignalGaveUp, "delivered": jc.SignalSent,
		"gone": jc.SignalGaveUp, "rejected": jc.SignalGaveUp, "db-error": jc.SignalPending,
	}
	for id, state := range want {
		if q.marks[id] != state {
			t.Errorf("%s marked %d, want %d", id, q.marks[id], state)
		}
	}
}
