package biz

import (
	"context"
	"slices"
	"testing"
)

func newAlertsResolvedUsecase(inc *Incident, firing int, rule *Rule) (*IncidentUsecase, *mapWriter, *recordingEvents) {
	writer := &mapWriter{byID: map[string]*Incident{inc.ID: inc}}
	events := &recordingEvents{}
	d := &Deps{
		Txm: fakeTxm{}, Writer: writer, Members: fakeMembers{}, Events: events, Dir: fakeDirectory{},
		Proposals: &fakeProposals{}, Actions: &claimActions{},
		Links:  &stubLinks{links: []*AlertLink{{IncidentID: inc.ID, AlertID: "a1"}}},
		Alerts: &stubAlerts{firing: firing},
		Rules:  stubRules{rule: rule},
		Notify: &fakeNotify{}, Webhooks: fakeWebhookRepo{}, Logger: testLogger(),
	}
	return NewIncidentUsecase(d, NewSettingsUsecase(fakeSettingRepo{})), writer, events
}

func eventTypes(events []*Event) []string {
	out := make([]string, 0, len(events))
	for _, ev := range events {
		out = append(out, ev.Type)
	}
	return out
}

// 仍有活跃告警：不做任何处理。
func TestOnAlertsResolvedSkipsWhileAlertsStillFiring(t *testing.T) {
	t.Parallel()
	inc := &Incident{ID: "i1", Status: StatusResponding, Severity: SeverityS3, RuleID: "r1"}
	rule := &Rule{ID: "r1", Spec: RuleSpec{Action: RuleAction{AutoResolve: AutoResolveResolve}}}
	uc, writer, events := newAlertsResolvedUsecase(inc, 1, rule)

	if err := uc.onAlertsResolved(context.Background(), "i1", DefaultSettings()); err != nil {
		t.Fatalf("onAlertsResolved() error = %v", err)
	}
	if len(events.appended) != 0 || len(writer.saved) != 0 || inc.Status != StatusResponding {
		t.Fatalf("still-firing alerts must be a no-op: events %+v, saves %d, status %d", eventTypes(events.appended), len(writer.saved), inc.Status)
	}
}

// 告警抖动（恢复 → 再触发被归并丢弃 → 再恢复）：只提示的故障此前每个恢复信号都写一条「关联告警已全部恢复」
// 并递增版本。上一次记录之后关联没有变化时不重复记录、不写回；有新告警关联进来后再次全部恢复才重新记录。
func TestOnAlertsResolvedRecordsOnceWhileLinksUnchanged(t *testing.T) {
	t.Parallel()
	inc := &Incident{ID: "i1", Status: StatusResponding, Severity: SeverityS1} // S1：只提示
	uc, writer, events := newAlertsResolvedUsecase(inc, 0, nil)
	settings := DefaultSettings()

	if err := uc.onAlertsResolved(context.Background(), "i1", settings); err != nil {
		t.Fatalf("onAlertsResolved() error = %v", err)
	}
	if !slices.Equal(eventTypes(events.appended), []string{EventAlertResolved}) || len(writer.saved) != 1 {
		t.Fatalf("first all-resolved: events %v, saves %d; want one alert_resolved and one save", eventTypes(events.appended), len(writer.saved))
	}

	// 抖动：备注、公告都不改变关联，时间线里最近的相关记录仍是上一条「全部恢复」。
	events.tail = append(slices.Clone(events.appended), &Event{Type: EventComment}, &Event{Type: EventUpdate})
	for range 3 {
		if err := uc.onAlertsResolved(context.Background(), "i1", settings); err != nil {
			t.Fatalf("onAlertsResolved() error = %v", err)
		}
	}
	if len(events.appended) != 1 || len(writer.saved) != 1 {
		t.Fatalf("flapping must not repeat the record: events %v, saves %d", eventTypes(events.appended), len(writer.saved))
	}

	// 新告警关联了进来，之后再次全部恢复：重新记录。
	events.tail = append(slices.Clone(events.appended), &Event{Type: EventAlertAttach}, &Event{Type: EventComment})
	if err := uc.onAlertsResolved(context.Background(), "i1", settings); err != nil {
		t.Fatalf("onAlertsResolved() error = %v", err)
	}
	if len(events.appended) != 2 || len(writer.saved) != 2 {
		t.Fatalf("a new link set must be recorded again: events %v, saves %d", eventTypes(events.appended), len(writer.saved))
	}
}

// 回看的窗口里找不到相关记录（更早的记不到）：按「没记录过」处理，照常记录，不会漏记。
func TestOnAlertsResolvedRecordsWhenHistoryNotVisible(t *testing.T) {
	t.Parallel()
	inc := &Incident{ID: "i1", Status: StatusResponding, Severity: SeverityS1}
	uc, _, events := newAlertsResolvedUsecase(inc, 0, nil)
	noise := make([]*Event, 0, resolvedLookback)
	for range resolvedLookback {
		noise = append(noise, &Event{Type: EventComment})
	}
	events.tail = noise

	if err := uc.onAlertsResolved(context.Background(), "i1", DefaultSettings()); err != nil {
		t.Fatalf("onAlertsResolved() error = %v", err)
	}
	if !slices.Equal(eventTypes(events.appended), []string{EventAlertResolved}) {
		t.Fatalf("events = %v, want one alert_resolved when the previous record is beyond the lookback", eventTypes(events.appended))
	}
}

// 自动缓解的规则：第一次记录并缓解；之后故障已是「已缓解」，同样的抖动不再写时间线。故障被改回活跃后再次
// 全部恢复（状态真的要变）才再记录一次。
func TestOnAlertsResolvedMitigateModeDoesNotRepeat(t *testing.T) {
	t.Parallel()
	inc := &Incident{ID: "i1", Status: StatusOpen, Severity: SeverityS3, RuleID: "r1"}
	rule := &Rule{ID: "r1", Spec: RuleSpec{Action: RuleAction{AutoResolve: AutoResolveMitigate}}}
	uc, writer, events := newAlertsResolvedUsecase(inc, 0, rule)
	settings := DefaultSettings() // S3 允许自动恢复

	if err := uc.onAlertsResolved(context.Background(), "i1", settings); err != nil {
		t.Fatalf("onAlertsResolved() error = %v", err)
	}
	if inc.Status != StatusMitigated || !slices.Equal(eventTypes(events.appended), []string{EventAlertResolved, EventStatus}) {
		t.Fatalf("first all-resolved: status %d, events %v; want mitigated with alert_resolved + status", inc.Status, eventTypes(events.appended))
	}

	events.tail = slices.Clone(events.appended)
	if err := uc.onAlertsResolved(context.Background(), "i1", settings); err != nil {
		t.Fatalf("onAlertsResolved() error = %v", err)
	}
	if len(events.appended) != 2 || len(writer.saved) != 1 {
		t.Fatalf("flapping on a mitigated incident must write nothing: events %v, saves %d", eventTypes(events.appended), len(writer.saved))
	}

	inc.Status = StatusResponding // 指挥把它改回处理中
	if err := uc.onAlertsResolved(context.Background(), "i1", settings); err != nil {
		t.Fatalf("onAlertsResolved() error = %v", err)
	}
	if inc.Status != StatusMitigated || len(events.appended) != 4 {
		t.Fatalf("a real status change must be recorded again: status %d, events %v", inc.Status, eventTypes(events.appended))
	}
}

// 自动恢复：第一次全部恢复即把故障置为已恢复并记录；之后的恢复信号不再处理（故障已不活跃）。
func TestOnAlertsResolvedResolveModeUnchanged(t *testing.T) {
	t.Parallel()
	inc := &Incident{ID: "i1", Status: StatusOpen, Severity: SeverityS3, RuleID: "r1"}
	rule := &Rule{ID: "r1", Spec: RuleSpec{Action: RuleAction{AutoResolve: AutoResolveResolve}}}
	uc, writer, events := newAlertsResolvedUsecase(inc, 0, rule)
	settings := DefaultSettings()

	if err := uc.onAlertsResolved(context.Background(), "i1", settings); err != nil {
		t.Fatalf("onAlertsResolved() error = %v", err)
	}
	if inc.Status != StatusResolved || !slices.Equal(eventTypes(events.appended), []string{EventAlertResolved, EventStatus}) {
		t.Fatalf("first all-resolved: status %d, events %v; want resolved with alert_resolved + status", inc.Status, eventTypes(events.appended))
	}
	if err := uc.onAlertsResolved(context.Background(), "i1", settings); err != nil {
		t.Fatalf("onAlertsResolved() error = %v", err)
	}
	if len(events.appended) != 2 || len(writer.saved) != 1 {
		t.Fatalf("a resolved incident must not be touched again: events %v, saves %d", eventTypes(events.appended), len(writer.saved))
	}
}

// 一条关联告警仍在告警中、只是被屏蔽 / 抑制了：不算恢复。此前按「告警中且未被屏蔽 / 抑制」计数，其余告警
// 恢复后故障被当成「全部恢复」而自动恢复。
func TestOnAlertsResolvedTreatsMutedFiringAlertsAsUnresolved(t *testing.T) {
	t.Parallel()
	inc := &Incident{ID: "i1", Status: StatusResponding, Severity: SeverityS3, RuleID: "r1"}
	rule := &Rule{ID: "r1", Spec: RuleSpec{Action: RuleAction{AutoResolve: AutoResolveResolve}}}
	uc, writer, events := newAlertsResolvedUsecase(inc, 0, rule)
	alerts, _ := uc.d.Alerts.(*stubAlerts)
	alerts.mutedFiring = 1

	if err := uc.onAlertsResolved(context.Background(), "i1", DefaultSettings()); err != nil {
		t.Fatalf("onAlertsResolved() error = %v", err)
	}
	if len(events.appended) != 0 || len(writer.saved) != 0 || inc.Status != StatusResponding {
		t.Fatalf("a muted alert that is still firing must keep the incident open: events %v, saves %d, status %d",
			eventTypes(events.appended), len(writer.saved), inc.Status)
	}

	// 那条告警也恢复了：照常自动恢复。
	alerts.mutedFiring = 0
	if err := uc.onAlertsResolved(context.Background(), "i1", DefaultSettings()); err != nil {
		t.Fatalf("onAlertsResolved() error = %v", err)
	}
	if inc.Status != StatusResolved {
		t.Fatalf("status = %d, want auto-resolved once nothing is firing", inc.Status)
	}
}
