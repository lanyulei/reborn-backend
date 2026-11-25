package biz

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"
)

// singleIncidents 故障读取桩：List 恒返回给定的那一条（风暴归并按规则找最近开出的活跃故障）。
type singleIncidents struct {
	IncidentReader
	item *Incident
}

func (r singleIncidents) List(context.Context, IncidentFilter, int64, int64) ([]*Incident, int64, error) {
	return []*Incident{r.item}, 1, nil
}

// Get 按告警表取一条；不在表里按不存在处理。
func (a *stubAlerts) Get(_ context.Context, id string) (*AlertRef, error) {
	if r := a.byID[id]; r != nil {
		return r, nil
	}
	return nil, ErrAlertNotFound
}

// ownedLinks 关联桩：告警已归别的活跃故障时，与数据层的部分唯一索引一致返回 ErrAlertLinked。
type ownedLinks struct{ stubLinks }

func (l *ownedLinks) AttachTx(_ context.Context, _ Tx, link *AlertLink) (bool, error) {
	if l.owner[link.AlertID] != "" {
		return false, ErrAlertLinked
	}
	l.attached = append(l.attached, link.AlertID)
	return true, nil
}

// 风暴归并通知里的关联告警数是归并之后的。commit 取关联数走连接池，本事务提交前读不到刚写入的关联：
// 此前通知里带的恒是归并之前的数。已被别的活跃故障关联的、已不存在的告警不计数。
func TestStormNotificationCountsAlertsAttachedInThisMutation(t *testing.T) {
	t.Parallel()
	target := &Incident{ID: "i1", Number: "7", Title: "数据库慢查询", Severity: SeverityS2, Status: StatusOpen, Source: SourceManual, CommanderID: "u1", OccurTime: time.Now()}
	notify := &fakeNotify{}
	links := &ownedLinks{stubLinks{owner: map[string]string{"a2": "iX"}, links: []*AlertLink{{AlertID: "old-1"}, {AlertID: "old-2"}}}}
	alerts := &stubAlerts{byID: map[string]*AlertRef{
		"a1": {ID: "a1", Status: 1},
		"a2": {ID: "a2", Status: 1}, // 已归别的活跃故障
	}}
	writer := &mapWriter{byID: map[string]*Incident{target.ID: target}}
	d := &Deps{
		Txm: fakeTxm{}, Writer: writer, Members: fakeMembers{}, Admins: fakeAdmins{}, Events: &recordingEvents{}, Links: links, Alerts: alerts,
		Dir: fakeDirectory{active: []string{"u1"}}, Notify: notify, Logger: testLogger(),
		Incidents:  singleIncidents{item: target},
		Candidates: &stubCandidates{c: &Candidate{ID: "c1"}},
	}
	settings := NewSettingsUsecase(fakeSettingRepo{})
	uc := NewCorrelatorUsecase(d, NewIncidentUsecase(d, settings), settings)

	err := uc.storm(context.Background(), &Rule{ID: "r1", Spec: RuleSpec{Name: "慢查询"}}, &AlertRef{ID: "a3"},
		&Candidate{ID: "c1", AlertIDs: []string{"a1", "a2", "gone"}}, DefaultSettings())
	if err != nil {
		t.Fatalf("storm() error = %v", err)
	}
	if !slices.Equal(links.attached, []string{"a1"}) {
		t.Fatalf("attached alerts = %v, want only the free one", links.attached)
	}
	if len(notify.events) != 1 {
		t.Fatalf("notify events = %d, want the storm notification", len(notify.events))
	}
	var payload struct {
		AlertCount int `json:"alert_count"`
	}
	if err := json.Unmarshal(notify.events[0].Payload, &payload); err != nil {
		t.Fatalf("payload = %s: %v", notify.events[0].Payload, err)
	}
	if payload.AlertCount != 3 {
		t.Fatalf("alert_count = %d, want 3 (2 existing + 1 attached by the storm merge)", payload.AlertCount)
	}
}
