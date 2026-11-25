package biz

import (
	"context"
	"reflect"
	"slices"
	"testing"
)

// legacyCandidateAlerts 改写前 candidateViews 对单个候选的组装方式（逐候选读取、逐条判断），作对照。
func legacyCandidateAlerts(t *testing.T, uc *IncidentUsecase, userID string, c *Candidate) []*AlertItem {
	t.Helper()
	links := make([]*AlertLink, 0, min(len(c.AlertIDs), 5))
	for _, id := range c.AlertIDs[:min(len(c.AlertIDs), 5)] {
		links = append(links, &AlertLink{AlertID: id})
	}
	items, err := uc.alertItems(context.Background(), userID, links)
	if err != nil {
		t.Fatalf("alertItems() error = %v", err)
	}
	return items
}

// 候选故障列表：一页候选的告警合并成一次读取、数据范围只解析一次，每个候选得到的内容与逐个组装相同
// （前 5 条、保持候选里的顺序、已不存在的告警留空、看不到的分组不可查看）。
func TestCandidateViewsBatchesAlertLookupWithSameResult(t *testing.T) {
	t.Parallel()
	byID := map[string]*AlertRef{
		"a1": {ID: "a1", GroupID: "g1", Status: 1},
		"a2": {ID: "a2", GroupID: "g2", Status: 2},
		"a3": {ID: "a3", GroupID: "g1", Status: 1},
		"a4": {ID: "a4", GroupID: "g1"}, "a5": {ID: "a5", GroupID: "g1"}, "a6": {ID: "a6", GroupID: "g1"}, "a7": {ID: "a7", GroupID: "g1"},
	}
	items := []*Candidate{
		{ID: "c1", RuleID: "r1", AlertIDs: []string{"a2", "a1", "gone"}},
		{ID: "c2", RuleID: "r1", AlertIDs: []string{"a1", "a3", "a4", "a5", "a6", "a7"}}, // 只展示前 5 条
		{ID: "c3", RuleID: "r1"}, // 没有告警
	}
	for _, userID := range []string{"u1", ""} {
		alerts := &stubAlerts{byID: byID, unreadable: map[string]bool{"g2": true}}
		d := &Deps{Alerts: alerts, Rules: stubRules{rule: &Rule{ID: "r1", Spec: RuleSpec{Name: "慢查询"}}}, Logger: testLogger()}
		uc := NewIncidentUsecase(d, NewSettingsUsecase(fakeSettingRepo{}))

		views, err := uc.candidateViews(context.Background(), userID, items)
		if err != nil {
			t.Fatalf("candidateViews() error = %v", err)
		}
		if len(alerts.listCalls) != 1 || !slices.Equal(alerts.listCalls[0], []string{"a2", "a1", "gone", "a3", "a4", "a5", "a6"}) {
			t.Fatalf("alert lookups = %v, want one batch with every previewed alert once", alerts.listCalls)
		}
		wantReader := 1
		if userID == "" {
			wantReader = 0 // 匿名视角不判断可见性，也就不解析范围
		}
		if alerts.readerCalls != wantReader {
			t.Fatalf("user %q: scope resolved %d times, want %d", userID, alerts.readerCalls, wantReader)
		}
		if len(views) != len(items) {
			t.Fatalf("views = %d, want %d", len(views), len(items))
		}
		for i, c := range items {
			if views[i].Candidate != c || views[i].RuleName != "慢查询" {
				t.Fatalf("view %d = %+v, want candidate %s with its rule name", i, views[i], c.ID)
			}
			want := legacyCandidateAlerts(t, uc, userID, c)
			if views[i].Alerts == nil || !reflect.DeepEqual(views[i].Alerts, want) {
				t.Fatalf("user %q candidate %s: alerts differ from the per-candidate assembly\n got %s\nwant %s", userID, c.ID, describeItems(views[i].Alerts), describeItems(want))
			}
		}
	}
}

func describeItems(items []*AlertItem) string {
	out := ""
	for _, it := range items {
		out += it.Link.AlertID
		if it.Alert == nil {
			out += "(missing)"
		}
		if it.CanView {
			out += "(view)"
		}
		out += " "
	}
	return out
}

// 一页关联告警只解析一次数据范围；没有要判断的告警（列表为空）时不解析。
func TestAlertItemsResolvesScopeOncePerRequest(t *testing.T) {
	t.Parallel()
	alerts := &stubAlerts{byID: map[string]*AlertRef{
		"a1": {ID: "a1", GroupID: "g1"}, "a2": {ID: "a2", GroupID: "g2"}, "a3": {ID: "a3", GroupID: "g1"},
	}, unreadable: map[string]bool{"g2": true}}
	uc := NewIncidentUsecase(&Deps{Alerts: alerts, Logger: testLogger()}, NewSettingsUsecase(fakeSettingRepo{}))

	items, err := uc.alertItems(context.Background(), "u1", []*AlertLink{{AlertID: "a1"}, {AlertID: "a2"}, {AlertID: "a3"}, {AlertID: "gone"}})
	if err != nil {
		t.Fatalf("alertItems() error = %v", err)
	}
	got := make([]bool, 0, len(items))
	for _, it := range items {
		got = append(got, it.CanView)
	}
	if !slices.Equal(got, []bool{true, false, true, false}) || alerts.readerCalls != 1 {
		t.Fatalf("can view = %v with %d scope resolutions; want [true false true false] with exactly one", got, alerts.readerCalls)
	}

	if _, err := uc.alertItems(context.Background(), "u1", nil); err != nil || alerts.readerCalls != 1 {
		t.Fatalf("an empty page must not resolve the scope: err %v, resolutions %d", err, alerts.readerCalls)
	}
}

// 手动选告警开故障 / 关联告警：有一条不在数据范围内即按不存在处理，范围同样只解析一次。
func TestReadableAlertsRejectsAlertOutsideScope(t *testing.T) {
	t.Parallel()
	alerts := &stubAlerts{byID: map[string]*AlertRef{"a1": {ID: "a1", GroupID: "g1"}, "a2": {ID: "a2", GroupID: "g2"}, "a3": {ID: "a3", GroupID: "g1"}}}
	uc := NewIncidentUsecase(&Deps{Alerts: alerts, Logger: testLogger()}, NewSettingsUsecase(fakeSettingRepo{}))

	refs, err := uc.readableAlerts(context.Background(), "u1", []string{"a1", "a2", "a3", "a1"})
	if err != nil || len(refs) != 3 || alerts.readerCalls != 1 {
		t.Fatalf("readableAlerts() = %d refs, %v with %d scope resolutions; want 3 refs with one", len(refs), err, alerts.readerCalls)
	}
	alerts.unreadable = map[string]bool{"g2": true}
	if _, err := uc.readableAlerts(context.Background(), "u1", []string{"a1", "a2"}); !ErrAlertNotFound.Is(err) {
		t.Fatalf("readableAlerts() error = %v, want ErrAlertNotFound for an alert outside the scope", err)
	}
	if _, err := uc.readableAlerts(context.Background(), "u1", []string{"a1", "gone"}); !ErrAlertNotFound.Is(err) {
		t.Fatalf("readableAlerts() error = %v, want ErrAlertNotFound for a missing alert", err)
	}
}
