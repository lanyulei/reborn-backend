package monitorpipeline

import (
	"testing"

	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

func eq(label, value string) mc.Matcher {
	return mc.Matcher{Label: label, Op: mc.OpEqual, Value: value}
}

func TestInhibit(t *testing.T) {
	t.Parallel()
	hostDown := mc.InhibitSelector{Matchers: []mc.Matcher{eq("alertname", "HostDown")}}
	in := CompileInhibits([]InhibitRule{
		// b：主机失联抑制同一 ident 的其他告警（目标不限）。
		{ID: "b", GroupPath: "/g1/", Source: hostDown, Equal: []string{"ident"}},
		// a：致命告警抑制同集群的低级别告警（两侧都按级别）。
		{ID: "a", Source: mc.InhibitSelector{Severities: []int32{4}}, Target: mc.InhibitSelector{Severities: []int32{1, 2}}, Equal: []string{"cluster"}},
		// c：按规则限定两侧。
		{ID: "c", Source: mc.InhibitSelector{RuleIDs: []string{"r-src"}}, Target: mc.InhibitSelector{RuleIDs: []string{"r-dst"}}},
		// 不合法的规则被跳过：源为空；目标与相等标签同时为空；匹配器非法。
		{ID: "bad1", Target: hostDown, Equal: []string{"ident"}},
		{ID: "bad2", Source: hostDown},
		{ID: "bad3", Source: mc.InhibitSelector{Matchers: []mc.Matcher{{Label: "a", Op: "=~", Value: "("}}}, Equal: []string{"ident"}},
	})
	if in.Len() != 3 {
		t.Fatalf("Len() = %d, want 3", in.Len())
	}
	active := []AlertRef{
		{ID: "s2", RuleID: "r1", GroupPath: "/g1/", Severity: 3, Labels: map[string]string{"alertname": "HostDown", "ident": "web-01"}},
		{ID: "s1", RuleID: "r1", GroupPath: "/g1/sub/", Severity: 3, Labels: map[string]string{"alertname": "HostDown", "ident": "web-01"}},
		{ID: "s3", RuleID: "r1", GroupPath: "/g2/", Severity: 3, Labels: map[string]string{"alertname": "HostDown", "ident": "web-09"}},
		{ID: "f1", RuleID: "r2", GroupPath: "/g2/", Severity: 4, Labels: map[string]string{"cluster": "c1"}},
		{ID: "src", RuleID: "r-src", GroupPath: "/g3/", Severity: 2, Labels: map[string]string{}, CIID: "ci-sw"},
		{ID: "ci2", RuleID: "r9", GroupPath: "/g3/", Severity: 2, Labels: map[string]string{"k": "v"}, CIID: "ci-sw"},
	}
	snap := in.Snapshot(active)
	for _, tc := range []struct {
		name   string
		target AlertRef
		want   InhibitHit
		hit    bool
	}{
		{
			"same ident in the group: earliest source wins",
			AlertRef{ID: "t1", RuleID: "r5", GroupPath: "/g1/", Severity: 2, Labels: map[string]string{"alertname": "CPUHigh", "ident": "web-01"}},
			InhibitHit{Kind: mc.InhibitRule, InhibitID: "b", AlertID: "s1"},
			true,
		},
		{
			"sub group of the rule's group",
			AlertRef{ID: "t2", RuleID: "r5", GroupPath: "/g1/sub/deep/", Severity: 2, Labels: map[string]string{"ident": "web-01"}},
			InhibitHit{Kind: mc.InhibitRule, InhibitID: "b", AlertID: "s1"},
			true,
		},
		{
			"different ident",
			AlertRef{ID: "t3", RuleID: "r5", GroupPath: "/g1/", Severity: 2, Labels: map[string]string{"ident": "web-02"}},
			InhibitHit{},
			false,
		},
		{
			"target outside the rule's group",
			AlertRef{ID: "t4", RuleID: "r5", GroupPath: "/g2/", Severity: 3, Labels: map[string]string{"ident": "web-01"}},
			InhibitHit{},
			false,
		},
		{
			"source outside the rule's group does not count",
			AlertRef{ID: "t5", RuleID: "r5", GroupPath: "/g1/", Severity: 3, Labels: map[string]string{"ident": "web-09"}},
			InhibitHit{},
			false,
		},
		{
			"an alert never inhibits itself or its peers that are sources too",
			AlertRef{ID: "s2", RuleID: "r1", GroupPath: "/g1/", Severity: 3, Labels: map[string]string{"alertname": "HostDown", "ident": "web-01"}},
			InhibitHit{},
			false,
		},
		{
			"global rule: fatal inhibits warning of the same cluster",
			AlertRef{ID: "t6", RuleID: "r7", GroupPath: "/g9/", Severity: 2, Labels: map[string]string{"cluster": "c1"}},
			InhibitHit{Kind: mc.InhibitRule, InhibitID: "a", AlertID: "f1"},
			true,
		},
		{
			"target severity not selected",
			AlertRef{ID: "t7", RuleID: "r7", GroupPath: "/g9/", Severity: 3, Labels: map[string]string{"cluster": "c1"}},
			InhibitHit{},
			false,
		},
		{
			"other cluster",
			AlertRef{ID: "t8", RuleID: "r7", GroupPath: "/g9/", Severity: 1, Labels: map[string]string{"cluster": "c2"}},
			InhibitHit{},
			false,
		},
		{
			"missing equal label equals a missing one, not a present one",
			AlertRef{ID: "t9", RuleID: "r7", GroupPath: "/g9/", Severity: 1, Labels: map[string]string{}},
			InhibitHit{},
			false,
		},
		{
			"rule-to-rule without equal labels",
			AlertRef{ID: "t10", RuleID: "r-dst", GroupPath: "/g1/", Severity: 4, Labels: map[string]string{"x": "y"}},
			InhibitHit{Kind: mc.InhibitRule, InhibitID: "c", AlertID: "src"},
			true,
		},
		{
			"several rules hit: the smallest rule id wins",
			AlertRef{ID: "t11", RuleID: "r-dst", GroupPath: "/g1/", Severity: 1, Labels: map[string]string{"cluster": "c1", "ident": "web-01"}},
			InhibitHit{Kind: mc.InhibitRule, InhibitID: "a", AlertID: "f1"},
			true,
		},
	} {
		got, hit := snap.Match(&tc.target)
		if hit != tc.hit || got != tc.want {
			t.Errorf("%s: Match() = %+v, %v; want %+v, %v", tc.name, got, hit, tc.want, tc.hit)
		}
	}

	// 缺失的相等标签按空串：两边都没有该标签时视为相等。
	both := CompileInhibits([]InhibitRule{{ID: "x", Source: hostDown, Target: mc.InhibitSelector{Severities: []int32{1}}, Equal: []string{"ident"}}})
	s := both.Snapshot([]AlertRef{{ID: "s", Severity: 3, Labels: map[string]string{"alertname": "HostDown"}}})
	if hit, ok := s.Match(&AlertRef{ID: "t", Severity: 1, Labels: map[string]string{}}); !ok || hit.AlertID != "s" {
		t.Errorf("missing equal labels on both sides: %+v, %v", hit, ok)
	}

	// 同时满足源与目标两侧的告警互不抑制，但仍抑制只满足目标一侧的告警。
	sym := CompileInhibits([]InhibitRule{{
		ID: "x", Source: mc.InhibitSelector{Matchers: []mc.Matcher{eq("tier", "db")}}, Target: mc.InhibitSelector{Matchers: []mc.Matcher{eq("env", "prod")}},
		Equal: []string{"dc"},
	}})
	a1 := AlertRef{ID: "a1", Severity: 2, Labels: map[string]string{"tier": "db", "env": "prod", "dc": "1"}}
	a2 := AlertRef{ID: "a2", Severity: 2, Labels: map[string]string{"tier": "db", "env": "prod", "dc": "1"}}
	s = sym.Snapshot([]AlertRef{a2, a1})
	for _, a := range []AlertRef{a1, a2} {
		if hit, ok := s.Match(&a); ok {
			t.Errorf("alerts matching both sides must not inhibit each other: %s by %+v", a.ID, hit)
		}
	}
	if hit, ok := s.Match(&AlertRef{ID: "web", Severity: 2, Labels: map[string]string{"tier": "web", "env": "prod", "dc": "1"}}); !ok || hit.AlertID != "a1" {
		t.Errorf("target-only alert: %+v, %v", hit, ok)
	}
	// 只满足源一侧的告警优先被报告为抑制者。
	s = sym.Snapshot([]AlertRef{a1, {ID: "z9", Severity: 2, Labels: map[string]string{"tier": "db", "env": "test", "dc": "1"}}})
	if hit, ok := s.Match(&a2); !ok || hit.AlertID != "z9" {
		t.Errorf("pure source must inhibit an alert that matches both sides: %+v, %v", hit, ok)
	}

	// 拓扑抑制。
	if hit, ok := snap.Upstream([]string{"ci-none", "ci-sw"}); !ok || hit != (InhibitHit{Kind: mc.InhibitTopology, AlertID: "ci2"}) {
		t.Errorf("Upstream() = %+v, %v", hit, ok)
	}
	if _, ok := snap.Upstream([]string{"ci-none"}); ok {
		t.Error("Upstream() hit without an upstream alert")
	}
	if _, ok := snap.Upstream(nil); ok {
		t.Error("Upstream(nil) hit")
	}

	// 没有抑制规则时快照仍可用于拓扑抑制；nil 快照不抑制任何事件。
	var none *Inhibitor
	if none.Len() != 0 {
		t.Error("nil Inhibitor Len")
	}
	topo := none.Snapshot(active)
	if _, ok := topo.Match(&AlertRef{ID: "t", Labels: map[string]string{"ident": "web-01"}}); ok {
		t.Error("nil Inhibitor must not match")
	}
	if hit, ok := topo.Upstream([]string{"ci-sw"}); !ok || hit.AlertID != "ci2" {
		t.Errorf("topology on a rule-less snapshot: %+v, %v", hit, ok)
	}
	var nilSnap *InhibitSnapshot
	if _, ok := nilSnap.Match(&AlertRef{}); ok {
		t.Error("nil snapshot Match")
	}
	if _, ok := nilSnap.Upstream([]string{"ci-sw"}); ok {
		t.Error("nil snapshot Upstream")
	}
	if _, ok := in.Snapshot(nil).Match(&AlertRef{ID: "t", GroupPath: "/g1/", Labels: map[string]string{"ident": "web-01"}}); ok {
		t.Error("empty snapshot must not match")
	}
}

func BenchmarkInhibitMatch(b *testing.B) {
	rules := make([]InhibitRule, 0, 20)
	for i := range 20 {
		rules = append(rules, InhibitRule{
			ID: "rule-" + itoa(100+i), Source: mc.InhibitSelector{Matchers: []mc.Matcher{eq("alertname", "Down"+itoa(i))}},
			Target: mc.InhibitSelector{Severities: []int32{1, 2, 3}}, Equal: []string{"ident", "cluster"},
		})
	}
	in := CompileInhibits(rules)
	active := make([]AlertRef, 0, 2000)
	for i := range 2000 {
		active = append(active, AlertRef{
			ID: "alert-" + itoa(100000+i), RuleID: "r", Severity: 4,
			Labels: map[string]string{"alertname": "Down" + itoa(i%20), "ident": "host-" + itoa(i), "cluster": "c" + itoa(i%8)},
		})
	}
	snap := in.Snapshot(active)
	hit := AlertRef{ID: "t", Severity: 2, Labels: map[string]string{"alertname": "CPUHigh", "ident": "host-1999", "cluster": "c7"}}
	miss := AlertRef{ID: "t", Severity: 2, Labels: map[string]string{"alertname": "CPUHigh", "ident": "host-none", "cluster": "c7"}}
	b.Run("hit", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, ok := snap.Match(&hit); !ok {
				b.Fatal("expected a hit")
			}
		}
	})
	b.Run("miss", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, ok := snap.Match(&miss); ok {
				b.Fatal("expected a miss")
			}
		}
	})
	b.Run("snapshot", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			in.Snapshot(active)
		}
	})
}
