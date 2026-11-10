package biz

import (
	"testing"
	"time"
)

func TestMatchPattern(t *testing.T) {
	t.Parallel()
	cases := []struct {
		pattern, event string
		want           bool
	}{
		{"*", "anything.here", true},
		{"monitor.alert.firing", "monitor.alert.firing", true},
		{"monitor.alert.firing", "monitor.alert.resolved", false},
		{"monitor.alert.*", "monitor.alert.firing", true},
		{"monitor.alert.*", "monitor.alert", false},
		{"monitor.alert.*", "monitor.alerts.firing", false},
		{"monitor.*", "monitor.alert.firing", true},
	}
	for _, c := range cases {
		if got := MatchPattern(c.pattern, c.event); got != c.want {
			t.Errorf("MatchPattern(%q, %q) = %v, want %v", c.pattern, c.event, got, c.want)
		}
	}
}

func TestSnapshotMatchRoute(t *testing.T) {
	t.Parallel()
	routes := []*Route{
		{ID: "r-star", BizID: "default", EventPattern: "*", SeverityMin: 1, Priority: 5, Enabled: true},
		{ID: "r-prefix", BizID: "default", EventPattern: "monitor.alert.*", SeverityMin: 2, Priority: 5, Enabled: true},
		{ID: "r-exact", BizID: "default", EventPattern: "monitor.alert.firing", SeverityMin: 1, Priority: 5, Enabled: true},
		{
			ID: "r-top", BizID: "default", EventPattern: "monitor.alert.firing", SeverityMin: 3, Priority: 1, Enabled: true,
			TagMatch: map[string]string{"env": "prod", "replicas": "3"},
		},
		{ID: "r-off", BizID: "default", EventPattern: "*", SeverityMin: 1, Priority: 1, Enabled: false},
		{ID: "r-other", BizID: "monitor", EventPattern: "*", SeverityMin: 1, Priority: 1, Enabled: true},
	}
	snap := testSnapshot(routes, nil, nil)
	cases := []struct {
		name    string
		event   string
		sev     int32
		payload map[string]any
		want    string
	}{
		{"priority 1 with matching tags", "monitor.alert.firing", 4, map[string]any{"env": "prod", "replicas": float64(3)}, "r-top"},
		{"tags mismatch falls to exact", "monitor.alert.firing", 4, map[string]any{"env": "staging", "replicas": float64(3)}, "r-exact"},
		{"severity below top route", "monitor.alert.firing", 2, map[string]any{"env": "prod", "replicas": float64(3)}, "r-exact"},
		{"exact beats prefix at same priority", "monitor.alert.firing", 2, nil, "r-exact"},
		{"prefix beats star", "monitor.alert.resolved", 2, nil, "r-prefix"},
		{"prefix severity floor -> star", "monitor.alert.resolved", 1, nil, "r-star"},
		{"disabled/other biz ignored", "resource.change", 1, nil, "r-star"},
	}
	for _, c := range cases {
		got := snap.MatchRoute("default", c.event, c.sev, c.payload)
		if got == nil || got.ID != c.want {
			t.Errorf("%s: MatchRoute = %v, want %s", c.name, got, c.want)
		}
	}
	if snap.MatchRoute("unknown-biz", "x", 1, nil) != nil {
		t.Error("unknown biz must not match")
	}
}

func TestSnapshotSilenced(t *testing.T) {
	t.Parallel()
	silences := []*Silence{
		{
			ID: "s-night", BizID: "default", EventPattern: "monitor.*", SeverityMax: 2, Enabled: true,
			Window: &SilenceWindow{Start: "22:00", End: "08:00", Tz: "Asia/Shanghai"},
		},
		{ID: "s-all", BizID: "default", EventPattern: "resource.change", SeverityMax: 4, Enabled: true},
		{ID: "s-off", BizID: "default", EventPattern: "*", SeverityMax: 4, Enabled: false},
	}
	snap := testSnapshot(nil, silences, nil)
	sh, _ := time.LoadLocation("Asia/Shanghai")
	night := time.Date(2026, 11, 10, 23, 30, 0, 0, sh)
	morning := time.Date(2026, 11, 10, 7, 59, 0, 0, sh)
	day := time.Date(2026, 11, 10, 12, 0, 0, 0, sh)
	cases := []struct {
		name  string
		event string
		sev   int32
		at    time.Time
		want  bool
	}{
		{"night window hit", "monitor.alert.firing", 2, night, true},
		{"cross-midnight morning hit", "monitor.alert.firing", 1, morning, true},
		{"daytime not silenced", "monitor.alert.firing", 2, day, false},
		{"severity above max", "monitor.alert.firing", 3, night, false},
		{"always-on silence", "resource.change", 4, day, true},
		{"disabled ignored", "user.status", 1, night, false},
	}
	for _, c := range cases {
		if got := snap.Silenced("default", c.event, c.sev, c.at); got != c.want {
			t.Errorf("%s: Silenced = %v, want %v", c.name, got, c.want)
		}
	}
	// 非法时刻：不命中（宁放行）。
	if (&SilenceWindow{Start: "25:00", End: "08:00"}).Contains(night) {
		t.Error("invalid window must not match")
	}
}

func TestSnapshotTemplateChain(t *testing.T) {
	t.Parallel()
	tpls := []*Template{
		{ID: "t1", BizID: "default", EventType: "a.b", ChannelType: "email", Enabled: true},
		{ID: "t2", BizID: "default", EventType: "a.b", ChannelType: "*", Enabled: true},
		{ID: "t3", BizID: "default", EventType: "*", ChannelType: "webhook", Enabled: true},
		{ID: "t4", BizID: "default", EventType: "*", ChannelType: "*", Enabled: false},
	}
	snap := testSnapshot(nil, nil, tpls)
	cases := []struct{ event, channel, want string }{
		{"a.b", "email", "t1"},
		{"a.b", "dingtalk", "t2"},
		{"x.y", "webhook", "t3"},
		{"x.y", "email", ""},
	}
	for _, c := range cases {
		got := snap.Template("default", c.event, c.channel)
		id := ""
		if got != nil {
			id = got.ID
		}
		if id != c.want {
			t.Errorf("Template(%s,%s) = %q, want %q", c.event, c.channel, id, c.want)
		}
	}
}

// TestTagsMatchNestedPath tag_match 键先按顶层字面键查，再按点分路径下钻嵌套对象。
func TestTagsMatchNestedPath(t *testing.T) {
	t.Parallel()
	payload := map[string]any{
		"env":       "prod",
		"labels":    map[string]any{"env": "staging", "region": "cn-north", "replicas": float64(3)},
		"dotted.ok": "literal",
	}
	cases := []struct {
		tags map[string]string
		want bool
	}{
		{map[string]string{"env": "prod"}, true},
		{map[string]string{"labels.env": "staging"}, true},
		{map[string]string{"labels.replicas": "3"}, true},
		{map[string]string{"dotted.ok": "literal"}, true}, // 顶层字面键优先
		{map[string]string{"labels.region": "cn-south"}, false},
		{map[string]string{"labels.missing": "x"}, false},
		{map[string]string{"env.deeper": "x"}, false}, // 非对象不可下钻
		{map[string]string{"env": "prod", "labels.region": "cn-north"}, true},
	}
	for _, c := range cases {
		if got := tagsMatch(c.tags, payload); got != c.want {
			t.Errorf("tagsMatch(%v) = %v, want %v", c.tags, got, c.want)
		}
	}
}
