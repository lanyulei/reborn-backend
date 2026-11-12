package notifycontract

import (
	"testing"
	"time"
)

func TestPatterns(t *testing.T) {
	t.Parallel()
	if EventTypePattern.String() != EventTypePatternLiteral || EventPatternPattern.String() != EventPatternPatternLiteral ||
		TemplateEventTypePattern.String() != TemplateEventTypePatternLiteral {
		t.Fatal("compiled patterns must match their literals")
	}
	cases := []struct {
		pattern, event string
		want           bool
	}{
		{"*", "a.b", true},
		{"monitor.alert.*", "monitor.alert.firing", true},
		{"monitor.alert.*", "monitor.alert", false},
		{"monitor.alert.firing", "monitor.alert.firing", true},
		{"monitor.alert.firing", "monitor.alert.resolved", false},
	}
	for _, c := range cases {
		if MatchPattern(c.pattern, c.event) != c.want {
			t.Errorf("MatchPattern(%q,%q) != %v", c.pattern, c.event, c.want)
		}
	}
	if PatternSpecificity("*") != 2 || PatternSpecificity("a.*") != 1 || PatternSpecificity("a.b") != 0 {
		t.Fatal("specificity order drifted")
	}
	if !EventTypePattern.MatchString("monitor.alert.firing") || EventTypePattern.MatchString("monitor.*") ||
		!EventPatternPattern.MatchString("monitor.*") || EventPatternPattern.MatchString("monitor.*.firing") {
		t.Fatal("event regexes drifted")
	}
}

func TestTagsMatchAndWindow(t *testing.T) {
	t.Parallel()
	payload := map[string]any{"env": "prod", "n": float64(3), "ok": true, "labels": map[string]any{"team": "ops"}}
	cases := []struct {
		tags map[string]string
		want bool
	}{
		{nil, true},
		{map[string]string{"env": "prod"}, true},
		{map[string]string{"n": "3"}, true},
		{map[string]string{"ok": "true"}, true},
		{map[string]string{"labels.team": "ops"}, true},
		{map[string]string{"labels.team": "dev"}, false},
		{map[string]string{"missing": "x"}, false},
	}
	for _, c := range cases {
		if got := TagsMatch(c.tags, payload); got != c.want {
			t.Errorf("TagsMatch(%v) = %v, want %v", c.tags, got, c.want)
		}
	}
	if TagsMatch(map[string]string{"env": "prod"}, nil) {
		t.Fatal("nil payload must not match non-empty tags")
	}
	noon := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	if !WindowContains(SilenceWindow{Start: "09:00", End: "18:00"}, noon) || WindowContains(SilenceWindow{Start: "13:00", End: "18:00"}, noon) {
		t.Fatal("daytime window")
	}
	if !WindowContains(SilenceWindow{Start: "22:00", End: "08:00", Tz: "Asia/Shanghai"}, time.Date(2026, 1, 1, 15, 0, 0, 0, time.UTC)) {
		t.Fatal("cross-midnight window in local tz (23:00 Shanghai) must match")
	}
	if WindowContains(SilenceWindow{Start: "25:00", End: "08:00"}, noon) {
		t.Fatal("unparseable clock must not match")
	}
}

func TestRateLimitAndDefaults(t *testing.T) {
	t.Parallel()
	if r := (RateLimit{QPS: 5}).Rate(); r != 5 {
		t.Fatalf("qps rate = %v", r)
	}
	if r := (RateLimit{PerMinute: 20}).Rate(); r < 0.33 || r > 0.34 {
		t.Fatalf("per-minute rate = %v", r)
	}
	if (RateLimit{}).Rate() != 0 || (RateLimit{Burst: 7}).Capacity() != 7 || (RateLimit{PerMinute: 20}).Capacity() != 1 || (RateLimit{QPS: 5}).Capacity() != 5 {
		t.Fatal("capacity/rate semantics")
	}
	if d := DefaultRateLimit(ChannelTypeDingtalk); d.PerMinute != 20 || d.Burst != 1 {
		t.Fatalf("dingtalk default = %+v", d)
	}
	if d := DefaultRateLimit(ChannelTypeEmail); d.Rate() != 0 {
		t.Fatalf("email default = %+v", d)
	}
	if e := EffectiveRateLimit(ChannelTypeWecom, RateLimit{QPS: 2}); e.QPS != 2 {
		t.Fatalf("explicit must win: %+v", e)
	}
	if e := EffectiveRateLimit(ChannelTypeFeishu, RateLimit{}); e.PerMinute != 100 || e.Burst != 5 {
		t.Fatalf("feishu default = %+v", e)
	}
}

func TestTemplateFuncsAndSSRF(t *testing.T) {
	t.Parallel()
	f := TemplateFuncs()
	if f["severity"].(func(int32) string)(4) != "critical" || SeverityLabel(9) != "9" {
		t.Fatal("severity label")
	}
	if got := f["trunc"].(func(string, int) string)("你好世界", 2); got != "你好…" {
		t.Fatalf("trunc = %q", got)
	}
	if got := f["json"].(func(any) string)(map[string]any{"a": 1}); got != `{"a":1}` {
		t.Fatalf("json = %q", got)
	}
	if got := f["fmtTime"].(func(time.Time, ...string) string)(time.Unix(0, 0).UTC(), "2006"); got != "1970" {
		t.Fatalf("fmtTime = %q", got)
	}
	for _, h := range []string{"localhost", "LOCALHOST.", "a.localhost", "127.0.0.1", "10.0.0.1", "172.16.5.5", "192.168.1.1", "169.254.169.254", "100.64.0.9", "::1", "fe80::1", "fc00::1", "0.0.0.0", ""} {
		if !UnsafeWebhookHost(h) {
			t.Errorf("%q must be unsafe", h)
		}
	}
	for _, h := range []string{"hooks.example.com", "8.8.8.8", "2606:4700::1111", "100.128.0.1"} {
		if UnsafeWebhookHost(h) {
			t.Errorf("%q must be allowed", h)
		}
	}
}

func TestEffectiveFormat(t *testing.T) {
	t.Parallel()
	cases := []struct{ ch, in, want string }{
		{ChannelTypeEmail, TemplateFormatHTML, TemplateFormatHTML},
		{ChannelTypeEmail, TemplateFormatCard, TemplateFormatText},
		{ChannelTypeEmail, "", TemplateFormatText},
		{ChannelTypeDingtalk, TemplateFormatCard, TemplateFormatCard},
		{ChannelTypeWecom, TemplateFormatHTML, TemplateFormatMarkdown},
		{ChannelTypeFeishu, TemplateFormatText, TemplateFormatText},
		{ChannelTypeWebhook, TemplateFormatHTML, TemplateFormatHTML},
		{ChannelTypeWebhook, "weird", TemplateFormatText},
	}
	for _, c := range cases {
		if got := EffectiveFormat(c.ch, c.in); got != c.want {
			t.Errorf("EffectiveFormat(%s,%s) = %s, want %s", c.ch, c.in, got, c.want)
		}
	}
}
