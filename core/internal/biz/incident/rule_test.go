package biz

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestCorrelationKey(t *testing.T) {
	t.Parallel()
	a := &AlertRef{ID: "a1", RuleID: "r1", GroupID: "g1", CIID: "ci1", Labels: map[string]string{"service": "pay", "env": "prod"}}
	cases := []struct {
		name     string
		keys     []string
		upstream []string
		want     string
	}{
		{"empty keys merge everything", nil, nil, "*"},
		{"business and rule", []string{"business", "rule"}, nil, "b=biz1|r=r1"},
		{"ci uses the top-most upstream", []string{"ci"}, []string{"mid", "top"}, "c=top"},
		{"ci without upstream", []string{"ci"}, nil, "c=ci1"},
		{"labels", []string{"label:service", "label:missing"}, nil, "l:service=pay|l:missing="},
		{"group", []string{"group"}, nil, "g=g1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			spec := &RuleSpec{Correlation: RuleCorrelation{Keys: c.keys}}
			if got := correlationKey(spec, a, "biz1", c.upstream); got != c.want {
				t.Fatalf("correlationKey() = %q, want %q", got, c.want)
			}
		})
	}
	long := &AlertRef{Labels: map[string]string{"x": strings.Repeat("v", 400)}}
	if got := correlationKey(&RuleSpec{Correlation: RuleCorrelation{Keys: []string{"label:x"}}}, long, "", nil); len(got) != 255 {
		t.Fatalf("correlationKey() length = %d, want 255", len(got))
	}
}

func TestSeverityFor(t *testing.T) {
	t.Parallel()
	if got := severityFor(&RuleSpec{Incident: RuleIncident{Severity: SeverityS1}}, nil); got != SeverityS1 {
		t.Fatalf("fixed severity = %d, want S1", got)
	}
	cases := []struct {
		alerts []int32
		want   int32
	}{
		{[]int32{4, 2}, SeverityS2},
		{[]int32{3}, SeverityS3},
		{[]int32{2, 1}, SeverityS4},
		{nil, SeverityS4},
	}
	for _, c := range cases {
		refs := make([]*AlertRef, 0, len(c.alerts))
		for _, s := range c.alerts {
			refs = append(refs, &AlertRef{Severity: s})
		}
		if got := severityFor(&RuleSpec{}, refs); got != c.want {
			t.Errorf("severityFor(%v) = %d, want %d", c.alerts, got, c.want)
		}
	}
}

func TestRenderTitle(t *testing.T) {
	t.Parallel()
	a := &AlertRef{RuleName: "CPU 过高", Labels: map[string]string{"host": "web-1"}}
	cases := []struct {
		name, tpl, business string
		want                string
	}{
		{"default without business", "", "", "CPU 过高"},
		{"default with business", "", "支付", "支付：CPU 过高"},
		{"placeholders", "{{business}} {{rule}} {{labels.host}} x{{count}}", "支付", "支付 CPU 过高 web-1 x3"},
		{"unknown label removed", "{{rule}} {{labels.nope}}", "", "CPU 过高"},
		{"empty result falls back to rule name", "{{labels.nope}}", "", "CPU 过高"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if got := renderTitle(c.tpl, a, c.business, 3); got != c.want {
				t.Fatalf("renderTitle() = %q, want %q", got, c.want)
			}
		})
	}
	// 超长标题截到不超过 255 个字符（列宽 varchar(255)），末位是省略号。
	if got := renderTitle(strings.Repeat("x", 300), a, "", 1); utf8.RuneCountInString(got) != 255 || !strings.HasSuffix(got, "…") {
		t.Fatalf("renderTitle() = %d runes, want at most 255 with a trailing ellipsis", utf8.RuneCountInString(got))
	}
}

// truncate 的返回值不得超过列宽：此前超长时返回 n+1 个字符，按 255 截断的标题落库报 22001。
func TestTruncateNeverExceedsLimit(t *testing.T) {
	t.Parallel()
	if got := truncate(strings.Repeat("标", 300), 255); utf8.RuneCountInString(got) != 255 || !strings.HasSuffix(got, "…") {
		t.Fatalf("truncate() = %d runes, want 255", utf8.RuneCountInString(got))
	}
	if got := truncate("abc", 255); got != "abc" {
		t.Fatalf("short input must be kept, got %q", got)
	}
	if got := truncate("abc", 3); got != "abc" {
		t.Fatalf("input of exactly n must be kept, got %q", got)
	}
	for _, c := range []struct {
		n    int
		want string
	}{{2, "a…"}, {1, "…"}, {0, ""}} {
		if got := truncate("abc", c.n); got != c.want {
			t.Errorf("truncate(abc, %d) = %q, want %q", c.n, got, c.want)
		}
	}
}

// correlationKey 含中文标签值时按字节硬切会产生非法 UTF-8（落库 22021）：必须在字符边界截断。
func TestCorrelationKeyStaysValidUTF8AtColumnWidth(t *testing.T) {
	t.Parallel()
	a := &AlertRef{Labels: map[string]string{"env": strings.Repeat("环", 400)}}
	key := correlationKey(&RuleSpec{Correlation: RuleCorrelation{Keys: []string{"label:env"}}}, a, "", nil)
	if len(key) > 255 || !utf8.ValidString(key) {
		t.Fatalf("correlationKey() bytes=%d valid=%v, want <=255 bytes of valid UTF-8", len(key), utf8.ValidString(key))
	}
}

func TestCompiledRuleMatches(t *testing.T) {
	t.Parallel()
	rule := &Rule{Spec: RuleSpec{Match: RuleMatch{MinSeverity: 3, GroupIDs: []string{"g1"}, ModelCodes: []string{"host"}}}}
	c := &compiledRule{rule: rule}
	mc := matchContext{ciModel: map[string]string{"ci1": "host", "ci2": "db"}}
	cases := []struct {
		name string
		a    *AlertRef
		want bool
	}{
		{"match", &AlertRef{Severity: 3, GroupID: "g1", CIID: "ci1"}, true},
		{"severity too low", &AlertRef{Severity: 2, GroupID: "g1", CIID: "ci1"}, false},
		{"other group", &AlertRef{Severity: 4, GroupID: "g2", CIID: "ci1"}, false},
		{"other model", &AlertRef{Severity: 4, GroupID: "g1", CIID: "ci2"}, false},
		{"no ci when model required", &AlertRef{Severity: 4, GroupID: "g1"}, false},
	}
	for _, tc := range cases {
		if got := c.matches(tc.a, mc); got != tc.want {
			t.Errorf("%s: matches() = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestEventTypeAndLabels(t *testing.T) {
	t.Parallel()
	if got := EventType(SeverityS1, NotifyOpened); got != "incident.s1.opened" {
		t.Fatalf("EventType() = %q", got)
	}
	for s := int32(1); s <= 4; s++ {
		if NotifySeverity(s) != 5-s {
			t.Errorf("NotifySeverity(%d) = %d", s, NotifySeverity(s))
		}
		if !ValidSeverity(s) {
			t.Errorf("ValidSeverity(%d) = false", s)
		}
	}
	if ValidSeverity(0) || ValidSeverity(5) {
		t.Error("ValidSeverity accepts out-of-range values")
	}
	for _, s := range []int32{StatusOpen, StatusResponding, StatusMitigated} {
		if !IsActive(s) {
			t.Errorf("IsActive(%d) = false", s)
		}
	}
	for _, s := range []int32{StatusResolved, StatusClosed, StatusMerged} {
		if IsActive(s) {
			t.Errorf("IsActive(%d) = true", s)
		}
	}
}
