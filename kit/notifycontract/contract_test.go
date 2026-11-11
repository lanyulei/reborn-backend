package notifycontract

import "testing"

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
		{"*", "a.b", true}, {"monitor.alert.*", "monitor.alert.firing", true}, {"monitor.alert.*", "monitor.alert", false},
		{"monitor.alert.firing", "monitor.alert.firing", true}, {"monitor.alert.firing", "monitor.alert.resolved", false},
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
