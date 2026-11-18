package monitorcontract

import (
	"encoding/json"
	"errors"
	"math"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestValidSeverity(t *testing.T) {
	t.Parallel()
	for n, want := range map[int32]bool{0: false, 1: true, 4: true, 5: false, -1: false} {
		if got := ValidSeverity(n); got != want {
			t.Errorf("ValidSeverity(%d) = %v, want %v", n, got, want)
		}
	}
}

func TestLabelValidation(t *testing.T) {
	t.Parallel()
	names := map[string]bool{
		"instance": true, "_x": true, "__name__": true, "a1_B": true,
		"": false, "1a": false, "a-b": false, "a.b": false, "标签": false, strings.Repeat("a", MaxLabelNameLen+1): false,
	}
	for name, want := range names {
		if got := ValidLabelName(name); got != want {
			t.Errorf("ValidLabelName(%q) = %v, want %v", name, got, want)
		}
	}
	if !ValidLabelValue(strings.Repeat("中", MaxLabelValueLen)) || ValidLabelValue(strings.Repeat("中", MaxLabelValueLen+1)) ||
		ValidLabelValue("\xff") {
		t.Error("ValidLabelValue length / utf8 boundary")
	}
	many := map[string]string{}
	for i := range MaxRuleLabels + 1 {
		many["l"+strings.Repeat("x", i)] = "v"
	}
	cases := []struct {
		name   string
		labels map[string]string
		ok     bool
	}{
		{"nil", nil, true},
		{"ok", map[string]string{"team": "运维", "env": "prod"}, true},
		{"bad name", map[string]string{"a-b": "v"}, false},
		{"reserved prefix", map[string]string{"__name__": "v"}, false},
		{"empty value", map[string]string{"a": ""}, false},
		{"invalid utf8", map[string]string{"a": "\xff"}, false},
		{"too many", many, false},
	}
	for _, c := range cases {
		err := ValidateLabels(c.labels)
		if (err == nil) != c.ok {
			t.Errorf("%s: err = %v, want ok=%v", c.name, err, c.ok)
		}
		if err != nil && !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: error must wrap ErrInvalid: %v", c.name, err)
		}
	}
}

func TestMatcherValidate(t *testing.T) {
	t.Parallel()
	manyValues := make([]string, MaxMatcherValues+1)
	for i := range manyValues {
		manyValues[i] = "v"
	}
	cases := []struct {
		name string
		m    Matcher
		ok   bool
	}{
		{"eq", Matcher{Label: "env", Op: OpEqual, Value: "prod"}, true},
		{"eq empty value", Matcher{Label: "env", Op: OpEqual}, true},
		{"ne", Matcher{Label: "env", Op: OpNotEqual, Value: "prod"}, true},
		{"re", Matcher{Label: "instance", Op: OpRegex, Value: `10\..*`}, true},
		{"nre", Matcher{Label: "instance", Op: OpNotRegex, Value: `a|b`}, true},
		{"in", Matcher{Label: "env", Op: OpIn, Values: []string{"a", "b"}}, true},
		{"not in", Matcher{Label: "env", Op: OpNotIn, Values: []string{"a"}}, true},
		{"bad label", Matcher{Label: "a-b", Op: OpEqual, Value: "x"}, false},
		{"empty label", Matcher{Op: OpEqual, Value: "x"}, false},
		{"bad op", Matcher{Label: "a", Op: "=", Value: "x"}, false},
		{"empty op", Matcher{Label: "a", Value: "x"}, false},
		{"bad regex", Matcher{Label: "a", Op: OpRegex, Value: "("}, false},
		{"regex that only compiles once wrapped", Matcher{Label: "a", Op: OpRegex, Value: "a)|(?:b"}, false},
		{"negated regex that only compiles once wrapped", Matcher{Label: "a", Op: OpNotRegex, Value: "a)|(?:b"}, false},
		{"regex too long", Matcher{Label: "a", Op: OpRegex, Value: strings.Repeat("a", MaxMatcherValueLen+1)}, false},
		{"value too long", Matcher{Label: "a", Op: OpEqual, Value: strings.Repeat("a", MaxMatcherValueLen+1)}, false},
		{"value invalid utf8", Matcher{Label: "a", Op: OpEqual, Value: "\xff"}, false},
		{"eq with values", Matcher{Label: "a", Op: OpEqual, Value: "x", Values: []string{"y"}}, false},
		{"in with value", Matcher{Label: "a", Op: OpIn, Value: "x", Values: []string{"y"}}, false},
		{"in without values", Matcher{Label: "a", Op: OpIn}, false},
		{"in too many values", Matcher{Label: "a", Op: OpNotIn, Values: manyValues}, false},
		{"in value too long", Matcher{Label: "a", Op: OpIn, Values: []string{strings.Repeat("a", MaxMatcherValueLen+1)}}, false},
	}
	for _, c := range cases {
		err := c.m.Validate()
		if (err == nil) != c.ok {
			t.Errorf("%s: err = %v, want ok=%v", c.name, err, c.ok)
		}
		if err != nil && !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: error must wrap ErrInvalid: %v", c.name, err)
		}
	}
	tooMany := make([]Matcher, MaxMatchers+1)
	for i := range tooMany {
		tooMany[i] = Matcher{Label: "a", Op: OpEqual, Value: "x"}
	}
	if err := ValidateMatchers(tooMany); !errors.Is(err, ErrInvalid) {
		t.Errorf("too many matchers: %v", err)
	}
	if err := ValidateMatchers(tooMany[:MaxMatchers]); err != nil {
		t.Errorf("max matchers: %v", err)
	}
	if err := ValidateMatchers([]Matcher{{Label: "a", Op: OpEqual, Value: "x"}, {Label: "b", Op: "~"}}); !errors.Is(err, ErrInvalid) ||
		!strings.Contains(err.Error(), "matcher 1") {
		t.Errorf("error must name the failing row: %v", err)
	}
}

func TestMatchAll(t *testing.T) {
	t.Parallel()
	labels := map[string]string{"instance": "10.0.0.1:9100", "env": "prod", "job": "node", "msg": "line1\nline2"}
	cases := []struct {
		name     string
		matchers []Matcher
		want     bool
	}{
		{"empty matches everything", nil, true},
		{"eq hit", []Matcher{{Label: "env", Op: OpEqual, Value: "prod"}}, true},
		{"eq miss", []Matcher{{Label: "env", Op: OpEqual, Value: "test"}}, false},
		{"eq empty matches absent label", []Matcher{{Label: "dc", Op: OpEqual}}, true},
		{"eq empty misses present label", []Matcher{{Label: "env", Op: OpEqual}}, false},
		{"ne hit", []Matcher{{Label: "env", Op: OpNotEqual, Value: "test"}}, true},
		{"ne miss", []Matcher{{Label: "env", Op: OpNotEqual, Value: "prod"}}, false},
		{"ne absent label", []Matcher{{Label: "dc", Op: OpNotEqual, Value: "bj"}}, true},
		{"re hit", []Matcher{{Label: "instance", Op: OpRegex, Value: `10\..*`}}, true},
		{"re is anchored at start", []Matcher{{Label: "instance", Op: OpRegex, Value: `0\.0\.1:9100`}}, false},
		{"re is anchored at end", []Matcher{{Label: "instance", Op: OpRegex, Value: `10\.0`}}, false},
		{"re alternation is grouped", []Matcher{{Label: "env", Op: OpRegex, Value: `test|prod`}}, true},
		{"re alternation stays anchored", []Matcher{{Label: "env", Op: OpRegex, Value: `pro|rod`}}, false},
		{"re that breaks out of the anchoring group never matches", []Matcher{{Label: "env", Op: OpRegex, Value: `pro)|(?:rod`}}, false},
		{"re dot matches newline", []Matcher{{Label: "msg", Op: OpRegex, Value: `line1.*`}}, true},
		{"re absent label vs .*", []Matcher{{Label: "dc", Op: OpRegex, Value: `.*`}}, true},
		{"re absent label vs .+", []Matcher{{Label: "dc", Op: OpRegex, Value: `.+`}}, false},
		{"nre hit", []Matcher{{Label: "env", Op: OpNotRegex, Value: `test.*`}}, true},
		{"nre miss", []Matcher{{Label: "env", Op: OpNotRegex, Value: `pr.*`}}, false},
		{"nre partial does not exclude", []Matcher{{Label: "env", Op: OpNotRegex, Value: `pro`}}, true},
		{"in hit", []Matcher{{Label: "env", Op: OpIn, Values: []string{"test", "prod"}}}, true},
		{"in miss", []Matcher{{Label: "env", Op: OpIn, Values: []string{"test", "dev"}}}, false},
		{"in absent label", []Matcher{{Label: "dc", Op: OpIn, Values: []string{"bj"}}}, false},
		{"not in hit", []Matcher{{Label: "env", Op: OpNotIn, Values: []string{"test", "dev"}}}, true},
		{"not in miss", []Matcher{{Label: "env", Op: OpNotIn, Values: []string{"prod"}}}, false},
		{"not in absent label", []Matcher{{Label: "dc", Op: OpNotIn, Values: []string{"bj"}}}, true},
		{"rows are ANDed: all hit", []Matcher{{Label: "env", Op: OpEqual, Value: "prod"}, {Label: "job", Op: OpIn, Values: []string{"node"}}}, true},
		{"rows are ANDed: one miss", []Matcher{{Label: "env", Op: OpEqual, Value: "prod"}, {Label: "job", Op: OpEqual, Value: "mysql"}}, false},
		{"invalid matcher never matches", []Matcher{{Label: "env", Op: OpRegex, Value: "("}}, false},
		{"unknown op never matches", []Matcher{{Label: "env", Op: "=", Value: "prod"}}, false},
	}
	for _, c := range cases {
		if got := MatchAll(c.matchers, labels); got != c.want {
			t.Errorf("%s: MatchAll = %v, want %v", c.name, got, c.want)
		}
	}
	if !MatchAll([]Matcher{{Label: "a", Op: OpNotEqual, Value: "x"}}, nil) {
		t.Error("nil labels behave as an empty label set")
	}
}

func TestCompiledMatchers(t *testing.T) {
	t.Parallel()
	var nilSet *Matchers
	if !nilSet.Match(map[string]string{"a": "b"}) || nilSet.Len() != 0 {
		t.Fatal("nil *Matchers matches everything")
	}
	if zero := (&Matchers{}); !zero.Match(nil) {
		t.Fatal("zero Matchers matches everything")
	}
	src := []Matcher{{Label: "env", Op: OpIn, Values: []string{"a", "b"}}, {Label: "host", Op: OpRegex, Value: "web-[0-9]+"}}
	ms, err := CompileMatchers(src)
	if err != nil || ms.Len() != 2 {
		t.Fatalf("CompileMatchers: %v, len %d", err, ms.Len())
	}
	// 编译结果不得与调用方的切片共享底层数组。
	src[0].Values[0] = "z"
	if !ms.Match(map[string]string{"env": "a", "host": "web-12"}) || ms.Match(map[string]string{"env": "z", "host": "web-12"}) {
		t.Fatal("compiled matchers must not alias caller slices")
	}
	if ms.Match(map[string]string{"env": "a", "host": "web-12x"}) {
		t.Fatal("regex must be anchored")
	}
	// 并发只读安全（-race 下验证）。
	done := make(chan struct{})
	for range 4 {
		go func() {
			for range 200 {
				ms.Match(map[string]string{"env": "b", "host": "web-1"})
			}
			done <- struct{}{}
		}()
	}
	for range 4 {
		<-done
	}
}

func TestFingerprint(t *testing.T) {
	t.Parallel()
	const rule, ds = "018f3e1a-0000-7000-8000-000000000001", "018f3e1a-0000-7000-8000-000000000002"
	base := Fingerprint(rule, ds, map[string]string{"instance": "a", "job": "node"}, 0)
	if !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(base) {
		t.Fatalf("fingerprint format: %q", base)
	}
	// 锁定算法：落库值跨版本、跨进程必须稳定。
	if want := "ae837db6a2b9b950d66b6c0473c57168"; base != want {
		t.Errorf("fingerprint golden = %q, want %q", base, want)
	}
	if got, want := Fingerprint(rule, ds, map[string]string{"instance": "a", "job": "node"}, 3), "93f931209657b4f58b690490f3771a0c"; got != want {
		t.Errorf("fingerprint with severity golden = %q, want %q", got, want)
	}
	if got := Fingerprint(rule, ds, map[string]string{"job": "node", "instance": "a"}, 0); got != base {
		t.Error("label order must not matter")
	}
	distinct := map[string]string{
		"other rule":           Fingerprint(ds, ds, map[string]string{"instance": "a", "job": "node"}, 0),
		"other datasource":     Fingerprint(rule, rule, map[string]string{"instance": "a", "job": "node"}, 0),
		"other value":          Fingerprint(rule, ds, map[string]string{"instance": "b", "job": "node"}, 0),
		"extra label":          Fingerprint(rule, ds, map[string]string{"instance": "a", "job": "node", "x": "y"}, 0),
		"with severity 3":      Fingerprint(rule, ds, map[string]string{"instance": "a", "job": "node"}, 3),
		"with severity 4":      Fingerprint(rule, ds, map[string]string{"instance": "a", "job": "node"}, 4),
		"no labels":            Fingerprint(rule, ds, nil, 0),
		"shifted name/value 1": Fingerprint(rule, ds, map[string]string{"ab": "c"}, 0),
		"shifted name/value 2": Fingerprint(rule, ds, map[string]string{"a": "bc"}, 0),
		"severity as label":    Fingerprint(rule, ds, map[string]string{"instance": "a", "job": "node", "severity": "3"}, 0),
	}
	seen := map[string]string{base: "base"}
	for name, fp := range distinct {
		if prev, dup := seen[fp]; dup {
			t.Errorf("%s collides with %s", name, prev)
		}
		seen[fp] = name
	}
	if Fingerprint(rule, ds, nil, 0) != Fingerprint(rule, ds, map[string]string{}, 0) {
		t.Error("nil and empty labels are the same series")
	}
}

func TestGroupKey(t *testing.T) {
	t.Parallel()
	const rule = "018f3e1a-0000-7000-8000-000000000001"
	labels := map[string]string{"cluster": "c1", "instance": "a", "env": "prod"}
	if got := GroupKey(rule, labels, nil); got != "monitor:rule:"+rule {
		t.Fatalf("rule-level key = %q", got)
	}
	k1 := GroupKey(rule, labels, []string{"cluster", "env"})
	if !strings.HasPrefix(k1, "monitor:rule:"+rule+":") || len(k1) > 128 {
		t.Fatalf("labelled key = %q", k1)
	}
	if GroupKey(rule, labels, []string{"env", "cluster", "env"}) != k1 {
		t.Error("label name order and duplicates must not matter")
	}
	if GroupKey(rule, map[string]string{"cluster": "c1", "instance": "other", "env": "prod"}, []string{"cluster", "env"}) != k1 {
		t.Error("labels outside the group key must not matter")
	}
	if GroupKey(rule, map[string]string{"cluster": "c2", "env": "prod"}, []string{"cluster", "env"}) == k1 {
		t.Error("different group label values must differ")
	}
	if GroupKey(rule, map[string]string{"env": "prod"}, []string{"cluster", "env"}) == k1 {
		t.Error("missing label counts as empty value")
	}
	if GroupKey(rule, labels, []string{"cluster"}) == k1 {
		t.Error("different label sets must differ")
	}
	if long := GroupKey(strings.Repeat("r", 36), labels, []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}); len(long) > 128 {
		t.Errorf("group key too long: %d", len(long))
	}
}

func TestEffectiveValidate(t *testing.T) {
	t.Parallel()
	manyWindows := make([]TimeWindow, MaxWindows+1)
	for i := range manyWindows {
		manyWindows[i] = TimeWindow{Days: []int32{1}, Start: "00:00", End: "01:00"}
	}
	cases := []struct {
		name string
		e    Effective
		ok   bool
	}{
		{"zero value", Effective{}, true},
		{"timezone only", Effective{Timezone: "Asia/Shanghai"}, true},
		{"workdays", Effective{Timezone: "Asia/Shanghai", Windows: []TimeWindow{{Days: []int32{1, 2, 3, 4, 5}, Start: "09:00", End: "18:00"}}}, true},
		{"cross midnight", Effective{Windows: []TimeWindow{{Days: []int32{7}, Start: "22:00", End: "06:00"}}}, true},
		{"whole day", Effective{Windows: []TimeWindow{{Days: []int32{6, 7}, Start: "00:00", End: "00:00"}}}, true},
		{"unknown timezone", Effective{Timezone: "Mars/Olympus"}, false},
		{"local timezone", Effective{Timezone: "Local"}, false},
		{"no days", Effective{Windows: []TimeWindow{{Start: "09:00", End: "18:00"}}}, false},
		{"day 0", Effective{Windows: []TimeWindow{{Days: []int32{0}, Start: "09:00", End: "18:00"}}}, false},
		{"day 8", Effective{Windows: []TimeWindow{{Days: []int32{8}, Start: "09:00", End: "18:00"}}}, false},
		{"duplicate day", Effective{Windows: []TimeWindow{{Days: []int32{1, 1}, Start: "09:00", End: "18:00"}}}, false},
		{"bad start", Effective{Windows: []TimeWindow{{Days: []int32{1}, Start: "9:00", End: "18:00"}}}, false},
		{"bad end hour", Effective{Windows: []TimeWindow{{Days: []int32{1}, Start: "09:00", End: "24:00"}}}, false},
		{"bad minute", Effective{Windows: []TimeWindow{{Days: []int32{1}, Start: "09:60", End: "18:00"}}}, false},
		{"not a clock", Effective{Windows: []TimeWindow{{Days: []int32{1}, Start: "ab:cd", End: "18:00"}}}, false},
		{"too many windows", Effective{Windows: manyWindows}, false},
	}
	for _, c := range cases {
		err := c.e.Validate()
		if (err == nil) != c.ok {
			t.Errorf("%s: err = %v, want ok=%v", c.name, err, c.ok)
		}
		if err != nil && !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: error must wrap ErrInvalid: %v", c.name, err)
		}
	}
}

func TestEffectiveContains(t *testing.T) {
	t.Parallel()
	sh, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	// 2026-11-16 是周一。
	at := func(day, hour, minute int) time.Time { return time.Date(2026, 11, day, hour, minute, 0, 0, sh) }
	workdays := Effective{Timezone: "Asia/Shanghai", Windows: []TimeWindow{{Days: []int32{1, 2, 3, 4, 5}, Start: "09:00", End: "18:00"}}}
	night := Effective{Timezone: "Asia/Shanghai", Windows: []TimeWindow{{Days: []int32{5}, Start: "22:00", End: "06:00"}}}
	sundayNight := Effective{Timezone: "Asia/Shanghai", Windows: []TimeWindow{{Days: []int32{7}, Start: "23:00", End: "01:00"}}}
	weekend := Effective{Timezone: "Asia/Shanghai", Windows: []TimeWindow{{Days: []int32{6, 7}, Start: "00:00", End: "00:00"}}}
	multi := Effective{Timezone: "Asia/Shanghai", Windows: []TimeWindow{
		{Days: []int32{1}, Start: "01:00", End: "02:00"}, {Days: []int32{1}, Start: "03:00", End: "04:00"},
	}}
	cases := []struct {
		name string
		e    Effective
		now  time.Time
		want bool
	}{
		{"zero value is always", Effective{}, at(16, 3, 0), true},
		{"no windows is always", Effective{Timezone: "Asia/Shanghai"}, at(21, 3, 0), true},
		{"workday inside", workdays, at(16, 10, 0), true},
		{"start is inclusive", workdays, at(16, 9, 0), true},
		{"before start", workdays, at(16, 8, 59), false},
		{"end is exclusive", workdays, at(16, 18, 0), false},
		{"last minute", workdays, at(16, 17, 59), true},
		{"friday inside", workdays, at(20, 12, 0), true},
		{"saturday outside", workdays, at(21, 12, 0), false},
		{"sunday outside", workdays, at(22, 12, 0), false},
		{"cross midnight: start day evening", night, at(20, 22, 0), true},
		{"cross midnight: before start", night, at(20, 21, 59), false},
		{"cross midnight: next morning", night, at(21, 5, 59), true},
		{"cross midnight: end is exclusive", night, at(21, 6, 0), false},
		{"cross midnight: next evening is another day", night, at(21, 23, 0), false},
		{"cross midnight: morning of start day belongs to the day before", night, at(20, 3, 0), false},
		{"cross midnight wraps the week: sunday night", sundayNight, at(22, 23, 30), true},
		{"cross midnight wraps the week: monday 00:30", sundayNight, at(23, 0, 30), true},
		{"cross midnight wraps the week: monday 01:00", sundayNight, at(23, 1, 0), false},
		{"cross midnight wraps the week: tuesday 00:30", sundayNight, at(24, 0, 30), false},
		{"whole day: saturday", weekend, at(21, 0, 0), true},
		{"whole day: sunday night", weekend, at(22, 23, 59), true},
		{"whole day: monday", weekend, at(23, 0, 0), false},
		{"any window may hit", multi, at(16, 3, 30), true},
		{"between windows", multi, at(16, 2, 30), false},
		{"timezone conversion: UTC instant evaluated in Shanghai", workdays, time.Date(2026, 11, 16, 1, 30, 0, 0, time.UTC), true},
		{"timezone conversion: UTC working hours are Shanghai night", workdays, time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC), false},
		{"timezone conversion crosses the date line", workdays, time.Date(2026, 11, 15, 17, 0, 0, 0, time.UTC), false},
		{"empty timezone is UTC", Effective{Windows: []TimeWindow{{Days: []int32{1}, Start: "09:00", End: "18:00"}}}, time.Date(2026, 11, 16, 9, 0, 0, 0, time.UTC), true},
		{"empty timezone is UTC, not caller location", Effective{Windows: []TimeWindow{{Days: []int32{1}, Start: "09:00", End: "18:00"}}}, at(16, 9, 0), false},
		{"unknown timezone falls back to UTC", Effective{Timezone: "Mars/Olympus", Windows: []TimeWindow{{Days: []int32{1}, Start: "09:00", End: "18:00"}}}, time.Date(2026, 11, 16, 9, 0, 0, 0, time.UTC), true},
		{"unparsable window never hits", Effective{Windows: []TimeWindow{{Days: []int32{1}, Start: "9", End: "18:00"}}}, time.Date(2026, 11, 16, 9, 0, 0, 0, time.UTC), false},
	}
	for _, c := range cases {
		if got := c.e.Contains(c.now); got != c.want {
			t.Errorf("%s: Contains(%s) = %v, want %v", c.name, c.now.Format(time.RFC3339), got, c.want)
		}
	}
}

// TestJSONShape 锁定落库格式：json 标签是两个进程之间的契约。
func TestJSONShape(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		v    any
		want string
	}{
		{"query", Query{Expr: "up == 0", Severity: SeverityCritical}, `{"expr":"up == 0","severity":3}`},
		{
			"effective",
			Effective{Timezone: "Asia/Shanghai", Windows: []TimeWindow{{Days: []int32{1, 2}, Start: "09:00", End: "18:00"}}},
			`{"timezone":"Asia/Shanghai","windows":[{"days":[1,2],"start":"09:00","end":"18:00"}]}`,
		},
		{
			"notify targets",
			NotifyTargets{UserIDs: []string{"u"}, ScheduleIDs: []string{"s"}, DeptIDs: []string{"d"}},
			`{"user_ids":["u"],"schedule_ids":["s"],"dept_ids":["d"]}`,
		},
		{"matcher value", Matcher{Label: "instance", Op: OpRegex, Value: `10\..*`}, `{"label":"instance","op":"=~","value":"10\\..*"}`},
		{"matcher values", Matcher{Label: "env", Op: OpIn, Values: []string{"a", "b"}}, `{"label":"env","op":"in","values":["a","b"]}`},
		{
			"receivers",
			Receivers{Self: true, UserIDs: []string{"u"}, ScheduleIDs: []string{"s"}, Groups: []string{"$group:ops"}},
			`{"self":true,"user_ids":["u"],"schedule_ids":["s"],"groups":["$group:ops"]}`,
		},
		{
			"eval detail",
			EvalDetail{
				Queries: []EvalQueryDetail{
					{Index: 0, Severity: 3, Series: 2},
					{Index: 1, Severity: 2, Error: "boom", Truncated: true},
					// R6：部分结果的键名是 partial（引擎写入、core 用同一个结构读出）。
					{Index: 2, Severity: 1, Series: 5, Partial: true},
				},
				Points:    []EvalPoint{{Fingerprint: "f", Labels: map[string]string{"a": "b"}, Severity: 3, Value: FormatValue(1.5), Step: StepFired, AlertID: "x"}},
				Truncated: true,
			},
			`{"queries":[{"index":0,"severity":3,"series":2},{"index":1,"severity":2,"series":0,"truncated":true,"error":"boom"},` +
				`{"index":2,"severity":1,"series":5,"partial":true}],` +
				`"points":[{"fingerprint":"f","labels":{"a":"b"},"severity":3,"value":"1.5","step":"fired","alert_id":"x"}],"truncated":true}`,
		},
		{
			"notify payload",
			NotifyPayload{
				AlertID: "a", AlertURL: "u", RuleID: "r", RuleName: "n", GroupName: "g", DatasourceName: "d", Severity: 4,
				Status: PayloadStatusFiring, Labels: map[string]string{"k": "v"}, Annotations: map[string]string{"s": "t"}, Value: "1",
				Expr: "up", FirstFireTime: "2026-11-17 10:00:00", Duration: "5m", NotifySeq: 1,
			},
			`{"alert_id":"a","alert_url":"u","rule_id":"r","rule_name":"n","group_name":"g","datasource_name":"d","severity":4,` +
				`"status":"firing","labels":{"k":"v"},"annotations":{"s":"t"},"value":"1","expr":"up",` +
				`"first_fire_time":"2026-11-17 10:00:00","duration":"5m","notify_seq":1}`,
		},
	}
	for _, c := range cases {
		raw, err := json.Marshal(c.v)
		if err != nil || string(raw) != c.want {
			t.Errorf("%s:\n got %s\nwant %s (err %v)", c.name, raw, c.want, err)
		}
	}
}

func TestFormatValue(t *testing.T) {
	t.Parallel()
	cases := map[float64]string{1: "1", 1.5: "1.5", 0.1: "0.1", 1e21: "1e+21", math.Inf(1): "+Inf", math.Inf(-1): "-Inf"}
	for v, want := range cases {
		if got := FormatValue(v); got != want {
			t.Errorf("FormatValue(%v) = %q, want %q", v, got, want)
		}
	}
	if got := FormatValue(math.NaN()); got != "NaN" {
		t.Errorf("FormatValue(NaN) = %q", got)
	}
}

// TestLiterals 锁定与迁移 0069 / 设计文档一致的字面量：改动即破坏已落库数据或另一端进程。
func TestLiterals(t *testing.T) {
	t.Parallel()
	if NotifyBizID != "default" || EventAlertFiring != "monitor.alert.firing" || EventAlertResolved != "monitor.alert.resolved" ||
		EventSubscriptionFiring != "monitor.subscription.firing" || EventSubscriptionResolved != "monitor.subscription.resolved" {
		t.Error("notify event literals")
	}
	if LockPlanner != "monitor:planner" || LockMaintenance != "monitor:maintenance" {
		t.Error("lock keys")
	}
	if CfgDatasource != "monitor_datasource" || CfgMute != "monitor_mute" || CfgRuleGroup != "monitor_rule_group" {
		t.Error("cfg version names")
	}
	if RootGroupID != "018f3e1a-0cc0-7000-8000-000000000001" {
		t.Error("root group id")
	}
	ints := [][2]int32{
		{SeverityInfo, 1},
		{SeverityWarning, 2},
		{SeverityCritical, 3},
		{SeverityFatal, 4},
		{AlertFiring, 1},
		{AlertResolved, 2},
		{AlertClosed, 3},
		{StatePending, 1},
		{StateFiring, 2},
		{StateRecovering, 3},
		{EvalNever, 0},
		{EvalOK, 1},
		{EvalFiring, 2},
		{EvalNoData, 3},
		{EvalError, 4},
		{EvalInactive, 5},
		{MuteModeNotify, 1},
		{MuteModeEvent, 2},
		{MuteTimeOnce, 1},
		{MuteTimePeriodic, 2},
		{NotifyKindFiring, 1},
		{NotifyKindRepeat, 2},
		{NotifyKindResolved, 3},
		{OutboxPending, 1},
		{OutboxDone, 2},
		{OutboxDropped, 3},
		{NotifySourceRule, 1},
		{NotifySourceSubscription, 2},
		{DatasourceScopeSpecified, 1},
		{DatasourceScopeAll, 2},
		{AuthNone, 1},
		{AuthBasic, 2},
		{AuthBearer, 3},
		{HealthUnknown, 0},
		{HealthOK, 1},
		{HealthFailed, 2},
		{ScopeNone, 0},
		{ScopeRead, 1},
		{ScopeManage, 2},
	}
	for i, p := range ints {
		if p[0] != p[1] {
			t.Errorf("literal #%d = %d, want %d", i, p[0], p[1])
		}
	}
}

func BenchmarkMatchersMatch(b *testing.B) {
	ms, err := CompileMatchers([]Matcher{
		{Label: "instance", Op: OpRegex, Value: `10\.0\..*`},
		{Label: "env", Op: OpIn, Values: []string{"prod", "pre"}},
		{Label: "job", Op: OpNotEqual, Value: "blackbox"},
	})
	if err != nil {
		b.Fatal(err)
	}
	labels := map[string]string{"instance": "10.0.3.7:9100", "env": "prod", "job": "node"}
	b.ReportAllocs()
	for b.Loop() {
		if !ms.Match(labels) {
			b.Fatal("must match")
		}
	}
}

func BenchmarkFingerprint(b *testing.B) {
	labels := map[string]string{"instance": "10.0.3.7:9100", "env": "prod", "job": "node", "device": "/dev/sda1", "team": "ops"}
	b.ReportAllocs()
	for b.Loop() {
		Fingerprint("018f3e1a-0000-7000-8000-000000000001", "018f3e1a-0000-7000-8000-000000000002", labels, 0)
	}
}
