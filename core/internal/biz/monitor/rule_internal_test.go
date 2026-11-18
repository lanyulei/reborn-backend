package biz

import (
	"fmt"
	"strings"
	"testing"
	"time"

	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	"github.com/go-kratos/kratos/v3/errors"
)

func TestParsePromDuration(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		want time.Duration
		ok   bool
	}{
		{"5m", 5 * time.Minute, true},
		{"1h30m", 90 * time.Minute, true},
		{"1d", 24 * time.Hour, true},
		{"1w2d", 9 * 24 * time.Hour, true},
		{"30s", 30 * time.Second, true},
		{"1500ms", 1500 * time.Millisecond, true},
		{"1y", 365 * 24 * time.Hour, true},
		{"0", 0, true},
		{"0s", 0, true},
		{"", 0, false},
		{"5", 0, false},
		{"m", 0, false},
		{"5m1h", 0, false}, // 单位须按从大到小的顺序
		{"1.5h", 0, false},
		{"-5m", 0, false},
		{"5 m", 0, false},
		{"999999999y", 0, false}, // 溢出
		{"5x", 0, false},
	}
	for _, c := range cases {
		got, ok := parsePromDuration(c.in)
		if ok != c.ok || got != c.want {
			t.Errorf("parsePromDuration(%q) = %v, %v; want %v, %v", c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestFormatPromDuration_RoundTrip(t *testing.T) {
	t.Parallel()
	for secs, want := range map[int32]string{0: "0s", 45: "45s", 60: "1m", 90: "90s", 3600: "1h", 5400: "90m", 86400: "1d"} {
		got := formatPromDuration(secs)
		if got != want {
			t.Errorf("formatPromDuration(%d) = %q, want %q", secs, got, want)
		}
		if d, ok := parsePromDuration(got); !ok || int32(d.Seconds()) != secs {
			t.Errorf("round trip of %d via %q = %v, %v", secs, got, d, ok)
		}
	}
}

func TestCheckExpr(t *testing.T) {
	t.Parallel()
	for _, expr := range []string{
		"up == 0",
		`sum by (instance) (rate(node_cpu_seconds_total{mode!="idle"}[5m])) > 0.9`,
		`histogram_quantile(0.99, sum(rate(http_request_duration_seconds_bucket[5m])) by (le)) > 1`,
		`absent(up{job="api"})`,
	} {
		if err := checkExpr(expr); err != nil {
			t.Errorf("checkExpr(%q) = %v, want nil", expr, err)
		}
	}
	for _, expr := range []string{"", "   ", "up ==", "sum(", `up{job="a"`, "up > > 1", "rate(up[5m]"} {
		if err := checkExpr(expr); err == nil {
			t.Errorf("checkExpr(%q) = nil, want error", expr)
		}
	}
}

func TestCheckTemplate(t *testing.T) {
	t.Parallel()
	for _, text := range []string{
		"实例 {{ $labels.instance }} 的 CPU 使用率为 {{ $value }}",
		`{{ printf "%.2f" $value }}`,
		"{{ humanizePercentage $value }} / {{ humanize $value }} / {{ humanizeDuration $value }}",
		`{{ if gt $value 90.0 }}严重{{ else }}{{ toUpper $labels.env }}{{ end }}`,
		"纯文本",
	} {
		if err := checkTemplate(text); err != nil {
			t.Errorf("checkTemplate(%q) = %v, want nil", text, err)
		}
	}
	for _, text := range []string{
		"{{ $labels.instance",            // 语法错误
		"{{ nope $value }}",              // 未知函数
		`{{ index $labels "instance" }}`, // 白名单之外的内建函数
		"{{ len $labels }}",
		`{{ template "x" }}`,
		`{{ call $value }}`,
		"{{ $labels.a | html }}",
	} {
		if err := checkTemplate(text); err == nil {
			t.Errorf("checkTemplate(%q) = nil, want error", text)
		}
	}
}

func validSpec() RuleSpec {
	return RuleSpec{
		GroupID: mc.RootGroupID, Name: "cpu", DatasourceScope: mc.DatasourceScopeAll, EvalIntervalS: 60, ForS: 60,
		Queries: []mc.Query{{Expr: "cpu > 85", Severity: mc.SeverityWarning}, {Expr: "cpu > 95", Severity: mc.SeverityCritical}},
	}
}

func TestValidateStatic(t *testing.T) {
	t.Parallel()
	ok := validSpec()
	if err := validateStatic(&ok); err != nil {
		t.Fatalf("valid spec rejected: %v", err)
	}
	if ok.Labels == nil || ok.Annotations == nil || ok.DatasourceIDs == nil || ok.Notify.UserIDs == nil || ok.Effective.Windows == nil {
		t.Fatalf("normalize must replace nil containers: %+v", ok)
	}
	many := func(n int, f func(i int) (string, string)) map[string]string {
		out := map[string]string{}
		for i := range n {
			k, v := f(i)
			out[k] = v
		}
		return out
	}
	cases := []struct {
		name   string
		mutate func(s *RuleSpec)
	}{
		{"blank name", func(s *RuleSpec) { s.Name = "  " }},
		{"name too long", func(s *RuleSpec) { s.Name = strings.Repeat("规", 129) }},
		{"no queries", func(s *RuleSpec) { s.Queries = nil }},
		{"five queries", func(s *RuleSpec) { s.Queries = append(s.Queries, s.Queries[0], s.Queries[0], s.Queries[0]) }},
		{"duplicate severity", func(s *RuleSpec) { s.Queries[1].Severity = s.Queries[0].Severity }},
		{"severity out of range", func(s *RuleSpec) { s.Queries[0].Severity = 5 }},
		{"bad promql", func(s *RuleSpec) { s.Queries[0].Expr = "cpu >" }},
		{"empty expr", func(s *RuleSpec) { s.Queries[0].Expr = " " }},
		{"too many labels", func(s *RuleSpec) {
			s.Labels = many(21, func(i int) (string, string) { return fmt.Sprintf("l%d", i), "v" })
		}},
		{"reserved label", func(s *RuleSpec) { s.Labels = map[string]string{"__name__": "x"} }},
		{"empty label value", func(s *RuleSpec) { s.Labels = map[string]string{"team": ""} }},
		{"too many annotations", func(s *RuleSpec) {
			s.Annotations = many(11, func(i int) (string, string) { return fmt.Sprintf("a%d", i), "v" })
		}},
		{"bad annotation template", func(s *RuleSpec) { s.Annotations = map[string]string{"summary": "{{ $labels.x"} }},
		{"annotation uses forbidden func", func(s *RuleSpec) { s.Annotations = map[string]string{"summary": "{{ len $labels }}"} }},
		{"bad annotation name", func(s *RuleSpec) { s.Annotations = map[string]string{"sum-mary": "x"} }},
		{"bad recovery expr", func(s *RuleSpec) { s.RecoveryExpr = "cpu{" }},
		{"interval too small", func(s *RuleSpec) { s.EvalIntervalS = 10 }},
		{"interval too large", func(s *RuleSpec) { s.EvalIntervalS = 3601 }},
		{"for too large", func(s *RuleSpec) { s.ForS = 86401 }},
		{"negative recover", func(s *RuleSpec) { s.RecoverS = -1 }},
		{"bad timezone", func(s *RuleSpec) { s.Effective.Timezone = "Mars/Olympus" }},
		{"bad window", func(s *RuleSpec) {
			s.Effective.Windows = []mc.TimeWindow{{Days: []int32{8}, Start: "09:00", End: "18:00"}}
		}},
		{"repeat too small", func(s *RuleSpec) { s.RepeatIntervalM = 3 }},
		{"repeat too large", func(s *RuleSpec) { s.RepeatIntervalM = 10081 }},
		{"max notify too large", func(s *RuleSpec) { s.MaxNotify = 1001 }},
		{"bad group key label", func(s *RuleSpec) { s.GroupKeyLabels = []string{"a-b"} }},
		{"specified without datasources", func(s *RuleSpec) { s.DatasourceScope = mc.DatasourceScopeSpecified }},
		{"all with datasources", func(s *RuleSpec) { s.DatasourceIDs = []string{"x"} }},
		{"duplicate datasources", func(s *RuleSpec) {
			s.DatasourceScope, s.DatasourceIDs = mc.DatasourceScopeSpecified, []string{"x", "x"}
		}},
		{"unknown datasource scope", func(s *RuleSpec) { s.DatasourceScope = 3 }},
		{"duplicate notify users", func(s *RuleSpec) { s.Notify.UserIDs = []string{"u", "u"} }},
	}
	for _, c := range cases {
		spec := validSpec()
		c.mutate(&spec)
		if err := validateStatic(&spec); !errors.Is(err, ErrRuleInvalid) {
			t.Errorf("%s: err = %v, want ErrRuleInvalid", c.name, err)
		}
	}
	// 带模板动作的恢复取值不做 PromQL 校验（渲染后才完整），但模板本身须合法。
	spec := validSpec()
	spec.RecoveryExpr = `cpu{instance="{{ $labels.instance }}"}`
	if err := validateStatic(&spec); err != nil {
		t.Fatalf("templated recovery expr rejected: %v", err)
	}
	spec.RepeatIntervalM, spec.MaxNotify = 5, 1000
	if err := validateStatic(&spec); err != nil {
		t.Fatalf("boundary repeat / max notify rejected: %v", err)
	}
}

func TestEvalRelevantChanged(t *testing.T) {
	t.Parallel()
	base := validSpec()
	base.Labels = map[string]string{"team": "infra"}
	same := func(mutate func(s *RuleSpec)) bool {
		next := base
		next.Queries = append([]mc.Query(nil), base.Queries...)
		next.Labels = map[string]string{"team": "infra"}
		mutate(&next)
		return !evalRelevantChanged(&base, &next)
	}
	// 不影响判定的字段：spec_time 不动。
	for name, mutate := range map[string]func(s *RuleSpec){
		"name":        func(s *RuleSpec) { s.Name = "renamed" },
		"description": func(s *RuleSpec) { s.Description = "x" },
		"enabled":     func(s *RuleSpec) { s.Enabled = !s.Enabled },
		"notify":      func(s *RuleSpec) { s.Notify.UserIDs = []string{"u"} },
		"repeat":      func(s *RuleSpec) { s.RepeatIntervalM = 30 },
		"annotations": func(s *RuleSpec) { s.Annotations = map[string]string{"summary": "x"} },
		"group":       func(s *RuleSpec) { s.GroupID = "other" },
		"effective":   func(s *RuleSpec) { s.Effective.Timezone = "Asia/Shanghai" },
	} {
		if !same(mutate) {
			t.Errorf("%s must not bump spec_time", name)
		}
	}
	// 影响判定的字段：spec_time 推进。
	for name, mutate := range map[string]func(s *RuleSpec){
		"expr":             func(s *RuleSpec) { s.Queries[0].Expr = "cpu > 80" },
		"severity":         func(s *RuleSpec) { s.Queries[0].Severity = mc.SeverityInfo },
		"query count":      func(s *RuleSpec) { s.Queries = s.Queries[:1] },
		"level inhibit":    func(s *RuleSpec) { s.LevelInhibit = !s.LevelInhibit },
		"interval":         func(s *RuleSpec) { s.EvalIntervalS = 30 },
		"for":              func(s *RuleSpec) { s.ForS = 0 },
		"recover":          func(s *RuleSpec) { s.RecoverS = 60 },
		"nodata keep":      func(s *RuleSpec) { s.NodataKeepS = 60 },
		"datasource scope": func(s *RuleSpec) { s.DatasourceScope, s.DatasourceIDs = mc.DatasourceScopeSpecified, []string{"d"} },
		"label value":      func(s *RuleSpec) { s.Labels["team"] = "db" },
		"label added":      func(s *RuleSpec) { s.Labels["env"] = "prod" },
	} {
		if same(mutate) {
			t.Errorf("%s must bump spec_time", name)
		}
	}
}

func TestBatchInput_CheckParams(t *testing.T) {
	t.Parallel()
	group, sev, n := "g", int32(3), int32(60)
	yes := true
	cases := []struct {
		name string
		in   BatchInput
		ok   bool
	}{
		{"enable", BatchInput{Action: BatchEnable}, true},
		{"delete", BatchInput{Action: BatchDelete}, true},
		{"enable with extra", BatchInput{Action: BatchEnable, GroupID: &group}, false},
		{"move", BatchInput{Action: BatchMove, GroupID: &group}, true},
		{"move without group", BatchInput{Action: BatchMove}, false},
		{"move with severity", BatchInput{Action: BatchMove, GroupID: &group, Severity: &sev}, false},
		{"set severity", BatchInput{Action: BatchSetSeverity, Severity: &sev}, true},
		{"set severity missing", BatchInput{Action: BatchSetSeverity}, false},
		{"set timing one field", BatchInput{Action: BatchSetTiming, ForS: &n}, true},
		{"set timing none", BatchInput{Action: BatchSetTiming}, false},
		{"add labels", BatchInput{Action: BatchAddLabels, Labels: map[string]string{"a": "b"}}, true},
		{"add labels empty", BatchInput{Action: BatchAddLabels}, false},
		{"remove labels", BatchInput{Action: BatchRemoveLabels, LabelNames: []string{"a"}}, true},
		{"remove labels with labels", BatchInput{Action: BatchRemoveLabels, Labels: map[string]string{"a": "b"}}, false},
		{"set notify", BatchInput{Action: BatchSetNotify, NotifyRecovery: &yes}, true},
		{"set notify none", BatchInput{Action: BatchSetNotify}, false},
		{"set notify with timing", BatchInput{Action: BatchSetNotify, NotifyRecovery: &yes, ForS: &n}, false},
		{"unknown", BatchInput{Action: "explode"}, false},
	}
	for _, c := range cases {
		err := c.in.checkParams()
		if (err == nil) != c.ok {
			t.Errorf("%s: err = %v, want ok=%v", c.name, err, c.ok)
		}
		if err != nil && !errors.Is(err, ErrRuleBatchInvalid) {
			t.Errorf("%s: err = %v, want ErrRuleBatchInvalid", c.name, err)
		}
	}
}

func TestBoundTimeRange(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	at := func(d time.Duration) *time.Time { t := now.Add(d); return &t }

	f := AlertFilter{}
	if err := boundTimeRange(&f, now); err != nil || f.HistorySince == nil || !f.HistorySince.Equal(now.Add(-alertHistoryWindow)) || f.FireStart != nil {
		t.Fatalf("default window: %+v, %v", f, err)
	}
	f = AlertFilter{FireStart: at(-24 * time.Hour)}
	if err := boundTimeRange(&f, now); err != nil || f.HistorySince != nil || !f.FireEnd.Equal(now) {
		t.Fatalf("start only: %+v, %v", f, err)
	}
	f = AlertFilter{FireEnd: at(-24 * time.Hour)}
	if err := boundTimeRange(&f, now); err != nil || !f.FireStart.Equal(now.Add(-24*time.Hour-alertHistoryWindow)) {
		t.Fatalf("end only: %+v, %v", f, err)
	}
	f = AlertFilter{FireStart: at(-91 * 24 * time.Hour), FireEnd: at(0)}
	if err := boundTimeRange(&f, now); !errors.Is(err, ErrTimeRangeInvalid) {
		t.Fatalf("over 90 days: %v", err)
	}
	f = AlertFilter{FireStart: at(0), FireEnd: at(-time.Hour)}
	if err := boundTimeRange(&f, now); !errors.Is(err, ErrTimeRangeInvalid) {
		t.Fatalf("inverted: %v", err)
	}
}

func TestMuteState(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 17, 10, 30, 0, 0, time.UTC) // 周二
	at := func(d time.Duration) *time.Time { t := now.Add(d); return &t }
	window := func(start, end string) *mc.Effective {
		return &mc.Effective{Windows: []mc.TimeWindow{{Days: []int32{2}, Start: start, End: end}}}
	}
	cases := []struct {
		name string
		spec MuteSpec
		want string
	}{
		{"disabled", MuteSpec{TimeType: mc.MuteTimeOnce, StartTime: at(-time.Hour), EndTime: at(time.Hour)}, MuteStateDisabled},
		{"active once", MuteSpec{Enabled: true, TimeType: mc.MuteTimeOnce, StartTime: at(-time.Hour), EndTime: at(time.Hour)}, MuteStateActive},
		{"pending once", MuteSpec{Enabled: true, TimeType: mc.MuteTimeOnce, StartTime: at(time.Hour), EndTime: at(2 * time.Hour)}, MuteStatePending},
		{"expired once", MuteSpec{Enabled: true, TimeType: mc.MuteTimeOnce, StartTime: at(-2 * time.Hour), EndTime: at(-time.Hour)}, MuteStateExpired},
		{"end is exclusive", MuteSpec{Enabled: true, TimeType: mc.MuteTimeOnce, StartTime: at(-time.Hour), EndTime: at(0)}, MuteStateExpired},
		{"periodic in window", MuteSpec{Enabled: true, TimeType: mc.MuteTimePeriodic, Periodic: window("10:00", "11:00")}, MuteStateActive},
		{"periodic out of window", MuteSpec{Enabled: true, TimeType: mc.MuteTimePeriodic, Periodic: window("11:00", "12:00")}, MuteStatePending},
		{"periodic expired", MuteSpec{Enabled: true, TimeType: mc.MuteTimePeriodic, Periodic: window("10:00", "11:00"), EndTime: at(-time.Minute)}, MuteStateExpired},
		{"periodic not started", MuteSpec{Enabled: true, TimeType: mc.MuteTimePeriodic, Periodic: window("10:00", "11:00"), StartTime: at(time.Minute)}, MuteStatePending},
	}
	for _, c := range cases {
		if got := muteState(&c.spec, now); got != c.want {
			t.Errorf("%s: state = %q, want %q", c.name, got, c.want)
		}
	}
}

func TestHumanDuration(t *testing.T) {
	t.Parallel()
	for d, want := range map[time.Duration]string{
		0:                                 "0 秒",
		-time.Second:                      "0 秒",
		42 * time.Second:                  "42 秒",
		5 * time.Minute:                   "5 分钟",
		5*time.Minute + 3*time.Second:     "5 分钟 3 秒",
		2*time.Hour + 30*time.Minute:      "2 小时 30 分钟",
		2*time.Hour + 5*time.Second:       "2 小时", // 只取相邻的两个单位
		51*time.Hour + 20*time.Minute:     "2 天 3 小时",
		24*time.Hour + 59*time.Second:     "1 天",
		3*24*time.Hour + 4*time.Hour + 10: "3 天 4 小时",
	} {
		if got := humanDuration(d); got != want {
			t.Errorf("humanDuration(%v) = %q, want %q", d, got, want)
		}
	}
}

func TestNumberedName(t *testing.T) {
	t.Parallel()
	if got := numberedName("cpu", 1); got != "cpu" {
		t.Fatalf("n=1: %q", got)
	}
	if got := numberedName("cpu", 3); got != "cpu (3)" {
		t.Fatalf("n=3: %q", got)
	}
	long := strings.Repeat("规", 128)
	if got := numberedName(long, 12); len([]rune(got)) != 128 || !strings.HasSuffix(got, " (12)") {
		t.Fatalf("long name must stay within 128 runes: %d %q", len([]rune(got)), got[len(got)-8:])
	}
}
