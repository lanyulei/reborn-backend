package biz

import (
	"strings"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/kit/monitorpipeline"
)

func TestTemplateRendering(t *testing.T) {
	labels := map[string]string{"instance": "db-1", "env": "prod"}
	tests := []struct {
		name string
		text string
		want string // "" 且 wantErr 为真 = 解析或执行失败
		err  bool
	}{
		{name: "prometheus style labels and value", text: "{{ $labels.instance }} = {{ $value }}", want: "db-1 = 0.5"},
		{name: "dot access works too", text: "{{ .Labels.env }}/{{ .Value }}", want: "prod/0.5"},
		{name: "missing label renders empty", text: "[{{ $labels.nope }}]", want: "[]"},
		{name: "printf", text: `{{ printf "%.2f" $value }}`, want: "0.50"},
		{name: "printf with width and literal percent", text: `{{ printf "%6.1f%% of %-4s|" $value "cpu" }}`, want: "   0.5% of cpu |"},
		{name: "pipeline", text: `{{ $value | humanizePercentage }}`, want: "50%"},
		{name: "humanize", text: `{{ humanize 1234567.0 }}`, want: "1.235M"},
		{name: "humanizeDuration", text: `{{ humanizeDuration 3725.0 }}`, want: "1h 2m 5s"},
		{name: "case helpers", text: `{{ toUpper $labels.env }} {{ toLower "ABC" }} {{ title "disk full" }}`, want: "PROD abc Disk Full"},
		{name: "if with comparison", text: `{{ if gt $value 0.4 }}high{{ else }}low{{ end }}`, want: "high"},
		{name: "if with eq on label", text: `{{ if eq $labels.env "prod" }}P{{ end }}`, want: "P"},
		{name: "range over labels", text: `{{ range $k, $v := $labels }}{{ $k }}={{ $v }};{{ end }}`, want: "env=prod;instance=db-1;"},
		{name: "plain text", text: "no actions", want: "no actions"},
		{name: "empty", text: "", want: ""},

		{name: "syntax error", text: "{{ $labels.instance ", err: true},
		{name: "unknown function", text: `{{ nope $value }}`, err: true},
		{name: "builtin outside whitelist: index", text: `{{ index $labels "env" }}`, err: true},
		{name: "builtin outside whitelist: len", text: `{{ len $labels }}`, err: true},
		{name: "builtin outside whitelist: call", text: `{{ call .Value }}`, err: true},
		{name: "builtin outside whitelist: html", text: `{{ html $labels.env }}`, err: true},
		{name: "builtin outside whitelist: print in pipeline", text: `{{ $value | print }}`, err: true},
		{name: "builtin hidden in if condition", text: `{{ if len $labels }}x{{ end }}`, err: true},
		{name: "builtin hidden in parenthesised argument", text: `{{ printf "%d" (len $labels) }}`, err: true},
		{name: "range over an integer is rejected", text: `{{ range 100000000 }}x{{ end }}`, err: true},
		{name: "range over a field other than labels", text: `{{ range .Value }}x{{ end }}`, err: true},
		{name: "range over .Labels (dot can be rebound)", text: `{{ range .Labels }}x{{ end }}`, err: true},
		{name: "E1: assign $labels then range over it", text: `{{$labels = 9000000000000000000}}{{range $labels}}{{end}}`, err: true},
		{name: "E1: redeclare $labels then range over it", text: `{{$labels := 9000000000000000000}}{{range $labels}}{{end}}`, err: true},
		{name: "E1: rebind dot then range over it", text: `{{with 9000000000000000000}}{{range .}}{{end}}{{end}}`, err: true},
		{name: "assign $value", text: `{{$value = 1}}{{$value}}`, err: true},
		{name: "printf with an explicit argument index", text: `{{ printf "%[1]d" 1 }}`, err: true},
		{name: "printf with an indexed star width", text: `{{ printf "%[2]*[1]d" 1 999999 }}`, err: true},
		{name: "printf with a dynamic format", text: `{{ printf $labels.env 1 }}`, err: true},
		{name: "define with a forbidden body", text: `{{ define "x" }}{{ range 100000000 }}{{ end }}{{ end }}ok`, err: true},
		{name: "define with a harmless body is inert", text: `{{ define "x" }}{{ .Value }}{{ end }}ok`, want: "ok"},
		{name: "nested range is rejected", text: `{{ range $labels }}{{ range $labels }}{{ end }}{{ end }}`, err: true},
		{name: "printf with a huge width", text: `{{ printf "%999999d" 1 }}`, err: true},
		{name: "printf with a huge precision", text: `{{ printf "%.999999f" $value }}`, err: true},
		{name: "printf with a star width", text: `{{ printf "%*d" 999999 1 }}`, err: true},
		{name: "template call is rejected", text: `{{ define "a" }}{{ template "a" }}{{ end }}{{ template "a" }}`, err: true},
		{name: "unknown field fails at execution", text: `{{ .Nope }}`, err: true},
		{name: "humanize on a non-number fails at execution", text: `{{ humanize $labels.env }}`, err: true},
		{name: "source too long", text: strings.Repeat("a", monitorcontract.MaxTemplateBytes+1), err: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tpl, err := monitorpipeline.CompileTemplate(tt.text)
			var got string
			if err == nil {
				got, err = tpl.Render(labels, 0.5)
			}
			if tt.err {
				if err == nil {
					t.Fatalf("want an error, got %q", got)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("got %q, %v; want %q", got, err, tt.want)
			}
		})
	}
}

func TestTemplateOutputCap(t *testing.T) {
	// 每个标签值 256 字符，20 个标签 → 超过 4KB。
	labels := map[string]string{}
	for i := range 20 {
		labels[string(rune('a'+i))] = strings.Repeat("x", 256)
	}
	tpl, err := monitorpipeline.CompileTemplate(`{{ range $k, $v := $labels }}{{ $v }}{{ end }}`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tpl.Render(labels, 0); err == nil || !strings.Contains(err.Error(), monitorpipeline.ErrRenderTooLarge.Error()) {
		t.Fatalf("err = %v, want output-too-large", err)
	}
	// 恰好在上限内可以通过。
	ok, err := monitorpipeline.CompileTemplate(strings.Repeat("a", monitorpipeline.MaxRenderBytes))
	if err != nil {
		t.Fatal(err)
	}
	if out, err := ok.Render(nil, 0); err != nil || len(out) != monitorpipeline.MaxRenderBytes {
		t.Fatalf("len = %d, err = %v", len(out), err)
	}
	// printf 不得把超长的标签值成倍放大：字符串实参总长受输出上限约束。
	big, err := monitorpipeline.CompileTemplate(`{{ printf "%s%s" $labels.a $labels.b }}`)
	if err != nil {
		t.Fatal(err)
	}
	huge := map[string]string{"a": strings.Repeat("x", monitorpipeline.MaxRenderBytes), "b": "y"}
	if _, err := big.Render(huge, 0); err == nil || !strings.Contains(err.Error(), monitorpipeline.ErrRenderTooLarge.Error()) {
		t.Fatalf("err = %v, want output-too-large", err)
	}
}

func TestRenderAnnotations(t *testing.T) {
	source := map[string]string{
		"summary": "{{ $labels.instance }} at {{ $value }}",
		"broken":  "{{ nope }}",
		"runtime": "{{ .Missing }}",
		"static":  "see runbook",
	}
	samples := []*Sample{
		{Fingerprint: "fp1", Labels: map[string]string{"instance": "db-1"}, Value: 3},
		{Fingerprint: "fp2", Labels: map[string]string{"instance": "db-2"}, Value: 4},
	}
	got := renderAnnotations(source, samples)
	if len(got) != 2 {
		t.Fatalf("got %v", got)
	}
	want := map[string]string{"summary": "db-1 at 3", "broken": templateError, "runtime": templateError, "static": "see runbook"}
	if len(got["fp1"]) != len(want) {
		t.Fatalf("got %v", got["fp1"])
	}
	for k, v := range want {
		if got["fp1"][k] != v {
			t.Errorf("%s = %q, want %q", k, got["fp1"][k], v)
		}
	}
	if got["fp2"]["summary"] != "db-2 at 4" {
		t.Fatalf("second sample = %v", got["fp2"])
	}
}

// E1 的两个攻击模板：校验拒绝，渲染立即得到 <template error>，不会进入任何循环。
func TestMaliciousAnnotationsAreNeverExecuted(t *testing.T) {
	source := map[string]string{
		"assign":    `{{$labels = 9000000000000000000}}{{range $labels}}{{end}}`,
		"redeclare": `{{$labels := 9000000000000000000}}{{range $labels}}{{end}}`,
		"rebind":    `{{with 9000000000000000000}}{{range .}}{{end}}{{end}}`,
		"indexed":   `{{printf "%[2]*[1]d" 1 999999}}`,
		"define":    `{{define "x"}}{{range 9000000000000000000}}{{end}}{{end}}`,
		"fine":      "{{ $labels.instance }}",
	}
	done := make(chan map[string]map[string]string, 1)
	go func() {
		done <- renderAnnotations(source, []*Sample{{Fingerprint: "fp", Labels: map[string]string{"instance": "a"}, Value: 1}})
	}()
	select {
	case got := <-done:
		for name, text := range got["fp"] {
			if want := map[bool]string{true: "a", false: templateError}[name == "fine"]; text != want {
				t.Errorf("%s rendered to %q, want %q", name, text, want)
			}
		}
		if len(got["fp"]) != len(source) {
			t.Fatalf("got %v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("rendering did not return: a malicious template is being executed")
	}
	for name, text := range source {
		if name == "fine" {
			continue
		}
		if _, err := monitorpipeline.CompileTemplate(text); err == nil {
			t.Errorf("%s: CompileTemplate accepted the template", name)
		}
	}
}

func TestRenderRecoveryExpr(t *testing.T) {
	tpl, err := monitorpipeline.CompileTemplate(`cpu_usage{instance="{{ $labels.instance }}"}`)
	if err != nil {
		t.Fatal(err)
	}
	if expr, ok := renderRecoveryExpr(tpl, map[string]string{"instance": "db-1"}, 0); !ok || expr != `cpu_usage{instance="db-1"}` {
		t.Fatalf("expr = %q, ok = %v", expr, ok)
	}
	blank, _ := monitorpipeline.CompileTemplate(`{{ $labels.nope }}  `)
	if _, ok := renderRecoveryExpr(blank, nil, 0); ok {
		t.Fatal("blank expression must be rejected")
	}
	failing, _ := monitorpipeline.CompileTemplate(`{{ .Nope }}`)
	if _, ok := renderRecoveryExpr(failing, nil, 0); ok {
		t.Fatal("failing template must be rejected")
	}
}

func TestMuteScoping(t *testing.T) {
	const (
		parent = "018f3e1a-0000-7000-8000-0000000000p1"
		other  = "018f3e1a-0000-7000-8000-0000000000p2"
	)
	rule := baseRule(func(r *Rule) { r.GroupPath = "/" + parent + "/" + testGroup + "/" })
	labels := map[string]string{"instance": "10.0.0.5", "env": "prod"}
	periodic := monitorcontract.Effective{Timezone: "UTC", Windows: []monitorcontract.TimeWindow{{Days: []int32{2}, Start: "07:00", End: "09:00"}}}
	// t0 = 2026-11-17 08:00 UTC，周二。
	tests := []struct {
		name string
		mute Mute
		now  time.Time
		sev  int32
		want bool
	}{
		{name: "global one-off inside the range", mute: Mute{TimeType: 1, Start: at(-60), End: at(60)}, want: true},
		{name: "start is inclusive", mute: Mute{TimeType: 1, Start: at(0), End: at(60)}, want: true},
		{name: "end is exclusive", mute: Mute{TimeType: 1, Start: at(-60), End: at(0)}},
		{name: "not started yet", mute: Mute{TimeType: 1, Start: at(10), End: at(60)}},
		{name: "one-off without a range never matches", mute: Mute{TimeType: 1}},
		{name: "unknown time type never matches", mute: Mute{TimeType: 9, Start: at(-60), End: at(60)}},

		{name: "periodic inside the window", mute: Mute{TimeType: 2, Periodic: periodic}, want: true},
		{name: "periodic outside the window", mute: Mute{TimeType: 2, Periodic: periodic}, now: at(2 * 3600)},
		{name: "periodic on another weekday", mute: Mute{TimeType: 2, Periodic: periodic}, now: at(24 * 3600)},
		{name: "periodic bounded by start_time", mute: Mute{TimeType: 2, Periodic: periodic, Start: at(600)}},
		{name: "periodic bounded by end_time", mute: Mute{TimeType: 2, Periodic: periodic, End: at(-600)}},
		{name: "periodic without windows never matches", mute: Mute{TimeType: 2}},

		{name: "group: the rule's own group", mute: Mute{TimeType: 1, Start: at(-60), End: at(60), GroupID: testGroup, GroupPath: "/" + parent + "/" + testGroup + "/"}, want: true},
		{name: "group: an ancestor covers descendants", mute: Mute{TimeType: 1, Start: at(-60), End: at(60), GroupID: parent, GroupPath: "/" + parent + "/"}, want: true},
		{name: "group: a sibling does not match", mute: Mute{TimeType: 1, Start: at(-60), End: at(60), GroupID: other, GroupPath: "/" + other + "/"}},
		{name: "group: deleted group matches nothing", mute: Mute{TimeType: 1, Start: at(-60), End: at(60), GroupID: parent}},

		{name: "rule_ids hit", mute: Mute{TimeType: 1, Start: at(-60), End: at(60), RuleIDs: []string{"x", testRuleID}}, want: true},
		{name: "rule_ids miss", mute: Mute{TimeType: 1, Start: at(-60), End: at(60), RuleIDs: []string{"x"}}},
		{name: "datasource_ids hit", mute: Mute{TimeType: 1, Start: at(-60), End: at(60), DatasourceIDs: []string{testDSID}}, want: true},
		{name: "datasource_ids miss", mute: Mute{TimeType: 1, Start: at(-60), End: at(60), DatasourceIDs: []string{"x"}}},
		{name: "severities hit", mute: Mute{TimeType: 1, Start: at(-60), End: at(60), Severities: []int32{2, 4}}, sev: 4, want: true},
		{name: "severities miss", mute: Mute{TimeType: 1, Start: at(-60), End: at(60), Severities: []int32{2, 4}}, sev: 3},

		{
			name: "matchers all hit",
			mute: Mute{TimeType: 1, Start: at(-60), End: at(60), Matchers: []monitorcontract.Matcher{
				{Label: "instance", Op: monitorcontract.OpRegex, Value: `10\..*`}, {Label: "env", Op: monitorcontract.OpIn, Values: []string{"prod", "pre"}},
			}},
			want: true,
		},
		{
			name: "one matcher misses",
			mute: Mute{TimeType: 1, Start: at(-60), End: at(60), Matchers: []monitorcontract.Matcher{
				{Label: "instance", Op: monitorcontract.OpRegex, Value: `10\..*`}, {Label: "env", Op: monitorcontract.OpNotEqual, Value: "prod"},
			}},
		},
		{
			name: "invalid matcher never mutes",
			mute: Mute{TimeType: 1, Start: at(-60), End: at(60), Matchers: []monitorcontract.Matcher{{Label: "env", Op: monitorcontract.OpRegex, Value: "("}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := tt.mute
			m.ID, m.Mode = "m1", monitorcontract.MuteModeNotify
			now, sev := tt.now, tt.sev
			if now.IsZero() {
				now = t0
			}
			if sev == 0 {
				sev = monitorcontract.SeverityWarning
			}
			id, mode := newMuteSet(compileMutes([]*Mute{&m}), rule, testDSID, now).Match(labels, sev)
			if got := mode != 0; got != tt.want {
				t.Fatalf("matched = %v (id %q), want %v", got, id, tt.want)
			}
		})
	}
	var none *MuteSet
	if id, mode := none.Match(labels, 2); id != "" || mode != 0 {
		t.Fatalf("nil MuteSet must match nothing")
	}
}

func TestTruncate(t *testing.T) {
	if got := truncate("héllo", 2); got != "h" {
		t.Fatalf("truncate must not split a rune, got %q", got)
	}
	if got := truncate("a\x00b\xffc", 10); got != "ab?c" {
		t.Fatalf("truncate must sanitise, got %q", got)
	}
	if got := truncate("short", 10); got != "short" {
		t.Fatalf("got %q", got)
	}
}
