package monitorpipeline

import (
	"errors"
	"maps"
	"strings"
	"testing"

	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

func TestTemplate(t *testing.T) {
	t.Parallel()
	labels := map[string]string{"instance": "10.0.0.1:9100", "env": "prod"}
	for _, tc := range []struct {
		src   string
		value float64
		want  string
	}{
		{"{{ $labels.instance }} is {{ $value }}", 95.5, "10.0.0.1:9100 is 95.5"},
		{"{{ $labels.missing }}|", 0, "|"},
		{`{{ printf "%.1f%%" $value }}`, 12.345, "12.3%"},
		{"{{ humanize $value }}", 1234567, "1.235M"},
		{"{{ humanize $value }}", 0.00012, "120u"},
		{"{{ humanize $value }}", 0, "0"},
		{"{{ humanizePercentage $value }}", 0.1234, "12.34%"},
		{"{{ humanizeDuration $value }}", 3725, "1h 2m 5s"},
		{"{{ humanizeDuration $value }}", 90061, "1d 1h 1m 1s"},
		{"{{ humanizeDuration $value }}", 65, "1m 5s"},
		{"{{ humanizeDuration $value }}", 1.5, "1.5s"},
		{"{{ humanizeDuration $value }}", -65, "-1m 5s"},
		{"{{ humanizeDuration $value }}", 0.0015, "1.5ms"},
		{"{{ humanizeDuration $value }}", 0, "0s"},
		{`{{ humanize "1500" }}`, 0, "1.5k"},
		{"{{ toUpper $labels.env }}/{{ toLower \"ABC\" }}/{{ title \"host down\" }}", 0, "PROD/abc/Host Down"},
		{"{{ range $k, $v := $labels }}{{ $k }}={{ $v }};{{ end }}", 0, "env=prod;instance=10.0.0.1:9100;"},
		{`{{ if and (eq $labels.env "prod") (gt $value 90.0) }}hot{{ else }}ok{{ end }}`, 91, "hot"},
	} {
		tpl, err := CompileTemplate(tc.src)
		if err != nil {
			t.Errorf("CompileTemplate(%q): %v", tc.src, err)
			continue
		}
		got, err := tpl.Render(labels, tc.value)
		if err != nil || got != tc.want {
			t.Errorf("Render(%q) = %q, %v; want %q", tc.src, got, err, tc.want)
		}
	}
	// 未通过契约校验的模板不编译。
	for _, src := range []string{
		"{{ .Labels | len }}", "{{ index $labels \"a\" }}", "{{ $labels = 1 }}", "{{ range .Labels }}{{ end }}",
		`{{ printf "%*d" 1000000 1 }}`, "{{ template \"x\" }}", "{{ unknown }}", "{{ if }}", strings.Repeat("a", mc.MaxTemplateBytes+1),
	} {
		if _, err := CompileTemplate(src); err == nil || !errors.Is(err, mc.ErrInvalid) {
			t.Errorf("CompileTemplate(%.40q) = %v, want ErrInvalid", src, err)
		}
	}
	// 输出上限：超长标签值不会被放大后输出。
	big := map[string]string{"a": strings.Repeat("x", MaxRenderBytes)}
	tpl, err := CompileTemplate("{{ $labels.a }}{{ $labels.a }}")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tpl.Render(big, 0); !errors.Is(err, ErrRenderTooLarge) {
		t.Errorf("oversized output: %v", err)
	}
	tpl, err = CompileTemplate(`{{ printf "%s%s" $labels.a $labels.a }}`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tpl.Render(big, 0); !errors.Is(err, ErrRenderTooLarge) {
		t.Errorf("oversized printf: %v", err)
	}
	tpl, err = CompileTemplate(`{{ printf "%v" $labels }}`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tpl.Render(big, 0); !errors.Is(err, ErrRenderTooLarge) {
		t.Errorf("oversized printf of the label map: %v", err)
	}
	// 函数收到不能换算成数字的实参：渲染报错，不 panic。
	tpl, err = CompileTemplate("{{ humanize $labels.env }}")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tpl.Render(labels, 0); err == nil {
		t.Error("humanize of a non-number must fail")
	}
}

func TestDropExpr(t *testing.T) {
	t.Parallel()
	for _, src := range []string{"", "  \n\t"} {
		d, err := CompileDropExpr(src)
		if d != nil || err != nil {
			t.Errorf("CompileDropExpr(%q) = %v, %v; want nil, nil", src, d, err)
		}
	}
	var none *DropExpr
	if drop, err := none.Drop(map[string]string{"env": "dev"}, 1); drop || err != nil {
		t.Error("nil DropExpr must keep everything")
	}
	for _, tc := range []struct {
		src    string
		labels map[string]string
		value  float64
		want   bool
	}{
		{`{{ if eq $labels.env "dev" }}true{{ end }}`, map[string]string{"env": "dev"}, 0, true},
		{`{{ if eq $labels.env "dev" }}true{{ end }}`, map[string]string{"env": "prod"}, 0, false},
		{`{{ if eq $labels.env "dev" }}true{{ end }}`, nil, 0, false},
		{"\n  {{ if lt $value 1.0 }} true {{ end }}\n", nil, 0.5, true},
		{"{{ if lt $value 1.0 }}true{{ end }}", nil, 2, false},
		{"true", nil, 0, true},
		{"TRUE", nil, 0, false}, // 只认小写的 true
		{"true false", nil, 0, false},
		{"1", nil, 0, false},
		{`{{ eq $labels.ci_env "test" }}`, map[string]string{"ci_env": "test"}, 0, true}, // 布尔值打印为 true
		{`{{ eq $labels.ci_env "test" }}`, map[string]string{"ci_env": "prod"}, 0, false},
		{`{{ or (eq $labels.a "x") (eq $labels.b "y") }}`, map[string]string{"b": "y"}, 0, true},
	} {
		d, err := CompileDropExpr(tc.src)
		if err != nil {
			t.Errorf("CompileDropExpr(%q): %v", tc.src, err)
			continue
		}
		got, err := d.Drop(tc.labels, tc.value)
		if err != nil || got != tc.want {
			t.Errorf("Drop(%q, %v, %v) = %v, %v; want %v", tc.src, tc.labels, tc.value, got, err, tc.want)
		}
	}
	for _, src := range []string{"{{ if }}", "{{ call $labels }}", strings.Repeat("x", mc.MaxDropExprBytes+1)} {
		if _, err := CompileDropExpr(src); err == nil || !errors.Is(err, mc.ErrInvalid) {
			t.Errorf("CompileDropExpr(%.30q) = %v, want ErrInvalid", src, err)
		}
	}
	// 渲染失败：不丢弃，并把原因交给调用方。
	d, err := CompileDropExpr("{{ humanize $labels.env }}")
	if err != nil {
		t.Fatal(err)
	}
	if drop, err := d.Drop(map[string]string{"env": "x"}, 0); drop || err == nil {
		t.Errorf("failed render = %v, %v; want keep with an error", drop, err)
	}
}

func relabel(t *testing.T, labels map[string]string, steps ...mc.RelabelStep) map[string]string {
	t.Helper()
	r, err := CompileRelabel(steps)
	if err != nil {
		t.Fatalf("CompileRelabel(%+v): %v", steps, err)
	}
	out := maps.Clone(labels)
	if out == nil {
		out = map[string]string{}
	}
	r.Apply(out)
	return out
}

func TestRelabel(t *testing.T) {
	t.Parallel()
	base := map[string]string{"instance": "10.0.0.1:9100", "job": "node", "env": "prod", "tmp_a": "1", "tmp_b": "2"}
	for _, tc := range []struct {
		name  string
		in    map[string]string
		steps []mc.RelabelStep
		want  map[string]string
	}{
		{
			"replace extracts with a capture group", base,
			[]mc.RelabelStep{{Action: "replace", SourceLabels: []string{"instance"}, Regex: "(.+):\\d+", TargetLabel: "host"}},
			map[string]string{"instance": "10.0.0.1:9100", "job": "node", "env": "prod", "tmp_a": "1", "tmp_b": "2", "host": "10.0.0.1"},
		},
		{
			"replace without match leaves labels alone", base,
			[]mc.RelabelStep{{Action: "replace", SourceLabels: []string{"job"}, Regex: "(.+):\\d+", TargetLabel: "host"}},
			base,
		},
		{
			"regex is anchored",
			map[string]string{"a": "xfoox"},
			[]mc.RelabelStep{{Action: "replace", SourceLabels: []string{"a"}, Regex: "foo", TargetLabel: "b", Replacement: "hit"}},
			map[string]string{"a": "xfoox"},
		},
		{
			"replace joins sources with the default separator", base,
			[]mc.RelabelStep{{Action: "replace", SourceLabels: []string{"job", "env"}, TargetLabel: "key"}},
			map[string]string{"instance": "10.0.0.1:9100", "job": "node", "env": "prod", "tmp_a": "1", "tmp_b": "2", "key": "node;prod"},
		},
		{
			"replace joins with a custom separator and named groups",
			map[string]string{"a": "x", "b": "y"},
			[]mc.RelabelStep{{Action: "replace", SourceLabels: []string{"a", "b"}, Separator: "/", Regex: "(?P<l>.+)/(?P<r>.+)", TargetLabel: "c", Replacement: "${r}-${l}"}},
			map[string]string{"a": "x", "b": "y", "c": "y-x"},
		},
		{
			"replace sets a constant when there are no sources",
			map[string]string{"a": "x"},
			[]mc.RelabelStep{{Action: "replace", TargetLabel: "team", Replacement: "ops"}},
			map[string]string{"a": "x", "team": "ops"},
		},
		{
			"missing source label is the empty string",
			map[string]string{"a": "x"},
			[]mc.RelabelStep{{Action: "replace", SourceLabels: []string{"nope"}, Regex: "", TargetLabel: "b", Replacement: "set"}},
			map[string]string{"a": "x", "b": "set"},
		},
		{
			"empty result deletes the target",
			map[string]string{"a": "x", "b": "old"},
			[]mc.RelabelStep{{Action: "replace", SourceLabels: []string{"nope"}, TargetLabel: "b"}},
			map[string]string{"a": "x"},
		},
		{
			"rename = replace then labeldrop",
			map[string]string{"old": "v", "k": "1"},
			[]mc.RelabelStep{{Action: "replace", SourceLabels: []string{"old"}, TargetLabel: "new"}, {Action: "labeldrop", Regex: "old"}},
			map[string]string{"new": "v", "k": "1"},
		},
		{
			"labeldrop", base,
			[]mc.RelabelStep{{Action: "labeldrop", Regex: "tmp_.*"}},
			map[string]string{"instance": "10.0.0.1:9100", "job": "node", "env": "prod"},
		},
		{
			"labeldrop is anchored", base, []mc.RelabelStep{{Action: "labeldrop", Regex: "tmp"}}, base,
		},
		{
			"labelkeep", base,
			[]mc.RelabelStep{{Action: "labelkeep", Regex: "instance|job"}},
			map[string]string{"instance": "10.0.0.1:9100", "job": "node"},
		},
		{
			"labelmap copies under a new name",
			map[string]string{"__meta_zone": "a", "tmp_a": "1"},
			[]mc.RelabelStep{{Action: "labelmap", Regex: "tmp_(.+)"}},
			map[string]string{"__meta_zone": "a", "tmp_a": "1", "a": "1"},
		},
		{
			"labelmap with replacement",
			map[string]string{"k8s_app": "web"},
			[]mc.RelabelStep{{Action: "labelmap", Regex: "k8s_(.+)", Replacement: "pod_$1"}},
			map[string]string{"k8s_app": "web", "pod_app": "web"},
		},
		{
			"labelmap never produces invalid or reserved names",
			map[string]string{"x_1a": "v", "y_a": "w"},
			[]mc.RelabelStep{{Action: "labelmap", Regex: "x_(.+)"}, {Action: "labelmap", Regex: "y_(.+)", Replacement: "__$1"}},
			map[string]string{"x_1a": "v", "y_a": "w"},
		},
		{
			"labelmap collisions resolve by name order",
			map[string]string{"b_x": "from_b", "a_x": "from_a"},
			[]mc.RelabelStep{{Action: "labelmap", Regex: "[ab]_(.+)"}},
			map[string]string{"b_x": "from_b", "a_x": "from_a", "x": "from_b"},
		},
		{
			"steps run in order",
			map[string]string{"a": "1"},
			[]mc.RelabelStep{
				{Action: "replace", SourceLabels: []string{"a"}, TargetLabel: "b", Replacement: "${1}2"},
				{Action: "replace", SourceLabels: []string{"b"}, TargetLabel: "c", Replacement: "${1}3"},
			},
			map[string]string{"a": "1", "b": "12", "c": "123"},
		},
	} {
		if got := relabel(t, tc.in, tc.steps...); !maps.Equal(got, tc.want) {
			t.Errorf("%s:\n got %v\nwant %v", tc.name, got, tc.want)
		}
	}

	// 写入的值有上限且不切断字符；NUL 被去掉。
	long := relabel(t, map[string]string{"a": strings.Repeat("好", 600)}, mc.RelabelStep{Action: "replace", SourceLabels: []string{"a"}, TargetLabel: "b"})
	if n := len(long["b"]); n > maxRelabelValueBytes || n < maxRelabelValueBytes-3 || !strings.HasPrefix(long["a"], long["b"]) {
		t.Errorf("clipped value is %d bytes", n)
	}
	if got := relabel(t, map[string]string{"a": "x\x00y"}, mc.RelabelStep{Action: "replace", SourceLabels: []string{"a"}, TargetLabel: "b"}); got["b"] != "xy" {
		t.Errorf("NUL not stripped: %q", got["b"])
	}
	// labelmap 不会把标签集无限撑大。
	wide := map[string]string{}
	for i := range maxRelabelLabels - 1 {
		wide["a_"+strings.Repeat("x", i+1)] = "v"
	}
	if got := relabel(t, wide, mc.RelabelStep{Action: "labelmap", Regex: "a_(.+)", Replacement: "b_$1"}); len(got) != maxRelabelLabels {
		t.Errorf("labelmap grew the label set to %d", len(got))
	}

	var none *Relabeler
	labels := maps.Clone(base)
	none.Apply(labels)
	if !maps.Equal(labels, base) {
		t.Error("nil Relabeler must not change labels")
	}
	if r, err := CompileRelabel(nil); r != nil || err != nil {
		t.Errorf("CompileRelabel(nil) = %v, %v", r, err)
	}
}

func TestCompileRelabelErrors(t *testing.T) {
	t.Parallel()
	eleven := make([]mc.RelabelStep, 11)
	for i := range eleven {
		eleven[i] = mc.RelabelStep{Action: "labeldrop", Regex: "a"}
	}
	long := strings.Repeat("a", 257)
	for name, steps := range map[string][]mc.RelabelStep{
		"too many steps":             eleven,
		"unknown action":             {{Action: "keep", Regex: "a"}},
		"empty action":               {{}},
		"replace without target":     {{Action: "replace", SourceLabels: []string{"a"}}},
		"replace bad target":         {{Action: "replace", TargetLabel: "1a"}},
		"replace reserved target":    {{Action: "replace", TargetLabel: "__name__"}},
		"replace bad source":         {{Action: "replace", SourceLabels: []string{"a-b"}, TargetLabel: "c"}},
		"replace too many sources":   {{Action: "replace", SourceLabels: []string{"a", "b", "c", "d", "e", "f"}, TargetLabel: "g"}},
		"bad regex":                  {{Action: "replace", Regex: "(", TargetLabel: "a"}},
		"unbalanced regex":           {{Action: "labeldrop", Regex: "a)|(?:b"}},
		"long regex":                 {{Action: "labeldrop", Regex: long}},
		"long replacement":           {{Action: "replace", TargetLabel: "a", Replacement: long}},
		"long separator":             {{Action: "replace", TargetLabel: "a", Separator: long}},
		"invalid utf8 replacement":   {{Action: "replace", TargetLabel: "a", Replacement: "\xff"}},
		"labelmap without regex":     {{Action: "labelmap"}},
		"labeldrop without regex":    {{Action: "labeldrop"}},
		"labelkeep without regex":    {{Action: "labelkeep"}},
		"labeldrop with target":      {{Action: "labeldrop", Regex: "a", TargetLabel: "b"}},
		"labeldrop with sources":     {{Action: "labeldrop", Regex: "a", SourceLabels: []string{"b"}}},
		"labeldrop with replacement": {{Action: "labeldrop", Regex: "a", Replacement: "b"}},
		"labelkeep with replacement": {{Action: "labelkeep", Regex: "a", Replacement: "b"}},
		"labelmap with separator":    {{Action: "labelmap", Regex: "a", Separator: ";"}},
	} {
		if r, err := CompileRelabel(steps); err == nil || r != nil || !errors.Is(err, mc.ErrInvalid) {
			t.Errorf("%s: CompileRelabel() = %v, %v; want ErrInvalid", name, r, err)
		}
	}
}

func TestCIKey(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]string{
		"":                        "",
		"   ":                     "",
		"10.0.0.1":                "10.0.0.1",
		"10.0.0.1:9100":           "10.0.0.1",
		" 10.0.0.1:9100 ":         "10.0.0.1",
		"Web-01":                  "web-01",
		"web-01.example.com:9100": "web-01.example.com",
		"WEB-01:80":               "web-01",
		"[fe80::1]:9100":          "fe80::1",
		"[FE80::1]":               "fe80::1",
		"fe80::1":                 "fe80::1", // 不带方括号的 IPv6：不是 host:port
		"2001:db8::9100":          "2001:db8::9100",
		"host:":                   "host:",    // 冒号后没有端口
		"host:abc":                "host:abc", // 不是数字端口
		":9100":                   ":9100",    // 没有主机
		"[fe80::1]x":              "[fe80::1]x",
		"[fe80::1":                "[fe80::1",
		"db:3306:extra":           "db:3306:extra",
	} {
		if got := CIKey(in); got != want {
			t.Errorf("CIKey(%q) = %q, want %q", in, got, want)
		}
	}
}

func testIndex() *CIIndex {
	return NewCIIndex([]CI{
		{ID: "h1", ModelCode: "host", Name: "Web-01", Env: "prod", Status: "active", Owner: "u1", Tags: []string{"core", "dmz zone"}, Attrs: map[string][]string{"ip": {"10.0.0.1", "192.168.0.1"}, "sn": {"SN-1"}}, UpstreamIDs: []string{"sw1"}},
		{ID: "h2", ModelCode: "host", Name: "web-02", Env: "test", Status: "maintenance", Attrs: map[string][]string{"ip": {"10.0.0.2"}}},
		{ID: "vm1", ModelCode: "vm", Name: "web-02", Attrs: map[string][]string{"ip": {"10.0.0.3", "10.0.0.3"}}}, // 与 h2 同名，属性值重复
		{ID: "sw1", ModelCode: "switch", Name: "core-sw", Attrs: map[string][]string{"ip": {"10.0.0.254"}}},
		{ID: "dup", ModelCode: "vm", Name: "shared", Attrs: map[string][]string{"ip": {"10.0.0.1"}}}, // 与 h1 同 IP
		{ID: "h1", ModelCode: "host", Name: "ignored-duplicate-id"},
		{ID: "", Name: "no-id"},
	})
}

func TestCILookup(t *testing.T) {
	t.Parallel()
	x := testIndex()
	if x.Len() != 5 {
		t.Fatalf("Len() = %d, want 5", x.Len())
	}
	if x.Get("h1").Name != "Web-01" || x.Get("nope") != nil || x.Get("") != nil {
		t.Error("Get")
	}
	for _, tc := range []struct {
		name   string
		m      mc.CIMatch
		labels map[string]string
		wantID string
		want   CIMatchStatus
	}{
		{"default: ident matches the name, case-insensitive", mc.CIMatch{}, map[string]string{"ident": "WEB-01"}, "h1", CIMatched},
		{"default: ident wins over instance", mc.CIMatch{}, map[string]string{"ident": "core-sw", "instance": "10.0.0.2:9100"}, "sw1", CIMatched},
		{"default: falls back to instance → ip, port stripped", mc.CIMatch{}, map[string]string{"instance": "10.0.0.2:9100"}, "h2", CIMatched},
		{"default: unknown ident falls back to instance", mc.CIMatch{}, map[string]string{"ident": "ghost", "instance": "10.0.0.254"}, "sw1", CIMatched},
		{"default: second ip of a resource", mc.CIMatch{}, map[string]string{"instance": "192.168.0.1:80"}, "h1", CIMatched},
		{"default: no usable label", mc.CIMatch{}, map[string]string{"job": "node"}, "", CINoLabel},
		{"default: label present, nothing found", mc.CIMatch{}, map[string]string{"instance": "10.9.9.9:9100"}, "", CINotFound},
		{"default: empty label value", mc.CIMatch{}, map[string]string{"ident": " "}, "", CINoLabel},
		{"default: ambiguous name stops the search", mc.CIMatch{}, map[string]string{"ident": "web-02", "instance": "10.0.0.254"}, "", CIAmbiguous},
		{"default: ambiguous ip", mc.CIMatch{}, map[string]string{"instance": "10.0.0.1:9100"}, "", CIAmbiguous},
		{"model filter disambiguates the name", mc.CIMatch{ModelCodes: []string{"vm"}}, map[string]string{"ident": "web-02"}, "vm1", CIMatched},
		{"model filter disambiguates the ip", mc.CIMatch{ModelCodes: []string{"host"}}, map[string]string{"instance": "10.0.0.1:9100"}, "h1", CIMatched},
		{"model filter excludes everything", mc.CIMatch{ModelCodes: []string{"db"}}, map[string]string{"ident": "web-01"}, "", CINotFound},
		{"explicit: by name", mc.CIMatch{Label: "host", By: "name"}, map[string]string{"host": "core-sw:22", "ident": "web-01"}, "sw1", CIMatched},
		{"explicit: by attr defaults to ip", mc.CIMatch{Label: "addr", By: "attr"}, map[string]string{"addr": "10.0.0.3"}, "vm1", CIMatched},
		{"explicit: named attr, case-insensitive", mc.CIMatch{Label: "serial", By: "attr", Attr: "sn"}, map[string]string{"serial": "sn-1"}, "h1", CIMatched},
		{"explicit: unknown attr", mc.CIMatch{Label: "serial", By: "attr", Attr: "asset"}, map[string]string{"serial": "sn-1"}, "", CINotFound},
		{"explicit: does not fall back to defaults", mc.CIMatch{Label: "host", By: "name"}, map[string]string{"ident": "web-01"}, "", CINoLabel},
		{"explicit: name is not matched against attrs", mc.CIMatch{Label: "host", By: "name"}, map[string]string{"host": "10.0.0.2"}, "", CINotFound},
		{"disabled", mc.CIMatch{Disabled: true}, map[string]string{"ident": "web-01"}, "", CIDisabled},
	} {
		ci, status := x.Lookup(tc.m, tc.labels)
		gotID := ""
		if ci != nil {
			gotID = ci.ID
		}
		if gotID != tc.wantID || status != tc.want {
			t.Errorf("%s: Lookup() = %q, %s; want %q, %s", tc.name, gotID, status, tc.wantID, tc.want)
		}
	}
	var none *CIIndex
	if ci, status := none.Lookup(mc.CIMatch{}, map[string]string{"ident": "web-01"}); ci != nil || status != CIUnavailable {
		t.Errorf("nil index: %v, %s", ci, status)
	}
	if _, status := none.Lookup(mc.CIMatch{Disabled: true}, nil); status != CIDisabled {
		t.Errorf("nil index, disabled: %s", status)
	}
	if none.Len() != 0 || none.Get("h1") != nil {
		t.Error("nil index accessors")
	}
	for s, want := range map[CIMatchStatus]string{
		CIMatched: "matched", CIDisabled: "disabled", CINoLabel: "no_label", CINotFound: "not_found", CIAmbiguous: "ambiguous",
		CIUnavailable: "unavailable", 0: "unknown",
	} {
		if s.String() != want {
			t.Errorf("CIMatchStatus(%d).String() = %q", s, s.String())
		}
	}
}

func TestEnrich(t *testing.T) {
	t.Parallel()
	x := testIndex()
	labels := map[string]string{"instance": "a", "ci_env": "forged", "ci_tag_gone": "true", "ci_tag_core": "forged"}
	Enrich(labels, x.Get("h1"), []string{"core", "dmz zone", "gone"})
	want := map[string]string{
		"instance": "a", "ci_env": "prod", "ci_owner": "u1", "ci_model": "host", "ci_tag_core": "true", "ci_tag_dmz_x20_zone": "true",
	}
	if !maps.Equal(labels, want) {
		t.Errorf("Enrich:\n got %v\nwant %v", labels, want)
	}
	// 资源上没有的内容：同名标签被删除，而不是保留序列自带的取值。
	labels = map[string]string{"ci_env": "forged", "ci_owner": "forged", "ci_model": "forged", "k": "v"}
	Enrich(labels, x.Get("vm1"), nil)
	if !maps.Equal(labels, map[string]string{"ci_model": "vm", "k": "v"}) {
		t.Errorf("Enrich without env / owner: %v", labels)
	}
	// 没有关联到资源：不动标签。
	labels = map[string]string{"ci_env": "from-series"}
	Enrich(labels, nil, []string{"core"})
	if !maps.Equal(labels, map[string]string{"ci_env": "from-series"}) {
		t.Errorf("Enrich(nil): %v", labels)
	}
	// 取值收进标签值的上限。
	labels = map[string]string{}
	Enrich(labels, &CI{ID: "x", Env: strings.Repeat("环", 300)}, nil)
	if n := len([]rune(labels["ci_env"])); n != mc.MaxLabelValueLen {
		t.Errorf("ci_env is %d runes", n)
	}
	// 标签名恒合法，且不同的资源标签不会落到同一个名称上（中文标签很常见）。
	seen := map[string]string{}
	for tag, want := range map[string]string{
		"core": "ci_tag_core", "Core_1": "ci_tag_Core_1", "dmz zone": "ci_tag_dmz_x20_zone", "dmz_zone": "ci_tag_dmz_zone",
		"核心": "ci_tag__x6838__x5fc3_", "边缘": "ci_tag__x8fb9__x7f18_", "a-b": "ci_tag_a_x2d_b", "a.b": "ci_tag_a_x2e_b", "": "ci_tag_",
	} {
		got := TagLabel(tag)
		if got != want || !mc.ValidLabelName(got) {
			t.Errorf("TagLabel(%q) = %q, want %q (valid label name)", tag, got, want)
		}
		if prev, dup := seen[got]; dup {
			t.Errorf("TagLabel collides for %q and %q", prev, tag)
		}
		seen[got] = tag
	}
	// 转义后超出标签名上限的资源标签：直接调用 Enrich 时跳过，不写出不合法的标签名。
	long := strings.Repeat("核", mc.MaxCodeLen/3)
	labels = map[string]string{}
	Enrich(labels, &CI{ID: "x", Tags: []string{long, "core"}}, []string{long, "core"})
	if !maps.Equal(labels, map[string]string{"ci_tag_core": "true"}) {
		t.Errorf("over-long tag label: %v", labels)
	}
}

func TestInScope(t *testing.T) {
	t.Parallel()
	x := testIndex()
	h1 := x.Get("h1")
	for _, tc := range []struct {
		name  string
		scope mc.ResourceScope
		ci    *CI
		want  bool
	}{
		{"disabled, no resource", mc.ResourceScope{}, nil, true},
		{"disabled ignores conditions", mc.ResourceScope{Envs: []string{"test"}}, h1, true},
		{"enabled requires a resource", mc.ResourceScope{Enabled: true}, nil, false},
		{"enabled without conditions", mc.ResourceScope{Enabled: true}, h1, true},
		{"model", mc.ResourceScope{Enabled: true, ModelCodes: []string{"vm", "host"}}, h1, true},
		{"other model", mc.ResourceScope{Enabled: true, ModelCodes: []string{"vm"}}, h1, false},
		{"env", mc.ResourceScope{Enabled: true, Envs: []string{"prod"}}, h1, true},
		{"other env", mc.ResourceScope{Enabled: true, Envs: []string{"test"}}, h1, false},
		{"resource without env", mc.ResourceScope{Enabled: true, Envs: []string{"prod"}}, x.Get("sw1"), false},
		{"any tag", mc.ResourceScope{Enabled: true, Tags: []string{"edge", "core"}}, h1, true},
		{"no tag", mc.ResourceScope{Enabled: true, Tags: []string{"edge"}}, h1, false},
		{"all conditions", mc.ResourceScope{Enabled: true, ModelCodes: []string{"host"}, Envs: []string{"prod"}, Tags: []string{"core"}}, h1, true},
		{"one condition fails", mc.ResourceScope{Enabled: true, ModelCodes: []string{"host"}, Envs: []string{"prod"}, Tags: []string{"edge"}}, h1, false},
	} {
		if got := InScope(tc.scope, tc.ci); got != tc.want {
			t.Errorf("%s: InScope() = %v, want %v", tc.name, got, tc.want)
		}
	}
	var none *CI
	if none.Maintenance() || h1.Maintenance() || !x.Get("h2").Maintenance() {
		t.Error("Maintenance")
	}
}

func TestPipeline(t *testing.T) {
	t.Parallel()
	x := testIndex()
	p, err := Compile(Spec{
		Relabel:  []mc.RelabelStep{{Action: "replace", SourceLabels: []string{"instance"}, Regex: "(.+):\\d+", TargetLabel: "host_ip"}, {Action: "labeldrop", Regex: "tmp_.*"}},
		DropExpr: `{{ if eq $labels.ci_env "test" }}true{{ end }}`,
		CIMatch:  mc.CIMatch{Label: "host_ip", By: "attr", EnrichTags: []string{"core"}, ModelCodes: []string{"host", "switch"}},
		Scope:    mc.ResourceScope{Enabled: true, ModelCodes: []string{"host"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	run := func(labels map[string]string) (map[string]string, Resolution) {
		p.Relabel(labels)
		return labels, p.Resolve(labels, 1, x)
	}

	// 关联到 h1：改写结果用于关联，富化标签写入，留在范围内。
	labels, res := run(map[string]string{"instance": "10.0.0.1:9100", "tmp_x": "1"})
	if res.Verdict != Keep || res.CI == nil || res.CI.ID != "h1" || res.CIStatus != CIMatched || res.Maintenance || res.DropErr != nil {
		t.Errorf("h1: %+v", res)
	}
	if !maps.Equal(labels, map[string]string{
		"instance": "10.0.0.1:9100", "host_ip": "10.0.0.1", "ci_env": "prod", "ci_owner": "u1", "ci_model": "host", "ci_tag_core": "true",
	}) {
		t.Errorf("h1 labels: %v", labels)
	}
	// h2 是 test 环境：条件丢弃读到了富化标签；丢弃先于范围过滤。
	if _, res = run(map[string]string{"instance": "10.0.0.2:9100"}); res.Verdict != Dropped || res.CI.ID != "h2" || !res.Maintenance {
		t.Errorf("h2: %+v", res)
	}
	// 交换机关联得上，但不在资源范围内。
	if _, res = run(map[string]string{"instance": "10.0.0.254:161"}); res.Verdict != OutOfScope || res.CI.ID != "sw1" {
		t.Errorf("sw1: %+v", res)
	}
	// 关联不上：启用了资源范围的规则不产生事件。
	if _, res = run(map[string]string{"instance": "10.9.9.9:9100"}); res.Verdict != OutOfScope || res.CI != nil || res.CIStatus != CINotFound {
		t.Errorf("unknown host: %+v", res)
	}
	// 索引尚未加载。
	labels = map[string]string{"instance": "10.0.0.1:9100"}
	p.Relabel(labels)
	if res = p.Resolve(labels, 1, nil); res.Verdict != OutOfScope || res.CIStatus != CIUnavailable {
		t.Errorf("no index: %+v", res)
	}

	// 条件丢弃渲染失败：不丢弃，原因带回。
	bad, err := Compile(Spec{DropExpr: "{{ humanize $labels.instance }}", CIMatch: mc.CIMatch{Disabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	if res = bad.Resolve(map[string]string{"instance": "x"}, 1, x); res.Verdict != Keep || res.DropErr == nil || res.CIStatus != CIDisabled {
		t.Errorf("drop error: %+v", res)
	}

	// nil 流水线 = 零值配置：不改写，按全局缺省关联并富化，不丢弃、不限范围。
	var none *Pipeline
	labels = map[string]string{"ident": "web-01", "tmp_x": "1"}
	none.Relabel(labels)
	res = none.Resolve(labels, 1, x)
	if res.Verdict != Keep || res.CI == nil || res.CI.ID != "h1" || labels["tmp_x"] != "1" || labels["ci_env"] != "prod" {
		t.Errorf("nil pipeline: %+v %v", res, labels)
	}
	zero, err := Compile(Spec{})
	if err != nil {
		t.Fatal(err)
	}
	labels2 := map[string]string{"ident": "web-01", "tmp_x": "1"}
	zero.Relabel(labels2)
	if res2 := zero.Resolve(labels2, 1, x); res2.CI != res.CI || !maps.Equal(labels, labels2) {
		t.Error("zero Spec must behave like the nil pipeline")
	}

	// 编译后不受调用方后续修改的影响。
	spec := Spec{CIMatch: mc.CIMatch{ModelCodes: []string{"vm"}}, Scope: mc.ResourceScope{Enabled: true, Envs: []string{"prod"}}}
	iso, err := Compile(spec)
	if err != nil {
		t.Fatal(err)
	}
	spec.CIMatch.ModelCodes[0], spec.Scope.Envs[0] = "host", "test"
	if res = iso.Resolve(map[string]string{"ident": "web-02"}, 1, x); res.CI == nil || res.CI.ID != "vm1" {
		t.Errorf("compiled pipeline observed a later mutation: %+v", res)
	}

	for name, spec := range map[string]Spec{
		"bad relabel":                      {Relabel: []mc.RelabelStep{{Action: "nope"}}},
		"bad drop expr":                    {DropExpr: "{{ if }}"},
		"bad ci match":                     {CIMatch: mc.CIMatch{By: "name"}},
		"enrich tag too long once escaped": {CIMatch: mc.CIMatch{EnrichTags: []string{strings.Repeat("核", mc.MaxCodeLen/3)}}},
		"bad scope":                        {Scope: mc.ResourceScope{Envs: []string{"prod"}}},
	} {
		if p, err := Compile(spec); err == nil || p != nil || !errors.Is(err, mc.ErrInvalid) {
			t.Errorf("%s: Compile() = %v, %v; want ErrInvalid", name, p, err)
		}
	}
}

func BenchmarkPipeline(b *testing.B) {
	cis := make([]CI, 0, 5000)
	for i := range 5000 {
		ip := "10." + itoa(i/65536) + "." + itoa(i/256%256) + "." + itoa(i%256)
		cis = append(cis, CI{ID: "ci-" + itoa(i), ModelCode: "host", Name: "host-" + itoa(i), Env: "prod", Owner: "u", Tags: []string{"core"}, Attrs: map[string][]string{"ip": {ip}}})
	}
	x := NewCIIndex(cis)
	p, err := Compile(Spec{
		Relabel:  []mc.RelabelStep{{Action: "replace", SourceLabels: []string{"instance"}, Regex: "(.+):\\d+", TargetLabel: "host_ip"}, {Action: "labeldrop", Regex: "tmp_.*"}},
		DropExpr: `{{ if eq $labels.ci_env "dev" }}true{{ end }}`,
		CIMatch:  mc.CIMatch{EnrichTags: []string{"core"}},
		Scope:    mc.ResourceScope{Enabled: true, Envs: []string{"prod"}},
	})
	if err != nil {
		b.Fatal(err)
	}
	src := map[string]string{"instance": "10.0.4.210:9100", "job": "node", "cpu": "0", "mode": "idle", "tmp_a": "1", "team": "ops"}
	labels := make(map[string]string, 16)
	b.ReportAllocs()
	for b.Loop() {
		clear(labels)
		maps.Copy(labels, src)
		p.Relabel(labels)
		if res := p.Resolve(labels, 1, x); res.Verdict != Keep || res.CI == nil {
			b.Fatalf("unexpected resolution: %+v", res)
		}
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
