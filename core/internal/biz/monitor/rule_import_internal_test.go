package biz

import (
	"fmt"
	"strings"
	"testing"

	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	"github.com/go-kratos/kratos/v3/errors"
	"gopkg.in/yaml.v3"
)

const sampleRuleFile = `
groups:
  - name: node
    interval: 30s
    rules:
      - alert: HighCPU
        expr: cpu_usage > 85
        for: 5m
        labels:
          severity: warning
          team: infra
        annotations:
          summary: "{{ $labels.instance }} CPU {{ $value }}"
      - alert: HighCPU
        expr: cpu_usage > 95
        for: 5m
        labels:
          severity: critical
          team: infra
      - record: job:up:sum
        expr: sum by (job) (up)
      - alert: Down
        expr: up == 0
        for: 1h30m
        keep_firing_for: 1d
        labels:
          severity: page
  - name: misc
    rules:
      - alert: Weird
        expr: up ==
      - alert: NoSeverity
        expr: up == 0
        unknown_field: 1
      - alert: OddSeverity
        expr: up == 0
        labels:
          severity: sev-9
      - expr: up == 0
`

func importOpts() *ImportOptions {
	return &ImportOptions{GroupID: mc.RootGroupID, DatasourceScope: mc.DatasourceScopeAll, DefaultSeverity: mc.SeverityInfo}
}

func convertAll(t *testing.T, content string) []*ImportItem {
	t.Helper()
	parsed, err := parseRuleFile(content)
	if err != nil {
		t.Fatalf("parseRuleFile() error = %v", err)
	}
	items := make([]*ImportItem, 0, len(parsed))
	for i, p := range parsed {
		items = append(items, convertRule(i, p, importOpts()))
	}
	return items
}

func TestConvertRule(t *testing.T) {
	t.Parallel()
	items := convertAll(t, sampleRuleFile)
	if len(items) != 8 {
		t.Fatalf("items = %d, want 8", len(items))
	}
	type want struct {
		name, status string
		severity     int32
		forS, recov  int32
		interval     int32
		errPart      string
		warnPart     string
	}
	wants := []want{
		{name: "HighCPU", status: ImportNew, severity: mc.SeverityWarning, forS: 300, interval: 30},
		{name: "HighCPU", status: ImportNew, severity: mc.SeverityCritical, forS: 300, interval: 30},
		{name: "job:up:sum", status: ImportSkipped, errPart: "记录规则"},
		{name: "Down", status: ImportNew, severity: mc.SeverityFatal, forS: 5400, recov: 86400, interval: 30},
		{name: "Weird", status: ImportInvalid, errPart: "query 1"},
		{name: "NoSeverity", status: ImportNew, severity: mc.SeverityInfo, interval: 60, warnPart: "unknown_field"},
		{name: "OddSeverity", status: ImportNew, severity: mc.SeverityInfo, interval: 60, warnPart: "sev-9"},
		{name: "", status: ImportInvalid, errPart: "alert"},
	}
	for i, w := range wants {
		it := items[i]
		if it.Index != int32(i) || it.Name != w.name || it.Status != w.status {
			t.Errorf("item %d = {%d %q %q} (%s), want {%q %q}", i, it.Index, it.Name, it.Status, it.Error, w.name, w.status)
			continue
		}
		if w.errPart != "" && !strings.Contains(it.Error, w.errPart) {
			t.Errorf("item %d error = %q, want containing %q", i, it.Error, w.errPart)
		}
		if w.warnPart != "" && !strings.Contains(strings.Join(it.Warnings, "|"), w.warnPart) {
			t.Errorf("item %d warnings = %v, want containing %q", i, it.Warnings, w.warnPart)
		}
		if w.status != ImportNew {
			if it.Spec != nil {
				t.Errorf("item %d: spec must be nil for status %s", i, it.Status)
			}
			continue
		}
		s := it.Spec
		if len(s.Queries) != 1 || s.Queries[0].Severity != w.severity || s.ForS != w.forS || s.RecoverS != w.recov || s.EvalIntervalS != w.interval {
			t.Errorf("item %d spec = sev %d for %d recover %d interval %d; want %d %d %d %d", i,
				s.Queries[0].Severity, s.ForS, s.RecoverS, s.EvalIntervalS, w.severity, w.forS, w.recov, w.interval)
		}
		if _, has := s.Labels["severity"]; has {
			t.Errorf("item %d: severity must not stay as an extra label", i)
		}
		if s.Enabled {
			t.Errorf("item %d: converted rule must default to disabled", i)
		}
	}
	first := items[0].Spec
	if first.Labels["team"] != "infra" || first.Annotations["summary"] != "{{ $labels.instance }} CPU {{ $value }}" {
		t.Errorf("labels / annotations not carried over: %+v", first)
	}
	if items[0].SourceGroup != "node" || items[5].SourceGroup != "misc" {
		t.Errorf("source group not recorded")
	}
}

func TestParseRuleFile_Rejects(t *testing.T) {
	t.Parallel()
	tooMany := "groups:\n- name: g\n  rules:\n" + strings.Repeat("  - alert: a\n    expr: up\n", MaxImportRules+1)
	deep := "groups:\n- name: g\n  rules:\n  - alert: a\n    expr: up\n    labels: " + strings.Repeat("{a: ", 20) + "x" + strings.Repeat("}", 20) + "\n"
	cases := map[string]string{
		"not yaml":        "groups: [unterminated",
		"scalar":          "just a string",
		"no groups":       "rules:\n- alert: a\n  expr: up\n",
		"empty groups":    "groups: []\n",
		"groups not list": "groups: 5\n",
		"oversized":       "groups:\n# " + strings.Repeat("x", maxImportBytes) + "\n",
		"too many rules":  tooMany,
		"too deep":        deep,
		"anchor":          "groups:\n- name: g\n  rules:\n  - &r\n    alert: a\n    expr: up\n",
		"alias": `
base: &b {alert: a, expr: up}
groups:
- name: g
  rules: [*b, *b]
`,
		// 经典的别名炸弹：每层把上一层引用多次，展开后指数增长。必须在展开前拒绝。
		"alias bomb": `
a: &a ["x","x","x","x","x","x","x","x","x"]
b: &b [*a,*a,*a,*a,*a,*a,*a,*a,*a]
c: &c [*b,*b,*b,*b,*b,*b,*b,*b,*b]
d: &d [*c,*c,*c,*c,*c,*c,*c,*c,*c]
e: &e [*d,*d,*d,*d,*d,*d,*d,*d,*d]
f: &f [*e,*e,*e,*e,*e,*e,*e,*e,*e]
g: &g [*f,*f,*f,*f,*f,*f,*f,*f,*f]
groups: *g
`,
	}
	for name, content := range cases {
		if _, err := parseRuleFile(content); !errors.Is(err, ErrRuleImportInvalid) {
			t.Errorf("%s: err = %v, want ErrRuleImportInvalid", name, err)
		}
	}
}

func TestParseRuleFile_PerRuleProblems(t *testing.T) {
	t.Parallel()
	// 单个条目的问题不影响其余条目。
	items := convertAll(t, `
groups:
- name: g
  interval: banana
  rules:
  - alert: BadInterval
    expr: up == 0
- name: h
  interval: 5s
  rules:
  - not-a-mapping
  - alert: BadFor
    expr: up == 0
    for: soon
  - alert: LongFor
    expr: up == 0
    for: 2d
  - alert: Clamped
    expr: up == 0
    for: 1500ms
  - alert: TemplatedLabel
    expr: up == 0
    labels: {severity: warn, owner: "{{ $labels.team }}"}
  - alert: BadLabel
    expr: up == 0
    labels: {"bad-name": x}
  - alert: BadTypes
    expr: up == 0
    labels: [a, b]
`)
	wants := []struct{ status, part string }{
		{ImportInvalid, "interval"},
		{ImportInvalid, "mapping"},
		{ImportInvalid, "for"},
		{ImportInvalid, "for"},
		{ImportNew, "已调整"},
		{ImportNew, "模板"},
		{ImportInvalid, "label"},
		{ImportInvalid, "cannot read rule"},
	}
	if len(items) != len(wants) {
		t.Fatalf("items = %d, want %d", len(items), len(wants))
	}
	for i, w := range wants {
		text := items[i].Error + "|" + strings.Join(items[i].Warnings, "|")
		if items[i].Status != w.status || !strings.Contains(text, w.part) {
			t.Errorf("item %d (%s) = %s %q, want %s containing %q", i, items[i].Name, items[i].Status, text, w.status, w.part)
		}
	}
	if s := items[4].Spec; s.EvalIntervalS != 15 || s.ForS != 2 {
		t.Errorf("clamped interval / rounded-up for = %d / %d, want 15 / 2", s.EvalIntervalS, s.ForS)
	}
}

func TestSeverityNames_ExportRoundTrip(t *testing.T) {
	t.Parallel()
	for sev := mc.SeverityInfo; sev <= mc.SeverityFatal; sev++ {
		name, ok := exportSeverityNames[sev]
		if !ok || severityNames[name] != sev {
			t.Errorf("severity %d exports as %q which imports as %d", sev, name, severityNames[name])
		}
	}
	for _, name := range []string{"CRITICAL", " Warning "} {
		items := convertAll(t, fmt.Sprintf("groups:\n- name: g\n  rules:\n  - alert: a\n    expr: up\n    labels: {severity: %q}\n", name))
		if items[0].Status != ImportNew || items[0].Spec.Queries[0].Severity == mc.SeverityInfo {
			t.Errorf("severity %q must be recognised case-insensitively: %+v", name, items[0])
		}
	}
}

func TestExportFile(t *testing.T) {
	t.Parallel()
	tree := newGroupTree([]*RuleGroup{
		{ID: "g1", Path: "/g1/", Name: "基础设施"},
		{ID: "g2", ParentID: "g1", Path: "/g1/g2/", Name: "主机"},
	})
	scope := Scope{admin: true, tree: tree}
	rule := func(id, group, name string, interval int32, queries ...mc.Query) *Rule {
		return &Rule{ID: id, Spec: RuleSpec{
			GroupID: group, Name: name, EvalIntervalS: interval, ForS: 300, RecoverS: 60, Queries: queries,
			Labels: map[string]string{"team": "infra"}, Annotations: map[string]string{"summary": "{{ $value }}"},
		}}
	}
	out, err := yaml.Marshal(exportFile(scope, []*Rule{
		rule("r2", "g2", "mem", 60, mc.Query{Expr: "mem > 90", Severity: mc.SeverityCritical}),
		rule("r1", "g2", "cpu", 60, mc.Query{Expr: "cpu > 85", Severity: mc.SeverityWarning}, mc.Query{Expr: "cpu > 95", Severity: mc.SeverityFatal}),
		rule("r3", "g1", "ping", 30, mc.Query{Expr: "up == 0", Severity: mc.SeverityInfo}),
	}))
	if err != nil {
		t.Fatal(err)
	}
	// 导出的内容必须能被自己的导入解析回来：多档阈值拆成多条、级别与时长不变。
	items := convertAll(t, string(out))
	if len(items) != 4 {
		t.Fatalf("exported rules = %d, want 4:\n%s", len(items), out)
	}
	got := map[string]int32{}
	for _, it := range items {
		if it.Status != ImportNew {
			t.Fatalf("re-import of exported rule failed: %+v\n%s", it, out)
		}
		s := it.Spec
		got[it.SourceGroup+"|"+it.Name+"|"+s.Queries[0].Expr] = s.Queries[0].Severity
		if it.Name != "ping" && (s.ForS != 300 || s.RecoverS != 60 || s.EvalIntervalS != 60 || s.Labels["team"] != "infra" || s.Annotations["summary"] != "{{ $value }}") {
			t.Errorf("round trip lost fields: %+v", s)
		}
	}
	want := map[string]int32{
		"基础设施|ping|up == 0":      mc.SeverityInfo,
		"基础设施 / 主机|cpu|cpu > 85": mc.SeverityWarning,
		"基础设施 / 主机|cpu|cpu > 95": mc.SeverityFatal,
		"基础设施 / 主机|mem|mem > 90": mc.SeverityCritical,
	}
	for k, sev := range want {
		if got[k] != sev {
			t.Errorf("exported %q severity = %d, want %d\n%s", k, got[k], sev, out)
		}
	}
}
