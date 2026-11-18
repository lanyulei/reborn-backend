package monitorcontract

import (
	"errors"
	"strings"
	"testing"
	"text/template"
)

func TestValidateTemplate(t *testing.T) {
	t.Parallel()
	valid := []string{
		"",
		"plain text",
		"{{ $labels.instance }} = {{ $value }}",
		"{{ .Labels.env }}/{{ .Value }}",
		`{{ printf "%.2f" $value }}`,
		`{{ printf "%6.1f%% of %-4s|" $value "cpu" }}`,
		`{{ $value | printf "%.1f" }}`,
		`{{ $value | humanizePercentage }}`,
		`{{ toUpper $labels.env }} {{ toLower "ABC" }} {{ title "disk full" }}`,
		`{{ if and (gt $value 0.4) (not (eq $labels.env "dev")) }}high{{ else }}low{{ end }}`,
		`{{ range $k, $v := $labels }}{{ $k }}={{ $v }};{{ end }}`,
		`{{ range $labels }}{{ . }}{{ else }}none{{ end }}`,
		`{{ range $k, $v := $labels }}{{ if eq $k "a" }}{{ break }}{{ end }}{{ continue }}{{ end }}`,
		`{{ $x := $labels.instance }}{{ $x }}`,
		`{{ with $labels.instance }}{{ . }}{{ end }}`,
		`{{/* a comment */}}text`,
		`{{ define "unused" }}{{ humanize 1.0 }}{{ end }}text`,
		strings.Repeat("a", MaxTemplateBytes),
	}
	for _, src := range valid {
		if err := ValidateTemplate(src); err != nil {
			t.Errorf("ValidateTemplate(%q) = %v, want nil", src, err)
		}
	}
	invalid := map[string]string{
		// 评审 E1 的原始攻击模板：渲染会在整数上循环 9e18 次。
		"E1 assign labels":              `{{$labels = 9000000000000000000}}{{range $labels}}{{end}}`,
		"E1 redeclare labels":           `{{$labels := 9000000000000000000}}{{range $labels}}{{end}}`,
		"redeclare labels in a scope":   `{{with 1}}{{$labels := 9000000000000000000}}{{range $labels}}{{end}}{{end}}`,
		"redeclare labels as range var": `{{range $labels, $x := $labels}}{{end}}`,
		"assign value":                  `{{$value = 1}}`,
		"redeclare value":               `{{$value := 1}}`,
		"assign root":                   `{{$ = 9000000000000000000}}{{range $.Labels}}{{end}}`,
		"redeclare root":                `{{$ := 1}}`,
		"assign any variable":           `{{$x := 1}}{{$x = 2}}`,
		"range over integer":            `{{range 100000000}}x{{end}}`,
		"range over variable":           `{{$n := 9000000000000000000}}{{range $n}}{{end}}`,
		"range over dot field":          `{{range .Labels}}x{{end}}`,
		"range over rebound dot":        `{{with 9000000000000000000}}{{range .}}{{end}}{{end}}`,
		"range over root field":         `{{range $.Labels}}x{{end}}`,
		"range over labels field":       `{{range $labels.x}}x{{end}}`,
		"range over pipeline":           `{{range $labels | toUpper}}x{{end}}`,
		"range assignment":              `{{$k := 1}}{{range $k = $labels}}{{end}}`,
		"nested range":                  `{{range $labels}}{{range $labels}}{{end}}{{end}}`,
		"nested range through if":       `{{range $labels}}{{if true}}{{range $labels}}{{end}}{{end}}{{end}}`,
		"nested range in else":          `{{range $labels}}{{else}}{{with 1}}{{range $labels}}{{end}}{{end}}{{end}}`,
		"unknown function":              `{{nope $value}}`,
		"builtin index":                 `{{index $labels "env"}}`,
		"builtin len":                   `{{len $labels}}`,
		"builtin call":                  `{{call .Value}}`,
		"builtin html":                  `{{html $labels.env}}`,
		"builtin print in pipeline":     `{{$value | print}}`,
		"builtin in if condition":       `{{if len $labels}}x{{end}}`,
		"builtin in parenthesised arg":  `{{printf "%d" (len $labels)}}`,
		"builtin in declaration":        `{{$x := len $labels}}`,
		"printf huge width":             `{{printf "%999999d" 1}}`,
		"printf huge precision":         `{{printf "%.999999f" $value}}`,
		"printf star width":             `{{printf "%*d" 999999 1}}`,
		"printf explicit index":         `{{printf "%[1]d" 1}}`,
		"printf index width":            `{{printf "%[2]*[1]d" 1 999999}}`,
		"printf dynamic format":         `{{printf $labels.fmt 1}}`,
		"printf format from pipeline":   `{{"%999999d" | printf}}`,
		"printf as argument":            `{{eq printf 1}}`,
		"printf without format":         `{{printf}}`,
		"template call":                 `{{define "a"}}x{{end}}{{template "a"}}`,
		"recursive template":            `{{define "a"}}{{template "a"}}{{end}}`,
		"block":                         `{{block "a" .}}x{{end}}`,
		"define with bad function":      `{{define "a"}}{{len .}}{{end}}`,
		"define with integer range":     `{{define "a"}}{{range 100000000}}{{end}}{{end}}`,
		"define with huge printf":       `{{define "a"}}{{printf "%999999d" 1}}{{end}}`,
		"define redefining main":        `{{define "t"}}x{{end}}`,
		"syntax error":                  `{{ $labels.instance `,
		"too long":                      strings.Repeat("a", MaxTemplateBytes+1),
	}
	for name, src := range invalid {
		err := ValidateTemplate(src)
		if err == nil {
			t.Errorf("%s: ValidateTemplate accepted the template", name)
			continue
		}
		if !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: error %v does not wrap ErrInvalid", name, err)
		}
	}
}

// 校验用的函数表必须与 text/template 的真实解析一致：凡通过校验的模板，带上 TemplateFuncs 的
// 实现后必能被 text/template 解析（执行方不会在校验之后再遇到解析失败）。
func TestValidatedTemplateParses(t *testing.T) {
	t.Parallel()
	funcs := template.FuncMap{}
	for _, name := range TemplateFuncs() {
		funcs[name] = func(...any) string { return "" }
	}
	for name := range templateFuncs {
		if _, custom := funcs[name]; custom {
			continue
		}
		// 其余必须是 text/template 的内建：不提供实现也能解析。
		if _, err := template.New("t").Parse("{{" + name + " 1 1}}"); err != nil {
			t.Errorf("%s is neither in TemplateFuncs nor a text/template builtin: %v", name, err)
		}
	}
	src := `{{ if gt $value 1.0 }}{{ printf "%.1f" $value | toUpper }}{{ end }}{{ range $k, $v := $labels }}{{ title $v }}{{ end }}`
	if err := ValidateTemplate(src); err != nil {
		t.Fatal(err)
	}
	if _, err := template.New("t").Funcs(funcs).Parse(TemplatePrefix + src); err != nil {
		t.Fatal(err)
	}
}

func TestCheckPrintfFormat(t *testing.T) {
	t.Parallel()
	for format, ok := range map[string]bool{
		"":           true,
		"plain":      true,
		"%d %s %v":   true,
		"100%%":      true,
		"%-10.3f":    true,
		"%+#x":       true,
		"%256d":      true,
		"%.256f":     true,
		"%257d":      false,
		"%.257f":     false,
		"%10.999f":   false,
		"%*d":        false,
		"%.*f":       false,
		"%[1]d":      false,
		"%[3]*.[2]f": false,
		"%-[1]d":     false,
		"%":          true,
	} {
		if err := CheckPrintfFormat(format); (err == nil) != ok {
			t.Errorf("CheckPrintfFormat(%q) = %v, want ok = %v", format, err, ok)
		}
	}
}

func FuzzValidateTemplate(f *testing.F) {
	for _, seed := range []string{
		`{{$labels = 9000000000000000000}}{{range $labels}}{{end}}`,
		`{{ range $k, $v := $labels }}{{ printf "%5s" $v }}{{ end }}`,
		`{{define "a"}}{{end}}`,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(_ *testing.T, src string) {
		_ = ValidateTemplate(src) // 不得 panic
	})
}
