package notifycontract

import (
	"errors"
	htmltemplate "html/template"
	"testing"
	"text/template"
	"text/template/parse"
)

// 嵌套的 range 是指数级迭代（n^k），必须在解析期拒绝；单层 range、if / with 里的 range 都是合法的。
func TestCheckRangeNesting(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		text    string
		nested  bool
		htmlToo bool
	}{
		{name: "no range", text: `{{.Title}}`},
		{name: "single range", text: `{{range .Samples}}- {{.title}}{{end}}`},
		{name: "single range with else", text: `{{range .Payload}}{{.}}{{else}}none{{end}}`},
		{name: "range inside if", text: `{{if .Count}}{{range .Samples}}{{.title}}{{end}}{{end}}`},
		{name: "range inside with", text: `{{with .Payload}}{{range .}}{{.}}{{end}}{{end}}`},
		{name: "if inside range", text: `{{range .Samples}}{{if .title}}{{.title}}{{end}}{{end}}`},
		{name: "two sibling ranges", text: `{{range .Samples}}{{.title}}{{end}}{{range .Payload}}{{.}}{{end}}`},
		{name: "range nested in range", text: `{{range $.Payload}}{{range $.Payload}}{{end}}{{end}}`, nested: true},
		{name: "range nested through if inside range", text: `{{range $.Payload}}{{if true}}{{range $.Payload}}{{end}}{{end}}{{end}}`, nested: true},
		{name: "range nested in else branch", text: `{{range $.Payload}}{{else}}{{range $.Payload}}{{end}}{{end}}`, nested: true},
		{name: "three levels", text: `x{{range $.Payload}}{{range $.Payload}}{{range $.Payload}}{{end}}{{end}}{{end}}`, nested: true},
		{name: "range nested inside a defined template", text: `{{define "inner"}}{{range .Payload}}{{range .}}{{end}}{{end}}{{end}}{{.Title}}`, nested: true, htmlToo: false},
	}
	for _, c := range cases {
		textT, err := template.New("t").Parse(c.text)
		if err != nil {
			t.Fatalf("%s: parse: %v", c.name, err)
		}
		if got := CheckRangeNesting(TextTrees(textT)) != nil; got != c.nested {
			t.Errorf("%s (text): nested = %v, want %v", c.name, got, c.nested)
		}
		if c.htmlToo {
			continue
		}
		htmlT, err := htmltemplate.New("t").Parse(c.text)
		if err != nil {
			t.Fatalf("%s: html parse: %v", c.name, err)
		}
		if got := CheckRangeNesting(HTMLTrees(htmlT)) != nil; got != c.nested {
			t.Errorf("%s (html): nested = %v, want %v", c.name, got, c.nested)
		}
	}
	if err := CheckRangeNesting(nil); err != nil {
		t.Fatalf("no trees must be accepted, got %v", err)
	}
	if err := CheckRangeNesting([]*parse.Tree{nil}); err != nil {
		t.Fatalf("nil tree must be accepted, got %v", err)
	}
	if !errors.Is(CheckRangeNesting(TextTrees(mustParse(`{{range .}}{{range .}}{{end}}{{end}}`))), ErrNestedRange) {
		t.Fatal("the rejection must wrap ErrNestedRange")
	}
}

func mustParse(text string) *template.Template {
	return template.Must(template.New("t").Parse(text))
}
