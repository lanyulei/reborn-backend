package docparse

import (
	"archive/zip"
	"bytes"
	"strings"
	"testing"
)

func TestKind(t *testing.T) {
	t.Parallel()
	cases := map[[2]string]string{
		{"a.md", "text/plain"}:             "markdown",
		{"a.TXT", "text/plain"}:            "text",
		{"page.html", "text/html"}:         "html",
		{"doc.pdf", "application/pdf"}:     "pdf",
		{"doc.docx", "application/zip"}:    "docx",
		{"noext", "text/plain"}:            "text",
		{"img.png", "image/png"}:           "",
		{"archive.zip", "application/zip"}: "",
	}
	for in, want := range cases {
		if got := Kind(in[0], in[1]); got != want {
			t.Errorf("Kind(%q, %q) = %q, want %q", in[0], in[1], got, want)
		}
	}
}

func TestParseTextAndMarkdown(t *testing.T) {
	t.Parallel()
	got, err := Parse("markdown", []byte("\uFEFF# T\r\n\r\nbody\x00\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	if got != "# T\n\nbody\n" {
		t.Fatalf("Parse(markdown) = %q", got)
	}
	if _, err := Parse("xlsx", nil); err == nil {
		t.Fatal("unsupported kind must fail")
	}
}

func TestParseHTML(t *testing.T) {
	t.Parallel()
	src := `<html><head><title>x</title><style>p{}</style><script>alert(1)</script></head>
<body><h1>标题</h1><p>第一段</p><ul><li>项一</li><li>项二</li></ul><h2>子标题</h2><pre>code line</pre><p>结尾</p></body></html>`
	got, err := Parse("html", []byte(src))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"# 标题", "第一段", "- 项一", "- 项二", "## 子标题", "```\ncode line\n```", "结尾"} {
		if !strings.Contains(got, want) {
			t.Errorf("html output missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "alert(1)") || strings.Contains(got, "p{}") {
		t.Errorf("script/style leaked:\n%s", got)
	}
}

func TestParseDocx(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create("word/document.xml")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = w.Write([]byte(`<?xml version="1.0"?>
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body>
<w:p><w:pPr><w:pStyle w:val="Heading1"/></w:pPr><w:r><w:t>部署</w:t></w:r></w:p>
<w:p><w:r><w:t>第一</w:t></w:r><w:r><w:t>段</w:t></w:r></w:p>
<w:p><w:pPr><w:numPr><w:ilvl w:val="0"/></w:numPr></w:pPr><w:r><w:t>列表项</w:t></w:r></w:p>
<w:tbl><w:tr><w:tc><w:p><w:r><w:t>A</w:t></w:r></w:p></w:tc><w:tc><w:p><w:r><w:t>B</w:t></w:r></w:p></w:tc></w:tr></w:tbl>
</w:body></w:document>`))
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := Parse("docx", buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"# 部署", "第一段", "- 列表项", "| A | B |"} {
		if !strings.Contains(got, want) {
			t.Errorf("docx output missing %q:\n%s", want, got)
		}
	}
	if _, err := Parse("docx", []byte("not a zip")); err == nil {
		t.Fatal("invalid docx must fail")
	}
}

func TestParsePDFInvalid(t *testing.T) {
	t.Parallel()
	if _, err := Parse("pdf", []byte("%PDF-1.7 garbage")); err == nil {
		t.Fatal("invalid pdf must fail, not panic")
	}
}
