package biz

import (
	"strings"
	"testing"
)

const sampleDoc = `# Nginx 运维手册

简介段落。

## 部署

部署步骤一。

部署步骤二。

### 回滚

回滚说明。

` + "```bash\nsystemctl restart nginx\n# not a heading\n```" + `

## 排障

| 现象 | 处理 |
|---|---|
| 502 | 查上游 |

排障结尾。
`

func TestParseSectionsAndHeadings(t *testing.T) {
	t.Parallel()
	got := Headings(sampleDoc)
	want := []string{"Nginx 运维手册", "Nginx 运维手册 > 部署", "Nginx 运维手册 > 部署 > 回滚", "Nginx 运维手册 > 排障"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("Headings = %v, want %v", got, want)
	}
}

func TestSection(t *testing.T) {
	t.Parallel()
	sec, ok := Section(sampleDoc, "Nginx 运维手册 > 部署")
	if !ok {
		t.Fatal("section not found")
	}
	for _, s := range []string{"部署步骤一", "回滚说明", "systemctl restart nginx"} {
		if !strings.Contains(sec, s) {
			t.Errorf("section missing %q:\n%s", s, sec)
		}
	}
	if strings.Contains(sec, "排障结尾") {
		t.Errorf("section leaked next sibling:\n%s", sec)
	}
	if _, ok := Section(sampleDoc, "不存在"); ok {
		t.Fatal("unknown heading must not match")
	}
	// 大小写与空白不敏感。
	if _, ok := Section(sampleDoc, "nginx 运维手册>部署 > 回滚"); !ok {
		t.Fatal("normalized heading path must match")
	}
}

func TestSplit(t *testing.T) {
	t.Parallel()
	chunks := Split("Nginx 运维手册", sampleDoc)
	if len(chunks) == 0 {
		t.Fatal("no chunks")
	}
	for i, c := range chunks {
		if c.Seq != int32(i+1) {
			t.Errorf("chunk %d seq = %d", i, c.Seq)
		}
		if c.Content == "" || c.Tokens == "" || len(c.SHA256) != 64 || c.TokenCount == 0 {
			t.Errorf("chunk %d incomplete: %+v", i, c)
		}
		if !strings.Contains(c.Tokens, "nginx") {
			t.Errorf("chunk %d tokens must carry the document title: %q", i, c.Tokens)
		}
		if strings.Contains(c.Content, "nginx 运维手册\n") {
			t.Errorf("chunk %d content must not carry the title prefix", i)
		}
	}
	// 代码块不被拆开。
	found := false
	for _, c := range chunks {
		if strings.Contains(c.Content, "systemctl restart nginx") {
			found = true
			if !strings.Contains(c.Content, "```") || !strings.Contains(c.Content, "# not a heading") {
				t.Errorf("code fence split or heading misparsed: %q", c.Content)
			}
		}
	}
	if !found {
		t.Fatal("code block chunk missing")
	}
}

func TestSplitLongParagraph(t *testing.T) {
	t.Parallel()
	para := strings.Repeat("这是一个很长的句子，用来测试分块。", 300) // ≈ 5400 字符
	chunks := Split("长文", "# 标题\n\n"+para)
	if len(chunks) < 4 {
		t.Fatalf("expected several chunks, got %d", len(chunks))
	}
	for _, c := range chunks {
		if n := runeLen(c.Content); n > maxChunkChars+minChunkChars {
			t.Errorf("chunk too long: %d", n)
		}
		if c.HeadingPath != "标题" {
			t.Errorf("heading path = %q", c.HeadingPath)
		}
	}
	// 相同内容 → 相同摘要（向量复用的前提）。
	again := Split("长文", "# 标题\n\n"+para)
	for i := range chunks {
		if chunks[i].SHA256 != again[i].SHA256 {
			t.Fatalf("chunk %d sha differs between identical splits", i)
		}
	}
}

func TestSplitEmpty(t *testing.T) {
	t.Parallel()
	if got := Split("t", "   \n\n"); len(got) != 0 {
		t.Fatalf("empty doc produced %d chunks", len(got))
	}
}

func TestSlugify(t *testing.T) {
	t.Parallel()
	id := "0193c9a0-1111-7000-8000-000000000001"
	cases := map[string]string{
		"Nginx 502 Troubleshooting": "nginx-502-troubleshooting",
		"  K8s/Pod  重启 ":            "k8s-pod",
		"纯中文标题":                     "doc-0193c9a0",
		"a":                         "doc-0193c9a0",
	}
	for in, want := range cases {
		if got := slugify(in, id); got != want {
			t.Errorf("slugify(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestMakeSnippet(t *testing.T) {
	t.Parallel()
	content := strings.Repeat("前置内容。", 50) + "当 nginx 返回 502 时，先检查上游连接池。" + strings.Repeat("后续内容。", 50)
	snippet := makeSnippet(content, []string{"502", "连接", "接池"})
	if !strings.Contains(snippet, "<mark>502</mark>") || !strings.Contains(snippet, "<mark>连接池</mark>") {
		t.Fatalf("snippet marks = %q", snippet)
	}
	if !strings.HasPrefix(snippet, "…") || !strings.HasSuffix(snippet, "…") {
		t.Fatalf("snippet should be windowed: %q", snippet)
	}
	// HTML 转义：正文里的标签不会原样输出。
	if s := makeSnippet("<script>alert(1)</script> 502", []string{"502"}); strings.Contains(s, "<script>") {
		t.Fatalf("snippet must escape html: %q", s)
	}
}

func TestEmbedGuard(t *testing.T) {
	t.Parallel()
	g := NewEmbedGuard()
	for i := 0; i < breakerThreshold-1; i++ {
		g.Failure()
	}
	if !g.Allow() {
		t.Fatal("guard opened too early")
	}
	g.Failure()
	if g.Allow() {
		t.Fatal("guard should be open after threshold")
	}
}
