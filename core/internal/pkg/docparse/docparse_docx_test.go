package docparse

import (
	"archive/zip"
	"bytes"
	"strings"
	"testing"
)

// docxOf 用给定的 word/document.xml 组一个最小 docx。
func docxOf(t *testing.T, documentXML string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create("word/document.xml")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte(documentXML)); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

const docxOpen = `<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><w:body>`

// 段落里的文字不只出现在直接子 run 里：超链接、修订插入、智能标记、行内内容控件包着的 run 也要收进来；
// 正文级内容控件里的段落不能整段丢失；删除的修订仍然不进正文。
func TestParseDocx_KeepsRunsNestedInParagraph(t *testing.T) {
	t.Parallel()
	src := `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` + docxOpen + `
<w:p><w:pPr><w:pStyle w:val="Heading1"/></w:pPr><w:r><w:t>部署</w:t></w:r></w:p>
<w:p><w:r><w:t xml:space="preserve">详见 </w:t></w:r><w:hyperlink r:id="rId5" w:history="1"><w:r><w:rPr><w:rStyle w:val="Hyperlink"/></w:rPr><w:t>部署手册</w:t></w:r></w:hyperlink><w:r><w:t> 结束</w:t></w:r></w:p>
<w:p><w:ins w:id="1" w:author="a" w:date="2024-01-01T00:00:00Z"><w:r><w:t>修订插入的文字</w:t></w:r></w:ins></w:p>
<w:p><w:smartTag w:element="place"><w:r><w:t>智能标记文字</w:t></w:r></w:smartTag></w:p>
<w:p><w:r><w:t>控件前</w:t></w:r><w:sdt><w:sdtPr><w:id w:val="-1"/></w:sdtPr><w:sdtContent><w:r><w:t>行内控件文字</w:t></w:r></w:sdtContent></w:sdt><w:r><w:t>控件后</w:t></w:r></w:p>
<w:p><w:r><w:fldChar w:fldCharType="begin"/></w:r><w:r><w:instrText xml:space="preserve"> HYPERLINK "http://x" </w:instrText></w:r><w:r><w:fldChar w:fldCharType="separate"/></w:r><w:r><w:t>域结果文字</w:t></w:r><w:r><w:fldChar w:fldCharType="end"/></w:r></w:p>
<w:sdt><w:sdtPr><w:id w:val="123456"/><w:alias w:val="块控件"/></w:sdtPr><w:sdtContent><w:p><w:r><w:t>控件里的段落</w:t></w:r></w:p></w:sdtContent></w:sdt>
<w:p><w:del w:id="2" w:author="a"><w:r><w:delText>已删除的文字</w:delText></w:r></w:del><w:r><w:t>保留的文字</w:t></w:r></w:p>
<w:sectPr><w:pgSz w:w="11906" w:h="16838"/></w:sectPr>
</w:body></w:document>`
	got, err := Parse("docx", docxOf(t, src))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"# 部署", "详见 部署手册 结束", "修订插入的文字", "智能标记文字",
		"控件前行内控件文字控件后", "域结果文字", "控件里的段落", "保留的文字",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("docx output missing %q:\n%s", want, got)
		}
	}
	for _, unwanted := range []string{"已删除的文字", "HYPERLINK", "http://x"} {
		if strings.Contains(got, unwanted) {
			t.Errorf("docx output must not contain %q:\n%s", unwanted, got)
		}
	}
	// 正文级内容控件里的段落按所在位置输出（在「控件前行内控件文字控件后」之后、「保留的文字」之前）。
	i, j, k := strings.Index(got, "控件前行内控件文字控件后"), strings.Index(got, "控件里的段落"), strings.Index(got, "保留的文字")
	if i < 0 || i >= j || j >= k {
		t.Fatalf("body-level content control paragraph out of place (i=%d j=%d k=%d):\n%s", i, j, k, got)
	}
}

// 表格留在它在正文里的位置：此前先写完全部段落再写全部表格，「说明 → 参数表 → 注意事项」会被排成
// 「说明 → 注意事项 → 参数表」，分块后表格挂到了最后一个标题下。
func TestParseDocx_TableKeepsDocumentOrder(t *testing.T) {
	t.Parallel()
	src := docxOpen + `
<w:p><w:pPr><w:pStyle w:val="Heading1"/></w:pPr><w:r><w:t>安装</w:t></w:r></w:p>
<w:p><w:r><w:t>说明段</w:t></w:r></w:p>
<w:tbl><w:tblPr><w:tblStyle w:val="TableGrid"/></w:tblPr><w:tblGrid><w:gridCol w:w="100"/><w:gridCol w:w="100"/></w:tblGrid>
  <w:tr><w:trPr/><w:tc><w:tcPr/><w:p><w:pPr><w:pStyle w:val="表头"/></w:pPr><w:r><w:t>参数</w:t></w:r></w:p></w:tc><w:tc><w:p><w:r><w:t>取值</w:t></w:r></w:p></w:tc></w:tr>
  <w:tr><w:tc><w:p><w:r><w:t>timeout</w:t></w:r></w:p><w:p><w:r><w:t>第二段</w:t></w:r></w:p></w:tc><w:tc><w:sdt><w:sdtPr/><w:sdtContent><w:p><w:r><w:t>30</w:t></w:r></w:p></w:sdtContent></w:sdt></w:tc></w:tr>
</w:tbl>
<w:p><w:r><w:t>注意事项</w:t></w:r></w:p>
<w:tbl><w:tr><w:tc><w:p><w:r><w:t>第二张表</w:t></w:r></w:p></w:tc><w:tc><w:p/></w:tc></w:tr>
  <w:tr><w:tc><w:p><w:r><w:t>外层</w:t></w:r></w:p><w:tbl><w:tr><w:tc><w:p><w:r><w:t>嵌套表</w:t></w:r></w:p></w:tc></w:tr></w:tbl></w:tc></w:tr></w:tbl>
<w:p><w:r><w:t>结尾段</w:t></w:r></w:p>
</w:body></w:document>`
	got, err := Parse("docx", docxOf(t, src))
	if err != nil {
		t.Fatal(err)
	}
	want := "# 安装\n\n" +
		"说明段\n\n" +
		"| 参数 | 取值 |\n| timeout 第二段 | 30 |\n\n" +
		"注意事项\n\n" +
		"| 第二张表 |  |\n| 外层 |\n\n" +
		"结尾段\n"
	if got != want {
		t.Fatalf("Parse(docx) =\n%q\nwant\n%q", got, want)
	}
}

// 只靠大纲级别表达层级的段落也是标题（自定义标题样式）；w:outlineLvl 为 9 明确表示正文，不是标题；
// 标题样式优先于大纲级别。
func TestParseDocx_OutlineLevelIsHeading(t *testing.T) {
	t.Parallel()
	src := docxOpen + `
<w:p><w:pPr><w:outlineLvl w:val="0"/></w:pPr><w:r><w:t>大纲一级</w:t></w:r></w:p>
<w:p><w:pPr><w:pStyle w:val="af3"/><w:outlineLvl w:val="2"/></w:pPr><w:r><w:t>大纲三级</w:t></w:r></w:p>
<w:p><w:pPr><w:outlineLvl w:val="5"/></w:pPr><w:r><w:t>大纲六级</w:t></w:r></w:p>
<w:p><w:pPr><w:outlineLvl w:val="8"/></w:pPr><w:r><w:t>大纲九级</w:t></w:r></w:p>
<w:p><w:pPr><w:outlineLvl w:val="9"/></w:pPr><w:r><w:t>正文段落</w:t></w:r></w:p>
<w:p><w:pPr><w:outlineLvl/></w:pPr><w:r><w:t>空大纲级别</w:t></w:r></w:p>
<w:p><w:pPr><w:outlineLvl w:val="x"/></w:pPr><w:r><w:t>非法大纲级别</w:t></w:r></w:p>
<w:p><w:pPr><w:pStyle w:val="Heading3"/><w:outlineLvl w:val="0"/></w:pPr><w:r><w:t>样式优先</w:t></w:r></w:p>
<w:p><w:pPr><w:outlineLvl w:val="1"/></w:pPr><w:r><w:pPr><w:outlineLvl w:val="7"/></w:pPr><w:t>文字</w:t></w:r></w:p>
<w:p><w:pPr><w:pPrChange w:id="9"><w:pPr><w:outlineLvl w:val="3"/></w:pPr></w:pPrChange></w:pPr><w:r><w:t>修订里的旧属性</w:t></w:r></w:p>
<w:p><w:r><w:t>普通段落</w:t></w:r></w:p>
</w:body></w:document>`
	got, err := Parse("docx", docxOf(t, src))
	if err != nil {
		t.Fatal(err)
	}
	want := "# 大纲一级\n\n" +
		"### 大纲三级\n\n" +
		"###### 大纲六级\n\n" +
		"###### 大纲九级\n\n" +
		"正文段落\n\n" +
		"空大纲级别\n\n" +
		"非法大纲级别\n\n" +
		"### 样式优先\n\n" +
		"## 文字\n\n" +
		"修订里的旧属性\n\n" +
		"普通段落\n"
	if got != want {
		t.Fatalf("Parse(docx) =\n%q\nwant\n%q", got, want)
	}
}

// 正文在闭合前结束（半截的 XML）按解析失败处理，不把读到的半截当成完整文档；没有正文与空正文沿用原有报错。
func TestParseDocx_TruncatedBodyFails(t *testing.T) {
	t.Parallel()
	for _, src := range []string{
		docxOpen + `<w:p><w:r><w:t>半截`,
		docxOpen + `<w:tbl><w:tr><w:tc><w:p><w:r><w:t>A</w:t>`,
		docxOpen + `<w:p><w:r><w:t>段落</w:t></w:r>`,
		`<?xml version="1.0"?>` + docxOpen + `<w:p><w:r><w:bad></w:p></w:body></w:document>`,
	} {
		if got, err := Parse("docx", docxOf(t, src)); err == nil || got != "" {
			t.Errorf("Parse(docx, truncated %q) = %q, %v; want an error", src[len(src)-24:], got, err)
		}
	}
	for _, src := range []string{
		`<?xml version="1.0"?><w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"></w:document>`,
		docxOpen + `<w:p><w:r><w:delText>只有删除的修订</w:delText></w:r></w:p><w:sectPr/></w:body></w:document>`,
	} {
		if _, err := Parse("docx", docxOf(t, src)); err == nil || !strings.Contains(err.Error(), "no text content") {
			t.Errorf("Parse(docx, empty body) error = %v, want no text content", err)
		}
	}
}

// 段落与表格外面无论包多少层容器（内容控件、customXml）都按所在位置算正文块：解析对容器嵌套是透明的，
// 只做迭代，不随嵌套深度增加调用栈。
func TestParseDocx_ContainersAreTransparentAtAnyDepth(t *testing.T) {
	t.Parallel()
	const nest = 20000
	var src strings.Builder
	src.WriteString(docxOpen)
	src.WriteString("<w:p><w:r><w:t>第一段</w:t></w:r></w:p>")
	for range nest {
		src.WriteString("<w:customXml><w:customXmlPr/><w:sdt><w:sdtPr><w:id w:val=\"1\"/></w:sdtPr><w:sdtContent>")
	}
	src.WriteString("<w:p><w:r><w:t>被包住的段落</w:t></w:r></w:p>")
	src.WriteString("<w:tbl><w:tr><w:tc><w:p><w:r><w:t>被包住的表格</w:t></w:r></w:p></w:tc></w:tr></w:tbl>")
	for range nest {
		src.WriteString("</w:sdtContent></w:sdt></w:customXml>")
	}
	src.WriteString("<w:p><w:r><w:t>最后一段</w:t></w:r></w:p></w:body></w:document>")
	got, err := Parse("docx", docxOf(t, src.String()))
	if err != nil {
		t.Fatalf("Parse(docx, %d nested containers) error = %v", nest, err)
	}
	if want := "第一段\n\n被包住的段落\n\n| 被包住的表格 |\n\n最后一段\n"; got != want {
		t.Fatalf("Parse(docx) = %q, want %q", got, want)
	}
}
