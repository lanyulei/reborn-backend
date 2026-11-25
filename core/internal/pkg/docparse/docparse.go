// Package docparse 把导入文件解析为 Markdown 文本：md / txt 原样，html 去标签保留标题与段落，
// pdf 提取文本（按页分节），docx 读取 word/document.xml 的段落（标题样式转 Markdown 标题）。
// 全部为纯 Go 实现，不调用外部进程；解析上限由调用方以 ctx 超时与输入大小约束。
package docparse

import (
	"archive/zip"
	"bytes"
	"encoding/binary"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"path"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/ledongthuc/pdf"
	"golang.org/x/net/html"
)

// ErrUnsupported 文件类型不支持。
var ErrUnsupported = errors.New("docparse: unsupported file type")

// ErrEncoding 文本类文件（md / txt / html）不是 UTF-8：只识别 UTF-8 与带 BOM 的 UTF-16，GBK 等其它编码没有
// 可靠的判别手段。不报错的话非法字节会原样进入正文，直到写库才被数据库拒绝。
var ErrEncoding = errors.New("文件不是 UTF-8 编码，请转存为 UTF-8（或带 BOM 的 UTF-16）后重新导入")

// Kind 按文件名与嗅探到的 MIME 判定解析类型；无法判定返回空串。
func Kind(filename, mime string) string {
	ext := strings.ToLower(path.Ext(filename))
	switch ext {
	case ".md", ".markdown":
		return "markdown"
	case ".txt", ".log":
		return "text"
	case ".html", ".htm":
		return "html"
	case ".pdf":
		return "pdf"
	case ".docx":
		return "docx"
	}
	switch mime {
	case "text/markdown":
		return "markdown"
	case "text/plain":
		return "text"
	case "text/html":
		return "html"
	case "application/pdf":
		return "pdf"
	}
	return ""
}

// Parse 解析为 Markdown；kind 为 Kind 的返回值。
func Parse(kind string, data []byte) (string, error) {
	switch kind {
	case "markdown", "text", "html":
		text, err := decodeText(data)
		if err != nil {
			return "", err
		}
		if kind == "html" {
			return parseHTML(text)
		}
		return normalizeText(string(text)), nil
	case "pdf":
		return parsePDF(data)
	case "docx":
		return parseDocx(data)
	default:
		return "", ErrUnsupported
	}
}

// utf16CodeUnit 一个 UTF-16 码元的字节数。
const utf16CodeUnit = 2

// 文本类文件识别的 BOM：UTF-16LE / UTF-16BE 各 2 字节。
var (
	bomUTF16BE = []byte{0xFE, 0xFF}
	bomUTF16LE = []byte{0xFF, 0xFE}
	bomUTF32LE = []byte{0xFF, 0xFE, 0x00, 0x00}
)

// decodeText 把文本类文件（markdown / text / html）的字节规整为 UTF-8：本身是合法 UTF-8 的原样返回；
// 以 BOM 开头的 UTF-16（LE / BE）转成 UTF-8。其余编码（GBK 等）返回 ErrEncoding。
func decodeText(data []byte) ([]byte, error) {
	// 先判 UTF-8：不带 BOM 的纯 ASCII UTF-16 也是合法 UTF-8，沿用原有处理（NUL 由 normalizeText 去掉）。
	if utf8.Valid(data) {
		return data, nil
	}
	var order binary.ByteOrder
	switch {
	case bytes.HasPrefix(data, bomUTF16BE):
		order = binary.BigEndian
	case bytes.HasPrefix(data, bomUTF16LE) && !bytes.HasPrefix(data, bomUTF32LE):
		// UTF-32LE 的 BOM 以 UTF-16LE 的 BOM 开头：按 UTF-16 解会得到错字而不报错，所以不认。
		order = binary.LittleEndian
	default:
		return nil, ErrEncoding
	}
	body := data[len(bomUTF16BE):] // 两种 BOM 等长
	units := make([]uint16, len(body)/utf16CodeUnit)
	for i := range units {
		units[i] = order.Uint16(body[i*utf16CodeUnit:])
	}
	return []byte(string(utf16.Decode(units))), nil
}

// normalizeText 统一换行、去掉 BOM 与 NUL。
func normalizeText(s string) string {
	s = strings.TrimPrefix(s, "\uFEFF")
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")
	s = strings.ReplaceAll(s, "\x00", "")
	return strings.TrimSpace(s) + "\n"
}

// parseHTML 遍历 DOM：h1-h6 → Markdown 标题，li → 列表项，pre/code → 代码块，其余块级元素 → 段落；
// script / style / 导航等不进正文。
func parseHTML(data []byte) (string, error) {
	doc, err := html.Parse(bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("parse html: %w", err)
	}
	var b strings.Builder
	var walk func(n *html.Node)
	writeBlock := func(prefix, text string) {
		text = strings.Join(strings.Fields(text), " ")
		if text == "" {
			return
		}
		b.WriteString(prefix + text + "\n\n")
	}
	textOf := func(n *html.Node) string {
		var t strings.Builder
		var f func(*html.Node)
		f = func(c *html.Node) {
			if c.Type == html.TextNode {
				t.WriteString(c.Data)
			}
			for cc := c.FirstChild; cc != nil; cc = cc.NextSibling {
				f(cc)
			}
		}
		f(n)
		return t.String()
	}
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode {
			switch n.Data {
			case "script", "style", "nav", "header", "footer", "noscript", "iframe", "svg":
				return
			case "h1", "h2", "h3", "h4", "h5", "h6":
				level := int(n.Data[1] - '0')
				writeBlock(strings.Repeat("#", level)+" ", textOf(n))
				return
			case "li":
				writeBlock("- ", textOf(n))
				return
			case "pre":
				code := strings.Trim(textOf(n), "\n")
				if code != "" {
					b.WriteString("```\n" + code + "\n```\n\n")
				}
				return
			case "p", "td", "th", "blockquote", "dd", "dt", "figcaption":
				writeBlock("", textOf(n))
				return
			case "br":
				b.WriteString("\n")
			}
		}
		if n.Type == html.TextNode && n.Parent != nil && (n.Parent.Data == "body" || n.Parent.Data == "div" || n.Parent.Data == "span" || n.Parent.Data == "section" || n.Parent.Data == "article") {
			writeBlock("", n.Data)
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(doc)
	return normalizeText(b.String()), nil
}

// parsePDF 逐页提取文本，每页一个二级标题（便于按页定位与分块）。
func parsePDF(data []byte) (out string, err error) {
	defer func() {
		// 底层库对损坏文件可能 panic：转为错误。
		if r := recover(); r != nil {
			err = fmt.Errorf("parse pdf: %v", r)
		}
	}()
	r, err := pdf.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return "", fmt.Errorf("parse pdf: %w", err)
	}
	var b strings.Builder
	pages := r.NumPage()
	for i := 1; i <= pages; i++ {
		p := r.Page(i)
		if p.V.IsNull() {
			continue
		}
		text, err := p.GetPlainText(nil)
		if err != nil {
			return "", fmt.Errorf("parse pdf page %d: %w", i, err)
		}
		text = strings.TrimSpace(text)
		if text == "" {
			continue
		}
		fmt.Fprintf(&b, "## 第 %d 页\n\n%s\n\n", i, text)
	}
	if b.Len() == 0 {
		return "", errors.New("parse pdf: no extractable text (scanned document?)")
	}
	return normalizeText(b.String()), nil
}

// docxParagraph 段落：文字与段落属性。
type docxParagraph struct {
	// Style 段落样式 ID（w:pPr/w:pStyle 的 w:val）。
	Style string
	// Numbered 段落带编号或项目符号（w:pPr/w:numPr）。
	Numbered bool
	// OutlineLevel 大纲级别（w:pPr/w:outlineLvl 的 w:val，0–8）；未设置或为正文（docxOutlineBody）时为 -1。
	OutlineLevel int
	texts        strings.Builder
}

// text 段落文字：按文档顺序拼接全部 w:t。
func (p *docxParagraph) text() string { return strings.TrimSpace(p.texts.String()) }

// docxTable 表格：每行各单元格的文字（单元格内多个段落以空格连接）。
type docxTable struct {
	Rows [][]string
}

// docxBlock 正文块：段落或表格，二者取一。
type docxBlock struct {
	para *docxParagraph
	tbl  *docxTable
}

// docxOutlineBody w:outlineLvl 的「正文」取值：明确表示不属于任何大纲级别，不是标题。
const docxOutlineBody = 9

// headingStyle 匹配 Word 标题样式名（Heading1 / heading 2 / 标题1 / Title）。
var headingStyle = regexp.MustCompile(`(?i)^(?:heading|标题)\s*([1-6])$`)

// headingLevel 段落作为标题的级别（1–6）；不是标题返回 false。依次看标题样式、Title 样式、大纲级别。
func (p *docxParagraph) headingLevel() (int, bool) {
	if m := headingStyle.FindStringSubmatch(p.Style); m != nil {
		level, _ := strconv.Atoi(m[1])
		return level, true
	}
	if strings.EqualFold(p.Style, "Title") {
		return 1, true
	}
	// 只靠大纲级别表达层级的段落（自定义标题样式）。Markdown 标题最多 6 级，更深的大纲级别按 6 级计。
	if p.OutlineLevel >= 0 {
		return min(p.OutlineLevel+1, 6), true
	}
	return 0, false
}

// docxReader 基于 token 读取 word/document.xml。段落与表格是正文的有序序列，按结构体标签解不出来
// （encoding/xml 只匹配直接子元素，且把两类元素分到各自的切片、丢掉先后关系）。
type docxReader struct {
	dec *xml.Decoder
}

// next 读下一个 token；正文在容器闭合前结束（半截的 XML）按解析失败处理，而不是把读到的半截当成完整正文。
func (d *docxReader) next() (xml.Token, error) {
	tok, err := d.dec.Token()
	if errors.Is(err, io.EOF) {
		return nil, errors.New("document.xml ended before the body was closed")
	}
	return tok, err
}

// readBlocks 读取正文（w:body）直到其结束，按出现顺序返回其中的段落与表格。段落与表格无论嵌在哪一层
// （内容控件 w:sdt/w:sdtContent、customXml 等容器里）都按所在位置算作正文块；容器的属性等其它内容跳过。
// 全程只做迭代、不递归：嵌套层数由输入决定，document.xml 解压后可达 64 MiB。
func (d *docxReader) readBlocks() ([]docxBlock, error) {
	var blocks []docxBlock
	depth := 0 // 当前打开的容器层数；回到 -1 即正文结束
	for depth >= 0 {
		tok, err := d.next()
		if err != nil {
			return nil, err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			switch t.Name.Local {
			case "p":
				p, err := d.readParagraph()
				if err != nil {
					return nil, err
				}
				blocks = append(blocks, docxBlock{para: p})
			case "tbl":
				tbl, err := d.readTable()
				if err != nil {
					return nil, err
				}
				blocks = append(blocks, docxBlock{tbl: tbl})
			default:
				depth++
			}
		case xml.EndElement:
			depth--
		}
	}
	return blocks, nil
}

// readParagraph 读取段落（起始标签已消费）直到其结束。文字按文档顺序收集段落里全部 w:t：只取直接子 run
// 会漏掉嵌在超链接、修订插入、智能标记、内容控件里的文字。删除的修订（w:delText）与域代码（w:instrText）
// 不是 w:t，仍不进正文。
func (d *docxReader) readParagraph() (*docxParagraph, error) {
	p := &docxParagraph{OutlineLevel: -1}
	depth, inProps, inText := 0, false, false // 段落内嵌套深度；正在读段落属性；正在读 w:t 的文字
	for {
		tok, err := d.next()
		if err != nil {
			return nil, err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			depth++
			inText = t.Name.Local == "t" && !inProps
			switch {
			case depth == 1 && t.Name.Local == "pPr":
				inProps = true
			case inProps && depth == 2:
				switch t.Name.Local {
				case "pStyle":
					p.Style = xmlAttr(t, "val")
				case "numPr":
					p.Numbered = true
				case "outlineLvl":
					p.OutlineLevel = docxOutlineLevel(xmlAttr(t, "val"))
				}
			}
		case xml.CharData:
			if inText {
				p.texts.Write(t)
			}
		case xml.EndElement:
			if depth == 1 && inProps {
				inProps = false
			}
			depth--
			inText = false
			if depth < 0 {
				return p, nil
			}
		}
	}
}

// readTable 读取表格（起始标签已消费）直到其结束：行（w:tr）→ 单元格（w:tc）→ 段落。单元格里的段落即使嵌在
// 内容控件里也按出现顺序收集；单元格里的嵌套表格不展开（Markdown 表格无法嵌套）。
func (d *docxReader) readTable() (*docxTable, error) {
	tbl := &docxTable{}
	tables, cells := 1, 0 // 当前打开的表格 / 单元格层数
	var row, cell []string
	for tables > 0 {
		tok, err := d.next()
		if err != nil {
			return nil, err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			switch t.Name.Local {
			case "tbl":
				tables++
			case "tc":
				cells++
				if cells == 1 {
					cell = nil
				}
			case "p":
				p, err := d.readParagraph()
				if err != nil {
					return nil, err
				}
				if tables == 1 && cells == 1 {
					if text := p.text(); text != "" {
						cell = append(cell, text)
					}
				}
			}
		case xml.EndElement:
			switch t.Name.Local {
			case "tbl":
				tables--
			case "tc":
				cells--
				if cells == 0 {
					row = append(row, strings.Join(cell, " "))
				}
			case "tr":
				if tables == 1 && cells == 0 {
					tbl.Rows = append(tbl.Rows, row)
					row = nil
				}
			}
		}
	}
	return tbl, nil
}

// xmlAttr 取元素上指定名称的属性值（不区分命名空间）；没有返回空串。
func xmlAttr(e xml.StartElement, name string) string {
	for _, a := range e.Attr {
		if a.Name.Local == name {
			return a.Value
		}
	}
	return ""
}

// docxOutlineLevel 解析 w:outlineLvl 的 w:val：0–8 是大纲级别；为空、无法解析或为正文（docxOutlineBody）时返回 -1。
func docxOutlineLevel(val string) int {
	n, err := strconv.Atoi(strings.TrimSpace(val))
	if err != nil || n < 0 || n >= docxOutlineBody {
		return -1
	}
	return n
}

// writeDocxTable 表格按行写成 Markdown 表格（每行各单元格以 | 分隔）。
func writeDocxTable(b *strings.Builder, tbl *docxTable) {
	for _, row := range tbl.Rows {
		b.WriteString("| " + strings.Join(row, " | ") + " |\n")
	}
	b.WriteString("\n")
}

// parseDocx 解压 docx，解析 word/document.xml：标题样式转 Markdown 标题，列表段落转列表项，表格按行拼接；
// 段落与表格按正文里的出现顺序输出。
func parseDocx(data []byte) (string, error) {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return "", fmt.Errorf("parse docx: %w", err)
	}
	var doc *zip.File
	for _, f := range zr.File {
		if f.Name == "word/document.xml" {
			doc = f
			break
		}
	}
	if doc == nil {
		return "", errors.New("parse docx: word/document.xml not found")
	}
	rc, err := doc.Open()
	if err != nil {
		return "", fmt.Errorf("parse docx: %w", err)
	}
	defer func() { _ = rc.Close() }()
	// 解压炸弹防御：正文 XML 不超过 64 MiB。
	raw, err := io.ReadAll(io.LimitReader(rc, 64<<20+1))
	if err != nil {
		return "", fmt.Errorf("parse docx: %w", err)
	}
	if len(raw) > 64<<20 {
		return "", errors.New("parse docx: document.xml too large")
	}
	blocks, err := readDocxBody(raw)
	if err != nil {
		return "", fmt.Errorf("parse docx: %w", err)
	}
	var b strings.Builder
	for _, blk := range blocks {
		if blk.tbl != nil {
			writeDocxTable(&b, blk.tbl)
			continue
		}
		text := blk.para.text()
		if text == "" {
			continue
		}
		if level, ok := blk.para.headingLevel(); ok {
			b.WriteString(strings.Repeat("#", level) + " " + text + "\n\n")
			continue
		}
		if blk.para.Numbered {
			b.WriteString("- " + text + "\n")
			continue
		}
		b.WriteString(text + "\n\n")
	}
	if b.Len() == 0 {
		return "", errors.New("parse docx: no text content")
	}
	return normalizeText(b.String()), nil
}

// readDocxBody 读取 word/document.xml 的正文块（段落与表格），按出现顺序返回；没有正文返回空。
func readDocxBody(raw []byte) ([]docxBlock, error) {
	d := &docxReader{dec: xml.NewDecoder(bytes.NewReader(raw))}
	for {
		tok, err := d.dec.Token()
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		if start, ok := tok.(xml.StartElement); ok && start.Name.Local == "body" {
			return d.readBlocks()
		}
	}
}
