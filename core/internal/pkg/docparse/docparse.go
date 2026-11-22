// Package docparse 把导入文件解析为 Markdown 文本：md / txt 原样，html 去标签保留标题与段落，
// pdf 提取文本（按页分节），docx 读取 word/document.xml 的段落（标题样式转 Markdown 标题）。
// 全部为纯 Go 实现，不调用外部进程；解析上限由调用方以 ctx 超时与输入大小约束。
package docparse

import (
	"archive/zip"
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"path"
	"regexp"
	"strconv"
	"strings"

	"github.com/ledongthuc/pdf"
	"golang.org/x/net/html"
)

// ErrUnsupported 文件类型不支持。
var ErrUnsupported = errors.New("docparse: unsupported file type")

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
	case "markdown", "text":
		return normalizeText(string(data)), nil
	case "html":
		return parseHTML(data)
	case "pdf":
		return parsePDF(data)
	case "docx":
		return parseDocx(data)
	default:
		return "", ErrUnsupported
	}
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

// docx 段落结构（只取需要的节点）。
type docxBody struct {
	Paragraphs []docxParagraph `xml:"body>p"`
	Tables     []struct {
		Rows []struct {
			Cells []struct {
				Paragraphs []docxParagraph `xml:"p"`
			} `xml:"tc"`
		} `xml:"tr"`
	} `xml:"body>tbl"`
}

type docxParagraph struct {
	Style struct {
		Val string `xml:"val,attr"`
	} `xml:"pPr>pStyle"`
	NumPr *struct{} `xml:"pPr>numPr"`
	Runs  []struct {
		Texts []string `xml:"t"`
	} `xml:"r"`
}

func (p docxParagraph) text() string {
	var b strings.Builder
	for _, r := range p.Runs {
		for _, t := range r.Texts {
			b.WriteString(t)
		}
	}
	return strings.TrimSpace(b.String())
}

// headingStyle 匹配 Word 标题样式名（Heading1 / heading 2 / 标题1 / Title）。
var headingStyle = regexp.MustCompile(`(?i)^(?:heading|标题)\s*([1-6])$`)

// parseDocx 解压 docx，解析 word/document.xml：标题样式转 Markdown 标题，列表段落转列表项，表格按行拼接。
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
	var body docxBody
	if err := xml.Unmarshal(raw, &body); err != nil {
		return "", fmt.Errorf("parse docx: %w", err)
	}
	var b strings.Builder
	for _, p := range body.Paragraphs {
		text := p.text()
		if text == "" {
			continue
		}
		if m := headingStyle.FindStringSubmatch(p.Style.Val); m != nil {
			level, _ := strconv.Atoi(m[1])
			b.WriteString(strings.Repeat("#", level) + " " + text + "\n\n")
			continue
		}
		if strings.EqualFold(p.Style.Val, "Title") {
			b.WriteString("# " + text + "\n\n")
			continue
		}
		if p.NumPr != nil {
			b.WriteString("- " + text + "\n")
			continue
		}
		b.WriteString(text + "\n\n")
	}
	for _, tbl := range body.Tables {
		for _, row := range tbl.Rows {
			cells := make([]string, 0, len(row.Cells))
			for _, c := range row.Cells {
				var ct []string
				for _, p := range c.Paragraphs {
					if t := p.text(); t != "" {
						ct = append(ct, t)
					}
				}
				cells = append(cells, strings.Join(ct, " "))
			}
			b.WriteString("| " + strings.Join(cells, " | ") + " |\n")
		}
		b.WriteString("\n")
	}
	if b.Len() == 0 {
		return "", errors.New("parse docx: no text content")
	}
	return normalizeText(b.String()), nil
}
