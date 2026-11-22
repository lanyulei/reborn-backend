package biz

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"unicode"

	"github.com/lanyulei/reborn/backend/core/internal/pkg/zhtext"
)

// 分块参数：目标约 500 token（中文按字、英文按 4 字符估一个 token）。
const (
	// maxChunkChars 单块正文字符上限（含标题路径前缀之外的正文）。
	maxChunkChars = 1200
	// minChunkChars 低于该长度的块尽量与相邻块合并。
	minChunkChars = 200
	// overlapChars 相邻块的重叠：上一块末尾这么多字符作为下一块开头的上下文。
	overlapChars = 100
	// maxHeadingLen 标题路径单段上限。
	maxHeadingLen = 80
	// headingSep 标题路径分隔符。
	headingSep = " > "
)

// block 一个 Markdown 块级单元。
type block struct {
	text string
	// atomic 代码块 / 表格：不拆分。
	atomic bool
}

// section 一个标题下的内容。
type section struct {
	path   string
	blocks []block
}

// parseSections 按标题切分 Markdown：遇到 # 标题开新节（标题路径随层级维护），代码围栏内的 # 不算标题；
// 段落以空行分隔，代码围栏与连续的表格行作为原子块。
func parseSections(md string) []section {
	lines := strings.Split(strings.ReplaceAll(md, "\r\n", "\n"), "\n")
	var out []section
	cur := section{}
	var stack []string // 标题栈，索引 = 层级 - 1
	var para []string
	var fence []string
	inFence := false
	fenceMark := ""
	inTable := false
	var table []string

	flushPara := func() {
		if len(para) == 0 {
			return
		}
		text := strings.TrimSpace(strings.Join(para, "\n"))
		para = para[:0]
		if text != "" {
			cur.blocks = append(cur.blocks, block{text: text})
		}
	}
	flushTable := func() {
		if len(table) == 0 {
			return
		}
		cur.blocks = append(cur.blocks, block{text: strings.Join(table, "\n"), atomic: true})
		table = table[:0]
		inTable = false
	}
	closeSection := func() {
		flushPara()
		flushTable()
		if len(cur.blocks) > 0 || cur.path != "" {
			out = append(out, cur)
		}
	}
	for _, raw := range lines {
		line := strings.TrimRight(raw, " \t")
		trimmed := strings.TrimSpace(line)
		if inFence {
			fence = append(fence, line)
			if strings.HasPrefix(trimmed, fenceMark) {
				cur.blocks = append(cur.blocks, block{text: strings.Join(fence, "\n"), atomic: true})
				fence = fence[:0]
				inFence = false
			}
			continue
		}
		if strings.HasPrefix(trimmed, "```") || strings.HasPrefix(trimmed, "~~~") {
			flushPara()
			flushTable()
			inFence = true
			fenceMark = trimmed[:3]
			fence = append(fence, line)
			continue
		}
		if level, title, ok := headingOf(trimmed); ok {
			closeSection()
			if level > len(stack) {
				for len(stack) < level-1 {
					stack = append(stack, "")
				}
				stack = append(stack, title)
			} else {
				stack = append(stack[:level-1], title)
			}
			cur = section{path: joinPath(stack)}
			continue
		}
		if strings.HasPrefix(trimmed, "|") {
			flushPara()
			inTable = true
			table = append(table, line)
			continue
		}
		if inTable {
			flushTable()
		}
		if trimmed == "" {
			flushPara()
			continue
		}
		para = append(para, line)
	}
	if inFence && len(fence) > 0 {
		cur.blocks = append(cur.blocks, block{text: strings.Join(fence, "\n"), atomic: true})
	}
	closeSection()
	return out
}

// headingOf 解析 ATX 标题（# 到 ######，后随空格）。
func headingOf(line string) (level int, title string, ok bool) {
	if !strings.HasPrefix(line, "#") {
		return 0, "", false
	}
	i := 0
	for i < len(line) && line[i] == '#' {
		i++
	}
	if i == 0 || i > 6 || i >= len(line) || line[i] != ' ' {
		return 0, "", false
	}
	title = strings.TrimSpace(strings.TrimRight(strings.TrimSpace(line[i:]), "#"))
	if title == "" {
		return 0, "", false
	}
	if r := []rune(title); len(r) > maxHeadingLen {
		title = string(r[:maxHeadingLen])
	}
	return i, title, true
}

// joinPath 把标题栈拼成标题路径（跳过层级空洞）。
func joinPath(stack []string) string {
	parts := make([]string, 0, len(stack))
	for _, s := range stack {
		if s != "" {
			parts = append(parts, s)
		}
	}
	return strings.Join(parts, headingSep)
}

// Headings 返回文档全部标题路径（按出现顺序，去重）。
func Headings(md string) []string {
	var out []string
	seen := map[string]struct{}{}
	for _, s := range parseSections(md) {
		if s.path == "" {
			continue
		}
		if _, ok := seen[s.path]; ok {
			continue
		}
		seen[s.path] = struct{}{}
		out = append(out, s.path)
	}
	return out
}

// Section 返回某标题路径下的一节原文（含其全部下级标题的内容），用于按节取正文。
// 匹配不区分大小写与首尾空白；未命中返回 false。
func Section(md, headingPath string) (string, bool) {
	want := normalizePath(headingPath)
	if want == "" {
		return "", false
	}
	var b strings.Builder
	found := false
	for _, s := range parseSections(md) {
		p := normalizePath(s.path)
		if p == want || strings.HasPrefix(p, want+normalizePath(headingSep)) {
			found = true
			if s.path != "" {
				b.WriteString("## " + lastSegment(s.path) + "\n\n")
			}
			for _, blk := range s.blocks {
				b.WriteString(blk.text + "\n\n")
			}
		}
	}
	return strings.TrimSpace(b.String()), found
}

func normalizePath(p string) string {
	parts := strings.Split(p, ">")
	for i := range parts {
		parts[i] = strings.ToLower(strings.TrimSpace(parts[i]))
	}
	return strings.Join(parts, ">")
}

func lastSegment(p string) string {
	if i := strings.LastIndex(p, headingSep); i >= 0 {
		return p[i+len(headingSep):]
	}
	return p
}

// Split 把 Markdown 切成分块：按节 → 节内按块累积到 maxChunkChars；超长的非原子块按句子 / 长度再切；
// 相邻块带 overlapChars 重叠。每块的检索词元带文档标题与标题路径前缀（脱离上下文也可被命中），
// 正文本身不含前缀。
func Split(title, md string) []*Chunk {
	var out []*Chunk
	seq := int32(0)
	title = strings.TrimSpace(title)
	// emit 落一块：正文不含重叠；检索词元含标题、标题路径与上一块末尾的重叠文本（召回用）。
	emit := func(path, text, overlap string) {
		text = strings.TrimSpace(text)
		if text == "" {
			return
		}
		seq++
		indexed := indexedText(title, path, text)
		sum := sha256.Sum256([]byte(indexed))
		tokensText := indexed
		if overlap != "" {
			tokensText = indexedText(title, path, overlap+"\n"+text)
		}
		out = append(out, &Chunk{
			Seq:         seq,
			HeadingPath: path,
			Content:     text,
			Tokens:      zhtext.Document(tokensText),
			SHA256:      hex.EncodeToString(sum[:]),
			TokenCount:  estimateTokens(indexed),
		})
	}
	for _, s := range parseSections(md) {
		var buf strings.Builder
		var tail string // 上一块末尾（重叠，只进词元）
		flush := func() {
			if buf.Len() == 0 {
				return
			}
			text := buf.String()
			emit(s.path, text, tail)
			tail = overlapTail(text)
			buf.Reset()
		}
		for _, blk := range s.blocks {
			pieces := []string{blk.text}
			if !blk.atomic && runeLen(blk.text) > maxChunkChars {
				pieces = splitLong(blk.text, maxChunkChars)
			}
			for _, piece := range pieces {
				if buf.Len() > 0 && runeLen(buf.String())+runeLen(piece)+2 > maxChunkChars {
					flush()
				}
				if buf.Len() > 0 {
					buf.WriteString("\n\n")
				}
				buf.WriteString(piece)
				if blk.atomic && runeLen(piece) >= maxChunkChars {
					flush()
				}
			}
		}
		flush()
	}
	return mergeTiny(title, out)
}

// indexedText 参与分词 / 向量化的文本：标题 + 标题路径 + 正文。
func indexedText(title, path, text string) string {
	var b strings.Builder
	if title != "" {
		b.WriteString(title + "\n")
	}
	if path != "" {
		b.WriteString(path + "\n")
	}
	b.WriteString(text)
	return b.String()
}

// mergeTiny 把过短的块并入前一块（同一标题路径下），避免大量碎块。
func mergeTiny(title string, chunks []*Chunk) []*Chunk {
	if len(chunks) < 2 {
		return chunks
	}
	out := make([]*Chunk, 0, len(chunks))
	for _, c := range chunks {
		if n := len(out); n > 0 && out[n-1].HeadingPath == c.HeadingPath &&
			runeLen(c.Content) < minChunkChars && runeLen(out[n-1].Content)+runeLen(c.Content) <= maxChunkChars+minChunkChars {
			prev := out[n-1]
			merged := prev.Content + "\n\n" + c.Content
			indexed := indexedText(title, prev.HeadingPath, merged)
			sum := sha256.Sum256([]byte(indexed))
			prev.Content = merged
			prev.Tokens = zhtext.Document(indexed)
			prev.SHA256 = hex.EncodeToString(sum[:])
			prev.TokenCount = estimateTokens(indexed)
			continue
		}
		out = append(out, c)
	}
	for i, c := range out {
		c.Seq = int32(i + 1)
	}
	return out
}

// overlapTail 取文本末尾约 overlapChars 字符（从句子边界起）。
func overlapTail(text string) string {
	r := []rune(text)
	if len(r) <= overlapChars {
		return ""
	}
	tail := r[len(r)-overlapChars:]
	for i, ch := range tail {
		if isSentenceEnd(ch) && i+1 < len(tail) {
			return strings.TrimSpace(string(tail[i+1:]))
		}
	}
	return strings.TrimSpace(string(tail))
}

// splitLong 把超长段落按句子边界切成不超过 limit 字符的片段；没有句子边界时硬切。
func splitLong(text string, limit int) []string {
	r := []rune(text)
	var out []string
	start := 0
	for start < len(r) {
		end := min(start+limit, len(r))
		if end < len(r) {
			cut := -1
			for i := end - 1; i > start+limit/2; i-- {
				if isSentenceEnd(r[i]) {
					cut = i + 1
					break
				}
			}
			if cut > 0 {
				end = cut
			}
		}
		piece := strings.TrimSpace(string(r[start:end]))
		if piece != "" {
			out = append(out, piece)
		}
		start = end
	}
	return out
}

func isSentenceEnd(ch rune) bool {
	switch ch {
	case '。', '！', '？', '；', '.', '!', '?', ';', '\n':
		return true
	}
	return false
}

func runeLen(s string) int { return len([]rune(s)) }

// estimateTokens 粗估 token 数：CJK 每字 1，其它每 4 字符 1。
func estimateTokens(s string) int32 {
	cjk, other := 0, 0
	for _, r := range s {
		if unicode.Is(unicode.Han, r) || unicode.Is(unicode.Hiragana, r) || unicode.Is(unicode.Katakana, r) || unicode.Is(unicode.Hangul, r) {
			cjk++
		} else if !unicode.IsSpace(r) {
			other++
		}
	}
	return int32(cjk + (other+3)/4)
}
