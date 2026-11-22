// Package zhtext 是知识库全文检索用的轻量分词器：中文（及其它 CJK 文字）按二元组（bigram）切分，
// 拉丁字母 / 数字按连续串切成小写词。不依赖词典与数据库扩展，结果可直接交给 PostgreSQL 的
// 'simple' 文本检索配置（to_tsvector / to_tsquery），索引与查询两侧使用同一函数保证口径一致。
//
// 选 bigram 而非词典分词：Lucene / Elasticsearch 的 CJK 分析器即为 bigram，召回稳定、无 OOV 问题；
// 词典分词（如 gse）需内嵌 20MB+ 词典并常驻数百兆内存，对模块化单体的每个实例都是固定开销。
package zhtext

import (
	"strings"
	"unicode"
)

// maxTokens 单段文本保留的词元上限：防止超长正文生成过大的 tsvector。
const maxTokens = 4096

// Tokenize 把文本切成词元：CJK 连续串 → 相邻两字的二元组（单字串保留单字），其它字母数字串 →
// 小写整词；标点与空白为分隔符。结果去重、保持首次出现顺序。
func Tokenize(text string) []string {
	return tokenize(text, maxTokens)
}

// tokenize 带上限的切分（测试用）。
func tokenize(text string, limit int) []string {
	out := make([]string, 0, 64)
	seen := make(map[string]struct{}, 64)
	add := func(tok string) bool {
		if tok == "" {
			return true
		}
		if _, ok := seen[tok]; ok {
			return true
		}
		if len(out) >= limit {
			return false
		}
		seen[tok] = struct{}{}
		out = append(out, tok)
		return true
	}
	var cjk []rune
	var word strings.Builder
	flushCJK := func() bool {
		defer func() { cjk = cjk[:0] }()
		switch len(cjk) {
		case 0:
			return true
		case 1:
			return add(string(cjk))
		}
		for i := 0; i+1 < len(cjk); i++ {
			if !add(string(cjk[i : i+2])) {
				return false
			}
		}
		return true
	}
	flushWord := func() bool {
		if word.Len() == 0 {
			return true
		}
		tok := strings.ToLower(word.String())
		word.Reset()
		return add(tok)
	}
	for _, r := range text {
		switch {
		case isCJK(r):
			if !flushWord() {
				return out
			}
			cjk = append(cjk, r)
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			if !flushCJK() {
				return out
			}
			word.WriteRune(r)
		default:
			if !flushCJK() || !flushWord() {
				return out
			}
		}
	}
	flushCJK()
	flushWord()
	return out
}

// Document 把正文转成供 to_tsvector('simple', …) 使用的词元串（空格分隔）。
func Document(text string) string {
	return strings.Join(Tokenize(text), " ")
}

// Query 把检索词转成 to_tsquery('simple', …) 的表达式：词元以 OR 连接（由 ts_rank_cd 按命中密度排序），
// 至多 maxQueryTokens 个。没有任何词元返回空串，调用方应跳过全文检索。
func Query(text string) string {
	toks := tokenize(text, maxQueryTokens)
	if len(toks) == 0 {
		return ""
	}
	return strings.Join(toks, " | ")
}

// maxQueryTokens 查询侧词元上限：检索词过长时只取前面的部分。
const maxQueryTokens = 64

// isCJK 报告 r 是否为需要按二元组切分的文字（中日韩统一表意文字、假名、谚文）。
func isCJK(r rune) bool {
	// U+30FC 长音符、U+3005 々 等属 Common 书写系统，但只出现在 CJK 文本里，随前后文字一起切分。
	if r == 0x30FC || r == 0x3005 || r == 0x303B {
		return true
	}
	return unicode.Is(unicode.Han, r) || unicode.Is(unicode.Hiragana, r) || unicode.Is(unicode.Katakana, r) || unicode.Is(unicode.Hangul, r)
}
