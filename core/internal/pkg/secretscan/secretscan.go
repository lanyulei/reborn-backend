// Package secretscan 在文本入库前扫描疑似密钥 / 口令：私钥块、云厂商 AK/SK、JWT、带值的口令赋值。
// 只做模式匹配（可能有误报），命中即由调用方拒绝保存并提示位置；不做任何网络校验。
package secretscan

import (
	"regexp"
	"strings"
)

// Finding 一处命中。
type Finding struct {
	// Line 行号（从 1 起）。
	Line int
	// Kind 类型标识（private_key / aws_access_key / aliyun_access_key / jwt / password_assignment / generic_token）。
	Kind string
}

// pattern 一条扫描规则。
type pattern struct {
	kind string
	re   *regexp.Regexp
}

// patterns 规则表：按误报率从低到高排列；同一行命中多条只报第一条。
var patterns = []pattern{
	{"private_key", regexp.MustCompile(`-----BEGIN (?:RSA |EC |DSA |OPENSSH |PGP )?PRIVATE KEY(?: BLOCK)?-----`)},
	{"aws_access_key", regexp.MustCompile(`\b(?:AKIA|ASIA)[0-9A-Z]{16}\b`)},
	{"aliyun_access_key", regexp.MustCompile(`\bLTAI[0-9A-Za-z]{12,24}\b`)},
	{"jwt", regexp.MustCompile(`\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b`)},
	// password / passwd / pwd / secret / token / api_key / access_key = 或 : 后跟 ≥ 8 个非空白字符（允许引号包裹）。
	// 键名前允许任意「下划线 / 连字符」前缀分段：真实配置里最常见的是 db_password、client_secret、
	// access_token 这类写法——此前关键字前要求单词边界，它们全部漏报。
	{"password_assignment", regexp.MustCompile(`(?i)\b(?:[a-z0-9]+[_-])*(?:password|passwd|pwd|secret|secret_key|api[_-]?key|access[_-]?key|token)\b\s*[:=]\s*["']?([^\s"']{8,})["']?`)},
}

// placeholderValues 赋值右侧出现这些片段视为占位符而非真实口令（文档里常见的 `password=<your-password>`）。
var placeholderValues = []string{"<", ">", "${", "{{", "xxx", "***", "your", "example", "changeme", "placeholder", "redacted"}

// Scan 逐行扫描文本，返回全部命中（按行号排序）。
func Scan(text string) []Finding {
	var out []Finding
	for i, line := range strings.Split(text, "\n") {
		for _, p := range patterns {
			m := p.re.FindStringSubmatch(line)
			if m == nil {
				continue
			}
			if p.kind == "password_assignment" && isPlaceholder(m[1]) {
				continue
			}
			out = append(out, Finding{Line: i + 1, Kind: p.kind})
			break
		}
	}
	return out
}

// isPlaceholder 报告赋值右侧是否为明显的占位符。
func isPlaceholder(v string) bool {
	lower := strings.ToLower(v)
	for _, ph := range placeholderValues {
		if strings.Contains(lower, ph) {
			return true
		}
	}
	return false
}
