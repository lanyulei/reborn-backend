// Package textx 收纳引擎与执行节点共用的文本小工具：落库 / 上报前的清洗与截断、登录用户名校验。
// 纯标准库，任何层可用。
package textx

import (
	"strings"
	"unicode"
)

// Clean 清洗要落库的文本：PostgreSQL 的 text 不接受 NUL 与非法 UTF-8。
func Clean(s string) string {
	s = strings.ToValidUTF8(s, "�")
	if strings.IndexByte(s, 0) >= 0 {
		s = strings.ReplaceAll(s, "\x00", "")
	}
	return s
}

// TruncateRunes 把 s 截到至多 n 个字符（列宽按字符计），不切坏 UTF-8。
func TruncateRunes(s string, n int) string {
	if len(s) <= n {
		return s
	}
	count := 0
	for i := range s {
		if count == n {
			return s[:i]
		}
		count++
	}
	return s
}

// ValidLogin 报告登录用户名是否可以安全地进入 Ansible 变量与 SSH 命令行：非空、至多 64 字节、
// 不以 - 开头、不含空白与控制字符。
func ValidLogin(name string) bool {
	if name == "" || len(name) > 64 || strings.HasPrefix(name, "-") {
		return false
	}
	for _, r := range name {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return false
		}
	}
	return true
}
