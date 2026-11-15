package service

import "testing"

// unquote 与前端导入的 unquoteCsvCell 同规则：首个单引号后跟 = + - @ 制表 回车 或单引号时去掉它。
func unquote(s string) string {
	if len(s) >= 2 && s[0] == '\'' {
		switch s[1] {
		case '=', '+', '-', '@', '\t', '\r', '\'':
			return s[1:]
		}
	}
	return s
}

// 导出的防公式前缀必须能被导入端原样还原，包括本身就以单引号开头的值。
func TestCsvSafeRoundTrip(t *testing.T) {
	for _, s := range []string{
		"", "plain", "=1+1", "+x", "-x", "@x", "\tx", "\rx",
		"'", "'x", "'=x", "''=x", "'-x", "'@x", "''", "'''",
	} {
		safe := csvSafe(s)
		if safe != "" {
			switch safe[0] {
			case '=', '+', '-', '@', '\t', '\r':
				t.Errorf("csvSafe(%q) = %q still starts a formula", s, safe)
			}
		}
		if got := unquote(safe); got != s {
			t.Errorf("round trip %q → %q → %q", s, safe, got)
		}
	}
}
