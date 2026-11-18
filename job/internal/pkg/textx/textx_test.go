package textx

import (
	"strings"
	"testing"
)

func TestClean(t *testing.T) {
	for in, want := range map[string]string{
		"plain":          "plain",
		"nul\x00byte":    "nulbyte",
		"bad\xffutf8":    "bad�utf8",
		"中文\x00\xe4\xb8": "中文�",
		"":               "",
	} {
		if got := Clean(in); got != want {
			t.Errorf("Clean(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestTruncateRunes(t *testing.T) {
	cases := []struct {
		in   string
		n    int
		want string
	}{
		{"hello", 10, "hello"},
		{"hello", 5, "hello"},
		{"hello", 2, "he"},
		{"中文字符", 2, "中文"},
		{"中文字符", 4, "中文字符"},
		{"a中b", 2, "a中"},
		{"x", 0, ""},
	}
	for _, c := range cases {
		if got := TruncateRunes(c.in, c.n); got != c.want {
			t.Errorf("TruncateRunes(%q, %d) = %q, want %q", c.in, c.n, got, c.want)
		}
	}
}

func TestValidLogin(t *testing.T) {
	for _, ok := range []string{"root", "ops", "svc_deploy-1", "DOMAIN\\user", "user@example.com", "运维"} {
		if !ValidLogin(ok) {
			t.Errorf("%q rejected", ok)
		}
	}
	for _, bad := range []string{"", "-oProxyCommand=id", "a b", "a\tb", "a\nb", "a\x00b", "a\x7fb", strings.Repeat("x", 65)} {
		if ValidLogin(bad) {
			t.Errorf("%q accepted", bad)
		}
	}
}
