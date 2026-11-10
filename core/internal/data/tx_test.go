package data

import "testing"

// TestLikeContains LIKE 元字符按字面转义，普通关键字两端加 %。
func TestLikeContains(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"ali":        "%ali%",
		"%":          `%\%%`,
		"a_c":        `%a\_c%`,
		`back\slash`: `%back\\slash%`,
		"":           "%%",
	}
	for in, want := range cases {
		if got := LikeContains(in); got != want {
			t.Errorf("LikeContains(%q) = %q, want %q", in, got, want)
		}
	}
}
