package dbx

import "testing"

func TestLikeContains(t *testing.T) {
	t.Parallel()
	cases := map[string]string{"ali": "%ali%", "%": `%\%%`, "a_c": `%a\_c%`, `back\slash`: `%back\\slash%`, "": "%%"}
	for in, want := range cases {
		if got := LikeContains(in); got != want {
			t.Errorf("LikeContains(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNilHelpers(t *testing.T) {
	t.Parallel()
	if NilIfEmpty("") != nil || NilIfEmpty("x") != "x" || NilIfEmptyBytes(nil) != nil || len(TextArray(nil)) != 0 || TextArray([]string{"a"})[0] != "a" {
		t.Fatal("nil helpers misbehave")
	}
	if LockID("a") == LockID("b") || LockID("a") != LockID("a"+"") {
		t.Fatal("lock id must be deterministic and distinct")
	}
}
