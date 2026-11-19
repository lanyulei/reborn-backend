package roles

import (
	"slices"
	"testing"
)

func TestResolve(t *testing.T) {
	cases := []struct {
		name     string
		env      string
		conf     []string
		want     []string
		explicit bool
		wantErr  bool
	}{
		{name: "empty means all", want: []string{"a", "b"}},
		{name: "conf", conf: []string{" B ", ""}, want: []string{"b"}, explicit: true},
		{name: "env overrides conf", env: "a", conf: []string{"b"}, want: []string{"a"}, explicit: true},
		{name: "blank env falls back to conf", env: "  ", conf: []string{"b"}, want: []string{"b"}, explicit: true},
		{name: "only separators means all", env: " , ", want: []string{"a", "b"}},
		{name: "unknown in conf", conf: []string{"a", "bogus"}, wantErr: true},
		{name: "unknown in env", env: "sendr", wantErr: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv(Env, c.env)
			set, explicit, err := Resolve(c.conf, "a", "b")
			if c.wantErr {
				if err == nil {
					t.Fatalf("Resolve() = %v, want an error", set)
				}
				return
			}
			if err != nil {
				t.Fatalf("Resolve() error = %v", err)
			}
			if got := set.List("a", "b"); !slices.Equal(got, c.want) || explicit != c.explicit {
				t.Fatalf("Resolve() = %v explicit=%v, want %v explicit=%v", got, explicit, c.want, c.explicit)
			}
		})
	}
}
