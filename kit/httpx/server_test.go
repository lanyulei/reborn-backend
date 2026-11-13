package httpx

import (
	"log/slog"
	"testing"
)

func TestMaskSensitiveAttr(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		groups []string
		attr   slog.Attr
		want   string
	}{
		{"login password", nil, slog.String("args", `username:"admin" password:"Admin123!"`), `username:"admin" password:"***"`},
		{"refresh token", nil, slog.String("args", `refresh_token:"a79a"`), `refresh_token:"***"`},
		{"others kept", nil, slog.String("args", `username:"alice" password:"p" nickname:"Alice" gender:1`), `username:"alice" password:"***" nickname:"Alice" gender:1`},
		{"case insensitive", nil, slog.String("args", `Password:"x"`), `Password:"***"`},
		{"sign_secret + signature", nil, slog.String("args", `sign_secret:"s" signature:"sig" token:"npt_x"`), `sign_secret:"***" signature:"***" token:"***"`},
		{"webhook header value", nil, slog.String("args", `headers:{key:"Authorization" value:"Bearer abc"} headers:{key:"X-Trace" value:"t"}`), `headers:{key:"Authorization" value:"***"} headers:{key:"X-Trace" value:"t"}`},
		{"escaped quote in password", nil, slog.String("args", `username:"a" password:"ab\"cd\\\"ef" nickname:"n"`), `username:"a" password:"***" nickname:"n"`},
		{"escaped quote in header value", nil, slog.String("args", `headers:{key:"Cookie" value:"a=\"b\"; c"}`), `headers:{key:"Cookie" value:"***"}`},
		{"no sensitive", nil, slog.String("args", `page:1 keyword:"ali"`), `page:1 keyword:"ali"`},
		{"nested group untouched", []string{"inner"}, slog.String("args", `password:"leak"`), `password:"leak"`},
		{"other keys untouched", nil, slog.String("password", "by-key"), "by-key"},
	}
	for _, tc := range cases {
		if got := MaskSensitiveAttr(tc.groups, tc.attr); got.Value.String() != tc.want {
			t.Errorf("%s: %q, want %q", tc.name, got.Value.String(), tc.want)
		}
	}
}
