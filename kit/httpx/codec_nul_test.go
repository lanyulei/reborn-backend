package httpx

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"
)

// hasNULEscape 只认真正起转义作用的 \u0000：偶数个反斜杠后的才是转义，`"\\u0000"` 是字面文本。
func TestHasNULEscape(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		json string
		want bool
	}{
		{"plain", `{"a":"b"}`, false},
		{"escaped newline", `{"a":"x\ny"}`, false},
		{"other escape", `{"a":"\u0001"}`, false},
		{"nul escape", `{"a":"\u0000"}`, true},
		{"nul escape inside text", `{"a":"x\u0000y"}`, true},
		{"nul escape in key", `{"k\u0000":1}`, true},
		{"three backslashes (escaped backslash + nul escape)", `{"a":"\\\u0000"}`, true},
		{"two backslashes: literal text", `{"a":"\\u0000"}`, false},
		{"four backslashes: literal text", `{"a":"\\\\u0000"}`, false},
		{"literal text then a real escape", `{"a":"\\u0000 \u0000"}`, true},
		{"escaped backslash before an ordinary quote", `{"a":"\"}`, false},
	}
	for _, c := range cases {
		if got := hasNULEscape([]byte(c.json)); got != c.want {
			t.Errorf("%s (%s): hasNULEscape() = %v, want %v", c.name, c.json, got, c.want)
		}
	}
}

// NUL 字符在数据库里存不下（落库报 22021 → 500）：在请求解码处统一按 400 拒绝；字面文本 "\u0000" 不受影响。
func TestDecodeRequest_RejectsNUL(t *testing.T) {
	t.Parallel()
	decode := func(body string) error {
		r := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		return DecodeRequest(r, &structpb.Struct{})
	}
	if err := decode(`{"name":"a` + `\u0000` + `b"}`); errorCode(err) != 400 {
		t.Fatalf("DecodeRequest(nul) error = %v, want 400", err)
	}
	if err := decode(`{"cmd":"printf '\\u0000'"}`); err != nil {
		t.Fatalf("DecodeRequest(literal backslash-u0000 text) error = %v, want nil", err)
	}
	if err := decode(`{"ok":true}`); err != nil {
		t.Fatalf("DecodeRequest(plain) error = %v, want nil", err)
	}
}
