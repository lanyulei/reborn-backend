package biz

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// decodeWithNumbers 以字面形式解码 JSON 的数字（json.Number），用来逐个比对数字的原文。
func decodeWithNumbers(t *testing.T, raw string) map[string]any {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	var v map[string]any
	if err := dec.Decode(&v); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	return v
}

// 命中敏感键后整段要重新编码：数字必须按原文保留。此前经 float64 中转，超过 2^53 的整数（雪花 ID、纳秒
// 时间戳）与 1.000 这类字面量都会被改写。
func TestMaskJSON_PreservesNumberLiterals(t *testing.T) {
	t.Parallel()
	in := json.RawMessage(`{"token":"t","nested":{"id":9007199254740993,"ts_ns":1732435200000000123,"big":18446744073709551615,` +
		`"small":123,"zero":0,"neg":-0,"ratio":1.5,"exp":1e3,"precise":1.000,"huge":1E400}}`)
	out := MaskJSON(in)
	want := `{"nested":{"big":18446744073709551615,"exp":1e3,"huge":1E400,"id":9007199254740993,"neg":-0,"precise":1.000,` +
		`"ratio":1.5,"small":123,"ts_ns":1732435200000000123,"zero":0},"token":"***"}`
	if string(out) != want {
		t.Fatalf("MaskJSON() =\n%s\nwant\n%s", out, want)
	}
}

// 敏感键名下的布尔与计数类数字保持原值（has_secret、prompt_tokens 这类只是字段名沾到敏感片段）；其余数字
// 与字符串值一律打码，包括长得像数字的字符串；空串保持为空；数组 / 对象 / null 仍整体打码。
func TestMaskJSON_KeepsBoolAndCounterNumbersUnderSensitiveKeys(t *testing.T) {
	t.Parallel()
	in := json.RawMessage(`{"has_secret":true,"has_become_secret":false,"prompt_tokens":1234,"agent_tokens":0,"auth_type":2,"tokens_estimated":true,` +
		`"max_tokens":4096,"token_usage_count":3,"passwd":123456,"token":9007199254740993,"access_key":7,` +
		`"password":"p","api_key":"","token_count":"12","secrets":[{"name":"n"}],"password_reset":true,"username":"u",` +
		`"nested":{"has_secret":false,"token":"x","completion_tokens":7},"list":[{"has_secret":true},{"token":"y"}]}`)
	got := decodeWithNumbers(t, string(MaskJSON(in)))
	want := map[string]any{
		"has_secret": true, "has_become_secret": false, "prompt_tokens": json.Number("1234"), "agent_tokens": json.Number("0"),
		// 计数类的数字保留；其余敏感键名下的数字（纯数字口令、令牌、密钥编号）照常打码。
		"max_tokens": json.Number("4096"), "token_usage_count": json.Number("3"),
		"auth_type": json.Number("2"), // 不是敏感键名，本来就不打码
		"passwd":    maskedValue, "token": maskedValue, "access_key": maskedValue,
		"tokens_estimated": true, "password_reset": true,
		"password": maskedValue, "api_key": "", "token_count": maskedValue, "secrets": maskedValue,
		"username": "u",
		"nested":   map[string]any{"has_secret": false, "token": maskedValue, "completion_tokens": json.Number("7")},
		"list":     []any{map[string]any{"has_secret": true}, map[string]any{"token": maskedValue}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("MaskJSON() =\n%s\nwant\n%v", MaskJSON(in), want)
	}
}

// 只接受恰好一个 JSON 值：Decoder 本身不校验顶层值之后的内容，非法输入必须和 json.Unmarshal 一样原样返回。
func TestMaskJSON_OnlyAcceptsExactlyOneJSONValue(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{
		`{"token":"t"} {"a":1}`, `{"token":"t"} x`, `{"token":"t"}{"a":1}`, `{"token":"t"} 1`, `{"token":"t"} ,`,
		`{"token":"t"`, `{"token":"t",}`, `not json`, "{\"token\":\"t\"}\x00", "{\"token\":\"t\"}\xff",
	} {
		if got := MaskJSON(json.RawMessage(raw)); string(got) != raw {
			t.Errorf("MaskJSON(%q) = %q, want it passed through unchanged", raw, got)
		}
	}
	// 无需脱敏时不重新编码：字节原样返回（键序、空白、数字字面量都不动）。
	unchanged := []string{`{ "b" : 1.50 , "a" : [ 2 , 3 ] }`, `{"token":"","n":9007199254740993}`, `"text"`, `123`, `null`, ``, `[1,2]`}
	for _, raw := range unchanged {
		if got := MaskJSON(json.RawMessage(raw)); string(got) != raw {
			t.Errorf("MaskJSON(%q) = %q, want it unchanged", raw, got)
		}
	}
	// 顶层数组里的对象同样递归处理。
	if got := MaskJSON(json.RawMessage(`[{"n":1,"token":"t"},0]`)); string(got) != `[{"n":1,"token":"***"},0]` {
		t.Errorf("MaskJSON(array) = %s", got)
	}
}
