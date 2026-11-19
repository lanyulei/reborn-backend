package data

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/lanyulei/reborn/backend/kit/kitconf"
	"github.com/lanyulei/reborn/backend/kit/secretbox"
	"github.com/lanyulei/reborn/backend/monitor/internal/conf"
)

func TestAppendJSONObject(t *testing.T) {
	tests := []struct {
		name string
		in   map[string]string
		want map[string]string // 解码后应得到的内容；nil = 与输入相同
	}{
		{name: "nil", in: nil, want: map[string]string{}},
		{name: "empty", in: map[string]string{}},
		{name: "plain", in: map[string]string{"instance": "10.0.0.1:9100", "job": "node"}},
		{name: "escapes", in: map[string]string{"q": `say "hi" \ bye`, "ws": "a\nb\rc\td", "ctl": "x\x01\x1fy"}},
		{name: "html-ish characters stay literal", in: map[string]string{"expr": "a<b && c>d"}},
		{name: "unicode", in: map[string]string{"名称": "磁盘 满了 ✓", "emoji": "🔥"}},
		{name: "line separators", in: map[string]string{"k": "a b c"}},
		{name: "invalid utf-8 is replaced", in: map[string]string{"k": "a\xffb\xc3"}, want: map[string]string{"k": "a�b�"}},
		{name: "NUL is dropped", in: map[string]string{"k": "a\x00b"}, want: map[string]string{"k": "ab"}},
		{name: "empty value and key", in: map[string]string{"": "", "k": ""}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := appendJSONObject(nil, tt.in)
			if !json.Valid(raw) {
				t.Fatalf("invalid JSON: %s", raw)
			}
			if strings.Contains(string(raw), `\u0000`) || strings.ContainsRune(string(raw), 0) {
				t.Fatalf("output must not contain NUL (jsonb rejects it): %q", raw)
			}
			var got map[string]string
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatal(err)
			}
			want := tt.want
			if want == nil {
				want = tt.in
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("round trip = %q, want %q (raw %s)", got, want, raw)
			}
		})
	}
	// 追加语义：复用缓冲区时不破坏已有内容。
	if got := string(appendJSONObject([]byte("x="), map[string]string{"a": "b"})); got != `x={"a":"b"}` {
		t.Fatalf("append = %s", got)
	}
}

func BenchmarkAppendJSONObject(b *testing.B) {
	labels := map[string]string{"instance": "10.0.12.34:9100", "job": "node", "dc": "bj", "team": "ops"}
	buf := make([]byte, 0, 256)
	b.ReportAllocs()
	for b.Loop() {
		buf = appendJSONObject(buf[:0], labels)
	}
}

func TestSecretBox(t *testing.T) {
	sealer, err := secretbox.New("k1")
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := sealer.Seal("p@ss")
	if err != nil {
		t.Fatal(err)
	}
	box, err := NewSecretBox(&conf.Notify{Master: &kitconf.Master{Key: "k1"}})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := box.Open(sealed); err != nil || got != "p@ss" {
		t.Fatalf("Open() = %q, %v", got, err)
	}
	// 主密钥未配置（含 nil 配置）：明确的哨兵错误，不阻断构造。
	for _, c := range []*conf.Notify{nil, {}, {Master: &kitconf.Master{}}} {
		none, err := NewSecretBox(c)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := none.Open(sealed); !errors.Is(err, ErrMasterKeyUnavailable) {
			t.Fatalf("err = %v, want ErrMasterKeyUnavailable", err)
		}
	}
	// 密钥不匹配：报错且错误信息不含明文。
	wrong, _ := NewSecretBox(&conf.Notify{Master: &kitconf.Master{Key: "k2"}})
	if _, err := wrong.Open(sealed); err == nil || strings.Contains(err.Error(), "p@ss") {
		t.Fatalf("err = %v", err)
	}
}

func TestPartitionName(t *testing.T) {
	for _, name := range []string{"monitor_eval_record_2026_11_17", "monitor_eval_record_2027_01_01"} {
		if !partitionPattern.MatchString(name) {
			t.Errorf("%s must match", name)
		}
	}
	for _, name := range []string{"monitor_eval_record_default", "monitor_eval_record_2026_11", "monitor_eval_record_2026_11_17; DROP TABLE x", "public.monitor_eval_record_2026_11_17x"} {
		if partitionPattern.MatchString(name) {
			t.Errorf("%s must not match", name)
		}
	}
}
