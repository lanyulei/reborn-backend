package biz

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestMaskJSON(t *testing.T) {
	t.Parallel()
	in := json.RawMessage(`{"username":"u","password":"p@ss","nested":{"apiKey":"k","list":[{"token":"t"},{"ok":1}]},"secretNote":""}`)
	out := MaskJSON(in)
	var v map[string]any
	if err := json.Unmarshal(out, &v); err != nil {
		t.Fatalf("masked output not json: %v", err)
	}
	if v["password"] != maskedValue || v["username"] != "u" {
		t.Fatalf("top-level mask wrong: %v", v)
	}
	nested := v["nested"].(map[string]any)
	if nested["apiKey"] != maskedValue {
		t.Fatalf("nested apiKey not masked: %v", nested)
	}
	list := nested["list"].([]any)
	if list[0].(map[string]any)["token"] != maskedValue || list[1].(map[string]any)["ok"] != float64(1) {
		t.Fatalf("list mask wrong: %v", list)
	}
	if v["secretNote"] != "" {
		t.Fatalf("empty sensitive value should stay empty: %v", v["secretNote"])
	}
	if got := MaskJSON(json.RawMessage(`not json`)); string(got) != "not json" {
		t.Fatalf("invalid json should pass through, got %s", got)
	}
}

func TestTruncateOutput(t *testing.T) {
	t.Parallel()
	small := json.RawMessage(`{"a":1}`)
	if got := TruncateOutput(small); string(got) != string(small) {
		t.Fatalf("small output altered: %s", got)
	}
	big := json.RawMessage(`{"items":"` + strings.Repeat("x", maxToolOutputBytes) + `"}`)
	got := TruncateOutput(big)
	if len(got) >= len(big) || !json.Valid(got) || !strings.Contains(string(got), "truncated") {
		t.Fatalf("big output not truncated into valid json: len=%d", len(got))
	}
}

func TestFilterTools(t *testing.T) {
	t.Parallel()
	catalog := ToolCatalog{{Name: "A", Code: "a"}, {Name: "B", Code: "b"}, {Name: "C", Code: "c"}}
	if got := filterTools(catalog, true, nil); len(got) != 3 {
		t.Fatalf("admin should see all tools, got %d", len(got))
	}
	got := filterTools(catalog, false, []string{"c", "zzz"})
	if len(got) != 1 || got[0].Name != "C" {
		t.Fatalf("filter by allowed codes wrong: %+v", got)
	}
	if got := filterTools(catalog, false, nil); len(got) != 0 {
		t.Fatalf("no grants should yield no tools, got %d", len(got))
	}
	if catalog.Find("B") == nil || catalog.Find("X") != nil {
		t.Fatal("ToolCatalog.Find mismatch")
	}
}

func TestMakeTitle(t *testing.T) {
	t.Parallel()
	if got := makeTitle("  帮我查一下用户列表\n第二行"); got != "帮我查一下用户列表" {
		t.Fatalf("title = %q", got)
	}
	long := strings.Repeat("字", maxTitleRunes+5)
	if got := makeTitle(long); got != strings.Repeat("字", maxTitleRunes)+"…" {
		t.Fatalf("long title = %q", got)
	}
}

func TestBuildSystemPrompt(t *testing.T) {
	t.Parallel()
	p := buildSystemPrompt("admin", "管理员", true)
	if !strings.Contains(p, "admin（管理员）") || !strings.Contains(p, "超级管理员") {
		t.Fatalf("prompt missing actor info: %s", p)
	}
	if !strings.HasPrefix(p, "你是管理后台的智能助手") {
		t.Fatalf("stable rules must come first: %s", p)
	}
}
