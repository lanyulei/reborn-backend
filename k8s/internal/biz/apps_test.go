package biz

import (
	"reflect"
	"testing"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

func TestMaskValues(t *testing.T) {
	t.Parallel()
	in := map[string]any{
		"image":    map[string]any{"tag": "1.0", "pullSecret": "abc"},
		"password": "p@ss",
		"auth":     map[string]any{"adminPassword": "x", "user": "root", "API_KEY": "k", "empty_token": ""},
		"env":      []any{map[string]any{"name": "A", "value": "1"}, map[string]any{"dbPasswd": "y"}},
		"replicas": 2,
		"tokens":   map[string]any{"a": "b"},
	}
	got := MaskValues(in)
	m := k8scontract.MaskedValue
	want := map[string]any{
		"image":    map[string]any{"tag": "1.0", "pullSecret": m},
		"password": m,
		"auth":     map[string]any{"adminPassword": m, "user": "root", "API_KEY": m, "empty_token": ""},
		"env":      []any{map[string]any{"name": "A", "value": "1"}, map[string]any{"dbPasswd": m}},
		"replicas": 2,
		"tokens":   m,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("MaskValues = %#v", got)
	}
	if in["password"] != "p@ss" || in["image"].(map[string]any)["pullSecret"] != "abc" {
		t.Fatal("input must not be modified")
	}
	if MaskValues(nil) != nil {
		t.Fatal("nil in, nil out")
	}
}

func TestMaskValuesNamedEntries(t *testing.T) {
	t.Parallel()
	in := map[string]any{"env": []any{
		map[string]any{"name": "DB_PASSWORD", "value": "s3cret"},
		map[string]any{"name": "LOG_LEVEL", "value": "debug"},
		map[string]any{"name": "API_TOKEN", "valueFrom": map[string]any{"secretKeyRef": map[string]any{"name": "x"}}},
	}}
	got := MaskValues(in)["env"].([]any)
	if got[0].(map[string]any)["value"] != k8scontract.MaskedValue {
		t.Fatalf("value of a sensitively named entry must be masked: %v", got[0])
	}
	if got[1].(map[string]any)["value"] != "debug" {
		t.Fatalf("ordinary entries stay: %v", got[1])
	}
}

func TestRestoreMasked(t *testing.T) {
	t.Parallel()
	m := k8scontract.MaskedValue
	current := map[string]any{
		"auth":     map[string]any{"password": "real-pw", "user": "root"},
		"replicas": 2,
		"env": []any{
			map[string]any{"name": "A", "value": "1"},
			map[string]any{"name": "DB_PASSWORD", "value": "db-pw"},
		},
		"tokens": []any{"t0", "t1"},
	}
	vals := map[string]any{
		"auth":     map[string]any{"password": m, "user": "admin"},
		"replicas": 3,
		// 顺序变了、多了一项：按 name 对应。
		"env": []any{
			map[string]any{"name": "DB_PASSWORD", "value": m},
			map[string]any{"name": "NEW", "value": "x"},
			map[string]any{"name": "A", "value": "1"},
		},
		"tokens": []any{m, "changed"},
	}
	if err := RestoreMasked(vals, current); err != nil {
		t.Fatal(err)
	}
	if vals["auth"].(map[string]any)["password"] != "real-pw" || vals["auth"].(map[string]any)["user"] != "admin" {
		t.Fatalf("auth = %v", vals["auth"])
	}
	if vals["env"].([]any)[0].(map[string]any)["value"] != "db-pw" {
		t.Fatalf("env = %v", vals["env"])
	}
	if vals["tokens"].([]any)[0] != "t0" || vals["tokens"].([]any)[1] != "changed" {
		t.Fatalf("tokens = %v", vals["tokens"])
	}
	// 现值里没有这条路径：拒绝并指出路径；掩码绝不作为真实值留下。
	for _, bad := range []map[string]any{
		{"auth": map[string]any{"newSecret": m}},
		{"env": []any{map[string]any{"name": "MISSING", "value": m}}},
		{"brandNew": m},
	} {
		err := RestoreMasked(bad, current)
		if err == nil || AsError(err).Code != 400 {
			t.Fatalf("%v: err = %v, want 400", bad, err)
		}
	}
	if err := RestoreMasked(map[string]any{"password": m}, nil); err == nil {
		t.Fatal("install (no current release) must refuse the mask placeholder")
	}
	if err := RestoreMasked(map[string]any{"a": "b"}, nil); err != nil {
		t.Fatalf("values without the placeholder pass: %v", err)
	}
}

func TestTruncateIsDatabaseSafe(t *testing.T) {
	t.Parallel()
	if got := Truncate("ab你好", 4); got != "ab" {
		t.Fatalf("must not cut a multi-byte character: %q", got)
	}
	if got := Truncate("a\x00b\xff\xfec", 100); got != "ab�c" {
		t.Fatalf("NUL and invalid UTF-8 must be removed: %q", got)
	}
	if got := Truncate("short", 100); got != "short" {
		t.Fatalf("short strings are unchanged: %q", got)
	}
}

func TestClampTermSize(t *testing.T) {
	t.Parallel()
	if c, r := ClampTermSize(65535, 65535); c != maxTermCols || r != maxTermRows {
		t.Fatalf("clamp = %dx%d", c, r)
	}
	if c, r := ClampTermSize(0, 0); c != 1 || r != 1 {
		t.Fatalf("clamp = %dx%d", c, r)
	}
	if c, r := ClampTermSize(120, 40); c != 120 || r != 40 {
		t.Fatalf("clamp = %dx%d", c, r)
	}
}
