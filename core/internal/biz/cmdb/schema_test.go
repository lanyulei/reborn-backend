package biz

import (
	"errors"
	"strings"
	"testing"
)

func ptr[T any](v T) *T { return &v }

func testFields() []*Field {
	return []*Field{
		{Code: "ip", Type: FieldTypeIP, Required: true, Unique: true},
		{Code: "cores", Type: FieldTypeInt, Rule: FieldRule{Min: ptr(1.0), Max: ptr(64.0)}},
		{Code: "ratio", Type: FieldTypeFloat},
		{Code: "on", Type: FieldTypeBool},
		{Code: "kind", Type: FieldTypeEnum, EnumValues: []string{"a", "b"}},
		{Code: "since", Type: FieldTypeDatetime},
		{Code: "name", Type: FieldTypeString, Unique: true, Rule: FieldRule{Pattern: ptr("^[a-z]+$"), MaxLen: ptr(int32(5))}},
		{Code: "meta", Type: FieldTypeJSON},
		{Code: "old", Type: FieldTypeString, Deprecated: true},
	}
}

func TestValidateAttrs(t *testing.T) {
	t.Parallel()
	fields := testFields()
	out, err := ValidateAttrs(fields, map[string]any{
		"ip": " 10.0.0.1 ", "cores": float64(8), "ratio": nil, "on": true, "kind": "a",
		"since": "2026-01-01T08:00:00+08:00", "name": "abc", "meta": map[string]any{"x": 1},
	}, false)
	if err != nil {
		t.Fatalf("ValidateAttrs() error = %v", err)
	}
	if out["ip"] != "10.0.0.1" || out["cores"] != int64(8) || out["since"] != "2026-01-01T00:00:00Z" {
		t.Fatalf("normalized = %v", out)
	}
	if _, ok := out["ratio"]; ok {
		t.Fatal("nil value must be dropped")
	}
	bad := map[string]map[string]any{
		"required missing": {"cores": float64(1)},
		"unknown field":    {"ip": "10.0.0.1", "nope": 1},
		"deprecated field": {"ip": "10.0.0.1", "old": "x"},
		"bad ip":           {"ip": "300.1.1.1"},
		"non-integer":      {"ip": "10.0.0.1", "cores": 1.5},
		"out of range":     {"ip": "10.0.0.1", "cores": float64(65)},
		"enum":             {"ip": "10.0.0.1", "kind": "zzz"},
		"datetime":         {"ip": "10.0.0.1", "since": "yesterday"},
		"pattern":          {"ip": "10.0.0.1", "name": "ABC"},
		"max len":          {"ip": "10.0.0.1", "name": "abcdef"},
		"bool type":        {"ip": "10.0.0.1", "on": "yes"},
	}
	for name, attrs := range bad {
		if _, err := ValidateAttrs(fields, attrs, false); !errors.Is(err, ErrCIInvalid) {
			t.Errorf("%s: error = %v, want ErrCIInvalid", name, err)
		}
	}
	// partial：缺少必填不报错，但类型仍校验。
	if _, err := ValidateAttrs(fields, map[string]any{"cores": float64(2)}, true); err != nil {
		t.Fatalf("partial should skip required: %v", err)
	}
	if _, err := ValidateAttrs(fields, map[string]any{"cores": "two"}, true); !errors.Is(err, ErrCIInvalid) {
		t.Fatalf("partial still validates types: %v", err)
	}
}

func TestUniqueKey(t *testing.T) {
	t.Parallel()
	fields := testFields()
	key := UniqueKey(fields, map[string]any{"ip": "10.0.0.1", "name": "abc", "cores": int64(8)})
	if key != "ip=10.0.0.1|name=abc" {
		t.Fatalf("UniqueKey() = %q", key)
	}
	if UniqueKey(fields, map[string]any{"cores": int64(8)}) != "" {
		t.Fatal("no unique values must yield empty key")
	}
}

func TestCanTransition(t *testing.T) {
	t.Parallel()
	allowed := [][2]string{{CIStatusPlanning, CIStatusActive}, {CIStatusActive, CIStatusMaintenance}, {CIStatusMaintenance, CIStatusActive}, {CIStatusActive, CIStatusRetired}}
	denied := [][2]string{{CIStatusRetired, CIStatusActive}, {CIStatusActive, CIStatusPlanning}, {CIStatusActive, CIStatusActive}, {"bogus", CIStatusActive}}
	for _, c := range allowed {
		if !CanTransition(c[0], c[1]) {
			t.Errorf("%s → %s should be allowed", c[0], c[1])
		}
	}
	for _, c := range denied {
		if CanTransition(c[0], c[1]) {
			t.Errorf("%s → %s should be denied", c[0], c[1])
		}
	}
}

func TestDiffCI(t *testing.T) {
	t.Parallel()
	cur := &CI{Name: "a", Tags: []string{"x"}, Attrs: map[string]any{"ip": "1", "cores": int64(1), "gone": "g"}}
	next := &CI{Name: "b", Tags: []string{"x"}, Attrs: map[string]any{"ip": "1", "cores": int64(2), "new": "n"}}
	before, after := diffCI(cur, next)
	if before["name"] != "a" || after["name"] != "b" {
		t.Fatalf("name diff wrong: %v %v", before, after)
	}
	ab, aa := before["attrs"].(map[string]any), after["attrs"].(map[string]any)
	if ab["cores"] != int64(1) || aa["cores"] != int64(2) || aa["gone"] != nil || ab["new"] != nil || aa["new"] != "n" {
		t.Fatalf("attrs diff wrong: %v %v", ab, aa)
	}
	if _, ok := ab["ip"]; ok {
		t.Fatal("unchanged attr must not appear")
	}
	if _, _ = diffCI(cur, cur); len(after) == 0 {
		t.Fatal("sanity")
	}
	if b, a := diffCI(cur, cur); len(b) != 0 || len(a) != 0 {
		t.Fatal("identical CIs must produce empty diff")
	}
	if !strings.Contains(ErrCIInvalid.Error(), "CMDB_CI_INVALID") {
		t.Fatal("sentinel reason")
	}
}
