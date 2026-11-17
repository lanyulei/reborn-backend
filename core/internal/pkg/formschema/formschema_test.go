package formschema

import (
	"errors"
	"testing"
)

func ptr[T any](v T) *T { return &v }

func testFields() []Field {
	return []Field{
		{Code: "ip", Type: TypeIP, Required: true},
		{Code: "cores", Type: TypeInt, Rule: Rule{Min: ptr(1.0), Max: ptr(64.0)}},
		{Code: "ratio", Type: TypeFloat},
		{Code: "on", Type: TypeBool},
		{Code: "kind", Type: TypeEnum, EnumValues: []string{"a", "b"}},
		{Code: "since", Type: TypeDatetime},
		{Code: "name", Type: TypeString, Rule: Rule{Pattern: ptr("^[a-z]+$"), MinLen: ptr(int32(2)), MaxLen: ptr(int32(5))}},
		{Code: "meta", Type: TypeJSON},
		{Code: "ref", Type: TypeRef},
		{Code: "old", Type: TypeString, Deprecated: true},
	}
}

func TestValidate(t *testing.T) {
	t.Parallel()
	fields := testFields()
	out, err := Validate(fields, map[string]any{
		"ip": " 10.0.0.1 ", "cores": float64(8), "ratio": nil, "on": true, "kind": "a",
		"since": "2026-01-01T08:00:00+08:00", "name": "abc", "meta": map[string]any{"x": 1}, "ref": "id-1",
	}, false)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if out["ip"] != "10.0.0.1" || out["cores"] != int64(8) || out["since"] != "2026-01-01T00:00:00Z" || out["ref"] != "id-1" {
		t.Fatalf("normalized = %v", out)
	}
	if _, ok := out["ratio"]; ok {
		t.Fatal("nil value must be dropped")
	}

	bad := map[string]struct {
		values map[string]any
		field  string
		msg    string
	}{
		"required missing": {map[string]any{"cores": float64(1)}, "ip", `field "ip" is required`},
		"unknown field":    {map[string]any{"ip": "10.0.0.1", "nope": 1}, "nope", `unknown field "nope"`},
		"deprecated field": {map[string]any{"ip": "10.0.0.1", "old": "x"}, "old", `unknown field "old"`},
		"bad ip":           {map[string]any{"ip": "300.1.1.1"}, "ip", `field "ip" must be a valid IP address`},
		"ip not string":    {map[string]any{"ip": 5}, "ip", `field "ip" must be an IP address string`},
		"non-integer":      {map[string]any{"ip": "10.0.0.1", "cores": 1.5}, "cores", `field "cores" must be an integer`},
		"below range":      {map[string]any{"ip": "10.0.0.1", "cores": float64(0)}, "cores", `field "cores" must be >= 1`},
		"above range":      {map[string]any{"ip": "10.0.0.1", "cores": float64(65)}, "cores", `field "cores" must be <= 64`},
		"not a number":     {map[string]any{"ip": "10.0.0.1", "ratio": "x"}, "ratio", `field "ratio" must be a number`},
		"enum":             {map[string]any{"ip": "10.0.0.1", "kind": "zzz"}, "kind", `field "kind" must be one of [a b]`},
		"datetime":         {map[string]any{"ip": "10.0.0.1", "since": "yesterday"}, "since", `field "since" must be an RFC3339 datetime string`},
		"pattern":          {map[string]any{"ip": "10.0.0.1", "name": "ABC"}, "name", `field "name" does not match pattern`},
		"min len":          {map[string]any{"ip": "10.0.0.1", "name": "a"}, "name", `field "name" length must be >= 2`},
		"max len":          {map[string]any{"ip": "10.0.0.1", "name": "abcdef"}, "name", `field "name" length must be <= 5`},
		"bool type":        {map[string]any{"ip": "10.0.0.1", "on": "yes"}, "on", `field "on" must be a boolean`},
		"ref type":         {map[string]any{"ip": "10.0.0.1", "ref": 5}, "ref", `field "ref" must be a string`},
	}
	for name, tt := range bad {
		_, err := Validate(fields, tt.values, false)
		var fe *Error
		if !errors.As(err, &fe) {
			t.Errorf("%s: error = %v, want *Error", name, err)
			continue
		}
		if fe.Field != tt.field || fe.Error() != tt.msg {
			t.Errorf("%s: got field %q msg %q, want field %q msg %q", name, fe.Field, fe.Error(), tt.field, tt.msg)
		}
	}

	// partial：缺少必填不报错，但类型仍校验。
	if _, err := Validate(fields, map[string]any{"cores": float64(2)}, true); err != nil {
		t.Fatalf("partial should skip required: %v", err)
	}
	if _, err := Validate(fields, map[string]any{"cores": "two"}, true); err == nil {
		t.Fatal("partial still validates types")
	}
}

func TestValidate_NumericInputs(t *testing.T) {
	t.Parallel()
	fields := []Field{{Code: "n", Type: TypeInt}, {Code: "f", Type: TypeFloat}}
	for _, v := range []any{float64(3), float32(3), int(3), int32(3), int64(3)} {
		out, err := Validate(fields, map[string]any{"n": v, "f": v}, true)
		if err != nil {
			t.Fatalf("Validate(%T) error = %v", v, err)
		}
		if out["n"] != int64(3) || out["f"] != float64(3) {
			t.Fatalf("Validate(%T) = %v, want int64(3)/float64(3)", v, out)
		}
	}
}

// 必填字段写入空串视为未填；只约束本次写入的键（partial 时未提供的必填键不查）。
func TestValidate_RequiredEmptyString(t *testing.T) {
	t.Parallel()
	fields := []Field{{Code: "site", Type: TypeString, Required: true}, {Code: "note", Type: TypeString}}
	for _, partial := range []bool{false, true} {
		if _, err := Validate(fields, map[string]any{"site": ""}, partial); err == nil {
			t.Fatalf("partial=%v: empty required string must be rejected", partial)
		}
	}
	if out, err := Validate(fields, map[string]any{"note": ""}, true); err != nil || out["note"] != "" {
		t.Fatalf("empty optional string must pass: %v %v", out, err)
	}
}

func TestValidate_UnsupportedTypeAndBadPattern(t *testing.T) {
	t.Parallel()
	fields := []Field{
		{Code: "odd", Type: "mystery"},
		{Code: "bad", Type: TypeString, Rule: Rule{Pattern: ptr("([")}},
	}
	if _, err := Validate(fields, map[string]any{"odd": "x"}, true); err == nil || err.Error() != `field "odd" has unsupported type` {
		t.Fatalf("unsupported type error = %v", err)
	}
	if _, err := Validate(fields, map[string]any{"bad": "x"}, true); err == nil || err.Error() != `field "bad" has invalid pattern` {
		t.Fatalf("invalid pattern error = %v", err)
	}
}

func TestCheckRequired(t *testing.T) {
	t.Parallel()
	fields := []Field{
		{Code: "ip", Type: TypeString, Required: true},
		{Code: "kind", Type: TypeEnum, EnumValues: []string{"a"}},
		{Code: "old", Type: TypeString, Required: true, Deprecated: true},
	}
	// 遗留的已废弃键、已移出枚举的存量值都不阻塞；已废弃的必填字段不再要求。
	if err := CheckRequired(fields, map[string]any{"ip": "1", "kind": "removed", "legacy": "x"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var fe *Error
	if err := CheckRequired(fields, map[string]any{"kind": "a"}); !errors.As(err, &fe) || fe.Field != "ip" {
		t.Fatalf("missing required field: error = %v", err)
	}
	if err := CheckRequired(fields, map[string]any{"ip": nil}); err == nil {
		t.Fatal("nil required field must fail")
	}
}

func TestCompilePattern(t *testing.T) {
	t.Parallel()
	first, err := CompilePattern("^a+$")
	if err != nil {
		t.Fatalf("CompilePattern() error = %v", err)
	}
	second, _ := CompilePattern("^a+$")
	if first != second {
		t.Fatal("compiled pattern must be cached")
	}
	if _, err := CompilePattern("(["); err == nil {
		t.Fatal("invalid pattern must fail")
	}
}

func BenchmarkValidate(b *testing.B) {
	fields := testFields()
	values := map[string]any{
		"ip": "10.0.0.1", "cores": float64(8), "on": true, "kind": "a",
		"since": "2026-01-01T08:00:00+08:00", "name": "abc",
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Validate(fields, values, false); err != nil {
			b.Fatal(err)
		}
	}
}

func TestValidate_TextUserDept(t *testing.T) {
	t.Parallel()
	fields := []Field{
		{Code: "note", Type: TypeText, Rule: Rule{MaxLen: ptr(int32(5))}},
		{Code: "owner", Type: TypeUser, Required: true},
		{Code: "dept", Type: TypeDept},
	}
	const id = "018F3E1A-0A00-7000-8000-000000000001"
	out, err := Validate(fields, map[string]any{"note": "多行\n文本", "owner": id, "dept": ""}, false)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if out["owner"] != "018f3e1a-0a00-7000-8000-000000000001" || out["note"] != "多行\n文本" {
		t.Fatalf("normalized = %v", out)
	}
	for name, values := range map[string]map[string]any{
		"text too long":       {"owner": id, "note": "123456"},
		"user not an id":      {"owner": "alice"},
		"user wrong type":     {"owner": 5},
		"dept not an id":      {"owner": id, "dept": "rd"},
		"required user empty": {"owner": ""},
	} {
		if _, err := Validate(fields, values, false); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}

// 扩展能力：多选、日期、内置格式、小数位。
func TestValidate_Extensions(t *testing.T) {
	t.Parallel()
	two := int32(2)
	fields := []Field{
		{Code: "envs", Type: TypeEnum, EnumValues: []string{"prod", "test"}, Multiple: true, Required: true},
		{Code: "owners", Type: TypeUser, Multiple: true},
		{Code: "day", Type: TypeDate},
		{Code: "mail", Type: TypeString, Format: FormatEmail},
		{Code: "amount", Type: TypeFloat, Precision: &two},
	}
	uid := "018F3E1A-0A10-7000-8000-000000000001"
	out, err := Validate(fields, map[string]any{
		"envs": []any{"prod", "prod", "test"}, "owners": []any{uid, ""}, "day": "2026-11-17",
		"mail": "a@b.co", "amount": 1.239,
	}, false)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if got := out["envs"].([]any); len(got) != 2 || got[0] != "prod" || got[1] != "test" {
		t.Errorf("envs = %v, want deduplicated [prod test]", got)
	}
	if got := out["owners"].([]any); len(got) != 1 || got[0] != "018f3e1a-0a10-7000-8000-000000000001" {
		t.Errorf("owners = %v, want one lower-cased id", got)
	}
	if out["amount"] != 1.24 {
		t.Errorf("amount = %v, want 1.24", out["amount"])
	}
	for name, values := range map[string]map[string]any{
		"scalar for multiple":   {"envs": "prod"},
		"unknown option":        {"envs": []any{"moon"}},
		"empty required list":   {"envs": []any{}},
		"bad id in list":        {"envs": []any{"prod"}, "owners": []any{"nope"}},
		"bad date":              {"envs": []any{"prod"}, "day": "2026-13-40"},
		"bad email":             {"envs": []any{"prod"}, "mail": "not-an-email"},
		"too many items":        {"envs": make([]any, MaxMultiple+1)},
		"non-string list items": {"envs": []any{1}},
	} {
		if _, err := Validate(fields, values, false); err == nil {
			t.Errorf("%s: Validate() error = nil, want an error", name)
		}
	}
	// 非必填的空列表不落值。
	if out, err := Validate(fields, map[string]any{"envs": []any{"prod"}, "owners": []any{}}, false); err != nil || out["owners"] != nil {
		t.Errorf("empty optional list: out = %v, err %v", out, err)
	}
	// JSON 字段的空数组是合法取值：原样保留，必填也算已填。
	js := []Field{{Code: "tags", Type: TypeJSON, Required: true}}
	if out, err := Validate(js, map[string]any{"tags": []any{}}, false); err != nil || out["tags"] == nil {
		t.Errorf("empty JSON array: out = %v, err %v, want kept", out, err)
	}
	if !ValidFormat("") || !ValidFormat(FormatURL) || ValidFormat("ssn") {
		t.Error("ValidFormat() mismatch")
	}
}
