package biz

import (
	"fmt"
	"math"
	"net"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"
)

// patternCache 正则编译缓存（字段规则不可变，按模式串缓存）。
var patternCache sync.Map

func compilePattern(pattern string) (*regexp.Regexp, error) {
	if v, ok := patternCache.Load(pattern); ok {
		return v.(*regexp.Regexp), nil
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	patternCache.Store(pattern, re)
	return re, nil
}

// ValidateAttrs 按字段定义校验并归一化属性：
//   - 未定义或已废弃的键拒绝（防脏数据）；
//   - partial=false 时必填字段必须出现且非空；
//   - 各类型归一：int → int64、float → float64、datetime → RFC3339 字符串、ip → 规范化文本；
//   - value 为 nil 表示清空该键（返回值中不包含）。
func ValidateAttrs(fields []*Field, attrs map[string]any, partial bool) (map[string]any, error) {
	byCode := make(map[string]*Field, len(fields))
	for _, f := range fields {
		if !f.Deprecated {
			byCode[f.Code] = f
		}
	}
	out := make(map[string]any, len(attrs))
	for key, raw := range attrs {
		f, ok := byCode[key]
		if !ok {
			return nil, detail(ErrCIInvalid, "unknown field %q", key)
		}
		if raw == nil {
			continue
		}
		v, err := normalizeValue(f, raw)
		if err != nil {
			return nil, err
		}
		out[key] = v
	}
	if !partial {
		for code, f := range byCode {
			if f.Required {
				if _, ok := out[code]; !ok {
					return nil, detail(ErrCIInvalid, "field %q is required", code)
				}
			}
		}
	}
	return out, nil
}

// checkRequired 合并后的属性须包含全部启用的必填字段。只看键是否存在且非空：
// 存量值不重校验（字段规则 / 枚举值收紧后不阻塞无关更新），已废弃字段的遗留键忽略。
func checkRequired(fields []*Field, attrs map[string]any) error {
	for _, f := range fields {
		if f.Deprecated || !f.Required {
			continue
		}
		if v, ok := attrs[f.Code]; !ok || v == nil {
			return detail(ErrCIInvalid, "field %q is required", f.Code)
		}
	}
	return nil
}

// normalizeValue 单字段校验与归一化。
func normalizeValue(f *Field, raw any) (any, error) {
	bad := func(msg string) error { return detail(ErrCIInvalid, "field %q %s", f.Code, msg) }
	switch f.Type {
	case FieldTypeString, FieldTypeRef:
		s, ok := raw.(string)
		if !ok {
			return nil, bad("must be a string")
		}
		if f.Type == FieldTypeString {
			if err := checkStringRule(f, s); err != nil {
				return nil, err
			}
		}
		return s, nil
	case FieldTypeInt:
		n, ok := toFloat(raw)
		if !ok || n != math.Trunc(n) {
			return nil, bad("must be an integer")
		}
		if err := checkRange(f, n); err != nil {
			return nil, err
		}
		return int64(n), nil
	case FieldTypeFloat:
		n, ok := toFloat(raw)
		if !ok {
			return nil, bad("must be a number")
		}
		if err := checkRange(f, n); err != nil {
			return nil, err
		}
		return n, nil
	case FieldTypeBool:
		b, ok := raw.(bool)
		if !ok {
			return nil, bad("must be a boolean")
		}
		return b, nil
	case FieldTypeEnum:
		s, ok := raw.(string)
		if !ok || !slices.Contains(f.EnumValues, s) {
			return nil, bad(fmt.Sprintf("must be one of %v", f.EnumValues))
		}
		return s, nil
	case FieldTypeDatetime:
		s, ok := raw.(string)
		if !ok {
			return nil, bad("must be an RFC3339 datetime string")
		}
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			return nil, bad("must be an RFC3339 datetime string")
		}
		return t.UTC().Format(time.RFC3339), nil
	case FieldTypeIP:
		s, ok := raw.(string)
		if !ok {
			return nil, bad("must be an IP address string")
		}
		ip := net.ParseIP(strings.TrimSpace(s))
		if ip == nil {
			return nil, bad("must be a valid IP address")
		}
		return ip.String(), nil
	case FieldTypeJSON:
		return raw, nil
	default:
		return nil, bad("has unsupported type")
	}
}

func toFloat(raw any) (float64, bool) {
	switch v := raw.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	default:
		return 0, false
	}
}

func checkRange(f *Field, n float64) error {
	if f.Rule.Min != nil && n < *f.Rule.Min {
		return detail(ErrCIInvalid, "field %q must be >= %v", f.Code, *f.Rule.Min)
	}
	if f.Rule.Max != nil && n > *f.Rule.Max {
		return detail(ErrCIInvalid, "field %q must be <= %v", f.Code, *f.Rule.Max)
	}
	return nil
}

func checkStringRule(f *Field, s string) error {
	n := len([]rune(s))
	if f.Rule.MinLen != nil && n < int(*f.Rule.MinLen) {
		return detail(ErrCIInvalid, "field %q length must be >= %d", f.Code, *f.Rule.MinLen)
	}
	if f.Rule.MaxLen != nil && n > int(*f.Rule.MaxLen) {
		return detail(ErrCIInvalid, "field %q length must be <= %d", f.Code, *f.Rule.MaxLen)
	}
	if f.Rule.Pattern != nil && *f.Rule.Pattern != "" {
		re, err := compilePattern(*f.Rule.Pattern)
		if err != nil {
			return detail(ErrCIInvalid, "field %q has invalid pattern", f.Code)
		}
		if !re.MatchString(s) {
			return detail(ErrCIInvalid, "field %q does not match pattern", f.Code)
		}
	}
	return nil
}
