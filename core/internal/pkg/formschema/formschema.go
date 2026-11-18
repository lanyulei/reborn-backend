// Package formschema 提供「元数据驱动的动态字段」的定义与取值校验：给定一组字段
// 定义（类型 / 必填 / 枚举 / 规则），校验并归一化一份 code → value 的取值。
//
// 中性包：纯计算、零 IO、不依赖任何业务域与错误码体系，biz 层可直接 import。
// 消费方（CMDB 资源属性、工单表单等）各自持有带域专有属性的字段模型，转换为
// 本包的 Field 后调用；校验失败返回 *Error，由消费方映射为自己的业务错误码。
// 需要查库的校验（引用是否存在、人员是否有效等）不在本包职责内，由消费方另行判定。
package formschema

import (
	"fmt"
	"math"
	"net"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
)

// 字段数据类型。
const (
	// TypeString 字符串（可带长度 / 正则规则）。
	TypeString = "string"
	// TypeInt 整数（可带范围规则），归一为 int64。
	TypeInt = "int"
	// TypeFloat 浮点数（可带范围规则），归一为 float64。
	TypeFloat = "float"
	// TypeBool 布尔。
	TypeBool = "bool"
	// TypeEnum 单选枚举，取值须在 EnumValues 内。
	TypeEnum = "enum"
	// TypeDatetime 时间，RFC3339 字符串，归一为 UTC。
	TypeDatetime = "datetime"
	// TypeJSON 任意 JSON 值，原样保留。
	TypeJSON = "json"
	// TypeIP IP 地址，归一为规范化文本。
	TypeIP = "ip"
	// TypeRef 引用（对端实体 ID 字符串）；是否存在由消费方判定。
	TypeRef = "ref"
	// TypeText 多行文本（规则同字符串，仅渲染为多行输入）。
	TypeText = "text"
	// TypeUser 人员（用户 ID，UUID 文本，归一为小写）；是否存在由消费方判定。
	TypeUser = "user"
	// TypeDept 部门（部门 ID，UUID 文本，归一为小写）；是否存在由消费方判定。
	TypeDept = "dept"
	// TypeDate 日期（不含时间），YYYY-MM-DD 字符串。
	TypeDate = "date"
	// TypeFile 附件：取值恒为文件 ID（UUID 文本，归一为小写）的列表，去重；未开多选至多 1 个，
	// 开了多选至多 MaxFiles 个。文件是否存在、归谁所有由消费方判定。
	TypeFile = "file"
)

// 单行文本的内置格式（Field.Format）。
const (
	FormatEmail = "email"
	FormatPhone = "phone"
	FormatURL   = "url"
)

// MaxMultiple 多选字段一次最多选的个数。
const MaxMultiple = 50

// MaxFiles 一个附件字段最多带的文件数。
const MaxFiles = 10

// formatPatterns 内置格式的校验正则：宽松但足以拦下明显写错的值。
var formatPatterns = map[string]*regexp.Regexp{
	FormatEmail: regexp.MustCompile(`^[^\s@]+@[^\s@]+\.[^\s@]+$`),
	FormatPhone: regexp.MustCompile(`^\+?[0-9][0-9 \-]{4,19}$`),
	FormatURL:   regexp.MustCompile(`^https?://[^\s]+$`),
}

// ValidFormat 报告 format 是否为受支持的内置格式（空串 = 不限格式）。
func ValidFormat(format string) bool {
	_, ok := formatPatterns[format]
	return format == "" || ok
}

// Rule 字段校验规则；nil 表示不约束。json 标签为持久化契约（jsonb）。
type Rule struct {
	Pattern *string  `json:"pattern,omitempty"`
	Min     *float64 `json:"min,omitempty"`
	Max     *float64 `json:"max,omitempty"`
	MinLen  *int32   `json:"minLen,omitempty"`
	MaxLen  *int32   `json:"maxLen,omitempty"`
}

// Field 字段定义中参与取值校验的部分。
type Field struct {
	// Code 字段码，取值 map 的键。
	Code string
	// Type 数据类型（Type* 常量）。
	Type string
	// Required 必填。
	Required bool
	// EnumValues 枚举可选值（Type 为 TypeEnum 时生效）。
	EnumValues []string
	// Rule 校验规则。
	Rule Rule
	// Deprecated 已废弃：不再接受写入，也不再要求必填。
	Deprecated bool
	// Multiple 多选（仅 TypeEnum / TypeUser / TypeDept）：取值为字符串列表，去重、至多 MaxMultiple 项。
	// 对 TypeFile 表示可以带多个文件（取值本来就是列表）。
	Multiple bool
	// Format 单行文本的内置格式（Format* 常量）；空 = 不限。
	Format string
	// Precision 小数位数上限（仅 TypeFloat）：超出的部分四舍五入；nil = 不限。
	Precision *int32
}

// Error 取值校验失败：Field 为出错的字段码，Error() 为面向调用方的英文说明。
type Error struct {
	Field string
	msg   string
}

// Error implements error.
func (e *Error) Error() string { return e.msg }

func fieldError(code, format string, args ...any) *Error {
	return &Error{Field: code, msg: fmt.Sprintf(format, args...)}
}

// patternCache 正则编译缓存（按模式串缓存；编译结果不可变，可并发复用）。
var patternCache sync.Map

// CompilePattern 编译并缓存字段正则；模式串非法返回编译错误（字段定义校验亦复用）。
func CompilePattern(pattern string) (*regexp.Regexp, error) {
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

// Validate 按字段定义校验并归一化取值：
//   - 未定义或已废弃的键拒绝（防脏数据）；
//   - partial=false 时必填字段必须出现且非空；
//   - 各类型归一：int → int64、float → float64、datetime → RFC3339 UTC 字符串、ip → 规范化文本；
//   - value 为 nil 表示清空该键（返回值中不包含）。
//
// 校验失败返回 *Error。
func Validate(fields []Field, values map[string]any, partial bool) (map[string]any, error) {
	byCode := make(map[string]*Field, len(fields))
	for i := range fields {
		if f := &fields[i]; !f.Deprecated {
			byCode[f.Code] = f
		}
	}
	out := make(map[string]any, len(values))
	for key, raw := range values {
		f, ok := byCode[key]
		if !ok {
			return nil, fieldError(key, "unknown field %q", key)
		}
		if raw == nil {
			continue
		}
		v, err := normalizeValue(f, raw)
		if err != nil {
			return nil, err
		}
		// 必填字段写入空串 / 空列表视为未填。只约束本次写入的键：存量值不重校验。
		if s, ok := v.(string); ok && s == "" && f.Required {
			return nil, fieldError(key, "field %q is required", key)
		}
		// 只对多选与附件字段这样处理：JSON 字段的空数组是合法取值，原样保留。
		if list, ok := v.([]any); ok && (f.Multiple || f.Type == TypeFile) && len(list) == 0 {
			if f.Required {
				return nil, fieldError(key, "field %q is required", key)
			}
			continue // 空列表 = 没选：不落值
		}
		out[key] = v
	}
	if !partial {
		for code, f := range byCode {
			if f.Required {
				if _, ok := out[code]; !ok {
					return nil, fieldError(code, "field %q is required", code)
				}
			}
		}
	}
	return out, nil
}

// CheckRequired 合并后的取值须包含全部启用的必填字段。只看键是否存在且非 nil：
// 存量值不重校验（字段规则 / 枚举值收紧后不阻塞无关更新），已废弃字段的遗留键忽略。
// 缺失返回 *Error。
func CheckRequired(fields []Field, values map[string]any) error {
	for i := range fields {
		f := &fields[i]
		if f.Deprecated || !f.Required {
			continue
		}
		if v, ok := values[f.Code]; !ok || v == nil {
			return fieldError(f.Code, "field %q is required", f.Code)
		}
	}
	return nil
}

// normalizeValue 单字段校验与归一化。
func normalizeValue(f *Field, raw any) (any, error) {
	bad := func(msg string) error { return fieldError(f.Code, "field %q %s", f.Code, msg) }
	if f.Type == TypeFile {
		items, ok := raw.([]any)
		if !ok {
			return nil, bad("must be a list of file ids")
		}
		limit := 1
		if f.Multiple {
			limit = MaxFiles
		}
		out := make([]any, 0, len(items))
		for _, item := range items {
			s, ok := item.(string)
			if !ok || !idgen.IsUUIDFold(s) {
				return nil, bad("must be a list of file ids")
			}
			if s = strings.ToLower(s); !slices.Contains(out, any(s)) {
				out = append(out, s)
			}
		}
		// 去重之后再数：同一个文件写两遍不算两个。
		if len(out) > limit {
			return nil, bad(fmt.Sprintf("accepts at most %d file(s)", limit))
		}
		return out, nil
	}
	if f.Multiple && (f.Type == TypeEnum || f.Type == TypeUser || f.Type == TypeDept) {
		items, ok := raw.([]any)
		if !ok {
			return nil, bad("must be a list")
		}
		if len(items) > MaxMultiple {
			return nil, bad(fmt.Sprintf("accepts at most %d items", MaxMultiple))
		}
		single := *f
		single.Multiple = false
		out := make([]any, 0, len(items))
		for _, item := range items {
			v, err := normalizeValue(&single, item)
			if err != nil {
				return nil, err
			}
			// 空串是「未填」的占位，不进列表；重复的只留一个。
			if v != "" && !slices.Contains(out, v) {
				out = append(out, v)
			}
		}
		return out, nil
	}
	switch f.Type {
	case TypeString, TypeRef, TypeText:
		s, ok := raw.(string)
		if !ok {
			return nil, bad("must be a string")
		}
		if f.Type != TypeRef {
			if err := checkStringRule(f, s); err != nil {
				return nil, err
			}
		}
		if re := formatPatterns[f.Format]; re != nil && f.Type == TypeString && s != "" && !re.MatchString(s) {
			return nil, bad("is not a valid " + f.Format)
		}
		return s, nil
	case TypeDate:
		s, ok := raw.(string)
		if !ok {
			return nil, bad("must be a date string (YYYY-MM-DD)")
		}
		if s == "" {
			return s, nil // 空串 = 未填
		}
		if _, err := time.Parse(time.DateOnly, s); err != nil {
			return nil, bad("must be a date string (YYYY-MM-DD)")
		}
		return s, nil
	case TypeUser, TypeDept:
		s, ok := raw.(string)
		if !ok {
			return nil, bad("must be an id string")
		}
		if s == "" {
			return s, nil // 空串 = 未填（必填由调用方的必填判定拦截）
		}
		if !idgen.IsUUIDFold(s) {
			return nil, bad("must be a valid id")
		}
		return strings.ToLower(s), nil
	case TypeInt:
		n, ok := toFloat(raw)
		if !ok || n != math.Trunc(n) {
			return nil, bad("must be an integer")
		}
		if err := checkRange(f, n); err != nil {
			return nil, err
		}
		return int64(n), nil
	case TypeFloat:
		n, ok := toFloat(raw)
		if !ok {
			return nil, bad("must be a number")
		}
		if err := checkRange(f, n); err != nil {
			return nil, err
		}
		if f.Precision != nil && *f.Precision >= 0 && *f.Precision <= 9 {
			scale := math.Pow10(int(*f.Precision))
			n = math.Round(n*scale) / scale
		}
		return n, nil
	case TypeBool:
		b, ok := raw.(bool)
		if !ok {
			return nil, bad("must be a boolean")
		}
		return b, nil
	case TypeEnum:
		s, ok := raw.(string)
		if !ok || !slices.Contains(f.EnumValues, s) {
			return nil, bad(fmt.Sprintf("must be one of %v", f.EnumValues))
		}
		return s, nil
	case TypeDatetime:
		s, ok := raw.(string)
		if !ok {
			return nil, bad("must be an RFC3339 datetime string")
		}
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			return nil, bad("must be an RFC3339 datetime string")
		}
		return t.UTC().Format(time.RFC3339), nil
	case TypeIP:
		s, ok := raw.(string)
		if !ok {
			return nil, bad("must be an IP address string")
		}
		ip := net.ParseIP(strings.TrimSpace(s))
		if ip == nil {
			return nil, bad("must be a valid IP address")
		}
		return ip.String(), nil
	case TypeJSON:
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
		return fieldError(f.Code, "field %q must be >= %v", f.Code, *f.Rule.Min)
	}
	if f.Rule.Max != nil && n > *f.Rule.Max {
		return fieldError(f.Code, "field %q must be <= %v", f.Code, *f.Rule.Max)
	}
	return nil
}

func checkStringRule(f *Field, s string) error {
	n := len([]rune(s))
	if f.Rule.MinLen != nil && n < int(*f.Rule.MinLen) {
		return fieldError(f.Code, "field %q length must be >= %d", f.Code, *f.Rule.MinLen)
	}
	if f.Rule.MaxLen != nil && n > int(*f.Rule.MaxLen) {
		return fieldError(f.Code, "field %q length must be <= %d", f.Code, *f.Rule.MaxLen)
	}
	if f.Rule.Pattern != nil && *f.Rule.Pattern != "" {
		re, err := CompilePattern(*f.Rule.Pattern)
		if err != nil {
			return fieldError(f.Code, "field %q has invalid pattern", f.Code)
		}
		if !re.MatchString(s) {
			return fieldError(f.Code, "field %q does not match pattern", f.Code)
		}
	}
	return nil
}
