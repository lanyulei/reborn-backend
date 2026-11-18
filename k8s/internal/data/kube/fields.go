package kube

import (
	"strconv"
	"time"
)

// 无拷贝的 map[string]any 取值助手（unstructured.Nested* 会深拷贝，缓存热路径上不可接受）。

func nested(obj map[string]any, path ...string) (any, bool) {
	var cur any = obj
	for _, p := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		if cur, ok = m[p]; !ok {
			return nil, false
		}
	}
	return cur, true
}

func nestedString(obj map[string]any, path ...string) (string, bool) {
	v, ok := nested(obj, path...)
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

func str(obj map[string]any, path ...string) string {
	s, _ := nestedString(obj, path...)
	return s
}

func nestedMap(obj map[string]any, path ...string) map[string]any {
	v, _ := nested(obj, path...)
	m, _ := v.(map[string]any)
	return m
}

func nestedList(obj map[string]any, path ...string) []any {
	v, _ := nested(obj, path...)
	l, _ := v.([]any)
	return l
}

// num 取数值字段（JSON 解码后可能是 int64 或 float64）。
func num(obj map[string]any, path ...string) (int64, bool) {
	v, ok := nested(obj, path...)
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case int64:
		return n, true
	case float64:
		return int64(n), true
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case string:
		i, err := strconv.ParseInt(n, 10, 64)
		return i, err == nil
	}
	return 0, false
}

func numOr(obj map[string]any, def int64, path ...string) int64 {
	if n, ok := num(obj, path...); ok {
		return n
	}
	return def
}

func boolean(obj map[string]any, path ...string) bool {
	v, _ := nested(obj, path...)
	b, _ := v.(bool)
	return b
}

func timeAt(obj map[string]any, path ...string) (time.Time, bool) {
	s, ok := nestedString(obj, path...)
	if !ok || s == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, s)
	return t, err == nil
}

func stringMap(obj map[string]any, path ...string) map[string]string {
	m := nestedMap(obj, path...)
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

// condition 取 status.conditions 里某个类型的条件。
func condition(obj map[string]any, typ string) map[string]any {
	for _, c := range nestedList(obj, "status", "conditions") {
		if m := asMap(c); str(m, "type") == typ {
			return m
		}
	}
	return nil
}
