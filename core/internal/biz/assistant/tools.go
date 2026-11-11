package biz

import (
	"encoding/json"
	"strings"
)

// maxToolOutputBytes 回传给模型/落审计的工具输出上限；超出截断并提示缩小分页。
const maxToolOutputBytes = 32 << 10

// sensitiveKeyParts 入参/出参脱敏的键名片段（大小写不敏感，子串匹配）；与
// kit/httpx 日志脱敏口径一致，宁可误打码不可漏打码。
var sensitiveKeyParts = []string{"password", "secret", "token", "authorization", "api_key", "apikey"}

// maskedValue 脱敏替换值。
const maskedValue = "***"

// MaskJSON 对 JSON 对象/数组中键名含敏感片段的值做脱敏（递归）；非法 JSON
// 原样返回（不阻断调用，审计仍可落库）。
func MaskJSON(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return raw
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return raw
	}
	if !maskValue(v) {
		return raw
	}
	out, err := json.Marshal(v)
	if err != nil {
		return raw
	}
	return out
}

// maskValue 递归脱敏，返回是否发生替换（无替换时调用方可跳过重编码）。
func maskValue(v any) bool {
	changed := false
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			if isSensitiveKey(k) {
				if s, ok := val.(string); !ok || s != "" {
					t[k] = maskedValue
					changed = true
				}
				continue
			}
			if maskValue(val) {
				changed = true
			}
		}
	case []any:
		for _, item := range t {
			if maskValue(item) {
				changed = true
			}
		}
	}
	return changed
}

func isSensitiveKey(k string) bool {
	lk := strings.ToLower(k)
	for _, part := range sensitiveKeyParts {
		if strings.Contains(lk, part) {
			return true
		}
	}
	return false
}

// TruncateOutput 把超长工具输出截断为提示文本（保持合法 JSON），避免撑爆模型上下文。
func TruncateOutput(raw json.RawMessage) json.RawMessage {
	if len(raw) <= maxToolOutputBytes {
		return raw
	}
	msg := map[string]any{
		"truncated": true,
		"message":   "工具返回内容过长已截断，请缩小 page_size 或增加过滤条件后重试",
		"preview":   string(raw[:1024]),
	}
	out, _ := json.Marshal(msg)
	return out
}

// filterTools 按用户有效授权过滤工具目录；超管直通全集。allowed 为授权 code 集合。
func filterTools(catalog ToolCatalog, isAdmin bool, allowed []string) []ToolSpec {
	if isAdmin {
		return append([]ToolSpec(nil), catalog...)
	}
	set := make(map[string]struct{}, len(allowed))
	for _, code := range allowed {
		set[code] = struct{}{}
	}
	out := make([]ToolSpec, 0, len(allowed))
	for _, spec := range catalog {
		if _, ok := set[spec.Code]; ok {
			out = append(out, spec)
		}
	}
	return out
}
