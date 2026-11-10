package server

import (
	"log/slog"
	"regexp"
)

// sensitiveFieldPattern 匹配 proto text 请求日志中的凭据类字段（ingress 请求
// 不含凭据字段，但 Authorization 头与未来字段按黑名单一并打码）。
var sensitiveFieldPattern = regexp.MustCompile(`(?i)\b(password|token|secret|authorization|signature):"[^"]*"`)

// MaskSensitiveAttr slog ReplaceAttr：args 属性内敏感字段打码。
func MaskSensitiveAttr(groups []string, a slog.Attr) slog.Attr {
	if len(groups) != 0 || a.Key != "args" || a.Value.Kind() != slog.KindString {
		return a
	}
	a.Value = slog.StringValue(sensitiveFieldPattern.ReplaceAllString(a.Value.String(), `$1:"***"`))
	return a
}
