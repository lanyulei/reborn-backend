package server

import (
	"log/slog"
	"regexp"
)

// sensitiveFieldPattern 匹配 proto text 格式请求体中的凭据类字段
// （kratos logging 中间件打的 args 形如 `password:"..."`）。黑名单模式：
// 未来新增的敏感字段名一旦命中同样打码，宁可误打码不可漏打码。
var sensitiveFieldPattern = regexp.MustCompile(`(?i)\b(password|refresh_token|access_token|secret|authorization|token):"[^"]*"`)

// sensitiveHeaderPattern 匹配 proto text 中 map<string,string> 形态的凭据
// 类请求头条目（如 webhook 渠道 config.headers 的 `headers:{key:"Authorization"
// value:"Bearer …"}`）：键名命中即把 value 打码。biz 侧已拒绝这类键写入
// 渠道配置（见 biz/notify validateWebhookHeaders），此处为日志侧第二道防线。
var sensitiveHeaderPattern = regexp.MustCompile(`(?i)(key:"(?:authorization|proxy-authorization|cookie|x-api-key|x-auth-token|api-key)"\s*value:)"[^"]*"`)

// MaskSensitiveAttr 作为 slog.HandlerOptions.ReplaceAttr 挂载：将 args 属性值中
// proto text 形式的敏感字段统一替换为 "***"。kratos logging 中间件把整个请求
// 拼成单个字符串挂在 args 键下，log.FilterKey 按属性键名过滤无法穿透字符串
// 内容（AGENTS §7：日志脱敏必须在 handler 构建处统一挂载）。
func MaskSensitiveAttr(groups []string, a slog.Attr) slog.Attr {
	if len(groups) != 0 || a.Key != "args" || a.Value.Kind() != slog.KindString {
		return a
	}
	masked := sensitiveFieldPattern.ReplaceAllString(a.Value.String(), `$1:"***"`)
	masked = sensitiveHeaderPattern.ReplaceAllString(masked, `$1"***"`)
	a.Value = slog.StringValue(masked)
	return a
}
