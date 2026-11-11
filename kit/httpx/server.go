package httpx

import (
	"log/slog"
	"regexp"
	"time"

	"buf.build/go/protovalidate"
	"github.com/go-kratos/kratos/contrib/otel/v3/tracing"
	"github.com/go-kratos/kratos/v3/log"
	kchttp "github.com/go-kratos/kratos/v3/transport/http"
	"google.golang.org/protobuf/proto"
)

// 连接级超时（Kratos 默认全不设，仅有请求 ctx 超时——覆盖不到请求头/请求体的网络
// 读取阶段）：ReadHeaderTimeout 防慢速头部攻击（slowloris）；IdleTimeout 回收空闲 keep-alive。
const (
	ReadHeaderTimeout = 10 * time.Second
	IdleTimeout       = 120 * time.Second
)

// HardenServer 对 kratos HTTP Server 设连接级超时（须在 NewServer 之后调用）。
func HardenServer(srv *kchttp.Server) {
	srv.ReadHeaderTimeout = ReadHeaderTimeout
	srv.IdleTimeout = IdleTimeout
}

// ProtoValidate 边界校验钩子：proto.Message 按 (buf.validate.field) 规则校验。
func ProtoValidate(v any) error {
	if msg, ok := v.(proto.Message); ok {
		if err := protovalidate.Validate(msg); err != nil {
			return err
		}
	}
	return nil
}

// sensitiveFieldPattern 匹配 proto text 请求日志中的凭据类字段（kratos logging
// 中间件把整个请求拼成单个字符串挂在 args 键下，按键名过滤穿透不了字符串内容）。
// 黑名单模式：宁可误打码不可漏打码。
var sensitiveFieldPattern = regexp.MustCompile(`(?i)\b(password|refresh_token|access_token|secret|sign_secret|authorization|token|signature):"[^"]*"`)

// sensitiveHeaderPattern 匹配 map<string,string> 形态的凭据类请求头条目
// （如 webhook config.headers 的 `headers:{key:"Authorization" value:"…"}`）。
var sensitiveHeaderPattern = regexp.MustCompile(`(?i)(key:"(?:authorization|proxy-authorization|cookie|x-api-key|x-auth-token|api-key)"\s*value:)"[^"]*"`)

// MaskSensitiveAttr slog.HandlerOptions.ReplaceAttr：args 属性内敏感字段统一替换为 ***。
func MaskSensitiveAttr(groups []string, a slog.Attr) slog.Attr {
	if len(groups) != 0 || a.Key != "args" || a.Value.Kind() != slog.KindString {
		return a
	}
	masked := sensitiveFieldPattern.ReplaceAllString(a.Value.String(), `$1:"***"`)
	masked = sensitiveHeaderPattern.ReplaceAllString(masked, `$1"***"`)
	a.Value = slog.StringValue(masked)
	return a
}

// NewLogger 构造服务统一的 slog JSON 日志（经 Kratos 包装）：敏感字段在 handler
// 构建处统一脱敏（FilterKey 按键过滤 + ReplaceAttr 按内容打码），附加 trace_id/span_id
// 与 service.id/name/version。
func NewLogger(w slog.Handler, id, name, version string) *slog.Logger {
	return log.NewLogger(
		w,
		log.WithFilter(log.FilterKey("password", "token", "secret", "sign_secret", "authorization")),
		log.WithExtractor(tracing.TraceAttrs),
	).With(
		slog.String("service.id", id),
		slog.String("service.name", name),
		slog.String("service.version", version),
	)
}

// JSONHandler 标准 JSON handler（AddSource、Info 级、挂 MaskSensitiveAttr）。
func JSONHandler(w interface{ Write([]byte) (int, error) }) slog.Handler {
	return slog.NewJSONHandler(w, &slog.HandlerOptions{AddSource: true, Level: slog.LevelInfo, ReplaceAttr: MaskSensitiveAttr})
}
