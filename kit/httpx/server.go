package httpx

import (
	"fmt"
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

// sensitiveName 敏感字段名的形态：关键字（password / secret / token / 凭据类 key …）作为完整的
// 下划线或连字符分段出现，前面可带任意前缀分段、后面可带后缀——old_password、new_password、
// become_secret、client_secret、client_key、refresh_token 都算。此前要求关键字前是单词边界，
// `old_password` 这类带前缀的名字整段漏打码（修改密码的旧 / 新密码曾明文进入访问日志）。
const sensitiveName = `(?:[a-z0-9]+[_-])*(?:password|passwd|pwd|secret|token|signature|authorization|credential|client[_-]?key|private[_-]?key|api[_-]?key|access[_-]?key)[\w-]*`

var (
	// sensitiveFieldPattern 匹配 proto text 请求日志中的凭据类字符串字段（kratos logging
	// 中间件把整个请求拼成单个字符串挂在 args 键下，按键名过滤穿透不了字符串内容）。
	// 黑名单模式：宁可误打码不可漏打码。
	// 值按 proto text 的带引号字符串整体匹配（`\"` 等转义序列不终止匹配），否则
	// 含 `"` 的密码会在首个转义引号处截断、尾部明文泄漏进日志。
	sensitiveFieldPattern = regexp.MustCompile(`(?i)(\b` + sensitiveName + `):"(?:[^"\\]|\\.)*"`)
	// sensitiveEntryPattern 匹配 map<string,string> 条目里键名敏感的 value（webhook 的 headers、CMDB 的
	// attrs 这类键值对：`{key:"X-Token" value:"…"}`）。键名同样按 sensitiveName 判定（原先是 6 个
	// 键名的黑名单，X-Token、Private-Token 这类鉴权头会漏），另加 Cookie；非凭据类的请求头保留取值。
	sensitiveEntryPattern = regexp.MustCompile(`(?i)(\bkey\s*:\s*"(?:` + sensitiveName + `|cookie)"\s+value\s*:\s*)"(?:[^"\\]|\\.)*"`)
)

// MaskSensitiveAttr slog.HandlerOptions.ReplaceAttr：args 属性内敏感字段统一替换为 ***。
func MaskSensitiveAttr(groups []string, a slog.Attr) slog.Attr {
	if len(groups) != 0 || a.Key != "args" || a.Value.Kind() != slog.KindString {
		return a
	}
	masked := sensitiveFieldPattern.ReplaceAllString(a.Value.String(), `$1:"***"`)
	masked = sensitiveEntryPattern.ReplaceAllString(masked, `$1"***"`)
	// 上传类请求（头像 / logo 的 bytes 字段）会把整份文件内容写进 args：
	// 超长的直接截断，保留头部便于排查，并标注原长度。
	if len(masked) > maxLoggedArgs {
		masked = masked[:maxLoggedArgs] + fmt.Sprintf("…(truncated, %d bytes total)", len(masked))
	}
	a.Value = slog.StringValue(masked)
	return a
}

// maxLoggedArgs 单条访问日志里 args 的最大长度（字节）。
const maxLoggedArgs = 4096

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
