// Package httpx 收纳 Kratos HTTP 服务器的共享装配：protojson 编解码、请求体上限、
// protovalidate 钩子、日志敏感字段掩码、连接级超时加固、slog 日志装配。
package httpx

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/go-kratos/kratos/v3/encoding"
	kerrors "github.com/go-kratos/kratos/v3/errors"
	kchttp "github.com/go-kratos/kratos/v3/transport/http"

	// 显式注册 v3 的两类 JSON 编解码器：encoding/json 负责普通 Go 值，
	// encoding/protojson 负责 proto.Message。禁止再注册 contrib 兼容编解码器（双 codec 覆盖）。
	_ "github.com/go-kratos/kratos/v3/encoding/json"
	_ "github.com/go-kratos/kratos/v3/encoding/protojson"

	"google.golang.org/genproto/googleapis/api/httpbody"
	"google.golang.org/protobuf/proto"
)

// MaxRequestBodyBytes HTTP 请求体上限（8 MiB）：Kratos 默认不限制且解码器整包
// io.ReadAll，protovalidate 的字段级长度规则在整包读入后才生效——无上限则任意匿名
// 连接可用超大 body 耗尽内存。取最大合法请求（2 MiB 二进制经 JSON base64 膨胀）约 3 倍。
const MaxRequestBodyBytes int64 = 8 << 20

// LimitRequestBody kratos HTTP Filter：路由前把 body 包成 MaxBytesReader。
func LimitRequestBody(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil && r.Body != http.NoBody {
			r.Body = http.MaxBytesReader(w, r.Body, MaxRequestBodyBytes)
		}
		next.ServeHTTP(w, r)
	})
}

// ErrBadRequest 将解码/校验错误包装为带 code 的结构化错误（reason CODEC）。
func ErrBadRequest(err error) error {
	return kerrors.BadRequest("CODEC", err.Error())
}

func errRequestTooLarge(err error) error {
	return kerrors.New(http.StatusRequestEntityTooLarge, "REQUEST_TOO_LARGE", err.Error())
}

// EncodeResponse 响应编码：proto.Message 且客户端协商 JSON（或未指定 Accept）时走
// protojson；HttpBody / Redirector / 非 proto 值 / 显式其它 codec 交回默认编码器。
func EncodeResponse(w http.ResponseWriter, r *http.Request, v any) error {
	if v == nil {
		return nil
	}
	switch v.(type) {
	case *httpbody.HttpBody, kchttp.Redirector:
		return kchttp.DefaultResponseEncoder(w, r, v)
	}
	if msg, ok := v.(proto.Message); ok && negotiatesJSON(r.Header.Values("Accept")) {
		return writeProtoJSON(w, msg)
	}
	return kchttp.DefaultResponseEncoder(w, r, v)
}

// DecodeRequest 请求解码：proto.Message 且 Content-Type 为 JSON（或未指定）时走
// protojson；HttpBody 与其它 Content-Type 交回默认解码器。body 超限返回 413。
func DecodeRequest(r *http.Request, v any) error {
	switch v.(type) {
	case *httpbody.HttpBody, **httpbody.HttpBody:
		return translateBodyLimit(kchttp.DefaultRequestDecoder(r, v))
	}
	if msg, ok := v.(proto.Message); ok && isJSONType(r.Header.Get("Content-Type")) {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				return errRequestTooLarge(err)
			}
			return ErrBadRequest(err)
		}
		// 重置 body，保证后续读取（如 HMAC 中间件）看到一致内容。
		r.Body = io.NopCloser(bytes.NewReader(data))
		if len(data) == 0 {
			return nil
		}
		// 文本里的 NUL 字符（JSON 转义 \u0000）解码后落到 PostgreSQL 会被 22021 拒绝、对外成 500：在入口
		// 统一按 400 拒绝。数据库的 text / varchar / jsonb 都存不下它，不存在能正常保存它的字段。
		if hasNULEscape(data) {
			return ErrBadRequest(errors.New(`text must not contain NUL characters (\u0000)`))
		}
		if err := encoding.GetCodec("protojson").Unmarshal(data, msg); err != nil {
			return ErrBadRequest(err)
		}
		return nil
	}
	return translateBodyLimit(kchttp.DefaultRequestDecoder(r, v))
}

// hasNULEscape 报告 JSON 文本里是否有 NUL 转义（\u0000）。只有「奇偶性上真正起转义作用」的反斜杠才算：
// 偶数个连续反斜杠后的 \u0000 是转义；`"\\u0000"`（两个反斜杠）表示字面文本，不是。
func hasNULEscape(b []byte) bool {
	const esc = `\u0000`
	for from := 0; ; {
		j := bytes.Index(b[from:], []byte(esc))
		if j < 0 {
			return false
		}
		start := from + j // 本次匹配到的第一个反斜杠
		prev := 0
		for start-prev-1 >= 0 && b[start-prev-1] == '\\' {
			prev++
		}
		if prev%2 == 0 {
			return true
		}
		from = start + len(esc)
	}
}

// translateBodyLimit 默认解码器把 io.ReadAll 错误包成 400 且无法 errors.As 到
// *http.MaxBytesError，只能按 net/http 固定文案识别并翻译为 413。
func translateBodyLimit(err error) error {
	if err == nil {
		return nil
	}
	if ke := kerrors.FromError(err); ke != nil && strings.Contains(ke.Message, "http: request body too large") {
		return errRequestTooLarge(errors.New(ke.Message))
	}
	return err
}

func writeProtoJSON(w http.ResponseWriter, msg proto.Message) error {
	data, err := encoding.GetCodec("protojson").Marshal(msg)
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/json")
	_, err = w.Write(data)
	return err
}

// negotiatesJSON 未携带 Accept 或显式 json 均视为 JSON；显式要求已注册的非 json codec 返回 false。
func negotiatesJSON(accepts []string) bool {
	for _, accept := range accepts {
		subtype := contentSubtype(accept)
		if subtype == "" || subtype == "json" {
			return true
		}
		if encoding.GetCodec(subtype) != nil {
			return false
		}
	}
	return true
}

func isJSONType(contentType string) bool {
	subtype := contentSubtype(contentType)
	return subtype == "" || subtype == "json"
}

func contentSubtype(contentType string) string {
	ct := strings.TrimSpace(strings.Split(contentType, ";")[0])
	if i := strings.IndexByte(ct, '/'); i >= 0 {
		ct = ct[i+1:]
	}
	return strings.ToLower(strings.TrimSpace(ct))
}
