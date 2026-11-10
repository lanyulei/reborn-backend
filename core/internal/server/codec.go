package server

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/go-kratos/kratos/v3/encoding"
	kerrors "github.com/go-kratos/kratos/v3/errors"
	kchttp "github.com/go-kratos/kratos/v3/transport/http"

	// 显式注册 v3 的两类 JSON 编解码器：encoding/json 负责普通 Go 值（标准库语义），
	// encoding/protojson 负责 proto.Message（protobuf JSON 语义）。
	// 禁止再注册 contrib/encoding/json/v3 兼容编解码器，或造成双 json codec 覆盖（AGENTS.md §8.11）。
	_ "github.com/go-kratos/kratos/v3/encoding/json"
	_ "github.com/go-kratos/kratos/v3/encoding/protojson"

	"google.golang.org/genproto/googleapis/api/httpbody"
	"google.golang.org/protobuf/proto"
)

// encodeResponse 响应编码：proto.Message 且客户端协商 JSON（或未指定 Accept）时
// 改走 protojson，避免标准库 json 对 proto 的非导出字段序列化为空对象；
// HttpBody / Redirector / 非 proto 值 / 显式其它 codec 一律交回默认编码器。
func encodeResponse(w http.ResponseWriter, r *http.Request, v any) error {
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

// maxRequestBodyBytes 是 HTTP 请求体上限（8 MiB）：Kratos 默认不限制 body
// 大小且解码器整包 io.ReadAll，protovalidate 的字段级长度规则（如 logo
// bytes ≤2 MiB）在整包读入后才生效——无上限则任意匿名连接可用超大 body
// 耗尽内存。上限取最大合法请求（2 MiB 二进制经 JSON base64 膨胀 ≈2.7 MiB）
// 的约 3 倍，正常请求零感知；gRPC 侧由其默认 4 MiB 接收上限兜底。
const maxRequestBodyBytes int64 = 8 << 20

// limitRequestBody 是 kratos HTTP Filter：在进入路由前把 body 包成
// MaxBytesReader，超限读到即报 *http.MaxBytesError 并由 net/http 关闭连接。
func limitRequestBody(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil && r.Body != http.NoBody {
			r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
		}
		next.ServeHTTP(w, r)
	})
}

// errRequestTooLarge 是 body 超限的结构化错误（413）。
func errRequestTooLarge(err error) error {
	return kerrors.New(http.StatusRequestEntityTooLarge, "REQUEST_TOO_LARGE", err.Error())
}

// decodeRequest 请求解码：proto.Message 且 Content-Type 为 JSON（或未指定）时
// 改走 protojson，保证 proto 的非导出字段能被正确填充；
// HttpBody（原始字节）与其它 Content-Type 一律交回默认解码器。
// body 超过 maxRequestBodyBytes 返回 413（limitRequestBody 已包裹 body）。
func decodeRequest(r *http.Request, v any) error {
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
			return errBadRequest(err)
		}
		// 重置 body，保证后续读取（如中间件）看到一致内容。
		r.Body = io.NopCloser(bytes.NewReader(data))
		if len(data) == 0 {
			return nil
		}
		if err := encoding.GetCodec("protojson").Unmarshal(data, msg); err != nil {
			return errBadRequest(err)
		}
		return nil
	}
	return translateBodyLimit(kchttp.DefaultRequestDecoder(r, v))
}

// translateBodyLimit 把默认解码器因 body 超限产生的 400 翻译为 413：默认
// 解码器将 io.ReadAll 错误文案包进 BadRequest，无法 errors.As 到
// *http.MaxBytesError，只能按 net/http 固定文案识别。
func translateBodyLimit(err error) error {
	if err == nil {
		return nil
	}
	if ke := kerrors.FromError(err); ke != nil && strings.Contains(ke.Message, "http: request body too large") {
		return errRequestTooLarge(errors.New(ke.Message))
	}
	return err
}

// writeProtoJSON 用 protojson 编码 proto.Message 并写出为 application/json 响应。
func writeProtoJSON(w http.ResponseWriter, msg proto.Message) error {
	data, err := encoding.GetCodec("protojson").Marshal(msg)
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/json")
	_, err = w.Write(data)
	return err
}

// negotiatesJSON 判断 Accept 协商结果是否落在 JSON：未携带 Accept（默认回退 json）
// 或显式给出 json 均视为 JSON；显式要求已注册的非 json codec 时返回 false，
// 交回默认编码器按其协商结果处理。
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

// isJSONType 判断 Content-Type 是否为 JSON（含未指定，默认按 JSON 处理）。
func isJSONType(contentType string) bool {
	subtype := contentSubtype(contentType)
	return subtype == "" || subtype == "json"
}

// contentSubtype 提取 Content-Type 的子类型：如 "application/json; charset=utf-8" -> "json"。
func contentSubtype(contentType string) string {
	ct := strings.TrimSpace(strings.Split(contentType, ";")[0])
	if i := strings.IndexByte(ct, '/'); i >= 0 {
		ct = ct[i+1:]
	}
	return strings.ToLower(strings.TrimSpace(ct))
}
