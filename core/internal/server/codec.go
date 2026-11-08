package server

import (
	"bytes"
	"io"
	"net/http"
	"strings"

	"github.com/go-kratos/kratos/v3/encoding"
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

// decodeRequest 请求解码：proto.Message 且 Content-Type 为 JSON（或未指定）时
// 改走 protojson，保证 proto 的非导出字段能被正确填充；
// HttpBody（原始字节）与其它 Content-Type 一律交回默认解码器。
func decodeRequest(r *http.Request, v any) error {
	switch v.(type) {
	case *httpbody.HttpBody, **httpbody.HttpBody:
		return kchttp.DefaultRequestDecoder(r, v)
	}
	if msg, ok := v.(proto.Message); ok && isJSONType(r.Header.Get("Content-Type")) {
		data, err := io.ReadAll(r.Body)
		if err != nil {
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
	return kchttp.DefaultRequestDecoder(r, v)
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
