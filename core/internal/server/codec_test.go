package server

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	v1 "github.com/lanyulei/reborn/backend/core/api/system/v1"

	"github.com/go-kratos/kratos/v3/errors"
	"google.golang.org/genproto/googleapis/api/httpbody"
)

// 核心回归：标准库 json 对 proto.Message 的非导出字段会序列化为空对象，
// protojson 通路必须输出真实字段。
func TestEncodeResponse_ProtoJSON(t *testing.T) {
	t.Parallel()
	resp := &v1.GetUserResponse{User: &v1.User{Id: "u-1", Username: "alice", Gender: "female"}}
	for _, accept := range []string{"", "application/json", "application/json; charset=utf-8"} {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/api/v1/system/user/u-1", nil)
		if accept != "" {
			r.Header.Set("Accept", accept)
		}
		if err := encodeResponse(w, r, resp); err != nil {
			t.Fatalf("accept=%q encodeResponse() error = %v", accept, err)
		}
		body := w.Body.String()
		if !strings.Contains(body, `"username":"alice"`) || !strings.Contains(body, `"gender":"female"`) {
			t.Fatalf("accept=%q body = %q, want proto-encoded fields", accept, body)
		}
		if strings.Contains(body, "password") {
			t.Fatalf("accept=%q body = %q, must not contain password", accept, body)
		}
		if ct := w.Header().Get("Content-Type"); ct != "application/json" {
			t.Fatalf("Content-Type = %q", ct)
		}
	}
}

// 显式 Accept 已注册的非 json codec 时交回默认协商（如 proto）。
func TestEncodeResponse_ExplicitNonJSONCodec(t *testing.T) {
	t.Parallel()
	resp := &v1.GetUserResponse{User: &v1.User{Id: "u-1"}}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/api/v1/system/user/u-1", nil)
	r.Header.Set("Accept", "application/proto")
	if err := encodeResponse(w, r, resp); err != nil {
		t.Fatalf("encodeResponse() error = %v", err)
	}
	if w.Header().Get("Content-Type") == "application/json" {
		t.Fatal("explicit proto Accept should not be forced to protojson")
	}
}

// 非 proto 值仍走默认编码器（标准库 json 语义）。
func TestEncodeResponse_NonProtoValue(t *testing.T) {
	t.Parallel()
	type plain struct {
		Name string `json:"name"`
	}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	if err := encodeResponse(w, r, plain{Name: "a"}); err != nil {
		t.Fatalf("encodeResponse() error = %v", err)
	}
	if got := w.Body.String(); got != `{"name":"a"}` {
		t.Fatalf("body = %q", got)
	}
}

func TestEncodeResponse_NilAndRedirector(t *testing.T) {
	t.Parallel()
	if err := encodeResponse(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil), nil); err != nil {
		t.Fatalf("nil value error = %v", err)
	}
}

func TestDecodeRequest_ProtoJSON(t *testing.T) {
	t.Parallel()
	body := `{"username":"alice","password":"plain-secret","gender":"female"}`
	for _, ct := range []string{"", "application/json", "application/json; charset=utf-8"} {
		r := httptest.NewRequest(http.MethodPost, "/api/v1/system/user", strings.NewReader(body))
		if ct != "" {
			r.Header.Set("Content-Type", ct)
		}
		in := &v1.CreateUserRequest{}
		if err := decodeRequest(r, in); err != nil {
			t.Fatalf("ct=%q decodeRequest() error = %v", ct, err)
		}
		if in.GetUsername() != "alice" || in.GetPassword() != "plain-secret" || in.GetGender() != "female" {
			t.Fatalf("ct=%q decoded = %+v", ct, in)
		}
		// body 被重置，可重复读取。
		rest, _ := io.ReadAll(r.Body)
		if !bytes.Equal(rest, []byte(body)) {
			t.Fatalf("ct=%q body not reset: %q", ct, rest)
		}
	}
}

func TestDecodeRequest_EmptyBody(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest(http.MethodPost, "/api/v1/system/user", nil)
	in := &v1.CreateUserRequest{}
	if err := decodeRequest(r, in); err != nil {
		t.Fatalf("decodeRequest() error = %v", err)
	}
	if in.GetUsername() != "" {
		t.Fatalf("decoded = %+v, want zero value", in)
	}
}

// 非法 JSON 返回带 code 的结构化错误（CODEC/BadRequest）。
func TestDecodeRequest_InvalidJSON(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest(http.MethodPost, "/api/v1/system/user", strings.NewReader("{invalid"))
	r.Header.Set("Content-Type", "application/json")
	err := decodeRequest(r, &v1.CreateUserRequest{})
	if err == nil {
		t.Fatal("decodeRequest() error = nil, want CODEC bad request")
	}
	if code := errorCode(err); code != 400 {
		t.Fatalf("error code = %d, want 400", code)
	}
}

func TestContentSubtype(t *testing.T) {
	t.Parallel()
	cases := []struct{ in, want string }{
		{"", ""},
		{"application/json", "json"},
		{"application/json; charset=utf-8", "json"},
		{"application/JSON", "json"},
		{"application/proto", "proto"},
		{"text/plain", "plain"},
	}
	for _, tc := range cases {
		if got := contentSubtype(tc.in); got != tc.want {
			t.Errorf("contentSubtype(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// errorCode 提取 Kratos 结构化错误的 code，非结构化错误返回 0。
func errorCode(err error) int32 {
	if se := errors.FromError(err); se != nil {
		return se.Code
	}
	return 0
}

// TestLimitRequestBody body 超过上限：解码返回 413 REQUEST_TOO_LARGE；
// 上限内请求不受影响。
func TestLimitRequestBody(t *testing.T) {
	t.Parallel()
	var decodeErr error
	h := limitRequestBody(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		decodeErr = decodeRequest(r, &v1.CreateUserRequest{})
	}))

	big := `{"username":"` + strings.Repeat("a", int(maxRequestBodyBytes)+1) + `"}`
	r := httptest.NewRequest(http.MethodPost, "/api/v1/system/user", strings.NewReader(big))
	r.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(httptest.NewRecorder(), r)
	if code := errorCode(decodeErr); code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body: error = %v (code %d), want 413", decodeErr, code)
	}

	r = httptest.NewRequest(http.MethodPost, "/api/v1/system/user", strings.NewReader(`{"username":"alice"}`))
	r.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(httptest.NewRecorder(), r)
	if decodeErr != nil {
		t.Fatalf("small body: error = %v, want nil", decodeErr)
	}

	// HttpBody（原始字节）路径走默认解码器，超限同样 413。
	var body httpbody.HttpBody
	h = limitRequestBody(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		decodeErr = decodeRequest(r, &body)
	}))
	r = httptest.NewRequest(http.MethodPost, "/upload", bytes.NewReader(make([]byte, maxRequestBodyBytes+1)))
	r.Header.Set("Content-Type", "application/octet-stream")
	h.ServeHTTP(httptest.NewRecorder(), r)
	if code := errorCode(decodeErr); code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized raw body: error = %v (code %d), want 413", decodeErr, code)
	}
}
