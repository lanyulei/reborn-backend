package httpx

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-kratos/kratos/v3/errors"
	"google.golang.org/genproto/googleapis/api/httpbody"
	"google.golang.org/protobuf/types/known/structpb"
)

func sample() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{"username": "alice", "gender": "female"})
	return s
}

func errorCode(err error) int32 {
	if se := errors.FromError(err); se != nil {
		return se.Code
	}
	return 0
}

// 标准库 json 对 proto.Message 的非导出字段会序列化为空对象，protojson 通路必须输出真实字段。
func TestEncodeResponse_ProtoJSON(t *testing.T) {
	t.Parallel()
	for _, accept := range []string{"", "application/json", "application/json; charset=utf-8"} {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/x", nil)
		if accept != "" {
			r.Header.Set("Accept", accept)
		}
		if err := EncodeResponse(w, r, sample()); err != nil {
			t.Fatalf("accept=%q error = %v", accept, err)
		}
		if body := w.Body.String(); !strings.Contains(body, `"username":"alice"`) || w.Header().Get("Content-Type") != "application/json" {
			t.Fatalf("accept=%q body = %q ct=%q", accept, body, w.Header().Get("Content-Type"))
		}
	}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/x", nil)
	r.Header.Set("Accept", "application/proto")
	if err := EncodeResponse(w, r, sample()); err != nil || w.Header().Get("Content-Type") == "application/json" {
		t.Fatalf("explicit proto Accept must not force protojson: %v %q", err, w.Header().Get("Content-Type"))
	}
	w = httptest.NewRecorder()
	if err := EncodeResponse(w, httptest.NewRequest(http.MethodGet, "/x", nil), map[string]int{"n": 1}); err != nil || !strings.Contains(w.Body.String(), `"n":1`) {
		t.Fatalf("non-proto value: %v %q", err, w.Body.String())
	}
	if err := EncodeResponse(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/x", nil), nil); err != nil {
		t.Fatal(err)
	}
}

func TestDecodeRequest(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(`{"username":"alice"}`))
	r.Header.Set("Content-Type", "application/json")
	in := &structpb.Struct{}
	if err := DecodeRequest(r, in); err != nil || in.GetFields()["username"].GetStringValue() != "alice" {
		t.Fatalf("decode = %v %v", in, err)
	}
	// body 回放：解码后仍可整读（HMAC 中间件依赖）。
	if data, _ := io.ReadAll(r.Body); string(data) != `{"username":"alice"}` {
		t.Fatalf("body not replayable: %q", data)
	}
	if err := DecodeRequest(httptest.NewRequest(http.MethodPost, "/x", nil), &structpb.Struct{}); err != nil {
		t.Fatalf("empty body: %v", err)
	}
	r = httptest.NewRequest(http.MethodPost, "/x", strings.NewReader("{invalid"))
	r.Header.Set("Content-Type", "application/json")
	if err := DecodeRequest(r, &structpb.Struct{}); errorCode(err) != 400 {
		t.Fatalf("invalid json: %v", err)
	}
	if contentSubtype("application/json; charset=utf-8") != "json" || contentSubtype("") != "" || contentSubtype("text/PLAIN") != "plain" {
		t.Fatal("contentSubtype drifted")
	}
}

// body 超过上限：解码返回 413；上限内不受影响；HttpBody 原始字节路径同样 413。
func TestLimitRequestBody(t *testing.T) {
	t.Parallel()
	var decodeErr error
	h := LimitRequestBody(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		decodeErr = DecodeRequest(r, &structpb.Struct{})
	}))
	big := `{"username":"` + strings.Repeat("a", int(MaxRequestBodyBytes)+1) + `"}`
	r := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(big))
	r.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(httptest.NewRecorder(), r)
	if code := errorCode(decodeErr); code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body: %v (code %d)", decodeErr, code)
	}
	r = httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(`{"username":"alice"}`))
	r.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(httptest.NewRecorder(), r)
	if decodeErr != nil {
		t.Fatalf("small body: %v", decodeErr)
	}
	var body httpbody.HttpBody
	h = LimitRequestBody(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { decodeErr = DecodeRequest(r, &body) }))
	r = httptest.NewRequest(http.MethodPost, "/upload", bytes.NewReader(make([]byte, MaxRequestBodyBytes+1)))
	r.Header.Set("Content-Type", "application/octet-stream")
	h.ServeHTTP(httptest.NewRecorder(), r)
	if code := errorCode(decodeErr); code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized raw body: %v (code %d)", decodeErr, code)
	}
}
