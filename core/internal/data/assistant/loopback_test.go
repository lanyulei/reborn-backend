package data

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/assistant"
)

type captured struct {
	method, path, query, auth, body string
}

// newCaptureServer 回环目标桩：记录请求并按预置状态码/体响应。
func newCaptureServer(t *testing.T, status int, respBody string) (*httptest.Server, *captured) {
	t.Helper()
	c := &captured{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		c.method, c.path, c.query, c.auth, c.body = r.Method, r.URL.Path, r.URL.RawQuery, r.Header.Get("Authorization"), string(b)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(respBody))
	}))
	t.Cleanup(ts.Close)
	return ts, c
}

func newInvoker(ts *httptest.Server) *LoopbackInvoker {
	return NewLoopbackInvokerWithResolver(func() string { return ts.URL }, 2*time.Second)
}

// 四类绑定：query（GET 无路径）、path（GET 带 {id}）、body（POST body:"*"）、
// 自定义动词（POST /{id}:replay）。
func TestLoopbackInvoker_Bindings(t *testing.T) {
	t.Parallel()
	ts, c := newCaptureServer(t, http.StatusOK, `{"items":[],"total":0}`)
	inv := newInvoker(ts)
	ctx := context.Background()

	// query
	res, err := inv.Invoke(ctx, biz.ToolSpec{HTTPMethod: "GET", PathTemplate: "/api/v1/system/user"},
		json.RawMessage(`{"page":1,"pageSize":20,"keyword":"张三","status":null,"ids":["a","b"],"range":{"start":"x"}}`), "Bearer tok")
	if err != nil || res.StatusCode != 200 {
		t.Fatalf("query invoke: res=%+v err=%v", res, err)
	}
	if c.method != "GET" || c.path != "/api/v1/system/user" || c.auth != "Bearer tok" || c.body != "" {
		t.Fatalf("query request wrong: %+v", c)
	}
	for _, want := range []string{"page=1", "pageSize=20", "keyword=%E5%BC%A0%E4%B8%89", "ids=a", "ids=b", "range.start=x"} {
		if !containsParam(c.query, want) {
			t.Fatalf("query %q missing %q", c.query, want)
		}
	}
	if containsParam(c.query, "status=") {
		t.Fatalf("null must be skipped: %q", c.query)
	}

	// path
	if _, err := inv.Invoke(ctx, biz.ToolSpec{HTTPMethod: "GET", PathTemplate: "/api/v1/system/user/{id}", PathFields: []string{"id"}},
		json.RawMessage(`{"id":"u-1/x"}`), ""); err != nil {
		t.Fatal(err)
	}
	if c.path != "/api/v1/system/user/u-1%2Fx" && c.path != "/api/v1/system/user/u-1/x" {
		t.Fatalf("path binding wrong: %q", c.path)
	}
	if c.query != "" {
		t.Fatalf("path field must not leak into query: %q", c.query)
	}

	// body
	if _, err := inv.Invoke(ctx, biz.ToolSpec{HTTPMethod: "POST", PathTemplate: "/api/v1/system/user", BodyAll: true},
		json.RawMessage(`{"username":"u","password":"p"}`), ""); err != nil {
		t.Fatal(err)
	}
	if c.method != "POST" || c.body != `{"username":"u","password":"p"}` || c.query != "" {
		t.Fatalf("body binding wrong: %+v", c)
	}

	// custom verb + path + body
	if _, err := inv.Invoke(ctx, biz.ToolSpec{HTTPMethod: "POST", PathTemplate: "/api/v1/notify/dead-letter/{id}:replay", PathFields: []string{"id"}, BodyAll: true},
		json.RawMessage(`{"id":"dl-1"}`), ""); err != nil {
		t.Fatal(err)
	}
	if c.path != "/api/v1/notify/dead-letter/dl-1:replay" || c.body != `{"id":"dl-1"}` {
		t.Fatalf("custom verb binding wrong: %+v", c)
	}
}

func TestLoopbackInvoker_Errors(t *testing.T) {
	t.Parallel()
	ts, _ := newCaptureServer(t, http.StatusForbidden, `{"code":403,"reason":"PERMISSION_DENIED","message":"permission denied"}`)
	inv := newInvoker(ts)
	res, err := inv.Invoke(context.Background(), biz.ToolSpec{HTTPMethod: "GET", PathTemplate: "/x"}, nil, "")
	if err != nil || res.StatusCode != 403 || res.Reason != "PERMISSION_DENIED" || res.Message != "permission denied" {
		t.Fatalf("403 parse wrong: res=%+v err=%v", res, err)
	}
	// 缺少路径参数：本地 400，不发请求。
	res, err = inv.Invoke(context.Background(), biz.ToolSpec{HTTPMethod: "GET", PathTemplate: "/x/{id}", PathFields: []string{"id"}}, json.RawMessage(`{}`), "")
	if err != nil || res.StatusCode != 400 || res.Reason != "INVALID_ARGUMENT" {
		t.Fatalf("missing path param: res=%+v err=%v", res, err)
	}
	// 非法 JSON 入参：本地 400。
	res, err = inv.Invoke(context.Background(), biz.ToolSpec{HTTPMethod: "GET", PathTemplate: "/x"}, json.RawMessage(`[1]`), "")
	if err != nil || res.StatusCode != 400 {
		t.Fatalf("invalid json input: res=%+v err=%v", res, err)
	}
}

func containsParam(query, want string) bool {
	for _, kv := range splitQuery(query) {
		if kv == want {
			return true
		}
	}
	return false
}

func splitQuery(q string) []string {
	var out []string
	cur := ""
	for _, ch := range q {
		if ch == '&' {
			out = append(out, cur)
			cur = ""
			continue
		}
		cur += string(ch)
	}
	return append(out, cur)
}
