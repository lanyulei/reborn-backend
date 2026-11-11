package data

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/assistant"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	"github.com/lanyulei/reborn/backend/kit/idgen"
)

var _ biz.ToolInvoker = (*LoopbackInvoker)(nil)

// 回环执行器缺省参数。
const (
	defaultLoopbackAddr   = "http://127.0.0.1:8010"
	defaultToolTimeout    = 5 * time.Second
	loopbackConnectTimout = 3 * time.Second
	// maxLoopbackBodyBytes 回环响应体读取上限（防异常接口撑爆内存）。
	maxLoopbackBodyBytes = 4 << 20
)

// pathVarPattern 路径模板占位 {field} / {field=**}。
var pathVarPattern = regexp.MustCompile(`\{[a-zA-Z0-9_.]+(?:=[^}]*)?\}`)

// LoopbackInvoker 是 biz.ToolInvoker 的 HTTP 回环实现：以用户的 Bearer 调用
// 本服务现有接口，权限由既有 auth/authz/bizGuard 判定；不重试（写操作不可
// 重放）、禁跳转、固定超时。
type LoopbackInvoker struct {
	client  *http.Client
	baseURL func() string
}

// NewLoopbackInvoker new a LoopbackInvoker（配置回环地址）。
func NewLoopbackInvoker(c *conf.Assistant) *LoopbackInvoker {
	base := strings.TrimRight(strings.TrimSpace(c.GetLoopbackAddr()), "/")
	if base == "" {
		base = defaultLoopbackAddr
	}
	timeout := defaultToolTimeout
	if d := c.GetToolTimeout(); d != nil && d.AsDuration() > 0 {
		timeout = d.AsDuration()
	}
	return NewLoopbackInvokerWithResolver(func() string { return base }, timeout)
}

// NewLoopbackInvokerWithResolver 以动态地址解析器构造（测试脚手架：httptest
// 地址在服务器启动后才可知）。
func NewLoopbackInvokerWithResolver(baseURL func() string, timeout time.Duration) *LoopbackInvoker {
	return &LoopbackInvoker{
		baseURL: baseURL,
		client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxIdleConns: 32, MaxIdleConnsPerHost: 16, IdleConnTimeout: 90 * time.Second,
				DialContext: (&net.Dialer{Timeout: loopbackConnectTimout}).DialContext,
			},
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
}

// Invoke implements biz.ToolInvoker。
func (l *LoopbackInvoker) Invoke(ctx context.Context, spec biz.ToolSpec, input json.RawMessage, bearer string) (*biz.InvokeResult, error) {
	fields := map[string]json.RawMessage{}
	if len(bytes.TrimSpace(input)) > 0 {
		if err := json.Unmarshal(input, &fields); err != nil {
			return &biz.InvokeResult{StatusCode: http.StatusBadRequest, Reason: "INVALID_ARGUMENT", Message: "工具参数不是合法的 JSON 对象"}, nil
		}
	}
	path, err := bindPath(spec, fields)
	if err != nil {
		return &biz.InvokeResult{StatusCode: http.StatusBadRequest, Reason: "INVALID_ARGUMENT", Message: err.Error()}, nil
	}
	target := strings.TrimRight(l.baseURL(), "/") + path
	var body io.Reader
	if spec.BodyAll {
		if len(bytes.TrimSpace(input)) == 0 {
			input = json.RawMessage("{}")
		}
		body = bytes.NewReader(input)
	} else if q := encodeQuery(fields); q != "" {
		target += "?" + q
	}
	req, err := http.NewRequestWithContext(ctx, spec.HTTPMethod, target, body)
	if err != nil {
		return nil, fmt.Errorf("build loopback request: %w", err)
	}
	if bearer != "" {
		req.Header.Set("Authorization", bearer)
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if rid, err := idgen.NewID(); err == nil {
		req.Header.Set("X-Request-Id", "assistant-"+rid)
	}
	resp, err := l.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("loopback %s %s: %w", spec.HTTPMethod, spec.PathTemplate, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxLoopbackBodyBytes))
	if err != nil {
		return nil, fmt.Errorf("read loopback response: %w", err)
	}
	res := &biz.InvokeResult{StatusCode: resp.StatusCode}
	if json.Valid(raw) {
		res.Body = json.RawMessage(raw)
	} else if len(raw) > 0 {
		b, _ := json.Marshal(map[string]string{"raw": string(raw)})
		res.Body = b
	} else {
		res.Body = json.RawMessage("{}")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var ke struct {
			Reason  string `json:"reason"`
			Message string `json:"message"`
		}
		_ = json.Unmarshal(raw, &ke)
		res.Reason, res.Message = ke.Reason, ke.Message
		if res.Reason == "" {
			res.Reason = http.StatusText(resp.StatusCode)
		}
	}
	return res, nil
}

// bindPath 以入参填充路径占位（按出现顺序对应 spec.PathFields），已用字段从
// fields 删除（query 绑定不再重复）。占位值缺失报错。
func bindPath(spec biz.ToolSpec, fields map[string]json.RawMessage) (string, error) {
	idx := 0
	var bindErr error
	path := pathVarPattern.ReplaceAllStringFunc(spec.PathTemplate, func(string) string {
		if idx >= len(spec.PathFields) {
			bindErr = fmt.Errorf("路径参数数量不匹配：%s", spec.PathTemplate)
			return ""
		}
		name := spec.PathFields[idx]
		idx++
		raw, ok := fields[name]
		val := scalarString(raw)
		if !ok || val == "" {
			bindErr = fmt.Errorf("缺少路径参数 %s", name)
			return ""
		}
		delete(fields, name)
		return url.PathEscape(val)
	})
	if bindErr != nil {
		return "", bindErr
	}
	return path, nil
}

// encodeQuery 把非路径字段编码为 query：标量直出；数组逐项重复键；对象按
// 点路径展开（kratos form 解码支持 a.b=c）；null 跳过。
func encodeQuery(fields map[string]json.RawMessage) string {
	q := url.Values{}
	for k, raw := range fields {
		addQuery(q, k, raw)
	}
	return q.Encode()
}

func addQuery(q url.Values, key string, raw json.RawMessage) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return
	}
	switch trimmed[0] {
	case '[':
		var items []json.RawMessage
		if json.Unmarshal(trimmed, &items) == nil {
			for _, item := range items {
				addQuery(q, key, item)
			}
		}
	case '{':
		var obj map[string]json.RawMessage
		if json.Unmarshal(trimmed, &obj) == nil {
			for sub, item := range obj {
				addQuery(q, key+"."+sub, item)
			}
		}
	default:
		q.Add(key, scalarString(trimmed))
	}
}

// scalarString JSON 标量转字符串：字符串去引号，其它原样（数字/布尔）。
func scalarString(raw json.RawMessage) string {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return ""
	}
	if trimmed[0] == '"' {
		var s string
		if err := json.Unmarshal(trimmed, &s); err == nil {
			return s
		}
	}
	return string(trimmed)
}
