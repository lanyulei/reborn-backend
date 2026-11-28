package promclient

// 标签 / 指标名读取：服务 PromQL 编辑器的自动补全（指标名、标签名、标签值）。
// 全部走 Prometheus 兼容的读接口（VictoriaMetrics 同样支持）：
//   - <base>/api/v1/labels              → 标签名列表
//   - <base>/api/v1/label/<name>/values → 指定标签的值列表（__name__ 即指标名）
// 响应外层与查询接口同构（status / data: [string...]）；复用同一套认证、自定义请求头、
// 超时、SSRF 地址守卫与 16MiB 响应上限，不跟随跳转。
// limit 为客户端截断上限（上游如支持 limit 参数会一并带上；不支持的旧版本会忽略该参数，
// 由本客户端截断兜底），≤0 表示不截断。

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
)

// labelNameRe Prometheus 标签名（含指标名 __name__）的合法字符集；同时防路径穿越。
var labelNameRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// MetricNames 列出数据源的指标名（上游按字典序返回）。失败返回 *Error。
func (c *Client) MetricNames(ctx context.Context, limit int) ([]string, error) {
	return c.labelValues(ctx, "__name__", limit)
}

// LabelNames 列出数据源的全部标签名。失败返回 *Error。
func (c *Client) LabelNames(ctx context.Context, limit int) ([]string, error) {
	return c.getStrings(ctx, c.apiBase+"/labels", limit)
}

// LabelValues 列出指定标签的值；label 名非法返回 ErrTypeBadData。失败返回 *Error。
func (c *Client) LabelValues(ctx context.Context, label string, limit int) ([]string, error) {
	if !labelNameRe.MatchString(label) {
		return nil, &Error{ErrorType: ErrTypeBadData, Message: "invalid label name"}
	}
	return c.labelValues(ctx, label, limit)
}

func (c *Client) labelValues(ctx context.Context, label string, limit int) ([]string, error) {
	return c.getStrings(ctx, c.apiBase+"/label/"+url.PathEscape(label)+"/values", limit)
}

// getStrings 执行 GET 请求并解码 `{status: "success", data: [string...]}` 形态的响应。
// 非 2xx 与 error 形态的响应沿用 statusError / apiError 的错误解析；limit > 0 时在客户端截断。
func (c *Client) getStrings(ctx context.Context, endpoint string, limit int) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, &Error{ErrorType: ErrTypeNetwork, Message: "build request failed"}
	}
	for k, v := range c.header {
		req.Header[k] = v
	}
	req.Header.Set("Accept", "application/json")
	if limit > 0 && limit <= 10000 {
		// Prometheus ≥3.0 与 VictoriaMetrics 支持 limit 参数；旧版 Prometheus 忽略未知参数。
		req.URL.RawQuery = "limit=" + strconv.Itoa(limit)
	}

	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, c.transportError(ctx, err, 0)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		return nil, &Error{ErrorType: ErrTypeRedirect, Status: resp.StatusCode, Message: "upstream answered with a redirect; redirects are not followed"}
	}
	body := &cappedReader{r: resp.Body, n: c.maxBody}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, c.statusError(ctx, resp.StatusCode, body)
	}

	var payload struct {
		Status    string   `json:"status"`
		ErrorType string   `json:"errorType"`
		Error     string   `json:"error"`
		Data      []string `json:"data"`
	}
	if err := json.NewDecoder(body).Decode(&payload); err != nil {
		return nil, c.decodeError(ctx, err, resp.StatusCode)
	}
	switch payload.Status {
	case "success":
	case "error":
		return nil, c.apiError(resp.StatusCode, &apiResponse{Status: payload.Status, ErrorType: payload.ErrorType, Error: payload.Error})
	default:
		return nil, &Error{ErrorType: ErrTypeBadResponse, Status: resp.StatusCode, Message: "unexpected response: missing status"}
	}
	names := payload.Data
	if limit > 0 && len(names) > limit {
		names = names[:limit]
	}
	return names, nil
}
