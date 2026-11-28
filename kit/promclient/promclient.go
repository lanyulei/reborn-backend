// Package promclient 是 Prometheus HTTP API（及兼容实现：VictoriaMetrics / Thanos / Mimir）
// 的最小客户端，供 core（测试连接、即时查询）与评估引擎（规则评估、探活、记录规则写回）共用：
// instant / range 查询，以及经 Remote Write 写入（Write，见 write.go）。
// 只依赖标准库与 protobuf 的线格式编码（protowire），无包级可变状态；每个 Client 持有一份独立的连接池。
//
// 出站安全口径（monitor 技术设计 §5.4）：仅 http / https、不跟随跳转、超时 ≤30s、
// 响应体 ≤16MiB、错误原文截断 512 字节、凭据不进错误信息。数据源地址天然指向内网，
// 本包不做私网拦截。
package promclient

import (
	"cmp"
	"container/heap"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"
)

// 认证方式（数值与 monitor_datasource.auth_type 一致）。
const (
	AuthNone   int32 = 1
	AuthBasic  int32 = 2
	AuthBearer int32 = 3
)

// 限额。
const (
	// DefaultTimeout Config.Timeout 未设置时的单次请求超时。
	DefaultTimeout = 10 * time.Second
	// MaxTimeout 单次请求超时上限，超出按此值夹紧。
	MaxTimeout = 30 * time.Second
	// MaxBodyBytes 响应体上限，超出返回 ErrTypeBodyTooLarge（不静默截断）。
	MaxBodyBytes = 16 << 20
	// MaxErrorBytes 错误信息（Error.Message）的字节上限。
	MaxErrorBytes = 512
	// MaxHeaders 自定义请求头数量上限。
	MaxHeaders = 20
)

// 结果类型（Result.Type，取自响应的 resultType）。
const (
	ResultVector = "vector"
	ResultMatrix = "matrix"
	ResultScalar = "scalar"
	ResultString = "string"
)

// 本包产生的错误类别（Error.ErrorType）；上游返回的类别（bad_data / execution /
// timeout / canceled / unavailable / internal …）原样透传。
const (
	// ErrTypeBadData PromQL 语法或参数错误（Prometheus 的 errorType，HTTP 400 / 422）。
	ErrTypeBadData = "bad_data"
	// ErrTypeTimeout 请求超时（本端超时，或上游报告查询超时）。
	ErrTypeTimeout = "timeout"
	// ErrTypeCanceled 调用方取消。
	ErrTypeCanceled = "canceled"
	// ErrTypeNetwork 连接 / TLS / 读写失败。
	ErrTypeNetwork = "network"
	// ErrTypeBlocked 连接被 Config.DialGuard 否决（解析出的地址不允许访问）。
	ErrTypeBlocked = "blocked"
	// ErrTypeRedirect 上游返回 3xx（不跟随跳转）。
	ErrTypeRedirect = "redirect"
	// ErrTypeBodyTooLarge 响应体超过 MaxBodyBytes。
	ErrTypeBodyTooLarge = "body_too_large"
	// ErrTypeBadResponse 响应不是合法的 Prometheus API JSON。
	ErrTypeBadResponse = "bad_response"
	// ErrTypeHTTP 非 2xx 且响应体不含 Prometheus 错误结构。
	ErrTypeHTTP = "http"
)

// Config 客户端配置。
type Config struct {
	// BaseURL 查询地址，不含 /api/v1/query；可带路径前缀与尾部斜杠
	// （如 http://vmselect:8481/select/0/prometheus/）。仅 http / https；不允许
	// userinfo、query、fragment。
	BaseURL string
	// AuthType 认证方式：AuthNone（0 同样视为无认证）/ AuthBasic / AuthBearer。
	AuthType int32
	// Username Basic 用户名（AuthBasic 必填）。
	Username string
	// Secret Basic 密码或 Bearer 令牌明文（AuthBearer 必填）。不会出现在任何错误信息中。
	Secret string
	// Headers 自定义请求头，须通过 ValidateHeaders；New 内部拷贝。
	Headers map[string]string
	// Timeout 单次请求总超时：≤0 取 DefaultTimeout，超过 MaxTimeout 夹紧。
	Timeout time.Duration
	// TLSSkipVerify 跳过服务端证书校验（自签证书的内网数据源）。
	TLSSkipVerify bool
	// MaxSeries 单次查询返回的序列数上限：0 = 不限；超出时只保留按标签集排序最靠前的 MaxSeries 条
	// 并置 Result.Truncated（保留哪些序列只取决于标签，与上游的返回顺序无关）。响应按流式解码，
	// 内存中至多保留 MaxSeries 条序列。
	MaxSeries int
	// WriteURL Remote Write 的完整写入地址（如 http://vminsert:8480/insert/0/prometheus/api/v1/write），
	// 可选：空 = 不可写（Write 返回 ErrTypeBadData）。仅 http / https；不允许 userinfo、fragment；
	// 可带 query。写入请求沿用同一套认证、自定义请求头、超时与 DialGuard——凭据会发往该地址的主机，
	// 它与 BaseURL 不同源时是否允许沿用已保存的凭据由调用方把关。
	WriteURL string
	// DialGuard 可选的连接前检查：每次建立连接之前，以 DNS 解析后实际要连接的 IP 调用；返回非 nil
	// 即否决这次连接，查询以 ErrTypeBlocked 失败（错误信息取自返回的 error，同样会去除凭据并截断）。
	// 检查的是真正要连接的地址，而不是配置里的主机名，因此不受 DNS 重绑定影响。nil = 不检查。
	// 可能被并发调用。
	DialGuard func(ip net.IP) error
}

// Point 一个采样点。
type Point struct {
	// Time 采样时间。
	Time time.Time
	// Value 采样值；可能为 NaN / ±Inf。
	Value float64
}

// Series 一条序列：标签集 + 采样点。instant 查询每条序列恰一个点；range 查询按时间升序多个点。
type Series struct {
	// Labels 序列标签（含 __name__，如有）；scalar 结果为空 map。
	Labels map[string]string
	// Points 采样点。
	Points []Point
}

// Result 查询结果。返回值归调用方所有，可自由修改。
type Result struct {
	// Type 结果类型：ResultVector / ResultMatrix / ResultScalar / ResultString。
	Type string
	// Series 序列列表；scalar 折算为一条无标签序列；string 结果恒为空。
	Series []Series
	// Truncated 序列数超过 Config.MaxSeries：Series 只保留了按标签集排序最靠前的 MaxSeries 条
	// （并按该顺序排列；未截断时保持上游的返回顺序）。
	Truncated bool
	// Partial 上游声明结果不完整（VictoriaMetrics 集群版的 isPartial：部分存储节点不可用）。
	// 与 Truncated 一样，缺失的序列不代表「不存在」。
	Partial bool
	// Warnings 上游附带的告警信息（每条截断至 MaxErrorBytes）。
	Warnings []string
}

// Error 查询失败。Message 已去除凭据并截断至 MaxErrorBytes。
type Error struct {
	// ErrorType 错误类别：上游的 errorType 或本包的 ErrType* 常量。
	ErrorType string
	// Message 错误说明。
	Message string
	// Status HTTP 状态码；未收到响应时为 0。
	Status int

	cause error
}

// Error implements error.
func (e *Error) Error() string {
	if e.Status != 0 {
		return fmt.Sprintf("promclient: %s (http %d): %s", e.ErrorType, e.Status, e.Message)
	}
	return fmt.Sprintf("promclient: %s: %s", e.ErrorType, e.Message)
}

// Unwrap 仅在超时 / 取消时返回 context.DeadlineExceeded / context.Canceled，
// 其余底层错误不外露（可能携带地址细节）。
func (e *Error) Unwrap() error { return e.cause }

// BadQuery 报告错误是否由查询本身引起（PromQL 语法 / 参数错误），而非上游故障。
func (e *Error) BadQuery() bool {
	return e.ErrorType == ErrTypeBadData || e.Status == http.StatusBadRequest || e.Status == http.StatusUnprocessableEntity
}

// Client Prometheus 查询客户端，可并发使用。零值不可用，须经 New 构造。
type Client struct {
	hc        *http.Client
	transport *http.Transport
	queryURL  string
	rangeURL  string
	writeURL  string
	apiBase   string
	header    http.Header
	secret    string
	timeout   time.Duration
	maxSeries int
	maxBody   int64
}

// deniedHeaders 不允许自定义的请求头：凭据、路由与逐跳头，以及由客户端自身控制的头。
var deniedHeaders = map[string]struct{}{
	"authorization":     {},
	"cookie":            {},
	"set-cookie":        {},
	"host":              {},
	"content-length":    {},
	"content-type":      {},
	"content-encoding":  {},
	"accept-encoding":   {},
	"connection":        {},
	"keep-alive":        {},
	"transfer-encoding": {},
	"te":                {},
	"trailer":           {},
	"upgrade":           {},
	"expect":            {},
}

// ValidateHeaders 校验自定义请求头（core 写入数据源与 New 共用同一规则）：至多 MaxHeaders 个；
// 名称须为合法 HTTP token（≤64 字节），大小写不敏感去重；拒绝 Authorization / Cookie /
// Host / Proxy-* / Content-Length / Content-Type 及逐跳头；值 ≤1024 字节且不含控制字符
// （杜绝 CR/LF 注入）。
func ValidateHeaders(headers map[string]string) error {
	if len(headers) > MaxHeaders {
		return fmt.Errorf("promclient: too many headers (max %d)", MaxHeaders)
	}
	seen := make(map[string]struct{}, len(headers))
	for name, value := range headers {
		if name == "" || len(name) > 64 || !validToken(name) {
			return errors.New("promclient: invalid header name")
		}
		lower := strings.ToLower(name)
		if _, ok := deniedHeaders[lower]; ok || strings.HasPrefix(lower, "proxy-") {
			return fmt.Errorf("promclient: header %q is not allowed", name)
		}
		if _, dup := seen[lower]; dup {
			return fmt.Errorf("promclient: duplicate header %q", name)
		}
		seen[lower] = struct{}{}
		if len(value) > 1024 || !validHeaderValue(value) {
			return fmt.Errorf("promclient: invalid value for header %q", name)
		}
	}
	return nil
}

// validToken RFC 7230 token。
func validToken(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case strings.IndexByte("!#$%&'*+-.^_`|~", c) >= 0:
		default:
			return false
		}
	}
	return true
}

func validHeaderValue(s string) bool {
	for i := 0; i < len(s); i++ {
		if c := s[i]; (c < 0x20 && c != '\t') || c == 0x7f {
			return false
		}
	}
	return true
}

// New 校验配置并构造客户端；不发起网络请求。
func New(cfg Config) (*Client, error) {
	u, err := url.Parse(strings.TrimSpace(cfg.BaseURL))
	if err != nil {
		// 不回显解析错误：其中包含原始地址（可能带 userinfo）。
		return nil, errors.New("promclient: invalid base url")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, errors.New("promclient: base url scheme must be http or https")
	}
	if u.User != nil {
		return nil, errors.New("promclient: base url must not contain credentials")
	}
	if u.Hostname() == "" || u.Opaque != "" {
		return nil, errors.New("promclient: base url host is required")
	}
	if u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return nil, errors.New("promclient: base url must not contain query or fragment")
	}
	if err := ValidateHeaders(cfg.Headers); err != nil {
		return nil, err
	}
	header := make(http.Header, len(cfg.Headers)+2)
	for k, v := range cfg.Headers {
		header.Set(k, v)
	}
	switch cfg.AuthType {
	case 0, AuthNone:
	case AuthBasic:
		if cfg.Username == "" {
			return nil, errors.New("promclient: basic auth requires username")
		}
		req := &http.Request{Header: header}
		req.SetBasicAuth(cfg.Username, cfg.Secret)
	case AuthBearer:
		if cfg.Secret == "" || !validHeaderValue(cfg.Secret) {
			return nil, errors.New("promclient: bearer auth requires a valid token")
		}
		header.Set("Authorization", "Bearer "+cfg.Secret)
	default:
		return nil, errors.New("promclient: unsupported auth type")
	}
	if cfg.MaxSeries < 0 {
		return nil, errors.New("promclient: max series must not be negative")
	}
	writeURL := ""
	if strings.TrimSpace(cfg.WriteURL) != "" {
		if writeURL, err = parseWriteURL(cfg.WriteURL); err != nil {
			return nil, err
		}
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	if timeout > MaxTimeout {
		timeout = MaxTimeout
	}

	base := strings.TrimRight(u.Path, "/")
	u.RawPath = ""
	u.Path = base + "/api/v1"
	apiBase := u.String()
	u.Path = base + "/api/v1/query"
	queryURL := u.String()
	u.Path = base + "/api/v1/query_range"
	rangeURL := u.String()

	dialer := &net.Dialer{Timeout: min(timeout, 5*time.Second), KeepAlive: 30 * time.Second}
	if guard := cfg.DialGuard; guard != nil {
		// Control 在解析之后、connect 之前对每个候选地址调用：address 恒为「IP:端口」。
		dialer.Control = func(_, address string, _ syscall.RawConn) error {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return &blockedError{reason: "unrecognized remote address"}
			}
			if i := strings.IndexByte(host, '%'); i >= 0 {
				host = host[:i] // IPv6 zone
			}
			ip := net.ParseIP(host)
			if ip == nil {
				return &blockedError{reason: "unrecognized remote address"}
			}
			if err := guard(ip); err != nil {
				return &blockedError{reason: err.Error()}
			}
			return nil
		}
	}
	transport := &http.Transport{
		// 数据源在内网直连：不读取环境变量代理，行为不随部署环境漂移。
		Proxy:                  nil,
		DialContext:            dialer.DialContext,
		ForceAttemptHTTP2:      true,
		MaxIdleConns:           32,
		MaxIdleConnsPerHost:    8,
		IdleConnTimeout:        90 * time.Second,
		TLSHandshakeTimeout:    min(timeout, 5*time.Second),
		ResponseHeaderTimeout:  timeout,
		ExpectContinueTimeout:  time.Second,
		MaxResponseHeaderBytes: 64 << 10,
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			//nolint:gosec // G402：由数据源管理员显式开启，用于自签证书的内网 Prometheus；缺省 false。
			InsecureSkipVerify: cfg.TLSSkipVerify,
		},
	}
	return &Client{
		hc: &http.Client{
			Transport: transport,
			// 不跟随跳转：3xx 原样返回并作为错误处理（防止凭据被带到别的主机）。
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		transport: transport,
		queryURL:  queryURL,
		rangeURL:  rangeURL,
		apiBase:   apiBase,
		writeURL:  writeURL,
		header:    header,
		secret:    cfg.Secret,
		timeout:   timeout,
		maxSeries: cfg.MaxSeries,
		maxBody:   MaxBodyBytes,
	}, nil
}

// Close 释放空闲连接；之后客户端仍可使用（会重新建连）。
func (c *Client) Close() { c.transport.CloseIdleConnections() }

// Query 执行 instant 查询。ts 为零值时不传 time 参数（由上游取当前时间）。
// 失败返回 *Error。
func (c *Client) Query(ctx context.Context, expr string, ts time.Time) (*Result, error) {
	if strings.TrimSpace(expr) == "" {
		return nil, &Error{ErrorType: ErrTypeBadData, Message: "empty query"}
	}
	form := url.Values{"query": {expr}}
	if !ts.IsZero() {
		form.Set("time", formatTime(ts))
	}
	return c.do(ctx, c.queryURL, form)
}

// QueryRange 执行 range 查询：start ≤ end，step > 0。失败返回 *Error。
func (c *Client) QueryRange(ctx context.Context, expr string, start, end time.Time, step time.Duration) (*Result, error) {
	if strings.TrimSpace(expr) == "" {
		return nil, &Error{ErrorType: ErrTypeBadData, Message: "empty query"}
	}
	if start.IsZero() || end.IsZero() || end.Before(start) || step <= 0 {
		return nil, &Error{ErrorType: ErrTypeBadData, Message: "invalid range: require start <= end and step > 0"}
	}
	form := url.Values{
		"query": {expr},
		"start": {formatTime(start)},
		"end":   {formatTime(end)},
		"step":  {strconv.FormatFloat(step.Seconds(), 'f', -1, 64)},
	}
	return c.do(ctx, c.rangeURL, form)
}

// Ping 探活：执行 instant 查询 `1`，上游返回 success 即视为可用。
func (c *Client) Ping(ctx context.Context) error {
	_, err := c.Query(ctx, "1", time.Time{})
	return err
}

func formatTime(t time.Time) string {
	return strconv.FormatFloat(float64(t.UnixNano())/1e9, 'f', 3, 64)
}

// apiResponse Prometheus API 响应外壳（不含 data：结果由 decodeBody 流式解码）。
type apiResponse struct {
	Status    string   `json:"status"`
	ErrorType string   `json:"errorType"`
	Error     string   `json:"error"`
	Warnings  []string `json:"warnings"`
	// IsPartial VictoriaMetrics 的部分结果标记。
	IsPartial bool `json:"isPartial"`
}

// errBadResult 响应是合法 JSON 但不是可用的查询结果（采样值非法、结果类型不支持、结构不符）。
var errBadResult = errors.New("bad query result")

// point 解码 [<unix 秒>, "<值>"]。
type point Point

func (p *point) UnmarshalJSON(b []byte) error {
	var raw [2]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	sec, err := strconv.ParseFloat(string(raw[0]), 64)
	if err != nil {
		return fmt.Errorf("%w: bad sample timestamp", errBadResult)
	}
	var s string
	if err := json.Unmarshal(raw[1], &s); err != nil {
		return fmt.Errorf("%w: bad sample value", errBadResult)
	}
	v, err := strconv.ParseFloat(s, 64) // 接受 NaN / +Inf / -Inf
	if err != nil {
		return fmt.Errorf("%w: bad sample value", errBadResult)
	}
	p.Time = time.UnixMilli(int64(math.Round(sec * 1000)))
	p.Value = v
	return nil
}

type rawSeries struct {
	Metric map[string]string `json:"metric"`
	Value  *point            `json:"value"`
	Values []point           `json:"values"`
}

// errBodyTooLarge 由 cappedReader 在超限时返回。
var errBodyTooLarge = errors.New("response body too large")

// cappedReader 读取至多 n 字节，超出即报错（而非像 io.LimitReader 那样静默 EOF）。
type cappedReader struct {
	r io.Reader
	n int64
}

func (c *cappedReader) Read(p []byte) (int, error) {
	if c.n < 0 {
		return 0, errBodyTooLarge
	}
	if int64(len(p)) > c.n+1 {
		p = p[:c.n+1]
	}
	n, err := c.r.Read(p)
	c.n -= int64(n)
	if c.n < 0 {
		return 0, errBodyTooLarge
	}
	return n, err
}

func (c *Client) do(ctx context.Context, endpoint string, form url.Values) (*Result, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, &Error{ErrorType: ErrTypeNetwork, Message: "build request failed"}
	}
	for k, v := range c.header {
		req.Header[k] = v
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

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
	api, data, err := decodeBody(body, c.maxSeries)
	if err != nil {
		return nil, c.decodeError(ctx, err, resp.StatusCode)
	}
	if api.Status != "success" {
		if api.Status == "error" {
			return nil, c.apiError(resp.StatusCode, api)
		}
		return nil, &Error{ErrorType: ErrTypeBadResponse, Status: resp.StatusCode, Message: "unexpected response: missing status"}
	}
	res, err := data.result()
	if err != nil {
		return nil, c.decodeError(ctx, err, resp.StatusCode)
	}
	res.Partial = api.IsPartial
	for _, w := range api.Warnings {
		res.Warnings = append(res.Warnings, c.clean(w))
	}
	return res, nil
}

// statusError 非 2xx：响应体是 Prometheus 错误结构（status = "error"）时取其中的错误说明；否则只给出
// 状态码对应的标准说明。响应体的其余内容一律不进错误信息：数据源地址由用户填写，错误信息会经测试
// 连接 / 即时查询回显给调用方，原样带出正文等于把任意内网 HTTP 服务的响应读给他。
func (c *Client) statusError(ctx context.Context, status int, body io.Reader) error {
	raw, err := io.ReadAll(io.LimitReader(body, 64<<10))
	if err != nil && len(raw) == 0 {
		return c.decodeError(ctx, err, status)
	}
	var api apiResponse
	if json.Unmarshal(raw, &api) == nil && api.Status == "error" {
		return c.apiError(status, &api)
	}
	return &Error{ErrorType: ErrTypeHTTP, Status: status, Message: statusReason(status)}
}

// statusReason 状态码的标准说明；非标准状态码给固定文案。
func statusReason(status int) string {
	if text := http.StatusText(status); text != "" {
		return text
	}
	return "unexpected HTTP status"
}

func (c *Client) apiError(status int, api *apiResponse) error {
	typ := api.ErrorType
	if typ == "" {
		typ = ErrTypeHTTP
	}
	msg := api.Error
	if msg == "" {
		msg = statusReason(status)
	}
	return &Error{ErrorType: c.clean(typ), Status: status, Message: c.clean(msg)}
}

// 不在链路本地段里的云元数据服务地址：AWS IMDS 的 IPv6 地址、阿里云、Oracle Cloud。
var metadataAddrs = []net.IP{
	net.ParseIP("fd00:ec2::254"),
	net.ParseIP("100.100.100.200"),
	net.ParseIP("192.0.0.192"),
}

// nat64Prefix NAT64 的知名前缀 64:ff9b::/96（RFC 6052）：末 4 字节是被映射的 IPv4 地址。
var nat64Prefix = net.IPNet{IP: net.ParseIP("64:ff9b::"), Mask: net.CIDRMask(96, 128)}

// GuardLinkLocal 可直接用作 Config.DialGuard：拒绝数据源绝不该落到的地址——链路本地单播
// （169.254.0.0/16，含云元数据 169.254.169.254；fe80::/10）、不在链路本地段里的云元数据地址
// （fd00:ec2::254、100.100.100.200、192.0.0.192）、未指定地址（0.0.0.0、::）与组播；IPv4 映射的
// IPv6 地址、以及经 NAT64 知名前缀 64:ff9b::/96 映射的地址，按其内含的 IPv4 地址判定。私网与回环
// 地址放行：数据源天然部署在内网，回环是否允许由调用方在此之上自行决定。
func GuardLinkLocal(ip net.IP) error {
	if len(ip) != net.IPv4len && len(ip) != net.IPv6len {
		return errors.New("invalid address")
	}
	if len(ip) == net.IPv6len && nat64Prefix.Contains(ip) {
		if err := guardAddr(ip[12:16]); err != nil {
			return fmt.Errorf("NAT64 address %s: %w", ip, err)
		}
		return nil
	}
	return guardAddr(ip)
}

func guardAddr(ip net.IP) error {
	switch {
	case ip.IsUnspecified():
		return fmt.Errorf("unspecified address %s is not allowed", ip)
	case ip.IsMulticast():
		return fmt.Errorf("multicast address %s is not allowed", ip)
	case ip.IsLinkLocalUnicast() || slices.ContainsFunc(metadataAddrs, ip.Equal):
		return fmt.Errorf("link-local or metadata address %s is not allowed", ip)
	}
	return nil
}

// blockedError DialGuard 否决了一次连接。
type blockedError struct {
	reason string
}

func (e *blockedError) Error() string { return e.reason }

// transportError 把请求阶段的错误归类；*url.Error 只取内层原因，不回显请求地址。
func (c *Client) transportError(ctx context.Context, err error, status int) error {
	var blocked *blockedError
	if errors.As(err, &blocked) {
		return &Error{ErrorType: ErrTypeBlocked, Status: status, Message: c.clean("connection refused by address policy: " + blocked.reason)}
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded):
		return &Error{ErrorType: ErrTypeTimeout, Status: status, Message: "request timed out", cause: context.DeadlineExceeded}
	case errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled):
		return &Error{ErrorType: ErrTypeCanceled, Status: status, Message: "request canceled", cause: context.Canceled}
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return &Error{ErrorType: ErrTypeTimeout, Status: status, Message: "request timed out", cause: context.DeadlineExceeded}
	}
	var ue *url.Error
	if errors.As(err, &ue) {
		err = ue.Err
	}
	return &Error{ErrorType: ErrTypeNetwork, Status: status, Message: c.clean(err.Error())}
}

func (c *Client) decodeError(ctx context.Context, err error, status int) error {
	switch {
	case errors.Is(err, errBodyTooLarge):
		return &Error{ErrorType: ErrTypeBodyTooLarge, Status: status, Message: fmt.Sprintf("response body exceeds %d bytes", c.maxBody)}
	case ctx.Err() != nil:
		return c.transportError(ctx, err, status)
	case errors.Is(err, errBadResult):
		return &Error{ErrorType: ErrTypeBadResponse, Status: status, Message: c.clean(err.Error())}
	}
	var syn *json.SyntaxError
	var typ *json.UnmarshalTypeError
	if errors.As(err, &syn) || errors.As(err, &typ) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return &Error{ErrorType: ErrTypeBadResponse, Status: status, Message: "response is not valid Prometheus API JSON"}
	}
	return c.transportError(ctx, err, status)
}

// resultData 流式解码出的 data 部分。
type resultData struct {
	resultType string
	// seen 响应里出现过 result 键。
	seen bool
	// raw 未能流式解码的 result 原文：标量 / 字符串结果，或 result 先于 resultType 出现（合法但少见）。
	raw    json.RawMessage
	series seriesCollector
}

// decodeBody 流式解码响应：外壳字段照常解码，data.result 的序列逐条解码并交给 seriesCollector，
// 内存占用与 maxSeries 成正比而不是与响应体大小成正比。键的顺序不作假设。
func decodeBody(body io.Reader, maxSeries int) (*apiResponse, *resultData, error) {
	dec := json.NewDecoder(body)
	api, data := &apiResponse{}, &resultData{series: seriesCollector{max: maxSeries}}
	err := decodeObject(dec, false, func(key string) error {
		switch key {
		case "status":
			return dec.Decode(&api.Status)
		case "errorType":
			return dec.Decode(&api.ErrorType)
		case "error":
			return dec.Decode(&api.Error)
		case "warnings":
			return dec.Decode(&api.Warnings)
		case "isPartial":
			return dec.Decode(&api.IsPartial)
		case "data":
			return data.decode(dec)
		}
		return skipValue(dec)
	})
	if err != nil {
		return nil, nil, err
	}
	return api, data, nil
}

// decodeObject 逐键遍历一个 JSON 对象：field 负责消费该键的值。nullable 为真时 null 视为空对象。
func decodeObject(dec *json.Decoder, nullable bool, field func(key string) error) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	if tok == nil && nullable {
		return nil
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return fmt.Errorf("%w: expected a JSON object", errBadResult)
	}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return err
		}
		key, _ := tok.(string)
		if err := field(key); err != nil {
			return err
		}
	}
	_, err = dec.Token() // '}'
	return err
}

// skipValue 跳过一个 JSON 值，不为它分配整块内存。
func skipValue(dec *json.Decoder) error {
	for depth := 0; ; {
		tok, err := dec.Token()
		if err != nil {
			return err
		}
		if d, ok := tok.(json.Delim); ok {
			if d == '{' || d == '[' {
				depth++
			} else {
				depth--
			}
		}
		if depth == 0 {
			return nil
		}
	}
}

func (d *resultData) decode(dec *json.Decoder) error {
	return decodeObject(dec, true, func(key string) error {
		switch key {
		case "resultType":
			return dec.Decode(&d.resultType)
		case "result":
			d.seen = true
			if d.resultType == ResultVector || d.resultType == ResultMatrix {
				return d.series.decode(dec)
			}
			return dec.Decode(&d.raw)
		}
		return skipValue(dec)
	})
}

// result 组装查询结果（响应外壳确认 success 之后调用）。
func (d *resultData) result() (*Result, error) {
	res := &Result{Type: d.resultType}
	switch d.resultType {
	case ResultVector, ResultMatrix:
		if !d.seen {
			return nil, fmt.Errorf("%w: missing result", errBadResult)
		}
		if d.raw != nil {
			if err := d.series.decode(json.NewDecoder(strings.NewReader(string(d.raw)))); err != nil {
				return nil, fmt.Errorf("%w: decode %s result", errBadResult, d.resultType)
			}
		}
		res.Series, res.Truncated = d.series.finish()
	case ResultScalar:
		var p point
		if err := json.Unmarshal(d.raw, &p); err != nil {
			return nil, fmt.Errorf("%w: decode scalar result", errBadResult)
		}
		res.Series = []Series{{Labels: map[string]string{}, Points: []Point{Point(p)}}}
	case ResultString:
		// 字符串结果没有数值序列：只回类型，Series 为空。
	default:
		return nil, fmt.Errorf("%w: unsupported result type", errBadResult)
	}
	return res, nil
}

// keyedSeries 一条序列及其标签集的规范排序键。
type keyedSeries struct {
	key    string
	series Series
}

// seriesHeap 以排序键为序的最大堆：堆顶是当前保留集合里最靠后的一条。
type seriesHeap []keyedSeries

func (h seriesHeap) Len() int           { return len(h) }
func (h seriesHeap) Less(i, j int) bool { return h[i].key > h[j].key }
func (h seriesHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *seriesHeap) Push(any)          {}
func (h *seriesHeap) Pop() any          { return nil }

// seriesCollector 收集至多 max 条序列（max ≤ 0 = 不限）。未超限时保持到达顺序；超限后只保留
// 排序键最小的 max 条：保留哪些序列是标签集的确定函数，同一批序列无论上游以什么顺序返回、
// 无论哪个实例来查，截断后留下的都是同一批。
type seriesCollector struct {
	max       int
	series    []Series
	top       seriesHeap // 超限后启用，此后 series 不再使用
	truncated bool
}

// decode 流式解码 result 数组（null 视为空）。
func (sc *seriesCollector) decode(dec *json.Decoder) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	if tok == nil {
		return nil
	}
	if d, ok := tok.(json.Delim); !ok || d != '[' {
		return fmt.Errorf("%w: result is not an array", errBadResult)
	}
	for dec.More() {
		var row rawSeries
		if err := dec.Decode(&row); err != nil {
			return err
		}
		s := Series{Labels: row.Metric}
		if s.Labels == nil {
			s.Labels = map[string]string{}
		}
		switch {
		case row.Value != nil:
			s.Points = []Point{Point(*row.Value)}
		case len(row.Values) > 0:
			s.Points = make([]Point, len(row.Values))
			for j, p := range row.Values {
				s.Points[j] = Point(p)
			}
		default:
			continue // 原生直方图等无浮点采样的序列：跳过
		}
		sc.add(s)
	}
	_, err = dec.Token() // ']'
	return err
}

func (sc *seriesCollector) add(s Series) {
	if sc.max <= 0 || (sc.top == nil && len(sc.series) < sc.max) {
		sc.series = append(sc.series, s)
		return
	}
	if sc.top == nil {
		sc.top = make(seriesHeap, len(sc.series))
		for i := range sc.series {
			sc.top[i] = keyedSeries{key: labelKey(sc.series[i].Labels), series: sc.series[i]}
		}
		sc.series = nil
		heap.Init(&sc.top)
	}
	sc.truncated = true
	if key := labelKey(s.Labels); key < sc.top[0].key {
		sc.top[0] = keyedSeries{key: key, series: s}
		heap.Fix(&sc.top, 0)
	}
}

func (sc *seriesCollector) finish() ([]Series, bool) {
	if sc.top == nil {
		if sc.series == nil {
			sc.series = []Series{}
		}
		return sc.series, false
	}
	slices.SortFunc(sc.top, func(a, b keyedSeries) int { return cmp.Compare(a.key, b.key) })
	out := make([]Series, len(sc.top))
	for i := range sc.top {
		out[i] = sc.top[i].series
	}
	return out, sc.truncated
}

// labelKey 标签集的规范排序键：按名称排序后以 0xff 分隔拼接（0xff 不出现在合法 UTF-8 里）。
func labelKey(labels map[string]string) string {
	names := make([]string, 0, len(labels))
	n := 0
	for name, value := range labels {
		names = append(names, name)
		n += len(name) + len(value) + 2
	}
	slices.Sort(names)
	var b strings.Builder
	b.Grow(n)
	for _, name := range names {
		b.WriteString(name)
		b.WriteByte(0xff)
		b.WriteString(labels[name])
		b.WriteByte(0xff)
	}
	return b.String()
}

// cleanFullMatchLen 凭据至少这么长才在说明里做全文替换。
const cleanFullMatchLen = 8

// clean 去除凭据并截断至 MaxErrorBytes（保持合法 UTF-8）。
//
// 凭据足够长时全文替换。Basic 认证的密码、Bearer 令牌也可以很短（"e"、"1"）：这么短的字符串在普通文字里
// 太常见，逐处替换会把上游的说明改得没法读（"parse error at char 3" → "pars*** ***rror at char 3"），写进
// 任务的 last_error 与判定记录后无从排查。短凭据只遮蔽确定形态的片段——请求实际带出的 Authorization 头的值。
func (c *Client) clean(s string) string {
	if len(c.secret) >= cleanFullMatchLen {
		s = strings.ReplaceAll(s, c.secret, "***")
	} else if authz := c.header.Get("Authorization"); authz != "" {
		s = strings.ReplaceAll(s, authz, "***")
	}
	s = strings.ToValidUTF8(s, "?")
	if len(s) <= MaxErrorBytes {
		return s
	}
	cut := MaxErrorBytes
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}
