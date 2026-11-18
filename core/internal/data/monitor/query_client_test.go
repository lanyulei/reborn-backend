package data

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/kit/promclient"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// promServer 一个最小的 Prometheus 查询接口（监听回环地址）；hits 记录收到的请求数。
func promServer(t *testing.T, body string) (url string, hits *int) {
	t.Helper()
	n := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv.URL, &n
}

const vectorBody = `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up"},"value":[1700000000,"1"]}]}}`

func conn(url string) biz.Connection {
	return biz.Connection{URL: url, AuthType: mc.AuthNone, TimeoutMS: 2000}
}

// 连接前的地址检查：链路本地 / 云元数据 / 未指定 / 组播恒拒绝；回环由配置决定；私网放行。
func TestQueryClient_Guard(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		ip                  string
		allowLoop, denyLoop bool // 期望：回环允许 / 回环禁止 两种配置下是否放行
	}{
		{"169.254.169.254", false, false}, // 云元数据
		{"169.254.1.1", false, false},
		{"fe80::1", false, false},
		{"fd00:ec2::254", false, false},
		{"::ffff:169.254.169.254", false, false}, // IPv4 映射
		{"0.0.0.0", false, false},
		{"224.0.0.1", false, false},
		{"127.0.0.1", true, false},
		{"127.8.9.10", true, false},
		{"::1", true, false},
		{"::ffff:127.0.0.1", true, false},
		{"10.0.0.5", true, true},
		{"192.168.1.10", true, true},
		{"172.16.3.4", true, true},
		{"8.8.8.8", true, true},
		{"fd12:3456::1", true, true},
	} {
		ip := net.ParseIP(c.ip)
		if got := NewQueryClient(nil).guard(ip) == nil; got != c.allowLoop {
			t.Errorf("default config: %s allowed = %v, want %v", c.ip, got, c.allowLoop)
		}
		if got := NewQueryClient(&conf.Monitor{BlockLoopback: true}).guard(ip) == nil; got != c.denyLoop {
			t.Errorf("block_loopback: %s allowed = %v, want %v", c.ip, got, c.denyLoop)
		}
	}
}

// 地址字面量在校验配置时就判定（保存、测试连接的第一步），不发任何请求。
func TestQueryClient_ValidateRejectsBlockedLiteralHosts(t *testing.T) {
	t.Parallel()
	open, strict := NewQueryClient(&conf.Monitor{}), NewQueryClient(&conf.Monitor{BlockLoopback: true})
	for _, c := range []struct {
		url                  string
		wantOpen, wantStrict bool // 是否通过校验
	}{
		{"http://169.254.169.254/latest/meta-data", false, false},
		{"http://[fe80::1]:9090", false, false},
		{"http://[fd00:ec2::254]", false, false},
		{"http://0.0.0.0:9090", false, false},
		{"http://127.0.0.1:9090", true, false},
		{"http://[::1]:9090", true, false},
		{"http://localhost:9090", true, false},
		{"http://LOCALHOST.:9090", true, false},
		{"http://vm.localhost:9090", true, false},
		{"http://10.1.2.3:8428/select/0/prometheus", true, true},
		{"http://vmselect.monitoring.svc:8481", true, true}, // 主机名在连接时再按解析结果判定
	} {
		for name, q := range map[string]*QueryClient{"default": open, "block_loopback": strict} {
			want := c.wantOpen
			if q == strict {
				want = c.wantStrict
			}
			err := q.Validate(conn(c.url))
			if (err == nil) != want {
				t.Errorf("%s: Validate(%s) = %v, want ok = %v", name, c.url, err, want)
			}
			if err != nil && !kerrors.Is(err, biz.ErrDatasourceAddressBlocked) {
				t.Errorf("%s: Validate(%s) = %v, want MONITOR_DATASOURCE_ADDRESS_BLOCKED", name, c.url, err)
			}
		}
	}
	// 其余配置错误仍是 MONITOR_DATASOURCE_INVALID。
	if err := open.Validate(conn("ftp://vm:21")); !kerrors.Is(err, biz.ErrDatasourceInvalid) {
		t.Fatalf("bad scheme = %v", err)
	}
}

// 每条出站路径（即时查询、区间查询；测试连接走的也是即时查询）都带连接前检查。
func TestQueryClient_OutboundPathsHonourLoopbackPolicy(t *testing.T) {
	t.Parallel()
	url, hits := promServer(t, vectorBody)
	ctx := context.Background()
	now := time.Now()

	open := NewQueryClient(nil)
	if res, err := open.Instant(ctx, conn(url), "up", time.Time{}, 10); err != nil || len(res.Series) != 1 {
		t.Fatalf("loopback is allowed by default: %+v err=%v", res, err)
	}
	if _, err := open.Range(ctx, conn(url), "up", now.Add(-time.Minute), now, time.Second*15, 10); err == nil {
		// 向量结果不是区间查询的合法响应；这里只关心请求确实发出去了。
		t.Log("range accepted a vector body")
	}
	if *hits != 2 {
		t.Fatalf("requests sent = %d, want 2", *hits)
	}

	strict := NewQueryClient(&conf.Monitor{BlockLoopback: true})
	_, err := strict.Instant(ctx, conn(url), "up", time.Time{}, 10)
	if !kerrors.Is(err, biz.ErrDatasourceAddressBlocked) {
		t.Fatalf("instant with block_loopback = %v, want MONITOR_DATASOURCE_ADDRESS_BLOCKED", err)
	}
	_, err = strict.Range(ctx, conn(url), "up", now.Add(-time.Minute), now, time.Second*15, 10)
	if !kerrors.Is(err, biz.ErrDatasourceAddressBlocked) {
		t.Fatalf("range with block_loopback = %v, want MONITOR_DATASOURCE_ADDRESS_BLOCKED", err)
	}
	if *hits != 2 {
		t.Fatalf("a blocked address must not receive a request (hits = %d)", *hits)
	}
}

// 连接时的检查（主机名解析出的真实地址）：绕过字面量预检，直接让 promclient 带着本包的 guard 去连
// 回环地址——连接被否决、没有请求发出，错误映射为 MONITOR_DATASOURCE_ADDRESS_BLOCKED。
func TestQueryClient_DialGuardVetoesAtConnectTime(t *testing.T) {
	t.Parallel()
	url, hits := promServer(t, vectorBody)
	strict := NewQueryClient(&conf.Monitor{BlockLoopback: true})
	c, err := promclient.New(promclient.Config{BaseURL: url, Timeout: 2 * time.Second, DialGuard: strict.guard})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = queryResult(c.Query(context.Background(), "up", time.Time{}))
	if !kerrors.Is(err, biz.ErrDatasourceAddressBlocked) || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("err = %v, want MONITOR_DATASOURCE_ADDRESS_BLOCKED naming the loopback policy", err)
	}
	if kerrors.FromError(err).Code != 400 || *hits != 0 {
		t.Fatalf("code = %d hits = %d, want 400 and no request", kerrors.FromError(err).Code, *hits)
	}
}

// 部分结果（VictoriaMetrics 集群版 isPartial）并入 warnings：接口契约没有单独的字段。
func TestQueryClient_PartialResultBecomesWarning(t *testing.T) {
	t.Parallel()
	url, _ := promServer(t, strings.Replace(vectorBody, `"status":"success"`, `"status":"success","isPartial":true`, 1))
	res, err := NewQueryClient(nil).Instant(context.Background(), conn(url), "up", time.Time{}, 10)
	if err != nil || len(res.Series) != 1 {
		t.Fatalf("res = %+v err=%v", res, err)
	}
	if len(res.Warnings) != 1 || !strings.Contains(res.Warnings[0], "部分结果") {
		t.Fatalf("warnings = %v, want the partial-result warning", res.Warnings)
	}
	full, _ := promServer(t, vectorBody)
	if res, _ := NewQueryClient(nil).Instant(context.Background(), conn(full), "up", time.Time{}, 10); len(res.Warnings) != 0 {
		t.Fatalf("a complete result must not carry the warning: %v", res.Warnings)
	}
}

// 评审第二轮 K4：非 2xx 响应的正文不回显给调用方（测试连接 / 即时查询会把错误说明原样返回，而数据源
// 地址由用户填写——带出正文等于把任意内网 HTTP 服务的响应读给他）。只留错误类型、状态码的标准说明；
// Prometheus 错误结构里的错误说明照常返回（表达式写错时要让用户看到原因）。
func TestQueryClient_Non2xxBodyIsNotEchoed(t *testing.T) {
	t.Parallel()
	const secret = "internal-admin-token-8f3a"
	for name, c := range map[string]struct {
		status  int
		body    string
		want    *kerrors.Error
		message string
	}{
		"html error page":       {http.StatusInternalServerError, "<html><body>stack trace " + secret + "</body></html>", biz.ErrDatasourceUnreachable, "http: Internal Server Error"},
		"plain text":            {http.StatusBadGateway, "upstream " + secret + " unavailable", biz.ErrDatasourceUnreachable, "http: Bad Gateway"},
		"foreign json":          {http.StatusForbidden, `{"error":"` + secret + `","message":"` + secret + `"}`, biz.ErrDatasourceUnreachable, "http: Forbidden"},
		"json array":            {http.StatusNotFound, `["` + secret + `"]`, biz.ErrDatasourceUnreachable, "http: Not Found"},
		"empty body":            {http.StatusServiceUnavailable, "", biz.ErrDatasourceUnreachable, "http: Service Unavailable"},
		"prometheus error body": {http.StatusBadRequest, `{"status":"error","errorType":"bad_data","error":"parse error: unexpected end of input"}`, biz.ErrQueryInvalid, "bad_data: parse error: unexpected end of input"},
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(c.status)
			_, _ = w.Write([]byte(c.body))
		}))
		t.Cleanup(srv.Close)
		q := NewQueryClient(nil)
		_, instantErr := q.Instant(context.Background(), conn(srv.URL), "up", time.Time{}, 10)
		_, rangeErr := q.Range(context.Background(), conn(srv.URL), "up", time.Now().Add(-time.Minute), time.Now(), 15*time.Second, 10)
		for kind, err := range map[string]error{"instant": instantErr, "range": rangeErr} {
			if !kerrors.Is(err, c.want) {
				t.Errorf("%s (%s): err = %v, want %s", name, kind, err, c.want.Reason)
				continue
			}
			if msg := kerrors.FromError(err).Message; msg != c.message || strings.Contains(err.Error(), secret) {
				t.Errorf("%s (%s): message = %q, want exactly %q with no part of the response body", name, kind, msg, c.message)
			}
		}
	}
}
