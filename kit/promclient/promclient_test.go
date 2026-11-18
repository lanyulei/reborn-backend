package promclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"
)

const (
	testSecret = "s3cr3t-token-value"
	vectorBody = `{"status":"success","data":{"resultType":"vector","result":[
		{"metric":{"__name__":"up","instance":"a:9100"},"value":[1700000000.123,"1"]},
		{"metric":{"__name__":"up","instance":"b:9100"},"value":[1700000000.123,"0"]},
		{"metric":{"__name__":"up","instance":"c:9100"},"value":[1700000000.123,"NaN"]}]}}`
)

// newTestClient 以 httptest 服务地址构造客户端。
func newTestClient(t *testing.T, base string, mutate func(*Config)) *Client {
	t.Helper()
	cfg := Config{BaseURL: base}
	if mutate != nil {
		mutate(&cfg)
	}
	c, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

func serve(t *testing.T, h http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

func asError(t *testing.T, err error) *Error {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("want *Error, got %T: %v", err, err)
	}
	return e
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	t.Parallel()
	cases := map[string]Config{
		"empty":            {},
		"no scheme":        {BaseURL: "prometheus:9090"},
		"ftp":              {BaseURL: "ftp://prometheus:9090"},
		"file":             {BaseURL: "file:///etc/passwd"},
		"userinfo":         {BaseURL: "http://user:" + testSecret + "@prometheus:9090"},
		"empty host":       {BaseURL: "http:///select/0"},
		"query":            {BaseURL: "http://prometheus:9090/?token=" + testSecret},
		"fragment":         {BaseURL: "http://prometheus:9090/#x"},
		"unparsable":       {BaseURL: "http://user:" + testSecret + "@[::1"},
		"auth type":        {BaseURL: "http://p:9090", AuthType: 9},
		"basic no user":    {BaseURL: "http://p:9090", AuthType: AuthBasic, Secret: testSecret},
		"bearer no token":  {BaseURL: "http://p:9090", AuthType: AuthBearer},
		"bearer crlf":      {BaseURL: "http://p:9090", AuthType: AuthBearer, Secret: "a\r\nX-Evil: 1"}, //nolint:gosec // G101：测试用的非法令牌字面量，不是凭据
		"negative series":  {BaseURL: "http://p:9090", MaxSeries: -1},
		"header authz":     {BaseURL: "http://p:9090", Headers: map[string]string{"authorization": "Bearer " + testSecret}},
		"header bad value": {BaseURL: "http://p:9090", Headers: map[string]string{"X-A": "v\r\nX-Evil: 1"}},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c, err := New(cfg)
			if err == nil {
				c.Close()
				t.Fatal("expected error")
			}
			if strings.Contains(err.Error(), testSecret) {
				t.Fatalf("error leaks secret: %v", err)
			}
		})
	}
}

func TestValidateHeaders(t *testing.T) {
	t.Parallel()
	many := map[string]string{}
	for i := range MaxHeaders + 1 {
		many[fmt.Sprintf("X-H-%d", i)] = "v"
	}
	cases := []struct {
		name    string
		headers map[string]string
		ok      bool
	}{
		{"nil", nil, true},
		{"plain", map[string]string{"X-Scope-OrgID": "tenant-1", "X-Tab": "a\tb"}, true},
		{"authorization", map[string]string{"Authorization": "x"}, false},
		{"cookie", map[string]string{"COOKIE": "x"}, false},
		{"host", map[string]string{"Host": "x"}, false},
		{"proxy", map[string]string{"Proxy-Authorization": "x"}, false},
		{"hop", map[string]string{"Transfer-Encoding": "chunked"}, false},
		{"connection", map[string]string{"connection": "close"}, false},
		{"content-length", map[string]string{"Content-Length": "1"}, false},
		{"content-type", map[string]string{"Content-Type": "text/plain"}, false},
		{"accept-encoding", map[string]string{"Accept-Encoding": "br"}, false},
		{"empty name", map[string]string{"": "x"}, false},
		{"space in name", map[string]string{"X A": "x"}, false},
		{"colon in name", map[string]string{"X:A": "x"}, false},
		{"crlf in name", map[string]string{"X\r\nA": "x"}, false},
		{"long name", map[string]string{strings.Repeat("a", 65): "x"}, false},
		{"lf value", map[string]string{"X-A": "a\nb"}, false},
		{"nul value", map[string]string{"X-A": "a\x00b"}, false},
		{"long value", map[string]string{"X-A": strings.Repeat("a", 1025)}, false},
		{"case duplicate", map[string]string{"X-A": "1", "x-a": "2"}, false},
		{"too many", many, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if err := ValidateHeaders(tc.headers); (err == nil) != tc.ok {
				t.Fatalf("ValidateHeaders = %v, want ok=%v", err, tc.ok)
			}
		})
	}
}

func TestURLJoin(t *testing.T) {
	t.Parallel()
	cases := []struct{ prefix, wantQuery, wantRange string }{
		{"", "/api/v1/query", "/api/v1/query_range"},
		{"/", "/api/v1/query", "/api/v1/query_range"},
		{"//", "/api/v1/query", "/api/v1/query_range"},
		{"/select/0/prometheus", "/select/0/prometheus/api/v1/query", "/select/0/prometheus/api/v1/query_range"},
		{"/select/0/prometheus/", "/select/0/prometheus/api/v1/query", "/select/0/prometheus/api/v1/query_range"},
	}
	for _, tc := range cases {
		t.Run("prefix="+tc.prefix, func(t *testing.T) {
			t.Parallel()
			var paths []string
			srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
				paths = append(paths, r.URL.Path)
				_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"vector","result":[]}}`)
			})
			c := newTestClient(t, srv.URL+tc.prefix, nil)
			if _, err := c.Query(context.Background(), "up", time.Time{}); err != nil {
				t.Fatal(err)
			}
			now := time.Now()
			if _, err := c.QueryRange(context.Background(), "up", now.Add(-time.Minute), now, 15*time.Second); err != nil {
				t.Fatal(err)
			}
			if len(paths) != 2 || paths[0] != tc.wantQuery || paths[1] != tc.wantRange {
				t.Fatalf("paths = %v", paths)
			}
		})
	}
}

func TestRequestShape(t *testing.T) {
	t.Parallel()
	type seen struct {
		method, contentType, accept, auth, org string
		form                                   url.Values
		rawQuery                               string
	}
	var got seen
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		got = seen{
			method: r.Method, contentType: r.Header.Get("Content-Type"), accept: r.Header.Get("Accept"),
			auth: r.Header.Get("Authorization"), org: r.Header.Get("X-Scope-OrgID"), rawQuery: r.URL.RawQuery,
		}
		raw, _ := io.ReadAll(r.Body)
		got.form, _ = url.ParseQuery(string(raw))
		_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"vector","result":[]}}`)
	})
	expr := `sum(rate(http_requests_total{job="a&b"}[5m])) > 1`

	t.Run("instant basic", func(t *testing.T) { // 子测试共享 got，顺序执行
		c := newTestClient(t, srv.URL, func(cfg *Config) {
			cfg.AuthType, cfg.Username, cfg.Secret = AuthBasic, "ops", testSecret
			cfg.Headers = map[string]string{"x-scope-orgid": "tenant-1"}
		})
		ts := time.Unix(1700000000, 500_000_000)
		if _, err := c.Query(context.Background(), expr, ts); err != nil {
			t.Fatal(err)
		}
		if got.method != http.MethodPost || got.contentType != "application/x-www-form-urlencoded" || got.accept != "application/json" || got.rawQuery != "" {
			t.Fatalf("request = %+v", got)
		}
		if got.form.Get("query") != expr || got.form.Get("time") != "1700000000.500" {
			t.Fatalf("form = %v", got.form)
		}
		req, _ := http.NewRequest(http.MethodGet, "http://x", nil)
		req.SetBasicAuth("ops", testSecret)
		if got.auth != req.Header.Get("Authorization") || got.org != "tenant-1" {
			t.Fatalf("auth = %q org = %q", got.auth, got.org)
		}
	})
	t.Run("zero ts omits time; bearer", func(t *testing.T) { // 顺序执行
		c := newTestClient(t, srv.URL, func(cfg *Config) { cfg.AuthType, cfg.Secret = AuthBearer, testSecret })
		if err := c.Ping(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got.form.Get("query") != "1" || got.form.Has("time") || got.auth != "Bearer "+testSecret {
			t.Fatalf("form = %v auth = %q", got.form, got.auth)
		}
	})
	t.Run("range; no auth", func(t *testing.T) { // 顺序执行
		c := newTestClient(t, srv.URL, func(cfg *Config) { cfg.AuthType = AuthNone })
		start := time.Unix(1700000000, 0)
		if _, err := c.QueryRange(context.Background(), expr, start, start.Add(time.Hour), 1500*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		if got.auth != "" || got.form.Get("start") != "1700000000.000" || got.form.Get("end") != "1700003600.000" || got.form.Get("step") != "1.5" {
			t.Fatalf("form = %v auth = %q", got.form, got.auth)
		}
	})
}

func TestArgumentValidation(t *testing.T) {
	t.Parallel()
	srv := serve(t, func(http.ResponseWriter, *http.Request) { t.Error("must not reach upstream") })
	c := newTestClient(t, srv.URL, nil)
	now := time.Now()
	errs := []error{}
	_, err := c.Query(context.Background(), "  ", now)
	errs = append(errs, err)
	_, err = c.QueryRange(context.Background(), "", now, now, time.Second)
	errs = append(errs, err)
	_, err = c.QueryRange(context.Background(), "up", now, now.Add(-time.Second), time.Second)
	errs = append(errs, err)
	_, err = c.QueryRange(context.Background(), "up", now, now, 0)
	errs = append(errs, err)
	for i, err := range errs {
		if err == nil || !asError(t, err).BadQuery() {
			t.Fatalf("case %d: %v", i, err)
		}
	}
}

func TestParseResults(t *testing.T) {
	t.Parallel()
	t.Run("vector", func(t *testing.T) {
		t.Parallel()
		srv := serve(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, vectorBody) })
		res, err := newTestClient(t, srv.URL, nil).Query(context.Background(), "up", time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		if res.Type != ResultVector || res.Truncated || len(res.Series) != 3 {
			t.Fatalf("res = %+v", res)
		}
		s := res.Series[0]
		if s.Labels["instance"] != "a:9100" || s.Labels["__name__"] != "up" || len(s.Points) != 1 ||
			s.Points[0].Value != 1 || !s.Points[0].Time.Equal(time.UnixMilli(1700000000123)) {
			t.Fatalf("series0 = %+v", s)
		}
		if !math.IsNaN(res.Series[2].Points[0].Value) {
			t.Fatalf("NaN = %v", res.Series[2].Points[0].Value)
		}
	})
	t.Run("special floats and warnings", func(t *testing.T) {
		t.Parallel()
		srv := serve(t, func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, `{"status":"success","warnings":["partial"],"data":{"resultType":"vector","result":[
				{"metric":{"k":"1"},"value":[1,"+Inf"]},{"metric":{"k":"2"},"value":[1,"-Inf"]},{"metric":{"k":"3"},"value":[1,"1.5e-3"]},
				{"metric":{"k":"4"},"histogram":[1,{"count":"1"}]},{"value":[2,"7"]}]}}`)
		})
		res, err := newTestClient(t, srv.URL, nil).Query(context.Background(), "x", time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Series) != 4 || !math.IsInf(res.Series[0].Points[0].Value, 1) || !math.IsInf(res.Series[1].Points[0].Value, -1) ||
			res.Series[2].Points[0].Value != 0.0015 || res.Series[3].Labels == nil || len(res.Series[3].Labels) != 0 {
			t.Fatalf("res = %+v", res)
		}
		if len(res.Warnings) != 1 || res.Warnings[0] != "partial" {
			t.Fatalf("warnings = %v", res.Warnings)
		}
	})
	t.Run("scalar", func(t *testing.T) {
		t.Parallel()
		srv := serve(t, func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"scalar","result":[1700000000,"42"]}}`)
		})
		res, err := newTestClient(t, srv.URL, nil).Query(context.Background(), "42", time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		if res.Type != ResultScalar || len(res.Series) != 1 || len(res.Series[0].Labels) != 0 || res.Series[0].Points[0].Value != 42 ||
			!res.Series[0].Points[0].Time.Equal(time.Unix(1700000000, 0)) {
			t.Fatalf("res = %+v", res)
		}
	})
	t.Run("string", func(t *testing.T) {
		t.Parallel()
		srv := serve(t, func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"string","result":[1700000000,"hello"]}}`)
		})
		res, err := newTestClient(t, srv.URL, nil).Query(context.Background(), `"hello"`, time.Time{})
		if err != nil || res.Type != ResultString || len(res.Series) != 0 {
			t.Fatalf("res = %+v, %v", res, err)
		}
	})
	t.Run("matrix", func(t *testing.T) {
		t.Parallel()
		srv := serve(t, func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"matrix","result":[
				{"metric":{"job":"a"},"values":[[1700000000,"1"],[1700000015,"2"],[1700000030,"NaN"]]},
				{"metric":{"job":"b"},"values":[[1700000000,"3"]]}]}}`)
		})
		start := time.Unix(1700000000, 0)
		res, err := newTestClient(t, srv.URL, nil).QueryRange(context.Background(), "x", start, start.Add(30*time.Second), 15*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if res.Type != ResultMatrix || len(res.Series) != 2 || len(res.Series[0].Points) != 3 || res.Series[0].Points[1].Value != 2 ||
			!res.Series[0].Points[1].Time.Equal(time.Unix(1700000015, 0)) || !math.IsNaN(res.Series[0].Points[2].Value) || res.Series[1].Labels["job"] != "b" {
			t.Fatalf("res = %+v", res)
		}
	})
	for name, body := range map[string]string{
		"unknown type":   `{"status":"success","data":{"resultType":"streams","result":[]}}`,
		"bad value":      `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,"abc"]}]}}`,
		"bad pair":       `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":["x","1"]}]}}`,
		"missing status": `{"data":{"resultType":"vector","result":[]}}`,
		"not json":       `<html>ok</html>`,
		"truncated json": `{"status":"success","data":{"resultType":"vec`,
		"empty":          ``,
	} {
		t.Run("malformed/"+name, func(t *testing.T) {
			t.Parallel()
			srv := serve(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, body) })
			_, err := newTestClient(t, srv.URL, nil).Query(context.Background(), "up", time.Time{})
			if e := asError(t, err); e.ErrorType != ErrTypeBadResponse || e.Status != http.StatusOK || e.BadQuery() {
				t.Fatalf("err = %+v", e)
			}
		})
	}
}

func TestPartialResult(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		body string
		want bool
	}{
		// VictoriaMetrics 集群版：部分存储节点不可用时 isPartial 为 true。
		"vm partial":       {`{"status":"success","isPartial":true,"data":{"resultType":"vector","result":[{"metric":{"a":"1"},"value":[1,"1"]}]}}`, true},
		"vm complete":      {`{"status":"success","isPartial":false,"data":{"resultType":"vector","result":[]},"stats":{"seriesFetched":"0"}}`, false},
		"partial at end":   {`{"status":"success","data":{"resultType":"vector","result":[]},"isPartial":true}`, true},
		"prometheus":       {`{"status":"success","data":{"resultType":"vector","result":[]}}`, false},
		"partial + scalar": {`{"isPartial":true,"status":"success","data":{"resultType":"scalar","result":[1,"1"]}}`, true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := serve(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, tc.body) })
			res, err := newTestClient(t, srv.URL, nil).Query(context.Background(), "up", time.Time{})
			if err != nil || res.Partial != tc.want {
				t.Fatalf("res = %+v, %v; want Partial = %v", res, err, tc.want)
			}
		})
	}
}

func TestStreamedResultShapes(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		body string
		want int
	}{
		"result before resultType": {`{"data":{"result":[{"metric":{"a":"1"},"value":[1,"1"]},{"metric":{"a":"2"},"value":[1,"2"]}],"resultType":"vector"},"status":"success"}`, 2},
		"null result":              {`{"status":"success","data":{"resultType":"vector","result":null}}`, 0},
		"unknown keys are skipped": {`{"status":"success","trace":{"a":[1,{"b":[]}],"c":"d"},"data":{"x":[[1,2],{"y":null}],"resultType":"vector","result":[{"metric":{},"value":[1,"1"]}],"z":1}}`, 1},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := serve(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, tc.body) })
			res, err := newTestClient(t, srv.URL, func(cfg *Config) { cfg.MaxSeries = 5 }).Query(context.Background(), "up", time.Time{})
			if err != nil || res.Type != ResultVector || len(res.Series) != tc.want || res.Truncated || res.Series == nil {
				t.Fatalf("res = %+v, %v", res, err)
			}
		})
	}
	for name, body := range map[string]string{
		"missing result":          `{"status":"success","data":{"resultType":"vector"}}`,
		"result is an object":     `{"status":"success","data":{"resultType":"vector","result":{}}}`,
		"data is an array":        `{"status":"success","data":[]}`,
		"late type, bad result":   `{"status":"success","data":{"result":{"a":1},"resultType":"vector"}}`,
		"series is not an object": `{"status":"success","data":{"resultType":"vector","result":[1]}}`,
	} {
		t.Run("malformed/"+name, func(t *testing.T) {
			t.Parallel()
			srv := serve(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, body) })
			_, err := newTestClient(t, srv.URL, nil).Query(context.Background(), "up", time.Time{})
			if e := asError(t, err); e.ErrorType != ErrTypeBadResponse {
				t.Fatalf("err = %+v", e)
			}
		})
	}
	// 上游报错时 data 常为 null：仍按错误结构处理。
	t.Run("error with null data", func(t *testing.T) {
		t.Parallel()
		srv := serve(t, func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, `{"status":"error","data":null,"errorType":"execution","error":"boom"}`)
		})
		_, err := newTestClient(t, srv.URL, nil).Query(context.Background(), "up", time.Time{})
		if e := asError(t, err); e.ErrorType != "execution" || e.Message != "boom" {
			t.Fatalf("err = %+v", e)
		}
	})
}

// vectorOf 生成一个 vector 响应：序列按给定顺序排列，标签 i=<编号>。
func vectorOf(ids []int) string {
	var b strings.Builder
	b.WriteString(`{"status":"success","data":{"resultType":"vector","result":[`)
	for n, id := range ids {
		if n > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"metric":{"__name__":"m","i":"%04d"},"value":[1,"%d"]}`, id, id)
	}
	b.WriteString(`]}}`)
	return b.String()
}

// 截断必须是确定的：同一批序列无论上游以什么顺序返回，留下的都是标签排序最靠前的那一批。
// 否则每轮评估看到的子集不同，没被看到的序列会被当成消失。
func TestSeriesCapIsDeterministic(t *testing.T) {
	t.Parallel()
	const total, limit = 500, 40
	ids := make([]int, total)
	for round := range 5 {
		// 每轮换一种确定的排列（不需要随机数）：按不同的步长跳着取。
		step := []int{1, 7, 13, 211, 499}[round]
		for i := range ids {
			ids[i] = (i*step + round) % total
		}
		body := vectorOf(ids)
		srv := serve(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, body) })
		res, err := newTestClient(t, srv.URL, func(cfg *Config) { cfg.MaxSeries = limit }).Query(context.Background(), "m", time.Time{})
		if err != nil || !res.Truncated || len(res.Series) != limit {
			t.Fatalf("round %d: res = %d series, truncated = %v, err = %v", round, len(res.Series), res.Truncated, err)
		}
		for i, s := range res.Series {
			if want := fmt.Sprintf("%04d", i); s.Labels["i"] != want || s.Points[0].Value != float64(i) {
				t.Fatalf("round %d: series %d = %v (value %v), want i=%s", round, i, s.Labels, s.Points[0].Value, want)
			}
		}
	}
	// 未超限：保持上游的返回顺序。
	body := vectorOf([]int{3, 1, 2})
	srv := serve(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, body) })
	res, err := newTestClient(t, srv.URL, func(cfg *Config) { cfg.MaxSeries = 3 }).Query(context.Background(), "m", time.Time{})
	if err != nil || res.Truncated || len(res.Series) != 3 || res.Series[0].Labels["i"] != "0003" || res.Series[2].Labels["i"] != "0002" {
		t.Fatalf("res = %+v, %v", res, err)
	}
	// 没有浮点采样的序列不占名额。
	srv = serve(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"vector","result":[
			{"metric":{"k":"h1"},"histogram":[1,{}]},{"metric":{"k":"h2"},"histogram":[1,{}]},
			{"metric":{"k":"b"},"value":[1,"1"]},{"metric":{"k":"a"},"value":[1,"2"]}]}}`)
	})
	res, err = newTestClient(t, srv.URL, func(cfg *Config) { cfg.MaxSeries = 2 }).Query(context.Background(), "m", time.Time{})
	if err != nil || res.Truncated || len(res.Series) != 2 {
		t.Fatalf("res = %+v, %v", res, err)
	}
}

// seriesStream 按需生成一个巨大的 vector 响应，并在生成过程中采样堆占用。
type seriesStream struct {
	total, next int
	buf         []byte
	sampleEvery int
	peak        uint64
}

func (s *seriesStream) Read(p []byte) (int, error) {
	for len(s.buf) < len(p) && s.next <= s.total {
		switch s.next {
		case 0:
			s.buf = append(s.buf, `{"status":"success","data":{"resultType":"vector","result":[`...)
		case s.total:
			s.buf = append(s.buf, `]}}`...)
		}
		if s.next < s.total {
			if s.next > 0 {
				s.buf = append(s.buf, ',')
			}
			// 编号倒序：每一条都比已保留的更靠前，走最坏路径（每条都替换堆顶）。
			s.buf = fmt.Appendf(s.buf, `{"metric":{"__name__":"m","instance":"host-%07d","job":"node"},"value":[1,"1"]}`, s.total-s.next)
			if s.next%s.sampleEvery == 0 {
				runtime.GC()
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				s.peak = max(s.peak, m.HeapAlloc)
			}
		}
		s.next++
	}
	if len(s.buf) == 0 {
		return 0, io.EOF
	}
	n := copy(p, s.buf)
	s.buf = s.buf[n:]
	return n, nil
}

// max_series 必须同时限制内存：20 万条序列的响应在解码过程中只保留 max 条。
func TestSeriesCapBoundsMemory(t *testing.T) { //nolint:paralleltest // 测量堆占用，不与其它测试并行
	const total, limit = 200_000, 100
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	src := &seriesStream{total: total, sampleEvery: total / 10}
	_, data, err := decodeBody(src, limit)
	if err != nil {
		t.Fatal(err)
	}
	res, err := data.result()
	if err != nil || !res.Truncated || len(res.Series) != limit || res.Series[0].Labels["instance"] != "host-0000001" {
		t.Fatalf("res = %d series, truncated = %v, first = %v, err = %v", len(res.Series), res.Truncated, res.Series[0].Labels, err)
	}
	// 全量保留 20 万条序列要占用上百 MiB；流式解码的增量应只有保留的 100 条加解码缓冲。
	const budget = 8 << 20
	if src.peak > before.HeapAlloc+budget {
		t.Fatalf("heap grew by %d bytes while decoding, budget %d", src.peak-before.HeapAlloc, budget)
	}
}

func TestSeriesCap(t *testing.T) {
	t.Parallel()
	srv := serve(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, vectorBody) })
	for _, tc := range []struct {
		max, want int
		truncated bool
	}{{0, 3, false}, {2, 2, true}, {3, 3, false}, {10, 3, false}} {
		t.Run(fmt.Sprintf("max=%d", tc.max), func(t *testing.T) {
			t.Parallel()
			c := newTestClient(t, srv.URL, func(cfg *Config) { cfg.MaxSeries = tc.max })
			res, err := c.Query(context.Background(), "up", time.Time{})
			if err != nil || len(res.Series) != tc.want || res.Truncated != tc.truncated {
				t.Fatalf("res = %+v, %v", res, err)
			}
		})
	}
}

func TestErrorPayloads(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		status   int
		body     string
		wantType string
		wantMsg  string
		badQuery bool
	}{
		{"400 bad_data", 400, `{"status":"error","errorType":"bad_data","error":"1:5: parse error: unexpected end of input"}`, "bad_data", "1:5: parse error: unexpected end of input", true},
		{"422 execution", 422, `{"status":"error","errorType":"execution","error":"many-to-many matching not allowed"}`, "execution", "many-to-many matching not allowed", true},
		{"500 internal", 500, `{"status":"error","errorType":"internal","error":"boom"}`, "internal", "boom", false},
		{"503 timeout", 503, `{"status":"error","errorType":"timeout","error":"query timed out in expression evaluation"}`, "timeout", "query timed out in expression evaluation", false},
		{"200 error status", 200, `{"status":"error","errorType":"unavailable","error":"no backend"}`, "unavailable", "no backend", false},
		{"vm error without type", 422, `{"status":"error","error":"cannot parse query"}`, ErrTypeHTTP, "cannot parse query", true},
		// K4：非 Prometheus 错误结构的响应体一律不回显，只有状态码与其标准说明。
		{"502 html", 502, "<html>\n<body>\n  internal-secret</body></html>", ErrTypeHTTP, "Bad Gateway", false},
		{"403 foreign json", 403, `{"error":"internal-secret","token":"internal-secret"}`, ErrTypeHTTP, "Forbidden", false},
		{"500 foreign json object", 500, `{"status":"down","error":"internal-secret"}`, ErrTypeHTTP, "Internal Server Error", false},
		{"500 plain text", 500, "internal-secret", ErrTypeHTTP, "Internal Server Error", false},
		{"non-standard status", 599, "internal-secret", ErrTypeHTTP, "unexpected HTTP status", false},
		{"error structure without a message", 503, `{"status":"error"}`, ErrTypeHTTP, "Service Unavailable", false},
		{"401 empty", 401, "", ErrTypeHTTP, "Unauthorized", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := serve(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			})
			_, err := newTestClient(t, srv.URL, nil).Query(context.Background(), "up", time.Time{})
			e := asError(t, err)
			if e.ErrorType != tc.wantType || e.Message != tc.wantMsg || e.Status != tc.status || e.BadQuery() != tc.badQuery {
				t.Fatalf("err = %+v", e)
			}
			if !strings.Contains(err.Error(), fmt.Sprintf("http %d", tc.status)) || strings.Contains(err.Error(), "internal-secret") {
				t.Fatalf("Error() = %q", err.Error())
			}
		})
	}
}

func TestRedirectNotFollowed(t *testing.T) {
	t.Parallel()
	var hits int
	target := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		hits++
		_, _ = io.WriteString(w, vectorBody)
	})
	for _, status := range []int{301, 302, 307, 308} {
		srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, target.URL+"/api/v1/query", status)
		})
		c := newTestClient(t, srv.URL, func(cfg *Config) { cfg.AuthType, cfg.Secret = AuthBearer, testSecret })
		_, err := c.Query(context.Background(), "up", time.Time{})
		if e := asError(t, err); e.ErrorType != ErrTypeRedirect || e.Status != status {
			t.Fatalf("status %d: err = %+v", status, e)
		}
		if strings.Contains(err.Error(), target.URL) {
			t.Fatalf("error echoes redirect target: %v", err)
		}
	}
	if hits != 0 {
		t.Fatalf("redirect target was hit %d times", hits)
	}
}

func TestBodyCap(t *testing.T) {
	t.Parallel()
	const limit = 4096
	pad := func(n int) string {
		const head, tail = `{"status":"success","data":{"resultType":"vector","result":[]},"warnings":["`, `"]}`
		return head + strings.Repeat("a", n-len(head)-len(tail)) + tail
	}
	for _, tc := range []struct {
		name   string
		status int
		size   int
		ok     bool
	}{
		{"exactly at cap", 200, limit, true},
		{"one byte over", 200, limit + 1, false},
		{"far over", 200, limit * 8, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := serve(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, pad(tc.size))
			})
			c := newTestClient(t, srv.URL, nil)
			c.maxBody = limit
			_, err := c.Query(context.Background(), "up", time.Time{})
			if tc.ok {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if e := asError(t, err); e.ErrorType != ErrTypeBodyTooLarge {
				t.Fatalf("err = %+v", e)
			}
		})
	}
	t.Run("default cap is 16MiB", func(t *testing.T) {
		t.Parallel()
		c := newTestClient(t, "http://127.0.0.1:1", nil)
		if c.maxBody != MaxBodyBytes || MaxBodyBytes != 16<<20 {
			t.Fatalf("maxBody = %d", c.maxBody)
		}
	})
}

func TestTimeoutAndCancel(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	srv := serve(t, func(_ http.ResponseWriter, r *http.Request) {
		// 先读完请求体：服务端读到 EOF 后才开始监测连接断开，r.Context() 才会随客户端放弃而结束。
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	t.Run("client timeout", func(t *testing.T) {
		t.Parallel()
		c := newTestClient(t, srv.URL, func(cfg *Config) { cfg.Timeout = 50 * time.Millisecond })
		start := time.Now()
		_, err := c.Query(context.Background(), "up", time.Time{})
		if e := asError(t, err); e.ErrorType != ErrTypeTimeout || !errors.Is(err, context.DeadlineExceeded) || e.BadQuery() {
			t.Fatalf("err = %+v", e)
		}
		if time.Since(start) > 5*time.Second {
			t.Fatal("timeout not enforced")
		}
	})
	t.Run("caller deadline", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_, err := newTestClient(t, srv.URL, nil).Query(ctx, "up", time.Time{})
		if e := asError(t, err); e.ErrorType != ErrTypeTimeout {
			t.Fatalf("err = %+v", e)
		}
	})
	t.Run("caller cancel", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(50*time.Millisecond, cancel)
		_, err := newTestClient(t, srv.URL, nil).Query(ctx, "up", time.Time{})
		if e := asError(t, err); e.ErrorType != ErrTypeCanceled || !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %+v", e)
		}
	})
	t.Run("stall mid body", func(t *testing.T) {
		t.Parallel()
		stall := serve(t, func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"vector","result":[`)
			_, _ = io.Copy(io.Discard, r.Body)
			w.(http.Flusher).Flush()
			select {
			case <-release:
			case <-r.Context().Done():
			}
		})
		c := newTestClient(t, stall.URL, func(cfg *Config) { cfg.Timeout = 50 * time.Millisecond })
		_, err := c.Query(context.Background(), "up", time.Time{})
		if e := asError(t, err); e.ErrorType != ErrTypeTimeout {
			t.Fatalf("err = %+v", e)
		}
	})
}

func TestTimeoutDefaults(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ in, want time.Duration }{
		{0, DefaultTimeout}, {-time.Second, DefaultTimeout}, {3 * time.Second, 3 * time.Second}, {time.Minute, MaxTimeout},
	} {
		c := newTestClient(t, "http://127.0.0.1:1", func(cfg *Config) { cfg.Timeout = tc.in })
		if c.timeout != tc.want {
			t.Fatalf("timeout(%v) = %v, want %v", tc.in, c.timeout, tc.want)
		}
	}
}

func TestNetworkErrorHasNoCredentials(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.NotFoundHandler())
	addr := srv.URL
	srv.Close() // 端口已关闭：连接被拒绝
	c := newTestClient(t, addr+"/prefix", func(cfg *Config) {
		cfg.AuthType, cfg.Username, cfg.Secret = AuthBasic, "ops", testSecret
	})
	_, err := c.Query(context.Background(), "up", time.Time{})
	e := asError(t, err)
	if e.ErrorType != ErrTypeNetwork || e.Status != 0 {
		t.Fatalf("err = %+v", e)
	}
	if strings.Contains(err.Error(), testSecret) || strings.Contains(err.Error(), "/api/v1/query") || strings.Contains(err.Error(), "ops") {
		t.Fatalf("error leaks request details: %v", err)
	}
}

// TestErrorsNeverContainCredentials 上游把凭据回显在各种位置时，错误信息里都不得出现。
func TestErrorsNeverContainCredentials(t *testing.T) {
	t.Parallel()
	bodies := []struct {
		status int
		body   string
	}{
		{401, `{"status":"error","errorType":"unauthorized","error":"bad token ` + testSecret + `"}`},
		{403, `denied for Authorization: Bearer ` + testSecret},
		{200, `{"status":"error","errorType":"` + testSecret + `","error":"x"}`},
		{200, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,"` + testSecret + `"]}]}}`},
	}
	for i, b := range bodies {
		srv := serve(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(b.status)
			_, _ = io.WriteString(w, b.body)
		})
		c := newTestClient(t, srv.URL, func(cfg *Config) { cfg.AuthType, cfg.Secret = AuthBearer, testSecret })
		_, err := c.Query(context.Background(), "up", time.Time{})
		if err == nil || strings.Contains(err.Error(), testSecret) {
			t.Fatalf("case %d: err = %v", i, err)
		}
	}
	srv := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"status":"success","warnings":["echo `+testSecret+`"],"data":{"resultType":"vector","result":[]}}`)
	})
	c := newTestClient(t, srv.URL, func(cfg *Config) { cfg.AuthType, cfg.Secret = AuthBearer, testSecret })
	res, err := c.Query(context.Background(), "up", time.Time{})
	if err != nil || len(res.Warnings) != 1 || strings.Contains(res.Warnings[0], testSecret) {
		t.Fatalf("warnings = %+v, %v", res, err)
	}
}

func TestErrorTruncation(t *testing.T) {
	t.Parallel()
	long := strings.Repeat("错", 400) // 1200 字节，3 字节一个字符：512 不在字符边界上
	for name, h := range map[string]http.HandlerFunc{
		"json": func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(400)
			_, _ = io.WriteString(w, `{"status":"error","errorType":"bad_data","error":"`+long+`"}`)
		},
		// 非错误结构的响应体不进错误信息（TestErrorPayloads），这里只有错误结构里的说明需要截断。
		"invalid utf8": func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(500)
			_, _ = w.Write([]byte(`{"status":"error","error":"` + strings.Repeat("a\xff", 600) + `"}`))
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := serve(t, h)
			_, err := newTestClient(t, srv.URL, nil).Query(context.Background(), "up", time.Time{})
			e := asError(t, err)
			if len(e.Message) > MaxErrorBytes || len(e.Message) < MaxErrorBytes-utf8.UTFMax || !utf8.ValidString(e.Message) {
				t.Fatalf("message len = %d valid = %v", len(e.Message), utf8.ValidString(e.Message))
			}
		})
	}
}

func TestTLSSkipVerify(t *testing.T) {
	t.Parallel()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, vectorBody) }))
	t.Cleanup(srv.Close)

	_, err := newTestClient(t, srv.URL, nil).Query(context.Background(), "up", time.Time{})
	if e := asError(t, err); e.ErrorType != ErrTypeNetwork {
		t.Fatalf("verify on: err = %+v", e)
	}
	res, err := newTestClient(t, srv.URL, func(cfg *Config) { cfg.TLSSkipVerify = true }).Query(context.Background(), "up", time.Time{})
	if err != nil || len(res.Series) != 3 {
		t.Fatalf("verify off: %+v, %v", res, err)
	}
}

// TestConnectionReuse 同一客户端的连续请求复用连接。
func TestConnectionReuse(t *testing.T) {
	t.Parallel()
	remotes := map[string]struct{}{}
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		remotes[r.RemoteAddr] = struct{}{}
		_, _ = io.WriteString(w, vectorBody)
	})
	c := newTestClient(t, srv.URL, nil)
	for range 5 {
		if _, err := c.Query(context.Background(), "up", time.Time{}); err != nil {
			t.Fatal(err)
		}
	}
	if len(remotes) != 1 {
		t.Fatalf("expected one reused connection, got %d", len(remotes))
	}
	c.Close()
	if _, err := c.Query(context.Background(), "up", time.Time{}); err != nil {
		t.Fatalf("client must stay usable after Close: %v", err)
	}
}

func TestDialGuard(t *testing.T) {
	t.Parallel()
	var hits atomic.Int32
	srv := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = io.WriteString(w, vectorBody)
	})
	port := srv.URL[strings.LastIndexByte(srv.URL, ':')+1:]

	t.Run("nil guard keeps the current behaviour", func(t *testing.T) {
		t.Parallel()
		if _, err := newTestClient(t, srv.URL, nil).Query(context.Background(), "up", time.Time{}); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("allowed", func(t *testing.T) {
		t.Parallel()
		var seen atomic.Pointer[net.IP]
		c := newTestClient(t, srv.URL, func(cfg *Config) {
			cfg.DialGuard = func(ip net.IP) error { seen.Store(&ip); return nil }
		})
		if _, err := c.Query(context.Background(), "up", time.Time{}); err != nil {
			t.Fatal(err)
		}
		if ip := seen.Load(); ip == nil || !ip.IsLoopback() {
			t.Fatalf("guard saw %v, want the loopback address", ip)
		}
	})
	t.Run("vetoed before any request is sent", func(t *testing.T) {
		t.Parallel()
		// 独立的服务：断言被否决时一个请求都没有发出。
		var reached atomic.Int32
		private := serve(t, func(http.ResponseWriter, *http.Request) { reached.Add(1) })
		c := newTestClient(t, private.URL, func(cfg *Config) {
			cfg.AuthType, cfg.Secret = AuthBearer, testSecret
			cfg.DialGuard = func(ip net.IP) error { return fmt.Errorf("%s is not allowed (token %s)", ip, testSecret) }
		})
		_, err := c.Query(context.Background(), "up", time.Time{})
		e := asError(t, err)
		if e.ErrorType != ErrTypeBlocked || e.Status != 0 || e.BadQuery() {
			t.Fatalf("err = %+v", e)
		}
		if !strings.Contains(e.Message, "127.0.0.1 is not allowed") || strings.Contains(e.Error(), testSecret) ||
			strings.Contains(e.Error(), private.URL) {
			t.Fatalf("message = %q", e.Error())
		}
		if reached.Load() != 0 {
			t.Fatal("a request reached the server despite the veto")
		}
		if err := c.Ping(context.Background()); asError(t, err).ErrorType != ErrTypeBlocked {
			t.Fatalf("Ping err = %v", err)
		}
	})
	// 检查的是解析后的地址：配置里写主机名，否决依据仍是它解析出的 IP（DNS 重绑定无法绕过）。
	t.Run("guard sees the resolved address of a hostname", func(t *testing.T) {
		t.Parallel()
		var seen atomic.Int32
		c := newTestClient(t, "http://localhost:"+port, func(cfg *Config) {
			cfg.DialGuard = func(ip net.IP) error {
				seen.Add(1)
				if ip.IsLoopback() {
					return errors.New("loopback is not allowed")
				}
				return nil
			}
		})
		_, err := c.Query(context.Background(), "up", time.Time{})
		if e := asError(t, err); e.ErrorType != ErrTypeBlocked || seen.Load() == 0 {
			t.Fatalf("err = %+v, guard calls = %d", e, seen.Load())
		}
	})
	t.Run("guard runs for every new connection", func(t *testing.T) {
		t.Parallel()
		var calls atomic.Int32
		c := newTestClient(t, srv.URL, func(cfg *Config) {
			cfg.DialGuard = func(net.IP) error { calls.Add(1); return nil }
		})
		for range 2 {
			if _, err := c.Query(context.Background(), "up", time.Time{}); err != nil {
				t.Fatal(err)
			}
			c.Close() // 丢弃空闲连接：下一次查询必须重新建连并再次过检查
		}
		if calls.Load() != 2 {
			t.Fatalf("guard calls = %d, want 2", calls.Load())
		}
	})
}

func TestGuardLinkLocal(t *testing.T) {
	t.Parallel()
	for addr, allowed := range map[string]bool{
		// 放行：公网、私网、回环、唯一本地地址。
		"8.8.8.8": true, "10.0.0.1": true, "172.16.5.4": true, "192.168.1.1": true, "100.64.0.1": true,
		"127.0.0.1": true, "::1": true, "2001:db8::1": true, "fd00::1": true, "fd00:ec2::253": true,
		"::ffff:10.0.0.1": true, "::ffff:127.0.0.1": true, "169.253.255.255": true, "169.255.0.0": true,
		// 拒绝：链路本地（含云元数据）、IPv6 元数据、未指定、组播。
		"169.254.169.254": false, "169.254.0.0": false, "169.254.255.255": false, "::ffff:169.254.169.254": false,
		"fe80::1": false, "febf::1": false, "fd00:ec2::254": false,
		"0.0.0.0": false, "::": false, "::ffff:0.0.0.0": false,
		"224.0.0.1": false, "239.255.255.250": false, "ff02::1": false, "ff05::2": false, "::ffff:224.0.0.1": false,
		// R5：不在链路本地段里的云元数据地址（阿里云、Oracle Cloud），及其相邻地址（放行）。
		"100.100.100.200": false, "::ffff:100.100.100.200": false, "192.0.0.192": false, "::ffff:192.0.0.192": false,
		"100.100.100.199": true, "100.100.100.201": true, "192.0.0.191": true, "192.0.0.193": true,
		// R5：NAT64 知名前缀 64:ff9b::/96 按内含的 IPv4 地址判定。
		"64:ff9b::a9fe:a9fe": false, "64:ff9b::169.254.0.1": false, "64:ff9b::6464:64c8": false, "64:ff9b::c000:c0": false,
		"64:ff9b::0.0.0.0": false, "64:ff9b::224.0.0.1": false,
		"64:ff9b::8.8.8.8": true, "64:ff9b::10.0.0.1": true, "64:ff9b::127.0.0.1": true,
		// 前缀之外（/96 以外的位不同）不是 NAT64 映射。
		"64:ff9b:1::a9fe:a9fe": true, "64:ff9c::a9fe:a9fe": true,
	} {
		ip := net.ParseIP(addr)
		if ip == nil {
			t.Fatalf("bad test address %q", addr)
		}
		if err := GuardLinkLocal(ip); (err == nil) != allowed {
			t.Errorf("GuardLinkLocal(%s) = %v, want allowed = %v", addr, err, allowed)
		}
		// 4 字节形式与 16 字节形式结论一致。
		if ip4 := ip.To4(); ip4 != nil {
			if err := GuardLinkLocal(ip4); (err == nil) != allowed {
				t.Errorf("GuardLinkLocal(%s as 4 bytes) = %v, want allowed = %v", addr, err, allowed)
			}
		}
	}
	for _, bad := range []net.IP{nil, {}, {1, 2, 3}} {
		if GuardLinkLocal(bad) == nil {
			t.Errorf("GuardLinkLocal(%v) accepted a malformed address", []byte(bad))
		}
	}
	// 接到客户端上：元数据地址在连接之前被否决（不依赖网络环境，不会真的发起连接）。
	c := newTestClient(t, "http://169.254.169.254", func(cfg *Config) { cfg.DialGuard = GuardLinkLocal })
	start := time.Now()
	_, err := c.Query(context.Background(), "up", time.Time{})
	if e := asError(t, err); e.ErrorType != ErrTypeBlocked || !strings.Contains(e.Message, "169.254.169.254 is not allowed") {
		t.Fatalf("err = %+v", e)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("veto took %v: the connection must not be attempted", time.Since(start))
	}
}
