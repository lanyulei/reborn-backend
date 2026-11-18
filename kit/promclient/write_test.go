package promclient

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
)

// snappyDecode 按格式说明独立实现的 Snappy 块解码器（含 1 / 2 / 4 字节偏移的拷贝），用来验证编码器的输出。
func snappyDecode(t testing.TB, src []byte) []byte {
	t.Helper()
	want, n := binary.Uvarint(src)
	if n <= 0 {
		t.Fatalf("snappy: bad length preamble")
	}
	src = src[n:]
	out := make([]byte, 0, want)
	for len(src) > 0 {
		tag := src[0]
		var length, offset, used int
		switch tag & 3 {
		case 0:
			length = int(tag>>2) + 1
			used = 1
			if extra := int(tag>>2) - 59; extra > 0 {
				length = 0
				for i := range extra {
					length |= int(src[1+i]) << (8 * i)
				}
				length++
				used = 1 + extra
			}
			if used+length > len(src) {
				t.Fatalf("snappy: literal overruns the input")
			}
			out = append(out, src[used:used+length]...)
			src = src[used+length:]
			continue
		case 1:
			length, offset, used = int(tag>>2&7)+4, int(tag>>5)<<8|int(src[1]), 2
		case 2:
			length, offset, used = int(tag>>2)+1, int(binary.LittleEndian.Uint16(src[1:])), 3
		case 3:
			length, offset, used = int(tag>>2)+1, int(binary.LittleEndian.Uint32(src[1:])), 5
		}
		if offset == 0 || offset > len(out) {
			t.Fatalf("snappy: copy offset %d out of range (decoded %d)", offset, len(out))
		}
		for range length { // 逐字节：拷贝可以与自身重叠
			out = append(out, out[len(out)-offset])
		}
		src = src[used:]
	}
	if uint64(len(out)) != want {
		t.Fatalf("snappy: decoded %d bytes, preamble says %d", len(out), want)
	}
	return out
}

func TestSnappyRoundTrip(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // G404：测试数据要可重现，不是安全用途
	random := make([]byte, 200_000)
	for i := 0; i < len(random); i += 8 {
		binary.LittleEndian.PutUint64(random[i:], rng.Uint64())
	}
	lowEntropy := make([]byte, 300_000)
	for i := range lowEntropy {
		lowEntropy[i] = "abc"[rng.IntN(3)]
	}
	far := append(append(bytes.Repeat([]byte("0123456789abcdef"), 4), random[:70_000]...), bytes.Repeat([]byte("0123456789abcdef"), 4)...)
	cases := map[string][]byte{
		"empty":             {},
		"one byte":          []byte("a"),
		"three bytes":       []byte("abc"),
		"four bytes":        []byte("abcd"),
		"no repeats":        []byte("abcdefghijklmnopqrstuvwxyz0123456789"),
		"run of one byte":   bytes.Repeat([]byte("a"), 10_000), // 与自身重叠的拷贝
		"short period":      bytes.Repeat([]byte("ab"), 5_000),
		"long period":       bytes.Repeat([]byte("instance=\"10.0.0.1:9100\",job=\"node\";"), 3_000),
		"literal of 60":     random[:60],
		"literal of 61":     random[:61],
		"literal of 256":    random[:256],
		"literal of 257":    random[:257],
		"literal of 65536":  random[:65536],
		"literal of 65537":  random[:65537],
		"random":            random,
		"low entropy":       lowEntropy,
		"match beyond 64KB": far, // 超出回溯距离的重复不得编码成拷贝
		"match of 64":       append(bytes.Repeat([]byte("x"), 68), random[:10]...),
		"match of 65":       append(bytes.Repeat([]byte("x"), 69), random[:10]...),
		"trailing literal":  append(bytes.Repeat([]byte("abcd"), 100), []byte("xyz")...),
	}
	for name, src := range cases {
		enc := snappyEncode(nil, src)
		if got := snappyDecode(t, enc); !bytes.Equal(got, src) {
			t.Errorf("%s: round trip mismatch (%d bytes in, %d out)", name, len(src), len(got))
		}
	}
	if enc := snappyEncode(nil, cases["long period"]); len(enc)*10 > len(cases["long period"]) {
		t.Errorf("repetitive input compressed to %d of %d bytes", len(enc), len(cases["long period"]))
	}
	// 不可压缩的输入只多出字面量头与长度前缀。
	if enc := snappyEncode(nil, random); len(enc) > len(random)+16 {
		t.Errorf("random input grew to %d of %d bytes", len(enc), len(random))
	}
	// 追加到已有内容之后。
	if enc := snappyEncode([]byte("head"), []byte("abcdabcdabcd")); !bytes.HasPrefix(enc, []byte("head")) ||
		!bytes.Equal(snappyDecode(t, enc[4:]), []byte("abcdabcdabcd")) {
		t.Error("snappyEncode must append to dst")
	}
	for range 300 {
		n := rng.IntN(5000)
		src := make([]byte, n)
		alphabet := 1 + rng.IntN(8)
		for i := range src {
			src[i] = byte(rng.IntN(alphabet)) //nolint:gosec // G115：alphabet 至多 8
		}
		if got := snappyDecode(t, snappyEncode(nil, src)); !bytes.Equal(got, src) {
			t.Fatalf("random case (len %d, alphabet %d): round trip mismatch", n, alphabet)
		}
	}
}

type decodedSample struct {
	value float64
	ms    int64
}

type decodedSeries struct {
	labels  [][2]string
	samples []decodedSample
}

// decodeWriteRequest 解出 prometheus.WriteRequest 的序列（按线格式逐字段解，任何多余字段都算错）。
func decodeWriteRequest(t testing.TB, b []byte) []decodedSeries {
	t.Helper()
	fields := func(b []byte, fn func(num protowire.Number, typ protowire.Type, b []byte) int) {
		for len(b) > 0 {
			num, typ, n := protowire.ConsumeTag(b)
			if n < 0 {
				t.Fatalf("protobuf: bad tag")
			}
			b = b[n:]
			used := fn(num, typ, b)
			if used < 0 {
				t.Fatalf("protobuf: bad field %d (wire type %d)", num, typ)
			}
			b = b[used:]
		}
	}
	var out []decodedSeries
	fields(b, func(num protowire.Number, typ protowire.Type, b []byte) int {
		if num != 1 || typ != protowire.BytesType {
			return -1
		}
		raw, n := protowire.ConsumeBytes(b)
		var s decodedSeries
		fields(raw, func(num protowire.Number, typ protowire.Type, b []byte) int {
			if typ != protowire.BytesType {
				return -1
			}
			sub, n := protowire.ConsumeBytes(b)
			switch num {
			case 1:
				var l [2]string
				fields(sub, func(num protowire.Number, typ protowire.Type, b []byte) int {
					if typ != protowire.BytesType || num < 1 || num > 2 {
						return -1
					}
					v, n := protowire.ConsumeString(b)
					l[num-1] = v
					return n
				})
				s.labels = append(s.labels, l)
			case 2:
				var p decodedSample
				fields(sub, func(num protowire.Number, typ protowire.Type, b []byte) int {
					switch {
					case num == 1 && typ == protowire.Fixed64Type:
						v, n := protowire.ConsumeFixed64(b)
						p.value = math.Float64frombits(v)
						return n
					case num == 2 && typ == protowire.VarintType:
						v, n := protowire.ConsumeVarint(b)
						p.ms = int64(v) //nolint:gosec // 补码还原
						return n
					}
					return -1
				})
				s.samples = append(s.samples, p)
			default:
				return -1
			}
			return n
		})
		out = append(out, s)
		return n
	})
	return out
}

func TestWrite(t *testing.T) {
	t.Parallel()
	var got struct {
		method, path, query string
		header              http.Header
		series              []decodedSeries
	}
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		got.method, got.path, got.query, got.header = r.Method, r.URL.Path, r.URL.RawQuery, r.Header.Clone()
		got.series = decodeWriteRequest(t, snappyDecode(t, body))
		w.WriteHeader(http.StatusNoContent)
	})
	c := newTestClient(t, srv.URL, func(cfg *Config) {
		cfg.WriteURL = srv.URL + "/insert/0/prometheus/api/v1/write?extra_label=source%3Dreborn"
		cfg.AuthType, cfg.Username, cfg.Secret = AuthBasic, "writer", "s3cret"
		cfg.Headers = map[string]string{"X-Scope-OrgID": "tenant-1"}
	})
	ts := time.UnixMilli(1_700_000_000_123).Add(456 * time.Microsecond) // 亚毫秒部分被截断
	err := c.Write(context.Background(), []Series{
		{
			Labels: map[string]string{"job": "node", "__name__": "job:cpu:avg5m", "instance": "10.0.0.1:9100", "empty": "", "区域": "华东"},
			Points: []Point{{Time: ts, Value: 0.25}, {Time: ts.Add(time.Minute), Value: math.Inf(1)}},
		},
		{Labels: map[string]string{"__name__": "skipped_without_points"}},
		{Labels: map[string]string{"__name__": "stale"}, Points: []Point{{Time: time.UnixMilli(-5), Value: math.NaN()}}},
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got.method != http.MethodPost || got.path != "/insert/0/prometheus/api/v1/write" || got.query != "extra_label=source%3Dreborn" {
		t.Errorf("request line: %s %s?%s", got.method, got.path, got.query)
	}
	for name, want := range map[string]string{
		"Content-Type": "application/x-protobuf", "Content-Encoding": "snappy", "X-Prometheus-Remote-Write-Version": "0.1.0",
		"X-Scope-Orgid": "tenant-1",
	} {
		if v := got.header.Get(name); v != want {
			t.Errorf("header %s = %q, want %q", name, v, want)
		}
	}
	req := &http.Request{Header: got.header}
	if user, pass, ok := req.BasicAuth(); !ok || user != "writer" || pass != "s3cret" {
		t.Error("write request must carry the datasource credentials")
	}
	if len(got.series) != 2 {
		t.Fatalf("decoded %d series, want 2 (the one without points is skipped)", len(got.series))
	}
	first := got.series[0]
	// 标签按名称（字节序）排序，空值标签被省略。
	wantLabels := [][2]string{{"__name__", "job:cpu:avg5m"}, {"instance", "10.0.0.1:9100"}, {"job", "node"}, {"区域", "华东"}}
	if len(first.labels) != len(wantLabels) {
		t.Fatalf("labels = %v", first.labels)
	}
	for i, l := range wantLabels {
		if first.labels[i] != l {
			t.Errorf("label %d = %v, want %v", i, first.labels[i], l)
		}
	}
	if len(first.samples) != 2 || first.samples[0] != (decodedSample{0.25, 1_700_000_000_123}) ||
		!math.IsInf(first.samples[1].value, 1) || first.samples[1].ms != 1_700_000_060_123 {
		t.Errorf("samples = %+v", first.samples)
	}
	if s := got.series[1]; len(s.samples) != 1 || !math.IsNaN(s.samples[0].value) || s.samples[0].ms != -5 {
		t.Errorf("NaN / negative timestamp sample = %+v", s.samples)
	}
}

func TestWriteSkipsEmptyAndValidates(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	srv := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	})
	c := newTestClient(t, srv.URL, func(cfg *Config) { cfg.WriteURL = srv.URL + "/api/v1/write" })
	ctx := context.Background()
	for name, series := range map[string][]Series{
		"nil":            nil,
		"no points":      {{Labels: map[string]string{"__name__": "a"}}},
		"only empty one": {{Labels: map[string]string{}}, {}},
	} {
		if err := c.Write(ctx, series); err != nil {
			t.Errorf("%s: Write = %v, want nil", name, err)
		}
	}
	pt := []Point{{Time: time.Unix(1, 0), Value: 1}}
	for name, series := range map[string][]Series{
		"no metric name":    {{Labels: map[string]string{"job": "x"}, Points: pt}},
		"empty metric name": {{Labels: map[string]string{"__name__": ""}, Points: pt}},
		"nil labels":        {{Points: pt}},
		"empty label name":  {{Labels: map[string]string{"__name__": "a", "": "x"}, Points: pt}},
		"invalid utf8 name": {{Labels: map[string]string{"__name__": "a", "\xff": "x"}, Points: pt}},
		"invalid utf8":      {{Labels: map[string]string{"__name__": "a", "k": "\xff"}, Points: pt}},
	} {
		e := asError(t, c.Write(ctx, series))
		if e.ErrorType != ErrTypeBadData || e.Status != 0 {
			t.Errorf("%s: %v", name, e)
		}
	}
	// 请求体上限：不发请求。
	big := Series{Labels: map[string]string{"__name__": "a", "pad": strings.Repeat("x", 1<<20)}, Points: pt}
	many := make([]Series, 17)
	for i := range many {
		many[i] = big
	}
	if e := asError(t, c.Write(ctx, many)); e.ErrorType != ErrTypeBadData || !strings.Contains(e.Message, "size limit") {
		t.Errorf("oversized request: %v", e)
	}
	if n := calls.Load(); n != 0 {
		t.Errorf("%d requests were sent, want none", n)
	}
	// 未配置写入地址。
	ro := newTestClient(t, srv.URL, nil)
	if e := asError(t, ro.Write(ctx, []Series{{Labels: map[string]string{"__name__": "a"}, Points: pt}})); e.ErrorType != ErrTypeBadData ||
		!strings.Contains(e.Message, "not configured") {
		t.Errorf("no write url: %v", e)
	}
}

func TestWriteErrors(t *testing.T) {
	t.Parallel()
	series := []Series{{Labels: map[string]string{"__name__": "a"}, Points: []Point{{Time: time.Unix(1, 0), Value: 1}}}}
	const secret = "tok-4f9a7c21" //nolint:gosec // G101：测试用的假令牌，用来断言它不出现在错误信息里
	for _, tc := range []struct {
		name     string
		status   int
		body     string
		wantType string
		wantMsg  string
	}{
		{"bad request", 400, `out of order sample; token=` + secret, ErrTypeHTTP, "Bad Request"},
		{"too many requests", 429, "slow down", ErrTypeHTTP, "Too Many Requests"},
		{"server error with a prometheus-like body", 500, `{"status":"error","errorType":"internal","error":"boom ` + secret + `"}`, ErrTypeHTTP, "Internal Server Error"},
		{"bad gateway html", 502, "<html>internal page</html>", ErrTypeHTTP, "Bad Gateway"},
		{"redirect", 307, "", ErrTypeRedirect, "redirects are not followed"},
		{"non-standard status", 599, "x", ErrTypeHTTP, "unexpected HTTP status"},
	} {
		var hits atomic.Int32
		srv := serve(t, func(w http.ResponseWriter, _ *http.Request) {
			hits.Add(1)
			if tc.status == 307 {
				w.Header().Set("Location", "http://127.0.0.1:1/elsewhere")
			}
			w.WriteHeader(tc.status)
			_, _ = io.WriteString(w, tc.body)
		})
		c := newTestClient(t, srv.URL, func(cfg *Config) {
			cfg.WriteURL, cfg.AuthType, cfg.Secret = srv.URL+"/api/v1/write", AuthBearer, secret
		})
		e := asError(t, c.Write(context.Background(), series))
		if e.ErrorType != tc.wantType || e.Status != tc.status || !strings.Contains(e.Message, tc.wantMsg) {
			t.Errorf("%s: %v", tc.name, e)
		}
		// 响应体与凭据都不进错误信息。
		if strings.Contains(e.Error(), secret) || strings.Contains(e.Error(), "boom") || strings.Contains(e.Error(), "internal page") ||
			strings.Contains(e.Error(), "out of order") {
			t.Errorf("%s: error leaks the response body or the credential: %v", tc.name, e)
		}
		if hits.Load() != 1 {
			t.Errorf("%s: %d requests, want exactly 1 (no retry, no redirect follow)", tc.name, hits.Load())
		}
	}
	for _, status := range []int{200, 202, 204} {
		srv := serve(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) })
		c := newTestClient(t, srv.URL, func(cfg *Config) { cfg.WriteURL = srv.URL })
		if err := c.Write(context.Background(), series); err != nil {
			t.Errorf("status %d: %v", status, err)
		}
	}
}

func TestWriteTimeoutCancelAndGuard(t *testing.T) {
	t.Parallel()
	series := []Series{{Labels: map[string]string{"__name__": "a"}, Points: []Point{{Time: time.Unix(1, 0), Value: 1}}}}
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	slow := serve(t, func(w http.ResponseWriter, r *http.Request) {
		// 先读完请求体：服务端读到 EOF 后才开始监测连接断开，r.Context() 才会随客户端放弃而结束。
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-release:
		case <-r.Context().Done():
		}
		w.WriteHeader(http.StatusNoContent)
	})
	c := newTestClient(t, slow.URL, func(cfg *Config) { cfg.WriteURL, cfg.Timeout = slow.URL, 50*time.Millisecond })
	e := asError(t, c.Write(context.Background(), series))
	if e.ErrorType != ErrTypeTimeout || !errors.Is(e, context.DeadlineExceeded) {
		t.Errorf("timeout: %v", e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	long := newTestClient(t, slow.URL, func(cfg *Config) { cfg.WriteURL = slow.URL })
	if e := asError(t, long.Write(ctx, series)); e.ErrorType != ErrTypeCanceled || !errors.Is(e, context.Canceled) {
		t.Errorf("cancel: %v", e)
	}

	// DialGuard 同样约束写入地址：检查的是解析后要连接的地址。
	var hits atomic.Int32
	srv := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusNoContent)
	})
	guarded := newTestClient(t, "http://prometheus.invalid:9090", func(cfg *Config) {
		cfg.WriteURL = srv.URL + "/api/v1/write"
		cfg.DialGuard = func(ip net.IP) error {
			if ip.IsLoopback() {
				return errors.New("loopback addresses are not allowed")
			}
			return nil
		}
	})
	e = asError(t, guarded.Write(context.Background(), series))
	if e.ErrorType != ErrTypeBlocked || !strings.Contains(e.Message, "loopback addresses are not allowed") || hits.Load() != 0 {
		t.Errorf("dial guard: %v (hits %d)", e, hits.Load())
	}
	// 连不上：错误信息里没有凭据。
	const secret = "pw-77aa31" //nolint:gosec // G101：测试用的假口令，用来断言它不出现在错误信息里
	dead := newTestClient(t, "http://127.0.0.1:1", func(cfg *Config) {
		cfg.WriteURL, cfg.AuthType, cfg.Username, cfg.Secret, cfg.Timeout = "http://127.0.0.1:1/write", AuthBasic, "u", secret, time.Second
	})
	if e := asError(t, dead.Write(context.Background(), series)); e.ErrorType != ErrTypeNetwork || strings.Contains(e.Error(), secret) {
		t.Errorf("network error: %v", e)
	}
}

func TestWriteURLValidation(t *testing.T) {
	t.Parallel()
	for raw, ok := range map[string]bool{
		"http://vminsert:8480/insert/0/prometheus/api/v1/write":   true,
		"https://prom.example.com/api/v1/write?extra_label=a%3Db": true,
		" http://prom:9090/api/v1/write ":                         true,
		"http://prom:9090":                                        true,
		"":                                                        false,
		"ftp://prom/api/v1/write":                                 false,
		"prom:9090/api/v1/write":                                  false,
		"http://user:pass@prom:9090/api/v1/write":                 false,
		"http://prom:9090/api/v1/write#frag":                      false,
		"http:///api/v1/write":                                    false,
		"http://[::1/api/v1/write":                                false,
	} {
		err := ValidateWriteURL(raw)
		if (err == nil) != ok {
			t.Errorf("ValidateWriteURL(%q) = %v, want ok=%v", raw, err, ok)
		}
		if err != nil && strings.Contains(err.Error(), "pass") {
			t.Errorf("ValidateWriteURL(%q) echoes the address: %v", raw, err)
		}
		// New 与 ValidateWriteURL 同一口径（空串对 New 表示「不可写」）。
		if raw == "" {
			continue
		}
		if _, err := New(Config{BaseURL: "http://prom:9090", WriteURL: raw}); (err == nil) != ok {
			t.Errorf("New(WriteURL=%q) = %v, want ok=%v", raw, err, ok)
		}
	}
	if _, err := New(Config{BaseURL: "http://prom:9090", WriteURL: "   "}); err != nil {
		t.Errorf("blank write url must mean read-only: %v", err)
	}
}

func BenchmarkWriteEncode(b *testing.B) {
	now := time.Unix(1_700_000_000, 0)
	series := make([]Series, 1000)
	for i := range series {
		series[i] = Series{
			Labels: map[string]string{
				"__name__": "job:node_cpu_seconds:rate5m", "instance": "10.0." + string(rune('0'+i/250)) + "." + string(rune('0'+i%10)) + ":9100",
				"job": "node", "cpu": string(rune('0' + i%8)), "mode": "idle", "env": "prod",
			},
			Points: []Point{{Time: now, Value: float64(i) / 7}},
		}
	}
	b.ReportAllocs()
	var raw, packed int
	for b.Loop() {
		payload, err := encodeWriteRequest(series)
		if err != nil {
			b.Fatal(err)
		}
		body := snappyEncode(make([]byte, 0, len(payload)/4+64), payload)
		raw, packed = len(payload), len(body)
	}
	b.ReportMetric(float64(raw)/float64(packed), "ratio")
}
