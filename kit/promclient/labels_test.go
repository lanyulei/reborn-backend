package promclient

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

func TestMetricNames(t *testing.T) {
	t.Parallel()
	seen := map[string]string{}
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		seen["path"] = r.URL.Path
		seen["limit"] = r.URL.Query().Get("limit")
		_, _ = w.Write([]byte(`{"status":"success","data":["alice_metric","bob_metric","carol_metric"]}`))
	})
	c := newTestClient(t, srv.URL, nil)

	got, err := c.MetricNames(context.Background(), 2)
	if err != nil {
		t.Fatalf("MetricNames: %v", err)
	}
	if strings.Join(got, ",") != "alice_metric,bob_metric" {
		t.Fatalf("names = %v, want prefix-truncated list", got)
	}
	if seen["path"] != "/api/v1/label/__name__/values" {
		t.Fatalf("path = %q", seen["path"])
	}
	if seen["limit"] != "2" {
		t.Fatalf("limit param = %q", seen["limit"])
	}

	// limit ≤ 0 不带 limit 参数（旧版本数据源不识别该参数时也能工作）
	if _, err = c.MetricNames(context.Background(), 0); err != nil {
		t.Fatalf("MetricNames(0): %v", err)
	}
	if seen["limit"] != "" {
		t.Fatalf("limit param sent for limit=0: %q", seen["limit"])
	}
}

func TestLabelNames(t *testing.T) {
	t.Parallel()
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/labels" {
			t.Errorf("path = %q", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"status":"success","data":["job","instance"]}`))
	})
	c := newTestClient(t, srv.URL, nil)

	got, err := c.LabelNames(context.Background(), 0)
	if err != nil {
		t.Fatalf("LabelNames: %v", err)
	}
	if strings.Join(got, ",") != "job,instance" {
		t.Fatalf("labels = %v", got)
	}
}

func TestLabelValues(t *testing.T) {
	t.Parallel()
	seen := map[string]string{}
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		seen["path"] = r.URL.Path
		_, _ = w.Write([]byte(`{"status":"success","data":["prod-01","prod-02"]}`))
	})
	c := newTestClient(t, srv.URL, nil)

	got, err := c.LabelValues(context.Background(), "cluster", 0)
	if err != nil {
		t.Fatalf("LabelValues: %v", err)
	}
	if strings.Join(got, ",") != "prod-01,prod-02" {
		t.Fatalf("values = %v", got)
	}
	if seen["path"] != "/api/v1/label/cluster/values" {
		t.Fatalf("path = %q", seen["path"])
	}

	// 非法标签名（含路径穿越）直接拒绝，不发请求
	if _, err = c.LabelValues(context.Background(), "../secrets", 0); err == nil {
		t.Fatal("want error for invalid label name")
	}
	if _, err = c.LabelValues(context.Background(), "a/b", 0); err == nil {
		t.Fatal("want error for slash in label name")
	}
}

func TestLabelsUpstreamErrors(t *testing.T) {
	t.Parallel()
	// 上游 5xx 带 Prometheus 错误结构 → 透传错误类型与说明
	srv := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"status":"error","errorType":"timeout","error":"query timed out"}`))
	})
	c := newTestClient(t, srv.URL, nil)
	_, err := c.MetricNames(context.Background(), 0)
	if err == nil {
		t.Fatal("want error")
	}
	e := asError(t, err)
	if e.ErrorType != "timeout" || !strings.Contains(e.Message, "timed out") {
		t.Fatalf("err = %+v", e)
	}

	// 2xx 但 status 非 success/error → 固定文案，不回显响应正文
	srv2 := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"status":"weird","secret":"top-secret"}`))
	})
	c2 := newTestClient(t, srv2.URL, nil)
	_, err = c2.LabelNames(context.Background(), 0)
	if err == nil {
		t.Fatal("want error")
	}
	e = asError(t, err)
	if e.ErrorType != ErrTypeBadResponse || strings.Contains(e.Message, "top-secret") {
		t.Fatalf("err = %+v", e)
	}
}
