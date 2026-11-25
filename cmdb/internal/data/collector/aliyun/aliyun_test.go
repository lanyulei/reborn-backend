package aliyun

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/cmdb/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/cmdbcontract"
)

type memSink struct{ items []*biz.Resource }

func (s *memSink) Put(_ context.Context, r *biz.Resource) error {
	s.items = append(s.items, r)
	return nil
}

// 文档示例向量：阿里云签名文档（AccessKeySecret=testsecret）给出的 StringToSign 编码规则。
func TestPercentEncode(t *testing.T) {
	t.Parallel()
	if got := percentEncode("a b*c~d/e"); got != "a%20b%2Ac~d%2Fe" {
		t.Fatalf("percentEncode = %q", got)
	}
}

func TestSign_Deterministic(t *testing.T) {
	t.Parallel()
	params := map[string]string{"Action": "DescribeInstances", "RegionId": "cn-hangzhou", "Timestamp": "2026-01-01T00:00:00Z", "SignatureNonce": "n"}
	a, b := Sign("GET", params, "s"), Sign("GET", params, "s")
	if a != b || a == "" {
		t.Fatal("signature must be deterministic")
	}
	if Sign("GET", params, "other") == a {
		t.Fatal("secret must influence signature")
	}
	params["Signature"] = a
	if Sign("GET", params, "s") != a {
		t.Fatal("Signature key must be excluded from canonicalization")
	}
}

// 桩服务校验签名、分页、限流重试与错误响应。
func TestCollector_Discover(t *testing.T) {
	t.Parallel()
	const secret = "sk"
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		params := map[string]string{}
		for k := range q {
			params[k] = q.Get(k)
		}
		if params["AccessKeyId"] != "ak" || params["Action"] != "DescribeInstances" || params["RegionId"] != "cn-hangzhou" {
			http.Error(w, `{"Code":"InvalidAccessKeyId.NotFound","Message":"bad"}`, http.StatusNotFound)
			return
		}
		if Sign(http.MethodGet, params, secret) != params["Signature"] {
			http.Error(w, `{"Code":"SignatureDoesNotMatch","Message":"bad sig"}`, http.StatusBadRequest)
			return
		}
		n := calls.Add(1)
		if n == 1 {
			// 首次限流：须重试。
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"Code":"Throttling","Message":"slow down"}`))
			return
		}
		page := params["PageNumber"]
		pageNo, _ := strconv.Atoi(page)
		body := map[string]any{"TotalCount": 3, "PageNumber": pageNo}
		switch page {
		case "1":
			body["Instances"] = map[string]any{"Instance": []map[string]any{
				{
					"InstanceId": "i-1", "InstanceName": "web-1", "RegionId": "cn-hangzhou", "ZoneId": "cn-hangzhou-h", "InstanceType": "ecs.g6.large", "Cpu": 2, "Memory": 8192, "OSName": "CentOS 7",
					"Status": "Running", "VpcAttributes": map[string]any{"PrivateIpAddress": map[string]any{"IpAddress": []string{"10.0.0.1"}}}, "PublicIpAddress": map[string]any{"IpAddress": []string{"47.1.1.1"}},
				},
				{"InstanceId": "i-2", "InstanceName": "", "Cpu": 4, "Memory": 16384, "InnerIpAddress": map[string]any{"IpAddress": []string{"172.16.0.2"}}, "EipAddress": map[string]any{"IpAddress": "47.2.2.2"}},
			}}
		case "2":
			body["Instances"] = map[string]any{"Instance": []map[string]any{{"InstanceId": "i-3", "InstanceName": "db-1"}}}
		default:
			body["Instances"] = map[string]any{"Instance": []map[string]any{}}
		}
		_ = json.NewEncoder(w).Encode(body)
	}))
	defer srv.Close()

	c := New(2 * time.Second)
	src := &biz.Source{Provider: cmdbcontract.ProviderAliyun, Config: []byte(fmt.Sprintf(`{"region_id":"cn-hangzhou","access_key_id":"ak","endpoint":%q}`, srv.URL))}
	sink := &memSink{}
	if err := c.Discover(context.Background(), src, secret, sink); err != nil {
		t.Fatalf("Discover() error = %v", err)
	}
	if len(sink.items) != 3 {
		t.Fatalf("expected 3 resources, got %d", len(sink.items))
	}
	first := sink.items[0]
	if first.ModelCode != cmdbcontract.ModelCloudHost || first.ExternalID != "i-1" || first.Name != "web-1" ||
		first.Attrs["private_ip"] != "10.0.0.1" || first.Attrs["public_ip"] != "47.1.1.1" || first.Attrs["memory_gb"] != 8 || first.Attrs["cpu_cores"] != 2 {
		t.Fatalf("unexpected first resource: %+v", first)
	}
	second := sink.items[1]
	if second.Name != "i-2" || second.Attrs["private_ip"] != "172.16.0.2" || second.Attrs["public_ip"] != "47.2.2.2" || second.Attrs["region"] != "cn-hangzhou" {
		t.Fatalf("unexpected second resource: %+v", second)
	}
	// 错误凭据：非重试错误直接返回。
	if err := c.Discover(context.Background(), src, "wrong", &memSink{}); err == nil {
		t.Fatal("wrong secret must fail")
	}
}

// 请求失败（连接被拒 / DNS / 超时）时，错误信息里不得带签名参数：它会落进任务 / 运行记录并随失败
// 通知外发。主机与路径保留（排障要用），错误链不变。
func TestCollector_RequestErrorRedactsSignedQuery(t *testing.T) {
	t.Parallel()
	// 起一个立刻关掉的服务：端口确定无人监听，请求必然在连接阶段失败。
	srv := httptest.NewServer(http.NotFoundHandler())
	endpoint := srv.URL
	srv.Close()

	c := New(time.Second)
	src := &biz.Source{Provider: cmdbcontract.ProviderAliyun, Config: []byte(fmt.Sprintf(`{"region_id":"cn-hangzhou","access_key_id":"AKIDEXAMPLE","endpoint":%q}`, endpoint))}
	err := c.Discover(context.Background(), src, "sk", &memSink{})
	if err == nil {
		t.Fatal("request to a closed port must fail")
	}
	msg := err.Error()
	for _, leaked := range []string{"AKIDEXAMPLE", "Signature", "SignatureNonce", "AccessKeyId", "?"} {
		if strings.Contains(msg, leaked) {
			t.Errorf("error must not carry %q: %s", leaked, msg)
		}
	}
	if !strings.Contains(msg, strings.TrimPrefix(endpoint, "http://")) {
		t.Errorf("error should keep the host for troubleshooting: %s", msg)
	}
	var ue *url.Error
	if !errors.As(err, &ue) {
		t.Fatalf("error chain must keep *url.Error, got %T: %v", err, err)
	}
}

func TestRedactQuery(t *testing.T) {
	t.Parallel()
	inner := errors.New("dial tcp: i/o timeout")
	got := redactQuery(&url.Error{Op: "Get", URL: "https://ecs.aliyuncs.com/?AccessKeyId=ak&Signature=sig", Err: inner})
	if got.Error() != `Get "https://ecs.aliyuncs.com/": dial tcp: i/o timeout` || !errors.Is(got, inner) {
		t.Fatalf("redactQuery() = %v", got)
	}
	// 没有 query 的 URL 错误与其它错误原样返回。
	plain := &url.Error{Op: "Get", URL: "https://ecs.aliyuncs.com/", Err: inner}
	if redactQuery(plain) != error(plain) || redactQuery(inner) != inner { //nolint:errorlint // 断言原样返回（同一指针）
		t.Fatal("errors without a query must pass through unchanged")
	}
}
