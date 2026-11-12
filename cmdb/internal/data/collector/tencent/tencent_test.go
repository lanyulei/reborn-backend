package tencent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
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

func TestAuthorization_Deterministic(t *testing.T) {
	t.Parallel()
	now := time.Unix(1700000000, 0)
	a := Authorization("id", "key", "cvm.tencentcloudapi.com", []byte(`{"Limit":100,"Offset":0}`), now)
	b := Authorization("id", "key", "cvm.tencentcloudapi.com", []byte(`{"Limit":100,"Offset":0}`), now)
	if a != b || Authorization("id", "other", "cvm.tencentcloudapi.com", []byte(`{}`), now) == a {
		t.Fatal("signature must be deterministic and key-dependent")
	}
	if a[:16] != "TC3-HMAC-SHA256 " {
		t.Fatalf("unexpected prefix: %s", a)
	}
}

func TestCollector_Discover(t *testing.T) {
	t.Parallel()
	const secret = "sk"
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		ts, _ := strconv.ParseInt(r.Header.Get("X-TC-Timestamp"), 10, 64)
		if r.Header.Get("Authorization") != Authorization("sid", secret, r.Host, body, time.Unix(ts, 0)) {
			_ = json.NewEncoder(w).Encode(map[string]any{"Response": map[string]any{"Error": map[string]string{"Code": "AuthFailure.SignatureFailure", "Message": "bad"}}})
			return
		}
		calls++
		if calls == 1 {
			_ = json.NewEncoder(w).Encode(map[string]any{"Response": map[string]any{"Error": map[string]string{"Code": "RequestLimitExceeded", "Message": "slow"}}})
			return
		}
		var req struct{ Offset, Limit int }
		_ = json.Unmarshal(body, &req)
		set := []map[string]any{}
		switch req.Offset {
		case 0:
			set = append(set, map[string]any{
				"InstanceId": "ins-1", "InstanceName": "web", "CPU": 4, "Memory": 8, "OsName": "TencentOS", "InstanceState": "RUNNING",
				"Placement": map[string]string{"Zone": "ap-guangzhou-3"}, "PrivateIpAddresses": []string{"10.0.0.1"}, "PublicIpAddresses": []string{"1.1.1.1"},
			})
			for i := 2; i <= 100; i++ {
				set = append(set, map[string]any{"InstanceId": fmt.Sprintf("ins-%d", i)})
			}
		case 100:
			set = append(set, map[string]any{"InstanceId": "ins-101"})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"Response": map[string]any{"TotalCount": 101, "InstanceSet": set}})
	}))
	defer srv.Close()
	c := New(2 * time.Second)
	src := &biz.Source{Provider: cmdbcontract.ProviderTencent, Config: []byte(fmt.Sprintf(`{"region":"ap-guangzhou","secret_id":"sid","endpoint":%q}`, srv.URL))}
	sink := &memSink{}
	if err := c.Discover(context.Background(), src, secret, sink); err != nil {
		t.Fatalf("Discover() error = %v", err)
	}
	if len(sink.items) != 101 {
		t.Fatalf("expected 101 resources, got %d", len(sink.items))
	}
	first := sink.items[0]
	if first.ExternalID != "ins-1" || first.Name != "web" || first.Attrs["private_ip"] != "10.0.0.1" || first.Attrs["public_ip"] != "1.1.1.1" ||
		first.Attrs["memory_gb"] != 8 || first.Attrs["zone"] != "ap-guangzhou-3" || first.Attrs["provider"] != "tencent" {
		t.Fatalf("unexpected first resource: %+v", first)
	}
	if sink.items[1].Name != "ins-2" {
		t.Fatal("name must fall back to instance id")
	}
	if err := c.Discover(context.Background(), src, "wrong", &memSink{}); err == nil {
		t.Fatal("wrong secret must fail")
	}
}
