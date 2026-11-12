package aws

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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

// AWS SigV4 官方测试向量 get-vanilla（AKIDEXAMPLE / us-east-1 / service / 2015-08-30T12:36:00Z）。
func TestAuthorization_OfficialVector(t *testing.T) {
	t.Parallel()
	now := time.Date(2015, 8, 30, 12, 36, 0, 0, time.UTC)
	got := Authorization(http.MethodGet, "/", "", map[string]string{"host": "example.amazonaws.com", "x-amz-date": "20150830T123600Z"},
		sha256Hex(nil), now, "us-east-1", "service", "AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY")
	want := "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
	if got != want {
		t.Fatalf("Authorization =\n%s\nwant\n%s", got, want)
	}
}

const page1 = `<?xml version="1.0" encoding="UTF-8"?>
<DescribeInstancesResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/">
  <requestId>r1</requestId>
  <reservationSet><item><instancesSet>
    <item><instanceId>i-1</instanceId><instanceType>t3.micro</instanceType><platformDetails>Linux/UNIX</platformDetails>
      <privateIpAddress>10.0.0.1</privateIpAddress><ipAddress>3.3.3.3</ipAddress><instanceState><name>running</name></instanceState>
      <placement><availabilityZone>us-east-1a</availabilityZone></placement><cpuOptions><coreCount>2</coreCount><threadsPerCore>2</threadsPerCore></cpuOptions>
      <tagSet><item><key>Name</key><value>web-1</value></item></tagSet></item>
    <item><instanceId>i-2</instanceId><instanceState><name>stopped</name></instanceState></item>
  </instancesSet></item></reservationSet>
  <nextToken>tok2</nextToken>
</DescribeInstancesResponse>`

const page2 = `<DescribeInstancesResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/">
  <reservationSet><item><instancesSet><item><instanceId>i-3</instanceId></item></instancesSet></item></reservationSet>
</DescribeInstancesResponse>`

func TestCollector_Discover(t *testing.T) {
	t.Parallel()
	const secret = "sk"
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		now, err := time.Parse("20060102T150405Z", r.Header.Get("X-Amz-Date"))
		if err != nil {
			http.Error(w, "bad date", http.StatusBadRequest)
			return
		}
		headers := map[string]string{"content-type": r.Header.Get("Content-Type"), "host": r.Host, "x-amz-date": r.Header.Get("X-Amz-Date")}
		if r.Header.Get("Authorization") != Authorization(http.MethodPost, "/", "", headers, sha256Hex(body), now, "us-east-1", "ec2", "AKID", secret) {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`<Response><Errors><Error><Code>AuthFailure</Code><Message>bad sig</Message></Error></Errors></Response>`))
			return
		}
		calls++
		if calls == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if strings.Contains(string(body), "NextToken=tok2") {
			_, _ = w.Write([]byte(page2))
			return
		}
		_, _ = w.Write([]byte(page1))
	}))
	defer srv.Close()
	c := New(2 * time.Second)
	src := &biz.Source{Provider: cmdbcontract.ProviderAWS, Config: []byte(fmt.Sprintf(`{"region":"us-east-1","access_key_id":"AKID","endpoint":%q}`, srv.URL))}
	sink := &memSink{}
	if err := c.Discover(context.Background(), src, secret, sink); err != nil {
		t.Fatalf("Discover() error = %v", err)
	}
	if len(sink.items) != 3 {
		t.Fatalf("expected 3 resources, got %d", len(sink.items))
	}
	first := sink.items[0]
	if first.Name != "web-1" || first.ExternalID != "i-1" || first.Attrs["private_ip"] != "10.0.0.1" || first.Attrs["public_ip"] != "3.3.3.3" ||
		first.Attrs["cpu_cores"] != 4 || first.Attrs["zone"] != "us-east-1a" || first.Attrs["power_state"] != "running" || first.Attrs["os"] != "Linux/UNIX" {
		t.Fatalf("unexpected first resource: %+v", first)
	}
	if sink.items[1].Name != "i-2" || sink.items[2].ExternalID != "i-3" {
		t.Fatalf("unexpected resources: %+v %+v", sink.items[1], sink.items[2])
	}
	if err := c.Discover(context.Background(), src, "wrong", &memSink{}); err == nil || !strings.Contains(err.Error(), "AuthFailure") {
		t.Fatalf("wrong secret must fail with AuthFailure, got %v", err)
	}
}
