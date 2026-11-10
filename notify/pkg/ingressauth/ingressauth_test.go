package ingressauth

import (
	"strings"
	"testing"
	"time"
)

func TestHeaderRoundTrip(t *testing.T) {
	t.Parallel()
	body := []byte(`{"events":[{"biz_id":"monitor"}]}`)
	now := time.Unix(1_800_000_000, 0)
	header := Header("prod-1", "npt_secret", "POST", "/ingress/v1/events:batch", body, now)
	if !strings.HasPrefix(header, "HMAC prod-1:") || !strings.HasSuffix(header, ":1800000000") {
		t.Fatalf("header = %q", header)
	}
	cred, err := Parse(header)
	if err != nil || cred.ProducerID != "prod-1" || cred.Timestamp != now.Unix() {
		t.Fatalf("Parse() = %+v, %v", cred, err)
	}
	key := DeriveKey("npt_secret")
	if !Verify(key, Canonical(cred.Timestamp, "POST", "/ingress/v1/events:batch", body), cred.Signature) {
		t.Fatal("Verify() = false, want true")
	}
	// 任一要素变化即失败：body / path / method / ts / key。
	for name, c := range map[string]string{
		"body":   Canonical(cred.Timestamp, "POST", "/ingress/v1/events:batch", []byte("x")),
		"path":   Canonical(cred.Timestamp, "POST", "/ingress/v1/events", body),
		"method": Canonical(cred.Timestamp, "PUT", "/ingress/v1/events:batch", body),
		"ts":     Canonical(cred.Timestamp+1, "POST", "/ingress/v1/events:batch", body),
	} {
		if Verify(key, c, cred.Signature) {
			t.Errorf("%s tampered but verified", name)
		}
	}
	if Verify(DeriveKey("npt_other"), Canonical(cred.Timestamp, "POST", "/ingress/v1/events:batch", body), cred.Signature) {
		t.Error("wrong key verified")
	}
}

func TestParseMalformed(t *testing.T) {
	t.Parallel()
	for _, h := range []string{"", "Bearer x", "HMAC", "HMAC a:b", "HMAC :sig:1", "HMAC a::1", "HMAC a:b:notanumber", "HMAC a:b:c:d"} {
		if _, err := Parse(h); err == nil {
			t.Errorf("Parse(%q) error = nil, want ErrMalformed", h)
		}
	}
}
