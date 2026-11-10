package biz

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/notify/pkg/ingressauth"
)

func TestProducerAuth_Verify(t *testing.T) {
	t.Parallel()
	const token = "npt_test-token" //nolint:gosec // 测试桩凭据，非真实密钥
	now := time.Unix(1_800_000_000, 0)
	expired := now.Add(-time.Hour)
	producers := []*Producer{
		{ID: "p-ok", BizID: "monitor", Enabled: true, TokenHash: ingressauth.DeriveKey(token)},
		{ID: "p-off", BizID: "monitor", Enabled: false, TokenHash: ingressauth.DeriveKey(token)},
		{ID: "p-exp", BizID: "monitor", Enabled: true, TokenHash: ingressauth.DeriveKey(token), ExpireTime: &expired},
	}
	snap := NewSnapshot(ConfigVersions{}, nil, nil, nil, nil, producers, now)
	auth := NewProducerAuth(newTestCache(t, snap))
	auth.now = func() time.Time { return now }
	body := []byte(`{"event":{}}`)
	sign := func(id string, at time.Time) string {
		return ingressauth.Header(id, token, "POST", "/ingress/v1/events", body, at)
	}
	p, err := auth.Verify(context.Background(), sign("p-ok", now), "POST", "/ingress/v1/events", body)
	if err != nil || p == nil || p.ID != "p-ok" {
		t.Fatalf("Verify(ok) = %v, %v", p, err)
	}
	cases := map[string]string{
		"disabled producer": sign("p-off", now),
		"expired producer":  sign("p-exp", now),
		"unknown producer":  sign("p-none", now),
		"stale timestamp":   sign("p-ok", now.Add(-6*time.Minute)),
		"future timestamp":  sign("p-ok", now.Add(6*time.Minute)),
		"wrong token":       ingressauth.Header("p-ok", "npt_other", "POST", "/ingress/v1/events", body, now),
		"malformed":         "Bearer abc",
	}
	for name, header := range cases {
		if _, err := auth.Verify(context.Background(), header, "POST", "/ingress/v1/events", body); !errors.Is(err, ErrUnauthorized) {
			t.Errorf("%s: error = %v, want ErrUnauthorized", name, err)
		}
	}
	if _, err := auth.Verify(context.Background(), sign("p-ok", now), "POST", "/ingress/v1/events", []byte("tampered")); !errors.Is(err, ErrUnauthorized) {
		t.Errorf("tampered body: error = %v", err)
	}
}

func TestProducerAuthorize(t *testing.T) {
	t.Parallel()
	p := &Producer{BizID: "monitor", EventWhitelist: []string{"monitor.alert.*", "resource.change"}}
	if err := p.Authorize("monitor", "monitor.alert.firing"); err != nil {
		t.Fatalf("whitelisted: %v", err)
	}
	if err := p.Authorize("monitor", "user.status"); !errors.Is(err, ErrForbidden) {
		t.Fatalf("not whitelisted: %v", err)
	}
	if err := p.Authorize("other", "monitor.alert.firing"); !errors.Is(err, ErrForbidden) {
		t.Fatalf("other biz: %v", err)
	}
	if err := (&Producer{BizID: "monitor"}).Authorize("monitor", "anything"); err != nil {
		t.Fatalf("empty whitelist allows all: %v", err)
	}
}
