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
	auth := NewProducerAuth(newTestCache(t, snap), nil)
	auth.now = func() time.Time { return now }
	body := []byte(`{"event":{}}`)
	sign := func(id string, at time.Time) string {
		return ingressauth.Header(id, token, "POST", "/ingress/v1/events", body, at)
	}
	ad, err := auth.Verify(context.Background(), sign("p-ok", now), "POST", "/ingress/v1/events", body)
	if err != nil || ad == nil || ad.Producer.ID != "p-ok" || ad.Replayed {
		t.Fatalf("Verify(ok) = %+v, %v", ad, err)
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

// TestProducerAuth_Replay 同一已签名请求在时间窗内再次出现标记为重放；撤销登记
// （请求未被受理）后可原样重发；时间戳不同的请求互不影响。
func TestProducerAuth_Replay(t *testing.T) {
	t.Parallel()
	const token = "npt_test-token" //nolint:gosec // 测试桩凭据，非真实密钥
	now := time.Unix(1_800_000_000, 0)
	producers := []*Producer{{ID: "p-ok", BizID: "monitor", Enabled: true, TokenHash: ingressauth.DeriveKey(token)}}
	auth := NewProducerAuth(newTestCache(t, NewSnapshot(ConfigVersions{}, nil, nil, nil, nil, producers, now)), nil)
	clock := now
	auth.now = func() time.Time { return clock }
	body := []byte(`{"event":{}}`)
	verify := func(at time.Time) *Admission {
		t.Helper()
		ad, err := auth.Verify(context.Background(), ingressauth.Header("p-ok", token, "POST", "/ingress/v1/events", body, at), "POST", "/ingress/v1/events", body)
		if err != nil {
			t.Fatalf("Verify() error = %v", err)
		}
		return ad
	}
	first := verify(now)
	if first.Replayed {
		t.Fatal("first request must not be a replay")
	}
	replay := verify(now)
	if !replay.Replayed {
		t.Fatal("identical signed request must be flagged as replay")
	}
	if verify(now.Add(time.Second)).Replayed {
		t.Fatal("request with a different timestamp is not a replay")
	}
	replay.Forget() // 重放请求不持有登记：no-op
	if !verify(now).Replayed {
		t.Fatal("Forget() on a replayed admission must not clear the original registration")
	}
	first.Forget() // 原请求未被受理：撤销后可原样重发
	if verify(now).Replayed {
		t.Fatal("after Forget() the identical request must be admitted again")
	}
	// 时间戳仍在时间窗内：登记继续有效。
	clock = now.Add(4 * time.Minute)
	if !verify(now).Replayed {
		t.Fatal("replay within the timestamp window must still be flagged")
	}
}

func TestReplayGuard_Bounded(t *testing.T) {
	t.Parallel()
	g := newReplayGuard(4)
	now := time.Unix(1_800_000_000, 0)
	until := now.Add(time.Minute)
	for i := range 100 {
		if g.seen(replayKey{producer: "p", sig: string(rune('a' + i))}, until, now, time.Hour) {
			t.Fatalf("key %d reported as seen", i)
		}
	}
	if len(g.cur) > 4 || len(g.prev) > 4 {
		t.Fatalf("guard grew past its bound: cur=%d prev=%d", len(g.cur), len(g.prev))
	}
	// 换代后上一代的登记仍被识别（至少存活一个 ttl）。
	live := replayKey{producer: "p", sig: "live"}
	g.seen(live, until, now, time.Hour)
	for i := range 4 {
		g.seen(replayKey{producer: "q", sig: string(rune('a' + i))}, until, now, time.Hour)
	}
	if !g.seen(live, until, now, time.Hour) {
		t.Fatal("registration must survive one rotation")
	}
	// 失效的登记不算重放（时间窗外的请求本就被时间戳校验拒绝）。
	k := replayKey{producer: "p", sig: "expired"}
	g.seen(k, now.Add(-time.Second), now, time.Hour)
	if g.seen(k, until, now, time.Hour) {
		t.Fatal("expired registration must not count as replay")
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

// fakeReplayStore 模拟跨副本共享登记。
type fakeReplayStore struct {
	seen    map[string]bool
	forgets int
}

func (f *fakeReplayStore) RegisterReplay(_ context.Context, producerID, signature string, _ time.Time) (bool, error) {
	k := producerID + "/" + signature
	if f.seen[k] {
		return false, nil
	}
	f.seen[k] = true
	return true, nil
}

func (f *fakeReplayStore) ForgetReplay(_ context.Context, producerID, signature string) error {
	delete(f.seen, producerID+"/"+signature)
	f.forgets++
	return nil
}

// TestProducerAuth_SharedReplayStore 两个副本（各自的进程内表、共享存储）：同一已签名
// 请求在第二个副本被识别为重放；持有登记的一方撤销后，另一副本可重新受理。
func TestProducerAuth_SharedReplayStore(t *testing.T) {
	t.Parallel()
	const token = "npt_test-token" //nolint:gosec // 测试桩凭据，非真实密钥
	now := time.Unix(1_800_000_000, 0)
	producers := []*Producer{{ID: "p-ok", BizID: "monitor", Enabled: true, TokenHash: ingressauth.DeriveKey(token)}}
	store := &fakeReplayStore{seen: map[string]bool{}}
	newReplica := func() *ProducerAuth {
		a := NewProducerAuth(newTestCache(t, NewSnapshot(ConfigVersions{}, nil, nil, nil, nil, producers, now)), store)
		a.now = func() time.Time { return now }
		return a
	}
	a, b := newReplica(), newReplica()
	body := []byte(`{"event":{}}`)
	header := ingressauth.Header("p-ok", token, "POST", "/ingress/v1/events", body, now)
	first, err := a.Verify(context.Background(), header, "POST", "/ingress/v1/events", body)
	if err != nil || first.Replayed {
		t.Fatalf("replica A first = %+v, %v", first, err)
	}
	second, err := b.Verify(context.Background(), header, "POST", "/ingress/v1/events", body)
	if err != nil || !second.Replayed {
		t.Fatalf("replica B must see the replay: %+v, %v", second, err)
	}
	second.Forget() // 重放请求不持有登记：no-op
	if store.forgets != 0 {
		t.Fatalf("replayed admission must not forget the shared registration")
	}
	first.Forget() // 持有方撤销（请求未被受理）
	if store.forgets != 1 || len(store.seen) != 0 {
		t.Fatalf("holder forget must clear the shared registration: forgets=%d seen=%d", store.forgets, len(store.seen))
	}
}
