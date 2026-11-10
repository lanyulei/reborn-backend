package biz

import (
	"context"
	"errors"
	"testing"
	"time"
)

// healthRepoStub 共享健康表内存桩（模拟多实例共享同一行）。
type healthRepoStub struct {
	open     map[string]time.Time
	probing  map[string]bool
	fails    map[string]int
	claims   int
	failNext bool
	now      func() time.Time
}

func newHealthStub(now func() time.Time) *healthRepoStub {
	return &healthRepoStub{open: map[string]time.Time{}, probing: map[string]bool{}, fails: map[string]int{}, now: now}
}

func (s *healthRepoStub) Record(_ context.Context, id string, ok bool) (time.Time, error) {
	if s.failNext {
		return time.Time{}, errors.New("db down")
	}
	if s.probing[id] {
		s.probing[id] = false
		if ok {
			delete(s.open, id)
			return time.Time{}, nil
		}
		s.open[id] = s.now().Add(60 * time.Second)
		return s.open[id], nil
	}
	if !ok {
		s.fails[id]++
	}
	if s.fails[id] >= 10 {
		s.fails[id] = 0
		s.open[id] = s.now().Add(60 * time.Second)
	}
	return s.open[id], nil
}

func (s *healthRepoStub) ClaimProbe(_ context.Context, id string) (bool, bool, error) {
	until, ok := s.open[id]
	if !ok {
		return false, true, nil
	}
	if s.now().Before(until) || s.probing[id] {
		return false, false, nil
	}
	s.probing[id] = true
	s.claims++
	return true, false, nil
}

func (s *healthRepoStub) ListOpen(context.Context) (map[string]time.Time, error) {
	out := map[string]time.Time{}
	for k, v := range s.open {
		out[k] = v
	}
	return out, nil
}

// TestSharedBreaker_MultiInstance 两个副本共享状态：A 累计失败打开 → B 刷新后拒绝；
// 到期后仅一方领到探测名额；探测成功双方放行；存储故障退化到本地判定。
func TestSharedBreaker_MultiInstance(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	clock := func() time.Time { return now }
	repo := newHealthStub(clock)
	a, b := NewSharedBreaker(repo, testLogger()), NewSharedBreaker(repo, testLogger())
	a.now, b.now = clock, clock
	a.local.now, b.local.now = clock, clock
	ctx := context.Background()
	const ch = "c1"
	for i := 0; i < 10; i++ {
		if !a.Allow(ctx, ch) {
			t.Fatal("must allow before trip")
		}
		a.Record(ctx, ch, false)
	}
	if !a.Open(ch) || a.Allow(ctx, ch) {
		t.Fatal("A must see open state immediately from Record")
	}
	if !b.Allow(ctx, ch) {
		t.Fatal("B has not refreshed yet: local view still allows")
	}
	now = now.Add(3 * time.Second)
	b.Refresh(ctx)
	if b.Allow(ctx, ch) {
		t.Fatal("B must deny after refresh propagated the open state")
	}
	// 到期：两副本同时竞争探测名额，仅一方成功。
	now = now.Add(61 * time.Second)
	allowed := 0
	for _, br := range []*SharedBreaker{a, b} {
		if br.Allow(ctx, ch) {
			allowed++
		}
	}
	if allowed != 1 || repo.claims != 1 {
		t.Fatalf("probe claims = %d allowed = %d, want exactly one", repo.claims, allowed)
	}
	// 探测成功 → 关闭；另一副本下次 Allow 经 ClaimProbe 得知已关闭 → 放行并清视图。
	a.Record(ctx, ch, true)
	if !a.Allow(ctx, ch) || !b.Allow(ctx, ch) || b.Open(ch) {
		t.Fatal("both replicas must allow after successful probe")
	}
	// 存储故障：Record/Allow 退化到本地熔断器，不阻塞发送。
	repo.failNext = true
	for i := 0; i < 10; i++ {
		a.Record(ctx, ch, false)
	}
	if a.Allow(ctx, ch) {
		t.Fatal("local fallback breaker should be open after 10 local failures")
	}
	// 无仓储：纯本地。
	solo := NewSharedBreaker(nil, testLogger())
	if !solo.Allow(ctx, ch) {
		t.Fatal("nil repo must behave as local breaker")
	}
}
