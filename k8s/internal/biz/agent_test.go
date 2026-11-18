package biz

import (
	"context"
	"log/slog"
	"slices"
	"sort"
	"testing"
	"time"
)

type fakeTunnels struct {
	active  map[string]string
	gen     uint64
	tokens  []string
	dropped []string
}

func (f *fakeTunnels) Active() (map[string]string, uint64) { return f.active, f.gen }
func (f *fakeTunnels) Tokens() []string                    { return f.tokens }
func (f *fakeTunnels) Drop(ids []string)                   { f.dropped = append(f.dropped, ids...) }

type fakeAgentRepo struct {
	alive    map[string]struct{}
	refuse   map[string]bool
	attached []string
	detached []string
	offline  []OfflineAgent
}

func (f *fakeAgentRepo) Exchange(context.Context, string, string) (AgentGrant, error) {
	return AgentGrant{}, ErrAgentToken
}

func (f *fakeAgentRepo) Verify(context.Context, string, string) (AgentGrant, error) {
	return AgentGrant{}, ErrAgentToken
}

func (f *fakeAgentRepo) Alive(context.Context, []string) (map[string]struct{}, error) {
	return f.alive, nil
}

func (f *fakeAgentRepo) Attach(_ context.Context, _, clusterID, _ string, _ time.Duration) (bool, error) {
	if f.refuse[clusterID] {
		return false, nil
	}
	f.attached = append(f.attached, clusterID)
	return true, nil
}

func (f *fakeAgentRepo) Detach(_ context.Context, _, clusterID string) error {
	f.detached = append(f.detached, clusterID)
	return nil
}

func (f *fakeAgentRepo) SweepOffline(context.Context, time.Duration) ([]OfflineAgent, error) {
	return f.offline, nil
}

type fakeOutbox struct{ events []OutboxEvent }

func (f *fakeOutbox) Enqueue(_ context.Context, ev OutboxEvent) error {
	f.events = append(f.events, ev)
	return nil
}

func newKeeper(t *fakeTunnels, r *fakeAgentRepo, o *fakeOutbox) *AgentKeeper {
	log := slog.New(slog.DiscardHandler)
	reg := NewRegistry(nil, nil, nil, nil, log)
	reg.WithOptions(RegistryOptions{InstanceID: "i1"})
	return NewAgentKeeper(reg, t, r, r, o, log)
}

// 持有隧道的实例负责集群；别的实例仍持有租约时本实例只作备用；隧道断开即放弃。
func TestAgentKeeperHeld(t *testing.T) {
	t.Parallel()
	tun := &fakeTunnels{active: map[string]string{"a": "v1", "b": "v1"}}
	repo := &fakeAgentRepo{refuse: map[string]bool{"b": true}}
	k := newKeeper(tun, repo, &fakeOutbox{})
	held, err := k.Held(context.Background())
	if err != nil || !slices.Equal(held, []string{"a"}) {
		t.Fatalf("held = %v, %v", held, err)
	}
	tun.active = map[string]string{"b": "v1"}
	repo.refuse = nil
	held, _ = k.Held(context.Background())
	sort.Strings(held)
	if !slices.Equal(held, []string{"b"}) || !slices.Equal(repo.detached, []string{"a"}) {
		t.Fatalf("held = %v detached = %v", held, repo.detached)
	}
}

// 令牌被吊销（或集群被停用 / 删除）：对应隧道被断开；由在线翻转为离线的集群发通知。
func TestAgentKeeperSweep(t *testing.T) {
	t.Parallel()
	tun := &fakeTunnels{tokens: []string{"t1", "t2", "t3"}}
	since := time.Unix(1_800_000_000, 0)
	repo := &fakeAgentRepo{
		alive: map[string]struct{}{"t2": {}},
		offline: []OfflineAgent{
			{Cluster: Cluster{ID: "c1", Name: "one"}, WasOnline: true, Since: since},
			{Cluster: Cluster{ID: "c2", Name: "never-connected"}, WasOnline: false, Since: since},
		},
	}
	out := &fakeOutbox{}
	if _, err := newKeeper(tun, repo, out).Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(tun.dropped, []string{"t1", "t3"}) {
		t.Fatalf("dropped = %v", tun.dropped)
	}
	if len(out.events) != 1 || out.events[0].ClusterID != "c1" || out.events[0].DedupKey != "k8s:c1:unreachable:1800000000" {
		t.Fatalf("events = %+v", out.events)
	}
}
