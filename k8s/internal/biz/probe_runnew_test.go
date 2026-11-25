package biz

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// gateProbe 按集群计探活次数；down 里的集群探活失败。gate > 0 时每次探活都等到同时在途的达到 gate
// 个（或超时）才返回，用来观察并发度。
type gateProbe struct {
	down map[string]bool
	gate int

	mu          sync.Mutex
	calls       map[string]int
	active, max int
	release     chan struct{}
}

func newGateProbe(gate int, down ...string) *gateProbe {
	g := &gateProbe{down: map[string]bool{}, gate: gate, calls: map[string]int{}, release: make(chan struct{})}
	for _, id := range down {
		g.down[id] = true
	}
	return g
}

func (g *gateProbe) Probe(_ context.Context, id string) (ProbeInfo, error) {
	g.mu.Lock()
	g.calls[id]++
	g.active++
	g.max = max(g.max, g.active)
	if g.gate > 0 && g.active == g.gate {
		select {
		case <-g.release:
		default:
			close(g.release)
		}
	}
	g.mu.Unlock()
	if g.gate > 0 {
		select {
		case <-g.release:
		case <-time.After(5 * time.Second):
		}
	}
	g.mu.Lock()
	g.active--
	g.mu.Unlock()
	if g.down[id] {
		return ProbeInfo{}, errors.New("connection refused")
	}
	return ProbeInfo{Version: "v1.35.0"}, nil
}

func (g *gateProbe) SelfCheck(context.Context, string) ([]k8scontract.SelfCheck, error) {
	return nil, nil
}

// recRuntime 记下每个集群最后一次落库的探活状态。
type recRuntime struct {
	nopRuntime
	mu    sync.Mutex
	saved map[string]int32
}

func (r *recRuntime) SaveProbe(_ context.Context, _ string, rec ProbeRecord) (int32, time.Time, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.saved == nil {
		r.saved = map[string]int32{}
	}
	r.saved[rec.ClusterID] = rec.Status
	return rec.Status, time.Now(), nil
}

func (r *recRuntime) snapshot() map[string]int32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return maps.Clone(r.saved)
}

func newRunNewProber(t *testing.T, n int, probe ClusterProbe) (*Prober, []*Cluster, *recRuntime) {
	t.Helper()
	clusters := make([]*Cluster, n)
	ids := make([]string, n)
	for i := range n {
		ids[i] = fmt.Sprintf("c%02d", i)
		clusters[i] = &Cluster{ID: ids[i], Name: ids[i]}
	}
	own := ownAll{ids: ids}
	reg := NewRegistry(own, own, staticClusters{list: clusters}, nopConn{}, quiet)
	reg.WithOptions(RegistryOptions{InstanceID: "i1"})
	if _, err := reg.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	rt := &recRuntime{}
	return NewProber(reg, probe, rt, nopRuntime{}, quiet), clusters, rt
}

// RunNew 只探活还没有结果的集群，每个恰好一次；有了结果之后不再探活；并发探活的落库结果与逐个串行探活相同。
func TestRunNewProbesOnlyFreshClustersOnce(t *testing.T) {
	t.Parallel()
	const n = 20
	down := []string{"c03", "c11", "c19"}

	// 基准：逐个串行探活（改动前 RunNew 的做法）。
	serialProbe := newGateProbe(0, down...)
	sp, clusters, serialRT := newRunNewProber(t, n, serialProbe)
	for _, c := range clusters {
		sp.probeOne(context.Background(), c)
	}

	probe := newGateProbe(0, down...)
	p, _, rt := newRunNewProber(t, n, probe)
	if got, err := p.RunNew(context.Background()); got != 0 || err != nil {
		t.Fatalf("RunNew() = %d, %v", got, err)
	}
	if !maps.Equal(rt.snapshot(), serialRT.snapshot()) {
		t.Fatalf("saved = %v, want the serial result %v", rt.snapshot(), serialRT.snapshot())
	}
	if len(probe.calls) != n {
		t.Fatalf("probed %d clusters, want %d", len(probe.calls), n)
	}
	for id, calls := range probe.calls {
		if calls != 1 {
			t.Fatalf("cluster %s probed %d times, want 1", id, calls)
		}
		if want := !probe.down[id]; p.Online(id) != want {
			t.Fatalf("Online(%s) = %v, want %v", id, !want, want)
		}
	}

	// 都有结果了：再跑不探活。
	if _, err := p.RunNew(context.Background()); err != nil {
		t.Fatal(err)
	}
	for id, calls := range probe.calls {
		if calls != 1 {
			t.Fatalf("cluster %s probed %d times after the second RunNew, want 1", id, calls)
		}
	}
}

// RunNew 有界并发：同时在途的探活达到、且不超过 probeConcurrency。
func TestRunNewIsBoundedConcurrent(t *testing.T) {
	t.Parallel()
	probe := newGateProbe(probeConcurrency)
	p, _, _ := newRunNewProber(t, 3*probeConcurrency, probe)
	start := time.Now()
	if _, err := p.RunNew(context.Background()); err != nil {
		t.Fatal(err)
	}
	if probe.max != probeConcurrency {
		t.Fatalf("max in-flight probes = %d, want %d", probe.max, probeConcurrency)
	}
	if d := time.Since(start); d > 4*time.Second {
		t.Fatalf("RunNew took %s: probes did not run concurrently", d)
	}
}

// 取消的 ctx：不落库、返回 ctx 的错误（与串行时一致）。
func TestRunNewCancelled(t *testing.T) {
	t.Parallel()
	p, _, rt := newRunNewProber(t, 4, newGateProbe(0))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.RunNew(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("RunNew(cancelled) error = %v, want context.Canceled", err)
	}
	if got := rt.snapshot(); len(got) != 0 {
		t.Fatalf("saved = %v, want nothing", got)
	}
}
