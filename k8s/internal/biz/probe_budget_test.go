package biz

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// budgetProbe 记录探活与自检各自拿到的剩余预算；探活先耗掉 probeDelay。
type budgetProbe struct {
	probeErr error
	checks   []k8scontract.SelfCheck

	mu                   sync.Mutex
	probeLeft, checkLeft time.Duration
	checkCalls           int
}

func (b *budgetProbe) Probe(ctx context.Context, _ string) (ProbeInfo, error) {
	dl, _ := ctx.Deadline()
	b.mu.Lock()
	b.probeLeft = time.Until(dl)
	b.mu.Unlock()
	return ProbeInfo{Version: "v1.35.0"}, b.probeErr
}

func (b *budgetProbe) SelfCheck(ctx context.Context, _ string) ([]k8scontract.SelfCheck, error) {
	dl, _ := ctx.Deadline()
	b.mu.Lock()
	b.checkLeft = time.Until(dl)
	b.checkCalls++
	b.mu.Unlock()
	return b.checks, nil
}

func newBudgetProber(t *testing.T, probe ClusterProbe) (*Prober, *Cluster) {
	t.Helper()
	c := &Cluster{ID: "c1", Name: "c1"}
	own := ownAll{ids: []string{c.ID}}
	reg := NewRegistry(own, own, staticClusters{list: []*Cluster{c}}, nopConn{}, quiet)
	reg.WithOptions(RegistryOptions{InstanceID: "i1"})
	if _, err := reg.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	return NewProber(reg, probe, nopRuntime{}, nopRuntime{}, quiet), c
}

// 首次探活的接入自检有自己的预算，不与探活共用同一个 10 秒：链路延迟偏高的可达集群不会因为
// 自检挤不进探活剩下的时间而被判成不可达。
func TestProbeSelfCheckHasItsOwnBudget(t *testing.T) {
	t.Parallel()
	bp := &budgetProbe{}
	p, c := newBudgetProber(t, bp)
	if _, err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	bp.mu.Lock()
	probeLeft, checkLeft := bp.probeLeft, bp.checkLeft
	bp.mu.Unlock()
	if probeLeft > probeTimeout || probeLeft < probeTimeout-5*time.Second {
		t.Fatalf("probe budget = %s, want about %s", probeLeft, probeTimeout)
	}
	if checkLeft <= probeTimeout || checkLeft > selfCheckTimeout {
		t.Fatalf("self-check budget = %s, want its own (%s), not what is left of the probe's %s", checkLeft, selfCheckTimeout, probeTimeout)
	}
	if !p.Online(c.ID) {
		t.Fatal("a reachable cluster that passes the self-check must be online")
	}
}

// 放宽的只是「可达之后」的自检：探活失败不进入自检，集群不在线；自检有未通过项同样不在线。
func TestProbeBudgetNeverTurnsFailureIntoOnline(t *testing.T) {
	t.Parallel()
	down := &budgetProbe{probeErr: errors.New("connection refused")}
	p, c := newBudgetProber(t, down)
	if _, err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if down.checkCalls != 0 {
		t.Fatal("an unreachable cluster must not be self-checked")
	}
	if p.Online(c.ID) {
		t.Fatal("an unreachable cluster must not be online")
	}

	failing := &budgetProbe{checks: []k8scontract.SelfCheck{{Name: "impersonate users", Message: "request timed out"}}}
	p2, c2 := newBudgetProber(t, failing)
	if _, err := p2.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if p2.Online(c2.ID) {
		t.Fatal("a cluster with a failed self-check item must not be online")
	}
}
