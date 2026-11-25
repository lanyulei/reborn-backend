package biz

import (
	"context"
	"errors"
	"hash/fnv"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// panelSource 指标数据源桩：返回值只取决于查询语句（与调用先后无关）——约三分之一的语句失败、
// 一部分返回 nil、其余返回由语句决定的点；同时统计在途并发与每条语句被查的次数。
type panelSource struct {
	insightFakes
	delay time.Duration

	mu          sync.Mutex
	calls       map[string]int
	active, max int
}

func exprHash(expr string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(expr))
	return h.Sum32()
}

func panelAnswer(expr string) ([][2]float64, error) {
	switch h := exprHash(expr); h % 6 {
	case 0, 1:
		return nil, kerrors.ServiceUnavailable("DATASOURCE_DOWN", "datasource failed for "+expr[:min(len(expr), 12)])
	case 2:
		return nil, nil
	default:
		return [][2]float64{{1, float64(h % 97)}, {2, float64(h % 89)}}, nil
	}
}

func (p *panelSource) QueryRange(ctx context.Context, _, expr string, _, _ time.Time, _ time.Duration) ([][2]float64, error) {
	p.mu.Lock()
	if p.calls == nil {
		p.calls = map[string]int{}
	}
	p.calls[expr]++
	p.active++
	p.max = max(p.max, p.active)
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		p.active--
		p.mu.Unlock()
	}()
	if p.delay > 0 {
		select {
		case <-time.After(p.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return panelAnswer(expr)
}

// serialPanels 改动前的串行算法（面板按序、面板内遇错即停），作为并发实现的对照。
func serialPanels(panels []MetricPanel, panelIDs []string) ([]*MetricPanelResult, map[string]int) {
	out := []*MetricPanelResult{}
	calls := map[string]int{}
	for _, p := range panels {
		if len(panelIDs) > 0 && !slices.Contains(panelIDs, p.ID) {
			continue
		}
		res := &MetricPanelResult{ID: p.ID, Title: p.Title, Unit: p.Unit, Series: []MetricSeries{}}
		for _, q := range p.Queries {
			calls[q.Expr]++
			points, err := panelAnswer(q.Expr)
			if err != nil {
				res.Error = kerrors.FromError(err).Message
				break
			}
			if points == nil {
				points = [][2]float64{}
			}
			res.Series = append(res.Series, MetricSeries{Name: q.Name, Points: points})
		}
		out = append(out, res)
	}
	return out, calls
}

// 面板并发查询的结果与串行逐字段相同：面板顺序、每个面板的序列与错误、发给数据源的语句集合（面板内遇错
// 即停，后面的语句不发）都不变；同时在查的面板数不超过上限。
func TestQueryMetricsParallelMatchesSerial(t *testing.T) {
	t.Parallel()
	ctx, caller := context.Background(), Caller{UserID: "u1"}
	targets := []MetricsTarget{
		{Kind: TargetCluster},
		{Kind: TargetNode, Name: "node-1"},
		{Kind: TargetWorkload, Namespace: "shop", Name: "web", WorkloadKind: "Deployment"},
		{Kind: TargetPod, Namespace: "shop", Name: "web-0"},
	}
	sawError, sawSeries, sawParallel := false, false, false
	for _, target := range targets {
		panels, err := BuildPanels("prod-1", target)
		if err != nil {
			t.Fatalf("BuildPanels(%+v) error = %v", target, err)
		}
		for _, ids := range [][]string{nil, {panels[0].ID}, {panels[len(panels)-1].ID, panels[0].ID}, {"no-such-panel"}} {
			src := &panelSource{delay: 2 * time.Millisecond}
			src.admin = true
			uc := &InsightUsecase{repo: src, briefs: src, roles: src, metrics: src, hosts: src, now: time.Now}
			res, err := uc.QueryMetrics(ctx, caller, "c1", target, ids, nil, nil)
			if err != nil {
				t.Fatalf("QueryMetrics(%+v, %v) error = %v", target, ids, err)
			}
			want, wantCalls := serialPanels(panels, ids)
			if !reflect.DeepEqual(res.Panels, want) {
				t.Fatalf("QueryMetrics(%+v, %v) panels differ from the serial result:\n got %+v\nwant %+v", target, ids, res.Panels, want)
			}
			if !reflect.DeepEqual(src.calls, wantCalls) && (len(src.calls) != 0 || len(wantCalls) != 0) {
				t.Fatalf("QueryMetrics(%+v, %v) queries = %v, want %v", target, ids, src.calls, wantCalls)
			}
			if src.max > metricsPanelConcurrency {
				t.Fatalf("in-flight queries = %d, want at most %d", src.max, metricsPanelConcurrency)
			}
			sawParallel = sawParallel || src.max > 1
			for _, p := range res.Panels {
				sawError = sawError || p.Error != ""
				sawSeries = sawSeries || len(p.Series) > 0
			}
		}
	}
	// 桩的分布确实覆盖到了失败、成功与并发三种情形，否则上面的比对是空转。
	if !sawError || !sawSeries || !sawParallel {
		t.Fatalf("fixture coverage: error=%v series=%v parallel=%v, want all true", sawError, sawSeries, sawParallel)
	}
}

// 请求取消：整个调用返回 ctx 的错误、不返回半截结果（与串行时一致）。
func TestQueryMetricsCancelled(t *testing.T) {
	t.Parallel()
	src := &panelSource{delay: time.Minute}
	src.admin = true
	uc := &InsightUsecase{repo: src, briefs: src, roles: src, metrics: src, hosts: src, now: time.Now}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var res *MetricsResult
	var err error
	go func() { // 退出路径：cancel 之后 QueryRange 立即返回
		defer close(done)
		res, err = uc.QueryMetrics(ctx, Caller{UserID: "u1"}, "c1", MetricsTarget{Kind: TargetCluster}, nil, nil, nil)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("QueryMetrics did not return after cancellation")
	}
	if res != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("QueryMetrics(cancelled) = %+v, %v, want nil, context.Canceled", res, err)
	}
	src.mu.Lock()
	defer src.mu.Unlock()
	if len(src.calls) > metricsPanelConcurrency {
		t.Fatalf("queries issued = %d, want at most %d (nothing new after cancellation)", len(src.calls), metricsPanelConcurrency)
	}
}
