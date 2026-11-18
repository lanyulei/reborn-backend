package biz

import (
	"context"
	"strings"
	"testing"
	"time"

	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"
)

func TestBuildPanelsRejectsHostileInput(t *testing.T) {
	t.Parallel()
	hostile := []string{`x"} or vector(1) #`, `a\`, "a\nb", `a{b}`, `a,b`, `A`, `a b`, `a"`, `a'`, strings.Repeat("a", 300)}
	for _, h := range hostile {
		for _, target := range []MetricsTarget{
			{Kind: TargetNode, Name: h},
			{Kind: TargetPod, Namespace: "default", Name: h},
			{Kind: TargetPod, Namespace: h, Name: "web"},
			{Kind: TargetWorkload, Namespace: "default", Name: h, WorkloadKind: "Deployment"},
			{Kind: TargetWorkload, Namespace: "default", Name: "web", WorkloadKind: h},
			{Kind: TargetCluster, Name: h},
			{Kind: h},
		} {
			if _, err := BuildPanels("prod-1", target); err == nil {
				t.Errorf("BuildPanels accepted hostile target %+v", target)
			}
		}
		if _, err := BuildPanels(h, MetricsTarget{Kind: TargetCluster}); err == nil {
			t.Errorf("BuildPanels accepted hostile cluster name %q", h)
		}
	}
}

func TestBuildPanelsAlwaysScopesToCluster(t *testing.T) {
	t.Parallel()
	targets := []MetricsTarget{
		{Kind: TargetCluster},
		{Kind: TargetNode, Name: "node-1.example.com"},
		{Kind: TargetPod, Namespace: "shop", Name: "web-5d9c-abcde"},
		{Kind: TargetWorkload, Namespace: "shop", Name: "web", WorkloadKind: "Deployment"},
		{Kind: TargetWorkload, Namespace: "shop", Name: "db", WorkloadKind: "StatefulSet"},
		{Kind: TargetWorkload, Namespace: "shop", Name: "agent", WorkloadKind: "DaemonSet"},
	}
	for _, target := range targets {
		panels, err := BuildPanels("prod-1", target)
		if err != nil || len(panels) == 0 {
			t.Fatalf("BuildPanels(%+v) = %v, %v", target, panels, err)
		}
		for _, p := range panels {
			for _, q := range p.Queries {
				// 每个选择器（花括号）里都必须有集群标签：没有它就会跨集群取数。
				for _, sel := range strings.Split(q.Expr, "{")[1:] {
					if body := sel[:strings.IndexByte(sel, '}')]; !strings.Contains(body, `cluster="prod-1"`) {
						t.Errorf("%s/%s: selector {%s} lacks the cluster label in %s", p.ID, q.Name, body, q.Expr)
					}
				}
				if target.Namespace != "" && !strings.Contains(q.Expr, `namespace="shop"`) {
					t.Errorf("%s/%s lacks the namespace label: %s", p.ID, q.Name, q.Expr)
				}
			}
		}
	}
	// 名称里的点在正则匹配里必须被转义，否则 node-1.example 会匹配到 node-1xexample。
	panels, _ := BuildPanels("prod-1", MetricsTarget{Kind: TargetNode, Name: "node-1.example.com"})
	if expr := panels[0].Queries[0].Expr; !strings.Contains(expr, `instance=~"node-1\\.example\\.com(:[0-9]+)?"`) {
		t.Errorf("node regex is not escaped: %s", expr)
	}
	panels, _ = BuildPanels("prod-1", MetricsTarget{Kind: TargetWorkload, Namespace: "shop", Name: "web", WorkloadKind: "Deployment"})
	if expr := panels[0].Queries[0].Expr; !strings.Contains(expr, `pod=~"web-[a-z0-9]+-[a-z0-9]+"`) {
		t.Errorf("deployment pod matcher: %s", expr)
	}
}

func TestPromEscaping(t *testing.T) {
	t.Parallel()
	if got := promString("a\"b\\c\nd"); got != `a\"b\\c\nd` {
		t.Errorf("promString = %q", got)
	}
	if got := promRegex(`a.b"c`); got != `a\\.b\"c` {
		t.Errorf("promRegex = %q", got)
	}
}

func TestMetricsWindow(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	s, e, step, err := metricsWindow(nil, nil, now)
	if err != nil || e != now || e.Sub(s) != time.Hour || step != 30*time.Second {
		t.Fatalf("default window = %v %v %v %v", s, e, step, err)
	}
	short := now.Add(-time.Minute)
	if _, _, step, _ := metricsWindow(&short, nil, now); step != minMetricsStep {
		t.Errorf("short range step = %v", step)
	}
	old := now.Add(-8 * 24 * time.Hour)
	if _, _, _, err := metricsWindow(&old, nil, now); err == nil {
		t.Error("range over 7 days must be rejected")
	}
	future := now.Add(time.Hour)
	if _, _, _, err := metricsWindow(&future, nil, now); err == nil {
		t.Error("start after end must be rejected")
	}
}

func TestAccess(t *testing.T) {
	t.Parallel()
	a := NewAccess([]*RoleScope{
		{ClusterID: "c1", Namespace: "shop", Level: 1},
		{ClusterID: "c1", Namespace: "pay", Level: 2},
		{ClusterID: "c2", Namespace: kc.AllNamespaces, Level: 1},
	})
	cases := []struct {
		cluster, ns string
		want        bool
	}{
		{"c1", "shop", true},
		{"c1", "pay", true},
		{"c1", "kube-system", false},
		{"c1", "", false},
		{"c2", "anything", true},
		{"c3", "shop", false},
	}
	for _, c := range cases {
		if got := a.AllowsNamespace(c.cluster, c.ns); got != c.want {
			t.Errorf("AllowsNamespace(%s, %s) = %v", c.cluster, c.ns, got)
		}
	}
	if !a.AllowsCluster("c1") || a.AllowsCluster("c3") || a.Empty() {
		t.Error("cluster access is wrong")
	}
	if !NewAccess(nil).Empty() || (Access{All: true}).Empty() || !(Access{All: true}).AllowsNamespace("x", "y") {
		t.Error("empty / admin access is wrong")
	}
}

type insightFakes struct {
	grants  []*RoleScope
	queries []string
	admin   bool
}

func (f *insightFakes) EnabledRoleIDs(context.Context, string) (bool, []string, error) {
	if f.admin {
		return true, nil, nil
	}
	return false, []string{"r1"}, nil
}

func (f *insightFakes) Grants(context.Context, []string) ([]*RoleScope, error) { return f.grants, nil }

func (f *insightFakes) ListEvents(_ context.Context, _ EventFilter, a Access, _, _ int64) ([]*Event, int64, error) {
	return nil, int64(len(a.Clusters)), nil
}

func (f *insightFakes) Summaries(context.Context, []string) (map[string]*ClusterSummaryRow, error) {
	return nil, nil
}

func (f *insightFakes) Brief(_ context.Context, id string) (*ClusterBrief, error) {
	return &ClusterBrief{ID: id, Name: "prod-1", Enabled: true, DatasourceID: "ds"}, nil
}

func (f *insightFakes) QueryRange(_ context.Context, _, expr string, _, _ time.Time, _ time.Duration) ([][2]float64, error) {
	f.queries = append(f.queries, expr)
	return nil, nil
}
func (f *insightFakes) Enabled(context.Context, string) (bool, error) { return false, nil }
func (f *insightFakes) FindByIP(context.Context, string, string) ([]HostCI, error) {
	return []HostCI{{ID: "ci"}}, nil
}

func TestInsightScopeEnforcement(t *testing.T) {
	t.Parallel()
	f := &insightFakes{grants: []*RoleScope{{ClusterID: "c1", Namespace: "shop", Level: 1}}}
	uc := &InsightUsecase{repo: f, briefs: f, roles: f, metrics: f, hosts: f, now: time.Now}
	ctx, caller := context.Background(), Caller{UserID: "u1"}

	// 范围外的集群：与不存在同样应答，且不发出任何查询。
	if _, err := uc.QueryMetrics(ctx, caller, "c2", MetricsTarget{Kind: TargetCluster}, nil, nil, nil); err != ErrClusterNotFound {
		t.Errorf("metrics on a cluster outside the scope: %v", err)
	}
	// 范围内的集群、范围外的命名空间。
	if _, err := uc.QueryMetrics(ctx, caller, "c1", MetricsTarget{Kind: TargetPod, Namespace: "pay", Name: "p"}, nil, nil, nil); err != ErrForbidden {
		t.Errorf("metrics on a namespace outside the scope: %v", err)
	}
	if len(f.queries) != 0 {
		t.Fatalf("denied requests must not reach the datasource: %v", f.queries)
	}
	res, err := uc.QueryMetrics(ctx, caller, "c1", MetricsTarget{Kind: TargetPod, Namespace: "shop", Name: "p"}, []string{"cpu"}, nil, nil)
	if err != nil || !res.Configured || len(res.Panels) != 1 || len(f.queries) != 3 {
		t.Fatalf("allowed metrics = %+v, %v, queries %d", res, err, len(f.queries))
	}
	if _, err := uc.NodeHost(ctx, caller, "c2", "10.0.0.1"); err != ErrClusterNotFound {
		t.Errorf("node host outside the scope: %v", err)
	}
	if items, err := uc.NodeHost(ctx, caller, "c1", "10.0.0.1"); err != nil || len(items) != 1 {
		t.Errorf("node host inside the scope: %v %v", items, err)
	}
	if _, err := uc.NodeHost(ctx, caller, "c1", "not-an-ip"); err == nil {
		t.Error("invalid ip must be rejected")
	}
	// 没有任何授权的用户：事件为空，不查库。
	none := &InsightUsecase{repo: &insightFakes{}, roles: &insightFakes{}, now: time.Now}
	if items, total, err := none.ListEvents(ctx, caller, EventFilter{}, nil, nil, 1, 20); err != nil || total != 0 || len(items) != 0 {
		t.Errorf("events without grants = %v %d %v", items, total, err)
	}
	old := time.Now().Add(-100 * 24 * time.Hour)
	if _, _, err := uc.ListEvents(ctx, caller, EventFilter{}, &old, nil, 1, 20); err == nil {
		t.Error("event window over 90 days must be rejected")
	}
}

func TestNormalizeInsight(t *testing.T) {
	t.Parallel()
	in := ClusterInput{EventReasons: []string{" FailedScheduling ", "failedscheduling", "", "BackOff"}}
	if err := in.normalizeInsight(); err != nil || len(in.EventReasons) != 2 {
		t.Fatalf("normalize = %v %v", in.EventReasons, err)
	}
	for _, bad := range []string{"k8s.event.x", "a b", "1abc", "a*"} {
		in := ClusterInput{EventReasons: []string{bad}}
		if err := in.normalizeInsight(); err == nil {
			t.Errorf("reason %q must be rejected", bad)
		}
	}
}
