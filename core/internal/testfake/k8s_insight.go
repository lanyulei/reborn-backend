package testfake

import (
	"context"
	"slices"
	"strings"
	"sync"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/k8s"
)

// K8sInsight 容器平台可观测部分的内存实现：事件、摘要、集群最小视图（读 K8sStore 的集群与数据范围），
// 以及记录查询语句的假指标来源与假主机查找。
type K8sInsight struct {
	store *K8sStore

	mu sync.Mutex
	// Events 事件表内容（测试直接追加）。
	Events []*biz.Event
	// Queries 假指标来源收到的查询语句。
	Queries []string
	// Hosts 假主机查找：IP → 主机资源。
	Hosts map[string][]biz.HostCI
	// EnabledDatasources 存在且启用的数据源 ID。
	EnabledDatasources []string
}

// NewK8sInsight new a K8sInsight.
func NewK8sInsight(store *K8sStore) *K8sInsight {
	return &K8sInsight{store: store, Hosts: map[string][]biz.HostCI{}}
}

var (
	_ biz.InsightRepo   = (*K8sInsight)(nil)
	_ biz.ClusterBriefs = (*K8sInsight)(nil)
	_ biz.MetricsSource = (*K8sInsight)(nil)
	_ biz.HostFinder    = (*K8sInsight)(nil)
)

// Grants implements biz.InsightRepo.
func (i *K8sInsight) Grants(_ context.Context, roleIDs []string) ([]*biz.RoleScope, error) {
	s := i.store
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*biz.RoleScope
	for _, roleID := range roleIDs {
		for _, sc := range s.scopes[roleID] {
			if c, ok := s.clusters[sc.ClusterID]; ok && c.Enabled {
				cp := *sc
				out = append(out, &cp)
			}
		}
	}
	return out, nil
}

// ListEvents implements biz.InsightRepo.
func (i *K8sInsight) ListEvents(_ context.Context, f biz.EventFilter, access biz.Access, page, pageSize int64) ([]*biz.Event, int64, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	eq := func(want *string, got string) bool { return want == nil || *want == got }
	var matched []*biz.Event
	for _, e := range i.Events {
		visible := access.AllowsNamespace(e.ClusterID, e.Namespace) || (e.Namespace == "" && access.AllowsCluster(e.ClusterID))
		if !visible || e.LastTime.Before(f.Start) || e.LastTime.After(f.End) ||
			!eq(f.ClusterID, e.ClusterID) || !eq(f.Namespace, e.Namespace) || !eq(f.Kind, e.Kind) || !eq(f.Name, e.Name) || !eq(f.Reason, e.Reason) {
			continue
		}
		if f.Keyword != "" && !strings.Contains(e.Name, f.Keyword) && !strings.Contains(e.Message, f.Keyword) {
			continue
		}
		matched = append(matched, e)
	}
	slices.SortStableFunc(matched, func(a, b *biz.Event) int { return b.LastTime.Compare(a.LastTime) })
	total := int64(len(matched))
	lo := min((page-1)*pageSize, total)
	return matched[lo:min(lo+pageSize, total)], total, nil
}

// Summaries implements biz.InsightRepo.
func (i *K8sInsight) Summaries(_ context.Context, clusterIDs []string) (map[string]*biz.ClusterSummaryRow, error) {
	s := i.store
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]*biz.ClusterSummaryRow{}
	for _, id := range clusterIDs {
		if c, ok := s.clusters[id]; ok {
			out[id] = &biz.ClusterSummaryRow{HasDatasource: c.DatasourceID != ""}
		}
	}
	return out, nil
}

// Brief implements biz.ClusterBriefs.
func (i *K8sInsight) Brief(_ context.Context, id string) (*biz.ClusterBrief, error) {
	s := i.store
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.clusters[id]
	if !ok {
		return nil, biz.ErrClusterNotFound
	}
	return &biz.ClusterBrief{ID: c.ID, Name: c.Name, Enabled: c.Enabled, DatasourceID: c.DatasourceID}, nil
}

// QueryRange implements biz.MetricsSource：记录查询语句并返回两个点。
func (i *K8sInsight) QueryRange(_ context.Context, _, expr string, start, end time.Time, _ time.Duration) ([][2]float64, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.Queries = append(i.Queries, expr)
	return [][2]float64{{float64(start.Unix()), 1}, {float64(end.Unix()), 2}}, nil
}

// Enabled implements biz.MetricsSource.
func (i *K8sInsight) Enabled(_ context.Context, id string) (bool, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	return slices.Contains(i.EnabledDatasources, id), nil
}

// FindByIP implements biz.HostFinder.
func (i *K8sInsight) FindByIP(_ context.Context, _, ip string) ([]biz.HostCI, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.Hosts[ip], nil
}

// TakeQueries 取走已记录的查询语句。
func (i *K8sInsight) TakeQueries() []string {
	i.mu.Lock()
	defer i.mu.Unlock()
	out := i.Queries
	i.Queries = nil
	return out
}
