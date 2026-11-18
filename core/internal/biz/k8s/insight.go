package biz

import (
	"context"
	"encoding/json"
	"math"
	"net/netip"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"
	"github.com/lanyulei/reborn/backend/kit/paging"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// Access 调用者在容器平台的数据范围（k8s_role_scope 的并集）。只用于 core 自己读库的查询
// （事件、总览、指标）；经数据面访问集群的请求仍由集群判定。
type Access struct {
	// All 超管：全部集群、全部命名空间。
	All bool
	// Clusters 集群 ID → 该集群上的范围；不在其中的集群不可见。
	Clusters map[string]*ClusterAccess
}

// ClusterAccess 调用者在一个集群上的范围。
type ClusterAccess struct {
	// AllNamespaces 有「全部命名空间」的授权。
	AllNamespaces bool
	Namespaces    map[string]struct{}
}

// NewAccess 由授权行构造数据范围。
func NewAccess(grants []*RoleScope) Access {
	a := Access{Clusters: map[string]*ClusterAccess{}}
	for _, g := range grants {
		ca := a.Clusters[g.ClusterID]
		if ca == nil {
			ca = &ClusterAccess{Namespaces: map[string]struct{}{}}
			a.Clusters[g.ClusterID] = ca
		}
		if g.Namespace == kc.AllNamespaces {
			ca.AllNamespaces = true
		} else {
			ca.Namespaces[g.Namespace] = struct{}{}
		}
	}
	return a
}

// AllowsCluster 调用者在该集群上是否有任意授权。
func (a Access) AllowsCluster(clusterID string) bool {
	if a.All {
		return true
	}
	_, ok := a.Clusters[clusterID]
	return ok
}

// AllowsNamespace 调用者能否查看该集群的该命名空间。
func (a Access) AllowsNamespace(clusterID, namespace string) bool {
	if a.All {
		return true
	}
	ca := a.Clusters[clusterID]
	if ca == nil {
		return false
	}
	if ca.AllNamespaces {
		return true
	}
	_, ok := ca.Namespaces[namespace]
	return ok
}

// Empty 没有任何可见集群。
func (a Access) Empty() bool { return !a.All && len(a.Clusters) == 0 }

// Event 一条 Warning 事件（k8s_event）。json 标签即接口响应格式。
type Event struct {
	ID          string    `json:"id"`
	ClusterID   string    `json:"clusterId"`
	ClusterName string    `json:"clusterName"`
	Namespace   string    `json:"namespace"`
	Kind        string    `json:"kind"`
	Name        string    `json:"name"`
	Reason      string    `json:"reason"`
	Message     string    `json:"message"`
	Source      string    `json:"source"`
	Count       int64     `json:"count"`
	FirstTime   time.Time `json:"firstTime"`
	LastTime    time.Time `json:"lastTime"`
}

// EventFilter 事件检索条件；指针为 nil 表示不过滤。
type EventFilter struct {
	Start, End time.Time
	ClusterID  *string
	Namespace  *string
	Kind       *string
	Name       *string
	Reason     *string
	Keyword    string
}

// ClusterBrief 集群的最小视图（指标与定位用）。
type ClusterBrief struct {
	ID           string
	Name         string
	Enabled      bool
	DatasourceID string
}

// ClusterSummaryRow 总览里一个集群的摘要。
type ClusterSummaryRow struct {
	Summary       json.RawMessage
	SummaryTime   *time.Time
	HasDatasource bool
}

// InsightRepo 可观测部分的只读查询（data 层实现）。
type InsightRepo interface {
	// Grants 这些角色的全部数据范围（只含未删除且启用的集群）。
	Grants(ctx context.Context, roleIDs []string) ([]*RoleScope, error)
	// ListEvents 按数据范围过滤的事件检索，最近发生的在前。
	ListEvents(ctx context.Context, filter EventFilter, access Access, page, pageSize int64) ([]*Event, int64, error)
	// Summaries 这些集群的摘要；Brief 取集群的最小视图，不存在返回 ErrClusterNotFound。
	Summaries(ctx context.Context, clusterIDs []string) (map[string]*ClusterSummaryRow, error)
}

// ClusterBriefs 集群最小视图读取（data 层实现）。
type ClusterBriefs interface {
	Brief(ctx context.Context, id string) (*ClusterBrief, error)
}

// MetricSeries 指标面板里的一条序列；Points 为 [Unix 秒, 值]。
type MetricSeries struct {
	Name   string       `json:"name"`
	Points [][2]float64 `json:"points"`
}

// MetricsSource 监控数据源访问（组合根以 monitor 域的 DatasourceUsecase 适配）。
type MetricsSource interface {
	// QueryRange 在数据源上执行区间查询，返回第一条序列的点（没有数据返回空）。
	QueryRange(ctx context.Context, datasourceID, expr string, start, end time.Time, step time.Duration) ([][2]float64, error)
	// Enabled 数据源是否存在且已启用。
	Enabled(ctx context.Context, datasourceID string) (bool, error)
}

// HostCI CMDB 里的一个主机资源（最小视图）。json 标签即接口响应格式。
type HostCI struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	ModelCode string `json:"modelCode"`
	ModelName string `json:"modelName"`
	Env       string `json:"env"`
	Status    string `json:"status"`
	Owner     string `json:"owner"`
	OwnerName string `json:"ownerName"`
}

// HostFinder 按 IP 查找 CMDB 主机资源（组合根以 cmdb 域适配，受调用者的 CMDB 数据范围约束）。
type HostFinder interface {
	FindByIP(ctx context.Context, userID, ip string) ([]HostCI, error)
}

// InsightUsecase 可观测用例：总览、事件、指标、主机关联。
type InsightUsecase struct {
	repo     InsightRepo
	briefs   ClusterBriefs
	clusters ClusterReader
	roles    RoleSource
	metrics  MetricsSource
	hosts    HostFinder
	planes   *PlaneUsecase
	now      func() time.Time
}

// NewInsightUsecase new an InsightUsecase.
func NewInsightUsecase(repo InsightRepo, briefs ClusterBriefs, clusters ClusterReader, roles RoleSource, metrics MetricsSource, hosts HostFinder, planes *PlaneUsecase, cluster *ClusterUsecase) *InsightUsecase {
	// 集群登记用例校验「关联的数据源」也用同一个监控数据源访问，在此注入（避免改动其构造签名）。
	cluster.datasources = metrics
	return &InsightUsecase{repo: repo, briefs: briefs, clusters: clusters, roles: roles, metrics: metrics, hosts: hosts, planes: planes, now: time.Now}
}

// access 解析调用者的数据范围。
func (uc *InsightUsecase) access(ctx context.Context, userID string) (Access, bool, []string, error) {
	isAdmin, roleIDs, err := uc.roles.EnabledRoleIDs(ctx, userID)
	if err != nil {
		return Access{}, false, nil, err
	}
	if isAdmin {
		return Access{All: true}, true, roleIDs, nil
	}
	if len(roleIDs) == 0 {
		return NewAccess(nil), false, nil, nil
	}
	grants, err := uc.repo.Grants(ctx, roleIDs)
	if err != nil {
		return Access{}, false, nil, err
	}
	return NewAccess(grants), false, roleIDs, nil
}

const (
	defaultEventWindow = 7 * 24 * time.Hour
	maxEventWindow     = 90 * 24 * time.Hour
)

// ListEvents 检索 Warning 事件：缺省回看 7 天；只返回调用者数据范围内的事件。
func (uc *InsightUsecase) ListEvents(ctx context.Context, caller Caller, filter EventFilter, start, end *time.Time, page, pageSize int64) ([]*Event, int64, error) {
	filter.End = uc.now()
	if end != nil {
		filter.End = *end
	}
	filter.Start = filter.End.Add(-defaultEventWindow)
	if start != nil {
		filter.Start = *start
	}
	switch {
	case filter.Start.After(filter.End):
		return nil, 0, detail(ErrTimeRangeInvalid, "start time must not be later than end time")
	case filter.End.Sub(filter.Start) > maxEventWindow:
		return nil, 0, detail(ErrTimeRangeInvalid, "time range must not exceed 90 days")
	}
	filter.Keyword = strings.TrimSpace(filter.Keyword)
	access, _, _, err := uc.access(ctx, caller.UserID)
	if err != nil {
		return nil, 0, err
	}
	if access.Empty() {
		return []*Event{}, 0, nil
	}
	page, pageSize = paging.Normalize(page, pageSize)
	return uc.repo.ListEvents(ctx, filter, access, page, pageSize)
}

// OverviewCluster 总览里的一个集群。
type OverviewCluster struct {
	ID            string          `json:"id"`
	Name          string          `json:"name"`
	DisplayName   string          `json:"displayName"`
	Env           string          `json:"env"`
	Status        int32           `json:"status"`
	LastError     string          `json:"lastError"`
	K8sVersion    string          `json:"k8sVersion"`
	Level         int32           `json:"level"`
	HasDatasource bool            `json:"hasDatasource"`
	SummaryTime   *time.Time      `json:"summaryTime"`
	Summary       json.RawMessage `json:"summary"`
}

// Overview 总览。
type Overview struct {
	Clusters []*OverviewCluster `json:"clusters"`
	Events   []*Event           `json:"events"`
}

// overviewEvents 总览里展示的最近事件数。
const overviewEvents = 20

// Overview 调用者数据范围内的集群状态、摘要与最近 24 小时的 Warning 事件。全部来自数据库
// （摘要由数据面周期计算后落库），不访问任何集群。
func (uc *InsightUsecase) Overview(ctx context.Context, caller Caller) (*Overview, error) {
	out := &Overview{Clusters: []*OverviewCluster{}, Events: []*Event{}}
	access, isAdmin, roleIDs, err := uc.access(ctx, caller.UserID)
	if err != nil || access.Empty() {
		return out, err
	}
	options, err := uc.clusters.Options(ctx, isAdmin, roleIDs)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(options))
	for _, o := range options {
		ids = append(ids, o.ID)
	}
	summaries, err := uc.repo.Summaries(ctx, ids)
	if err != nil {
		return nil, err
	}
	for _, o := range options {
		c := &OverviewCluster{
			ID: o.ID, Name: o.Name, DisplayName: o.DisplayName, Env: o.Env, Status: o.Status, LastError: o.LastError,
			K8sVersion: o.K8sVersion, Level: o.Level, Summary: json.RawMessage("null"),
		}
		if s := summaries[o.ID]; s != nil {
			c.HasDatasource, c.SummaryTime = s.HasDatasource, s.SummaryTime
			if s.SummaryTime != nil && len(s.Summary) > 0 {
				c.Summary = scopeSummary(s.Summary, access, o.ID)
			}
		}
		out.Clusters = append(out.Clusters, c)
	}
	now := uc.now()
	out.Events, _, err = uc.repo.ListEvents(ctx, EventFilter{Start: now.Add(-24 * time.Hour), End: now}, access, 1, overviewEvents)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// MetricPanelResult 一个面板的查询结果。
type MetricPanelResult struct {
	ID     string         `json:"id"`
	Title  string         `json:"title"`
	Unit   string         `json:"unit"`
	Series []MetricSeries `json:"series"`
	// Error 该面板查询失败的原因（其余面板不受影响）。
	Error string `json:"error,omitempty"`
}

// MetricsResult 指标查询结果。Configured 为 false 表示集群未关联数据源。
type MetricsResult struct {
	Configured bool                 `json:"configured"`
	Start      time.Time            `json:"start"`
	End        time.Time            `json:"end"`
	StepS      int64                `json:"stepSeconds"`
	Panels     []*MetricPanelResult `json:"panels"`
}

// QueryMetrics 查询指标面板。数据范围：调用者须在该集群上有授权；工作负载 / Pod 目标须能查看其命名空间。
func (uc *InsightUsecase) QueryMetrics(ctx context.Context, caller Caller, clusterID string, target MetricsTarget, panelIDs []string, start, end *time.Time) (*MetricsResult, error) {
	access, _, _, err := uc.access(ctx, caller.UserID)
	if err != nil {
		return nil, err
	}
	// 范围外的集群与不存在的集群同样应答，不透露集群是否存在。
	if !access.AllowsCluster(clusterID) {
		return nil, ErrClusterNotFound
	}
	c, err := uc.briefs.Brief(ctx, clusterID)
	if err != nil {
		return nil, err
	}
	panels, err := BuildPanels(c.Name, target)
	if err != nil {
		return nil, err
	}
	if (target.Kind == TargetWorkload || target.Kind == TargetPod) && !access.AllowsNamespace(clusterID, target.Namespace) {
		return nil, ErrForbidden
	}
	s, e, step, err := metricsWindow(start, end, uc.now())
	if err != nil {
		return nil, err
	}
	out := &MetricsResult{Start: s, End: e, StepS: int64(step / time.Second), Panels: []*MetricPanelResult{}}
	if c.DatasourceID == "" {
		return out, nil
	}
	out.Configured = true
	for _, p := range panels {
		if len(panelIDs) > 0 && !slices.Contains(panelIDs, p.ID) {
			continue
		}
		res := &MetricPanelResult{ID: p.ID, Title: p.Title, Unit: p.Unit, Series: []MetricSeries{}}
		for _, q := range p.Queries {
			points, err := uc.metrics.QueryRange(ctx, c.DatasourceID, q.Expr, s, e, step)
			if err != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				res.Error = kerrors.FromError(err).Message
				break
			}
			if points == nil {
				points = [][2]float64{}
			}
			res.Series = append(res.Series, MetricSeries{Name: q.Name, Points: points})
		}
		out.Panels = append(out.Panels, res)
	}
	return out, nil
}

// SanitizePoints 去掉 NaN / Inf（JSON 无法表示）。
func SanitizePoints(points [][2]float64) [][2]float64 {
	out := points[:0]
	for _, p := range points {
		if !math.IsNaN(p[1]) && !math.IsInf(p[1], 0) {
			out = append(out, p)
		}
	}
	return out
}

// HostLocation 主机在某个集群里的位置。
type HostLocation struct {
	ClusterID   string               `json:"clusterId"`
	ClusterName string               `json:"clusterName"`
	DisplayName string               `json:"displayName"`
	Node        string               `json:"node"`
	State       string               `json:"state"`
	Status      string               `json:"status"`
	Pods        int                  `json:"pods"`
	Workloads   []kc.LocatedWorkload `json:"workloads"`
	Truncated   bool                 `json:"truncated,omitempty"`
}

const (
	locateConcurrency = 8
	locateTimeout     = 5 * time.Second
)

func checkIP(ip string) (string, error) {
	addr, err := netip.ParseAddr(strings.TrimSpace(ip))
	if err != nil {
		return "", detail(ErrInvalidArgument, "ip is not a valid address")
	}
	return addr.String(), nil
}

// LocateHost 在调用者数据范围内的在线集群里按内网 IP 查找节点。各集群并发询问数据面（以调用者的身份，
// 能看到什么由集群判定），单个集群失败或超时只是不出现在结果里。
func (uc *InsightUsecase) LocateHost(ctx context.Context, caller Caller, ip string) ([]*HostLocation, error) {
	ip, err := checkIP(ip)
	if err != nil {
		return nil, err
	}
	isAdmin, roleIDs, err := uc.roles.EnabledRoleIDs(ctx, caller.UserID)
	if err != nil {
		return nil, err
	}
	out := []*HostLocation{}
	if !isAdmin && len(roleIDs) == 0 {
		return out, nil
	}
	options, err := uc.clusters.Options(ctx, isAdmin, roleIDs)
	if err != nil {
		return nil, err
	}
	var (
		mu  sync.Mutex
		wg  sync.WaitGroup
		sem = make(chan struct{}, locateConcurrency)
	)
	for _, o := range options {
		if o.Status != kc.ClusterOnline {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(o *ClusterOption) { // 退出路径：locateTimeout
			defer wg.Done()
			defer func() { <-sem }()
			cctx, cancel := context.WithTimeout(ctx, locateTimeout)
			defer cancel()
			raw, err := uc.planes.Call(cctx, caller, o.ID, kc.OpLocateNode, kc.LocateNodeRequest{IP: ip})
			if err != nil {
				return
			}
			var r kc.LocateNodeResponse
			if json.Unmarshal(raw, &r) != nil || !r.Found {
				return
			}
			if r.Workloads == nil {
				r.Workloads = []kc.LocatedWorkload{}
			}
			mu.Lock()
			out = append(out, &HostLocation{
				ClusterID: o.ID, ClusterName: o.Name, DisplayName: o.DisplayName, Node: r.Node, State: r.State, Status: r.Status,
				Pods: r.Pods, Workloads: r.Workloads, Truncated: r.Truncated,
			})
			mu.Unlock()
		}(o)
	}
	wg.Wait()
	slices.SortFunc(out, func(a, b *HostLocation) int { return strings.Compare(a.ClusterName, b.ClusterName) })
	return out, nil
}

// NodeHost 节点对应的 CMDB 主机资源：调用者须在该集群上有授权；结果受其 CMDB 数据范围约束。
func (uc *InsightUsecase) NodeHost(ctx context.Context, caller Caller, clusterID, ip string) ([]HostCI, error) {
	ip, err := checkIP(ip)
	if err != nil {
		return nil, err
	}
	access, _, _, err := uc.access(ctx, caller.UserID)
	if err != nil {
		return nil, err
	}
	if !access.AllowsCluster(clusterID) {
		return nil, ErrClusterNotFound
	}
	items, err := uc.hosts.FindByIP(ctx, caller.UserID, ip)
	if err != nil {
		return nil, err
	}
	if items == nil {
		items = []HostCI{}
	}
	return items, nil
}

// ErrForbidden 调用者的数据范围不含目标命名空间（403）。
var ErrForbidden = kerrors.Forbidden("K8S_FORBIDDEN", "your data scope does not cover this namespace")

var eventReasonPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]{0,63}$`)

// maxEventReasons 单个集群的上报原因上限。
const maxEventReasons = 50

// normalizeInsight 规整并校验集群的可观测配置：上报原因去重、格式合法；数据源 ID 去空白。
func (in *ClusterInput) normalizeInsight() error {
	in.DatasourceID = strings.TrimSpace(in.DatasourceID)
	seen := map[string]struct{}{}
	out := make([]string, 0, len(in.EventReasons))
	for _, r := range in.EventReasons {
		r = strings.TrimSpace(r)
		if r == "" {
			continue
		}
		if !eventReasonPattern.MatchString(r) {
			return detail(ErrClusterInvalid, "invalid event reason %q: letters and digits only, e.g. FailedScheduling", r)
		}
		if _, dup := seen[strings.ToLower(r)]; dup {
			continue
		}
		seen[strings.ToLower(r)] = struct{}{}
		out = append(out, r)
	}
	if len(out) > maxEventReasons {
		return detail(ErrClusterInvalid, "at most %d event reasons", maxEventReasons)
	}
	in.EventReasons = out
	return nil
}

// checkDatasource 关联的监控数据源须存在且已启用；沿用原值（cur）时不再校验——数据源之后被停用
// 不应让集群的其他字段也改不了。
func (uc *ClusterUsecase) checkDatasource(ctx context.Context, id, cur string) error {
	if id == "" || id == cur || uc.datasources == nil {
		return nil
	}
	ok, err := uc.datasources.Enabled(ctx, id)
	if err != nil {
		return err
	}
	if !ok {
		return detail(ErrClusterInvalid, "monitor datasource does not exist or is disabled")
	}
	return nil
}

// scopeSummary 按调用者的数据范围裁剪集群摘要：摘要由数据面以平台身份对整个集群计算，其中的异常工作负载、
// 频繁重启的 Pod 带着命名空间与名称。没有「全部命名空间」授权的调用者只保留其命名空间内的条目，
// 节点清单整体去掉；总量类数字（节点数、Pod 数、水位）保留。解析失败时不返回摘要（宁缺毋漏）。
func scopeSummary(raw json.RawMessage, access Access, clusterID string) json.RawMessage {
	if access.All {
		return raw
	}
	ca := access.Clusters[clusterID]
	if ca != nil && ca.AllNamespaces {
		return raw
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil || m == nil {
		return json.RawMessage("null")
	}
	for _, key := range []string{"abnormalWorkloads", "restartPods"} {
		var items []map[string]any
		if json.Unmarshal(m[key], &items) != nil {
			m[key] = json.RawMessage("[]")
			continue
		}
		kept := make([]map[string]any, 0, len(items))
		for _, it := range items {
			ns, _ := it["namespace"].(string)
			if _, ok := ca.Namespaces[ns]; ca != nil && ok && ns != "" {
				kept = append(kept, it)
			}
		}
		m[key], _ = json.Marshal(kept)
	}
	m["abnormalNodes"] = json.RawMessage("[]")
	out, err := json.Marshal(m)
	if err != nil {
		return json.RawMessage("null")
	}
	return out
}
