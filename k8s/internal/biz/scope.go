package biz

import (
	"context"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// Binding 平台在集群内维护的一条角色绑定：Namespace 为空 = ClusterRoleBinding，否则为该命名空间的
// RoleBinding；主体是分组 Group，引用 ClusterRole Role。
type Binding struct {
	Namespace string
	Name      string
	Group     string
	Role      string
}

func (b Binding) key() string { return b.Namespace + "/" + b.Name }

// RBAC 集群内由平台管理的 RBAC 对象（data/kube 实现，平台身份；只触碰带 managed-by=reborn 标签的对象）。
type RBAC interface {
	// EnsureClusterView 确保 ClusterRole reborn:cluster-view 存在且规则正确。
	EnsureClusterView(ctx context.Context, clusterID string) error
	// ListBindings 全部由平台管理的 RoleBinding 与 ClusterRoleBinding（带平台标签，或名称以 reborn: 开头——
	// 标签可被有权编辑绑定的人摘掉，名称不可改）。foreign 为不归平台管理、但主体引用了平台用户 / 分组的绑定
	// （命名空间/名称）：它们让授权在平台收回后依然有效，平台不删除、只上报。
	ListBindings(ctx context.Context, clusterID string) (managed []Binding, foreign []string, err error)
	// ApplyBindings 先删除 remove，再创建 / 替换 upsert。单个对象失败不影响其余，错误合并返回。
	ApplyBindings(ctx context.Context, clusterID string, upsert, remove []Binding) error
}

// NamespaceLister 集群的全部命名空间（来自缓存，平台身份）。
type NamespaceLister interface {
	Namespaces(ctx context.Context, clusterID string) ([]string, error)
}

// DesiredBindings 由数据范围推出应有的绑定（技术设计 §4.4）；不存在的命名空间跳过并在 skipped 中返回。
func DesiredBindings(scopes []Scope, namespaces []string) (want []Binding, skipped []string) {
	exists := make(map[string]struct{}, len(namespaces))
	for _, ns := range namespaces {
		exists[ns] = struct{}{}
	}
	seen := map[string]struct{}{}
	add := func(b Binding) {
		if _, ok := seen[b.key()]; !ok {
			seen[b.key()] = struct{}{}
			want = append(want, b)
		}
	}
	add(Binding{Name: k8scontract.BindingPrefix + "admin", Group: k8scontract.AdminGroup, Role: "cluster-admin"})
	for _, s := range scopes {
		level := k8scontract.LevelClusterRole(s.Level)
		if level == "" || s.RoleID == "" {
			continue
		}
		group := k8scontract.RoleGroup(s.RoleID)
		name := k8scontract.BindingPrefix + s.RoleID + ":" + level
		role := level
		if s.Level == k8scontract.LevelAdmin {
			// 命名空间管理员不绑定内置的 admin：它能写 RoleBinding，持有者可以给自己（reborn:<用户>）另建一条
			// 不归平台管理的绑定，授权被收回后依然有效。改绑平台维护的 reborn:ns-admin（admin 去掉 RBAC 写权限）。
			role = NamespaceAdminRole
		}
		switch {
		case s.Namespace == k8scontract.AllNamespaces:
			add(Binding{Name: name, Group: group, Role: role})
			if debugLevel(s.Level) {
				add(Binding{Name: k8scontract.BindingPrefix + s.RoleID + ":debug", Group: group, Role: k8scontract.DebugRole})
			}
		case s.Level == k8scontract.LevelClusterAdmin:
			continue // 集群管理员只能授予全部命名空间（表上有 CHECK，这里再守一道）
		default:
			if _, ok := exists[s.Namespace]; !ok {
				skipped = append(skipped, s.Namespace)
				continue
			}
			add(Binding{Namespace: s.Namespace, Name: name, Group: group, Role: role})
			if debugLevel(s.Level) {
				add(Binding{Namespace: s.Namespace, Name: k8scontract.BindingPrefix + s.RoleID + ":debug", Group: group, Role: k8scontract.DebugRole})
			}
		}
		add(Binding{Name: k8scontract.BindingPrefix + s.RoleID + ":cluster-view", Group: group, Role: k8scontract.ClusterViewRole})
	}
	sort.Slice(want, func(i, j int) bool { return want[i].key() < want[j].key() })
	slices.Sort(skipped)
	return want, slices.Compact(skipped)
}

// NamespaceAdminRole 平台维护的 ClusterRole：内置 admin 的规则去掉对 RBAC 对象的写权限与
// escalate / bind / impersonate（data/kube 在对账时由内置 admin 的规则派生）。
const NamespaceAdminRole = "reborn:ns-admin"

// debugLevel 该档位是否随授权绑定调试容器的权限：开发与命名空间管理员（集群管理员本就拥有全部权限）。
func debugLevel(level int32) bool {
	return level == k8scontract.LevelEdit || level == k8scontract.LevelAdmin
}

// DiffBindings 对账：upsert 为缺失或内容不符的绑定，remove 为多余的绑定。
func DiffBindings(want, have []Binding) (upsert, remove []Binding) {
	current := make(map[string]Binding, len(have))
	for _, b := range have {
		current[b.key()] = b
	}
	for _, b := range want {
		if cur, ok := current[b.key()]; !ok || cur != b {
			upsert = append(upsert, b)
		}
		delete(current, b.key())
	}
	for _, b := range current {
		remove = append(remove, b)
	}
	sort.Slice(remove, func(i, j int) bool { return remove[i].key() < remove[j].key() })
	return upsert, remove
}

// scopeResyncInterval 无事件时的全量对账周期。
const scopeResyncInterval = 10 * time.Minute

// scopeGrace 数据范围配置变更后，允许集群内的绑定滞后多久。超过它仍未成功对账，就不再放行用户请求：
// 此时集群里生效的还是旧授权（可能是已被收回的）。
const scopeGrace = 60 * time.Second

type scopeRetry struct {
	at   time.Time
	wait time.Duration
}

type scopeState struct {
	gen        string
	version    int64
	namespaces string
	last       time.Time
}

// ScopeSyncer 把角色数据范围对账为集群内的 RoleBinding / ClusterRoleBinding。只有负责该集群的实例执行；
// 对账幂等。触发条件：数据范围版本变化、命名空间集合变化、距上次对账超过 10 分钟。
type ScopeSyncer struct {
	registry *Registry
	prober   *Prober
	clusters ClusterRepo
	rbac     RBAC
	ns       NamespaceLister
	runtime  RuntimeRepo
	log      *slog.Logger
	now      func() time.Time

	mu     sync.Mutex
	states map[string]*scopeState
	// onApplied 某集群的绑定发生变更后的回调（装配期设置）：清空该集群的权限询问缓存。
	onApplied []func(clusterID string)
	// retry 对账失败的集群 → 下次重试的时间与当前退避。
	retry map[string]scopeRetry
	// seen / seenAt 最近一次观察到的数据范围配置版本，及首次观察到它的时间（宽限期的起点）。
	seen   int64
	seenAt time.Time
}

// NewScopeSyncer new a ScopeSyncer.
func NewScopeSyncer(registry *Registry, prober *Prober, clusters ClusterRepo, rbac RBAC, ns NamespaceLister, runtime RuntimeRepo, log *slog.Logger) *ScopeSyncer {
	return &ScopeSyncer{
		registry: registry, prober: prober, clusters: clusters, rbac: rbac, ns: ns, runtime: runtime, log: log,
		now: time.Now, states: map[string]*scopeState{}, retry: map[string]scopeRetry{},
	}
}

// OnApplied 登记绑定变更后的回调（装配期调用）。
func (s *ScopeSyncer) OnApplied(fn func(clusterID string)) { s.onApplied = append(s.onApplied, fn) }

// Ready 报告该集群内的绑定是否与当前的数据范围配置一致：在当前连接配置下成功对账过，且对账的版本就是
// 当前版本——或版本刚变更、仍在宽限期内。对账失败（原因见 scope_error）超过宽限期即不再放行。
func (s *ScopeSyncer) Ready(c *Cluster) bool {
	version := s.registry.ScopeVersion()
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.states[c.ID]
	if st == nil || st.gen != c.Generation() {
		return false
	}
	if st.version == version {
		return true
	}
	// 版本刚变更：从第一次观察到它起算宽限期（这里也算一次观察：对账循环若停摆，宽限期照样会到）。
	if s.seen != version {
		s.seen, s.seenAt = version, s.now()
	}
	return s.now().Sub(s.seenAt) < scopeGrace
}

// 对账的并发与时限。集群之间互不等待：一个挂起的集群不应拖住其余集群的授权同步。
const (
	scopeSyncTimeout  = 30 * time.Second
	scopeSyncParallel = 4
	// scopeRetryMin / scopeRetryMax 某集群对账失败后的重试间隔（指数退避）。
	scopeRetryMin = 5 * time.Second
	scopeRetryMax = 2 * time.Minute
)

// Run 检查并按需对账本实例负责的在线集群（循环：每 2 秒）。
func (s *ScopeSyncer) Run(ctx context.Context) (int, error) {
	version := s.registry.ScopeVersion()
	s.mu.Lock()
	if s.seen != version {
		s.seen, s.seenAt = version, s.now()
	}
	s.mu.Unlock()
	alive := map[string]struct{}{}
	sem := make(chan struct{}, scopeSyncParallel)
	var wg sync.WaitGroup
	for _, c := range s.registry.OwnedClusters() {
		alive[c.ID] = struct{}{}
		if ctx.Err() != nil {
			break
		}
		if !s.prober.Online(c.ID) {
			continue
		}
		s.mu.Lock()
		retryAt := s.retry[c.ID].at
		s.mu.Unlock()
		if s.now().Before(retryAt) {
			continue
		}
		sem <- struct{}{}
		wg.Add(1)
		go func() { // 退出路径：syncOne 受 scopeSyncTimeout 约束；Run 返回前 wg.Wait
			defer wg.Done()
			defer func() { <-sem }()
			defer func() {
				if p := recover(); p != nil {
					s.log.Error("scope sync panicked", "cluster", c.Name, "panic", p)
				}
			}()
			s.syncOne(ctx, c, version)
		}()
	}
	wg.Wait()
	s.mu.Lock()
	for id := range s.states {
		if _, ok := alive[id]; !ok {
			delete(s.states, id)
		}
	}
	for id := range s.retry {
		if _, ok := alive[id]; !ok {
			delete(s.retry, id)
		}
	}
	s.mu.Unlock()
	return 0, nil
}

// syncOne 对账一个集群（带时限），并把结果写回 scope_version / scope_error。
func (s *ScopeSyncer) syncOne(ctx context.Context, c *Cluster, version int64) {
	cctx, cancel := context.WithTimeout(ctx, scopeSyncTimeout)
	defer cancel()
	namespaces, err := s.ns.Namespaces(cctx, c.ID)
	var skipped, foreign []string
	nsKey := ""
	if err == nil {
		nsKey = strings.Join(namespaces, ",")
		s.mu.Lock()
		st := s.states[c.ID]
		s.mu.Unlock()
		if st != nil && st.gen == c.Generation() && st.version == version && st.namespaces == nsKey && s.now().Sub(st.last) < scopeResyncInterval {
			return
		}
		skipped, foreign, err = s.sync(cctx, c, namespaces)
	}
	if ctx.Err() != nil {
		return // 服务退出中：不把「被取消」记成失败
	}
	msg := ""
	switch {
	case err != nil:
		// 含命名空间缓存建不起来（内存上限、集群慢）：同样要留下原因，否则集群一直「初始化中」却查不到为什么。
		msg = truncate(err.Error(), 512)
		s.log.Warn("scope sync failed", "cluster", c.Name, "error", err)
	case len(foreign) > 0:
		// 不归平台管理、却把权限授给平台用户 / 分组的绑定：平台收回授权后它们依然有效，需人工处理。
		msg = truncate("bindings not managed by the platform grant access to platform users (remove them in the cluster): "+strings.Join(foreign, ", "), 512)
		s.log.Warn("unmanaged bindings reference platform subjects", "cluster", c.Name, "bindings", len(foreign))
	case len(skipped) > 0:
		msg = truncate("namespaces not found, skipped: "+strings.Join(skipped, ", "), 512)
	}
	saved := version
	if err != nil {
		saved = 0 // 未成功：不前移已同步版本
	}
	wctx, wcancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	serr := s.runtime.SaveScope(wctx, s.registry.InstanceID(), c.ID, saved, msg)
	wcancel()
	if serr != nil {
		s.log.Warn("save scope sync result failed", "cluster", c.Name, "error", serr)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err == nil {
		s.states[c.ID] = &scopeState{gen: c.Generation(), version: version, namespaces: nsKey, last: s.now()}
		delete(s.retry, c.ID)
		return
	}
	r := s.retry[c.ID]
	if r.wait = min(max(r.wait*2, scopeRetryMin), scopeRetryMax); r.wait == 0 {
		r.wait = scopeRetryMin
	}
	r.at = s.now().Add(r.wait)
	s.retry[c.ID] = r
}

func (s *ScopeSyncer) sync(ctx context.Context, c *Cluster, namespaces []string) (skipped, foreign []string, err error) {
	scopes, err := s.clusters.Scopes(ctx, c.ID)
	if err != nil {
		return nil, nil, err
	}
	if err := s.rbac.EnsureClusterView(ctx, c.ID); err != nil {
		return nil, nil, err
	}
	have, foreign, err := s.rbac.ListBindings(ctx, c.ID)
	if err != nil {
		return nil, nil, err
	}
	want, skipped := DesiredBindings(scopes, namespaces)
	upsert, remove := DiffBindings(want, have)
	if len(upsert)+len(remove) == 0 {
		return skipped, foreign, nil
	}
	err = s.rbac.ApplyBindings(ctx, c.ID, upsert, remove)
	// 无论成败都通知：部分绑定可能已经变更，此前缓存的「拒绝 / 允许」不再可信。
	for _, fn := range s.onApplied {
		fn(c.ID)
	}
	if err != nil {
		return skipped, foreign, err
	}
	s.log.Info("scope synced", "cluster", c.Name, "upsert", len(upsert), "remove", len(remove))
	return skipped, foreign, nil
}
