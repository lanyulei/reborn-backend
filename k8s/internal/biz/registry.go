package biz

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// InstanceRepo 数据面实例登记（k8s_instance）。
type InstanceRepo interface {
	// Heartbeat 写入或刷新本实例的心跳（数据库时钟）。
	Heartbeat(ctx context.Context, id, addr string, clusters int) error
	// Remove 删除本实例（正常退出）。
	Remove(ctx context.Context, id string) error
}

// OwnershipRepo 集群归属（k8s_cluster_runtime 的租约列）。
type OwnershipRepo interface {
	// Renew 续租并返回本实例仍持有的集群 ID；已停用 / 删除的集群一并放弃。
	Renew(ctx context.Context, instance string, lease time.Duration) ([]string, error)
	// Claim 领取至多 limit 个无主或租约过期的已启用集群（缺运行行则补建），返回新领取的集群 ID。
	Claim(ctx context.Context, instance string, lease time.Duration, limit int) ([]string, error)
	// Release 放弃本实例持有的全部集群（正常退出）。
	Release(ctx context.Context, instance string) error
}

// ClusterRepo 配置表只读访问。
type ClusterRepo interface {
	// ListEnabled 全部未删除且启用的集群。
	ListEnabled(ctx context.Context) ([]*Cluster, error)
	// Versions 配置版本（k8s_cfg_version）。
	Versions(ctx context.Context) (map[string]int64, error)
	// Scopes 某集群的全部角色数据范围。
	Scopes(ctx context.Context, clusterID string) ([]Scope, error)
}

// Connector 集群连接的生命周期（data/kube 实现）。
type Connector interface {
	// Connect 按配置建立（或以新配置替换）到集群的连接；不发起网络请求。
	Connect(ctx context.Context, c *Cluster) error
	// Disconnect 断开并释放该集群的连接、缓存、日志流与终端会话。
	Disconnect(clusterID string)
}

// RegistryOptions 归属参数。
type RegistryOptions struct {
	InstanceID  string
	Addr        string
	Lease       time.Duration
	MaxClusters int
}

// DefaultLease 缺省的归属租约。
const DefaultLease = 30 * time.Second

// Registry 本实例的登记与集群归属：心跳、领取 / 续租、配置变更后重建连接。
// owned 是「本实例当前负责且连接已建立」的集群快照，所有请求经 Owned 判定归属。
type Registry struct {
	instances InstanceRepo
	ownership OwnershipRepo
	clusters  ClusterRepo
	conn      Connector
	log       *slog.Logger
	opts      RegistryOptions

	mu       sync.RWMutex
	owned    map[string]*Cluster // 不变式：键集合 ⊆ 最近一次续租 / 领取成功的集合
	known    map[string]struct{} // 最近一次读到的全部已启用集群（区分「不存在」与「不归我」）
	versions map[string]int64
	// onScope 数据范围版本变化时的回调（装配期设置，只读）。
	onScope []func()
	// onDrop 放弃某集群时的回调（终端会话等随归属结束）。
	onDrop []func(clusterID string)
	reload bool
	// agents Agent 集群的归属来源（装配期设置；nil = 不接入 Agent 集群）。
	agents AgentHolder
	// syncMu 串行化 Sync：归属循环、配置轮询与隧道变更都会触发它。
	syncMu sync.Mutex
	closed bool // 受 syncMu 保护：Shutdown 之后为 true
}

// NewRegistry new a Registry.
func NewRegistry(instances InstanceRepo, ownership OwnershipRepo, clusters ClusterRepo, conn Connector, log *slog.Logger) *Registry {
	return &Registry{
		instances: instances, ownership: ownership, clusters: clusters, conn: conn, log: log,
		opts:  RegistryOptions{Lease: DefaultLease},
		owned: map[string]*Cluster{}, known: map[string]struct{}{}, versions: map[string]int64{}, reload: true,
	}
}

// WithOptions 注入参数（装配期调用一次）。
func (r *Registry) WithOptions(o RegistryOptions) {
	if o.Lease <= 0 {
		o.Lease = DefaultLease
	}
	r.opts = o
}

// WithAgents 注入 Agent 集群的归属来源（装配期调用一次）。
func (r *Registry) WithAgents(a AgentHolder) { r.agents = a }

// Lease 归属租约。
func (r *Registry) Lease() time.Duration { return r.opts.Lease }

// OnScopeChange 登记数据范围版本变化的回调（装配期调用）。
func (r *Registry) OnScopeChange(fn func()) { r.onScope = append(r.onScope, fn) }

// OnDrop 登记放弃集群时的回调（装配期调用）。
func (r *Registry) OnDrop(fn func(clusterID string)) { r.onDrop = append(r.onDrop, fn) }

// InstanceID 本实例 ID。
func (r *Registry) InstanceID() string { return r.opts.InstanceID }

// Owned 返回本实例负责的集群；不存在返回 404，不归本实例返回 421。
func (r *Registry) Owned(clusterID string) (*Cluster, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if c, ok := r.owned[clusterID]; ok {
		return c, nil
	}
	if _, ok := r.known[clusterID]; ok {
		return nil, errNotOwner
	}
	return nil, errClusterNotFound
}

// OwnedClusters 本实例负责的集群快照。
func (r *Registry) OwnedClusters() []*Cluster {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Cluster, 0, len(r.owned))
	for _, c := range r.owned {
		out = append(out, c)
	}
	return out
}

// ScopeVersion 最近一次读到的数据范围配置版本。
func (r *Registry) ScopeVersion() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.versions[k8scontract.CfgRoleScope]
}

// Heartbeat 刷新实例心跳（循环：每 5 秒）。
func (r *Registry) Heartbeat(ctx context.Context) (int, error) {
	r.mu.RLock()
	n := len(r.owned)
	r.mu.RUnlock()
	r.syncMu.Lock()
	defer r.syncMu.Unlock()
	if r.closed { // Shutdown 已删除实例登记：不再写回
		return 0, nil
	}
	return 0, r.instances.Heartbeat(ctx, r.opts.InstanceID, r.opts.Addr, n)
}

// PollConfig 轮询配置版本（循环：每 5 秒）：集群配置变化则下一轮 Sync 重读集群；数据范围变化触发回调。
func (r *Registry) PollConfig(ctx context.Context) (int, error) {
	vs, err := r.clusters.Versions(ctx)
	if err != nil {
		return 0, err
	}
	r.mu.Lock()
	clusterChanged := vs[k8scontract.CfgCluster] != r.versions[k8scontract.CfgCluster]
	scopeChanged := vs[k8scontract.CfgRoleScope] != r.versions[k8scontract.CfgRoleScope]
	first := len(r.versions) == 0
	r.versions = vs
	if clusterChanged {
		r.reload = true
	}
	r.mu.Unlock()
	if scopeChanged && !first {
		for _, fn := range r.onScope {
			fn()
		}
	}
	if clusterChanged && !first {
		return r.Sync(ctx)
	}
	return 0, nil
}

// Sync 续租、领取并对齐连接（循环：每 lease/3）。续租失败时不动现有连接：租约仍在有效期内，
// 数据库短暂抖动不应中断全部终端与日志；租约到期后他人可能接任，届时 Renew 的结果会把它们剔除。
//
// 直连集群走租约领取；Agent 集群不参与领取，由持有隧道的实例负责（AgentHolder）。
func (r *Registry) Sync(ctx context.Context) (int, error) {
	r.syncMu.Lock()
	defer r.syncMu.Unlock()
	if r.closed { // Shutdown 之后不再领取：否则在途的一轮会把刚放弃的集群又领回来
		return 0, nil
	}
	held, err := r.ownership.Renew(ctx, r.opts.InstanceID, r.opts.Lease)
	if err != nil {
		return 0, err
	}
	if r.agents != nil {
		ids, aerr := r.agents.Held(ctx)
		if aerr != nil {
			r.log.Warn("agent cluster ownership partially failed", "error", aerr)
		}
		held = append(held, ids...)
	}
	// max_clusters 只能限制直连集群的领取：Agent 连到哪个实例由 Agent 一侧决定，但已负责的 Agent 集群计入总数。
	limit := 64
	if r.opts.MaxClusters > 0 {
		if limit = r.opts.MaxClusters - len(held); limit < 0 {
			limit = 0
		}
	}
	claimed := 0
	if limit > 0 {
		ids, err := r.ownership.Claim(ctx, r.opts.InstanceID, r.opts.Lease, limit)
		if err != nil {
			return 0, err
		}
		claimed = len(ids)
		held = append(held, ids...)
	}
	all, err := r.clusters.ListEnabled(ctx)
	if err != nil {
		return 0, err
	}
	byID := make(map[string]*Cluster, len(all))
	known := make(map[string]struct{}, len(all))
	for _, c := range all {
		byID[c.ID] = c
		known[c.ID] = struct{}{}
	}
	want := make(map[string]*Cluster, len(held))
	for _, id := range held {
		if c, ok := byID[id]; ok {
			want[id] = c
		}
	}

	r.mu.RLock()
	prev := r.owned
	r.mu.RUnlock()
	next := make(map[string]*Cluster, len(want))
	var dropped []string
	for id := range prev {
		if _, ok := want[id]; !ok {
			dropped = append(dropped, id)
		}
	}
	for id, c := range want {
		if old, ok := prev[id]; ok && old.Generation() == c.Generation() {
			next[id] = old
			continue
		}
		if err := r.conn.Connect(ctx, c); err != nil {
			// 连接构造失败（证书 / 凭据无法解析）：仍登记为负责，由探活把原因写进 last_error。
			r.log.Warn("build cluster connection failed", "cluster", c.Name, "error", err)
		}
		next[id] = c
	}
	r.mu.Lock()
	r.owned, r.known, r.reload = next, known, false
	r.mu.Unlock()
	for _, id := range dropped {
		r.conn.Disconnect(id)
		for _, fn := range r.onDrop {
			fn(id)
		}
		r.log.Info("cluster ownership dropped", "cluster_id", id)
	}
	_ = claimed
	return 0, nil
}

// Shutdown 正常退出：放弃归属、断开连接、删除实例登记。ctx 应带独立超时。
func (r *Registry) Shutdown(ctx context.Context) {
	r.syncMu.Lock() // 等在途的 Sync 结束，并让之后的 Sync / Heartbeat 变成空操作
	r.closed = true
	r.syncMu.Unlock()
	r.mu.Lock()
	owned := r.owned
	r.owned = map[string]*Cluster{}
	r.mu.Unlock()
	for id := range owned {
		r.conn.Disconnect(id)
		for _, fn := range r.onDrop {
			fn(id)
		}
	}
	if err := r.ownership.Release(ctx, r.opts.InstanceID); err != nil {
		r.log.Warn("release cluster ownership failed", "error", err)
	}
	if err := r.instances.Remove(ctx, r.opts.InstanceID); err != nil {
		r.log.Warn("remove instance failed", "error", err)
	}
}
