package data

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/k8s"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/k8splane"

	"github.com/jackc/pgx/v5"
)

const (
	// instanceTTL 归属缓存有效期：归属转移后最迟这么久（或数据面一次 K8S_NOT_OWNER 应答后立即）改走新实例。
	instanceTTL = 3 * time.Second
	// instanceLiveSeconds 实例存活判定：心跳 5 秒一次，15 秒无心跳视为下线。
	instanceLiveSeconds = 15.0
	// instanceCacheMax 归属缓存的条数上限（集群数为几十个量级；超过即整体清空）。
	instanceCacheMax = 4096
)

var _ k8splane.Resolver = (*instanceResolver)(nil)

type cachedAddr struct {
	addr   string
	expire time.Time
}

// instanceResolver 从 k8s_cluster_runtime / k8s_instance 定位数据面实例，带短时缓存。
// 地址只来自数据面自己登记的 k8s_instance.addr。
type instanceResolver struct {
	data *basedata.Data

	mu     sync.Mutex // 保护 owners / any
	owners map[string]cachedAddr
	any    cachedAddr
}

// NewInstanceResolver new an instanceResolver.
func NewInstanceResolver(data *basedata.Data) *instanceResolver {
	return &instanceResolver{data: data, owners: map[string]cachedAddr{}}
}

// Owner implements k8splane.Resolver.
func (r *instanceResolver) Owner(ctx context.Context, clusterID string) (string, error) {
	now := time.Now()
	r.mu.Lock()
	if c, ok := r.owners[clusterID]; ok && now.Before(c.expire) {
		r.mu.Unlock()
		return c.addr, nil
	}
	r.mu.Unlock()

	var enabled bool
	var addr string
	err := r.data.Pool.QueryRow(ctx,
		`SELECT c.enabled, COALESCE(i.addr, '')
		 FROM k8s_cluster c
		 LEFT JOIN k8s_cluster_runtime rt ON rt.cluster_id = c.id
		 LEFT JOIN k8s_instance i ON i.id = rt.owner_instance AND i.heartbeat_time > now() - make_interval(secs => $2)
		 WHERE c.id = $1 AND c.delete_time IS NULL`, clusterID, instanceLiveSeconds).Scan(&enabled, &addr)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && !enabled) {
		return "", biz.ErrClusterNotFound
	}
	if err != nil {
		return "", fmt.Errorf("resolve k8s cluster owner: %w", err)
	}
	if addr == "" {
		return "", biz.PlaneUnavailable("no live data plane instance owns this cluster yet")
	}
	r.mu.Lock()
	if len(r.owners) >= instanceCacheMax {
		clear(r.owners)
	}
	r.owners[clusterID] = cachedAddr{addr: addr, expire: now.Add(instanceTTL)}
	r.mu.Unlock()
	return addr, nil
}

// Any implements k8splane.Resolver。
func (r *instanceResolver) Any(ctx context.Context) (string, error) {
	now := time.Now()
	r.mu.Lock()
	if r.any.addr != "" && now.Before(r.any.expire) {
		addr := r.any.addr
		r.mu.Unlock()
		return addr, nil
	}
	r.mu.Unlock()

	var addr string
	err := r.data.Pool.QueryRow(ctx,
		`SELECT addr FROM k8s_instance WHERE heartbeat_time > now() - make_interval(secs => $1) ORDER BY clusters, id LIMIT 1`,
		instanceLiveSeconds).Scan(&addr)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", biz.ErrPlaneUnavailable
	}
	if err != nil {
		return "", fmt.Errorf("resolve k8s data plane instance: %w", err)
	}
	r.mu.Lock()
	r.any = cachedAddr{addr: addr, expire: now.Add(instanceTTL)}
	r.mu.Unlock()
	return addr, nil
}

// Invalidate implements k8splane.Resolver。
func (r *instanceResolver) Invalidate(clusterID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if clusterID == "" {
		r.any = cachedAddr{}
		return
	}
	delete(r.owners, clusterID)
}
