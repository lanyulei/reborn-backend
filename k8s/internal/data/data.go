// Package data 实现 biz 定义的仓储与集群访问接口，是唯一允许 import 数据库驱动与 client-go 的层。
// 与 core 同库：配置表只读，运行表读写（k8s_plane_role，见 core/migrations/0072_k8s_init）。
package data

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/k8s/internal/conf"
	"github.com/lanyulei/reborn/backend/k8s/internal/data/kube"
	"github.com/lanyulei/reborn/backend/k8s/internal/data/tunnel"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	"github.com/lanyulei/reborn/backend/kit/loop"
	"github.com/lanyulei/reborn/backend/kit/secretbox"

	"github.com/google/wire"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ProviderSet is data providers.
var ProviderSet = wire.NewSet(
	NewData,
	wire.Bind(new(biz.Pinger), new(*Data)),
	NewLocker, wire.Bind(new(loop.Locker), new(*locker)),
	NewSecretBox,
	NewRegistryRepo, wire.Bind(new(biz.InstanceRepo), new(*registryRepo)), wire.Bind(new(biz.OwnershipRepo), new(*registryRepo)),
	NewClusterRepo, wire.Bind(new(biz.ClusterRepo), new(*clusterRepo)),
	NewRuntimeRepo, wire.Bind(new(biz.RuntimeRepo), new(*runtimeRepo)), wire.Bind(new(biz.OutboxRepo), new(*runtimeRepo)),
	NewAuditRepo, wire.Bind(new(biz.AuditRepo), new(*auditRepo)), wire.Bind(new(biz.ExecRepo), new(*auditRepo)),
	NewMaintenanceRepo, wire.Bind(new(biz.MaintenanceRepo), new(*maintenanceRepo)),
	NewInsightRepo, wire.Bind(new(biz.SummaryRepo), new(*insightRepo)), wire.Bind(new(biz.EventRepo), new(*insightRepo)),
	wire.Bind(new(biz.EventPurger), new(*insightRepo)),
	wire.Bind(new(biz.ClusterSummarizer), new(*kube.Manager)), wire.Bind(new(biz.EventSource), new(*kube.Manager)),
	NewTunnelHub, NewTunnelDialer, wire.Bind(new(biz.Tunnels), new(*tunnel.Hub)), NewKubeManager,
	NewAgentRepo, wire.Bind(new(biz.AgentTokenRepo), new(*agentRepo)), wire.Bind(new(biz.AgentRuntimeRepo), new(*agentRepo)),
	wire.Bind(new(biz.Connector), new(*kube.Manager)), wire.Bind(new(biz.ClusterProbe), new(*kube.Manager)),
	wire.Bind(new(biz.RBAC), new(*kube.Manager)), wire.Bind(new(biz.NamespaceLister), new(*kube.Manager)),
	wire.Bind(new(biz.Executor), new(*kube.Manager)), wire.Bind(new(biz.Streamer), new(*kube.Manager)),
	wire.Bind(new(biz.ClusterTester), new(*kube.Manager)), wire.Bind(new(biz.Shell), new(*kube.Manager)),
	NewChangeRepo, wire.Bind(new(biz.ChangeRepo), new(*changeRepo)), wire.Bind(new(biz.ChangeExecutor), new(*kube.Manager)),
	wire.Bind(new(biz.KubeProxier), new(*kube.Manager)), wire.Bind(new(biz.FileStore), new(*kube.Manager)),
	wire.Bind(new(biz.Inspector), new(*kube.Manager)),
	NewInspectionRepo, wire.Bind(new(biz.InspectionRepo), new(*inspectionRepo)),
)

// NewTunnelHub 本实例上的 Agent 隧道注册表；cleanup 关闭全部隧道。
func NewTunnelHub(logger *slog.Logger) (*tunnel.Hub, func()) {
	h := tunnel.NewHub(logger)
	return h, h.Close
}

// NewTunnelDialer Agent 集群的拨号器：在该集群的隧道上开流；没有隧道即「Agent 未连接」。
func NewTunnelDialer(h *tunnel.Hub) kube.TunnelDialer { return h }

// NewKubeManager 构造集群连接注册表；cleanup 断开全部连接。
func NewKubeManager(c *conf.Plane, box *SecretBox, clusters *clusterRepo, tunnel kube.TunnelDialer, logger *slog.Logger) (*kube.Manager, func()) {
	m := kube.NewManager(box, clusters, clusters, tunnel, kube.Options{
		QPS: c.GetQps(), Burst: int(c.GetBurst()),
		CacheIdle: c.GetCacheIdle().AsDuration(), SyncTimeout: c.GetCacheSyncTimeout().AsDuration(),
		MemoryLimit: uint64(max(c.GetMemoryLimitMb(), 0)) << 20,
	}, logger)
	m.EnableApps(clusters)
	return m, m.Close
}

// dataPingTimeout 启动连通性检查超时。
const dataPingTimeout = 3 * time.Second

// Data 持有共享连接池。
type Data struct {
	Pool *pgxpool.Pool
}

// NewData 构造连接池并 Ping 验证；cleanup 释放连接。
func NewData(c *conf.Data) (*Data, func(), error) {
	pool, err := dbx.NewPool(c.GetDatabase().GetSource(), dbx.PoolOptions{
		MaxConns:    c.GetDatabase().GetMaxConns(),
		MinConns:    c.GetDatabase().GetMinConns(),
		MaxIdleTime: c.GetDatabase().GetMaxConnIdleTime().AsDuration(),
		PingTimeout: dataPingTimeout,
	})
	if err != nil {
		return nil, nil, err
	}
	return &Data{Pool: pool}, func() { pool.Close() }, nil
}

// Ping 健康检查（1s 超时）。
func (d *Data) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return d.Pool.Ping(ctx)
}

// SecretBox 凭据解密器 = kit/secretbox.Box（与 core 写路径同一实现）；数据面只 Open。
type SecretBox struct {
	box *secretbox.Box
}

// ErrMasterKeyUnavailable 主密钥未配置。
var ErrMasterKeyUnavailable = errors.New("credential master key unavailable")

// NewSecretBox 由配置构造（主密钥为空 → 解密失败，直连集群记为不可达；不阻断启动）。
func NewSecretBox(c *conf.Notify) (*SecretBox, error) {
	box, err := secretbox.New(c.GetMaster().GetKey())
	if err != nil {
		return nil, err
	}
	return &SecretBox{box: box}, nil
}

// Open 解密 k8s_cluster.credential；主密钥未配置返回 ErrMasterKeyUnavailable。
// 明文只应驻留调用栈与集群连接配置里，不得写入日志与错误信息。
func (b *SecretBox) Open(ciphertext []byte) (string, error) {
	out, err := b.box.Open(ciphertext)
	if errors.Is(err, secretbox.ErrUnavailable) {
		return "", ErrMasterKeyUnavailable
	}
	return out, err
}

var (
	_ loop.Locker = (*locker)(nil)
	_ biz.Pinger  = (*Data)(nil)
)

// locker PG advisory lock 分布式互斥（会话级锁绑定专用连接，连接断即失主）。
type locker struct {
	data *Data
}

// NewLocker new a locker.
func NewLocker(data *Data) *locker { return &locker{data: data} }

// TryLock implements loop.Locker.
func (l *locker) TryLock(ctx context.Context, key string) (func(), bool, error) {
	return dbx.TryAdvisoryLock(ctx, l.data.Pool, key)
}
