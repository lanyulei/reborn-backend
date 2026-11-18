// Package worker 把 biz 的后台用例包成 kratos 托管的循环（kit/loop）。
package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/k8s/internal/conf"
	"github.com/lanyulei/reborn/backend/kit/loop"

	"github.com/google/wire"
	"google.golang.org/protobuf/types/known/durationpb"
)

// ProviderSet is worker providers.
var ProviderSet = wire.NewSet(NewAgentLoops, NewSet, NewGovLoops)

// Set 数据面的全部后台循环。
type Set struct {
	Loops []*loop.Loop
}

const (
	heartbeatInterval     = 5 * time.Second
	configPollInterval    = 5 * time.Second
	scopeSyncInterval     = 2 * time.Second
	newProbeInterval      = 2 * time.Second
	instancePurgeInterval = 15 * time.Second
	maintenanceInterval   = time.Hour
	// summaryInterval 总览摘要的到期检查间隔（各集群的实际刷新间隔见 biz.Summarizer）。
	summaryInterval = 15 * time.Second
	// eventCollectInterval Warning 事件的采集间隔。
	eventCollectInterval = 30 * time.Second
	defaultProbeInterval = 30 * time.Second
	// lockMaintenance 维护任务的选主锁键。
	lockMaintenance = "k8s:maintenance"
	// lockInstancePurge 下线实例清理的选主锁键。
	lockInstancePurge = "k8s:instance-purge"
)

func dur(d *durationpb.Duration, def time.Duration) time.Duration {
	if d == nil || d.AsDuration() <= 0 {
		return def
	}
	return d.AsDuration()
}

// NewSet 构造循环并把 conf 参数（未设置或 ≤0 取缺省值）注入各用例。
//
// 心跳、归属、配置轮询、探活、授权同步每个实例都跑（各自只处理本实例负责的集群）；
// 下线实例清理与分区维护选主，只为省重复劳动（语句同样幂等）。
func NewSet(c *conf.Plane, registry *biz.Registry, prober *biz.Prober, scopes *biz.ScopeSyncer, maint *biz.Maintenance,
	locker loop.Locker, rec loop.Recorder, logger *slog.Logger, summarizer *biz.Summarizer, events *biz.EventCollector, changes *biz.ChangeRunner,
) *Set {
	lease := dur(c.GetLease(), biz.DefaultLease)
	maint.WithInstanceDeadAfter(2 * lease)
	// audit_retention：未设置取缺省，显式 0s = 永久保留。
	if c.GetAuditRetention() != nil {
		maint.WithRetention(c.GetAuditRetention().AsDuration())
	}
	purgeLeader := loop.NewLeader(lockInstancePurge, locker, logger)
	maintLeader := loop.NewLeader(lockMaintenance, locker, logger)
	eventPurgeLeader := loop.NewLeader("k8s:event-purge", locker, logger)
	return &Set{Loops: []*loop.Loop{
		// 心跳与续租必须在租约之内成功一次：失败退避封顶在各自的周期上（缺省的指数退避几次失败后
		// 就会超过租约，数据库恢复了本实例却还在等，集群被别人领走）。
		guarded("heartbeat", heartbeatInterval, logger, rec, registry.Heartbeat).WithMaxBackoff(heartbeatInterval),
		guarded("ownership", lease/3, logger, rec, registry.Sync).WithMaxBackoff(lease / 3),
		guarded("config-poll", configPollInterval, logger, rec, registry.PollConfig),
		guarded("probe", dur(c.GetProbeInterval(), defaultProbeInterval), logger, rec, prober.Run),
		guarded("probe-new", newProbeInterval, logger, rec, prober.RunNew),
		guarded("scope-sync", scopeSyncInterval, logger, rec, scopes.Run),
		guarded("summary", summaryInterval, logger, rec, summarizer.Run),
		guarded("event-collect", eventCollectInterval, logger, rec, events.Run),
		// 已批准的变更单：每个实例只执行自己负责的集群上的（行锁 SKIP LOCKED 领取）。
		guarded("change-exec", 3*time.Second, logger, rec, changes.Run),
		guarded("event-purge", maintenanceInterval, logger, rec, func(ctx context.Context) (int, error) {
			return eventPurgeLeader.Run(ctx, events.Purge)
		}),
		guarded("instance-purge", instancePurgeInterval, logger, rec, func(ctx context.Context) (int, error) {
			return purgeLeader.Run(ctx, maint.PurgeInstances)
		}),
		guarded("maintenance", maintenanceInterval, logger, rec, func(ctx context.Context) (int, error) {
			return maintLeader.Run(ctx, maint.Run)
		}),
	}}
}
