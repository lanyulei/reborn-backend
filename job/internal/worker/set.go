// Package worker 把 biz 的后台用例包成 kratos 托管的循环（kit/loop）：租约清扫与日志保留期清理。
// 只随 engine 角色运行；角色门控在 server 装配层。
package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/lanyulei/reborn/backend/job/internal/biz"
	"github.com/lanyulei/reborn/backend/job/internal/conf"
	"github.com/lanyulei/reborn/backend/kit/loop"

	"github.com/google/wire"
	"google.golang.org/protobuf/types/known/durationpb"
)

// ProviderSet is worker providers.
var ProviderSet = wire.NewSet(NewSet)

// Set engine 角色的全部后台循环。
type Set struct {
	// Engine 租约清扫（每个实例都跑，语句幂等）与日志清理（advisory lock 选主）。
	Engine []*loop.Loop
}

// conf.proto 注明的缺省值。
const (
	defaultSweepInterval    = 5 * time.Second
	defaultLogPurgeInterval = time.Hour
	// lockLogPurge 日志清理的选主锁键（仅引擎使用）。
	lockLogPurge = "job:log-purge"
)

func dur(d *durationpb.Duration, def time.Duration) time.Duration {
	if d == nil || d.AsDuration() <= 0 {
		return def
	}
	return d.AsDuration()
}

// NewSet 构造循环并把 conf 参数（未设置或 ≤0 取缺省值）注入各用例。
//
// 租约清扫不选主：单语句条件更新 + SKIP LOCKED，多实例并发执行结果一致，且任一实例存活即可
// 及时判定失联。日志清理是成批删除，选主只为省重复劳动（语句同样幂等）。
func NewSet(c *conf.Engine, runners *biz.RunnerUsecase, housekeep *biz.HousekeepUsecase, locker loop.Locker,
	rec loop.Recorder, logger *slog.Logger,
) *Set {
	runners.WithOptions(biz.RunnerOptions{Lease: c.GetLease().AsDuration()})
	// log_retention：未设置取缺省，显式 0s = 永久保留。
	retention := biz.DefaultLogRetention
	if c.GetLogRetention() != nil {
		retention = c.GetLogRetention().AsDuration()
	}
	// 启动后一个租约之内不清扫：给停机期间仍在跑的节点留出续租的时间。
	housekeep.WithOptions(biz.HousekeepOptions{LogRetention: retention, SweepGrace: runners.Lease()})

	purgeLeader := loop.NewLeader(lockLogPurge, locker, logger)
	return &Set{Engine: []*loop.Loop{
		loop.New("lease-sweep", dur(c.GetSweepInterval(), defaultSweepInterval), logger, rec, housekeep.SweepLeases),
		loop.New("log-purge", dur(c.GetLogPurgeInterval(), defaultLogPurgeInterval), logger, rec, func(ctx context.Context) (int, error) {
			return purgeLeader.Run(ctx, housekeep.PurgeLogs)
		}),
	}}
}
