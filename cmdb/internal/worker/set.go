// Package worker 把 biz 用例包成 kratos 托管的后台循环（kit/loop）：调度器为选主循环，
// 执行器按并发数生成多条循环（每条一次处理一个任务）。角色门控在 server 装配层。
package worker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/lanyulei/reborn/backend/cmdb/internal/biz"
	"github.com/lanyulei/reborn/backend/cmdb/internal/conf"
	"github.com/lanyulei/reborn/backend/kit/cmdbcontract"
	"github.com/lanyulei/reborn/backend/kit/loop"

	"github.com/google/wire"
	"google.golang.org/protobuf/types/known/durationpb"
)

// ProviderSet is worker providers.
var ProviderSet = wire.NewSet(NewSet)

// Set 全部后台循环。
type Set struct {
	Scheduler   *loop.Loop
	Maintenance *loop.Loop
	Workers     []*loop.Loop
}

func dur(d *durationpb.Duration, def time.Duration) time.Duration {
	if d == nil || d.AsDuration() <= 0 {
		return def
	}
	return d.AsDuration()
}

func num(v int32, def int) int {
	if v <= 0 {
		return def
	}
	return int(v)
}

// NewSet 构造循环并注入 conf 参数到各用例。属主令牌 = 主机名 + 随机串 + 循环序号。
func NewSet(c *conf.Sync, scheduler *biz.SchedulerUsecase, worker *biz.WorkerUsecase, maintenance *biz.MaintenanceUsecase,
	locker loop.Locker, rec loop.Recorder, logger *slog.Logger,
) (*Set, error) {
	sc, wc, mc := c.GetScheduler(), c.GetWorker(), c.GetMaintenance()
	retention := 365 * 24 * time.Hour
	if mc.GetHistoryRetention() != nil {
		retention = mc.GetHistoryRetention().AsDuration()
	}
	maintenance.WithOptions(biz.MaintenanceOptions{PartitionsAhead: num(mc.GetPartitionsAhead(), 2), HistoryRetention: retention})
	scheduler.WithOptions(biz.SchedulerOptions{StaleMultiplier: sc.GetStaleMultiplier(), StagingRetention: dur(sc.GetStagingRetention(), 24*time.Hour)})
	worker.WithOptions(biz.WorkerOptions{
		Lease: dur(wc.GetLease(), 5*time.Minute), CollectTimeout: dur(wc.GetCollectTimeout(), 10*time.Minute), StagingBatch: num(wc.GetStagingBatch(), 500),
	})
	nonce := make([]byte, 6)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	host, _ := os.Hostname()
	prefix := host + "-" + hex.EncodeToString(nonce)

	leader := loop.NewLeader(cmdbcontract.LockScheduler, locker, logger)
	s := &Set{
		Scheduler: loop.New("scheduler", dur(sc.GetPollInterval(), 10*time.Second), logger, rec, func(ctx context.Context) (int, error) {
			return leader.Run(ctx, func(ctx context.Context) (int, error) { return scheduler.TickOnce(ctx, time.Now()) })
		}),
	}
	maintLeader := loop.NewLeader(cmdbcontract.LockMaintenance, locker, logger)
	s.Maintenance = loop.New("maintenance", dur(mc.GetInterval(), time.Hour), logger, rec, func(ctx context.Context) (int, error) {
		return maintLeader.Run(ctx, func(ctx context.Context) (int, error) { return 0, maintenance.RunOnce(ctx, time.Now().UTC()) })
	})
	concurrency := num(wc.GetConcurrency(), 2)
	for i := range concurrency {
		token := fmt.Sprintf("%s-%d", prefix, i)
		s.Workers = append(s.Workers, loop.New(fmt.Sprintf("worker-%d", i), dur(wc.GetPollInterval(), 2*time.Second), logger, rec, func(ctx context.Context) (int, error) {
			return worker.RunOnce(ctx, token)
		}))
	}
	return s, nil
}
