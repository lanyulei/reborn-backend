// Package worker 把 biz 用例包成 kratos 托管的后台循环（kit/loop）：规划器侧为选主循环
// （任务物化、数据源探活、维护、自监控检测），评估器侧按并发数生成评估循环与记录规则循环、外加一条
// 事件指令循环；心跳循环不分角色。角色门控在 server 装配层。
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/lanyulei/reborn/backend/kit/loop"
	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/monitor/internal/biz"
	"github.com/lanyulei/reborn/backend/monitor/internal/conf"

	"github.com/google/wire"
	"google.golang.org/protobuf/types/known/durationpb"
)

// ProviderSet is worker providers.
var ProviderSet = wire.NewSet(NewUsecases, NewSet)

// Set 全部后台循环，按角色分组；server.NewServers 据此门控。
type Set struct {
	// Planner 规划器角色的循环：任务物化、数据源探活、维护、自监控检测（各自经 advisory lock 选主）。
	Planner []*loop.Loop
	// Evaluators 评估器角色的循环：评估循环与记录规则循环（各自独立领取一批任务）、事件指令循环。
	Evaluators []*loop.Loop
	// Always 不分角色、每个实例都跑的循环：心跳。
	Always []*loop.Loop
}

// conf.proto 注明的缺省值。
const (
	defaultMaterializeInterval = 5 * time.Second
	defaultProbeInterval       = 30 * time.Second
	defaultProbeTimeout        = 5 * time.Second
	defaultPollInterval        = time.Second
	defaultConcurrency         = 4
	defaultBatch               = 10
	defaultLease               = 60 * time.Second
	defaultMaxSeries           = 2000
	defaultQueryTimeoutMax     = 30 * time.Second
	defaultMaintenanceInterval = time.Hour
	defaultPartitionsAhead     = 2
	defaultEvalRecordRetention = 7 * 24 * time.Hour
	defaultAlertRetention      = 180 * 24 * time.Hour
	defaultWatchInterval       = 30 * time.Second
	defaultDatasourceDownAfter = 5 * time.Minute
	defaultRuleFailingAfter    = 10 * time.Minute
	defaultRuleFailingMin      = 3
	defaultInstanceStaleAfter  = time.Hour
	defaultInhibitRefresh      = 5 * time.Second
	defaultInhibitMaxAlerts    = 20000
	defaultMaxCI               = 100000
	defaultCommandInterval     = 2 * time.Second
	defaultCommandBatch        = 100
	defaultRecorderConcurrency = 1
	defaultHeartbeatInterval   = 10 * time.Second
	// probeLockKey 探活循环的选主锁键（派生自契约的 planner 锁键，仅引擎使用）。
	probeLockKey = monitorcontract.LockPlanner + ":probe"
	// leaderBeats 「持有选主锁」在心跳里的有效期 = 这么多个物化周期（物化每轮都标记一次）。
	leaderBeats = 3
)

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

// Usecases NewSet 装配的全部用例。
type Usecases struct {
	Cache       *biz.ConfigCache
	Inhibits    *biz.InhibitCache
	Stats       *biz.InstanceStats
	Planner     *biz.PlannerUsecase
	Evaluator   *biz.EvaluatorUsecase
	Maintenance *biz.MaintenanceUsecase
	Commands    *biz.CommandUsecase
	Records     *biz.RecordUsecase
	Watch       *biz.WatchUsecase
	Heartbeat   *biz.HeartbeatUsecase
}

// NewUsecases 把用例收成一个参数（wire 逐个注入）。
func NewUsecases(cache *biz.ConfigCache, inhibits *biz.InhibitCache, stats *biz.InstanceStats, planner *biz.PlannerUsecase,
	evaluator *biz.EvaluatorUsecase, maintenance *biz.MaintenanceUsecase, commands *biz.CommandUsecase, records *biz.RecordUsecase,
	watch *biz.WatchUsecase, heartbeat *biz.HeartbeatUsecase,
) *Usecases {
	return &Usecases{
		Cache: cache, Inhibits: inhibits, Stats: stats, Planner: planner, Evaluator: evaluator, Maintenance: maintenance,
		Commands: commands, Records: records, Watch: watch, Heartbeat: heartbeat,
	}
}

// NewSet 构造循环并把 conf 参数（未设置或 ≤0 取缺省值）注入各用例。
// 属主令牌 = 实例 ID（主机名 + 启动时的随机串）+ 循环序号：进程重启后令牌必不相同，旧进程的写回被围栏拒绝。
func NewSet(c *conf.Engine, info biz.InstanceInfo, uc *Usecases, locker loop.Locker, rec loop.Recorder, logger *slog.Logger) *Set {
	cache, planner, evaluator, maintenance := uc.Cache, uc.Planner, uc.Evaluator, uc.Maintenance
	pc, ec, mc, rc := c.GetPlanner(), c.GetEvaluator(), c.GetMaintenance(), c.GetRecorder()
	queryTimeoutMax := dur(ec.GetQueryTimeoutMax(), defaultQueryTimeoutMax)
	cache.WithOptions(biz.CacheOptions{
		MaxSeries: num(ec.GetMaxSeries(), defaultMaxSeries), QueryTimeoutMax: queryTimeoutMax, MaxCI: num(ec.GetMaxCi(), defaultMaxCI),
	})
	uc.Inhibits.WithOptions(biz.InhibitOptions{
		Refresh:   dur(ec.GetInhibitRefreshInterval(), defaultInhibitRefresh),
		MaxAlerts: num(ec.GetInhibitMaxAlerts(), defaultInhibitMaxAlerts),
	})
	planner.WithOptions(biz.PlannerOptions{ProbeTimeout: dur(pc.GetProbeTimeout(), defaultProbeTimeout)})
	evalOpts := biz.EvaluatorOptions{
		Batch: num(ec.GetBatch(), defaultBatch), Lease: dur(ec.GetLease(), defaultLease), QueryTimeoutMax: queryTimeoutMax,
	}
	evaluator.WithOptions(evalOpts)
	recOpts := evalOpts
	recOpts.Batch = num(rc.GetBatch(), defaultBatch)
	uc.Records.WithOptions(recOpts)
	uc.Commands.WithOptions(biz.CommandOptions{Batch: num(ec.GetCommandBatch(), defaultCommandBatch)})
	uc.Watch.WithOptions(biz.WatchOptions{
		DatasourceDownAfter:    dur(pc.GetDatasourceDownAfter(), defaultDatasourceDownAfter),
		RuleFailingAfter:       dur(pc.GetRuleFailingAfter(), defaultRuleFailingAfter),
		RuleFailingMinFailures: num(pc.GetRuleFailingMinFailures(), defaultRuleFailingMin),
		InstanceStaleAfter:     dur(pc.GetInstanceStaleAfter(), defaultInstanceStaleAfter),
	})
	materializeInterval := dur(pc.GetMaterializeInterval(), defaultMaterializeInterval)
	heartbeatInterval := dur(c.GetHeartbeatInterval(), defaultHeartbeatInterval)
	uc.Heartbeat.WithOptions(biz.HeartbeatOptions{LeaderTTL: max(leaderBeats*materializeInterval, 2*heartbeatInterval)})
	// alert_retention：未设置取缺省，显式 0s = 不删除。
	alertRetention := defaultAlertRetention
	if mc.GetAlertRetention() != nil {
		alertRetention = mc.GetAlertRetention().AsDuration()
	}
	maintenance.WithOptions(biz.MaintenanceOptions{
		PartitionsAhead:     num(mc.GetPartitionsAhead(), defaultPartitionsAhead),
		EvalRecordRetention: dur(mc.GetEvalRecordRetention(), defaultEvalRecordRetention),
		AlertRetention:      alertRetention,
	})

	prefix := info.ID

	// 任务物化与探活是两条独立节奏的循环（探活受网络超时拖累，不得阻塞物化）。loop.Leader 不可并发
	// 共用，而同一把 advisory lock 在两个连接上互斥，所以探活用派生锁键 planner 锁键 + ":probe"；
	// 两者可能由不同实例担任，语句各自幂等，无妨。
	materializeLeader := loop.NewLeader(monitorcontract.LockPlanner, locker, logger)
	probeLeader := loop.NewLeader(probeLockKey, locker, logger)
	maintenanceLeader := loop.NewLeader(monitorcontract.LockMaintenance, locker, logger)
	watchLeader := loop.NewLeader(monitorcontract.LockEngineWatch, locker, logger)
	s := &Set{Planner: []*loop.Loop{
		loop.New("materialize", materializeInterval, logger, rec, func(ctx context.Context) (int, error) {
			return materializeLeader.Run(ctx, func(ctx context.Context) (int, error) {
				uc.Stats.MarkLeader()
				return planner.Materialize(ctx)
			})
		}),
		loop.New("probe", dur(pc.GetProbeInterval(), defaultProbeInterval), logger, rec, func(ctx context.Context) (int, error) {
			return probeLeader.Run(ctx, planner.Probe)
		}),
		loop.New("maintenance", dur(mc.GetInterval(), defaultMaintenanceInterval), logger, rec, func(ctx context.Context) (int, error) {
			return maintenanceLeader.Run(ctx, func(ctx context.Context) (int, error) { return 0, maintenance.RunOnce(ctx) })
		}),
		loop.New("watch", dur(pc.GetWatchInterval(), defaultWatchInterval), logger, rec, func(ctx context.Context) (int, error) {
			// 恒返回 0：按周期执行，不连续。
			return watchLeader.Run(ctx, func(ctx context.Context) (int, error) {
				_, err := uc.Watch.RunOnce(ctx)
				return 0, err
			})
		}),
	}}
	s.Always = []*loop.Loop{
		loop.New("heartbeat", heartbeatInterval, logger, rec, func(ctx context.Context) (int, error) {
			return 0, uc.Heartbeat.RunOnce(ctx)
		}),
	}
	poll := dur(ec.GetPollInterval(), defaultPollInterval)
	for i := range num(ec.GetConcurrency(), defaultConcurrency) {
		token := fmt.Sprintf("%s-%d", prefix, i)
		s.Evaluators = append(s.Evaluators, loop.New(fmt.Sprintf("evaluator-%d", i), poll, logger, rec, func(ctx context.Context) (int, error) {
			return evaluator.RunOnce(ctx, token)
		}))
	}
	for i := range num(rc.GetConcurrency(), defaultRecorderConcurrency) {
		token := fmt.Sprintf("%s-r%d", prefix, i)
		s.Evaluators = append(s.Evaluators, loop.New(fmt.Sprintf("recorder-%d", i), poll, logger, rec, func(ctx context.Context) (int, error) {
			return uc.Records.RunOnce(ctx, token)
		}))
	}
	// 事件指令不选主：每个评估器实例一条循环，指令行以 SKIP LOCKED 互斥。
	s.Evaluators = append(s.Evaluators, loop.New("command", dur(ec.GetCommandInterval(), defaultCommandInterval), logger, rec,
		uc.Commands.RunOnce))
	return s
}
