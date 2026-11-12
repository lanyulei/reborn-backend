package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/lanyulei/reborn/backend/notify/internal/biz"
	"github.com/lanyulei/reborn/backend/notify/internal/conf"

	"github.com/google/wire"
	"google.golang.org/protobuf/types/known/durationpb"
)

// ProviderSet is worker providers.
var ProviderSet = wire.NewSet(NewSet)

// Set 全部后台循环（按角色由 server 层挑选装配）。
type Set struct {
	ConfigRefresher *Loop
	Relay           *Loop // 仅 NATS 骨干（pg 下为 nil）
	Router          *Loop
	Sweeper         *Loop
	Sender          *Loop
	Dlq             *Loop
	Maintenance     *Loop
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

// NewSet 构造循环并注入 conf 参数到各用例；配置快照在此同步完成首次加载
// （失败即装配失败，禁止空配置上线）。
func NewSet(c *conf.Pipeline, cfg *biz.ConfigCache, router *biz.RouterUsecase, sweeper *biz.SweeperUsecase,
	sender *biz.SenderUsecase, dlq *biz.DlqUsecase, maintenance *biz.MaintenanceUsecase, relay *biz.RelayUsecase, locker biz.Locker,
	health biz.ChannelHealthRepo, hooks *Hooks, logger *slog.Logger,
) (*Set, error) {
	sender.WithHealth(health)
	if err := cfg.Load(context.Background()); err != nil {
		return nil, fmt.Errorf("initial config load: %w", err)
	}
	rc, sc, dc, mc := c.GetRouter(), c.GetSender(), c.GetDlq(), c.GetMaintenance()
	router.WithOptions(biz.RouterOptions{MaxRouteAttempts: rc.GetMaxRouteAttempts(), MaxAggWindow: dur(rc.GetMaxAggregateWindow(), 5*time.Minute)})
	sender.WithOptions(biz.SenderOptions{
		Lease: dur(sc.GetLease(), 5*time.Minute), SendTimeout: dur(sc.GetSendTimeout(), 3*time.Second),
		MaxSendTimeout: dur(sc.GetMaxSendTimeout(), 10*time.Second),
		RenderTimeout:  dur(sc.GetRenderTimeout(), 100*time.Millisecond), Concurrency: num(sc.GetConcurrency(), 16),
		MaxAttempts: sc.GetMaxAttempts(),
	})
	if hooks != nil && hooks.Observer != nil {
		sender.WithObserver(hooks.Observer)
	}
	if hooks != nil && hooks.OnBreaker != nil {
		hooks.OnBreaker(sender.Breaker())
	}
	ret := mc.GetRetention()
	maintenance.WithPartitionsAhead(num(mc.GetPartitionsAhead(), 2)).WithRetention(biz.Retention{
		Event: dur(ret.GetEvent(), 0), Dispatch: dur(ret.GetDispatch(), 0), DeliveryLog: dur(ret.GetDeliveryLog(), 0),
		Dlq: dur(ret.GetDlq(), 0), Dedup: dur(ret.GetDedup(), 0), AggGroup: dur(ret.GetAggGroup(), 0),
	})

	routerBatch, senderBatch, dlqBatch := num(rc.GetBatchSize(), 500), num(sc.GetBatchSize(), 100), num(dc.GetBatchSize(), 100)
	reapEvery := dur(sc.GetReapInterval(), 30*time.Second)
	var lastReap time.Time

	s := &Set{
		ConfigRefresher: NewLoop("config", dur(rc.GetConfigPoll(), 10*time.Second), logger, hooks, func(ctx context.Context) (int, error) {
			return 0, cfg.Refresh(ctx)
		}),
		Router: NewLoop("router", dur(rc.GetPollInterval(), 200*time.Millisecond), logger, hooks, func(ctx context.Context) (int, error) {
			return router.RouteOnce(ctx, routerBatch)
		}),
		Sender: NewLoop("sender", dur(sc.GetPollInterval(), 200*time.Millisecond), logger, hooks, func(ctx context.Context) (int, error) {
			if time.Since(lastReap) >= reapEvery {
				lastReap = time.Now()
				if n, err := sender.ReapOnce(ctx, senderBatch); err != nil {
					logger.ErrorContext(ctx, "reap expired leases failed", "error", err)
				} else if n > 0 {
					logger.WarnContext(ctx, "expired dispatch leases reaped", "count", n)
				}
			}
			return sender.SendOnce(ctx, senderBatch)
		}),
		Dlq: NewLoop("dlq", dur(dc.GetPollInterval(), 5*time.Second), logger, hooks, func(ctx context.Context) (int, error) {
			return dlq.ReplayOnce(ctx, dlqBatch)
		}),
	}
	sweepLeader := &leaderLoop{key: biz.LockSweeper, locker: locker, log: logger}
	s.Sweeper = NewLoop("sweeper", dur(rc.GetSweepInterval(), time.Second), logger, hooks, func(ctx context.Context) (int, error) {
		return sweepLeader.run(ctx, func(ctx context.Context) (int, error) { return sweeper.SweepOnce(ctx, 100) })
	})
	if relay != nil && relay.Enabled() {
		nc := c.GetNats()
		relayBatch := num(nc.GetRelayBatch(), 500)
		s.Relay = NewLoop("relay", dur(nc.GetRelayPoll(), 200*time.Millisecond), logger, hooks, func(ctx context.Context) (int, error) {
			return relay.RelayOnce(ctx, relayBatch)
		})
	}
	maintLeader := &leaderLoop{key: biz.LockMaintenance, locker: locker, log: logger}
	s.Maintenance = NewLoop("maintenance", dur(mc.GetInterval(), time.Hour), logger, hooks, func(ctx context.Context) (int, error) {
		return maintLeader.run(ctx, func(ctx context.Context) (int, error) {
			return 0, maintenance.RunOnce(ctx, time.Now().UTC())
		})
	})
	return s, nil
}
