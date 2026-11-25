package biz

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"
)

// AggGroup 聚合组行（sweeper 关闭窗口时 RETURNING 的权威数据）。
type AggGroup struct {
	Key            string
	BizID          string
	EventType      string
	RouteID        string
	FirstEventID   string
	FirstEventTime time.Time
	Count          int
	Samples        []AggSample
	Suppressed     bool
}

// AggRepo 聚合组仓储（sweeper 侧）。
type AggRepo interface {
	// ListDueGroups 列出窗口已到期的开放组键（非锁定扫描，候选集）。
	ListDueGroups(ctx context.Context, limit int) ([]string, error)
	// CloseGroup 在一个事务内：条件关闭组（status open→closed，RETURNING 权威
	// 计数/样本；count>threshold 置 suppressed_until），并经回调插入汇总
	// dispatch；组已被他人关闭/延长返回 (false, nil)。
	CloseGroup(ctx context.Context, key string, threshold int32, suppressMinutes int32,
		fn func(ctx context.Context, tx RouterTx, group *AggGroup) error) (bool, error)
	// RouteOf 读取组当前 route_id（关闭前取路由参数；非锁定）。
	RouteOf(ctx context.Context, key string) (string, error)
}

// SweeperUsecase 聚合窗口关闭器（router 角色内任务，advisory lock 选主单执行；
// 每组一个短事务，UPDATE…RETURNING 为汇总唯一数据源——无 SELECT-then-UPDATE
// 丢计数，评审 #3/#9）。
type SweeperUsecase struct {
	repo AggRepo
	cfg  *ConfigCache
	log  *slog.Logger
}

// NewSweeperUsecase new a SweeperUsecase.
func NewSweeperUsecase(repo AggRepo, cfg *ConfigCache, logger *slog.Logger) *SweeperUsecase {
	return &SweeperUsecase{repo: repo, cfg: cfg, log: logger}
}

// SweepOnce 关闭一批到期窗口；返回关闭数。
func (uc *SweeperUsecase) SweepOnce(ctx context.Context, limit int) (int, error) {
	snap := uc.cfg.Current()
	if snap == nil {
		return 0, errors.New("config snapshot not loaded")
	}
	keys, err := uc.repo.ListDueGroups(ctx, limit)
	if err != nil {
		return 0, err
	}
	closed, firstErr := 0, error(nil)
	for _, key := range keys {
		routeID, err := uc.repo.RouteOf(ctx, key)
		if err != nil {
			return closed, err
		}
		route := snap.routeByID(routeID)
		threshold, duration := int32(500), int32(30)
		if route != nil {
			threshold, duration = route.SuppressThreshold, route.SuppressDurationM
		}
		ok, err := uc.repo.CloseGroup(ctx, key, threshold, duration, func(ctx context.Context, tx RouterTx, g *AggGroup) error {
			if route == nil {
				// 路由已删除：组关闭但无处可发（配置变更导致）。组内事件此前已标 routed，
				// 汇总却发不出去——把本窗口内归入该组的**全部**事件改标 failed（一条有界
				// UPDATE，成员判定见 RouterTx.FailAggregatedEvents），控制台按失败状态可查。
				n, err := tx.FailAggregatedEvents(ctx, g.BizID, strings.TrimPrefix(g.Key, g.BizID+"|"), g.FirstEventTime)
				if err != nil {
					return err
				}
				uc.log.ErrorContext(ctx, "aggregate group route deleted, summary dropped", "group", key, "route_id", routeID,
					"count", g.Count, "first_event_id", g.FirstEventID, "marked_failed", n)
				return nil
			}
			var subs []Subscriber
			if route.Recipient.Subscription {
				var err error
				if subs, err = tx.ListSubscribers(ctx, g.BizID, g.EventType); err != nil {
					return err
				}
			}
			targets := ResolveTargets(snap, route, nil, subs)
			if len(targets) == 0 {
				uc.log.WarnContext(ctx, "aggregate summary resolved no targets", "group", key, "route_id", route.ID)
				return nil
			}
			agg := &AggContext{Count: g.Count, Samples: g.Samples, Digest: g.Suppressed}
			if agg.Digest {
				agg.Samples = nil // 摘要模式不列明细
			}
			ds, err := buildDispatches(g.FirstEventID, targets, time.Now().UTC(), &DispatchContent{Agg: agg})
			if err != nil {
				return err
			}
			return tx.InsertDispatches(ctx, ds)
		})
		if err != nil {
			if ctx.Err() != nil {
				return closed, err
			}
			// 单个组的错误不能阻塞其余组：CloseGroup 自身是短事务、失败即回滚，该组保持 open、
			// 下一轮重试。此前这里直接 return，一个确定性出错的组（如绑定了无法落库的渠道）会
			// 永远排在最前，其后所有组的汇总都发不出去。错误仍向上返回，让循环按退避节奏重试。
			uc.log.ErrorContext(ctx, "close aggregate group failed, continuing with the remaining groups",
				"group", key, "route_id", routeID, "error", err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if ok {
			closed++
		}
	}
	return closed, firstErr
}

// routeByID 在快照内按 ID 查路由（聚合组记录的 route_id）。
func (s *Snapshot) routeByID(id string) *Route {
	for _, list := range s.routes {
		for _, r := range list {
			if r.ID == id {
				return r
			}
		}
	}
	return nil
}
