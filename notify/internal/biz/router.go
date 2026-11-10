package biz

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/lanyulei/reborn/backend/notify/internal/pkg/idgen"
)

// RouterTx 是 router 批事务内可用的写原语（data 层以 pgx.Tx 实现）：批内全部
// 写与事件声明同事务，崩溃即整体回滚、事件仍 pending（无租约、零丢失）。
type RouterTx interface {
	// MarkEvent 事件终态：publish_state=1 + status（routed/suppressed/failed）。
	MarkEvent(ctx context.Context, eventID string, createTime time.Time, status int32) error
	// DeferEvent 瞬时错误：publish_attempts+1、next_publish_time 后移，事件仍 pending。
	DeferEvent(ctx context.Context, eventID string, createTime time.Time, next time.Time) error
	// InsertDispatches 批量插入发送任务（next_retry_time=now()，立即可领）。
	InsertDispatches(ctx context.Context, dispatches []*Dispatch) error
	// UpsertAggGroups 按排序后的键依次 upsert 聚合组，返回每键当前是否处于抑制期。
	UpsertAggGroups(ctx context.Context, increments []AggIncrement) (map[string]AggVerdict, error)
	// ListSubscribers 事务内解析订阅者。
	ListSubscribers(ctx context.Context, bizID, eventType string) ([]Subscriber, error)
	// LookupUserEmails 按用户 ID 集合取邮箱（显式收件里的用户 ID；停用/已删/无邮箱不返回）。
	LookupUserEmails(ctx context.Context, userIDs []string) (map[string]string, error)
}

// EventSource 待处理事件源（PG 骨干：FOR UPDATE SKIP LOCKED 声明一批 pending
// 事件并在同一事务内回调 fn；fn 返回 nil 提交，否则整批回滚）。NATS 骨干实现
// 后续按同接口接入。
type EventSource interface {
	ProcessPending(ctx context.Context, limit int, fn func(ctx context.Context, tx RouterTx, events []*Event) error) (int, error)
}

// AggIncrement 聚合组一次批内增量（同键事件合并后）。
type AggIncrement struct {
	Key            string // biz|group_key
	BizID          string
	EventType      string
	RouteID        string
	FirstEventID   string
	FirstEventTime time.Time
	Count          int
	Samples        []AggSample
	WindowS        int32
	MaxWindow      time.Duration
}

// AggVerdict upsert 返回：Suppressed=组处于抑制期（事件置 suppressed）。
type AggVerdict struct {
	Suppressed bool
}

// AggKey 聚合组主键 = biz|group_key（varchar(191)）。
func AggKey(bizID, groupKey string) string { return bizID + "|" + groupKey }

// maxAggSamples 聚合样本上限（jsonb 体积受控）。
const maxAggSamples = 20

// RouterOptions router 用例参数。
type RouterOptions struct {
	MaxRouteAttempts int32
	MaxAggWindow     time.Duration
}

// RouterUsecase 路由用例：对声明的一批事件做 静默 → 选路 → 聚合或生成 dispatch
// → 状态门。单事件错误分级（配置错误→failed；瞬时错误→延后重试；DB 错误→
// 整批回滚），不让一条毒事件卡住整批（评审 #2）。
type RouterUsecase struct {
	source EventSource
	cfg    *ConfigCache
	opts   RouterOptions
	log    *slog.Logger
}

// NewRouterUsecase new a RouterUsecase.
func NewRouterUsecase(source EventSource, cfg *ConfigCache, logger *slog.Logger) *RouterUsecase {
	return &RouterUsecase{source: source, cfg: cfg, log: logger, opts: RouterOptions{MaxRouteAttempts: 5, MaxAggWindow: 5 * time.Minute}}
}

// WithOptions 覆盖参数（装配时由 conf 注入）。
func (uc *RouterUsecase) WithOptions(opts RouterOptions) *RouterUsecase {
	if opts.MaxRouteAttempts > 0 {
		uc.opts.MaxRouteAttempts = opts.MaxRouteAttempts
	}
	if opts.MaxAggWindow > 0 {
		uc.opts.MaxAggWindow = opts.MaxAggWindow
	}
	return uc
}

// RouteOnce 声明并处理一批事件；返回处理条数（0 = 空闲）。
func (uc *RouterUsecase) RouteOnce(ctx context.Context, limit int) (int, error) {
	snap := uc.cfg.Current()
	if snap == nil {
		return 0, errors.New("config snapshot not loaded")
	}
	return uc.source.ProcessPending(ctx, limit, func(ctx context.Context, tx RouterTx, events []*Event) error {
		return uc.processBatch(ctx, tx, snap, events)
	})
}

// transientError 标记瞬时错误（订阅者查询失败等）：事件延后重试而非 failed。
type transientError struct{ err error }

func (e transientError) Error() string { return e.err.Error() }
func (e transientError) Unwrap() error { return e.err }

func (uc *RouterUsecase) processBatch(ctx context.Context, tx RouterTx, snap *Snapshot, events []*Event) error {
	now := time.Now().UTC()
	subscriberCache := make(map[string][]Subscriber)
	loadSubscribers := func(bizID, eventType string) ([]Subscriber, error) {
		key := bizID + "\x00" + eventType
		if subs, ok := subscriberCache[key]; ok {
			return subs, nil
		}
		subs, err := tx.ListSubscribers(ctx, bizID, eventType)
		if err != nil {
			return nil, transientError{fmt.Errorf("list subscribers: %w", err)}
		}
		subscriberCache[key] = subs
		return subs, nil
	}

	// 显式收件里的用户 ID 批内一次性解析（评审：避免逐事件查询）。
	var userIDs []string
	for _, ev := range events {
		userIDs = append(userIDs, UserIDs(ev.Recipients)...)
	}
	users := map[string]string{}
	if len(userIDs) > 0 {
		var err error
		if users, err = tx.LookupUserEmails(ctx, userIDs); err != nil {
			return fmt.Errorf("lookup recipient users: %w", err) // DB 级错误：整批回滚重试
		}
	}

	var dispatches []*Dispatch
	aggs := make(map[string]*AggIncrement)
	aggEvents := make(map[string][]*Event) // key → 本批归入该组的事件（等 upsert 裁决后定状态）

	for _, ev := range events {
		if snap.Silenced(ev.BizID, ev.EventType, ev.Severity, now) {
			if err := tx.MarkEvent(ctx, ev.ID, ev.CreateTime, EventStatusSuppressed); err != nil {
				return err
			}
			continue
		}
		route := snap.MatchRoute(ev.BizID, ev.EventType, ev.Severity, ev.PayloadObject())
		if route == nil {
			uc.log.WarnContext(ctx, "event has no matching route", "event_id", ev.ID, "biz", ev.BizID, "event_type", ev.EventType)
			if err := tx.MarkEvent(ctx, ev.ID, ev.CreateTime, EventStatusFailed); err != nil {
				return err
			}
			continue
		}
		if route.AggregateWindowS > 0 && ev.GroupKey != "" {
			key := AggKey(ev.BizID, ev.GroupKey)
			inc, ok := aggs[key]
			if !ok {
				inc = &AggIncrement{
					Key: key, BizID: ev.BizID, EventType: ev.EventType, RouteID: route.ID,
					FirstEventID: ev.ID, FirstEventTime: ev.CreateTime,
					WindowS: route.AggregateWindowS, MaxWindow: uc.opts.MaxAggWindow,
				}
				aggs[key] = inc
			}
			inc.Count++
			if len(inc.Samples) < maxAggSamples {
				inc.Samples = append(inc.Samples, AggSample{EventID: ev.ID, Title: ev.Title, Severity: ev.Severity, CreateTime: ev.CreateTime})
			}
			aggEvents[key] = append(aggEvents[key], ev)
			continue
		}
		var subs []Subscriber
		if route.Recipient.Subscription {
			var err error
			if subs, err = loadSubscribers(ev.BizID, ev.EventType); err != nil {
				if derr := uc.deferEvent(ctx, tx, ev, now, err); derr != nil {
					return derr
				}
				continue
			}
		}
		explicit := ClassifyRecipients(ev.Recipients, users)
		if explicit != nil && len(explicit.Unresolved) > 0 {
			uc.log.WarnContext(ctx, "explicit recipients unresolved", "event_id", ev.ID, "recipients", explicit.Unresolved)
		}
		targets := ResolveTargets(snap, route, explicit, subs)
		if len(targets) == 0 {
			uc.log.WarnContext(ctx, "route resolved no targets", "event_id", ev.ID, "route_id", route.ID)
			if err := tx.MarkEvent(ctx, ev.ID, ev.CreateTime, EventStatusFailed); err != nil {
				return err
			}
			continue
		}
		ds, err := buildDispatches(ev.ID, targets, now, nil)
		if err != nil {
			return err
		}
		dispatches = append(dispatches, ds...)
		if err := tx.MarkEvent(ctx, ev.ID, ev.CreateTime, EventStatusRouted); err != nil {
			return err
		}
	}

	if len(aggs) > 0 {
		keys := make([]string, 0, len(aggs))
		for k := range aggs {
			keys = append(keys, k)
		}
		sort.Strings(keys) // 确定锁序，消除副本间交叉死锁（评审 #1）
		incs := make([]AggIncrement, 0, len(keys))
		for _, k := range keys {
			incs = append(incs, *aggs[k])
		}
		verdicts, err := tx.UpsertAggGroups(ctx, incs)
		if err != nil {
			return err
		}
		for _, k := range keys {
			status := EventStatusRouted
			if verdicts[k].Suppressed {
				status = EventStatusSuppressed
			}
			for _, ev := range aggEvents[k] {
				if err := tx.MarkEvent(ctx, ev.ID, ev.CreateTime, status); err != nil {
					return err
				}
			}
		}
	}
	if len(dispatches) > 0 {
		if err := tx.InsertDispatches(ctx, dispatches); err != nil {
			return err
		}
	}
	return nil
}

// deferEvent 瞬时错误延后：publish_attempts ≥ 上限转 failed。
func (uc *RouterUsecase) deferEvent(ctx context.Context, tx RouterTx, ev *Event, now time.Time, cause error) error {
	if ev.PublishAttempts+1 >= uc.opts.MaxRouteAttempts {
		uc.log.ErrorContext(ctx, "event routing failed after retries", "event_id", ev.ID, "error", cause)
		return tx.MarkEvent(ctx, ev.ID, ev.CreateTime, EventStatusFailed)
	}
	shift := min(max(ev.PublishAttempts, 0), 4)
	backoff := time.Duration(1<<shift) * 5 * time.Second // 5s,10s,20s,40s,80s
	uc.log.WarnContext(ctx, "event routing deferred", "event_id", ev.ID, "attempt", ev.PublishAttempts+1, "error", cause)
	return tx.DeferEvent(ctx, ev.ID, ev.CreateTime, now.Add(backoff))
}

// buildDispatches 为目标集合生成发送任务（id UUIDv7，send_key 含 dispatch id）。
func buildDispatches(eventID string, targets []Target, now time.Time, content *DispatchContent) ([]*Dispatch, error) {
	var raw []byte
	if content != nil {
		var err error
		if raw, err = EncodeContent(content); err != nil {
			return nil, fmt.Errorf("encode dispatch content: %w", err)
		}
	}
	out := make([]*Dispatch, 0, len(targets))
	for _, t := range targets {
		id, err := idgen.NewID()
		if err != nil {
			return nil, err
		}
		out = append(out, &Dispatch{
			ID: id, EventID: eventID, ChannelID: t.ChannelID, Recipient: t.Recipient,
			Content: raw, Status: DispatchPending, Attempts: 0, NextRetryTime: now,
			SendKey: SendKey(eventID, t.ChannelID, t.Recipient, id), CreateTime: now,
		})
	}
	return out, nil
}
