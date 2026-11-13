package biz

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

// 退避表（设计 §7.1）：30s → 2m → 8m → 30m → 2h，+0~20% jitter；首发 + 5 次重试。
var retryBackoff = []time.Duration{30 * time.Second, 2 * time.Minute, 8 * time.Minute, 30 * time.Minute, 2 * time.Hour}

// Backoff 返回第 attempt 次（1 起）失败后的下次重试延迟（含 jitter）。
func Backoff(attempt int32) time.Duration {
	idx := int(attempt) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(retryBackoff) {
		idx = len(retryBackoff) - 1
	}
	base := retryBackoff[idx]
	return base + time.Duration(rand.Int64N(int64(base)/5+1)) //nolint:gosec // 抖动非安全用途
}

// 发送侧延后口径（不计尝试）：无令牌按桶缺口、熔断打开、渠道停用、secret 不可用、
// 发送前瞬时错误（DB 抖动 / 渲染超时：放回而非烧掉 5 分钟租约 + 一次尝试）。
const (
	releaseMinDelay       = time.Second
	releaseBreakerDelay   = 5 * time.Second
	releaseDisabledDelay  = 60 * time.Second
	releaseSecretDelay    = 60 * time.Second
	releaseTransientDelay = 5 * time.Second
)

// writebackTimeout 发送后落账（Finish/Release/熔断记录）的独立时限：落账不随
// 停机取消，只受本时限约束。
const writebackTimeout = 10 * time.Second

// defaultMaxSendTimeout 渠道显式超时的缺省上限。
const defaultMaxSendTimeout = 10 * time.Second

// ClaimedDispatch 已声明的任务（含围栏令牌 Attempts：声明时 +1 后的值）。
type ClaimedDispatch struct {
	Dispatch
}

// FinishResult 一次尝试的落账结果。
type FinishResult struct {
	OK            bool
	Retryable     bool
	Error         string
	ProviderMsgID string
	LatencyMs     int32
	// RetryAfter 失败且可重试时距下次重试的间隔（仓储写 now() + RetryAfter，时钟一律
	// 取数据库）；Dead 由仓储按 attempts ≥ MaxAttempts 或 !Retryable 判定。
	RetryAfter time.Duration
	// Content / TemplateID 本次首次渲染的快照（nil = 本次未渲染，列保持不变）：随
	// 落账同事务写入，省一次发送前的独立往返。
	Content    []byte
	TemplateID string
}

// DispatchRepo 发送任务仓储（sender 侧）。所有写语句以 (id, create_time, status=2,
// attempts=声明值) 围栏：迟到的旧持有者被拒绝（ErrStaleClaim），评审 #4。
type DispatchRepo interface {
	// Claim 声明一批到期任务：status→2、attempts+1、租约 next_retry_time=now()+lease；
	// paused 渠道排除。
	Claim(ctx context.Context, limit int, lease time.Duration, paused []string) ([]*ClaimedDispatch, error)
	// Finish 落账：delivery_log + 终态/退避 + 渲染快照（围栏）；dead 同事务写
	// notify_dlq。单事务单批（一次往返）。
	Finish(ctx context.Context, d *ClaimedDispatch, res FinishResult, maxAttempts int32) error
	// Release 不计尝试放回：status→1、attempts-1、next_retry_time=now()+delay（围栏）。
	Release(ctx context.Context, d *ClaimedDispatch, delay time.Duration) error
	// ReapExpired 回收租约过期的 sending 行：记失败尝试并按退避转 failed / dead。
	ReapExpired(ctx context.Context, limit int, maxAttempts int32, backoff func(int32) time.Duration) (int, error)
	// GetEvents 批量取源事件（渲染上下文）：一批 dispatch 的 distinct event_id 一次
	// 往返；maxCreateTime 为分区剪枝上界（事件先于其 dispatch 创建）。缺失的 id
	// 不在结果中（已按保留期清理）。
	GetEvents(ctx context.Context, ids []string, maxCreateTime time.Time) (map[string]*Event, error)
}

// ErrStaleClaim 围栏拒绝：任务已被回收/他人重领，本次结果丢弃（at-least-once 既定）。
var ErrStaleClaim = errors.New("dispatch claim is stale")

// ErrEventNotFound 源事件不存在（已按保留期清理）→ 不可重试。
var ErrEventNotFound = errors.New("source event not found")

// RateLimiter 令牌桶（PG 条件 UPDATE 共享配额）：ok=false 时 wait 为建议等待。
type RateLimiter interface {
	Take(ctx context.Context, ch *Channel) (ok bool, wait time.Duration, err error)
}

// SendObserver 发送结果观测钩子（指标）：result ∈ sent / retry / dead / released；
// ObserveSendLatency 记录一次真实出站的耗时（成功与失败都记）。
type SendObserver interface {
	ObserveDispatch(channelType, result string)
	ObserveSendLatency(channelType string, d time.Duration)
}

// 观测结果取值。
const (
	ObserveSent     = "sent"
	ObserveRetry    = "retry"
	ObserveDead     = "dead"
	ObserveReleased = "released"
)

type noopObserver struct{}

func (noopObserver) ObserveDispatch(string, string)           {}
func (noopObserver) ObserveSendLatency(string, time.Duration) {}

// SenderOptions sender 用例参数。
type SenderOptions struct {
	Lease time.Duration
	// SendTimeout 渠道未显式配置超时时的单次发送超时。
	SendTimeout time.Duration
	// MaxSendTimeout 渠道显式超时（webhook timeout_s）的上限。
	MaxSendTimeout time.Duration
	RenderTimeout  time.Duration
	Concurrency    int
	MaxAttempts    int32
}

// SenderUsecase 发送用例：Claim → 渠道健康检查 → 渲染快照 → 熔断 → 令牌桶 →
// 适配器发送 → Finish（成功/退避重试/死信）。
type SenderUsecase struct {
	repo     DispatchRepo
	cfg      *ConfigCache
	limiter  RateLimiter
	sender   ChannelSender
	breaker  *SharedBreaker
	renderer *Renderer
	observer SendObserver
	opts     SenderOptions
	log      *slog.Logger

	pausedMu sync.Mutex
	paused   map[string]time.Time // channel → 跳过至（限流/熔断期间不再声明该渠道）
}

// NewSenderUsecase new a SenderUsecase.
func NewSenderUsecase(repo DispatchRepo, cfg *ConfigCache, limiter RateLimiter, sender ChannelSender, renderer *Renderer, logger *slog.Logger) *SenderUsecase {
	return &SenderUsecase{
		repo: repo, cfg: cfg, limiter: limiter, sender: sender, renderer: renderer, log: logger,
		breaker: NewSharedBreaker(nil, logger), paused: make(map[string]time.Time), observer: noopObserver{},
		opts: SenderOptions{
			Lease: 5 * time.Minute, SendTimeout: 3 * time.Second, MaxSendTimeout: defaultMaxSendTimeout,
			RenderTimeout: defaultRenderTimeout, Concurrency: 16, MaxAttempts: 6,
		},
	}
}

// WithOptions 覆盖参数。
func (uc *SenderUsecase) WithOptions(o SenderOptions) *SenderUsecase {
	if o.Lease > 0 {
		uc.opts.Lease = o.Lease
	}
	if o.SendTimeout > 0 {
		uc.opts.SendTimeout = o.SendTimeout
	}
	if o.MaxSendTimeout > 0 {
		uc.opts.MaxSendTimeout = o.MaxSendTimeout
	}
	if o.RenderTimeout > 0 {
		uc.opts.RenderTimeout = o.RenderTimeout
		uc.renderer = uc.renderer.WithTimeout(o.RenderTimeout)
	}
	if o.Concurrency > 0 {
		uc.opts.Concurrency = o.Concurrency
	}
	if o.MaxAttempts > 0 {
		uc.opts.MaxAttempts = o.MaxAttempts
	}
	return uc
}

// WithObserver 注入指标观测器。
func (uc *SenderUsecase) WithObserver(o SendObserver) *SenderUsecase {
	if o != nil {
		uc.observer = o
	}
	return uc
}

// WithHealth 注入共享熔断仓储（nil = 纯进程内熔断）。
func (uc *SenderUsecase) WithHealth(repo ChannelHealthRepo) *SenderUsecase {
	uc.breaker = NewSharedBreaker(repo, uc.log)
	return uc
}

// Breaker 暴露熔断器（指标/测试）。
func (uc *SenderUsecase) Breaker() *SharedBreaker { return uc.breaker }

// SendOnce 声明并并发处理一批任务；返回处理条数（0 = 空闲）。
func (uc *SenderUsecase) SendOnce(ctx context.Context, limit int) (int, error) {
	snap := uc.cfg.Current()
	if snap == nil {
		return 0, errors.New("config snapshot not loaded")
	}
	uc.breaker.Refresh(ctx)
	claimed, err := uc.repo.Claim(ctx, limit, uc.opts.Lease, uc.pausedChannels())
	if err != nil {
		return 0, err
	}
	if len(claimed) == 0 {
		return 0, nil
	}
	events, err := uc.loadEvents(ctx, claimed)
	if err != nil {
		// 瞬时：整批放回（不计尝试、短延后），不让 100 条任务各占 5 分钟租约。
		uc.releaseAll(ctx, claimed, releaseTransientDelay)
		return 0, err
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(uc.opts.Concurrency)
	for _, d := range claimed {
		g.Go(func() error {
			// 单任务处理失败只记日志（租约到期后由回收器兜底），不中止其余任务。
			if err := uc.handle(gctx, snap, d, events); err != nil && !errors.Is(err, ErrStaleClaim) {
				uc.log.ErrorContext(gctx, "dispatch handling failed", "dispatch_id", d.ID, "error", err)
			}
			return nil
		})
	}
	_ = g.Wait()
	return len(claimed), nil
}

// ReapOnce 回收过期租约。
func (uc *SenderUsecase) ReapOnce(ctx context.Context, limit int) (int, error) {
	return uc.repo.ReapExpired(ctx, limit, uc.opts.MaxAttempts, Backoff)
}

// loadEvents 一批任务的源事件一次取回（distinct event_id；create_time 上界剪枝）。
func (uc *SenderUsecase) loadEvents(ctx context.Context, claimed []*ClaimedDispatch) (map[string]*Event, error) {
	ids := make([]string, 0, len(claimed))
	seen := make(map[string]struct{}, len(claimed))
	var maxCreate time.Time
	for _, d := range claimed {
		if d.CreateTime.After(maxCreate) {
			maxCreate = d.CreateTime
		}
		if _, ok := seen[d.EventID]; ok {
			continue
		}
		seen[d.EventID] = struct{}{}
		ids = append(ids, d.EventID)
	}
	events, err := uc.repo.GetEvents(ctx, ids, maxCreate)
	if err != nil {
		return nil, fmt.Errorf("load events: %w", err)
	}
	return events, nil
}

// releaseAll 整批放回（落账 ctx 不随取消）；失败只记日志，租约回收兜底。
func (uc *SenderUsecase) releaseAll(ctx context.Context, claimed []*ClaimedDispatch, delay time.Duration) {
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), writebackTimeout)
	defer cancel()
	for _, d := range claimed {
		if err := uc.repo.Release(wctx, d, delay); err != nil && !errors.Is(err, ErrStaleClaim) {
			uc.log.WarnContext(ctx, "release claimed dispatch failed", "dispatch_id", d.ID, "error", err)
		}
	}
}

// breakerDelay 熔断打开时的放回/暂停时长：到 open_until 为止（下限 5s、上限一个
// 打开周期）。固定 5s 会让打开渠道的积压每 5s 被声明再放回一次（每行两次含
// content 的 UPDATE），10 万积压 = 每秒数万次无效写。
func (uc *SenderUsecase) breakerDelay(channelID string) time.Duration {
	delay := releaseBreakerDelay
	if until := uc.breaker.OpenUntil(channelID); !until.IsZero() {
		delay = max(releaseBreakerDelay, min(time.Until(until), breakerOpenFor))
	}
	return delay
}

func (uc *SenderUsecase) handle(ctx context.Context, snap *Snapshot, d *ClaimedDispatch, events map[string]*Event) error {
	ch := snap.Channel(d.ChannelID)
	// 落账一律在不受取消影响的 ctx 上执行（独立超时）：消息一旦发出（或已决定放回），
	// 写回失败即意味着行停在 sending 直到租约到期被回收重发——停机取消不得造成重复投递。
	writeback := func(fn func(ctx context.Context) error) error {
		wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), writebackTimeout)
		defer cancel()
		return fn(wctx)
	}
	if ch == nil {
		uc.observer.ObserveDispatch("unknown", ObserveDead)
		return writeback(func(c context.Context) error {
			return uc.repo.Finish(c, d, FinishResult{Retryable: false, Error: "channel deleted"}, uc.opts.MaxAttempts)
		})
	}
	finish := func(res FinishResult) error {
		switch {
		case res.OK:
			uc.observer.ObserveDispatch(ch.Type, ObserveSent)
		case res.Retryable && d.Attempts < uc.opts.MaxAttempts:
			uc.observer.ObserveDispatch(ch.Type, ObserveRetry)
		default:
			uc.observer.ObserveDispatch(ch.Type, ObserveDead)
		}
		return writeback(func(c context.Context) error { return uc.repo.Finish(c, d, res, uc.opts.MaxAttempts) })
	}
	release := func(delay time.Duration) error {
		uc.observer.ObserveDispatch(ch.Type, ObserveReleased)
		return writeback(func(c context.Context) error { return uc.repo.Release(c, d, delay) })
	}
	// transient 发送前瞬时错误：尽力放回（不计尝试、短延后）再上报；放回失败（含围栏
	// 拒绝）只记日志——最坏仍由租约回收兜底。
	transient := func(err error) error {
		if rerr := release(releaseTransientDelay); rerr != nil && !errors.Is(rerr, ErrStaleClaim) {
			uc.log.WarnContext(ctx, "release after transient error failed", "dispatch_id", d.ID, "error", rerr)
		}
		return err
	}
	switch {
	case !ch.Enabled:
		return release(releaseDisabledDelay)
	case ch.SecretErr != nil:
		uc.log.ErrorContext(ctx, "channel secret unavailable, dispatch deferred", "channel_id", ch.ID, "dispatch_id", d.ID)
		return release(releaseSecretDelay)
	case !uc.sender.Supports(ch.Type):
		return finish(FinishResult{Retryable: false, Error: ErrChannelTypeUnsupported.Error()})
	}
	event, ok := events[d.EventID]
	if !ok {
		return finish(FinishResult{Retryable: false, Error: ErrEventNotFound.Error()})
	}
	content, err := DecodeContent(d.Content)
	if err != nil {
		return finish(FinishResult{Retryable: false, Error: Truncate(err.Error(), 512)})
	}
	// snapshot 本次首次渲染的快照：随落账同事务写入（省一次独立往返）；渲染后又被
	// 放回（熔断/限流）则下次重渲染——模板已缓存，代价微秒级。
	var snapshot []byte
	if !content.Rendered() {
		tpl := snap.Template(event.BizID, event.EventType, ch.Type)
		subject, body, format, err := uc.renderer.Render(ctx, tpl, ch.Type, RenderContextFor(event, content.Agg))
		if err != nil {
			if errors.Is(err, ErrRenderTimeout) {
				return transient(fmt.Errorf("render: %w", err))
			}
			return finish(FinishResult{Retryable: false, Error: Truncate("render: "+err.Error(), 512)})
		}
		content.Subject, content.Body, content.Format = subject, body, format
		if tpl != nil {
			content.TemplateID, content.TemplateVersion = tpl.ID, tpl.Version
		}
		if snapshot, err = EncodeContent(content); err != nil {
			// 确定性失败：落账为死信，不让行停在 sending 等租约回收后重复同一失败。
			return finish(FinishResult{Retryable: false, Error: Truncate("encode content: "+err.Error(), 512)})
		}
	}
	if !uc.breaker.Allow(ctx, ch.ID) {
		delay := uc.breakerDelay(ch.ID)
		uc.pause(ch.ID, delay)
		return release(delay)
	}
	ok, wait, err := uc.limiter.Take(ctx, ch)
	if err != nil {
		uc.breaker.CancelProbe(ctx, ch.ID) // Allow 可能放出了半开探测名额，未真正发送须归还
		return transient(fmt.Errorf("rate limiter: %w", err))
	}
	if !ok {
		uc.breaker.CancelProbe(ctx, ch.ID)
		if wait < releaseMinDelay {
			wait = releaseMinDelay
		}
		uc.pause(ch.ID, wait)
		return release(wait)
	}
	msg := RenderedMessage{
		EventID: event.ID, BizID: event.BizID, EventType: event.EventType, Severity: event.Severity,
		Title: event.Title, Subject: content.Subject, Body: content.Body, Format: content.Format, Payload: event.Payload,
		Recipient: d.Recipient, SendKey: d.SendKey, Agg: content.Agg,
	}
	sendCtx, cancel := context.WithTimeout(ctx, uc.sendTimeout(ch))
	start := time.Now()
	res, sendErr := uc.sender.Send(sendCtx, ch, msg)
	cancel()
	elapsed := time.Since(start)
	uc.observer.ObserveSendLatency(ch.Type, elapsed)
	latency := int32(elapsed.Milliseconds()) //nolint:gosec // 毫秒级时延不会溢出 int32
	// 熔断只统计反映渠道健康的结果：成功，或可重试失败（超时 / 连接错误 / 5xx / 限流）。
	// 不可重试失败（4xx、内容非法、收件地址错误）是单条消息的问题——计入会让几条坏消息
	// 把健康渠道熔断；此时只归还 Allow 可能放出的半开探测名额。
	_ = writeback(func(c context.Context) error {
		if sendErr == nil || res.Retryable {
			uc.breaker.Record(c, ch.ID, sendErr == nil)
		} else {
			uc.breaker.CancelProbe(c, ch.ID)
		}
		return nil
	})
	fr := FinishResult{OK: sendErr == nil, ProviderMsgID: res.ProviderMsgID, LatencyMs: latency, Content: snapshot, TemplateID: content.TemplateID}
	if sendErr != nil {
		fr.Retryable = res.Retryable
		fr.Error = Truncate(sendErr.Error(), 512)
		// 退避取 max(退避表, 对端 Retry-After)，上限一个退避表最大值：对端明确要求等更久
		// 时按对端，否则固定退避把 429 直接变成下一次 429。
		fr.RetryAfter = max(Backoff(d.Attempts), min(res.RetryAfter, retryBackoff[len(retryBackoff)-1]))
	}
	return finish(fr)
}

// sendTimeout 单次发送超时：渠道显式 timeout_s（webhook）取 min(timeout_s,
// MaxSendTimeout)，未配置取 SendTimeout。此前渠道值被嵌在固定 3s 内，>3s 的配置
// 形同虚设。
func (uc *SenderUsecase) sendTimeout(ch *Channel) time.Duration {
	if ch.Webhook != nil && ch.Webhook.TimeoutS > 0 {
		return min(time.Duration(ch.Webhook.TimeoutS)*time.Second, uc.opts.MaxSendTimeout)
	}
	return uc.opts.SendTimeout
}

func (uc *SenderUsecase) pause(channelID string, d time.Duration) {
	uc.pausedMu.Lock()
	defer uc.pausedMu.Unlock()
	uc.paused[channelID] = time.Now().Add(d)
}

func (uc *SenderUsecase) pausedChannels() []string {
	uc.pausedMu.Lock()
	defer uc.pausedMu.Unlock()
	now := time.Now()
	out := make([]string, 0, len(uc.paused))
	for id, until := range uc.paused {
		if now.After(until) {
			delete(uc.paused, id)
			continue
		}
		out = append(out, id)
	}
	return out
}
