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

// 发送侧延后口径（不计尝试）：无令牌按桶缺口、熔断打开、渠道停用、secret 不可用。
const (
	releaseMinDelay      = time.Second
	releaseBreakerDelay  = 5 * time.Second
	releaseDisabledDelay = 60 * time.Second
	releaseSecretDelay   = 60 * time.Second
)

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
	// NextRetry 失败且可重试时的下次时间；Dead 由仓储按 attempts ≥ MaxAttempts 或 !Retryable 判定。
	NextRetry time.Time
}

// DispatchRepo 发送任务仓储（sender 侧）。所有写语句以 (id, create_time, status=2,
// attempts=声明值) 围栏：迟到的旧持有者被拒绝（ErrStaleClaim），评审 #4。
type DispatchRepo interface {
	// Claim 声明一批到期任务：status→2、attempts+1、租约 next_retry_time=now()+lease；
	// paused 渠道排除。
	Claim(ctx context.Context, limit int, lease time.Duration, paused []string) ([]*ClaimedDispatch, error)
	// SaveContent 写渲染快照并回填 template_id 列（空=NULL；围栏）。
	SaveContent(ctx context.Context, d *ClaimedDispatch, content []byte, templateID string) error
	// Finish 落账：delivery_log + 终态/退避（围栏）；dead 同事务写 notify_dlq。
	Finish(ctx context.Context, d *ClaimedDispatch, res FinishResult, maxAttempts int32) error
	// Release 不计尝试放回：status→1、attempts-1、next_retry_time=now()+delay（围栏）。
	Release(ctx context.Context, d *ClaimedDispatch, delay time.Duration) error
	// ReapExpired 回收租约过期的 sending 行：记失败尝试并按退避转 failed / dead。
	ReapExpired(ctx context.Context, limit int, maxAttempts int32, backoff func(int32) time.Duration) (int, error)
	// GetEvent 取源事件（渲染上下文）。
	GetEvent(ctx context.Context, id string) (*Event, error)
}

// ErrStaleClaim 围栏拒绝：任务已被回收/他人重领，本次结果丢弃（at-least-once 既定）。
var ErrStaleClaim = errors.New("dispatch claim is stale")

// ErrEventNotFound 源事件不存在（已按保留期清理）→ 不可重试。
var ErrEventNotFound = errors.New("source event not found")

// RateLimiter 令牌桶（PG 条件 UPDATE 共享配额）：ok=false 时 wait 为建议等待。
type RateLimiter interface {
	Take(ctx context.Context, ch *Channel) (ok bool, wait time.Duration, err error)
}

// SendObserver 发送结果观测钩子（指标）：result ∈ sent / retry / dead / released。
type SendObserver interface {
	ObserveDispatch(channelType, result string)
}

// 观测结果取值。
const (
	ObserveSent     = "sent"
	ObserveRetry    = "retry"
	ObserveDead     = "dead"
	ObserveReleased = "released"
)

type noopObserver struct{}

func (noopObserver) ObserveDispatch(string, string) {}

// SenderOptions sender 用例参数。
type SenderOptions struct {
	Lease         time.Duration
	SendTimeout   time.Duration
	RenderTimeout time.Duration
	Concurrency   int
	MaxAttempts   int32
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
		opts: SenderOptions{Lease: 5 * time.Minute, SendTimeout: 3 * time.Second, RenderTimeout: defaultRenderTimeout, Concurrency: 16, MaxAttempts: 6},
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
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(uc.opts.Concurrency)
	for _, d := range claimed {
		g.Go(func() error {
			// 单任务处理失败只记日志（租约到期后由回收器兜底），不中止其余任务。
			if err := uc.handle(gctx, snap, d); err != nil && !errors.Is(err, ErrStaleClaim) {
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

func (uc *SenderUsecase) handle(ctx context.Context, snap *Snapshot, d *ClaimedDispatch) error {
	ch := snap.Channel(d.ChannelID)
	if ch == nil {
		uc.observer.ObserveDispatch("unknown", ObserveDead)
		return uc.repo.Finish(ctx, d, FinishResult{Retryable: false, Error: "channel deleted"}, uc.opts.MaxAttempts)
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
		return uc.repo.Finish(ctx, d, res, uc.opts.MaxAttempts)
	}
	release := func(delay time.Duration) error {
		uc.observer.ObserveDispatch(ch.Type, ObserveReleased)
		return uc.repo.Release(ctx, d, delay)
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
	event, err := uc.repo.GetEvent(ctx, d.EventID)
	if err != nil {
		if errors.Is(err, ErrEventNotFound) {
			return finish(FinishResult{Retryable: false, Error: err.Error()})
		}
		return fmt.Errorf("load event: %w", err) // 瞬时：租约到期回收重试
	}
	content, err := DecodeContent(d.Content)
	if err != nil {
		return finish(FinishResult{Retryable: false, Error: Truncate(err.Error(), 512)})
	}
	if !content.Rendered() {
		tpl := snap.Template(event.BizID, event.EventType, ch.Type)
		subject, body, err := uc.renderer.Render(ctx, tpl, RenderContextFor(event, content.Agg))
		if err != nil {
			if errors.Is(err, ErrRenderTimeout) {
				return fmt.Errorf("render: %w", err) // 瞬时
			}
			return finish(FinishResult{Retryable: false, Error: Truncate("render: "+err.Error(), 512)})
		}
		content.Subject, content.Body = subject, body
		if tpl != nil {
			content.TemplateID, content.TemplateVersion = tpl.ID, tpl.Version
		}
		raw, err := EncodeContent(content)
		if err != nil {
			return err
		}
		if err := uc.repo.SaveContent(ctx, d, raw, content.TemplateID); err != nil {
			return err
		}
	}
	if !uc.breaker.Allow(ctx, ch.ID) {
		uc.pause(ch.ID, releaseBreakerDelay)
		return release(releaseBreakerDelay)
	}
	ok, wait, err := uc.limiter.Take(ctx, ch)
	if err != nil {
		return fmt.Errorf("rate limiter: %w", err)
	}
	if !ok {
		if wait < releaseMinDelay {
			wait = releaseMinDelay
		}
		uc.pause(ch.ID, wait)
		return release(wait)
	}
	msg := RenderedMessage{
		EventID: event.ID, BizID: event.BizID, EventType: event.EventType, Severity: event.Severity,
		Title: event.Title, Subject: content.Subject, Body: content.Body, Payload: event.Payload,
		Recipient: d.Recipient, SendKey: d.SendKey, Agg: content.Agg,
	}
	sendCtx, cancel := context.WithTimeout(ctx, uc.opts.SendTimeout)
	start := time.Now()
	res, sendErr := uc.sender.Send(sendCtx, ch, msg)
	cancel()
	latency := int32(time.Since(start).Milliseconds()) //nolint:gosec // 毫秒级时延不会溢出 int32
	uc.breaker.Record(ctx, ch.ID, sendErr == nil)
	fr := FinishResult{OK: sendErr == nil, ProviderMsgID: res.ProviderMsgID, LatencyMs: latency}
	if sendErr != nil {
		fr.Retryable = res.Retryable
		fr.Error = Truncate(sendErr.Error(), 512)
		fr.NextRetry = time.Now().Add(Backoff(d.Attempts))
	}
	return finish(fr)
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
