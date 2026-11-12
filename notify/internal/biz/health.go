package biz

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// ChannelHealthRepo 渠道共享熔断状态仓储（notify_channel_health，多实例同一视图）。
type ChannelHealthRepo interface {
	// Record 记录一次发送结果并重算状态（单语句 upsert：滑窗重置/累加、≥10 样本
	// 失败率 >50% 打开 60s、探测成功关闭/失败重开）；返回打开截止（零值 = 关闭）。
	Record(ctx context.Context, channelID string, ok bool) (openUntil time.Time, err error)
	// ClaimProbe 打开到期后领取半开探测名额：claimed=true 本副本发 1 条探测；
	// closed=true 表示已被他人关闭（直接放行）；两者皆 false = 他人正在探测。
	ClaimProbe(ctx context.Context, channelID string) (claimed, closed bool, err error)
	// CancelProbe 归还本副本领取但未真正发送的探测名额（probing → false），让
	// 其他副本或下一轮立即可再领取，而非等 30s 失联接管。
	CancelProbe(ctx context.Context, channelID string) error
	// ListOpen 列出打开中（含刚到期待探测）的渠道及其截止时间。
	ListOpen(ctx context.Context) (map[string]time.Time, error)
}

// healthRefreshEvery 共享视图刷新周期（其他副本打开的熔断在此时限内传播）。
const healthRefreshEvery = 2 * time.Second

// SharedBreaker 跨实例共享熔断：状态落 PG（ChannelHealthRepo），本地只持只读视图
// （Record 返回值 + 周期 ListOpen 刷新）。存储故障时退化为进程内 Breaker 判定
// （健康表不可用不得阻塞发送），恢复后自然收敛。
type SharedBreaker struct {
	repo  ChannelHealthRepo
	local *Breaker
	log   *slog.Logger
	now   func() time.Time

	mu        sync.Mutex
	open      map[string]time.Time // channel → open_until（含刚到期待探测的）
	refreshed time.Time
	degraded  bool // 最近一次存储操作失败：判定退化到本地熔断器，成功后恢复
}

// NewSharedBreaker new a SharedBreaker（repo 为 nil 时等价于纯进程内 Breaker）。
func NewSharedBreaker(repo ChannelHealthRepo, logger *slog.Logger) *SharedBreaker {
	return &SharedBreaker{repo: repo, local: NewBreaker(), log: logger, now: time.Now, open: map[string]time.Time{}}
}

// Local 暴露进程内熔断器（测试/退化路径）。
func (b *SharedBreaker) Local() *Breaker { return b.local }

// Refresh 到期即从存储刷新打开视图（调用方每轮 SendOnce 调用一次，内部按周期节流）。
func (b *SharedBreaker) Refresh(ctx context.Context) {
	if b.repo == nil {
		return
	}
	b.mu.Lock()
	due := b.now().Sub(b.refreshed) >= healthRefreshEvery
	if due {
		b.refreshed = b.now()
	}
	b.mu.Unlock()
	if !due {
		return
	}
	open, err := b.repo.ListOpen(ctx)
	b.mu.Lock()
	defer b.mu.Unlock()
	if err != nil {
		b.degraded = true
		b.log.WarnContext(ctx, "channel health refresh failed, keeping local view", "error", err)
		return
	}
	b.degraded = false
	b.open = open
}

// Allow 报告渠道当前是否允许发送：视图中未打开 → 允许；打开未到期 → 拒绝；
// 到期 → 向存储领取探测名额（集群内仅一副本放 1 条）。
func (b *SharedBreaker) Allow(ctx context.Context, channelID string) bool {
	if b.repo == nil {
		return b.local.Allow(channelID)
	}
	b.mu.Lock()
	until, isOpen := b.open[channelID]
	degraded := b.degraded
	b.mu.Unlock()
	if degraded {
		return b.local.Allow(channelID)
	}
	if !isOpen {
		return true
	}
	if b.now().Before(until) {
		return false
	}
	claimed, closed, err := b.repo.ClaimProbe(ctx, channelID)
	if err != nil {
		b.mu.Lock()
		b.degraded = true
		b.mu.Unlock()
		b.log.WarnContext(ctx, "claim breaker probe failed, falling back to local breaker", "channel_id", channelID, "error", err)
		return b.local.Allow(channelID)
	}
	if closed {
		b.mu.Lock()
		delete(b.open, channelID)
		b.mu.Unlock()
		return true
	}
	return claimed
}

// Record 记录发送结果到共享状态并更新本地视图。
func (b *SharedBreaker) Record(ctx context.Context, channelID string, ok bool) {
	if b.repo == nil {
		b.local.Record(channelID, ok)
		return
	}
	b.local.Record(channelID, ok) // 退化路径的本地视图保持同步
	until, err := b.repo.Record(ctx, channelID, ok)
	b.mu.Lock()
	defer b.mu.Unlock()
	if err != nil {
		b.degraded = true
		b.log.WarnContext(ctx, "record channel health failed, breaker degraded to local view", "channel_id", channelID, "error", err)
		return
	}
	b.degraded = false
	if until.IsZero() {
		delete(b.open, channelID)
	} else {
		b.open[channelID] = until
	}
}

// CancelProbe 归还半开探测名额（本地 + 共享；共享侧失败只记日志，30s 失联接管兜底）。
func (b *SharedBreaker) CancelProbe(ctx context.Context, channelID string) {
	b.local.CancelProbe(channelID)
	if b.repo == nil {
		return
	}
	b.mu.Lock()
	degraded := b.degraded
	b.mu.Unlock()
	if degraded {
		return
	}
	if err := b.repo.CancelProbe(ctx, channelID); err != nil {
		b.log.WarnContext(ctx, "cancel breaker probe failed", "channel_id", channelID, "error", err)
	}
}

// BreakerView 熔断只读视图（指标）。
type BreakerView interface {
	OpenChannels() map[string]time.Time
}

// OpenChannels 当前打开中的渠道（本地视图；已到期待探测的不计）。
func (b *SharedBreaker) OpenChannels() map[string]time.Time {
	if b.repo == nil {
		return b.local.OpenChannels()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	out := make(map[string]time.Time, len(b.open))
	for id, until := range b.open {
		if now.Before(until) {
			out[id] = until
		}
	}
	return out
}

// OpenUntil 返回打开截止（本地视图；零值 = 未打开或已到期）。
func (b *SharedBreaker) OpenUntil(channelID string) time.Time {
	if b.repo == nil {
		return b.local.OpenUntil(channelID)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	until, ok := b.open[channelID]
	if !ok || !b.now().Before(until) {
		return time.Time{}
	}
	return until
}

// Open 报告渠道是否处于打开状态（本地视图）。
func (b *SharedBreaker) Open(channelID string) bool {
	if b.repo == nil {
		return b.local.Open(channelID)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	until, ok := b.open[channelID]
	return ok && b.now().Before(until)
}
