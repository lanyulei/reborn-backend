package biz

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lanyulei/reborn/backend/kit/monitorpipeline"
)

// 规则间抑制与拓扑抑制的判定（P2/P3 设计 §3.5）。
//
// 一致性模型：抑制是「此刻全部活跃告警」的一个确定函数，不读告警行上的 inhibited 标记——
//
//	U = 活跃告警（含被屏蔽的、留观中的）；x 被 S 抑制 = S 里有告警经某条抑制规则命中 x，
//	    或 x 的规则开启了拓扑抑制且 x 关联资源的某个上游资源在 S 里有告警
//	S = 「未被抑制」的告警集合，满足：S 里的告警都不被 S 抑制，S 之外的告警都被 S 抑制
//
// 「被抑制的告警不能再抑制别人」正是这个定义：a 抑制 b、b 抑制 c 时 S = {a, c}。S 用交替不动点求出
// （下界从空集、上界从 U 出发，两边单调收敛）：抑制关系无环时 S 唯一，与告警的评估顺序、由哪个实例
// 评估无关。有环时（A 抑制 B、B 抑制 A）不存在满足定义的唯一解，两界之间剩下的告警就是环上的那些
// （以及只被环上告警抑制的）：把其中「抑制它的告警比它自己更晚触发」的那些（按事件 ID 比较，UUIDv7 =
// 先触发的 ID 小）「钉」为未被抑制，再继续收敛——每个环上最早触发的告警必被钉住，互不相干的环一次解开，
// 结果仍然只取决于数据。迭代遍数有上限（maxInhibitPasses，足够解开三十级左右的抑制链），用尽时两界之间
// 剩下的告警一律按未被抑制处理（宁可多通知，不漏通知）。
//
// 各实例各自按周期（inhibit_refresh_interval）读一次活跃告警并求 S：一遍 = 对每个告警逐条抑制规则判定一次
// （告警数 × 规则数，规则数有上限 MaxInhibits；实测 2100 个告警 × 20 条规则一次刷新约 3ms），通常 2–3 遍、
// 至多 maxInhibitPasses 遍。评估时每个样本只做一次按事件 ID 的查表；不在快照里的事件（快照之后才触发的、
// 超出装入上限的）对着 S 现算一次（kit 的索引查找）。因为不依赖 inhibited 标记，各任务何时评估、
// 标记何时落库都不会让结果来回翻转；抑制的生效与解除最多滞后「刷新周期 + 该规则的评估周期」。
const (
	// maxInhibitPasses 求 S 时「对全部告警判定一遍」的次数上限。无环时需要的遍数 ≈ 抑制链的长度 + 2。
	maxInhibitPasses = 32
	// defaultInhibitRefresh / defaultInhibitMaxAlerts 缺省的刷新周期与装入上限。
	defaultInhibitRefresh   = 5 * time.Second
	defaultInhibitMaxAlerts = 20000
)

// InhibitView 一份活跃告警快照上的抑制结论。创建后只读，可并发使用；nil 不抑制任何事件。
type InhibitView struct {
	// snap 以 S 建立的索引：快照之外的事件对着它现算。
	snap *monitorpipeline.InhibitSnapshot
	// known 快照里的事件 → hits 的下标；-1 = 未被抑制。
	known map[string]int32
	hits  []monitorpipeline.InhibitHit
	// Pinned 因抑制关系成环而被钉为未被抑制的告警数。
	Pinned int
}

// Size 快照里的活跃告警数。
func (v *InhibitView) Size() int {
	if v == nil {
		return 0
	}
	return len(v.known)
}

// Match 报告事件此刻是否被抑制。ref.ID 是它的活跃告警 ID（尚未建事件为空）；topology 为真时
// 另按 upstream（事件关联资源的上游资源）做拓扑抑制。规则间抑制优先于拓扑抑制。
func (v *InhibitView) Match(ref *monitorpipeline.AlertRef, upstream []string, topology bool) (monitorpipeline.InhibitHit, bool) {
	if v == nil {
		return monitorpipeline.InhibitHit{}, false
	}
	if ref.ID != "" {
		if at, ok := v.known[ref.ID]; ok {
			if at < 0 {
				return monitorpipeline.InhibitHit{}, false
			}
			return v.hits[at], true
		}
	}
	if hit, ok := v.snap.Match(ref); ok {
		return hit, true
	}
	if topology && len(upstream) > 0 {
		return v.snap.Upstream(withoutCI(upstream, ref.CIID))
	}
	return monitorpipeline.InhibitHit{}, false
}

// Known 报告快照对告警 alertID 的结论：ok 为假 = 它不在快照里。
func (v *InhibitView) Known(alertID string) (inhibited, ok bool) {
	if v == nil {
		return false, false
	}
	at, ok := v.known[alertID]
	return at >= 0, ok
}

// withoutCI 去掉上游列表里的资源自身（脏数据：资源不能是自己的上游）。没有时原样返回，不分配。
func withoutCI(upstream []string, ciID string) []string {
	if ciID == "" || !slices.Contains(upstream, ciID) {
		return upstream
	}
	return slices.DeleteFunc(slices.Clone(upstream), func(id string) bool { return id == ciID })
}

// buildInhibitView 在一份活跃告警快照上求抑制结论（见本文件开头的一致性模型）。alerts 须按事件 ID
// 升序；函数接管它。cis 用来取各告警关联资源的上游。纯函数：同样的输入恒得同样的结果。
func buildInhibitView(in *monitorpipeline.Inhibitor, alerts []ActiveAlert, cis *monitorpipeline.CIIndex) *InhibitView {
	n := len(alerts)
	view := &InhibitView{known: make(map[string]int32, n)}
	upstream := make([][]string, n)
	for i := range alerts {
		a := &alerts[i]
		view.known[a.Ref.ID] = -1
		if a.Topology && a.Ref.CIID != "" {
			if ci := cis.Get(a.Ref.CIID); ci != nil {
				upstream[i] = withoutCI(ci.UpstreamIDs, ci.ID)
			}
		}
	}
	refs := make([]monitorpipeline.AlertRef, 0, n)
	snapshot := func(member []bool) *monitorpipeline.InhibitSnapshot {
		refs = refs[:0]
		for i := range alerts {
			if member[i] {
				refs = append(refs, alerts[i].Ref)
			}
		}
		return in.Snapshot(refs)
	}
	match := func(snap *monitorpipeline.InhibitSnapshot, i int) (monitorpipeline.InhibitHit, bool) {
		if hit, ok := snap.Match(&alerts[i].Ref); ok {
			return hit, true
		}
		if len(upstream[i]) > 0 {
			return snap.Upstream(upstream[i])
		}
		return monitorpipeline.InhibitHit{}, false
	}
	pinned := make([]bool, n)
	// free 返回「不被 member 抑制」的告警集合（钉住的恒在其中）。
	free := func(member []bool) []bool {
		snap := snapshot(member)
		out := make([]bool, n)
		for i := range alerts {
			_, hit := match(snap, i)
			out[i] = pinned[i] || !hit
		}
		return out
	}
	// lo ⊆ S ⊆ hi：lo 只增，hi 只减。
	hi := make([]bool, n)
	for i := range hi {
		hi[i] = true
	}
	lo := free(hi)
	for passes := 1; ; {
		if slices.Equal(lo, hi) {
			break
		}
		if passes+2 > maxInhibitPasses {
			// 预算用尽：两界之间剩下的都按未被抑制处理。
			for i := range lo {
				if hi[i] && !lo[i] {
					pinned[i], lo[i] = true, true
					view.Pinned++
				}
			}
			break
		}
		nextHi := free(lo)
		passes++
		if slices.Equal(nextHi, lo) {
			// 上界落到了下界上：已经合拢（最常见的情形——抑制链只有一层），省掉一遍。
			break
		}
		nextLo := free(nextHi)
		passes++
		if slices.Equal(nextHi, hi) && slices.Equal(nextLo, lo) {
			// 两界都不动了却没有合拢：剩下的告警（hi 有、lo 没有）只被彼此抑制，其中必有环。只拿它们建一次
			// 索引：抑制它的告警比它晚触发的钉为未被抑制——其中 ID 最小的那个必被钉住，所以每次都有进展。
			undecided := make([]bool, n)
			for i := range lo {
				undecided[i] = hi[i] && !lo[i]
			}
			snap := snapshot(undecided)
			passes++
			for i := range alerts {
				if !undecided[i] {
					continue
				}
				if hit, ok := match(snap, i); ok && hit.AlertID > alerts[i].Ref.ID {
					pinned[i], nextLo[i] = true, true
					view.Pinned++
				}
			}
		}
		hi, lo = nextHi, nextLo
	}
	view.snap = snapshot(lo)
	for i := range alerts {
		if lo[i] {
			continue
		}
		if hit, ok := match(view.snap, i); ok {
			view.known[alerts[i].Ref.ID] = int32(len(view.hits)) // #nosec G115 -- 告警数受装入上限约束
			view.hits = append(view.hits, hit)
		}
	}
	return view
}

// InhibitRepo 抑制快照的数据来源。
type InhibitRepo interface {
	// ListActiveAlerts 活跃告警（含所属规则分组的物化路径与规则的拓扑抑制开关），至多 limit 条（超出时保留
	// 指纹排序最靠前的，是一个稳定的子集）。withLabels 为假时只返回关联了资源的告警且不带标签（没有抑制规则时
	// 只做拓扑抑制，用不到标签）。
	ListActiveAlerts(ctx context.Context, withLabels bool, limit int) ([]ActiveAlert, error)
}

// InhibitOptions 抑制快照参数。
type InhibitOptions struct {
	// Refresh 刷新周期；MaxAlerts 装入的活跃告警数上限。
	Refresh   time.Duration
	MaxAlerts int
}

// InhibitCache 抑制结论的进程内只读缓存：按周期重读活跃告警并重算。只是把「全局读一次」摊到各任务上的
// 优化，正确性不依赖各实例的缓存一致（见本文件开头的一致性模型）。读路径无锁；同一时刻只有一个调用方在刷新。
type InhibitCache struct {
	repo  InhibitRepo
	cache *ConfigCache
	obs   Observer
	log   *slog.Logger
	opts  InhibitOptions

	refreshing atomic.Bool
	lastPoll   atomic.Int64 // UnixNano；仅用于限频
	view       atomic.Pointer[InhibitView]
	loaded     chan struct{}
	loadedOnce sync.Once
	// cycles / truncated 日志限频状态。
	cycles    failureLog
	truncated failureLog
}

// NewInhibitCache new an InhibitCache.
func NewInhibitCache(repo InhibitRepo, cache *ConfigCache, obs Observer, log *slog.Logger) *InhibitCache {
	return &InhibitCache{
		repo: repo, cache: cache, obs: obs, log: log, loaded: make(chan struct{}),
		opts: InhibitOptions{Refresh: defaultInhibitRefresh, MaxAlerts: defaultInhibitMaxAlerts},
	}
}

// WithOptions 注入参数（非法值保持缺省）。
func (c *InhibitCache) WithOptions(o InhibitOptions) *InhibitCache {
	if o.Refresh > 0 {
		c.opts.Refresh = o.Refresh
	}
	if o.MaxAlerts > 0 {
		c.opts.MaxAlerts = o.MaxAlerts
	}
	return c
}

// View 当前的抑制结论；尚未加载过返回 nil。
func (c *InhibitCache) View() *InhibitView { return c.view.Load() }

// Refresh 按周期重算抑制结论（须在 ConfigCache.Refresh 成功之后调用：要用到抑制规则与资源索引）。
// 可被多条循环并发调用，同一时刻只有一个真正执行。失败时保留旧结论；从未加载成功则返回错误——
// 评估器据此不评估：没有结论就评估会把已被抑制的告警当成「抑制解除」而补发通知。
func (c *InhibitCache) Refresh(ctx context.Context) error {
	if !c.refreshing.CompareAndSwap(false, true) {
		return awaitLoaded(ctx, c.loaded)
	}
	defer c.refreshing.Store(false)
	old := c.view.Load()
	now := time.Now()
	if old != nil && now.UnixNano()-c.lastPoll.Load() < int64(c.opts.Refresh) {
		return nil
	}
	c.lastPoll.Store(now.UnixNano())
	view, err := c.load(ctx, now)
	// 周期从这一次算完起计：告警与规则都顶到上限时一次刷新要算上秒级，不让刷新首尾相接。
	c.lastPoll.Store(time.Now().UnixNano())
	if err != nil {
		if old == nil {
			return fmt.Errorf("load inhibit snapshot: %w", err)
		}
		c.log.WarnContext(ctx, "inhibit snapshot refresh failed, keeping the previous one", "error", err)
		return nil
	}
	c.view.Store(view)
	c.loadedOnce.Do(func() { close(c.loaded) })
	return nil
}

func (c *InhibitCache) load(ctx context.Context, now time.Time) (*InhibitView, error) {
	inhibitor := c.cache.Inhibitor()
	// 多取一条用来发现超限。
	alerts, err := c.repo.ListActiveAlerts(ctx, inhibitor.Len() > 0, c.opts.MaxAlerts+1)
	if err != nil {
		return nil, err
	}
	over := len(alerts) > c.opts.MaxAlerts
	if over {
		alerts = alerts[:c.opts.MaxAlerts]
		c.obs.AddTruncated(ctx, TruncatedInhibitAlerts)
	}
	if c.truncated.observe("", over, now) && over {
		c.log.WarnContext(ctx, "too many active alerts for the inhibit snapshot: alerts beyond the limit cannot act as inhibit sources",
			"limit", c.opts.MaxAlerts)
	}
	slices.SortFunc(alerts, func(a, b ActiveAlert) int { return cmp.Compare(a.Ref.ID, b.Ref.ID) })
	view := buildInhibitView(inhibitor, alerts, c.cache.CIs())
	if view.Pinned > 0 {
		c.obs.AddInhibitCycles(ctx, view.Pinned)
	}
	if c.cycles.observe("", view.Pinned > 0, now) && view.Pinned > 0 {
		c.log.WarnContext(ctx, "inhibit rules form a cycle: the oldest alert on each cycle is treated as not inhibited",
			"alerts", view.Pinned)
	}
	return view, nil
}

// awaitLoaded 已加载过立即返回；否则等正在进行的首次加载（限时），仍未就绪返回 ErrCacheNotLoaded。
func awaitLoaded(ctx context.Context, loaded <-chan struct{}) error {
	timer := time.NewTimer(firstLoadWait)
	defer timer.Stop()
	select {
	case <-loaded:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ErrCacheNotLoaded
	}
}
