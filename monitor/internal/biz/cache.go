package biz

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/kit/monitorpipeline"
	"github.com/lanyulei/reborn/backend/kit/promclient"
)

// 配置缓存的节奏。缓存只是只读优化（设计 §7）：各实例最终一致，不作为正确性依赖。
const (
	// cachePollInterval 轮询 monitor_cfg_version 的最小间隔（配置变更 ≤ 该时长生效）。
	cachePollInterval = 2 * time.Second
	// cacheForceReload 即使版本号未变也整体重读的周期：兜底版本号漏加一的写入。
	cacheForceReload = time.Minute
	// firstLoadWait 等待别的循环完成首次加载的时限。
	firstLoadWait = 3 * time.Second
	// datasourceConcurrency 单实例对同一数据源的并发查询上限（保护数据源）。
	datasourceConcurrency = 8
	// MaxMutes 单次加载的屏蔽规则数上限（进程内缓存的内存上界；已过期的一次性规则不加载）。
	// 超出时按 ID 升序（先创建的在前）保留前 MaxMutes 条，其余不生效，并记 WARN 与指标。
	MaxMutes = 10000
	// MaxInhibits / MaxEscalations 单次加载的抑制规则 / 升级策略数上限，口径同 MaxMutes：超出时按 ID 升序
	// 保留前若干条，其余不生效，并记 WARN 与指标。
	MaxInhibits    = 500
	MaxEscalations = 5000
	// defaultMaxCI 资源索引缺省最多加载的有效资源数（进程内缓存的内存上界：每个资源约 0.5–1 KB）。
	defaultMaxCI = 100000
	// ciFullReload 资源索引整表重读的周期。其间只按 sync_time 增量读取有变化的行（版本号变化时，以及每个
	// cacheForceReload 兜底一次）；整表重读兜住增量读不到的情况（行被物理删除、物化事务提交得比游标晚）。
	ciFullReload = 10 * time.Minute
	// ciSyncOverlap 增量读取时游标回退的时长：sync_time 是物化事务的开始时刻，可能早于它的提交时刻。
	ciSyncOverlap = time.Minute
)

// 加载被上限截断的缓存种类（Observer.AddTruncated 的 what）。
const (
	TruncatedInhibits      = "inhibit"
	TruncatedEscalations   = "escalation"
	TruncatedCIs           = "ci"
	TruncatedInhibitAlerts = "inhibit_alert"
)

// ErrCacheNotLoaded 配置缓存尚未成功加载过。
var ErrCacheNotLoaded = errors.New("config cache not loaded yet")

// errSlotBusy 在等待预算内没有排上数据源的并发槽：查询根本没有发出。这是引擎的自我保护，不是数据源故障。
var errSlotBusy = errors.New("datasource busy: no query slot became free within the wait budget")

// CacheOptions 配置缓存参数。
type CacheOptions struct {
	// MaxSeries 单条查询返回的序列数上限。
	MaxSeries int
	// QueryTimeoutMax 单次查询超时的上限。
	QueryTimeoutMax time.Duration
	// MaxCI 资源索引最多加载的有效资源数。
	MaxCI int
}

// DatasourceHandle 一个数据源及其查询客户端。创建后只读，可并发使用；数据源配置变更后
// 缓存换发新的 Handle，旧 Handle 上在途的查询不受影响。
type DatasourceHandle struct {
	Info *Datasource
	// client 为 nil 时 unavailable 说明原因（不含凭据）。
	client      *promclient.Client
	unavailable string
	// writeUnavailable 非空 = 写入地址不可用（配置非法）：查询不受影响，记录规则的写回以它为错误。
	writeUnavailable string
	timeout          time.Duration
	// slots 评估查询的并发槽：进程内对单个数据源的自我保护，不承担任何跨实例语义。
	slots chan struct{}
	// probeSlot 探活专用的槽位：探活不与评估查询争用 slots，否则繁忙的数据源会因为探活排不上队
	// 被误写成异常。
	probeSlot chan struct{}
}

// Timeout 该数据源的单次查询超时（已按上限夹紧）。
func (h *DatasourceHandle) Timeout() time.Duration { return h.timeout }

// Unavailable 非空表示无法构造查询客户端（如凭据解密失败），内容可直接记入判定记录。
func (h *DatasourceHandle) Unavailable() string { return h.unavailable }

// Query 执行一次 instant 查询（受并发槽限制）。等槽与查询是两段各自有界的时间：等槽至多到 slotBy
// （零值 = 只受 ctx 约束），到时还没排上返回 errSlotBusy；查询超时（Timeout）从拿到槽之后才起算，
// 所以晚拿到槽的查询不会被等槽耗掉的时间挤成一次「上游超时」。
func (h *DatasourceHandle) Query(ctx context.Context, slotBy time.Time, expr string) (*promclient.Result, error) {
	return h.query(ctx, h.slots, slotBy, expr)
}

// Write 经 Remote Write 把序列写入数据源的写入地址（记录规则）。不占查询并发槽；超时由客户端自己计。
// 失败恒为 *promclient.Error 或不含凭据的固定文案。
func (h *DatasourceHandle) Write(ctx context.Context, series []promclient.Series) error {
	switch {
	case h.client == nil:
		return errors.New(h.unavailable)
	case h.writeUnavailable != "":
		return errors.New(h.writeUnavailable)
	}
	return h.client.Write(ctx, series)
}

// Ping 探活（query=1）：走专用槽位，不排在评估查询后面。
func (h *DatasourceHandle) Ping(ctx context.Context) error {
	_, err := h.query(ctx, h.probeSlot, time.Time{}, "1")
	return err
}

func (h *DatasourceHandle) query(ctx context.Context, slots chan struct{}, slotBy time.Time, expr string) (*promclient.Result, error) {
	if h.client == nil {
		return nil, errors.New(h.unavailable)
	}
	if err := acquireSlot(ctx, slots, slotBy); err != nil {
		return nil, err
	}
	defer func() { <-slots }()
	// 单次查询的超时由客户端自己计（promclient.Config.Timeout = h.timeout），起点是这里。
	return h.client.Query(ctx, expr, time.Time{})
}

// acquireSlot 占一个并发槽：有空槽立即返回；否则等到 by（零值 = 不设）或 ctx 结束，返回 errSlotBusy。
func acquireSlot(ctx context.Context, slots chan struct{}, by time.Time) error {
	select {
	case slots <- struct{}{}:
		return nil
	default:
	}
	var expired <-chan time.Time
	if !by.IsZero() {
		timer := time.NewTimer(time.Until(by))
		defer timer.Stop()
		expired = timer.C
	}
	select {
	case slots <- struct{}{}:
		return nil
	case <-expired:
	case <-ctx.Done():
	}
	return errSlotBusy
}

type cacheSnapshot struct {
	versions    CfgVersions
	loadedAt    time.Time
	datasources map[string]*DatasourceHandle
	mutes       []*compiledMute
	// cis 资源索引（从未为 nil：表为空时是空索引）；calendar 工作日历（nil = 尚未物化或配置非法）；
	// inhibitor 全部启用的抑制规则；escalations 启用的升级策略（ID → 策略）。
	cis         *monitorpipeline.CIIndex
	calendar    *monitorcontract.Calendar
	inhibitor   *monitorpipeline.Inhibitor
	escalations map[string]*monitorpipeline.EscalationPolicy
}

// ciState 资源索引的增量加载状态。只由持有刷新标志的调用方读写。
type ciState struct {
	// rows 全部有效资源（ID → 行）；cursor 已读到的最大 sync_time；fullAt 上次整表重读的时刻。
	rows   map[string]CIRow
	cursor time.Time
	fullAt time.Time
}

// ConfigCache 数据源（含查询客户端）、屏蔽规则、抑制规则、升级策略、资源索引与工作日历的进程内只读缓存，
// 按 monitor_cfg_version 失效。读路径无锁（原子快照）；刷新由一个原子标志保证同一时刻只有一个调用方在读库。
type ConfigCache struct {
	repo   ConfigRepo
	opener SecretOpener
	obs    Observer
	log    *slog.Logger
	opts   CacheOptions

	refreshing atomic.Bool
	lastPoll   atomic.Int64 // UnixNano；仅用于限频
	snap       atomic.Pointer[cacheSnapshot]
	ci         ciState
	// loaded 首次加载成功后关闭：启动瞬间撞上别人正在做首次加载的调用方等它，而不是空转一个周期。
	loaded     chan struct{}
	loadedOnce sync.Once
}

// NewConfigCache new a ConfigCache；cleanup 释放全部查询客户端的空闲连接。
func NewConfigCache(repo ConfigRepo, opener SecretOpener, obs Observer, log *slog.Logger) (*ConfigCache, func()) {
	c := &ConfigCache{
		repo: repo, opener: opener, obs: obs, log: log, loaded: make(chan struct{}),
		opts: CacheOptions{MaxSeries: 2000, QueryTimeoutMax: promclient.MaxTimeout, MaxCI: defaultMaxCI},
	}
	return c, c.close
}

// WithOptions 注入参数（非法值保持缺省）；须在首次 Refresh 之前调用。
func (c *ConfigCache) WithOptions(o CacheOptions) *ConfigCache {
	if o.MaxSeries > 0 {
		c.opts.MaxSeries = o.MaxSeries
	}
	if o.QueryTimeoutMax > 0 {
		c.opts.QueryTimeoutMax = min(o.QueryTimeoutMax, promclient.MaxTimeout)
	}
	if o.MaxCI > 0 {
		c.opts.MaxCI = o.MaxCI
	}
	return c
}

func (c *ConfigCache) close() {
	if snap := c.snap.Swap(nil); snap != nil {
		for _, h := range snap.datasources {
			if h.client != nil {
				h.client.Close()
			}
		}
	}
}

// Refresh 按需刷新：限频轮询版本号，变化（或到了兜底周期）才重读对应的表。可被多条循环并发调用，
// 同一时刻只有一个真正执行，其余立即返回。失败时保留旧快照；从未加载成功则返回错误。
func (c *ConfigCache) Refresh(ctx context.Context) error {
	if !c.refreshing.CompareAndSwap(false, true) {
		return awaitLoaded(ctx, c.loaded)
	}
	defer c.refreshing.Store(false)
	old := c.snap.Load()
	now := time.Now()
	if old != nil && now.UnixNano()-c.lastPoll.Load() < int64(cachePollInterval) {
		return nil
	}
	// 失败也记一次轮询：数据库抖动时不让每条循环每轮都重试。
	c.lastPoll.Store(now.UnixNano())
	if err := c.reload(ctx, old, now); err != nil {
		if old == nil {
			return fmt.Errorf("load config cache: %w", err)
		}
		c.log.WarnContext(ctx, "config cache refresh failed, keeping previous snapshot", "error", err)
	}
	return nil
}

func (c *ConfigCache) reload(ctx context.Context, old *cacheSnapshot, now time.Time) error {
	versions, err := c.repo.Versions(ctx)
	if err != nil {
		return err
	}
	force := old == nil || now.Sub(old.loadedAt) >= cacheForceReload
	if !force && versions == old.versions {
		return nil
	}
	next := &cacheSnapshot{versions: versions, loadedAt: now}
	if old != nil {
		*next = *old
		next.versions = versions
		if force {
			next.loadedAt = now
		}
	}
	var replaced []*DatasourceHandle
	if force || versions.Datasource != old.versions.Datasource {
		list, err := c.repo.ListDatasources(ctx)
		if err != nil {
			return err
		}
		var prev map[string]*DatasourceHandle
		if old != nil {
			prev = old.datasources
		}
		next.datasources, replaced = c.buildDatasources(list, prev)
	}
	// 屏蔽规则带着作用分组的路径：分组变更（移动 / 删除）同样要重读。
	if force || versions.Mute != old.versions.Mute || versions.RuleGroup != old.versions.RuleGroup {
		// 多取一条用来发现超限：静默截断会让一部分屏蔽规则无声失效。
		list, err := c.repo.ListMutes(ctx, MaxMutes+1)
		if err != nil {
			return err
		}
		if len(list) > MaxMutes {
			list = list[:MaxMutes]
			c.obs.AddMutesTruncated(ctx)
			c.log.WarnContext(ctx, "too many active mute rules: only the oldest are in effect, the rest are ignored", "limit", MaxMutes)
		}
		next.mutes = compileMutes(list)
	}
	// 抑制规则同样带着作用分组的路径。
	if force || versions.Inhibit != old.versions.Inhibit || versions.RuleGroup != old.versions.RuleGroup {
		list, err := c.repo.ListInhibits(ctx, MaxInhibits+1)
		if err != nil {
			return err
		}
		if len(list) > MaxInhibits {
			list = list[:MaxInhibits]
			c.truncated(ctx, TruncatedInhibits, "too many inhibit rules: only the oldest are in effect, the rest are ignored", MaxInhibits)
		}
		next.inhibitor = monitorpipeline.CompileInhibits(list)
	}
	if force || versions.Escalation != old.versions.Escalation {
		list, err := c.repo.ListEscalations(ctx, MaxEscalations+1)
		if err != nil {
			return err
		}
		if len(list) > MaxEscalations {
			list = list[:MaxEscalations]
			c.truncated(ctx, TruncatedEscalations, "too many escalation policies: only the oldest are in effect, the rest are ignored", MaxEscalations)
		}
		next.escalations = make(map[string]*monitorpipeline.EscalationPolicy, len(list))
		for _, e := range list {
			// 不合法的策略（core 保存时已校验，这里防脏数据）按没有策略处理：宁可不升级，不乱升级。
			if monitorcontract.ValidEscalationMode(e.Policy.Mode) && monitorcontract.ValidateEscalationLevels(e.Policy.Levels) == nil {
				next.escalations[e.ID] = &e.Policy
			}
		}
	}
	if force || versions.Calendar != old.versions.Calendar {
		cal, err := c.repo.LoadCalendar(ctx)
		if err != nil {
			return err
		}
		// 零值（尚未物化）或配置非法：没有日历，按日历生效的规则恒生效（monitorcontract.ContainsWithCalendar）。
		next.calendar, _ = cal.Compile()
	}
	if full := old == nil || now.Sub(c.ci.fullAt) >= ciFullReload; full || force || versions.CIIndex != old.versions.CIIndex {
		cis, err := c.loadCIs(ctx, full, now)
		if err != nil {
			return err
		}
		if cis != nil {
			next.cis = cis
		}
	}
	c.snap.Store(next)
	c.loadedOnce.Do(func() { close(c.loaded) })
	// 换下来的客户端只释放空闲连接：仍持有旧 Handle 的在途查询照常完成。
	for _, h := range replaced {
		if h.client != nil {
			h.client.Close()
		}
	}
	return nil
}

func (c *ConfigCache) truncated(ctx context.Context, what, msg string, limit int) {
	c.obs.AddTruncated(ctx, what)
	c.log.WarnContext(ctx, msg, "limit", limit)
}

// loadCIs 读资源索引并重建。full 为真时整表重读（只取有效的行）；否则只读 sync_time 晚于游标（回退
// ciSyncOverlap）的行并入已有的集合，无效的行从集合里去掉。返回 nil = 没有任何变化，沿用原索引。
// 内存有上界：至多保留 MaxCI 个资源（按 ID 升序），超出的记 WARN 与指标。
func (c *ConfigCache) loadCIs(ctx context.Context, full bool, now time.Time) (*monitorpipeline.CIIndex, error) {
	since := time.Time{}
	if !full {
		since = c.ci.cursor.Add(-ciSyncOverlap)
	}
	rows, err := c.repo.ListCIs(ctx, since, c.opts.MaxCI+1)
	if err != nil {
		return nil, err
	}
	if !full && len(rows) > c.opts.MaxCI {
		// 变化的行比上限还多：增量没有意义，整表重读。
		return c.loadCIs(ctx, true, now)
	}
	changed := full
	if full {
		c.ci.rows, c.ci.fullAt = make(map[string]CIRow, len(rows)), now
	}
	for i := range rows {
		row := &rows[i]
		if row.SyncTime.After(c.ci.cursor) {
			c.ci.cursor = row.SyncTime
		}
		have, ok := c.ci.rows[row.CI.ID]
		switch {
		case !row.Valid:
			if ok {
				delete(c.ci.rows, row.CI.ID)
				changed = true
			}
		case !ok || !have.SyncTime.Equal(row.SyncTime):
			c.ci.rows[row.CI.ID] = *row
			changed = true
		}
	}
	if !changed {
		return nil, nil
	}
	ids := make([]string, 0, len(c.ci.rows))
	for id := range c.ci.rows {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	if len(ids) > c.opts.MaxCI {
		for _, id := range ids[c.opts.MaxCI:] {
			delete(c.ci.rows, id)
		}
		ids = ids[:c.opts.MaxCI]
		c.truncated(ctx, TruncatedCIs, "too many CMDB resources in monitor_ci_index: resources beyond the limit are not linked to alerts", c.opts.MaxCI)
	}
	cis := make([]monitorpipeline.CI, len(ids))
	for i, id := range ids {
		cis[i] = c.ci.rows[id].CI
	}
	return monitorpipeline.NewCIIndex(cis), nil
}

// buildDatasources 生成新的数据源表：更新标记未变的沿用原 Handle（复用连接），其余重建。
// 返回被换下（变更或已删除）的旧 Handle。
func (c *ConfigCache) buildDatasources(list []*Datasource, prev map[string]*DatasourceHandle) (map[string]*DatasourceHandle, []*DatasourceHandle) {
	next := make(map[string]*DatasourceHandle, len(list))
	for _, ds := range list {
		if h, ok := prev[ds.ID]; ok && h.Info.Version == ds.Version && h.Info.UpdateTime.Equal(ds.UpdateTime) {
			next[ds.ID] = h
			continue
		}
		next[ds.ID] = c.newHandle(ds)
	}
	var replaced []*DatasourceHandle
	for id, h := range prev {
		if next[id] != h {
			replaced = append(replaced, h)
		}
	}
	return next, replaced
}

func (c *ConfigCache) newHandle(ds *Datasource) *DatasourceHandle {
	h := &DatasourceHandle{
		Info:      ds,
		timeout:   min(max(ds.Timeout, time.Second), c.opts.QueryTimeoutMax),
		slots:     make(chan struct{}, datasourceConcurrency),
		probeSlot: make(chan struct{}, 1),
	}
	if !ds.Enabled {
		h.unavailable = "datasource is disabled"
		return h
	}
	secret := ""
	if ds.AuthType != monitorcontract.AuthNone && len(ds.Secret) > 0 {
		var err error
		if secret, err = c.opener.Open(ds.Secret); err != nil {
			// 解密错误不含明文；仍只记录固定文案，避免把底层细节带进判定记录。
			c.log.Warn("datasource credential cannot be decrypted", "datasource_id", ds.ID, "error", err)
			h.unavailable = "datasource credential unavailable: master key missing or mismatched"
			return h
		}
	}
	// 写入地址非法不该连累查询：按没有写入地址构造客户端，记录规则的写回以固定文案报错。
	writeURL := ds.RemoteWriteURL
	if writeURL != "" && promclient.ValidateWriteURL(writeURL) != nil {
		writeURL, h.writeUnavailable = "", "datasource remote write url is invalid"
	}
	client, err := promclient.New(promclient.Config{
		BaseURL: ds.URL, AuthType: ds.AuthType, Username: ds.Username, Secret: secret, Headers: ds.Headers,
		Timeout: h.timeout, TLSSkipVerify: ds.TLSSkipVerify, MaxSeries: c.opts.MaxSeries, WriteURL: writeURL,
		// 数据源在内网是常态，私网与回环放行；但链路本地 / 云元数据地址绝不是数据源：按解析后的
		// 实际地址在连接前拒绝（评估查询、恢复取值查询、探活走同一个客户端）。
		DialGuard: promclient.GuardLinkLocal,
	})
	if err != nil {
		// promclient.New 的错误是固定文案，不回显地址与凭据。
		h.unavailable = truncate("datasource config invalid: "+err.Error(), maxErrorBytes)
		return h
	}
	h.client = client
	return h
}

// Datasource 返回数据源 Handle；不存在（已删除或缓存未加载）返回 nil。
func (c *ConfigCache) Datasource(id string) *DatasourceHandle {
	snap := c.snap.Load()
	if snap == nil {
		return nil
	}
	return snap.datasources[id]
}

// EnabledDatasources 返回全部启用的数据源 Handle（探活用）。
func (c *ConfigCache) EnabledDatasources() []*DatasourceHandle {
	snap := c.snap.Load()
	if snap == nil {
		return nil
	}
	out := make([]*DatasourceHandle, 0, len(snap.datasources))
	for _, h := range snap.datasources {
		if h.Info.Enabled {
			out = append(out, h)
		}
	}
	return out
}

// Mutes 返回对该任务在 now 生效的屏蔽规则；没有时为 nil。
func (c *ConfigCache) Mutes(rule *Rule, datasourceID string, now time.Time) *MuteSet {
	snap := c.snap.Load()
	if snap == nil {
		return nil
	}
	return newMuteSet(snap.mutes, rule, datasourceID, now)
}

// Loaded 报告缓存是否已成功加载过。
func (c *ConfigCache) Loaded() bool { return c.snap.Load() != nil }

// CIs 返回资源索引；缓存未加载返回 nil（此时不关联资源）。
func (c *ConfigCache) CIs() *monitorpipeline.CIIndex {
	if snap := c.snap.Load(); snap != nil {
		return snap.cis
	}
	return nil
}

// Calendar 返回工作日历；尚未物化、配置非法或缓存未加载返回 nil。
func (c *ConfigCache) Calendar() *monitorcontract.Calendar {
	if snap := c.snap.Load(); snap != nil {
		return snap.calendar
	}
	return nil
}

// Inhibitor 返回全部启用的抑制规则；缓存未加载返回 nil（没有任何规则）。
func (c *ConfigCache) Inhibitor() *monitorpipeline.Inhibitor {
	if snap := c.snap.Load(); snap != nil {
		return snap.inhibitor
	}
	return nil
}

// Escalation 返回一条启用的升级策略；id 为空、策略不存在 / 已停用 / 已删除 / 不合法时返回 nil。
func (c *ConfigCache) Escalation(id string) *monitorpipeline.EscalationPolicy {
	snap := c.snap.Load()
	if snap == nil || id == "" {
		return nil
	}
	return snap.escalations[id]
}
