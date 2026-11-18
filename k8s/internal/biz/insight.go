package biz

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// ---------------------------------------------------------------------------
// 总览摘要
// ---------------------------------------------------------------------------

// SummaryBuilder 逐个对象累加出集群总览摘要：调用方按页读取对象、转成 Entry 后喂进来，不需要
// 同时持有全部对象。非并发安全。
type SummaryBuilder struct {
	s        k8scontract.ClusterSummary
	restarts []k8scontract.SummaryItem
}

// NewSummaryBuilder new a SummaryBuilder.
func NewSummaryBuilder() *SummaryBuilder {
	return &SummaryBuilder{s: k8scontract.ClusterSummary{
		Pods: map[string]int{}, AbnormalWorkloads: []k8scontract.SummaryItem{},
		RestartPods: []k8scontract.SummaryItem{}, AbnormalNodes: []k8scontract.SummaryItem{},
	}}
}

func stateText(e *Entry) string {
	if c, ok := e.Row.Cells["status"].(k8scontract.StateCell); ok {
		return c.Text
	}
	return e.Row.State
}

// AddNode 累加一个节点。
func (b *SummaryBuilder) AddNode(e *Entry) {
	b.s.NodeTotal++
	switch e.Row.State {
	case k8scontract.StateOK:
		b.s.NodeReady++
	case k8scontract.StatePending: // Ready 但已停止调度
		b.s.NodeReady++
		b.s.NodeCordoned++
	default:
		if len(b.s.AbnormalNodes) < k8scontract.SummaryTopN {
			b.s.AbnormalNodes = append(b.s.AbnormalNodes, k8scontract.SummaryItem{Kind: "Node", Name: e.Row.Name, Text: stateText(e)})
		}
	}
	b.s.CPUAllocatable += e.CPU
	b.s.MemoryAllocatable += e.Memory
	if n, ok := e.Row.Cells["pods"].(int64); ok {
		b.s.PodCapacity += n
	}
}

// AddPod 累加一个 Pod。
func (b *SummaryBuilder) AddPod(e *Entry) {
	b.s.PodTotal++
	st := e.Row.State
	if st == "" {
		st = k8scontract.StateUnknown
	}
	b.s.Pods[st]++
	b.s.CPURequested += e.CPU
	b.s.MemoryRequested += e.Memory
	if n, ok := e.Row.Cells["restarts"].(int64); ok && n >= k8scontract.SummaryRestartThreshold {
		b.restarts = append(b.restarts, k8scontract.SummaryItem{Namespace: e.Row.Namespace, Kind: "Pod", Name: e.Row.Name, Text: stateText(e), Value: n})
		if len(b.restarts) > 4*k8scontract.SummaryTopN { // 有界：只保留重启最多的
			b.trimRestarts()
		}
	}
}

func (b *SummaryBuilder) trimRestarts() {
	sort.SliceStable(b.restarts, func(i, j int) bool { return b.restarts[i].Value > b.restarts[j].Value })
	if len(b.restarts) > k8scontract.SummaryTopN {
		b.restarts = b.restarts[:k8scontract.SummaryTopN]
	}
}

// AddWorkload 累加一个工作负载（Deployment / StatefulSet / DaemonSet）。
func (b *SummaryBuilder) AddWorkload(kind string, e *Entry) {
	b.s.WorkloadTotal++
	if e.Row.State != k8scontract.StateAbnormal {
		return
	}
	b.s.WorkloadAbnormal++
	if len(b.s.AbnormalWorkloads) < k8scontract.SummaryTopN {
		text := stateText(e)
		if r, ok := e.Row.Cells["ready"].(string); ok && r != "" {
			text = r + " " + text
		}
		b.s.AbnormalWorkloads = append(b.s.AbnormalWorkloads, k8scontract.SummaryItem{Namespace: e.Row.Namespace, Kind: kind, Name: e.Row.Name, Text: text})
	}
}

// MarkPartial 标记本次统计不完整。
func (b *SummaryBuilder) MarkPartial() { b.s.Partial = true }

// Result 取出摘要。
func (b *SummaryBuilder) Result() k8scontract.ClusterSummary {
	b.trimRestarts()
	b.s.RestartPods = append(b.s.RestartPods[:0], b.restarts...)
	return b.s
}

// ClusterSummarizer 计算一个集群的总览摘要（data/kube 实现，平台身份）。
type ClusterSummarizer interface {
	// Summary 缓存已建立的资源直接读缓存（warm = true 表示 Pod 来自缓存，本次几乎没有开销）；
	// 否则向集群分页读取。
	Summary(ctx context.Context, clusterID string) (s k8scontract.ClusterSummary, warm bool, err error)
}

// SummaryRepo 摘要落库（k8s_cluster_runtime.summary）。
type SummaryRepo interface {
	SaveSummary(ctx context.Context, instance, clusterID string, s k8scontract.ClusterSummary) error
}

const (
	// summaryWarmInterval Pod 缓存已建立的集群的摘要刷新间隔（纯内存计算）。
	summaryWarmInterval = time.Minute
	// summaryColdInterval 没有 Pod 缓存的集群的摘要刷新间隔：需要向集群分页读取全部 Pod，
	// 大集群上是数十次列表请求，不宜频繁。
	summaryColdInterval = 5 * time.Minute
	summaryTimeout      = 2 * time.Minute
)

// Summarizer 周期计算本实例负责的在线集群的总览摘要。总览页只读库，不实时查集群。
type Summarizer struct {
	registry *Registry
	prober   *Prober
	src      ClusterSummarizer
	repo     SummaryRepo
	log      *slog.Logger

	mu   sync.Mutex
	next map[string]time.Time // 集群 ID → 下次计算时间
}

// NewSummarizer new a Summarizer.
func NewSummarizer(registry *Registry, prober *Prober, src ClusterSummarizer, repo SummaryRepo, log *slog.Logger) *Summarizer {
	return &Summarizer{registry: registry, prober: prober, src: src, repo: repo, log: log, next: map[string]time.Time{}}
}

// Run 一轮（循环：每 15 秒检查哪些集群到期）。逐个集群串行：摘要不是紧急任务，不与用户请求争抢限速额度。
func (s *Summarizer) Run(ctx context.Context) (int, error) {
	clusters := s.registry.OwnedClusters()
	alive := map[string]struct{}{}
	now := time.Now()
	for _, c := range clusters {
		alive[c.ID] = struct{}{}
		if !s.prober.Online(c.ID) {
			continue
		}
		s.mu.Lock()
		due := !now.Before(s.next[c.ID])
		s.mu.Unlock()
		if !due {
			continue
		}
		sctx, cancel := context.WithTimeout(ctx, summaryTimeout)
		start := time.Now()
		sum, warm, err := s.src.Summary(sctx, c.ID)
		cancel()
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		interval := summaryColdInterval
		if warm {
			interval = summaryWarmInterval
		}
		s.mu.Lock()
		s.next[c.ID] = time.Now().Add(interval)
		s.mu.Unlock()
		if err != nil {
			s.log.Warn("compute cluster summary failed", "cluster", c.Name, "error", err)
			continue
		}
		sum.DurationMS = time.Since(start).Milliseconds()
		if err := s.repo.SaveSummary(ctx, s.registry.InstanceID(), c.ID, sum); err != nil {
			s.log.Warn("save cluster summary failed", "cluster", c.Name, "error", err)
		}
	}
	s.mu.Lock()
	for id := range s.next {
		if _, ok := alive[id]; !ok {
			delete(s.next, id)
		}
	}
	s.mu.Unlock()
	return 0, nil
}

// ---------------------------------------------------------------------------
// Warning 事件
// ---------------------------------------------------------------------------

// WarningEvent 集群里的一个 Warning 事件对象。
type WarningEvent struct {
	UID       string
	Namespace string
	Kind      string
	Name      string
	Reason    string
	Message   string
	Source    string
	Count     int64
	FirstTime time.Time
	LastTime  time.Time
}

// EventSource 读取集群当前的 Warning 事件（data/kube 实现，平台身份）。
type EventSource interface {
	// Warnings 至多返回 limit 个；truncated 表示还有更多。
	Warnings(ctx context.Context, clusterID string, limit int) (events []WarningEvent, truncated bool, err error)
}

// EventRecord 落库的一行（k8s_event）：同一集群同一对象同一原因一行。
type EventRecord struct {
	ClusterID   string
	ClusterName string
	Namespace   string
	Kind        string
	Name        string
	Reason      string
	Message     string
	Source      string
	// Absolute 当前集群里该对象该原因的事件次数合计；Delta 相比上一轮新增的次数。
	Absolute  int64
	Delta     int64
	FirstTime time.Time
	LastTime  time.Time
}

// EventRepo Warning 事件落库与通知门闩。
type EventRepo interface {
	// Reasons 各集群配置的上报原因（k8s_cluster.event_reasons）；没有配置的集群不在结果中。
	Reasons(ctx context.Context) (map[string][]string, error)
	// Upsert 写入或累加一行，返回行 ID 与累计次数。first 为 true（本实例对该集群的第一轮采集）时次数取
	// max(已有, Absolute)：实例重启后重放的事件不重复累加；否则累加 Delta。
	Upsert(ctx context.Context, rec EventRecord, first bool) (id string, count int64, err error)
	// ClaimNotify 同事务：该行在 window 内没有通知过才置上通知时间并写入通知发件箱（build 按通知时间
	// 构造发件箱行）。返回是否写入。
	ClaimNotify(ctx context.Context, id string, window time.Duration, build func(at time.Time) OutboxEvent) (bool, error)
}

const (
	// eventPollLimit 单轮单集群至多处理的 Warning 事件数（事件风暴时的上限）。
	eventPollLimit = 2000
	eventTimeout   = 30 * time.Second
	maxEventMsg    = 1024
)

type seenEvent struct {
	count int64
	last  time.Time
}

// EventCollector 周期采集本实例负责的集群的 Warning 事件：去重落库，并把配置的原因转成通知。
//
// 采用轮询而不是长连接 Watch：K8s 事件缺省保留 1 小时，30 秒一轮不会漏；每轮只是一次带字段筛选的
// 列表请求，失败自然在下一轮重试，没有需要管理的常驻连接。按事件对象记住上次看到的次数，
// 只把有变化的写库。
type EventCollector struct {
	registry *Registry
	prober   *Prober
	src      EventSource
	repo     EventRepo
	purger   EventPurger
	log      *slog.Logger
	now      func() time.Time

	mu   sync.Mutex
	seen map[string]map[string]seenEvent // 集群 ID → 事件 UID → 上次看到的次数；不变式：只含本实例负责的集群
}

// NewEventCollector new an EventCollector.
func NewEventCollector(registry *Registry, prober *Prober, src EventSource, repo EventRepo, purger EventPurger, log *slog.Logger) *EventCollector {
	return &EventCollector{registry: registry, prober: prober, src: src, repo: repo, purger: purger, log: log, now: time.Now, seen: map[string]map[string]seenEvent{}}
}

// EventKey 去重键：命名空间 / Kind / 名称 / 原因。
func EventKey(e WarningEvent) string {
	return e.Namespace + "\x00" + e.Kind + "\x00" + e.Name + "\x00" + e.Reason
}

// DiffEvents 把一轮读到的事件与上一轮的记录比较，归并出需要写库的行（只含有变化的），并返回新的记录。
// prev 为 nil 表示第一轮：全部事件都算变化。纯函数。
func DiffEvents(prev map[string]seenEvent, events []WarningEvent) (changed []EventRecord, next map[string]seenEvent) {
	next = make(map[string]seenEvent, len(events))
	type agg struct {
		rec   EventRecord
		dirty bool
	}
	groups := map[string]*agg{}
	var order []string
	for _, e := range events {
		if e.Count < 1 {
			e.Count = 1
		}
		next[e.UID] = seenEvent{count: e.Count, last: e.LastTime}
		key := EventKey(e)
		g := groups[key]
		if g == nil {
			g = &agg{rec: EventRecord{Namespace: e.Namespace, Kind: e.Kind, Name: e.Name, Reason: e.Reason, FirstTime: e.FirstTime, LastTime: e.LastTime, Message: e.Message, Source: e.Source}}
			groups[key] = g
			order = append(order, key)
		}
		g.rec.Absolute += e.Count
		if e.FirstTime.Before(g.rec.FirstTime) {
			g.rec.FirstTime = e.FirstTime
		}
		if !e.LastTime.Before(g.rec.LastTime) {
			g.rec.LastTime, g.rec.Message, g.rec.Source = e.LastTime, e.Message, e.Source
		}
		old, ok := prev[e.UID]
		switch {
		case prev == nil || !ok:
			g.rec.Delta += e.Count
			g.dirty = true
		case e.Count > old.count:
			g.rec.Delta += e.Count - old.count
			g.dirty = true
		case e.LastTime.After(old.last):
			g.dirty = true
		}
	}
	for _, key := range order {
		if g := groups[key]; g.dirty {
			if len(g.rec.Message) > maxEventMsg {
				g.rec.Message = truncate(g.rec.Message, maxEventMsg)
			}
			changed = append(changed, g.rec)
		}
	}
	return changed, next
}

// ShouldNotify 该原因是否在上报清单里（不区分大小写；清单为空用缺省清单），且事件足够新。
func ShouldNotify(reasons []string, reason string, last, now time.Time) bool {
	if len(reasons) == 0 {
		reasons = k8scontract.DefaultEventReasons()
	}
	if now.Sub(last) > k8scontract.EventNotifyFreshness {
		return false
	}
	return slices.ContainsFunc(reasons, func(r string) bool { return strings.EqualFold(r, reason) })
}

func eventSeverity(reason string) int32 {
	switch strings.ToLower(reason) {
	case "nodenotready", "oomkilling", "oomkilled":
		return 3
	}
	return 2
}

// Run 一轮采集（循环：每 30 秒）。
func (c *EventCollector) Run(ctx context.Context) (int, error) {
	clusters := c.registry.OwnedClusters()
	alive := map[string]struct{}{}
	for _, cl := range clusters {
		alive[cl.ID] = struct{}{}
	}
	c.mu.Lock()
	for id := range c.seen {
		if _, ok := alive[id]; !ok {
			delete(c.seen, id)
		}
	}
	c.mu.Unlock()
	reasons, err := c.repo.Reasons(ctx)
	if err != nil {
		return 0, err
	}
	for _, cl := range clusters {
		if !c.prober.Online(cl.ID) {
			continue
		}
		c.collect(ctx, cl, reasons[cl.ID])
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
	}
	return 0, nil
}

func (c *EventCollector) collect(ctx context.Context, cl *Cluster, reasons []string) {
	cctx, cancel := context.WithTimeout(ctx, eventTimeout)
	defer cancel()
	events, truncated, err := c.src.Warnings(cctx, cl.ID, eventPollLimit)
	if err != nil {
		c.log.Warn("collect warning events failed", "cluster", cl.Name, "error", err)
		return
	}
	if truncated {
		c.log.Warn("too many warning events, only part of them is collected this round", "cluster", cl.Name, "limit", eventPollLimit)
	}
	c.mu.Lock()
	prev := c.seen[cl.ID]
	c.mu.Unlock()
	changed, next := DiffEvents(prev, events)
	if truncated && prev != nil && len(prev) < 4*eventPollLimit {
		// 这一轮只读到了一部分：没读到的事件保留上一轮的记录，否则它们下次出现会被当成新事件、次数重复累加。
		for uid, old := range prev {
			if _, ok := next[uid]; !ok {
				next[uid] = old
			}
		}
	}
	now := c.now()
	failed := false
	saved := map[string]struct{}{}
	for _, rec := range changed {
		rec.ClusterID, rec.ClusterName = cl.ID, cl.Name
		id, count, err := c.repo.Upsert(cctx, rec, prev == nil)
		if err != nil {
			c.log.Warn("save warning event failed", "cluster", cl.Name, "error", err)
			failed = true
			break
		}
		saved[EventKey(WarningEvent{Namespace: rec.Namespace, Kind: rec.Kind, Name: rec.Name, Reason: rec.Reason})] = struct{}{}
		if !ShouldNotify(reasons, rec.Reason, rec.LastTime, now) {
			continue
		}
		object := rec.Kind + "/" + rec.Name
		if rec.Namespace != "" {
			object = rec.Namespace + "/" + object
		}
		_, err = c.repo.ClaimNotify(cctx, id, k8scontract.EventNotifyWindow, func(at time.Time) OutboxEvent {
			return OutboxEvent{
				ClusterID: cl.ID, EventType: k8scontract.EventNotifyType(rec.Reason), Severity: eventSeverity(rec.Reason),
				DedupKey: fmt.Sprintf("k8s:event:%s:%d", id, at.Unix()),
				GroupKey: "k8s:event:" + cl.ID + ":" + object + ":" + rec.Reason,
				Payload: k8scontract.EventNotifyPayload{
					Title:     fmt.Sprintf("集群事件 %s：%s %s", cl.Name, rec.Reason, object),
					ClusterID: cl.ID, ClusterName: cl.Name, Namespace: rec.Namespace, Kind: rec.Kind, Name: rec.Name,
					Reason: rec.Reason, Message: rec.Message, Count: count, Time: rec.LastTime.Local().Format("2006-01-02 15:04:05 MST"),
				},
			}
		})
		if err != nil {
			c.log.Warn("enqueue event notification failed", "cluster", cl.Name, "error", err)
		}
	}
	if failed {
		if prev == nil {
			return // 第一轮按绝对值写库，整轮重做是幂等的
		}
		// 只前移已写库的那些：其余的下一轮重新算作变化；已写库的若不前移，下一轮会把增量再加一遍。
		partial := make(map[string]seenEvent, len(prev))
		for uid, old := range prev {
			partial[uid] = old
		}
		for _, e := range events {
			if _, ok := saved[EventKey(e)]; ok {
				partial[e.UID] = next[e.UID]
			}
		}
		next = partial
	}
	c.mu.Lock()
	c.seen[cl.ID] = next
	c.mu.Unlock()
}

// EventPurger 清理过期的 Warning 事件（维护任务）。
type EventPurger interface {
	PurgeEvents(ctx context.Context, retention time.Duration) (int, error)
}

// Purge 清理超过保留期的 Warning 事件（循环：每小时，选主）。
func (c *EventCollector) Purge(ctx context.Context) (int, error) {
	_, err := c.purger.PurgeEvents(ctx, k8scontract.EventRetention)
	return 0, err
}
