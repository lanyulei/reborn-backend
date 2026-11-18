package biz

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// ProbeInfo 一次探活读到的集群信息。
type ProbeInfo struct {
	Version   string
	NodeTotal int
	NodeReady int
}

// ClusterProbe 探活与接入自检（data/kube 实现，均以平台身份执行）。
type ClusterProbe interface {
	// Probe 读取版本与节点数。
	Probe(ctx context.Context, clusterID string) (ProbeInfo, error)
	// SelfCheck 接入自检：返回各检查项；error 仅表示自检本身无法执行。
	SelfCheck(ctx context.Context, clusterID string) ([]k8scontract.SelfCheck, error)
}

// ProbeRecord 探活结果的落库形态。
type ProbeRecord struct {
	ClusterID string
	Status    int32
	Info      ProbeInfo
	Error     string
}

// RuntimeRepo 集群运行状态（k8s_cluster_runtime 的非租约列）。
type RuntimeRepo interface {
	// SaveProbe 写入探活结果（以 owner_instance 围栏）；返回写入前的状态与当前状态的起始时间。
	SaveProbe(ctx context.Context, instance string, r ProbeRecord) (prev int32, since time.Time, err error)
	// SaveScope 回写授权同步结果。
	SaveScope(ctx context.Context, instance, clusterID string, version int64, errMsg string) error
}

// OutboxEvent 待派发的通知（k8s_notify_outbox）。
type OutboxEvent struct {
	ClusterID string
	EventType string
	Severity  int32
	DedupKey  string
	GroupKey  string
	Payload   any
}

// OutboxRepo 通知发件箱。
type OutboxRepo interface {
	// Enqueue 写入一条通知；dedup_key 已存在时为空操作。
	Enqueue(ctx context.Context, ev OutboxEvent) error
}

const (
	probeTimeout = 10 * time.Second
	// probeFailThreshold 在线集群连续失败多少次才判为不可达。
	probeFailThreshold = 3
	probeConcurrency   = 8
)

type probeState struct {
	gen      string
	failures int
	checked  bool
	status   int32
}

// Prober 集群探活：周期读取版本与节点数，维护在线状态，状态翻转时写通知发件箱。
type Prober struct {
	registry *Registry
	probe    ClusterProbe
	runtime  RuntimeRepo
	outbox   OutboxRepo
	log      *slog.Logger

	mu     sync.Mutex
	states map[string]*probeState // 键：集群 ID；不变式：只含本实例负责的集群
}

// NewProber new a Prober.
func NewProber(registry *Registry, probe ClusterProbe, runtime RuntimeRepo, outbox OutboxRepo, log *slog.Logger) *Prober {
	return &Prober{registry: registry, probe: probe, runtime: runtime, outbox: outbox, log: log, states: map[string]*probeState{}}
}

// Online 报告集群是否在线（最近一次探活成功且通过自检）。
func (p *Prober) Online(clusterID string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	st, ok := p.states[clusterID]
	return ok && st.status == k8scontract.ClusterOnline
}

// Run 对本实例负责的全部集群探活一轮（循环：每 probe_interval）。
func (p *Prober) Run(ctx context.Context) (int, error) {
	clusters := p.registry.OwnedClusters()
	p.mu.Lock()
	alive := make(map[string]struct{}, len(clusters))
	for _, c := range clusters {
		alive[c.ID] = struct{}{}
	}
	for id := range p.states {
		if _, ok := alive[id]; !ok {
			delete(p.states, id)
		}
	}
	p.mu.Unlock()

	sem := make(chan struct{}, probeConcurrency)
	var wg sync.WaitGroup
	for _, c := range clusters {
		sem <- struct{}{}
		wg.Add(1)
		go func(c *Cluster) { // 退出路径：probeOne 受 probeTimeout 约束，wg.Wait 在本函数返回前收齐
			defer wg.Done()
			defer func() { <-sem }()
			defer func() {
				if r := recover(); r != nil {
					p.log.Error("cluster probe panicked", "cluster", c.Name, "panic", r)
				}
			}()
			p.probeOne(ctx, c)
		}(c)
	}
	wg.Wait()
	return 0, ctx.Err() // 恒返回 0：kit/loop 把 >0 视为「还有积压，立即再跑一轮」
}

// RunNew 只探活还没有结果的集群（刚领取或配置刚变更）：让它们在几秒内可用，不必等一个探活周期
// （循环：每 2 秒）。
func (p *Prober) RunNew(ctx context.Context) (int, error) {
	for _, c := range p.registry.OwnedClusters() {
		p.mu.Lock()
		st, ok := p.states[c.ID]
		fresh := !ok || st.gen != c.Generation()
		p.mu.Unlock()
		if fresh {
			p.probeOne(ctx, c)
		}
	}
	return 0, ctx.Err()
}

func (p *Prober) probeOne(ctx context.Context, c *Cluster) {
	p.mu.Lock()
	st, ok := p.states[c.ID]
	if !ok || st.gen != c.Generation() {
		prev := k8scontract.ClusterUnknown
		if ok {
			prev = st.status
		}
		st = &probeState{gen: c.Generation(), status: prev}
		p.states[c.ID] = st
	}
	failures, checked, prevStatus := st.failures, st.checked, st.status
	p.mu.Unlock()

	pctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	rec := ProbeRecord{ClusterID: c.ID}
	info, err := p.probe.Probe(pctx, c.ID)
	if err == nil && !checked {
		var checks []k8scontract.SelfCheck
		if checks, err = p.probe.SelfCheck(pctx, c.ID); err == nil {
			if msg := FailedChecks(checks); msg != "" {
				err = fmt.Errorf("self-check failed: %s", msg)
			} else {
				checked = true
			}
		}
	}
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		failures++
		rec.Error = truncate(err.Error(), 512)
		// 在线的集群容忍短暂抖动；从未在线过的集群第一次失败即标为不可达，让接入问题立刻可见。
		if prevStatus == k8scontract.ClusterOnline && failures < probeFailThreshold {
			rec.Status = k8scontract.ClusterOnline
		} else {
			rec.Status = k8scontract.ClusterUnreachable
		}
	} else {
		failures = 0
		rec.Status, rec.Info = k8scontract.ClusterOnline, info
	}
	prev, since, serr := p.runtime.SaveProbe(ctx, p.registry.InstanceID(), rec)
	if serr != nil {
		p.log.Warn("save probe result failed", "cluster", c.Name, "error", serr)
		return
	}
	p.mu.Lock()
	if cur, ok := p.states[c.ID]; ok && cur.gen == st.gen {
		cur.failures, cur.checked, cur.status = failures, checked, rec.Status
	}
	p.mu.Unlock()
	if prev == rec.Status || (prev == k8scontract.ClusterUnknown && rec.Status == k8scontract.ClusterOnline) {
		return
	}
	ev := OutboxEvent{ClusterID: c.ID, Severity: 3, GroupKey: "k8s:cluster:" + c.ID}
	payload := k8scontract.ClusterNotifyPayload{
		ClusterID: c.ID, ClusterName: c.Name, DisplayName: c.DisplayName, Env: c.Env,
		Time: since.Format("2006-01-02 15:04:05 MST"),
	}
	kind := "recovered"
	if rec.Status == k8scontract.ClusterUnreachable {
		kind, ev.EventType, payload.Error = "unreachable", k8scontract.NotifyClusterUnreachable, rec.Error
	} else {
		ev.EventType, ev.Severity = k8scontract.NotifyClusterRecovered, 2
	}
	ev.Payload = payload
	ev.DedupKey = fmt.Sprintf("k8s:%s:%s:%d", c.ID, kind, since.Unix())
	if err := p.outbox.Enqueue(ctx, ev); err != nil {
		p.log.Warn("enqueue cluster notification failed", "cluster", c.Name, "error", err)
	}
	p.log.Info("cluster status changed", "cluster", c.Name, "status", rec.Status, "error", rec.Error)
}

// FailedChecks 汇总未通过的自检项；全部通过返回空串。
func FailedChecks(checks []k8scontract.SelfCheck) string {
	var failed []string
	for _, c := range checks {
		if !c.OK {
			failed = append(failed, c.Name+": "+c.Message)
		}
	}
	return strings.Join(failed, "; ")
}

func truncate(s string, n int) string { return Truncate(s, n) }

// Truncate 把文本截到至多 n 字节，可安全落库：不切断多字节字符，并去掉非法的 UTF-8 序列与 NUL
// （PostgreSQL 的 text / varchar 两者都拒收——集群返回的错误信息、日志片段里都可能出现）。
func Truncate(s string, n int) string {
	if !utf8.ValidString(s) || strings.IndexByte(s, 0) >= 0 {
		s = strings.ReplaceAll(strings.ToValidUTF8(s, "\uFFFD"), "\x00", "")
	}
	if len(s) <= n {
		return s
	}
	for n > 0 && s[n]&0xC0 == 0x80 { // 不切断多字节字符
		n--
	}
	return s[:n]
}
