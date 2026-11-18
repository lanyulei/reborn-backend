package biz

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// ErrAgentToken Agent 令牌无效：不存在、已过期、密钥不对、集群已停用——对外应答一律相同。
var ErrAgentToken = errors.New("agent token invalid")

// ErrAgentClusterDisabled 长期令牌有效，但集群已被停用（只在令牌的密钥比对通过之后返回）。
// 与 ErrAgentToken 区分开：Agent 遇到它应保留令牌、稍后重连，而不是当作被吊销。
var ErrAgentClusterDisabled = errors.New("agent cluster disabled")

// AgentGrant 一次成功的令牌校验结果。
type AgentGrant struct {
	TokenID     string
	ClusterID   string
	ClusterName string
	// Token 注册时换发的长期令牌明文（仅 Register 返回）。
	Token string
}

// AgentTokenRepo Agent 令牌（k8s_agent_token）。
type AgentTokenRepo interface {
	// Exchange 用一次性的注册令牌换取长期令牌（单事务：校验 → 删除注册令牌 → 写长期令牌 → 超过上限删最旧的）。
	// 并发提交同一个注册令牌只有一个成功；其余返回 ErrAgentToken。
	Exchange(ctx context.Context, tokenID, secret string) (AgentGrant, error)
	// Verify 校验长期令牌（常数时间比较）；无效返回 ErrAgentToken。
	Verify(ctx context.Context, tokenID, secret string) (AgentGrant, error)
	// Alive 给定的长期令牌里仍然有效的那些（令牌被吊销、集群被删除或停用即失效）。
	Alive(ctx context.Context, tokenIDs []string) (map[string]struct{}, error)
}

// OfflineAgent 被判定为离线的 Agent 集群（由在线翻转为不可达时需要通知）。
type OfflineAgent struct {
	Cluster   Cluster
	WasOnline bool
	Since     time.Time
}

// AgentRuntimeRepo Agent 集群的归属与在线状态（k8s_cluster_runtime）。
type AgentRuntimeRepo interface {
	// Attach 以「持有隧道」为由领取或续租 Agent 集群，并回写 Agent 版本与在线时间；另一个存活实例
	// 仍持有有效租约时返回 false（本实例的隧道作为备用保持连接）。
	Attach(ctx context.Context, instance, clusterID, agentVersion string, lease time.Duration) (bool, error)
	// Detach 隧道已断：放弃归属（以 owner_instance 围栏）。
	Detach(ctx context.Context, instance, clusterID string) error
	// SweepOffline 把没有任何实例持有隧道、且离线超过 grace 的 Agent 集群标为不可达，返回本次翻转的集群。
	SweepOffline(ctx context.Context, grace time.Duration) ([]OfflineAgent, error)
}

// Tunnels 本实例上的 Agent 隧道（data/tunnel 实现）。
type Tunnels interface {
	// Active 有隧道的集群（集群 ID → 承载请求的那条隧道的 Agent 版本）与变更计数。
	Active() (clusters map[string]string, generation uint64)
	// Tokens 全部隧道所用的令牌 ID。
	Tokens() []string
	// Drop 断开使用这些令牌的隧道。
	Drop(tokenIDs []string)
}

// AgentHolder Agent 集群的归属来源：持有隧道的实例负责该集群（Registry 消费）。
type AgentHolder interface {
	// Held 领取 / 续租有隧道的 Agent 集群、放弃隧道已断的，返回本实例当前负责的 Agent 集群 ID。
	Held(ctx context.Context) ([]string, error)
}

// agentOfflineGrace Agent 离线多久后把集群标为不可达：容忍短暂的重连（网络抖动、数据面滚动发布）。
const agentOfflineGrace = 20 * time.Second

// AgentKeeper Agent 集群的归属维护、令牌吊销后的断连、离线判定。
type AgentKeeper struct {
	registry *Registry
	tunnels  Tunnels
	tokens   AgentTokenRepo
	runtime  AgentRuntimeRepo
	outbox   OutboxRepo
	log      *slog.Logger

	mu       sync.Mutex
	attached map[string]struct{} // 上一轮本实例负责的 Agent 集群
	lastGen  uint64
	synced   bool
}

var _ AgentHolder = (*AgentKeeper)(nil)

// NewAgentKeeper new an AgentKeeper.
func NewAgentKeeper(registry *Registry, tunnels Tunnels, tokens AgentTokenRepo, runtime AgentRuntimeRepo, outbox OutboxRepo, log *slog.Logger) *AgentKeeper {
	return &AgentKeeper{registry: registry, tunnels: tunnels, tokens: tokens, runtime: runtime, outbox: outbox, log: log, attached: map[string]struct{}{}}
}

// Held implements AgentHolder.
func (k *AgentKeeper) Held(ctx context.Context) ([]string, error) {
	active, _ := k.tunnels.Active()
	instance, lease := k.registry.InstanceID(), k.registry.Lease()
	k.mu.Lock()
	prev := k.attached
	k.mu.Unlock()

	next := make(map[string]struct{}, len(active))
	held := make([]string, 0, len(active))
	var firstErr error
	for id, version := range active {
		ok, err := k.runtime.Attach(ctx, instance, id, version, lease)
		if err != nil {
			// 数据库抖动：之前已负责的继续算作负责（租约仍有效），不因一次失败断开全部流。
			if _, was := prev[id]; was {
				next[id] = struct{}{}
				held = append(held, id)
			}
			if firstErr == nil {
				firstErr = fmt.Errorf("attach agent cluster: %w", err)
			}
			continue
		}
		if ok {
			next[id] = struct{}{}
			held = append(held, id)
		}
	}
	for id := range prev {
		if _, ok := active[id]; ok {
			continue
		}
		if err := k.runtime.Detach(ctx, instance, id); err != nil {
			k.log.Warn("detach agent cluster failed", "cluster_id", id, "error", err)
		}
	}
	k.mu.Lock()
	k.attached = next
	k.mu.Unlock()
	return held, firstErr
}

// Follow 隧道有增减时立即对齐归属（循环：每秒）：Agent 连上后几秒内可用，断开后立即停掉缓存与流。
func (k *AgentKeeper) Follow(ctx context.Context) (int, error) {
	_, gen := k.tunnels.Active()
	k.mu.Lock()
	changed := !k.synced || gen != k.lastGen
	k.mu.Unlock()
	if !changed {
		return 0, nil
	}
	if _, err := k.registry.Sync(ctx); err != nil {
		return 0, err
	}
	k.mu.Lock()
	k.lastGen, k.synced = gen, true
	k.mu.Unlock()
	return 0, nil
}

// Sweep 断开令牌已失效的隧道，并把离线的 Agent 集群标为不可达（循环：每 5 秒，每个实例都跑；
// 标记语句自身原子，多实例并发只有一个翻转成功并发通知）。
func (k *AgentKeeper) Sweep(ctx context.Context) (int, error) {
	if ids := k.tunnels.Tokens(); len(ids) > 0 {
		alive, err := k.tokens.Alive(ctx, ids)
		if err != nil {
			return 0, fmt.Errorf("check agent tokens: %w", err)
		}
		var revoked []string
		for _, id := range ids {
			if _, ok := alive[id]; !ok {
				revoked = append(revoked, id)
			}
		}
		if len(revoked) > 0 {
			k.tunnels.Drop(revoked)
			k.log.Info("agent tunnels dropped: token revoked or cluster disabled", "count", len(revoked))
		}
	}
	offline, err := k.runtime.SweepOffline(ctx, agentOfflineGrace)
	if err != nil {
		return 0, fmt.Errorf("sweep offline agents: %w", err)
	}
	for i := range offline {
		o := &offline[i]
		k.log.Info("agent cluster marked unreachable", "cluster", o.Cluster.Name)
		if !o.WasOnline {
			continue
		}
		ev := OutboxEvent{
			ClusterID: o.Cluster.ID, EventType: k8scontract.NotifyClusterUnreachable, Severity: 3,
			GroupKey: "k8s:cluster:" + o.Cluster.ID,
			DedupKey: fmt.Sprintf("k8s:%s:unreachable:%d", o.Cluster.ID, o.Since.Unix()),
			Payload: k8scontract.ClusterNotifyPayload{
				ClusterID: o.Cluster.ID, ClusterName: o.Cluster.Name, DisplayName: o.Cluster.DisplayName, Env: o.Cluster.Env,
				Error: AgentOfflineMessage, Time: o.Since.Format("2006-01-02 15:04:05 MST"),
			},
		}
		if err := k.outbox.Enqueue(ctx, ev); err != nil {
			k.log.Warn("enqueue cluster notification failed", "cluster", o.Cluster.Name, "error", err)
		}
	}
	return 0, nil
}

// AgentOfflineMessage Agent 离线时写入 last_error 的原因。
const AgentOfflineMessage = "agent is not connected"
