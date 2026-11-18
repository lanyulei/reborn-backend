package biz

import (
	"context"
	"sync"
	"time"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// Executor 在集群上执行一个内部接口操作（data/kube 实现）。读操作与写操作都经它；
// 写操作连同失败一起返回审计事件，由 Ops 落库。
type Executor interface {
	Execute(ctx context.Context, c *Cluster, id Identity, op string, body []byte) (resp any, audits []*AuditEvent, err error)
}

// ClusterTester 测试未保存的直连配置（data/kube 实现）。
type ClusterTester interface {
	Test(ctx context.Context, req k8scontract.ClusterTestRequest) k8scontract.ClusterTestResponse
}

// Emit 向流写出一个 SSE 事件；返回错误表示对端已断开，流应结束。
type Emit func(event, id string, data any) error

// Streamer 在集群上执行一个流式操作（data/kube 实现），阻塞到流结束。
type Streamer interface {
	Stream(ctx context.Context, c *Cluster, id Identity, op string, body []byte, emit Emit) ([]*AuditEvent, error)
}

// Ops 内部接口的统一入口：归属判定 → 在线判定 → 执行 → 审计。
type Ops struct {
	registry *Registry
	prober   *Prober
	scopes   *ScopeSyncer
	exec     Executor
	stream   Streamer
	tester   ClusterTester
	auditor  *Auditor
	obs      Observer

	mu   sync.Mutex
	logs map[string]int // 每用户并发日志流数
}

// NewOps new an Ops.
func NewOps(registry *Registry, prober *Prober, scopes *ScopeSyncer, exec Executor, stream Streamer, tester ClusterTester, auditor *Auditor, obs Observer) *Ops {
	if s, ok := exec.(AuditSinkSetter); ok {
		s.SetAuditSink(auditor.Record)
	}
	return &Ops{registry: registry, prober: prober, scopes: scopes, exec: exec, stream: stream, tester: tester, auditor: auditor, obs: obs, logs: map[string]int{}}
}

// cluster 取本实例负责且在线的集群。
func (o *Ops) cluster(clusterID string) (*Cluster, error) {
	if clusterID == "" {
		return nil, errInvalid("missing cluster id")
	}
	c, err := o.registry.Owned(clusterID)
	if err != nil {
		return nil, err
	}
	if !o.prober.Online(c.ID) {
		return nil, ErrClusterOffline("cluster %s is unreachable or has not passed the access self-check", c.Name)
	}
	if !o.scopes.Ready(c) {
		return nil, ErrClusterOffline("cluster %s: authorisation sync pending (initialising, or the last sync failed — see the cluster's scope error), retry in a few seconds", c.Name)
	}
	return c, nil
}

// TestCluster 测试直连配置；ClusterID 非空且未带凭据时沿用已保存的凭据（仅限本实例可见的集群行）。
func (o *Ops) TestCluster(ctx context.Context, req k8scontract.ClusterTestRequest) k8scontract.ClusterTestResponse {
	return o.tester.Test(ctx, req)
}

// Call 执行一个普通操作。
func (o *Ops) Call(ctx context.Context, clusterID string, id Identity, op string, body []byte) (any, error) {
	c, err := o.cluster(clusterID)
	if err != nil {
		return nil, err
	}
	resp, audits, err := o.exec.Execute(ctx, c, id, op, body)
	o.auditor.Record(ctx, c, id, audits)
	return resp, err
}

// MaxStreamDuration 单条流的最长时长：到点下发 end（max_duration），客户端自动续接。
const MaxStreamDuration = time.Hour

// Stream 执行一个流式操作。流开始前的失败以 error 返回（service 层按普通错误响应）；
// 到达时长上限时返回 nil 并已下发 end 事件。
func (o *Ops) Stream(ctx context.Context, clusterID string, id Identity, op string, body []byte, emit Emit) error {
	c, err := o.cluster(clusterID)
	if err != nil {
		return err
	}
	kind := streamKind(op)
	if kind == "log" {
		if !o.acquireLog(id.UserID) {
			return errTooManyStreams
		}
		defer o.releaseLog(id.UserID)
	}
	o.obs.StreamDelta(kind, 1)
	defer o.obs.StreamDelta(kind, -1)

	sctx, cancel := context.WithTimeout(ctx, MaxStreamDuration)
	defer cancel()
	audits, err := o.stream.Stream(sctx, c, id, op, body, emit)
	o.auditor.Record(ctx, c, id, audits)
	if err == nil && sctx.Err() != nil && ctx.Err() == nil {
		_ = emit(k8scontract.EventEnd, "", k8scontract.StreamEnd{Reason: k8scontract.EndMaxDuration})
	}
	return err
}

func streamKind(op string) string {
	switch op {
	case k8scontract.StreamPodLog, k8scontract.StreamWorkloadLog:
		return "log"
	case k8scontract.StreamDrain:
		return "drain"
	}
	return "watch"
}

func (o *Ops) acquireLog(userID string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.logs[userID] >= k8scontract.MaxLogStreams {
		return false
	}
	o.logs[userID]++
	return true
}

func (o *Ops) releaseLog(userID string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.logs[userID]--; o.logs[userID] <= 0 {
		delete(o.logs, userID)
	}
}
