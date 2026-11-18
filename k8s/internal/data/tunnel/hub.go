// Package tunnel 数据面一侧的 Agent 隧道：每条隧道是一条 WebSocket 连接上的 yamux 会话，
// **数据面是 yamux 客户端——只有它能开流**；Agent 是 yamux 服务端，把每条流当作一条到其 API Server 的
// 明文 HTTP/1.1 连接来服务。Agent 主动开流视为协议违规，整条隧道立即关闭。
//
// 同一集群可以有多条隧道（转发节点主备）：最早连上的那条承载请求，其余保持连接作为备用，
// 承载的那条断开后自动接替。
package tunnel

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/k8s/internal/data/kube"
	"github.com/lanyulei/reborn/backend/k8s/internal/tunnelproto"

	"github.com/hashicorp/yamux"
)

type session struct {
	clusterID string
	tokenID   string
	version   string
	mux       *yamux.Session
	since     time.Time
}

// Hub 本实例上的全部隧道。
type Hub struct {
	log *slog.Logger
	gen atomic.Uint64

	mu       sync.RWMutex
	clusters map[string][]*session // 不变式：切片按连接先后排序，[0] 承载请求；不含已关闭的会话
	closed   bool
}

var (
	_ kube.TunnelDialer = (*Hub)(nil)
	_ biz.Tunnels       = (*Hub)(nil)
)

// NewHub new a Hub.
func NewHub(log *slog.Logger) *Hub {
	return &Hub{log: log, clusters: map[string][]*session{}}
}

// ErrHubClosed 数据面正在退出。
var ErrHubClosed = errors.New("tunnel hub is closed")

// Serve 在一条已通过认证的连接上建立隧道并阻塞到它结束。同一个令牌的旧隧道被新连接顶替
// （Agent 重连时旧连接可能仍半开）。conn 由本函数负责关闭。
func (h *Hub) Serve(ctx context.Context, clusterID, tokenID, version string, conn net.Conn) error {
	mux, err := yamux.Client(conn, tunnelproto.SessionConfig())
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("start tunnel session: %w", err)
	}
	s := &session{clusterID: clusterID, tokenID: tokenID, version: version, mux: mux, since: time.Now()}

	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		_ = mux.Close()
		return ErrHubClosed
	}
	var stale []*session
	kept := h.clusters[clusterID][:0:0]
	for _, old := range h.clusters[clusterID] {
		if old.tokenID == tokenID {
			stale = append(stale, old)
			continue
		}
		kept = append(kept, old)
	}
	h.clusters[clusterID] = append(kept, s)
	standby := len(kept) > 0
	h.mu.Unlock()
	for _, old := range stale {
		_ = old.mux.Close()
	}
	h.gen.Add(1)
	h.log.Info("agent tunnel connected", "cluster_id", clusterID, "agent_version", version, "standby", standby)

	// Agent 不得开流：一旦收到入站流即关闭隧道。Accept 在会话关闭时返回错误，协程随之退出。
	violated := make(chan struct{})
	go func() {
		defer close(violated)
		if st, aerr := mux.Accept(); aerr == nil {
			_ = st.Close()
			h.log.Warn("agent opened a stream toward the data plane: tunnel closed", "cluster_id", clusterID)
			_ = mux.Close()
		}
	}()
	select {
	case <-mux.CloseChan():
	case <-ctx.Done():
	}
	_ = mux.Close()
	<-violated
	h.remove(s)
	h.log.Info("agent tunnel closed", "cluster_id", clusterID, "duration", time.Since(s.since).Round(time.Second).String())
	return nil
}

func (h *Hub) remove(s *session) {
	h.mu.Lock()
	list := h.clusters[s.clusterID]
	kept := list[:0:0]
	for _, o := range list {
		if o != s {
			kept = append(kept, o)
		}
	}
	if len(kept) == 0 {
		delete(h.clusters, s.clusterID)
	} else {
		h.clusters[s.clusterID] = kept
	}
	h.mu.Unlock()
	h.gen.Add(1)
}

// DialCluster implements kube.TunnelDialer：在承载请求的隧道上开一条流；它刚断开时顺延到备用隧道。
func (h *Hub) DialCluster(ctx context.Context, clusterID string) (net.Conn, error) {
	h.mu.RLock()
	list := append([]*session(nil), h.clusters[clusterID]...)
	h.mu.RUnlock()
	lastErr := error(kube.ErrAgentOffline)
	for _, s := range list {
		if s.mux.IsClosed() {
			continue
		}
		st, err := s.mux.OpenStream()
		if err == nil {
			return st, nil
		}
		lastErr = fmt.Errorf("%w: %s", kube.ErrAgentOffline, err.Error())
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	return nil, lastErr
}

// Active implements biz.Tunnels.
func (h *Hub) Active() (map[string]string, uint64) {
	gen := h.gen.Load()
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make(map[string]string, len(h.clusters))
	for id, list := range h.clusters {
		if len(list) > 0 {
			out[id] = list[0].version
		}
	}
	return out, gen
}

// Tokens implements biz.Tunnels.
func (h *Hub) Tokens() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	var out []string
	for _, list := range h.clusters {
		for _, s := range list {
			out = append(out, s.tokenID)
		}
	}
	return out
}

// Drop implements biz.Tunnels.
func (h *Hub) Drop(tokenIDs []string) {
	drop := make(map[string]struct{}, len(tokenIDs))
	for _, id := range tokenIDs {
		drop[id] = struct{}{}
	}
	h.mu.RLock()
	var victims []*session
	for _, list := range h.clusters {
		for _, s := range list {
			if _, ok := drop[s.tokenID]; ok {
				victims = append(victims, s)
			}
		}
	}
	h.mu.RUnlock()
	for _, s := range victims {
		_ = s.mux.Close() // Serve 随之返回并把会话移出注册表
	}
}

// Count 当前隧道数（指标）。
func (h *Hub) Count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	n := 0
	for _, list := range h.clusters {
		n += len(list)
	}
	return n
}

// Close 关闭全部隧道并拒绝新的（进程退出）。
func (h *Hub) Close() {
	h.mu.Lock()
	h.closed = true
	var all []*session
	for _, list := range h.clusters {
		all = append(all, list...)
	}
	h.mu.Unlock()
	for _, s := range all {
		_ = s.mux.Close()
	}
}
