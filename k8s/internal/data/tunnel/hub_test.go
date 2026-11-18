package tunnel

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/data/kube"
	"github.com/lanyulei/reborn/backend/k8s/internal/tunnelproto"

	"github.com/hashicorp/yamux"
)

// connect 建一条隧道：返回 Agent 一侧的 yamux 会话与 Serve 的结束信号。
func connect(t *testing.T, h *Hub, cluster, token, version string) (*yamux.Session, <-chan struct{}) {
	t.Helper()
	planeSide, agentSide := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = h.Serve(context.Background(), cluster, token, version, planeSide)
	}()
	mux, err := yamux.Server(agentSide, tunnelproto.SessionConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mux.Close() })
	waitFor(t, func() bool { return hasToken(h, token) })
	return mux, done
}

func hasToken(h *Hub, token string) bool {
	for _, id := range h.Tokens() {
		if id == token {
			return true
		}
	}
	return false
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func waitClosed(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("tunnel did not close")
	}
}

// serveEcho Agent 一侧把每条流当 HTTP 连接服务，应答自己的名字。
func serveEcho(mux *yamux.Session, name string) {
	go func() {
		_ = (&http.Server{ReadHeaderTimeout: 5 * time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, name) })}).Serve(mux)
	}()
}

func ask(t *testing.T, h *Hub, cluster string) string {
	t.Helper()
	c, err := h.DialCluster(context.Background(), cluster)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = c.Close() }()
	_, _ = io.WriteString(c, "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	return string(body)
}

func TestDialWithoutTunnelIsOffline(t *testing.T) {
	t.Parallel()
	h := NewHub(slog.New(slog.DiscardHandler))
	if _, err := h.DialCluster(context.Background(), "c1"); !errors.Is(err, kube.ErrAgentOffline) {
		t.Fatalf("err = %v, want ErrAgentOffline", err)
	}
}

// Agent 不得向数据面开流：一旦开流，整条隧道被关闭。
func TestAgentCannotOpenStreams(t *testing.T) {
	t.Parallel()
	h := NewHub(slog.New(slog.DiscardHandler))
	mux, done := connect(t, h, "c1", "t1", "v1")
	st, err := mux.OpenStream()
	if err == nil {
		_, _ = st.Write([]byte("GET /internal/v1/op/list HTTP/1.1\r\n\r\n"))
	}
	waitClosed(t, done)
	if n := h.Count(); n != 0 {
		t.Fatalf("tunnels after violation = %d", n)
	}
	if _, err := h.DialCluster(context.Background(), "c1"); !errors.Is(err, kube.ErrAgentOffline) {
		t.Fatalf("dial after violation: %v", err)
	}
}

func TestStandbyTakesOverAndDropRevokes(t *testing.T) {
	t.Parallel()
	h := NewHub(slog.New(slog.DiscardHandler))
	primary, primaryDone := connect(t, h, "c1", "t1", "v-primary")
	serveEcho(primary, "primary")
	standby, standbyDone := connect(t, h, "c1", "t2", "v-standby")
	serveEcho(standby, "standby")
	other, _ := connect(t, h, "c2", "t3", "v-other")
	serveEcho(other, "other")

	active, gen := h.Active()
	if active["c1"] != "v-primary" || active["c2"] != "v-other" || h.Count() != 3 {
		t.Fatalf("active = %v count = %d", active, h.Count())
	}
	if got := ask(t, h, "c1"); got != "primary" {
		t.Fatalf("served by %q, want primary", got)
	}
	// 一个集群的流永远到不了另一个集群的 Agent。
	if got := ask(t, h, "c2"); got != "other" {
		t.Fatalf("c2 served by %q", got)
	}
	// 吊销主隧道的令牌：立即断开，备用接替。
	h.Drop([]string{"t1"})
	waitClosed(t, primaryDone)
	if got := ask(t, h, "c1"); got != "standby" {
		t.Fatalf("after drop served by %q, want standby", got)
	}
	active, gen2 := h.Active()
	if active["c1"] != "v-standby" || gen2 == gen {
		t.Fatalf("active after drop = %v, generation %d → %d", active, gen, gen2)
	}
	h.Close()
	waitClosed(t, standbyDone)
	planeSide, _ := net.Pipe()
	if err := h.Serve(context.Background(), "c1", "t9", "v", planeSide); !errors.Is(err, ErrHubClosed) {
		t.Fatalf("serve after close: %v", err)
	}
}

// 同一个令牌重连：旧隧道被顶替，不会残留两条。
func TestSameTokenReplacesOldTunnel(t *testing.T) {
	t.Parallel()
	h := NewHub(slog.New(slog.DiscardHandler))
	_, oldDone := connect(t, h, "c1", "t1", "v1")
	planeSide, agentSide := net.Pipe()
	go func() { _ = h.Serve(context.Background(), "c1", "t1", "v2", planeSide) }()
	mux, err := yamux.Server(agentSide, tunnelproto.SessionConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mux.Close() }()
	waitClosed(t, oldDone)
	waitFor(t, func() bool { a, _ := h.Active(); return a["c1"] == "v2" && h.Count() == 1 })
}
