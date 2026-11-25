package kube

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// 握手恰在超时定时器触发的那一刻完成：按超时返回，并且关掉刚刚建立的连接。握手完成后 ctx 就不再约束
// 这条连接，不主动关的话它一直挂着——到集群的 exec 连接与容器里的 shell 进程随之泄漏。
func TestLateHandshakeClosesConnection(t *testing.T) {
	t.Parallel()
	peerDone := make(chan error, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{execProtocol}})
		if err != nil {
			peerDone <- err
			return
		}
		defer func() { _ = c.CloseNow() }()
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		_, _, err = c.Read(ctx) // 模拟集群：客户端不关连接，就一直没有结果
		peerDone <- err
	}))
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ws, _, err := websocket.Dial(ctx, srv.URL, &websocket.DialOptions{Subprotocols: []string{execProtocol}})
	if err != nil {
		t.Fatal(err)
	}
	if err := lateHandshakeError(ws); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a late handshake must be reported as a timeout: %v", err)
	}
	select {
	case err := <-peerDone:
		if errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("the connection to the cluster stayed open after a late handshake")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the cluster side did not observe the connection being closed")
	}
	_ = ws.CloseNow() // 重复关闭不 panic
}
