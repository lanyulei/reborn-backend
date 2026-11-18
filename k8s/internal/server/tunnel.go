package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/conf"
	"github.com/lanyulei/reborn/backend/k8s/internal/data/tunnel"
	"github.com/lanyulei/reborn/backend/k8s/internal/service"
)

// defaultTunnelAddr Agent 隧道入口的缺省监听地址。
const defaultTunnelAddr = "0.0.0.0:8061"

// TunnelServer Agent 隧道入口监听：独立端口（本模块唯一对外暴露的面），只有注册与连接两个路径。
// 不挂任何日志中间件——请求头里有令牌。
type TunnelServer struct {
	srv     *http.Server
	network string
	tls     bool
	log     *slog.Logger
}

// NewTunnelBackend 隧道入口认证通过后，连接交给本实例的隧道注册表承载。
func NewTunnelBackend(h *tunnel.Hub) service.TunnelServer { return h }

// NewTunnelServer 构造隧道入口；配置了证书则启用 TLS（1.2+），否则明文（仅限开发或已有外层 TLS 终结）。
func NewTunnelServer(c *conf.Server, agents *service.AgentService, logger *slog.Logger) (*TunnelServer, error) {
	tc := c.GetTunnel()
	addr := tc.GetAddr()
	if addr == "" {
		addr = defaultTunnelAddr
	}
	network := tc.GetNetwork()
	if network == "" {
		network = "tcp"
	}
	srv := &http.Server{
		Addr: addr, Handler: agents.Handler(),
		// 认证之前的读取都很小：头部 10 秒读不完即断开（慢速攻击）；隧道建立后连接被接管，不受这些超时约束。
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
	if err := agents.TrustProxies(tc.GetTrustedProxies()); err != nil {
		return nil, err
	}
	t := &TunnelServer{srv: srv, network: network, log: logger}
	cert, key := tc.GetCertFile(), tc.GetKeyFile()
	switch {
	case cert != "" && key != "":
		pair, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return nil, fmt.Errorf("load tunnel tls certificate: %w", err)
		}
		// 只协商 HTTP/1.1：隧道靠 WebSocket 升级接管连接。
		srv.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair}, NextProtos: []string{"http/1.1"}}
		t.tls = true
	case cert != "" || key != "":
		return nil, errors.New("server.tunnel.cert_file and server.tunnel.key_file must be set together")
	default:
		logger.Warn("agent tunnel serves plain HTTP: agent tokens and cluster traffic travel unencrypted; configure server.tunnel.cert_file/key_file or terminate TLS in front of it")
	}
	return t, nil
}

// Start implements transport.Server.
func (t *TunnelServer) Start(ctx context.Context) error {
	ln, err := (&net.ListenConfig{}).Listen(ctx, t.network, t.srv.Addr)
	if err != nil {
		return fmt.Errorf("listen agent tunnel: %w", err)
	}
	// 连接数上限：限速在读完请求头之后才生效，在那之前的连接洪水只能在这里挡。上限含已建立的隧道。
	ln = &limitListener{Listener: ln, sem: make(chan struct{}, tunnelMaxConns)}
	t.log.Info("agent tunnel listening", "addr", ln.Addr().String(), "tls", t.tls)
	if t.tls {
		err = t.srv.ServeTLS(ln, "", "")
	} else {
		err = t.srv.Serve(ln)
	}
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Stop implements transport.Server。已建立的隧道是被接管的连接，由 tunnel.Hub 的 cleanup 关闭。
func (t *TunnelServer) Stop(ctx context.Context) error {
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return t.srv.Shutdown(sctx)
}

// tunnelMaxConns 隧道入口同时打开的连接数上限（含已建立的隧道；几十个集群 × 主备，远小于它）。
const tunnelMaxConns = 4096

// limitListener 限制同时打开的连接数：满了就不再 Accept（连接留在内核的等待队列里），连接关闭时归还名额。
type limitListener struct {
	net.Listener
	sem chan struct{}
}

// Accept implements net.Listener.
func (l *limitListener) Accept() (net.Conn, error) {
	l.sem <- struct{}{}
	c, err := l.Listener.Accept()
	if err != nil {
		<-l.sem
		return nil, err
	}
	return &limitConn{Conn: c, release: func() { <-l.sem }}, nil
}

type limitConn struct {
	net.Conn
	once    sync.Once
	release func()
}

// Close implements net.Conn.
func (c *limitConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}
