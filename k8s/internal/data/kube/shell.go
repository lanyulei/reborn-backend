package kube

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	"github.com/coder/websocket"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// 终端走集群 exec 子资源的 WebSocket 协议 v5.channel.k8s.io（K8s ≥ 1.32 均支持，不实现 SPDY）。
// 每条二进制消息的首字节是通道号；v5 新增「关闭某通道」的信号：首字节 255，第二字节为通道号。
//
// 没有用 client-go 的 remotecommand：它的 WebSocket 执行器自带拨号器（gorilla），没有自定义拨号的入口，
// 无法在 Agent 隧道上建立连接；它也不经过本包的用户身份传输层。这里直接用连接的 userH1 传输层发起
// 升级，身份（Impersonate-* 头）与隧道两条路径都与普通请求完全一致。
const (
	execProtocol = "v5.channel.k8s.io"

	chStdin  byte = 0
	chStdout byte = 1
	chStderr byte = 2
	chError  byte = 3
	chResize byte = 4
	chClose  byte = 255

	execDialTimeout = 15 * time.Second
	execReadLimit   = 1 << 20
)

// shellCommand 优先 bash，没有则 sh。
var shellCommand = []string{"/bin/sh", "-c", "TERM=xterm-256color; export TERM; if command -v bash >/dev/null 2>&1; then exec bash; else exec sh; fi"}

type shellConn struct {
	ws     *websocket.Conn
	cancel context.CancelFunc
	// stop 解除「集群连接关闭即断开终端」的挂钩（Run 返回时调用）。
	stop func() bool

	mu sync.Mutex // 串行化写（stdin 与 resize 来自不同 goroutine）
}

// Open implements biz.Shell：以用户身份与容器建立终端。能否进入由集群判定（pods/exec 的 create 权限）。
func (m *Manager) Open(ctx context.Context, cl *biz.Cluster, id biz.Identity, p k8scontract.ExecParams) (biz.ShellConn, error) {
	c, err := m.conn(cl.ID)
	if err != nil {
		return nil, err
	}
	u, err := c.asUser(ctx, id)
	if err != nil {
		return nil, err
	}
	// 先读 Pod：尽早给出 403 / 404，并确定缺省容器。
	pod, err := u.kube.CoreV1().Pods(p.Namespace).Get(u.ctx, p.Pod, metav1.GetOptions{})
	if err != nil {
		return nil, mapError(err)
	}
	container := p.Container
	if container == "" {
		if len(pod.Spec.Containers) == 0 {
			return nil, biz.ErrInvalid("pod has no containers")
		}
		container = pod.Spec.Containers[0].Name
	}
	command := shellCommand
	debug := ""
	if p.Kind == k8scontract.ExecKindDebug {
		// 调试容器：先在 Pod 里启动一个临时容器，再进入它（目标容器没有 shell 也能排查）。
		if container, err = u.startDebug(pod, p); err != nil {
			return nil, err
		}
		debug, command = container, debugShellCommand
	}
	target := execTarget(c.host, p.Namespace, p.Pod, container, command, url.Values{
		"stdin": {"true"}, "stdout": {"true"}, "stderr": {"false"}, "tty": {"true"}, // TTY 模式下 stderr 并入 stdout
	})

	// 连接的生命周期跟随集群连接（c.ctx），不跟随建立它的请求；握手超时由定时器另行约束。
	dctx, cancel := context.WithCancel(context.WithValue(c.ctx, identityKey{}, id))
	// 调试容器已经启动：之后终端建不起来（集群拒绝 exec、握手失败或超时、协议不符）就得把它拆掉，
	// 否则它会空转到会话时长上限，一直占着目标容器的资源。
	fail := func(err error) error {
		cancel()
		if debug != "" {
			u.stopDebug(p.Namespace, p.Pod, debug)
		}
		return err
	}
	timer := time.AfterFunc(execDialTimeout, cancel)
	ws, resp, err := websocket.Dial(dctx, target, &websocket.DialOptions{
		HTTPClient: &http.Client{Transport: c.userH1}, Subprotocols: []string{execProtocol},
	})
	if !timer.Stop() && err == nil {
		err = lateHandshakeError(ws)
	}
	if err != nil {
		return nil, fail(dialError(resp, err))
	}
	if ws.Subprotocol() != execProtocol {
		_ = ws.Close(websocket.StatusProtocolError, "")
		return nil, fail(biz.ErrUnsupported("cluster does not support the %s terminal protocol (kubernetes >= 1.32 required)", execProtocol))
	}
	ws.SetReadLimit(execReadLimit)
	// 握手之后 WebSocket 不再跟随 dctx：集群连接被替换（配置变更：换了凭据、禁用了终端）或断开时，
	// 必须主动关闭，否则会话会在旧凭据上一直跑到自己的时长上限。
	stop := context.AfterFunc(c.ctx, func() { _ = ws.CloseNow() })
	return &shellConn{ws: ws, cancel: cancel, stop: stop}, nil
}

// execTarget exec 子资源的地址：在 Pod 的 container 里执行 command；extra 为其余查询参数。
func execTarget(host, namespace, pod, container string, command []string, extra url.Values) string {
	q := url.Values{}
	for k, vs := range extra {
		q[k] = vs
	}
	q.Set("container", container)
	for _, arg := range command {
		q.Add("command", arg)
	}
	return fmt.Sprintf("%s/api/v1/namespaces/%s/pods/%s/exec?%s", host, url.PathEscape(namespace), url.PathEscape(pod), q.Encode())
}

// stopDebug 终端没能建立：结束调试容器的主进程，不让它空转到会话时长上限。尽力而为——以用户身份对调试容器
// 再 exec 一次 kill（同样需要 pods/exec 权限；没有权限、集群不应答时放弃），失败只记日志：容器里的看门狗
// （见 debugMainScript）几分钟后兜底。不跟随请求的取消：客户端断开正是要清理的情形之一。
func (u *userClients) stopDebug(namespace, pod, container string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(u.ctx), debugStopTimeout)
	defer cancel()
	ws, _, err := websocket.Dial(ctx, execTarget(u.c.host, namespace, pod, container, debugKillCommand, url.Values{"stdout": {"true"}}),
		&websocket.DialOptions{HTTPClient: &http.Client{Transport: u.c.userH1}, Subprotocols: []string{execProtocol}})
	if err != nil {
		u.c.m.log.Warn("cannot stop the debug container of a failed terminal", "pod", namespace+"/"+pod, "container", container, "error", mapError(err))
		return
	}
	defer func() { _ = ws.CloseNow() }()
	ws.SetReadLimit(execReadLimit)
	// 命令结束时状态通道（chError）带回结果，或对端关闭连接；迟迟不结束由 ctx 的时限兜底。
	for {
		typ, data, err := ws.Read(ctx)
		if err != nil {
			return
		}
		if typ == websocket.MessageBinary && len(data) > 0 && data[0] == chError {
			return
		}
	}
}

// lateHandshakeError 握手恰在超时定时器触发的那一刻完成：按超时处理，并且必须关掉刚刚建立的连接。
// 握手完成后 ctx 就不再约束它（协议升级后 net/http 与 coder/websocket 都不会因请求 ctx 取消而关闭连接），
// 不关的话到集群的 exec 连接与容器里的 shell 进程会一直挂着。
func lateHandshakeError(ws *websocket.Conn) error {
	_ = ws.CloseNow()
	return context.DeadlineExceeded
}

// dialError 握手失败：集群以普通 HTTP 响应拒绝时，响应体是 Status 对象。
func dialError(resp *http.Response, err error) error {
	if resp != nil && resp.Body != nil {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		var st metav1.Status
		if json.Unmarshal(raw, &st) == nil && st.Code != 0 {
			return mapError(&apierrors.StatusError{ErrStatus: st})
		}
		if resp.StatusCode >= 400 {
			return mapError(&apierrors.StatusError{ErrStatus: metav1.Status{Code: int32(resp.StatusCode), Message: strings.TrimSpace(string(raw))}}) //nolint:gosec // HTTP 状态码
		}
	}
	return mapError(err)
}

func (s *shellConn) write(ctx context.Context, ch byte, p []byte) error {
	buf := make([]byte, 1+len(p))
	buf[0] = ch
	copy(buf[1:], p)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ws.Write(ctx, websocket.MessageBinary, buf)
}

// Run implements biz.ShellConn.
func (s *shellConn) Run(ctx context.Context, sio biz.ShellIO) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer s.cancel()
	defer func() { _ = s.ws.CloseNow() }()
	if s.stop != nil {
		defer s.stop()
	}

	go func() { // 退出路径：stdin 关闭（会话结束时由 ExecManager 关闭）或写失败
		buf := make([]byte, 16<<10)
		for {
			n, err := sio.Stdin.Read(buf)
			if n > 0 {
				if s.write(ctx, chStdin, buf[:n]) != nil {
					return
				}
			}
			if err != nil {
				_ = s.write(ctx, chClose, []byte{chStdin})
				return
			}
		}
	}()
	go func() { // 退出路径：ctx 取消
		for {
			select {
			case <-ctx.Done():
				return
			case r := <-sio.Resize:
				raw, _ := json.Marshal(map[string]uint16{"Width": r.Cols, "Height": r.Rows})
				if s.write(ctx, chResize, raw) != nil {
					return
				}
			}
		}
	}()
	err := s.readLoop(ctx, sio.Stdout)
	cancel()
	_ = s.ws.CloseNow()
	// stdin 的读取由调用方关闭管道来解除；这里不等它（ExecManager 在 Run 返回后立即关闭）。
	return err
}

func (s *shellConn) readLoop(ctx context.Context, stdout io.Writer) error {
	for {
		typ, data, err := s.ws.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			var ce websocket.CloseError
			if errors.As(err, &ce) || errors.Is(err, io.EOF) {
				return nil
			}
			return biz.NewError(502, k8scontract.ReasonUpstream, "terminal connection to the cluster was lost")
		}
		if typ != websocket.MessageBinary || len(data) == 0 {
			continue
		}
		switch data[0] {
		case chStdout, chStderr:
			if _, err := stdout.Write(data[1:]); err != nil {
				return err
			}
		case chError:
			// 命令结束时的状态：Success，或带退出码的 Failure。shell 以非零码退出也算正常结束。
			var st metav1.Status
			if json.Unmarshal(data[1:], &st) != nil || st.Status == metav1.StatusSuccess || st.Reason == "NonZeroExitCode" {
				return nil
			}
			return biz.NewError(502, k8scontract.ReasonUpstream, "%s", st.Message)
		}
	}
}
