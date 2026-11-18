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
	if p.Kind == k8scontract.ExecKindDebug {
		// 调试容器：先在 Pod 里启动一个临时容器，再进入它（目标容器没有 shell 也能排查）。
		if container, err = u.startDebug(pod, p); err != nil {
			return nil, err
		}
		command = debugShellCommand
	}
	q := url.Values{}
	q.Set("container", container)
	for _, arg := range command {
		q.Add("command", arg)
	}
	q.Set("stdin", "true")
	q.Set("stdout", "true")
	q.Set("stderr", "false") // TTY 模式下 stderr 并入 stdout
	q.Set("tty", "true")
	target := fmt.Sprintf("%s/api/v1/namespaces/%s/pods/%s/exec?%s", c.host, url.PathEscape(p.Namespace), url.PathEscape(p.Pod), q.Encode())

	// 连接的生命周期跟随集群连接（c.ctx），不跟随建立它的请求；握手超时由定时器另行约束。
	dctx, cancel := context.WithCancel(context.WithValue(c.ctx, identityKey{}, id))
	timer := time.AfterFunc(execDialTimeout, cancel)
	ws, resp, err := websocket.Dial(dctx, target, &websocket.DialOptions{
		HTTPClient: &http.Client{Transport: c.userH1}, Subprotocols: []string{execProtocol},
	})
	if !timer.Stop() && err == nil {
		err = context.DeadlineExceeded
	}
	if err != nil {
		cancel()
		return nil, dialError(resp, err)
	}
	if ws.Subprotocol() != execProtocol {
		_ = ws.Close(websocket.StatusProtocolError, "")
		cancel()
		return nil, biz.ErrUnsupported("cluster does not support the %s terminal protocol (kubernetes >= 1.32 required)", execProtocol)
	}
	ws.SetReadLimit(execReadLimit)
	// 握手之后 WebSocket 不再跟随 dctx：集群连接被替换（配置变更：换了凭据、禁用了终端）或断开时，
	// 必须主动关闭，否则会话会在旧凭据上一直跑到自己的时长上限。
	stop := context.AfterFunc(c.ctx, func() { _ = ws.CloseNow() })
	return &shellConn{ws: ws, cancel: cancel, stop: stop}, nil
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
