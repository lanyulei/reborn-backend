package kube

import (
	"archive/tar"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"

	"github.com/coder/websocket"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ biz.FileStore = (*Manager)(nil)

// execPipe 以用户身份在容器里运行一条非交互命令：stdin 来自 in（nil = 不接），stdout 写给 out，
// 返回 stderr 的前 4KB。命令以非零码退出返回 *execExit。协议同终端（v5.channel.k8s.io，无 TTY）。
func (u *userClients) execPipe(ctx context.Context, namespace, pod, container string, cmd []string, in io.Reader, out io.Writer) (string, error) {
	q := url.Values{}
	if container != "" {
		q.Set("container", container)
	}
	for _, arg := range cmd {
		q.Add("command", arg)
	}
	q.Set("stdin", fmt.Sprint(in != nil))
	q.Set("stdout", "true")
	q.Set("stderr", "true")
	q.Set("tty", "false")
	target := fmt.Sprintf("%s/api/v1/namespaces/%s/pods/%s/exec?%s", u.c.host, url.PathEscape(namespace), url.PathEscape(pod), q.Encode())

	ctx, cancel := context.WithCancel(context.WithValue(ctx, identityKey{}, u.id))
	defer cancel()
	stop := context.AfterFunc(u.c.ctx, cancel)
	defer stop()
	dctx, dcancel := context.WithTimeout(ctx, execDialTimeout)
	ws, resp, err := websocket.Dial(dctx, target, &websocket.DialOptions{
		HTTPClient: &http.Client{Transport: u.c.userH1}, Subprotocols: []string{execProtocol},
	})
	dcancel()
	if err != nil {
		return "", dialError(resp, err)
	}
	defer func() { _ = ws.CloseNow() }()
	if ws.Subprotocol() != execProtocol {
		return "", biz.ErrUnsupported("cluster does not support the %s exec protocol (kubernetes >= 1.32 required)", execProtocol)
	}
	ws.SetReadLimit(execReadLimit)

	var wmu sync.Mutex
	write := func(ch byte, p []byte) error {
		buf := make([]byte, 1+len(p))
		buf[0] = ch
		copy(buf[1:], p)
		wmu.Lock()
		defer wmu.Unlock()
		return ws.Write(ctx, websocket.MessageBinary, buf)
	}
	inErr := make(chan error, 1)
	if in != nil {
		go func() { // 退出路径：in 读完 / 出错，或 ctx 取消使写失败
			buf := make([]byte, 32<<10)
			for {
				n, rerr := in.Read(buf)
				if n > 0 {
					if werr := write(chStdin, buf[:n]); werr != nil {
						inErr <- werr
						return
					}
				}
				if rerr != nil {
					_ = write(chClose, []byte{chStdin})
					if errors.Is(rerr, io.EOF) {
						rerr = nil
					}
					inErr <- rerr
					return
				}
			}
		}()
	} else {
		inErr <- nil
	}

	var stderr strings.Builder
	for {
		typ, data, rerr := ws.Read(ctx)
		if rerr != nil {
			if ctx.Err() != nil {
				return stderr.String(), ctx.Err()
			}
			var ce websocket.CloseError
			if errors.As(rerr, &ce) || errors.Is(rerr, io.EOF) {
				return stderr.String(), <-inErr
			}
			return stderr.String(), biz.NewError(http.StatusBadGateway, "K8S_UPSTREAM_ERROR", "exec connection to the cluster was lost")
		}
		if typ != websocket.MessageBinary || len(data) == 0 {
			continue
		}
		switch data[0] {
		case chStdout:
			if _, werr := out.Write(data[1:]); werr != nil {
				return stderr.String(), werr
			}
		case chStderr:
			if stderr.Len() < 4096 {
				stderr.Write(data[1:min(len(data), 1+4096-stderr.Len())])
			}
		case chError:
			var st metav1.Status
			if json.Unmarshal(data[1:], &st) != nil || st.Status == metav1.StatusSuccess {
				select {
				case ierr := <-inErr:
					return stderr.String(), ierr
				case <-time.After(2 * time.Second):
					return stderr.String(), nil
				}
			}
			return stderr.String(), &execExit{message: st.Message, stderr: stderr.String()}
		}
	}
}

// execExit 容器内的命令失败（非零退出码，或无法启动）。
type execExit struct {
	message string
	stderr  string
}

func (e *execExit) Error() string { return strings.TrimSpace(e.message + ": " + e.stderr) }

// tarError 把 tar 命令的失败翻译成给用户看的错误。
func tarError(err error, stderr string) error {
	var ee *execExit
	if !errors.As(err, &ee) {
		return mapError(err)
	}
	text := strings.ToLower(ee.message + " " + ee.stderr)
	switch {
	case strings.Contains(text, "executable file not found") || strings.Contains(text, "no such file or directory: unknown") ||
		strings.Contains(text, "exec: \"tar\""):
		return biz.ErrFileInvalid("tar is not available in the container; file transfer needs it (try a debug container)")
	case strings.Contains(text, "no such file or directory"):
		return biz.ErrFileInvalid("path does not exist in the container")
	case strings.Contains(text, "permission denied"):
		return biz.ErrFileInvalid("permission denied inside the container")
	case strings.Contains(text, "not a directory"):
		return biz.ErrFileInvalid("target is not a directory")
	}
	msg := strings.TrimSpace(firstNonEmpty(ee.stderr, stderr, ee.message))
	msg = biz.Truncate(msg, 300)
	return biz.ErrFileInvalid("tar failed in the container: %s", msg)
}

func firstNonEmpty(ss ...string) string {
	for _, s := range ss {
		if strings.TrimSpace(s) != "" {
			return s
		}
	}
	return ""
}

func (m *Manager) fileUser(ctx context.Context, cl *biz.Cluster, id biz.Identity, ref biz.FileRef) (*userClients, string, error) {
	c, err := m.conn(cl.ID)
	if err != nil {
		return nil, "", err
	}
	u, err := c.asUser(ctx, id)
	if err != nil {
		return nil, "", err
	}
	// 先读 Pod：尽早给出 403 / 404，并确定缺省容器。
	pod, err := u.kube.CoreV1().Pods(ref.Namespace).Get(u.ctx, ref.Pod, metav1.GetOptions{})
	if err != nil {
		return nil, "", mapError(err)
	}
	container := ref.Container
	if container == "" {
		if len(pod.Spec.Containers) == 0 {
			return nil, "", biz.ErrInvalid("pod has no containers")
		}
		container = pod.Spec.Containers[0].Name
	}
	return u, container, nil
}

// limitWriter 写出累计超过 limit 即失败。
type limitWriter struct {
	w     io.Writer
	limit int64
	n     int64
}

var (
	errOverLimit = errors.New("transfer limit exceeded")
	// errNoData 容器产生了空归档：通常是路径不存在，具体原因在命令的错误输出里。
	errNoData = errors.New("the container produced no data")
)

func (l *limitWriter) Write(p []byte) (int, error) {
	if l.n+int64(len(p)) > l.limit {
		return 0, errOverLimit
	}
	n, err := l.w.Write(p)
	l.n += int64(n)
	return n, err
}

// Download implements biz.FileStore。在容器里执行 tar cf - -C <目录> -- <名称>，把输出当作不可信的 tar 流
// 逐条目解析：
//   - 首个条目是与目标同名的普通文件 → 只把它的内容作为原文下发；
//   - 否则是目录 → 重新编码为 tar 下发，丢弃名称不安全（绝对路径、含 ..、不在目标之下）的条目、
//     指向目标之外的链接，以及设备文件等特殊条目。
//
// 累计超过 maxBytes 即中止。
func (m *Manager) Download(ctx context.Context, cl *biz.Cluster, id biz.Identity, ref biz.FileRef, maxBytes int64, begin biz.FileBegin) (int64, error) {
	u, container, err := m.fileUser(ctx, cl, id, ref)
	if err != nil {
		return 0, err
	}
	dir, base := path.Split(ref.Path)
	if dir != "/" {
		dir = strings.TrimSuffix(dir, "/")
	}
	pr, pw := io.Pipe()
	ectx, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		stderr string
		err    error
	}
	done := make(chan result, 1)
	go func() { // 退出路径：命令结束，或 ectx 取消
		stderr, err := u.execPipe(ectx, ref.Namespace, ref.Pod, container, []string{"tar", "cf", "-", "-C", dir, "--", base}, nil, pw)
		_ = pw.CloseWithError(err)
		done <- result{stderr, err}
	}()
	n, werr := relayTar(pr, base, maxBytes, begin)
	if werr != nil && !errors.Is(werr, errNoData) {
		cancel()
		_ = pr.CloseWithError(werr)
	} else {
		// 正常结束时读完 tar 的结尾填充；空归档（路径不存在等）时等命令自己退出，拿到它的错误输出。
		_, _ = io.Copy(io.Discard, pr)
	}
	res := <-done
	switch {
	case errors.Is(werr, errOverLimit):
		return n, biz.ErrFileTooLarge(maxBytes)
	case res.err != nil && ctx.Err() == nil && (werr == nil || n == 0):
		return n, tarError(res.err, res.stderr)
	case werr != nil:
		return n, biz.ErrFileInvalid("download failed: %s", werr.Error())
	}
	return n, nil
}

// relayTar 解析容器产生的 tar 流并下发（见 Download）。返回下发的内容字节数。
func relayTar(r io.Reader, base string, maxBytes int64, begin biz.FileBegin) (int64, error) {
	tr := tar.NewReader(r)
	first, err := tr.Next()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return 0, errNoData
		}
		return 0, err
	}
	if first.Typeflag == tar.TypeReg && path.Clean(first.Name) == base {
		if first.Size > maxBytes {
			return 0, errOverLimit
		}
		lw := &limitWriter{w: begin(base, false, first.Size), limit: maxBytes}
		_, err := io.Copy(lw, tr) //nolint:gosec // tar 不压缩；写出端 limitWriter 已按 maxBytes 截断
		return lw.n, err
	}
	lw := &limitWriter{w: begin(base+".tar", true, -1), limit: maxBytes}
	tw := tar.NewWriter(lw)
	for h := first; ; {
		if keepTarEntry(h, base) {
			out := &tar.Header{
				Typeflag: h.Typeflag, Name: h.Name, Linkname: h.Linkname, Size: h.Size,
				Mode: h.Mode & 0o777, ModTime: h.ModTime, Format: tar.FormatPAX,
			}
			if err := tw.WriteHeader(out); err != nil {
				return lw.n, err
			}
			if h.Typeflag == tar.TypeReg {
				if _, err := io.Copy(tw, tr); err != nil { //nolint:gosec // 同上：下游 limitWriter 截断
					return lw.n, err
				}
			}
		}
		h, err = tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return lw.n, err
		}
	}
	return lw.n, tw.Close()
}

// keepTarEntry 报告条目是否下发：只保留名称安全的目录、普通文件与不逃出目标的链接。
func keepTarEntry(h *tar.Header, base string) bool {
	if !biz.SafeTarEntry(h.Name, base) {
		return false
	}
	switch h.Typeflag {
	case tar.TypeReg, tar.TypeDir:
		return true
	case tar.TypeSymlink:
		return biz.SafeTarLink(h.Name, h.Linkname)
	case tar.TypeLink:
		return biz.SafeTarEntry(h.Linkname, base)
	}
	return false
}

// Upload implements biz.FileStore。本端构造只含一个条目的 tar 流（名称已校验为单个路径段），
// 在容器里执行 tar xf - -C <目录>：容器内写入的位置只可能是该目录下的这一个文件。
func (m *Manager) Upload(ctx context.Context, cl *biz.Cluster, id biz.Identity, ref biz.FileRef, name string, size int64, r io.Reader) error {
	u, container, err := m.fileUser(ctx, cl, id, ref)
	if err != nil {
		return err
	}
	pr, pw := io.Pipe()
	go func() { // 退出路径：写完、读端关闭或 r 出错
		tw := tar.NewWriter(pw)
		err := tw.WriteHeader(&tar.Header{Typeflag: tar.TypeReg, Name: name, Size: size, Mode: 0o644, ModTime: time.Now(), Format: tar.FormatPAX})
		if err == nil {
			var n int64
			n, err = io.CopyN(tw, r, size)
			if err != nil && n < size {
				err = fmt.Errorf("upload body ended after %d of %d bytes: %w", n, size, err)
			}
		}
		if err == nil {
			err = tw.Close()
		}
		_ = pw.CloseWithError(err)
	}()
	stderr, err := u.execPipe(ctx, ref.Namespace, ref.Pod, container, []string{"tar", "xf", "-", "-C", ref.Path}, pr, io.Discard)
	_ = pr.CloseWithError(errors.New("upload finished"))
	if err != nil {
		var ee *execExit
		if errors.As(err, &ee) {
			return tarError(err, stderr)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return biz.ErrFileInvalid("upload failed: %s", biz.AuditErrorText(mapError(err)))
	}
	return nil
}
