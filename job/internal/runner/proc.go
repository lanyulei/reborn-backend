package runner

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	// pipeDrainWait 子进程退出后等待输出管道读完的时长：正常情况下进程一退出管道即 EOF；
	// 若有脱离了进程组的后代仍握着管道的写端（哪怕还在写），退出后的读取总共只给这么久。
	pipeDrainWait = 2 * time.Second
	// tailLimit 短命令（git、ssh-add）保留的输出上限，只用于拼错误信息。
	tailLimit = 16 << 10
)

// proc 一个运行在独立进程组里的子进程。独立进程组让终止信号能一并送达它的全部后代
// （ansible-playbook 的 worker 与 ssh），也让终端的 Ctrl-C 不会越过节点直接打到作业上。
type proc struct {
	cmd  *exec.Cmd
	done chan struct{} // cmd.Wait 返回后关闭
	err  error         // cmd.Wait 的结果；done 关闭后可读
}

// startProc 启动子进程。env 是完整环境（不继承节点进程的环境）；stdin 可为 nil；
// 标准输出与标准错误都写到 out（*os.File，由调用方负责关闭）。track 非 nil 时在启动成功后以进程组号
// 调用一次（作业据此把进程组记进作业目录，供节点崩溃后的下次启动清理）。
//
// 启动与等待都在同一个锁定了 OS 线程的 goroutine 里：Linux 的 Pdeathsig 在「创建子进程的线程」退出时
// 触发，线程必须活到子进程退出为止，否则子进程会被无故杀死（见 sysProcAttr）。
func startProc(path string, args []string, dir string, env []string, stdin io.Reader, out *os.File, track func(pgid int)) (*proc, error) {
	cmd := exec.Command(path, args...) //nolint:gosec // 可执行文件来自节点配置，参数由平台构造并已按契约校验
	cmd.Dir = dir
	cmd.Env = env
	cmd.Stdin = stdin
	cmd.Stdout = out
	cmd.Stderr = out
	cmd.SysProcAttr = sysProcAttr()
	p := &proc{cmd: cmd, done: make(chan struct{})}
	started := make(chan error, 1)
	// 启动并等待进程退出的 goroutine：启动失败立即结束，否则进程退出（或被 stop 杀死）即结束。
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		if err := cmd.Start(); err != nil {
			started <- err
			return
		}
		started <- nil
		p.err = cmd.Wait()
		close(p.done)
	}()
	if err := <-started; err != nil {
		return nil, err
	}
	if track != nil {
		track(cmd.Process.Pid) // Setpgid：进程组号 = 组长的 PID
	}
	return p, nil
}

// signal 向整个进程组发信号；进程组已不存在时是空操作。
func (p *proc) signal(sig syscall.Signal) {
	_ = syscall.Kill(-p.cmd.Process.Pid, sig)
}

// stop 终止序列：向进程组发 SIGTERM，等至多 grace；仍未退出则 SIGKILL。返回时进程已退出。
func (p *proc) stop(grace time.Duration) {
	p.signal(syscall.SIGTERM)
	t := time.NewTimer(grace)
	defer t.Stop()
	select {
	case <-p.done:
	case <-t.C:
	}
	// 无论主进程是否已退出，都把进程组里残留的后代清掉。
	p.signal(syscall.SIGKILL)
	<-p.done
}

// exitCode 进程的退出码；被信号杀死或无法取得时为 -1。done 关闭后调用。
func (p *proc) exitCode() int32 {
	if p.err == nil {
		return 0
	}
	var ee *exec.ExitError
	if errors.As(p.err, &ee) {
		return int32(ee.ExitCode()) //nolint:gosec // 退出码 0-255 或 -1
	}
	return -1
}

// tailBuffer 只保留最后 tailLimit 字节的写入缓冲（并发安全）。
type tailBuffer struct {
	mu  sync.Mutex
	buf []byte
}

func (b *tailBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	if len(b.buf) > tailLimit {
		b.buf = append(b.buf[:0], b.buf[len(b.buf)-tailLimit:]...)
	}
	return len(p), nil
}

func (b *tailBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(bytes.ToValidUTF8(b.buf, []byte("�")))
}

// errInterrupted 命令因作业被中断（取消 / 超时 / 租约失效 / 节点停止）而终止。
var errInterrupted = errors.New("interrupted")

// runShort 运行一条短命令（git、ssh-add）并等它结束，返回它的输出尾部。intr 关闭或超过 timeout
// 时按终止序列杀掉进程组：前者返回 errInterrupted，后者返回超时错误。
func runShort(intr <-chan struct{}, timeout, grace time.Duration, path string, args []string, dir string, env []string, stdin io.Reader, track func(pgid int)) (string, error) {
	pr, pw, err := os.Pipe()
	if err != nil {
		return "", fmt.Errorf("create pipe: %w", err)
	}
	p, err := startProc(path, args, dir, env, stdin, pw, track)
	_ = pw.Close()
	if err != nil {
		_ = pr.Close()
		return "", err
	}
	out := &tailBuffer{}
	copied := make(chan struct{})
	// 读输出的 goroutine：管道 EOF 或被关闭即结束。
	go func() {
		defer close(copied)
		_, _ = io.Copy(out, pr)
	}()
	t := time.NewTimer(timeout)
	defer t.Stop()
	var cause error
	select {
	case <-p.done:
	case <-intr:
		cause = errInterrupted
		p.stop(grace)
	case <-t.C:
		cause = fmt.Errorf("timed out after %s", timeout)
		p.stop(grace)
	}
	p.signal(syscall.SIGKILL) // 清掉进程组里残留的后代
	waitOrClose(copied, pr)
	switch {
	case cause != nil:
		return out.String(), cause
	case p.err != nil:
		return out.String(), p.err
	}
	return out.String(), nil
}

// waitOrClose 等读输出的 goroutine 结束；超过 pipeDrainWait 就关闭读端强制它结束。
func waitOrClose(copied <-chan struct{}, pr *os.File) {
	t := time.NewTimer(pipeDrainWait)
	defer t.Stop()
	select {
	case <-copied:
	case <-t.C:
	}
	_ = pr.Close()
	<-copied
}

// streamProc 启动一个长时间运行的子进程，把它的输出逐段交给 sink（在读输出的 goroutine 里顺序调用；
// sink 可以阻塞，阻塞即经管道对子进程施加背压）。返回的 wait 等输出读完。
//
// 进程退出后管道通常立即 EOF。若有脱离了进程组的后代（setsid）仍握着写端——甚至还在不停地写——
// 读取不能无限期继续：进程退出之后，花在「读」上的时间总共至多 pipeDrainWait（阻塞在 sink 里的时间
// 不计，否则背压期间尚未读出的输出会被丢掉），用完即关闭管道。
func streamProc(path string, args []string, dir string, env []string, sink func(chunk []byte), track func(pgid int)) (p *proc, wait func(), err error) {
	pr, pw, err := os.Pipe()
	if err != nil {
		return nil, nil, fmt.Errorf("create pipe: %w", err)
	}
	p, err = startProc(path, args, dir, env, nil, pw, track)
	_ = pw.Close()
	if err != nil {
		_ = pr.Close()
		return nil, nil, err
	}
	var exitedAt atomic.Int64 // 进程退出的时刻（UnixNano）；0 = 尚未退出
	copied := make(chan struct{})
	// 读输出的 goroutine：管道 EOF、读错误或退出后的读取预算用完即结束。
	go func() {
		defer close(copied)
		defer func() { _ = pr.Close() }()
		buf := make([]byte, 32<<10)
		left := pipeDrainWait
		for {
			if exitedAt.Load() != 0 {
				if left <= 0 {
					return
				}
				_ = pr.SetReadDeadline(time.Now().Add(left))
			}
			begin := time.Now()
			n, err := pr.Read(buf)
			if at := exitedAt.Load(); at != 0 {
				// 只计进程退出之后的读取时间。
				if exit := time.Unix(0, at); exit.After(begin) {
					begin = exit
				}
				left -= time.Since(begin)
			}
			if n > 0 {
				sink(buf[:n])
			}
			if err != nil {
				return
			}
		}
	}()
	// 进程退出后给「正阻塞在读上」的那一次读设期限。
	go func() {
		select {
		case <-p.done:
			exitedAt.Store(time.Now().UnixNano())
			_ = pr.SetReadDeadline(time.Now().Add(pipeDrainWait))
		case <-copied:
		}
	}()
	return p, func() { <-copied }, nil
}
