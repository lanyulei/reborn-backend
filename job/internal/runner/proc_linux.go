//go:build linux

package runner

import (
	"bytes"
	"os"
	"strconv"
	"syscall"
	"time"
)

// sysProcAttr 子进程的进程属性：独立进程组；节点进程死亡（含被 SIGKILL）时内核向子进程发 SIGKILL。
// Pdeathsig 绑定的是「创建子进程的那个 OS 线程」而不是进程：startProc 在锁定了 OS 线程的 goroutine 里
// 启动并等待子进程，线程在子进程退出之前不会被回收。它只送达直接子进程（ansible-playbook、git、
// ssh-agent 本身）；它们的后代由启动时的残留进程组清理兜底（killStaleGroup）。
func sysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
}

// killStaleGroup 杀掉上次运行残留的、确属作业目录 jobDir 的进程，返回杀掉的进程数。
// PID 会被系统复用，所以不看登记的进程组号，只认环境：见 killJobProcesses。
func killStaleGroup(_ int, _ time.Time, jobDir string) int { return killJobProcesses(jobDir) }

// killJobProcesses 杀掉环境里 HOME 正是作业目录 jobDir 的全部进程，返回杀掉的进程数。平台给每个子进程
// 都设了 HOME=<作业目录>（目录名是执行 ID，不会与别的进程相同），其后代继承——包括 setsid 脱离了
// 进程组的那些。读不到环境的（别的用户的进程）不动。
func killJobProcesses(jobDir string) int {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return 0
	}
	want := []byte("HOME=" + jobDir)
	killed := 0
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid <= 1 || pid == os.Getpid() {
			continue
		}
		environ, err := os.ReadFile("/proc/" + e.Name() + "/environ")
		if err != nil {
			continue
		}
		ours := false
		for _, kv := range bytes.Split(environ, []byte{0}) {
			if bytes.Equal(kv, want) {
				ours = true
				break
			}
		}
		if ours && syscall.Kill(pid, syscall.SIGKILL) == nil {
			killed++
		}
	}
	return killed
}
