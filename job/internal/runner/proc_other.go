//go:build !linux

package runner

import (
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// sysProcAttr 子进程的进程属性：独立进程组。Linux 以外没有 Pdeathsig：节点进程被强杀后子进程会残留，
// 只能靠下次启动时的残留进程组清理（killStaleGroup）。
func sysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true}
}

// staleStartSlack 记录的启动时刻与 ps 报告的启动时刻之间允许的误差。
const staleStartSlack = 3 * time.Second

// killStaleGroup 杀掉上次运行残留的进程组 pgid，返回 1（已发信号）或 0。尽力而为：没有 /proc，只能核对
// 组长进程的启动时刻（ps lstart，精确到秒）与记录的时刻相符——PID 被无关进程复用时两者几乎不可能
// 相同。组长已退出而组内还有后代时无从核对，不动它们。
func killStaleGroup(pgid int, started time.Time, _ string) int {
	cmd := exec.Command("ps", "-o", "lstart=", "-p", strconv.Itoa(pgid)) //nolint:gosec // 参数是数字
	cmd.Env = []string{"PATH=/usr/bin:/bin", "LC_ALL=C"}
	out, err := cmd.Output()
	if err != nil {
		return 0
	}
	at, err := time.ParseInLocation("Mon Jan _2 15:04:05 2006", strings.TrimSpace(string(out)), time.Local)
	if err != nil {
		return 0
	}
	if d := at.Sub(started); d < -staleStartSlack || d > staleStartSlack {
		return 0
	}
	if syscall.Kill(-pgid, syscall.SIGKILL) != nil {
		return 0
	}
	return 1
}

// killJobProcesses 没有 /proc，无法按环境找出脱离了进程组的后代：不做。
func killJobProcesses(string) int { return 0 }
