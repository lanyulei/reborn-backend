//go:build linux

package runner

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// 辅助进程：冒充节点——用 startProc 起一个子进程，打印它的 PID，然后一直不退出。
func TestMain(m *testing.M) {
	if os.Getenv("JOB_TEST_PDEATHSIG_HELPER") == "1" {
		devnull, _ := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
		p, err := startProc("/bin/sleep", []string{"300"}, "", []string{"PATH=/usr/bin:/bin"}, nil, devnull, nil)
		if err != nil {
			fmt.Println("error", err)
			os.Exit(1)
		}
		fmt.Println(p.cmd.Process.Pid)
		select {}
	}
	os.Exit(m.Run())
}

func alive(pid int) bool { return syscall.Kill(pid, 0) == nil }

// 4. 节点进程被 SIGKILL：子进程随之被内核杀死（Pdeathsig）。
func TestChildDiesWithRunner(t *testing.T) {
	helper := exec.Command(os.Args[0], "-test.run=^$")
	helper.Env = append(os.Environ(), "JOB_TEST_PDEATHSIG_HELPER=1")
	stdout, err := helper.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := helper.Start(); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		t.Fatalf("read helper output: %v", err)
	}
	child, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil {
		t.Fatalf("helper said %q", line)
	}
	t.Cleanup(func() { _ = syscall.Kill(child, syscall.SIGKILL) })
	if !alive(child) {
		t.Fatal("child not running")
	}
	_ = helper.Process.Kill() // SIGKILL：节点来不及做任何清理
	_ = helper.Wait()
	deadline := time.Now().Add(10 * time.Second)
	for alive(child) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if alive(child) {
		t.Fatal("the child outlived its killed parent")
	}
}

// 4. Pdeathsig 绑定创建子进程的 OS 线程：节点还活着时，子进程不能因为线程被回收而无故被杀。
func TestChildSurvivesThreadChurn(t *testing.T) {
	devnull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = devnull.Close() }()
	p, err := startProc("/bin/sleep", []string{"300"}, "", []string{"PATH=/usr/bin:/bin"}, nil, devnull, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { p.signal(syscall.SIGKILL); <-p.done }()
	// 制造线程的创建与销毁：锁定线程后直接退出的 goroutine 会让它的 OS 线程终止。
	for range 200 {
		done := make(chan struct{})
		go func() {
			runtime.LockOSThread()
			close(done) // 不解锁就返回：线程随 goroutine 一起销毁
		}()
		<-done
	}
	time.Sleep(500 * time.Millisecond)
	select {
	case <-p.done:
		t.Fatal("the child was killed although the runner is still alive")
	default:
	}
}
