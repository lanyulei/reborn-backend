package runner

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/jobcontract"
)

// 1. 执行 ID 不合法的任务：在拼任何路径之前就被丢弃——工作目录原封不动，不 panic，不向引擎报结束。
func TestLaunchDropsTaskWithBadID(t *testing.T) {
	cfg := testConfig(t)
	if err := os.MkdirAll(cfg.WorkDir, 0o700); err != nil {
		t.Fatal(err)
	}
	keep := filepath.Join(cfg.WorkDir, "known_hosts")
	if err := os.WriteFile(keep, []byte("host key\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	eng := newFakeEngine()
	logs := &syncBuffer{}
	r := newRunner(cfg, eng, testLogger(logs))
	for _, id := range []string{
		"./././././././.", // filepath.Join 之后等于工作目录本身
		"abc", "", "..", "../../../../etc", strings.Repeat("a", 36), "018F0000-0000-7000-8000-000000000001",
	} {
		task := adhocTask(jobcontract.KindCommand, jobcontract.Adhoc{Command: "id"})
		task.ExecutionID = id
		if j, err := newJob(cfg, eng, testLogger(nil), task); err == nil || j != nil {
			t.Fatalf("id %q: job constructed", id)
		}
		r.launch(context.Background(), task) // 不得 panic
	}
	noLease := adhocTask(jobcontract.KindCommand, jobcontract.Adhoc{Command: "id"})
	noLease.LeaseToken = ""
	r.launch(context.Background(), noLease)
	r.wg.Wait()

	if r.running() != 0 {
		t.Fatal("a dropped task was registered as running")
	}
	if got, err := os.ReadFile(keep); err != nil || string(got) != "host key\n" {
		t.Fatalf("work dir was touched: %q, %v", got, err)
	}
	if entries, _ := os.ReadDir(cfg.WorkDir); len(entries) != 1 {
		t.Fatalf("work dir entries: %v", entries)
	}
	eng.mu.Lock()
	defer eng.mu.Unlock()
	if len(eng.finishes) != 0 || eng.reports != 0 {
		t.Fatalf("a dropped task reached the engine: %d finishes, %d reports", len(eng.finishes), eng.reports)
	}
	if strings.Count(logs.String(), "claimed task dropped") != 8 {
		t.Fatalf("drops not logged:\n%s", logs.String())
	}
	// 短 ID 不会让控制目录的计算越界。
	for _, id := range []string{"", "abc", "./././././././."} {
		if got := controlDirFor("/w", id); got != "" {
			t.Fatalf("controlDirFor(%q) = %q", id, got)
		}
	}
}

// 1. 清理只删本作业自己建的目录：作业目录没建成（这里：工作目录不可写）时，什么都不删。
func TestCleanupRemovesOnlyOwnDirs(t *testing.T) {
	cfg := testConfig(t)
	eng := newFakeEngine()
	task := adhocTask(jobcontract.KindCommand, jobcontract.Adhoc{Command: "id"})
	j := newTestJob(cfg, eng, nil, task)
	// 作业还没运行：两个目录都不是它建的。放两个同名目录冒充「别人的」。
	for _, dir := range []string{j.dir, j.ctlDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	j.cleanup()
	for _, dir := range []string{j.dir, j.ctlDir} {
		if _, err := os.Stat(dir); err != nil {
			t.Fatalf("cleanup removed a directory the job did not create: %v", err)
		}
	}
}

// 3. 脱离了进程组的后代（setsid）握着输出管道不停地写：进程退出后读取只再持续有限的时间，
// 作业不会被它永远拖住。
func TestStreamProcBoundedAfterExit(t *testing.T) {
	if _, err := exec.LookPath("perl"); err != nil {
		t.Skip("perl not available")
	}
	pidFile := filepath.Join(t.TempDir(), "pid")
	script := `perl -MPOSIX -e 'exit 0 if fork(); setsid(); $|=1; open(F, ">", $ARGV[0]); print F $$; close(F); while (1) { print "noise\n"; select(undef, undef, undef, 0.05); }' ` + pidFile + `; echo main-done`
	var out syncBuffer
	start := time.Now()
	p, wait, err := streamProc("/bin/sh", []string{"-c", script}, "", []string{"PATH=" + os.Getenv("PATH")}, func(b []byte) { _, _ = out.Write(b) }, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if raw, err := os.ReadFile(pidFile); err == nil {
			if pid, err := strconv.Atoi(strings.TrimSpace(string(raw))); err == nil {
				_ = exec.Command("kill", "-9", strconv.Itoa(pid)).Run()
			}
		}
	})
	<-p.done
	done := make(chan struct{})
	go func() { wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(pipeDrainWait + 10*time.Second):
		t.Fatal("reading never stopped: a detached descendant pins the job")
	}
	if elapsed := time.Since(start); elapsed > pipeDrainWait+8*time.Second {
		t.Fatalf("took %s", elapsed)
	}
	if !strings.Contains(out.String(), "main-done") || !strings.Contains(out.String(), "noise") {
		t.Fatalf("output before the cut-off must still be delivered: %q", out.String())
	}
}

// 3. sink 阻塞（背压）的时间不计入退出后的读取预算：进程早已退出，积压在管道里的输出仍然读得全。
func TestStreamProcSlowSinkLosesNothing(t *testing.T) {
	var out syncBuffer
	first := true
	sink := func(b []byte) {
		if first {
			first = false
			time.Sleep(pipeDrainWait + time.Second) // 比整个预算还长
		}
		_, _ = out.Write(b)
	}
	// 40KB：超过一次读取（32KB），小于管道缓冲，进程写完立即退出。
	p, wait, err := streamProc("/bin/sh", []string{"-c", `i=0; while [ $i -lt 1000 ]; do echo "line $i ..............................."; i=$((i+1)); done`},
		"", []string{"PATH=" + os.Getenv("PATH")}, sink, nil)
	if err != nil {
		t.Fatal(err)
	}
	<-p.done
	wait()
	if !strings.Contains(out.String(), "line 0 ") || !strings.Contains(out.String(), "line 999 ") {
		t.Fatalf("output lost under backpressure: %d bytes", len(out.String()))
	}
}

// 4. 作业把子进程的进程组记进作业目录；节点下次启动时据此杀掉残留的进程组，再删目录。
func TestStaleProcessGroupsAreKilledOnStart(t *testing.T) {
	cfg := testConfig(t)
	const id = "018f0000-0000-7000-8000-0000000000f1"
	dir := filepath.Join(cfg.WorkDir, id)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "secrets.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	j := newTestJob(cfg, newFakeEngine(), nil, func() *jobcontract.Task {
		task := adhocTask(jobcontract.KindCommand, jobcontract.Adhoc{Command: "id"})
		task.ExecutionID = id
		return task
	}())
	devnull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = devnull.Close() }()
	// 一个「残留」的作业进程：环境与工作目录同真实的子进程（HOME = 作业目录），自己还有一个子进程。
	leftover, err := startProc("/bin/sh", []string{"-c", "sleep 300 & wait"}, dir, baseEnv(dir), nil, devnull, j.track)
	if err != nil {
		t.Fatal(err)
	}
	// 一个无关的进程组，冒充「PID 被复用」：记录里的启动时刻对不上，环境里的 HOME 也不是作业目录。
	unrelated, err := startProc("/bin/sh", []string{"-c", "sleep 300"}, "", []string{"PATH=" + os.Getenv("PATH"), "HOME=/nonexistent"}, nil, devnull, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { unrelated.signal(9); <-unrelated.done; leftover.signal(9); <-leftover.done })

	raw, err := os.ReadFile(filepath.Join(dir, pgidsName))
	if err != nil || !strings.HasPrefix(string(raw), strconv.Itoa(leftover.cmd.Process.Pid)+" ") {
		t.Fatalf("pgids file: %q, %v", raw, err)
	}
	stale := fmt.Sprintf("%d %d\n", unrelated.cmd.Process.Pid, time.Now().Add(-time.Hour).Unix())
	if err := os.WriteFile(filepath.Join(dir, pgidsName), append(raw, []byte(stale+"garbage line\n1 0\n")...), 0o600); err != nil {
		t.Fatal(err)
	}

	logs := &syncBuffer{}
	r := newRunner(cfg, newFakeEngine(), testLogger(logs))
	if err := r.prepareWorkDir(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-leftover.done:
	case <-time.After(10 * time.Second):
		t.Fatal("the leftover process group was not killed")
	}
	select {
	case <-unrelated.done:
		t.Fatal("an unrelated process (reused pid) was killed")
	case <-time.After(300 * time.Millisecond):
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("stale job dir not removed (err %v)", err)
	}
	if !strings.Contains(logs.String(), "killed process groups left by a previous run") {
		t.Fatalf("not logged:\n%s", logs.String())
	}
}

// 4. ssh-agent 里的私钥带寿命：作业时限 + 余量。
func TestAgentArgs(t *testing.T) {
	got := agentArgs("/w/job/agent.sock", 10*time.Minute)
	want := []string{"-D", "-a", "/w/job/agent.sock", "-t", "900"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args %q, want %q", got, want)
	}
}

// 5. 作业目录在子进程结束后立刻删除，不等日志交完与结束上报（引擎不可达时那两步会一直重试）。
func TestJobDirRemovedBeforeDrain(t *testing.T) {
	cfg := testConfig(t)
	eng := newFakeEngine()
	eng.reportDown = true
	task := adhocTask(jobcontract.KindScript, jobcontract.Adhoc{Script: "echo hi\n", Interpreter: "sh"})
	j := newTestJob(cfg, eng, nil, task)
	done := make(chan struct{})
	go func() { defer close(done); j.run(context.Background()) }()
	waitFor(t, "job dir to be removed while the engine is down", func() bool {
		entries, err := os.ReadDir(cfg.WorkDir)
		return err == nil && len(entries) == 0 && j.buf.sizeForTest() > 0
	})
	select {
	case <-done:
		t.Fatal("job finished although the engine never acknowledged anything")
	default:
	}
	eng.set(func(e *fakeEngine) { e.reportDown = false })
	if fin := eng.finish(t); fin.Outcome != jobcontract.OutcomeExited || fin.ExitCode != 0 || len(fin.Hosts) != 2 {
		t.Fatalf("finish: %+v", fin)
	}
	<-done
}

// 6a. 只有真正的 PEM 标记行不掩码；恰好以 ----- 开头的密码照常掩码。
func TestMaskerDashedSecret(t *testing.T) {
	m := newMasker("-----my-password-----", testKey)
	out := m.Mask("pw=-----my-password----- -----BEGIN OPENSSH PRIVATE KEY----- -----END OPENSSH PRIVATE KEY-----")
	if out != "pw="+maskText+" -----BEGIN OPENSSH PRIVATE KEY----- -----END OPENSSH PRIVATE KEY-----" {
		t.Fatalf("got %q", out)
	}
	if got := newMasker("-----BEGIN something else").Mask("x -----BEGIN something else y"); got != "x "+maskText+" y" {
		t.Fatalf("got %q", got)
	}
}

// 6b. 相互重叠的凭据：合并成一段掩码，不漏尾巴——整段与流式两种用法都是。
func TestMaskerOverlappingSecrets(t *testing.T) {
	cases := []struct {
		secrets  []string
		in, want string
	}{
		{[]string{"abcd", "bcdefgh12345"}, "xabcdefgh12345y", "x" + maskText + "y"},
		{[]string{"abcd", "cdef", "efgh"}, "1abcdefgh2", "1" + maskText + "2"},
		{[]string{"aaaa"}, "xaaaaaay", "x" + maskText + "y"},
		{[]string{"abcd", "efgh"}, "abcdefgh", maskText + maskText}, // 首尾相接、不重叠：各自成段
		{[]string{"abcd", "bcdefgh12345"}, "abcd.bcdefgh12345", maskText + "." + maskText},
		{[]string{"secret", "secret-longer"}, "a secret-longer b", "a " + maskText + " b"},
	}
	for _, c := range cases {
		if got := newMasker(c.secrets...).Mask(c.in); got != c.want {
			t.Errorf("%v: Mask(%q) = %q, want %q", c.secrets, c.in, got, c.want)
		}
		for step := 1; step <= len(c.in); step++ {
			m := newMasker(c.secrets...)
			var b strings.Builder
			for i := 0; i < len(c.in); i += step {
				b.WriteString(m.Write([]byte(c.in[i:min(i+step, len(c.in))])))
			}
			b.WriteString(m.Flush())
			if b.String() != c.want {
				t.Errorf("%v step %d: stream(%q) = %q, want %q", c.secrets, step, c.in, b.String(), c.want)
			}
		}
	}
}

// 6c. 用户名为空时请求头用占位用户名 git；掩码目标必须是同一个编码。
func TestMaskerBasicAuthDefaultUser(t *testing.T) {
	auth := jobcontract.GitAuth{Type: jobcontract.GitAuthToken, Secret: "tok-SECRET-123"}
	env := strings.Join(gitEnv(&Config{GitAllowProtocols: defaultGitProtocols}, "/w/job", auth, ""), "\n")
	header := "Basic " + basicAuth("git", "tok-SECRET-123")
	if !strings.Contains(env, header) {
		t.Fatalf("header not found in git env:\n%s", env)
	}
	m := newMasker(taskSecrets(&jobcontract.Task{Git: auth})...)
	if got := m.Mask("fatal: sent Authorization: " + header); strings.Contains(got, basicAuth("git", "tok-SECRET-123")) {
		t.Fatalf("the header value git actually sends is not masked: %q", got)
	}
}
