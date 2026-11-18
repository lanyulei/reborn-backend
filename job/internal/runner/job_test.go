package runner

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/job/internal/biz"
	"github.com/lanyulei/reborn/backend/job/internal/biztest"
	"github.com/lanyulei/reborn/backend/job/internal/service"
	"github.com/lanyulei/reborn/backend/kit/httpx"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/jobcontract"
)

// ---------------------------------------------------------------------------
// 终止序列
// ---------------------------------------------------------------------------

// startScript 起一个 sh 脚本并逐行读它的输出。
func startScript(t *testing.T, script string) (*proc, *bufio.Reader) {
	t.Helper()
	pr, pw, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	p, err := startProc("/bin/sh", []string{"-c", script}, "", []string{"PATH=" + os.Getenv("PATH")}, nil, pw, nil)
	_ = pw.Close()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		p.signal(9)
		<-p.done
		_ = pr.Close()
	})
	return p, bufio.NewReader(pr)
}

func readLine(t *testing.T, r *bufio.Reader) string {
	t.Helper()
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read script output: %v", err)
	}
	return strings.TrimSpace(line)
}

// 进程响应 SIGTERM：宽限期内退出，不动用 SIGKILL。
func TestStopGraceful(t *testing.T) {
	p, out := startScript(t, `trap 'echo bye; exit 7' TERM; echo ready; while :; do sleep 0.05; done`)
	if readLine(t, out) != "ready" {
		t.Fatal("script not ready")
	}
	start := time.Now()
	p.stop(10 * time.Second)
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("graceful stop took %s", d)
	}
	if readLine(t, out) != "bye" {
		t.Fatal("TERM trap did not run")
	}
	if code := p.exitCode(); code != 7 {
		t.Fatalf("exit code %d, want 7 (the trap's own exit)", code)
	}
}

// 进程无视 SIGTERM：宽限期满后 SIGKILL，连同它的子进程（同一进程组）一起清掉。
func TestStopEscalatesToKill(t *testing.T) {
	p, out := startScript(t, `trap '' TERM; sleep 300 & echo "child $!"; echo ready; while :; do sleep 0.05; done`)
	child, err := strconv.Atoi(strings.TrimPrefix(readLine(t, out), "child "))
	if err != nil {
		t.Fatal(err)
	}
	if readLine(t, out) != "ready" {
		t.Fatal("script not ready")
	}
	const grace = 300 * time.Millisecond
	start := time.Now()
	p.stop(grace)
	elapsed := time.Since(start)
	if elapsed < grace {
		t.Fatalf("killed after %s: SIGKILL must wait for the grace period", elapsed)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("stop took %s", elapsed)
	}
	if code := p.exitCode(); code != -1 {
		t.Fatalf("exit code %d, want -1 (killed by signal)", code)
	}
	waitFor(t, "child process to die", func() bool { return !pidAlive(child) })
}

func TestRunShort(t *testing.T) {
	env := []string{"PATH=" + os.Getenv("PATH")}
	never := make(chan struct{})
	out, err := runShort(never, 10*time.Second, time.Second, "/bin/sh", []string{"-c", "cat; echo err >&2; exit 0"}, "", env, strings.NewReader("in\n"), nil)
	if err != nil || out != "in\nerr\n" {
		t.Fatalf("out %q err %v", out, err)
	}
	if _, err := runShort(never, 10*time.Second, time.Second, "/bin/sh", []string{"-c", "echo nope; exit 3"}, "", env, nil, nil); err == nil {
		t.Fatal("non-zero exit must be an error")
	}
	start := time.Now()
	if _, err := runShort(never, 100*time.Millisecond, 100*time.Millisecond, "/bin/sh", []string{"-c", "trap '' TERM; sleep 300"}, "", env, nil, nil); err == nil || time.Since(start) > 10*time.Second {
		t.Fatalf("timeout: err %v after %s", err, time.Since(start))
	}
	intr := make(chan struct{})
	close(intr)
	if _, err := runShort(intr, 10*time.Second, 100*time.Millisecond, "/bin/sh", []string{"-c", "sleep 300"}, "", env, nil, nil); err != errInterrupted {
		t.Fatalf("interrupt: %v", err)
	}
	if _, err := runShort(never, time.Second, time.Second, "/nonexistent/binary", nil, "", env, nil, nil); err == nil {
		t.Fatal("missing binary must be an error")
	}
	// 脱离进程组的后代握着管道不放：命令退出后不会被它拖住。
	start = time.Now()
	out, err = runShort(never, 10*time.Second, time.Second, "/bin/sh", []string{"-c", "(trap '' HUP; set -m; sleep 30 &) ; echo done"}, "", env, nil, nil)
	if err != nil || !strings.Contains(out, "done") || time.Since(start) > 10*time.Second {
		t.Fatalf("out %q err %v after %s", out, err, time.Since(start))
	}
}

// ---------------------------------------------------------------------------
// 作业（假引擎 + 假 ansible-playbook + 真实 git over HTTP）
// ---------------------------------------------------------------------------

func TestJobSuccess(t *testing.T) {
	t.Setenv("KRATOS_RUNNER_TOKEN", "runner-token-must-not-leak")
	g := newGitFixture(t, "")
	cfg := testConfig(t)
	eng := newFakeEngine()
	task := gitTask(g, "site.yml")
	logs := &syncBuffer{}
	newTestJob(cfg, eng, logs, task).run(context.Background())

	fin := eng.finish(t)
	if fin.Outcome != jobcontract.OutcomeExited || fin.ExitCode != 0 || fin.Error != "" {
		t.Fatalf("finish: %+v", fin)
	}
	if fin.ExecutionID != task.ExecutionID || fin.LeaseToken != task.LeaseToken || fin.CommitID != g.Commit {
		t.Fatalf("finish identity: %+v (commit want %s)", fin, g.Commit)
	}
	wantHosts := []jobcontract.HostResult{
		{Address: "10.0.0.1", OK: 3, Changed: 1, Skipped: 2},
		{Address: "10.0.0.2", OK: 2},
	}
	if len(fin.Hosts) != 2 || fin.Hosts[0] != wantHosts[0] || fin.Hosts[1] != wantHosts[1] {
		t.Fatalf("hosts: %+v", fin.Hosts)
	}
	out := eng.log()
	jobDir := filepath.Join(cfg.WorkDir, task.ExecutionID)
	for _, want := range []string{
		"[runner] 拉取代码 " + g.URL + "（main）",
		"[runner] 代码已就绪，提交 " + g.Commit,
		"[runner] 开始执行 site.yml（主机 2 台，并发 5）",
		"ARGS: -i " + jobDir + "/inventory.json site.yml -e @" + jobDir + "/vars.json -e @" + jobDir + "/secrets.json --forks 5",
		"/" + task.ExecutionID + "/repo\n", // 工作目录 = 仓库目录
		"HOME=" + jobDir + "\n",
		"ANSIBLE_CONFIG=" + jobDir + "/ansible.cfg\n",
		"ANSIBLE_FORCE_COLOR=0\n", "ANSIBLE_NOCOLOR=1\n", "PYTHONUNBUFFERED=1\n",
		`"ansible_port": 2222`, `"cmdb_name": !unsafe "web-1"`, `"ansible_become": true`,
		`FILE vars.json: {"version": !unsafe "1.2.3"}`,
		`FILE secrets.json: {"ansible_become_password": !unsafe "********", "ansible_password": !unsafe "********"}`,
		"MODES: drwx------ -rw------- \n", // 作业目录 0700，其中的文件 0600
		"KEYFILES: []",
		"PLAY RECAP",
		"[runner] 执行结束，退出码 0",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("log lacks %q:\n%s", want, out)
		}
	}
	// 凭据与节点自己的环境都不在输出里；密码凭据不需要 agent。
	for _, leak := range []string{"login-PASSWORD-1", "become-PASSWORD-2", "runner-token-must-not-leak", "KRATOS_", "SSH_AUTH_SOCK", cfg.Token} {
		if strings.Contains(out, leak) || strings.Contains(logs.String(), leak) {
			t.Fatalf("%q leaked:\n%s", leak, out)
		}
	}
	if eng.commit != g.Commit {
		t.Fatalf("commit reported via report = %q", eng.commit)
	}
	if eng.afterFinish {
		t.Fatal("a report arrived after finish")
	}
	// 作业目录与控制目录都已删除：工作目录里只剩假脚本之外的东西——没有。
	if _, err := os.Stat(jobDir); !os.IsNotExist(err) {
		t.Fatalf("job dir still exists (err %v)", err)
	}
	if entries, err := os.ReadDir(cfg.WorkDir); err != nil || len(entries) != 0 {
		t.Fatalf("work dir not empty after the job: %v %v", entries, err)
	}
}

func TestJobFailedPlaybook(t *testing.T) {
	g := newGitFixture(t, "")
	eng := newFakeEngine()
	newTestJob(testConfig(t), eng, nil, gitTask(g, "fail.yml")).run(context.Background())
	fin := eng.finish(t)
	if fin.Outcome != jobcontract.OutcomeExited || fin.ExitCode != 2 || fin.Error != "1 台主机失败，0 台不可达（ansible-playbook 退出码 2）" {
		t.Fatalf("finish: %+v", fin)
	}
	if len(fin.Hosts) != 2 || fin.Hosts[0].Failed != 1 || fin.Hosts[1].Failed != 0 {
		t.Fatalf("hosts: %+v", fin.Hosts)
	}
	if jobcontract.FinalStatus(fin.Outcome, fin.ExitCode, fin.Hosts) != jobcontract.StatusPartial {
		t.Fatal("one failed + one ok host must be partial")
	}
	// 标准错误与标准输出合流。
	if !strings.Contains(eng.log(), "FAILED!") {
		t.Fatalf("stderr missing from the log:\n%s", eng.log())
	}
}

func TestExitError(t *testing.T) {
	hosts := []jobcontract.HostResult{
		{Address: "a", OK: 3},
		{Address: "b", OK: 1, Failed: 2},
		{Address: "c", Unreachable: 1},
		{Address: "d", Failed: 1, Unreachable: 1}, // 既失败又不可达：按失败算（与主机结果的口径一致）
		{Address: "e", Unreachable: 1},
	}
	if got := exitError(4, hosts); got != "2 台主机失败，2 台不可达（ansible-playbook 退出码 4）" {
		t.Fatalf("got %q", got)
	}
	// 没有解析到 PLAY RECAP，或其中没有失败 / 不可达的主机：只给退出码。
	for _, none := range [][]jobcontract.HostResult{nil, {{Address: "a", OK: 1}}} {
		if got := exitError(1, none); got != "ansible-playbook 以退出码 1 结束" {
			t.Fatalf("got %q", got)
		}
	}
}

// SSH 私钥凭据：私钥经 ssh-agent 提供，不落盘；agent 随作业结束而终止。
func TestJobSSHKeyCredential(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "id")
	if out, err := exec.Command("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", "test", "-f", keyFile).CombinedOutput(); err != nil {
		t.Skipf("ssh-keygen unavailable: %v %s", err, out)
	}
	key, err := os.ReadFile(keyFile)
	if err != nil {
		t.Fatal(err)
	}
	g := newGitFixture(t, "")
	cfg := testConfig(t)
	eng := newFakeEngine()
	task := gitTask(g, "site.yml")
	task.Credential = jobcontract.TaskCredential{Type: jobcontract.CredentialSSHKey, Username: "ops", Secret: strings.TrimRight(string(key), "\n")}
	task.Options.Become = false
	newTestJob(cfg, eng, nil, task).run(context.Background())

	fin := eng.finish(t)
	out := eng.log()
	if fin.Outcome != jobcontract.OutcomeExited || fin.ExitCode != 0 {
		t.Fatalf("finish: %+v\n%s", fin, out)
	}
	sock := filepath.Join(cfg.WorkDir, task.ExecutionID, "agent.sock")
	for _, want := range []string{"SSH_AUTH_SOCK=" + sock, "AGENT-KEYS: 256 SHA256:", "(ED25519)", "KEYFILES: []"} {
		if !strings.Contains(out, want) {
			t.Fatalf("log lacks %q:\n%s", want, out)
		}
	}
	// 没有口令 → 没有 secrets.json。
	if strings.Contains(out, "secrets.json") {
		t.Fatalf("unexpected secrets file:\n%s", out)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(key)), "\n") {
		if !strings.HasPrefix(line, "-----") && strings.Contains(out, line) {
			t.Fatal("private key material leaked into the log")
		}
	}
	if _, err := os.Stat(sock); !os.IsNotExist(err) {
		t.Fatalf("agent socket still exists (err %v)", err)
	}
	if out, _ := exec.Command("pgrep", "-f", sock).Output(); strings.TrimSpace(string(out)) != "" {
		t.Fatalf("ssh-agent still running: %s", out)
	}

	// 不是私钥的内容：ssh-add 拒绝 → 未能启动，错误里没有凭据。
	eng2 := newFakeEngine()
	bad := gitTask(g, "site.yml")
	bad.Credential = jobcontract.TaskCredential{Type: jobcontract.CredentialSSHKey, Username: "ops", Secret: "not-a-private-key-SECRET"}
	newTestJob(cfg, eng2, nil, bad).run(context.Background())
	fin = eng2.finish(t)
	if fin.Outcome != jobcontract.OutcomeError || !strings.Contains(fin.Error, "私钥") || strings.Contains(fin.Error+eng2.log(), "not-a-private-key-SECRET") {
		t.Fatalf("bad key: %+v\n%s", fin, eng2.log())
	}
}

// Git 令牌：经 http.extraHeader 送达，拉取失败时的错误信息里没有令牌。
func TestJobGitToken(t *testing.T) {
	want := "Basic " + basicAuth("deploy", "git-TOKEN-xyz")
	g := newGitFixture(t, want)
	cfg := testConfig(t)

	eng := newFakeEngine()
	task := gitTask(g, "site.yml")
	task.Git = jobcontract.GitAuth{Type: jobcontract.GitAuthToken, Username: "deploy", Secret: "git-TOKEN-xyz"}
	newTestJob(cfg, eng, nil, task).run(context.Background())
	if fin := eng.finish(t); fin.Outcome != jobcontract.OutcomeExited || fin.CommitID != g.Commit {
		t.Fatalf("finish: %+v\n%s", fin, eng.log())
	}
	seen := false
	for _, a := range g.auths() {
		seen = seen || a == want
	}
	if !seen {
		t.Fatalf("git server never saw the token header: %q", g.auths())
	}
	// 令牌只在拉取时使用：ansible-playbook 的环境里没有它。
	if out := eng.log(); strings.Contains(out, "git-TOKEN-xyz") || strings.Contains(out, basicAuth("deploy", "git-TOKEN-xyz")) || strings.Contains(out, "GIT_CONFIG") {
		t.Fatalf("token leaked into the job log:\n%s", out)
	}

	// 令牌不对 → 拉取失败。
	eng2 := newFakeEngine()
	wrong := gitTask(g, "site.yml")
	wrong.Git = jobcontract.GitAuth{Type: jobcontract.GitAuthToken, Username: "deploy", Secret: "wrong-TOKEN-abc"}
	newTestJob(cfg, eng2, nil, wrong).run(context.Background())
	fin := eng2.finish(t)
	if fin.Outcome != jobcontract.OutcomeError || !strings.Contains(fin.Error, "拉取代码失败") || fin.CommitID != "" {
		t.Fatalf("finish: %+v", fin)
	}
	for _, leak := range []string{"wrong-TOKEN-abc", basicAuth("deploy", "wrong-TOKEN-abc")} {
		if strings.Contains(fin.Error, leak) || strings.Contains(eng2.log(), leak) {
			t.Fatalf("token leaked: %+v\n%s", fin, eng2.log())
		}
	}
}

func TestJobStartFailures(t *testing.T) {
	g := newGitFixture(t, "")
	cases := map[string]struct {
		mutate func(task *jobcontract.Task, cfg *Config)
		want   string
	}{
		"missing playbook":     {func(task *jobcontract.Task, _ *Config) { task.Playbook = "nope.yml" }, "不存在"},
		"playbook escapes":     {func(task *jobcontract.Task, _ *Config) { task.Playbook = "escape.yml" }, "仓库之外"},
		"unknown ref":          {func(task *jobcontract.Task, _ *Config) { task.Ref = "no-such-branch" }, "拉取代码失败"},
		"invalid task":         {func(task *jobcontract.Task, _ *Config) { task.ExtraVars["ansible_connection"] = "local" }, "参数名"},
		"file protocol":        {func(task *jobcontract.Task, _ *Config) { task.RepoURL = "file:///etc" }, "仓库地址"},
		"ansible not found":    {func(_ *jobcontract.Task, cfg *Config) { cfg.AnsiblePlaybook = "/nonexistent/ansible-playbook" }, "无法启动"},
		"git not found":        {func(_ *jobcontract.Task, cfg *Config) { cfg.Git = "/nonexistent/git" }, "拉取代码失败"},
		"protocol not allowed": {func(_ *jobcontract.Task, cfg *Config) { cfg.GitAllowProtocols = "ssh" }, "拉取代码失败"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := testConfig(t)
			task := gitTask(g, "site.yml")
			tc.mutate(task, cfg)
			eng := newFakeEngine()
			newTestJob(cfg, eng, nil, task).run(context.Background())
			fin := eng.finish(t)
			if fin.Outcome != jobcontract.OutcomeError || !strings.Contains(fin.Error, tc.want) || len(fin.Hosts) != 0 {
				t.Fatalf("finish: %+v", fin)
			}
			if jobcontract.FinalStatus(fin.Outcome, fin.ExitCode, fin.Hosts) != jobcontract.StatusFailed {
				t.Fatal("start failure must map to failed")
			}
			// 失败原因也写进了作业日志。
			if !strings.Contains(eng.log(), "[runner] ") || !strings.Contains(eng.log(), tc.want) {
				t.Fatalf("log lacks the reason:\n%s", eng.log())
			}
			for _, leak := range []string{"login-PASSWORD-1", "become-PASSWORD-2"} {
				if strings.Contains(fin.Error+eng.log(), leak) {
					t.Fatal("secret leaked")
				}
			}
			if _, err := os.Stat(filepath.Join(cfg.WorkDir, task.ExecutionID)); !os.IsNotExist(err) {
				t.Fatalf("job dir still exists (err %v)", err)
			}
		})
	}
}

func TestJobCancel(t *testing.T) {
	g := newGitFixture(t, "")
	eng := newFakeEngine()
	eng.cancelOn = "STARTED"
	start := time.Now()
	newTestJob(testConfig(t), eng, nil, gitTask(g, "sleep.yml")).run(context.Background())
	fin := eng.finish(t)
	if fin.Outcome != jobcontract.OutcomeCanceled || fin.Error != "" || fin.CommitID != g.Commit {
		t.Fatalf("finish: %+v", fin)
	}
	out := eng.log()
	// 脚本收到了 SIGTERM 并自行退出（没有等到 SIGKILL）。
	if !strings.Contains(out, "GOT-TERM") || !strings.Contains(out, "[runner] 已按取消请求终止") {
		t.Fatalf("log:\n%s", out)
	}
	if time.Since(start) > 30*time.Second {
		t.Fatalf("cancel took %s", time.Since(start))
	}
}

func TestJobCancelEscalatesToKill(t *testing.T) {
	g := newGitFixture(t, "")
	eng := newFakeEngine()
	eng.cancelOn = "STARTED"
	cfg := testConfig(t)
	cfg.CancelGrace = 300 * time.Millisecond
	newTestJob(cfg, eng, nil, gitTask(g, "stubborn.yml")).run(context.Background())
	if fin := eng.finish(t); fin.Outcome != jobcontract.OutcomeCanceled {
		t.Fatalf("finish: %+v", fin)
	}
	if out, _ := exec.Command("pgrep", "-f", cfg.AnsiblePlaybook).Output(); strings.TrimSpace(string(out)) != "" {
		t.Fatalf("fake ansible still running: %s", out)
	}
}

func TestJobTimeout(t *testing.T) {
	g := newGitFixture(t, "")
	eng := newFakeEngine()
	j := newTestJob(testConfig(t), eng, nil, gitTask(g, "sleep.yml"))
	j.timeout = 1500 * time.Millisecond
	j.run(context.Background())
	fin := eng.finish(t)
	if fin.Outcome != jobcontract.OutcomeTimeout || !strings.Contains(fin.Error, "超时") {
		t.Fatalf("finish: %+v", fin)
	}
	if !strings.Contains(eng.log(), "GOT-TERM") {
		t.Fatalf("log:\n%s", eng.log())
	}
	// 缺省时限来自任务选项。
	if d := newTestJob(testConfig(t), eng, nil, gitTask(g, "site.yml")).timeout; d != 600*time.Second {
		t.Fatalf("timeout from options = %s", d)
	}
}

// 租约失效（409）：终止作业并丢弃，不再报结束。
func TestJobLeaseLost(t *testing.T) {
	g := newGitFixture(t, "")
	eng := newFakeEngine()
	cfg := testConfig(t)
	task := gitTask(g, "sleep.yml")
	j := newTestJob(cfg, eng, nil, task)
	done := make(chan struct{})
	go func() { defer close(done); j.run(context.Background()) }()
	waitFor(t, "playbook to start", func() bool { return strings.Contains(eng.log(), "STARTED") })
	eng.set(func(e *fakeEngine) { e.reportErrs = []error{errLeaseLost} })
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("job did not stop after losing its lease")
	}
	eng.mu.Lock()
	finishes := len(eng.finishes)
	eng.mu.Unlock()
	if finishes != 0 {
		t.Fatal("a discarded job must not send finish")
	}
	if out, _ := exec.Command("pgrep", "-f", cfg.AnsiblePlaybook).Output(); strings.TrimSpace(string(out)) != "" {
		t.Fatalf("fake ansible still running: %s", out)
	}
	if _, err := os.Stat(filepath.Join(cfg.WorkDir, task.ExecutionID)); !os.IsNotExist(err) {
		t.Fatalf("job dir still exists (err %v)", err)
	}
}

// 结束请求被答复 409：丢弃，不再重试。
func TestJobFinishLeaseLost(t *testing.T) {
	g := newGitFixture(t, "")
	eng := newFakeEngine()
	eng.finishErrs = []error{errLeaseLost}
	done := make(chan struct{})
	go func() {
		defer close(done)
		newTestJob(testConfig(t), eng, nil, gitTask(g, "site.yml")).run(context.Background())
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("job did not return after finish was rejected with 409")
	}
	if len(eng.finishes) != 0 {
		t.Fatal("finish must not be retried after 409")
	}
}

// 引擎一度不可达：作业照常跑完，日志从确认序号之后重传，不丢、不乱序；结束请求重试到成功。
func TestJobSurvivesEngineOutage(t *testing.T) {
	g := newGitFixture(t, "")
	eng := newFakeEngine()
	eng.reportDown = true
	eng.dropAfter = 1 // 恢复之后每次只确认一块：其余的要重传
	eng.finishErrs = []error{io.ErrUnexpectedEOF, &statusError{code: http.StatusBadGateway}, io.ErrUnexpectedEOF}
	logs := &syncBuffer{}
	j := newTestJob(testConfig(t), eng, logs, gitTask(g, "flood.yml"))
	j.buf = newLogBuffer(4<<10, 64<<10) // 小缓冲：断连期间必然写满，靠背压顶住
	done := make(chan struct{})
	go func() { defer close(done); j.run(context.Background()) }()

	// 断连期间作业不结束、不放弃；缓冲写满后子进程被背压挡住。
	waitFor(t, "buffer to fill", func() bool { return j.buf.sizeForTest() >= 64<<10 })
	time.Sleep(200 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("job gave up while the engine was unreachable")
	default:
	}
	if size := j.buf.sizeForTest(); size > 64<<10+40<<10 {
		t.Fatalf("buffer grew to %d bytes: backpressure is not bounding it", size)
	}
	eng.set(func(e *fakeEngine) { e.reportDown = false })

	fin := eng.finish(t)
	<-done
	if fin.Outcome != jobcontract.OutcomeExited || fin.ExitCode != 0 || len(fin.Hosts) != 2 {
		t.Fatalf("finish: %+v", fin)
	}
	out := eng.log()
	prev := -1
	for _, line := range strings.Split(out, "\n") {
		if !strings.HasPrefix(line, "flood line ") {
			continue
		}
		n, err := strconv.Atoi(strings.Fields(line)[2])
		if err != nil || n != prev+1 {
			t.Fatalf("log line out of order or lost: %q after %d", line, prev)
		}
		prev = n
	}
	if prev != 3999 {
		t.Fatalf("last flood line = %d, want 3999", prev)
	}
	eng.mu.Lock()
	defer eng.mu.Unlock()
	for seq := int64(1); seq <= eng.ack; seq++ {
		if _, ok := eng.chunks[seq]; !ok {
			t.Fatalf("chunk %d missing", seq)
		}
	}
	if eng.afterFinish {
		t.Fatal("finish was sent before all chunks were acknowledged")
	}
	if !strings.Contains(logs.String(), "report to engine failed") || !strings.Contains(logs.String(), "finish failed") {
		t.Fatalf("outage not logged:\n%s", logs.String())
	}
}

// 没有输出时也按 lease/4 上报以续租。
func TestJobKeepalive(t *testing.T) {
	g := newGitFixture(t, "")
	eng := newFakeEngine()
	task := gitTask(g, "sleep.yml")
	task.LeaseSeconds = 1 // 每 250ms 续租一次
	cfg := testConfig(t)
	cfg.reportEvery = defaultReportEvery
	j := newTestJob(cfg, eng, nil, task)
	done := make(chan struct{})
	go func() { defer close(done); j.run(context.Background()) }()
	waitFor(t, "playbook to start", func() bool { return strings.Contains(eng.log(), "STARTED") })
	eng.mu.Lock()
	before := eng.reports
	eng.mu.Unlock()
	time.Sleep(1200 * time.Millisecond)
	eng.mu.Lock()
	idle := eng.reports - before
	eng.mu.Unlock()
	if idle < 3 || idle > 8 {
		t.Fatalf("%d keepalive reports in 1.2s with a 1s lease, want about 4", idle)
	}
	eng.set(func(e *fakeEngine) { e.cancel = true })
	<-done
}

// 结束请求持续被引擎判为不合法：有限次重试后放弃，不永久占着并发位。
func TestJobFinishGivesUpOnPermanentRejection(t *testing.T) {
	g := newGitFixture(t, "")
	eng := newFakeEngine()
	for range finishGiveUp + 5 {
		eng.finishErrs = append(eng.finishErrs, errUnauthorized)
	}
	logs := &syncBuffer{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		newTestJob(testConfig(t), eng, logs, gitTask(g, "site.yml")).run(context.Background())
	}()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("job never gave up")
	}
	if len(eng.finishErrs) != 5 || len(eng.finishes) != 0 || !strings.Contains(logs.String(), "giving up") {
		t.Fatalf("attempts left %d finishes %d", len(eng.finishErrs), len(eng.finishes))
	}
}

// ---------------------------------------------------------------------------
// 节点循环（假引擎）
// ---------------------------------------------------------------------------

func startRunner(t *testing.T, r *Runner) (stop func()) {
	t.Helper()
	started := make(chan error, 1)
	go func() { started <- r.Start(context.Background()) }()
	stopped := false
	stop = func() {
		if stopped {
			return
		}
		stopped = true
		if err := r.Stop(context.Background()); err != nil {
			t.Errorf("stop: %v", err)
		}
		select {
		case err := <-started:
			if err != nil {
				t.Errorf("start returned %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Error("Start did not return after Stop")
		}
	}
	t.Cleanup(stop)
	return stop
}

func taskN(g *gitFixture, playbook string, n int) *jobcontract.Task {
	task := gitTask(g, playbook)
	task.ExecutionID = "018f0000-0000-7000-8000-00000000000" + strconv.Itoa(n)
	return task
}

func TestRunnerRespectsCapacityAndEnabled(t *testing.T) {
	g := newGitFixture(t, "")
	cfg := testConfig(t)
	cfg.Capacity = 2
	eng := newFakeEngine()
	eng.enabled = false
	eng.tasks = []*jobcontract.Task{taskN(g, "sleep.yml", 1), taskN(g, "sleep.yml", 2), taskN(g, "site.yml", 3)}
	// 上次崩溃残留的作业目录在启动时清掉。
	stale := filepath.Join(cfg.WorkDir, "018f0000-0000-7000-8000-0000000000ff")
	staleCtl, keep := filepath.Join(cfg.WorkDir, "c0123456789ab"), filepath.Join(cfg.WorkDir, "cache")
	for _, dir := range []string{stale, staleCtl, keep} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(stale, "secrets.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	r := newRunner(cfg, eng, testLogger(nil))
	stop := startRunner(t, r)

	// 节点被停用：心跳照常，但不领取。
	waitFor(t, "heartbeats", func() bool { eng.mu.Lock(); defer eng.mu.Unlock(); return len(eng.heartbeats) >= 3 })
	eng.mu.Lock()
	if eng.claims != 0 {
		t.Fatalf("claimed %d times while disabled", eng.claims)
	}
	hb := eng.heartbeats[0]
	eng.enabled = true
	eng.mu.Unlock()
	if hb.Capacity != 2 || hb.Running != 0 || hb.AgentVersion != "test" || hb.Hostname == "" {
		t.Fatalf("heartbeat: %+v", hb)
	}
	for _, dir := range []string{stale, staleCtl} {
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Fatalf("stale dir %s not removed (err %v)", dir, err)
		}
	}
	if _, err := os.Stat(keep); err != nil {
		t.Fatalf("unrelated dir must be left alone: %v", err)
	}

	// 启用后领两条（并发数 2），第三条留在队列里，占满期间不再发领取请求。
	waitFor(t, "two running jobs", func() bool { return r.running() == 2 })
	eng.mu.Lock()
	claims := eng.claims
	eng.mu.Unlock()
	time.Sleep(150 * time.Millisecond)
	eng.mu.Lock()
	if len(eng.tasks) != 1 || eng.claims != claims {
		t.Fatalf("queue %d claims %d→%d: runner must not claim beyond its capacity", len(eng.tasks), claims, eng.claims)
	}
	last := eng.heartbeats[len(eng.heartbeats)-1]
	eng.cancel = true // 取消在跑的两条，腾出并发位
	eng.mu.Unlock()
	if last.Running != 2 {
		t.Fatalf("heartbeat running = %d, want 2", last.Running)
	}
	waitFor(t, "third task to be claimed and all jobs to finish", func() bool {
		eng.mu.Lock()
		defer eng.mu.Unlock()
		return len(eng.tasks) == 0 && len(eng.finishes) == 3
	})
	stop()
	if r.running() != 0 {
		t.Fatalf("%d jobs still registered after stop", r.running())
	}
}

// 停止：等在途作业自行结束（不取消）。
func TestRunnerStopDrainsRunningJobs(t *testing.T) {
	g := newGitFixture(t, "")
	cfg := testConfig(t)
	cfg.Capacity = 1 // 在途作业占满并发位：排队的第二条只可能在它结束之后被领取
	eng := newFakeEngine()
	eng.tasks = []*jobcontract.Task{taskN(g, "slow.yml", 1)}
	r := newRunner(cfg, eng, testLogger(nil))
	stop := startRunner(t, r)
	waitFor(t, "job to start", func() bool { return strings.Contains(eng.log(), "slow line 0") })
	eng.set(func(e *fakeEngine) { e.tasks = []*jobcontract.Task{taskN(g, "site.yml", 2)} })
	stop()
	fin := eng.finish(t)
	if fin.Outcome != jobcontract.OutcomeExited || fin.ExitCode != 0 || !strings.Contains(eng.log(), "slow line 29") {
		t.Fatalf("the running job must be allowed to finish: %+v", fin)
	}
	// 停止之后不再领取。
	eng.mu.Lock()
	defer eng.mu.Unlock()
	if len(eng.tasks) != 1 || len(eng.finishes) != 1 {
		t.Fatalf("runner claimed new work while stopping: queue %d finishes %d", len(eng.tasks), len(eng.finishes))
	}
}

// 停止：等待期过后取消在途作业，以「已取消」结束并注明节点已停止。
func TestRunnerStopCancelsAfterDrainTimeout(t *testing.T) {
	g := newGitFixture(t, "")
	cfg := testConfig(t)
	cfg.DrainTimeout = 200 * time.Millisecond
	eng := newFakeEngine()
	eng.tasks = []*jobcontract.Task{taskN(g, "sleep.yml", 1)}
	r := newRunner(cfg, eng, testLogger(nil))
	stop := startRunner(t, r)
	waitFor(t, "job to start", func() bool { return strings.Contains(eng.log(), "STARTED") })
	begin := time.Now()
	stop()
	if d := time.Since(begin); d < 200*time.Millisecond || d > 20*time.Second {
		t.Fatalf("stop took %s", d)
	}
	fin := eng.finish(t)
	if fin.Outcome != jobcontract.OutcomeCanceled || !strings.Contains(fin.Error, "执行节点已停止") {
		t.Fatalf("finish: %+v", fin)
	}
	if !strings.Contains(eng.log(), "GOT-TERM") {
		t.Fatalf("log:\n%s", eng.log())
	}
}

// 停止：引擎不可达、作业收尾不了时，硬停止不会永远卡住。
func TestRunnerStopForcesWhenEngineIsDown(t *testing.T) {
	g := newGitFixture(t, "")
	cfg := testConfig(t)
	cfg.DrainTimeout = 100 * time.Millisecond
	cfg.CancelGrace = 100 * time.Millisecond
	eng := newFakeEngine()
	eng.tasks = []*jobcontract.Task{taskN(g, "sleep.yml", 1)}
	r := newRunner(cfg, eng, testLogger(nil))
	started := make(chan error, 1)
	go func() { started <- r.Start(context.Background()) }()
	waitFor(t, "job to start", func() bool { return strings.Contains(eng.log(), "STARTED") })
	eng.set(func(e *fakeEngine) { e.reportDown = true })
	// Stop 的 ctx 到期即硬停止（不等 cancel_grace + 30s）。
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	begin := time.Now()
	if err := r.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-started:
		if err != nil {
			t.Fatalf("start returned %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Start did not return")
	}
	if d := time.Since(begin); d > 15*time.Second {
		t.Fatalf("forced stop took %s", d)
	}
	if _, err := os.Stat(filepath.Join(cfg.WorkDir, taskN(g, "sleep.yml", 1).ExecutionID)); !os.IsNotExist(err) {
		t.Fatalf("job dir still exists after forced stop (err %v)", err)
	}
	if out, _ := exec.Command("pgrep", "-f", cfg.AnsiblePlaybook).Output(); strings.TrimSpace(string(out)) != "" {
		t.Fatalf("fake ansible still running: %s", out)
	}
}

// ---------------------------------------------------------------------------
// 端到端：真实的引擎处理器（service + biz + 内存仓储）+ HTTP 客户端 + 执行节点
// ---------------------------------------------------------------------------

type e2e struct {
	t     *testing.T
	store *biztest.Store
	house *biz.HousekeepUsecase
	cfg   *Config
	git   *gitFixture
	logs  *syncBuffer
}

func newE2E(t *testing.T) *e2e {
	t.Helper()
	e := &e2e{t: t, store: biztest.NewStore(), git: newGitFixture(t, "Basic "+basicAuth("deploy", "git-TOKEN-e2e")), logs: &syncBuffer{}}
	logger := slog.New(slog.NewTextHandler(e.logs, nil))
	obs := biztest.NewObserver()
	uc := biz.NewRunnerUsecase(e.store, e.store, e.store, e.store, e.store, &biztest.Opener{}, obs, logger)
	e.house = biz.NewHousekeepUsecase(e.store, obs, logger)
	mux := http.NewServeMux()
	for path, h := range service.NewRunnerService(uc, logger).Routes() {
		mux.HandleFunc(path, h)
	}
	srv := httptest.NewServer(httpx.LimitRequestBody(mux))
	t.Cleanup(srv.Close)

	runnerID, err := idgen.NewID()
	if err != nil {
		t.Fatal(err)
	}
	token, hash, err := jobcontract.NewRunnerToken(runnerID)
	if err != nil {
		t.Fatal(err)
	}
	e.store.AddRunner(biz.Runner{ID: runnerID, Zone: "dmz", Enabled: true, TokenHash: hash})
	e.cfg = testConfig(t)
	e.cfg.EngineURL, e.cfg.Token = srv.URL, token
	e.store.AddCredential(e2eCredID, biz.Credential{
		Type: jobcontract.CredentialSSHPassword, Username: "ops", Secret: biztest.Seal("login-PASSWORD-e2e"),
		BecomeMethod: jobcontract.BecomeSudo, BecomeSecret: biztest.Seal("become-PASSWORD-e2e"),
	})
	e.store.AddProject(e2eProjID, biz.Project{AuthType: jobcontract.GitAuthToken, Username: "deploy", Secret: biztest.Seal("git-TOKEN-e2e")})
	return e
}

const (
	e2eCredID = "018f0000-0000-7000-8000-0000000000c1"
	e2eProjID = "018f0000-0000-7000-8000-0000000000d1"
)

// submit 建一条排队中的执行（相当于 core 建单），返回 ID。
func (e *e2e) submit(playbook string) string {
	e.t.Helper()
	id, err := idgen.NewID()
	if err != nil {
		e.t.Fatal(err)
	}
	spec, err := json.Marshal(jobcontract.Spec{
		ProjectID: e2eProjID, RepoURL: e.git.URL, Ref: "main", Playbook: playbook, CredentialID: e2eCredID,
		ExtraVars: map[string]any{"version": "1.2.3", "replicas": 3, "note": "{{ lookup('pipe', 'id') }}"},
		Options:   jobcontract.Options{Forks: 5, TimeoutS: 600, Port: 22, Become: true},
	})
	if err != nil {
		e.t.Fatal(err)
	}
	e.store.AddExecution(biztest.Execution{
		ID: id, TemplateID: id, RunnerZone: "dmz", Spec: spec,
		Hosts: []biztest.HostRow{
			{Host: biz.Host{Address: "10.0.0.1", Port: 22, CIID: e2eCredID, Name: "web-1", Env: "prod"}},
			{Host: biz.Host{Address: "10.0.0.2", Port: 22, CIID: e2eProjID, Name: "web-2", Env: "prod"}},
		},
	})
	return id
}

func (e *e2e) start() (stop func()) {
	e.t.Helper()
	r, err := New(e.cfg, testLogger(e.logs))
	if err != nil {
		e.t.Fatal(err)
	}
	return startRunner(e.t, r)
}

// wait 等执行到达终态（结果未知不算：节点恢复上报后它还会变）。
func (e *e2e) wait(id string) *biztest.Execution {
	e.t.Helper()
	waitFor(e.t, "execution to finish", func() bool {
		s := e.store.Get(id).Status
		return !jobcontract.Active(s) && s != jobcontract.StatusUnknown
	})
	return e.store.Get(id)
}

func (e *e2e) log(x *biztest.Execution) string {
	var b strings.Builder
	for seq := int64(1); seq <= x.LogSeq; seq++ {
		b.WriteString(x.Logs[seq])
	}
	return b.String()
}

func TestEndToEnd(t *testing.T) {
	t.Setenv("KRATOS_RUNNER_TOKEN", "env-token-must-not-leak")
	e := newE2E(t)
	okID := e.submit("site.yml")
	failID := e.submit("fail.yml")
	e.start()

	ok := e.wait(okID)
	out := e.log(ok)
	if ok.Status != jobcontract.StatusSuccess || ok.Error != "" || ok.CommitID != e.git.Commit {
		t.Fatalf("execution: status %d error %q commit %q\n%s", ok.Status, ok.Error, ok.CommitID, out)
	}
	if ok.HostOK != 2 || ok.HostFailed != 0 || ok.Hosts[0].Status != jobcontract.HostChanged || ok.Hosts[1].Status != jobcontract.HostOK ||
		ok.Hosts[0].OK != 3 || ok.Hosts[0].Skipped != 2 {
		t.Fatalf("host results: ok=%d failed=%d %+v", ok.HostOK, ok.HostFailed, ok.Hosts)
	}
	for seq := int64(1); seq <= ok.LogSeq; seq++ {
		if _, has := ok.Logs[seq]; !has {
			t.Fatalf("log chunk %d missing", seq)
		}
	}
	for _, want := range []string{
		"[runner] 代码已就绪，提交 " + e.git.Commit,
		`"cmdb_env": !unsafe "prod"`,
		// 整数原样送达；含模板语法的取值被标为不可信。
		`"replicas": 3,`, `"note": !unsafe "{{ lookup('pipe', 'id') }}"`,
		`"ansible_password": !unsafe "********"`,
		"PLAY RECAP",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("log lacks %q:\n%s", want, out)
		}
	}
	fail := e.wait(failID)
	if fail.Status != jobcontract.StatusPartial || fail.HostOK != 1 || fail.HostFailed != 1 || !strings.Contains(fail.Error, "退出码 2") {
		t.Fatalf("failed execution: status %d ok=%d failed=%d error %q", fail.Status, fail.HostOK, fail.HostFailed, fail.Error)
	}
	// 凭据、接入令牌、领取令牌不出现在执行日志与两端的进程日志里。
	all := out + e.log(fail) + e.logs.String()
	for _, leak := range []string{
		"login-PASSWORD-e2e", "become-PASSWORD-e2e", "git-TOKEN-e2e", basicAuth("deploy", "git-TOKEN-e2e"),
		"env-token-must-not-leak", e.cfg.Token, strings.SplitN(e.cfg.Token, ".", 2)[1], ok.LeaseToken,
	} {
		if strings.Contains(all, leak) {
			t.Fatalf("%q leaked", leak)
		}
	}
	// 节点心跳已落到引擎侧。
	for id, hb := range e.store.Heartbeats {
		if hb.Capacity != 2 || hb.AgentVersion != "test" || e.store.LastSeen[id].IsZero() {
			t.Fatalf("heartbeat: %+v", hb)
		}
	}
	if len(e.store.Heartbeats) != 1 {
		t.Fatal("no heartbeat recorded")
	}
}

func TestEndToEndCancel(t *testing.T) {
	e := newE2E(t)
	id := e.submit("sleep.yml")
	e.start()
	waitFor(t, "playbook to start", func() bool { return strings.Contains(e.log(e.store.Get(id)), "STARTED") })
	// core 请求取消：节点在下一次上报时得知。
	e.store.Update(id, func(x *biztest.Execution) { x.CancelRequested = true })
	x := e.wait(id)
	if x.Status != jobcontract.StatusCanceled || !strings.Contains(e.log(x), "GOT-TERM") {
		t.Fatalf("status %d\n%s", x.Status, e.log(x))
	}
}

// 引擎判定节点失联（租约到期 → 结果未知）之后节点恢复上报：执行回到执行中并正常结束。
func TestEndToEndReviveAfterLeaseExpiry(t *testing.T) {
	e := newE2E(t)
	id := e.submit("slow.yml")
	e.start()
	waitFor(t, "playbook to start", func() bool { return strings.Contains(e.log(e.store.Get(id)), "slow line 0") })
	e.store.Advance(5 * time.Minute)
	if n, err := e.house.SweepLeases(context.Background()); err != nil || n != 1 {
		t.Fatalf("sweep = %d, %v", n, err)
	}
	x := e.wait(id)
	if x.Status != jobcontract.StatusSuccess || x.Error != "" || !strings.Contains(e.log(x), "slow line 29") {
		t.Fatalf("status %d error %q\n%s", x.Status, x.Error, e.log(x))
	}
}

// 执行在引擎侧被作废（这里模拟为换了领取令牌）：节点收到 409 后终止作业并丢弃。
func TestEndToEndLeaseLost(t *testing.T) {
	e := newE2E(t)
	id := e.submit("sleep.yml")
	e.start()
	waitFor(t, "playbook to start", func() bool { return strings.Contains(e.log(e.store.Get(id)), "STARTED") })
	e.store.Update(id, func(x *biztest.Execution) { x.LeaseToken = strings.Repeat("f", 64) })
	waitFor(t, "runner to kill the job", func() bool {
		out, _ := exec.Command("pgrep", "-f", e.cfg.AnsiblePlaybook).Output()
		return strings.TrimSpace(string(out)) == ""
	})
	waitFor(t, "runner to log the discard", func() bool { return strings.Contains(e.logs.String(), "job discarded") })
	if x := e.store.Get(id); x.Status != jobcontract.StatusRunning {
		t.Fatalf("a fenced-off runner must not change the execution: status %d", x.Status)
	}
}

// 接入令牌不对：节点不领取，也不崩溃。
func TestEndToEndBadToken(t *testing.T) {
	e := newE2E(t)
	id := e.submit("site.yml")
	bad, _, _ := jobcontract.NewRunnerToken("018f0000-0000-7000-8000-0000000000ee")
	e.cfg.Token = bad
	stop := e.start()
	waitFor(t, "heartbeat failure to be logged", func() bool { return strings.Contains(e.logs.String(), "heartbeat failed") })
	time.Sleep(100 * time.Millisecond)
	stop()
	if x := e.store.Get(id); x.Status != jobcontract.StatusQueued {
		t.Fatalf("status %d, want queued", x.Status)
	}
	if strings.Contains(e.logs.String(), bad) {
		t.Fatal("token leaked into logs")
	}
}

// 引擎一直不确认日志（不应出现）：节点按节拍重发，不原地空转。
func TestJobDoesNotSpinWhenEngineNeverAcks(t *testing.T) {
	g := newGitFixture(t, "")
	eng := &stuckEngine{fakeEngine: newFakeEngine()}
	j := newTestJob(testConfig(t), eng, nil, gitTask(g, "site.yml"))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); j.run(ctx) }()
	time.Sleep(time.Second)
	cancel() // 硬停止：放弃等待
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("job did not return after a hard stop")
	}
	// 节拍 20ms：一秒内至多几十次上报，而不是成千上万次。
	if n := eng.calls.Load(); n == 0 || n > 200 {
		t.Fatalf("%d reports in one second", n)
	}
}

// stuckEngine 收下上报但从不确认任何日志块。
type stuckEngine struct {
	*fakeEngine
	calls atomic.Int64
}

func (e *stuckEngine) Report(context.Context, *jobcontract.ReportRequest) (*jobcontract.ReportResponse, error) {
	e.calls.Add(1)
	return &jobcontract.ReportResponse{}, nil
}
