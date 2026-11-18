package runner

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/cgi"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/job/internal/conf"
	"github.com/lanyulei/reborn/backend/kit/jobcontract"
)

// confRunner 让表驱动的配置用例写得短一些。
type confRunner = conf.Runner

// fakeAnsible 冒充 ansible-playbook 的脚本：回显参数、环境、清单与变量文件（用来断言掩码与隔离），
// 行为由 Playbook 文件名决定。
const fakeAnsible = `#!/bin/sh
playbook="$3"
echo "ARGS: $*"
echo "CWD: $(pwd)"
echo "ENV-BEGIN"
env | sort
echo "ENV-END"
echo "INVENTORY: $(cat "$2")"
for a in "$@"; do
  case "$a" in
    @*) echo "FILE $(basename "${a#@}"): $(cat "${a#@}")" ;;
  esac
done
echo "MODES: $(ls -ld "$HOME" | cut -c1-10) $(ls -l "$HOME"/*.json | cut -c1-10 | sort -u | tr '\n' ' ')"
echo "KEYFILES: [$(grep -rl 'PRIVATE KEY' "$HOME" 2>/dev/null | tr '\n' ' ')]"
if [ -n "$SSH_AUTH_SOCK" ]; then
  echo "AGENT-KEYS: $(ssh-add -l 2>&1)"
fi
# 快速执行：平台生成的 Playbook（绝对路径）——回显它与作业目录里的文件，供测试断言。
case "$playbook" in
  /*/playbook.yml)
    echo "PLAYBOOK: $(cat "$playbook")"
    echo "JOBFILES: $(cd "$HOME" && find . -type f \( -name script -o -path './payload/*' -o -name playbook.yml \) -exec ls -l {} \; | awk '{print $1":"$NF}' | sort | tr '\n' ' ')"
    [ -f "$HOME/script" ] && printf 'SCRIPT-HEX: %s\n' "$(od -An -c "$HOME/script" | tr -s ' \n' ' ')"
    [ -d "$HOME/payload" ] && echo "PAYLOAD-SHA: $(cat "$HOME"/payload/* | { sha256sum 2>/dev/null || shasum -a 256; } | cut -d' ' -f1)"
    ;;
esac
# 滚动执行：--limit @文件 —— 只对其中的主机出统计。主机名含 bad 的失败、含 gone 的不可达。
limit=""
prev=""
for a in "$@"; do
  [ "$prev" = "--limit" ] && limit="${a#@}"
  prev="$a"
done
if [ -n "$limit" ]; then
  echo "BATCH-START: $(tr '\n' ' ' < "$limit")"
  case "$playbook" in
    rollslow.yml) sleep 0.5 ;;
    rollbroken.yml) echo "ERROR! the playbook is broken" >&2; exit 4 ;;
  esac
  code=0
  echo ""
  echo "PLAY RECAP *********************************************************************"
  while read -r h; do
    case "$h" in
      *bad*) echo "$h : ok=1 changed=0 unreachable=0 failed=1 skipped=0 rescued=0 ignored=0"; code=2 ;;
      *gone*) echo "$h : ok=0 changed=0 unreachable=1 failed=0 skipped=0 rescued=0 ignored=0"; [ $code -eq 0 ] && code=4 ;;
      *) echo "$h : ok=2 changed=1 unreachable=0 failed=0 skipped=0 rescued=0 ignored=0" ;;
    esac
  done < "$limit"
  echo ""
  [ "$playbook" = "rollzero.yml" ] && exit 0
  exit $code
fi
recap() {
  echo ""
  echo "PLAY RECAP *********************************************************************"
  echo "10.0.0.1                   : ok=3    changed=1    unreachable=0    failed=$1    skipped=2    rescued=0    ignored=0   "
  echo "10.0.0.2                   : ok=2    changed=0    unreachable=0    failed=0    skipped=0    rescued=0    ignored=0   "
  echo ""
}
case "$playbook" in
  fail.yml)
    echo "fatal: [10.0.0.1]: FAILED! => {\"msg\": \"boom\"}" >&2
    recap 1
    exit 2 ;;
  sleep.yml)
    trap 'echo "GOT-TERM"; exit 143' TERM
    echo "STARTED"
    sleep 300 &
    wait $! ;;
  stubborn.yml)
    trap '' TERM
    echo "STARTED"
    while :; do sleep 1; done ;;
  flood.yml)
    i=0
    while [ $i -lt 4000 ]; do
      echo "flood line $i ................................................................"
      i=$((i+1))
    done
    recap 0 ;;
  slow.yml)
    i=0
    while [ $i -lt 30 ]; do
      echo "slow line $i"
      i=$((i+1))
      sleep 0.05
    done
    recap 0 ;;
  *)
    echo "TASK [ok] ***"
    echo "ok: [10.0.0.1]"
    recap 0 ;;
esac
`

// gitFixture 一个经 HTTP（smart 协议，git-http-backend）提供的测试仓库。
type gitFixture struct {
	URL    string
	Commit string
	// mu 保护 authSeen。
	mu       sync.Mutex
	authSeen []string
}

func (g *gitFixture) auths() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.authSeen...)
}

func runGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1",
		"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

// newGitFixture 建仓库并起 HTTP 服务。wantAuth 非空时要求请求带这个 Authorization 头，否则 401。
// 仓库里有一个 ansible.cfg（平台应让它不生效）和一个指向仓库之外的符号链接 Playbook。
func newGitFixture(t *testing.T, wantAuth string) *gitFixture {
	t.Helper()
	backend := filepath.Join(runGit(t, "", "--exec-path"), "git-http-backend")
	if _, err := os.Stat(backend); err != nil {
		t.Skipf("git-http-backend not available: %v", err)
	}
	root := t.TempDir()
	src := filepath.Join(root, "src")
	if err := os.Mkdir(src, 0o700); err != nil {
		t.Fatal(err)
	}
	runGit(t, src, "init", "-q", "-b", "main")
	for _, name := range []string{
		"site.yml", "fail.yml", "sleep.yml", "stubborn.yml", "flood.yml", "slow.yml",
		"roll.yml", "rollslow.yml", "rollbroken.yml", "rollzero.yml",
	} {
		if err := os.WriteFile(filepath.Join(src, name), []byte("- hosts: all\n  tasks: []\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(src, "ansible.cfg"), []byte("[defaults]\nhost_key_checking = False\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/etc/hosts", filepath.Join(src, "escape.yml")); err != nil {
		t.Fatal(err)
	}
	runGit(t, src, "add", "-A")
	runGit(t, src, "commit", "-q", "-m", "init")
	g := &gitFixture{Commit: runGit(t, src, "rev-parse", "HEAD")}
	runGit(t, root, "clone", "-q", "--bare", src, filepath.Join(root, "repo.git"))

	h := &cgi.Handler{
		Path: backend,
		Env:  []string{"GIT_PROJECT_ROOT=" + root, "GIT_HTTP_EXPORT_ALL=1"},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		g.mu.Lock()
		g.authSeen = append(g.authSeen, r.Header.Get("Authorization"))
		g.mu.Unlock()
		if wantAuth != "" && r.Header.Get("Authorization") != wantAuth {
			w.Header().Set("WWW-Authenticate", `Basic realm="git"`)
			http.Error(w, "authentication required", http.StatusUnauthorized)
			return
		}
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	g.URL = srv.URL + "/repo.git"
	return g
}

// testConfig 节点配置：短路径的工作目录（其下要建 UNIX 套接字）、假的 ansible-playbook、真实的
// git / ssh-agent / ssh-add。
func testConfig(t *testing.T) *Config {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "rj")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	script := filepath.Join(dir, "fake-ansible-playbook")
	if err := os.WriteFile(script, []byte(fakeAnsible), 0o755); err != nil { //nolint:gosec // 测试脚本需要可执行
		t.Fatal(err)
	}
	token, _, err := jobcontract.NewRunnerToken("018f0000-0000-7000-8000-0000000000aa")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := NewConfig(&conf.Runner{
		EngineUrl: "http://127.0.0.1:1", Token: token, WorkDir: filepath.Join(dir, "w"), AnsiblePlaybook: script,
	}, "test")
	if err != nil {
		t.Fatal(err)
	}
	cfg.reportEvery = 100 * time.Millisecond
	cfg.CancelGrace = 500 * time.Millisecond
	cfg.PollInterval = 20 * time.Millisecond
	cfg.HeartbeatInterval = 30 * time.Millisecond
	cfg.DrainTimeout = 10 * time.Second
	return cfg
}

// syncBuffer 并发安全的日志缓冲。
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func testLogger(w io.Writer) *slog.Logger {
	if w == nil {
		w = io.Discard
	}
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// gitTask 指向测试仓库的任务（两台主机、密码凭据 + 提权密码）。
func gitTask(g *gitFixture, playbook string) *jobcontract.Task {
	return &jobcontract.Task{
		ExecutionID: "018f0000-0000-7000-8000-000000000001", LeaseToken: "lease-token-1", LeaseSeconds: 60,
		RepoURL: g.URL, Ref: "main", Playbook: playbook,
		Git: jobcontract.GitAuth{Type: jobcontract.GitAuthNone},
		Hosts: []jobcontract.TaskHost{
			{Address: "10.0.0.1", Port: 22, Vars: map[string]string{"cmdb_id": "ci-1", "cmdb_name": "web-1", "cmdb_env": "prod"}},
			{Address: "10.0.0.2", Port: 2222, Vars: map[string]string{"cmdb_id": "ci-2", "cmdb_name": "web-2", "cmdb_env": "prod"}},
		},
		Credential: jobcontract.TaskCredential{
			Type: jobcontract.CredentialSSHPassword, Username: "ops", Secret: "login-PASSWORD-1",
			BecomeMethod: jobcontract.BecomeSudo, BecomeSecret: "become-PASSWORD-2",
		},
		ExtraVars: map[string]any{"version": "1.2.3"},
		Options:   jobcontract.Options{Forks: 5, TimeoutS: 600, Port: 22, Become: true},
	}
}

// fakeEngine 进程内的假引擎：按协议语义收日志（连续序号、遇空洞即停），可注入故障。
type fakeEngine struct {
	mu          sync.Mutex
	chunks      map[int64]string
	ack         int64
	commit      string
	cancel      bool
	cancelOn    string  // 日志里出现这段文字后答复取消
	reportErrs  []error // 依次返回的上报错误（nil = 成功）；用完后一律成功
	reportDown  bool    // 为 true 时上报一律失败
	dropAfter   int     // > 0：每次上报只收前 dropAfter 块（模拟引擎只确认了一部分）
	finishErrs  []error // 依次返回的结束错误
	reports     int
	duplicates  int
	finishes    []*jobcontract.FinishRequest
	finished    chan struct{} // 首次成功的结束后关闭
	finishOnce  sync.Once
	heartbeats  []jobcontract.HeartbeatRequest
	enabled     bool
	tasks       []*jobcontract.Task
	claims      int
	claimErr    error
	afterFinish bool // 结束之后又收到了上报（协议违规）
}

func newFakeEngine() *fakeEngine {
	return &fakeEngine{chunks: map[int64]string{}, finished: make(chan struct{}), enabled: true}
}

var _ engine = (*fakeEngine)(nil)

func (e *fakeEngine) Heartbeat(_ context.Context, req *jobcontract.HeartbeatRequest) (*jobcontract.HeartbeatResponse, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.heartbeats = append(e.heartbeats, *req)
	return &jobcontract.HeartbeatResponse{Enabled: e.enabled}, nil
}

func (e *fakeEngine) Claim(context.Context) (*jobcontract.Task, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.claims++
	if e.claimErr != nil {
		return nil, e.claimErr
	}
	if len(e.tasks) == 0 {
		return nil, nil
	}
	t := e.tasks[0]
	e.tasks = e.tasks[1:]
	return t, nil
}

func (e *fakeEngine) Report(_ context.Context, req *jobcontract.ReportRequest) (*jobcontract.ReportResponse, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.reports++
	if len(e.finishes) > 0 {
		e.afterFinish = true
	}
	if e.reportDown {
		return nil, io.ErrUnexpectedEOF
	}
	if len(e.reportErrs) > 0 {
		err := e.reportErrs[0]
		e.reportErrs = e.reportErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	if len(req.Chunks) > jobcontract.MaxLogChunks {
		return nil, &statusError{code: http.StatusBadRequest}
	}
	for i, c := range req.Chunks {
		if len(c.Content) > jobcontract.MaxLogChunkBytes {
			return nil, &statusError{code: http.StatusBadRequest}
		}
		if e.dropAfter > 0 && i >= e.dropAfter {
			break
		}
		if c.Seq <= e.ack {
			e.duplicates++
			continue
		}
		if c.Seq != e.ack+1 {
			break
		}
		e.chunks[c.Seq], e.ack = c.Content, c.Seq
	}
	if req.CommitID != "" {
		e.commit = req.CommitID
	}
	if e.cancelOn != "" && strings.Contains(e.logLocked(), e.cancelOn) {
		e.cancel = true
	}
	return &jobcontract.ReportResponse{Cancel: e.cancel, AckSeq: e.ack}, nil
}

func (e *fakeEngine) Finish(_ context.Context, req *jobcontract.FinishRequest) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.finishErrs) > 0 {
		err := e.finishErrs[0]
		e.finishErrs = e.finishErrs[1:]
		if err != nil {
			return err
		}
	}
	e.finishes = append(e.finishes, req)
	e.finishOnce.Do(func() { close(e.finished) })
	return nil
}

func (e *fakeEngine) logLocked() string {
	seqs := make([]int64, 0, len(e.chunks))
	for s := range e.chunks {
		seqs = append(seqs, s)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	var b strings.Builder
	for _, s := range seqs {
		b.WriteString(e.chunks[s])
	}
	return b.String()
}

// log 已收到的全部日志（按序号拼接）。
func (e *fakeEngine) log() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.logLocked()
}

func (e *fakeEngine) set(fn func(e *fakeEngine)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	fn(e)
}

// finish 等到结束请求并返回它；超时即失败。
func (e *fakeEngine) finish(t *testing.T) *jobcontract.FinishRequest {
	t.Helper()
	select {
	case <-e.finished:
	case <-time.After(60 * time.Second):
		t.Fatalf("no finish request; log so far:\n%s", e.log())
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.finishes[0]
}

// waitFor 轮询直到条件成立。
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// newTestJob 构造一个节奏调快的作业。
func newTestJob(cfg *Config, eng engine, logs io.Writer, task *jobcontract.Task) *job {
	j, err := newJob(cfg, eng, testLogger(logs), task)
	if err != nil {
		panic(err)
	}
	j.flushEvery = 20 * time.Millisecond
	j.retryBase = 10 * time.Millisecond
	return j
}

// pidAlive 报告进程是否还在。
func pidAlive(pid int) bool {
	return exec.Command("kill", "-0", strconv.Itoa(pid)).Run() == nil
}
