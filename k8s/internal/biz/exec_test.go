package biz

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// fakeShell 回显 shell：把输入原样写回；收到 "exit\n" 结束。
type fakeShell struct {
	mu      sync.Mutex
	opened  int
	running int // 已进入 Run 的会话数
	err     error
	out     io.Writer
}

func (f *fakeShell) Open(context.Context, *Cluster, Identity, k8scontract.ExecParams) (ShellConn, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.mu.Lock()
	f.opened++
	f.mu.Unlock()
	return f, nil
}

func (f *fakeShell) Run(ctx context.Context, sio ShellIO) error {
	f.mu.Lock()
	f.out = sio.Stdout
	f.running++
	f.mu.Unlock()
	lines := make(chan string)
	go func() {
		buf := make([]byte, 256)
		for {
			n, err := sio.Stdin.Read(buf)
			if n > 0 {
				select {
				case lines <- string(buf[:n]):
				case <-ctx.Done():
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case l := <-lines:
			if l == "exit\n" {
				return nil
			}
			_, _ = sio.Stdout.Write([]byte(l))
		}
	}
}

// emitDetached 模拟容器在客户端断开期间继续输出（写给最近一个已开始运行的会话）。
func (f *fakeShell) emitDetached(s string) {
	for range 200 {
		f.mu.Lock()
		ok := f.running == f.opened
		f.mu.Unlock()
		if ok {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	f.mu.Lock()
	w := f.out
	f.mu.Unlock()
	_, _ = w.Write([]byte(s))
}

type memExec struct {
	mu       sync.Mutex
	started  []string
	finished map[string]string
	in, out  int64

	chunks    [][]byte
	seqs      []int
	truncated bool
}

func (m *memExec) Start(_ context.Context, s *ExecSession) error {
	m.mu.Lock()
	m.started = append(m.started, s.ID)
	m.mu.Unlock()
	return nil
}

func (m *memExec) Record(_ context.Context, _ string, seq int, data []byte, truncated bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(data) > 0 {
		m.chunks = append(m.chunks, data)
	}
	m.seqs = append(m.seqs, seq)
	m.truncated = m.truncated || truncated
	return nil
}

func (m *memExec) Finish(_ context.Context, id, reason string, in, out int64) error {
	m.mu.Lock()
	if m.finished == nil {
		m.finished = map[string]string{}
	}
	m.finished[id], m.in, m.out = reason, in, out
	m.mu.Unlock()
	return nil
}

func (m *memExec) reason(id string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.finished[id]
}

// client 收集一次接入收到的帧。
type client struct {
	mu     sync.Mutex
	out    strings.Builder
	status []k8scontract.ExecStatus
	broken bool
}

func (c *client) sink(typ byte, p []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.broken {
		return io.ErrClosedPipe
	}
	switch typ {
	case k8scontract.FrameOutput:
		c.out.Write(p)
	case k8scontract.FrameStatus:
		var st k8scontract.ExecStatus
		_ = json.Unmarshal(p, &st)
		c.status = append(c.status, st)
	}
	return nil
}

func (c *client) output() string { c.mu.Lock(); defer c.mu.Unlock(); return c.out.String() }
func (c *client) last() k8scontract.ExecStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.status) == 0 {
		return k8scontract.ExecStatus{}
	}
	return c.status[len(c.status)-1]
}

type fixedProbe struct{}

func (fixedProbe) Probe(context.Context, string) (ProbeInfo, error) {
	return ProbeInfo{Version: "v1.35.0"}, nil
}

func (fixedProbe) SelfCheck(context.Context, string) ([]k8scontract.SelfCheck, error) {
	return nil, nil
}

type nopRuntime struct{}

func (nopRuntime) SaveProbe(context.Context, string, ProbeRecord) (int32, time.Time, error) {
	return k8scontract.ClusterOnline, time.Now(), nil
}
func (nopRuntime) SaveScope(context.Context, string, string, int64, string) error { return nil }
func (nopRuntime) Enqueue(context.Context, OutboxEvent) error                     { return nil }

type staticClusters struct{ list []*Cluster }

func (s staticClusters) ListEnabled(context.Context) ([]*Cluster, error) { return s.list, nil }
func (staticClusters) Versions(context.Context) (map[string]int64, error) {
	return map[string]int64{}, nil
}
func (staticClusters) Scopes(context.Context, string) ([]Scope, error) { return nil, nil }

type ownAll struct{ ids []string }

func (o ownAll) Renew(context.Context, string, time.Duration) ([]string, error) { return o.ids, nil }

func (ownAll) Claim(context.Context, string, time.Duration, int) ([]string, error) { return nil, nil }
func (ownAll) Release(context.Context, string) error                               { return nil }
func (ownAll) Heartbeat(context.Context, string, string, int) error                { return nil }
func (ownAll) Remove(context.Context, string) error                                { return nil }

type nopConn struct{}

func (nopConn) Connect(context.Context, *Cluster) error { return nil }
func (nopConn) Disconnect(string)                       {}

func newExecEnv(t *testing.T, clusters ...*Cluster) (*ExecManager, *fakeShell, *memExec, *memAudit, *Registry) {
	t.Helper()
	ids := make([]string, len(clusters))
	for i, c := range clusters {
		ids[i] = c.ID
	}
	own := ownAll{ids: ids}
	reg := NewRegistry(own, own, staticClusters{list: clusters}, nopConn{}, quiet)
	reg.WithOptions(RegistryOptions{InstanceID: "i1"})
	if _, err := reg.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	prober := NewProber(reg, fixedProbe{}, nopRuntime{}, nopRuntime{}, quiet)
	if _, err := prober.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	shell, repo, audits := &fakeShell{}, &memExec{}, &memAudit{}
	m := NewExecManager(reg, prober, nil, shell, repo, NewAuditor(audits, &obs{}, quiet), &obs{}, quiet)
	m.WithLimits(ExecLimits{DetachGrace: 300 * time.Millisecond, IdleTimeout: time.Hour, MaxDuration: time.Hour, Tick: 20 * time.Millisecond})
	t.Cleanup(func() { m.Shutdown(context.Background()) })
	return m, shell, repo, audits, reg
}

func waitFor(t *testing.T, what string, fn func() bool) {
	t.Helper()
	for range 200 {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestExecDetachResume(t *testing.T) {
	t.Parallel()
	c1 := &Cluster{ID: "c1", Name: "c1"}
	m, shell, repo, audits, _ := newExecEnv(t, c1)
	alice, bob := Identity{UserID: "alice"}, Identity{UserID: "bob"}
	params := k8scontract.ExecParams{Namespace: "ns", Pod: "p"}

	first := &client{}
	h, err := m.Attach(context.Background(), "c1", alice, params, first.sink)
	if err != nil {
		t.Fatal(err)
	}
	if st := first.last(); st.State != k8scontract.ExecReady || st.SessionID != h.SessionID() {
		t.Fatalf("ready status: %+v", st)
	}
	if err := h.Input([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "echo", func() bool { return first.output() == "hello" })

	// 断线：期间的输出进环形缓冲。
	h.Detach()
	shell.emitDetached("while-away")

	// 别人、别的集群都接不回来，且应答与「不存在」相同。
	for _, try := range []struct {
		cluster string
		id      Identity
	}{{"c1", bob}, {"c2", alice}} {
		p := params
		p.SessionID = h.SessionID()
		if _, err := m.Attach(context.Background(), try.cluster, try.id, p, (&client{}).sink); AsError(err).Code != 404 {
			t.Fatalf("resume by %s on %s: %v", try.id.UserID, try.cluster, err)
		}
	}

	second := &client{}
	p := params
	p.SessionID = h.SessionID()
	h2, err := m.Attach(context.Background(), "c1", alice, p, second.sink)
	if err != nil {
		t.Fatal(err)
	}
	if st := second.last(); st.State != k8scontract.ExecResumed {
		t.Fatalf("resumed status: %+v", st)
	}
	if second.output() != "while-away" {
		t.Fatalf("replay = %q", second.output())
	}
	// 旧接入迟到的 Detach 不得把新接入踢掉。
	h.Detach()
	_ = h2.Input([]byte("again"))
	waitFor(t, "echo after resume", func() bool { return second.output() == "while-awayagain" })
	if first.output() != "hello" {
		t.Fatalf("old attachment must not receive new output: %q", first.output())
	}

	_ = h2.Input([]byte("exit\n"))
	waitFor(t, "session end", func() bool { return repo.reason(h.SessionID()) != "" })
	if repo.reason(h.SessionID()) != k8scontract.ExecEndContainerExit {
		t.Fatalf("end reason = %s", repo.reason(h.SessionID()))
	}
	if st := second.last(); st.State != k8scontract.ExecClosed || st.Reason != k8scontract.ExecEndContainerExit {
		t.Fatalf("closed status: %+v", st)
	}
	if repo.in != int64(len("hello")+len("again")+len("exit\n")) || repo.out != int64(len("hello")+len("while-away")+len("again")) {
		t.Fatalf("byte counts in=%d out=%d", repo.in, repo.out)
	}
	if len(audits.rows) != 1 || audits.rows[0].Action != k8scontract.ActionExec || audits.rows[0].Result != k8scontract.AuditOK {
		t.Fatalf("exec audit: %+v", audits.rows)
	}
	// 已结束的会话接不回来。
	if _, err := m.Attach(context.Background(), "c1", alice, p, (&client{}).sink); AsError(err).Code != 404 {
		t.Fatalf("resume a finished session: %v", err)
	}
}

func TestExecLimits(t *testing.T) {
	t.Parallel()
	c1 := &Cluster{ID: "c1", Name: "c1"}
	locked := &Cluster{ID: "c2", Name: "c2", ExecDisabled: true}
	m, shell, repo, audits, reg := newExecEnv(t, c1, locked)
	alice := Identity{UserID: "alice"}
	params := k8scontract.ExecParams{Namespace: "ns", Pod: "p"}

	if _, err := m.Attach(context.Background(), "c2", alice, params, (&client{}).sink); AsError(err).Reason != k8scontract.ReasonExecDisabled {
		t.Fatalf("exec on a locked cluster: %v", err)
	}
	if _, err := m.Attach(context.Background(), "nope", alice, params, (&client{}).sink); AsError(err).Reason != k8scontract.ReasonClusterNotFound {
		t.Fatalf("unknown cluster: %v", err)
	}
	if _, err := m.Attach(context.Background(), "c1", alice, k8scontract.ExecParams{}, (&client{}).sink); AsError(err).Code != 400 {
		t.Fatalf("missing pod: %v", err)
	}

	// 断线超过保留期：会话结束。
	gone := &client{}
	h, err := m.Attach(context.Background(), "c1", alice, params, gone.sink)
	if err != nil {
		t.Fatal(err)
	}
	h.Detach()
	waitFor(t, "detach timeout", func() bool { return repo.reason(h.SessionID()) == k8scontract.ExecEndDetachTimeout })

	// 写出失败等同断线（连接已坏但读循环尚未发现）。
	flaky := &client{}
	h, err = m.Attach(context.Background(), "c1", alice, params, flaky.sink)
	if err != nil {
		t.Fatal(err)
	}
	flaky.mu.Lock()
	flaky.broken = true
	flaky.mu.Unlock()
	shell.emitDetached("x")
	waitFor(t, "broken sink treated as detach", func() bool { return repo.reason(h.SessionID()) == k8scontract.ExecEndDetachTimeout })

	// 并发上限：按用户计。
	var handles []*ExecHandle
	for range k8scontract.MaxExecSessions {
		h, err := m.Attach(context.Background(), "c1", alice, params, (&client{}).sink)
		if err != nil {
			t.Fatal(err)
		}
		handles = append(handles, h)
	}
	if _, err := m.Attach(context.Background(), "c1", alice, params, (&client{}).sink); AsError(err).Reason != k8scontract.ReasonTooManyStreams {
		t.Fatalf("session limit: %v", err)
	}
	if _, err := m.Attach(context.Background(), "c1", Identity{UserID: "bob"}, params, (&client{}).sink); err != nil {
		t.Fatalf("the limit is per user: %v", err)
	}
	handles[0].Close()
	waitFor(t, "client close", func() bool { return repo.reason(handles[0].SessionID()) == k8scontract.ExecEndClientClose })

	// 归属转移：其上的会话全部结束。
	for _, fn := range reg.onDrop {
		fn("c1")
	}
	waitFor(t, "sessions end with ownership", func() bool { return repo.reason(handles[1].SessionID()) == k8scontract.ExecEndShutdown })

	// 集群拒绝进入容器：记一条失败的审计，不产生会话。
	before := len(repo.started)
	shell.err = NewError(403, k8scontract.ReasonForbidden, "pods/exec is forbidden")
	if _, err := m.Attach(context.Background(), "c1", alice, params, (&client{}).sink); AsError(err).Code != 403 {
		t.Fatalf("refused exec: %v", err)
	}
	last := audits.rows[len(audits.rows)-1]
	if len(repo.started) != before || last.Result != k8scontract.AuditFailed || last.Action != k8scontract.ActionExec {
		t.Fatalf("refused exec must be audited as failed without a session: %+v", last)
	}
}
