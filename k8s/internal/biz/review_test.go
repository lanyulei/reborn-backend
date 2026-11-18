package biz

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// 命名空间管理员绑定平台维护的 reborn:ns-admin，而不是内置的 admin（它能自建绑定，授权收不回来）。
func TestNamespaceAdminBindsManagedRole(t *testing.T) {
	t.Parallel()
	want, _ := DesiredBindings([]Scope{
		{RoleID: "r1", Namespace: "app", Level: k8scontract.LevelAdmin},
		{RoleID: "r2", Namespace: "*", Level: k8scontract.LevelAdmin},
	}, []string{"app"})
	got := map[string]string{}
	for _, b := range want {
		got[b.Namespace+"|"+b.Name] = b.Role
	}
	if got["app|reborn:r1:admin"] != NamespaceAdminRole || got["|reborn:r2:admin"] != NamespaceAdminRole {
		t.Fatalf("bindings = %v", got)
	}
	for k, role := range got {
		if role == "admin" {
			t.Fatalf("binding %s still references the built-in admin role", k)
		}
	}
}

type flakyRBAC struct {
	mu      sync.Mutex
	fail    bool
	foreign []string
	applied int
}

func (f *flakyRBAC) EnsureClusterView(context.Context, string) error { return nil }
func (f *flakyRBAC) ListBindings(context.Context, string) ([]Binding, []string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return nil, nil, errors.New("apiserver down")
	}
	return nil, f.foreign, nil
}

func (f *flakyRBAC) ApplyBindings(context.Context, string, []Binding, []Binding) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied++
	return nil
}

type fixedNS struct{}

func (fixedNS) Namespaces(context.Context, string) ([]string, error) { return []string{"app"}, nil }

type scopeRuntime struct {
	nopRuntime
	mu      sync.Mutex
	version int64
	msg     string
}

func (s *scopeRuntime) SaveScope(_ context.Context, _, _ string, version int64, msg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.version, s.msg = version, msg
	return nil
}

// Ready：成功对账过才放行；配置版本变更后有宽限期；对账一直失败、过了宽限期即不再放行；
// 不归平台管理的绑定会写进 scope_error。
func TestScopeReadyTracksSuccessfulSync(t *testing.T) {
	t.Parallel()
	c := &Cluster{ID: "c1", Name: "c1"}
	own := ownAll{ids: []string{c.ID}}
	reg := NewRegistry(own, own, staticClusters{list: []*Cluster{c}}, nopConn{}, quiet)
	reg.WithOptions(RegistryOptions{InstanceID: "i1"})
	if _, err := reg.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	prober := NewProber(reg, fixedProbe{}, nopRuntime{}, nopRuntime{}, quiet)
	if _, err := prober.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	rbac, rt := &flakyRBAC{fail: true}, &scopeRuntime{}
	s := NewScopeSyncer(reg, prober, staticClusters{}, rbac, fixedNS{}, rt, quiet)
	now := time.Unix(1_800_000_000, 0)
	s.now = func() time.Time { return now }
	setVersion := func(v int64) {
		reg.mu.Lock()
		reg.versions = map[string]int64{k8scontract.CfgRoleScope: v}
		reg.mu.Unlock()
	}
	setVersion(1)

	_, _ = s.Run(context.Background())
	if s.Ready(c) {
		t.Fatal("a cluster whose first sync failed must not serve user requests")
	}
	if rt.msg == "" || rt.version != 0 {
		t.Fatalf("failure must be recorded: version %d msg %q", rt.version, rt.msg)
	}
	// 失败后退避：立刻再跑不会重试。
	rbac.mu.Lock()
	rbac.fail = false
	rbac.mu.Unlock()
	_, _ = s.Run(context.Background())
	if s.Ready(c) {
		t.Fatal("retry must wait for the backoff")
	}
	now = now.Add(scopeRetryMin + time.Second)
	_, _ = s.Run(context.Background())
	if !s.Ready(c) || rt.version != 1 || rt.msg != "" {
		t.Fatalf("after a successful sync: ready %v version %d msg %q", s.Ready(c), rt.version, rt.msg)
	}

	// 版本变更：宽限期内仍放行；对账持续失败、过了宽限期即拒绝。
	setVersion(2)
	rbac.mu.Lock()
	rbac.fail = true
	rbac.mu.Unlock()
	_, _ = s.Run(context.Background())
	if !s.Ready(c) {
		t.Fatal("within the grace period the previous bindings are still accepted")
	}
	now = now.Add(scopeGrace + time.Second)
	if s.Ready(c) {
		t.Fatal("past the grace period a cluster behind the configured scope version must be refused")
	}
	rbac.mu.Lock()
	rbac.fail, rbac.foreign = false, []string{"app/team-admins"}
	rbac.mu.Unlock()
	now = now.Add(scopeRetryMax)
	_, _ = s.Run(context.Background())
	if !s.Ready(c) || rt.version != 2 {
		t.Fatalf("recovered sync: ready %v version %d", s.Ready(c), rt.version)
	}
	if rt.msg == "" {
		t.Fatal("bindings not managed by the platform must be reported in scope_error")
	}
}

type changeRepoFake struct {
	mu       sync.Mutex
	started  int
	finished []int32
	messages []string
	results  []k8scontract.ChangeResult
	released int
	failN    int // Finish 前 N 次失败
}

func (r *changeRepoFake) Claim(context.Context, string, []string, time.Duration) (*Change, error) {
	return nil, nil
}

func (r *changeRepoFake) MarkStarted(context.Context, string, string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.started++
	return nil
}

func (r *changeRepoFake) Finish(_ context.Context, _, _ string, status int32, message string, result k8scontract.ChangeResult) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failN > 0 {
		r.failN--
		return errors.New("db down")
	}
	r.finished, r.messages, r.results = append(r.finished, status), append(r.messages, message), append(r.results, result)
	return nil
}

func (r *changeRepoFake) Release(context.Context, string, string, string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.released++
	return nil
}

type changeExecFake struct {
	start bool
	err   error
}

func (e changeExecFake) ExecuteChange(_ context.Context, _ *Cluster, _ Identity, _ int32, _ []byte, started func() error) (k8scontract.ChangeResult, []*AuditEvent, error) {
	if e.start {
		if err := started(); err != nil {
			return k8scontract.ChangeResult{}, nil, err
		}
	}
	return k8scontract.ChangeResult{}, nil, e.err
}

// 变更单执行：开始提交之前的暂时性失败放回队列；开始之后中断记为失败（不重跑）；结果回写失败会重试。
func TestChangeRunnerNeverRerunsStartedChanges(t *testing.T) {
	t.Parallel()
	c := &Cluster{ID: "c1", Name: "c1"}
	own := ownAll{ids: []string{c.ID}}
	reg := NewRegistry(own, own, staticClusters{list: []*Cluster{c}}, nopConn{}, quiet)
	reg.WithOptions(RegistryOptions{InstanceID: "i1"})
	run := func(exec ChangeExecutor, repo *changeRepoFake) {
		r := NewChangeRunner(reg, nil, nil, repo, exec, NewAuditor(&memAudit{}, &obs{}, quiet), quiet)
		r.execute(context.Background(), c, &Change{ID: "ch1", ClusterID: c.ID, Requester: k8scontract.ChangeRequester{UserID: "u"}})
	}

	// 尚未开始、集群不可用：放回队列。
	repo := &changeRepoFake{}
	run(changeExecFake{err: ErrClusterOffline("down")}, repo)
	if repo.released != 1 || len(repo.finished) != 0 {
		t.Fatalf("transient failure before start: released %d finished %v", repo.released, repo.finished)
	}
	// 已经开始、随后被取消（停机、超时）：记为失败并说明结果未知，绝不放回队列。
	repo = &changeRepoFake{}
	run(changeExecFake{start: true, err: context.Canceled}, repo)
	if repo.released != 0 || len(repo.finished) != 1 || repo.finished[0] != k8scontract.ChangeFailed {
		t.Fatalf("interrupted after start: released %d finished %v", repo.released, repo.finished)
	}
	if repo.messages[0] != changeInterruptedMessage || !repo.results[0].Started {
		t.Fatalf("interrupted change: message %q result %+v", repo.messages[0], repo.results[0])
	}
	// 已经开始、集群中途不可用：同样不重跑。
	repo = &changeRepoFake{}
	run(changeExecFake{start: true, err: ErrClusterOffline("down")}, repo)
	if repo.released != 0 || len(repo.finished) != 1 || repo.finished[0] != k8scontract.ChangeFailed {
		t.Fatalf("offline after start: released %d finished %v", repo.released, repo.finished)
	}
	// 成功。
	repo = &changeRepoFake{}
	run(changeExecFake{start: true}, repo)
	if len(repo.finished) != 1 || repo.finished[0] != k8scontract.ChangeSucceeded || repo.started != 1 {
		t.Fatalf("success: finished %v started %d", repo.finished, repo.started)
	}
}

// 证明「放回队列」只在尚未开始时发生（取消本身不再一律算暂时性失败）。
func TestCancelBeforeStartIsReleased(t *testing.T) {
	t.Parallel()
	c := &Cluster{ID: "c1", Name: "c1"}
	own := ownAll{ids: []string{c.ID}}
	reg := NewRegistry(own, own, staticClusters{list: []*Cluster{c}}, nopConn{}, quiet)
	reg.WithOptions(RegistryOptions{InstanceID: "i1"})
	repo := &changeRepoFake{}
	r := NewChangeRunner(reg, nil, nil, repo, changeExecFake{err: context.Canceled}, NewAuditor(&memAudit{}, &obs{}, quiet), quiet)
	r.execute(context.Background(), c, &Change{ID: "ch1", ClusterID: c.ID})
	if repo.released != 1 {
		t.Fatalf("a change cancelled before it touched the cluster is retried later: released %d finished %v", repo.released, repo.finished)
	}
}

// 终端：失败的接回不会踢掉仍连着的客户端。
func TestResumeFailureKeepsCurrentClient(t *testing.T) {
	t.Parallel()
	c := &Cluster{ID: "c1", Name: "c1"}
	m, _, _, _, _ := newExecEnv(t, c)
	id := Identity{UserID: "u1"}
	var mu sync.Mutex
	var got int
	good := func(byte, []byte) error { mu.Lock(); got++; mu.Unlock(); return nil }
	h, err := m.Attach(context.Background(), c.ID, id, k8scontract.ExecParams{Namespace: "ns", Pod: "p"}, good)
	if err != nil {
		t.Fatal(err)
	}
	bad := func(byte, []byte) error { return errors.New("broken pipe") }
	if _, err := m.Attach(context.Background(), c.ID, id, k8scontract.ExecParams{SessionID: h.SessionID()}, bad); err == nil {
		t.Fatal("resume over a broken connection must fail")
	}
	h.s.mu.Lock()
	still := h.s.sink != nil && h.s.attach == h.attach
	h.s.mu.Unlock()
	if !still {
		t.Fatal("the client that was still attached must stay attached after a failed resume")
	}
}
