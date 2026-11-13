package biz

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/cmdbcontract"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// ---- 桩 ----

type fakeStore struct {
	mu        sync.Mutex
	source    *Source
	task      *Task
	claimed   bool
	renews    int
	renewErr  error
	renewFail int             // 前 N 次续约返回瞬时错误
	active    map[string]bool // 类型 → 该来源下仍有未退役资源
	finished  []finishCall
	staging   map[string][]*Resource
	cleared   []string
	fields    map[string][]ModelField
	reconcile []ReconcileParams
	relations []string
	recErr    error
	counts    Counts
	samples   []cmdbcontract.RunError
}

type finishCall struct {
	status string
	counts Counts
	err    string
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		staging: map[string][]*Resource{},
		fields: map[string][]ModelField{
			cmdbcontract.ModelCloudHost:      {{Code: "instance_id", Owner: "both", Unique: true}, {Code: "private_ip", Owner: "both"}, {Code: "owner_note", Owner: "manual"}},
			cmdbcontract.ModelPhysicalServer: {{Code: "ip", Owner: "both", Unique: true}},
			cmdbcontract.ModelVirtualMachine: {{Code: "ip", Owner: "both", Unique: true}},
		},
	}
}

func (f *fakeStore) Get(_ context.Context, id string) (*Source, error) {
	if f.source == nil || f.source.ID != id {
		return nil, errors.New("source not found")
	}
	return f.source, nil
}

func (f *fakeStore) Claim(_ context.Context, token string, _ time.Duration) (*Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.task == nil || f.claimed {
		return nil, nil
	}
	f.claimed = true
	t := *f.task
	t.Token, t.Attempts, t.RunID = token, 1, "run-1"
	return &t, nil
}

func (f *fakeStore) Renew(context.Context, *Task, time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.renews++
	if f.renews <= f.renewFail {
		return errors.New("db blip")
	}
	return f.renewErr
}

func (f *fakeStore) Finish(_ context.Context, _ *Task, status string, counts Counts, runErr string, _ []cmdbcontract.RunError) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.finished = append(f.finished, finishCall{status, counts, runErr})
	return nil
}

func (f *fakeStore) Append(_ context.Context, runID string, rows []*Resource) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.staging[runID] = append(f.staging[runID], rows...)
	return nil
}

func (f *fakeStore) Models(_ context.Context, runID string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	seen := map[string]bool{}
	var out []string
	for _, r := range f.staging[runID] {
		if !seen[r.ModelCode] {
			seen[r.ModelCode] = true
			out = append(out, r.ModelCode)
		}
	}
	return out, nil
}

func (f *fakeStore) Count(_ context.Context, runID string) (int32, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return int32(len(f.staging[runID])), nil //nolint:gosec // 测试桩，行数远小于 int32 上界
}

func (f *fakeStore) Clear(_ context.Context, runID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cleared = append(f.cleared, runID)
	delete(f.staging, runID)
	return nil
}

func (f *fakeStore) Fields(_ context.Context, model string) ([]ModelField, error) {
	return f.fields[model], nil
}

func (f *fakeStore) ReconcileModel(_ context.Context, p ReconcileParams) (Counts, []cmdbcontract.RunError, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reconcile = append(f.reconcile, p)
	return f.counts, f.samples, f.recErr
}

func (f *fakeStore) HasActive(_ context.Context, model, _ string) (bool, error) {
	return f.active[model], nil
}

func (f *fakeStore) ReconcileRelations(_ context.Context, runID, source string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.relations = append(f.relations, runID+"|"+source)
	return nil
}

type fakeOpener struct{ err error }

func (o fakeOpener) Open(ct []byte) (string, error) { return "plain:" + string(ct), o.err }

type fakeCollector struct {
	provider string
	items    []*Resource
	err      error
	gotCred  string
	block    bool
}

func (c *fakeCollector) Provider() string { return c.provider }

func (c *fakeCollector) Discover(ctx context.Context, _ *Source, credential string, sink Sink) error {
	c.gotCred = credential
	if c.block {
		<-ctx.Done()
		return ctx.Err()
	}
	for _, r := range c.items {
		if err := sink.Put(ctx, r); err != nil {
			return err
		}
	}
	return c.err
}

type fakeNotifier struct {
	mu     sync.Mutex
	events []SyncEvent
}

func (n *fakeNotifier) Notify(_ context.Context, e SyncEvent) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.events = append(n.events, e)
	return nil
}

func newWorker(store *fakeStore, collectors ...Collector) *WorkerUsecase {
	return newWorkerWithNotifier(store, &fakeNotifier{}, collectors...)
}

func newWorkerWithNotifier(store *fakeStore, n Notifier, collectors ...Collector) *WorkerUsecase {
	uc := NewWorkerUsecase(store, store, store, store, store, fakeOpener{}, n, collectors, testLogger())
	return uc.WithOptions(WorkerOptions{Lease: 30 * time.Millisecond, CollectTimeout: time.Second, StagingBatch: 2})
}

func aliyunSource() *Source {
	return &Source{ID: "src-1", Provider: cmdbcontract.ProviderAliyun, Credential: []byte("ct"), Env: "prod", GracePeriodSeconds: 60}
}

// ---- 用例 ----

func TestWorker_NoTask(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	if n, err := newWorker(store).RunOnce(context.Background(), "w"); n != 0 || err != nil {
		t.Fatalf("RunOnce = %d err=%v", n, err)
	}
}

func TestWorker_Success(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	store.source = aliyunSource()
	store.task = &Task{ID: "t1", SourceID: "src-1", Trigger: cmdbcontract.TriggerManual}
	store.counts = Counts{Created: 2, Updated: 1}
	store.samples = []cmdbcontract.RunError{{Reason: "x"}}
	col := &fakeCollector{provider: cmdbcontract.ProviderAliyun, items: []*Resource{
		{ModelCode: cmdbcontract.ModelCloudHost, ExternalID: "i-1", Name: " web ", Attrs: map[string]any{"instance_id": "i-1", "private_ip": "10.0.0.1", "owner_note": "x", "unknown": 1}},
		{ModelCode: cmdbcontract.ModelCloudHost, ExternalID: "i-2", Attrs: map[string]any{"instance_id": "i-2", "private_ip": nil}},
		{ModelCode: cmdbcontract.ModelCloudHost, ExternalID: "i-3", Attrs: map[string]any{"instance_id": "i-3"}},
		{ModelCode: "unknown_model", ExternalID: "x"},
	}}
	uc := newWorker(store, col)
	n, err := uc.RunOnce(context.Background(), "w")
	if n != 1 || err != nil {
		t.Fatalf("RunOnce = %d err=%v", n, err)
	}
	if col.gotCred != "plain:ct" {
		t.Fatalf("credential must be decrypted and passed: %q", col.gotCred)
	}
	// 暂存：未知类型丢弃、人工字段与未定义键过滤、nil 值丢弃、名称裁剪/回退。
	rows := store.staging["run-1"]
	if len(rows) != 0 { // 已 Clear
		t.Fatalf("staging must be cleared after run, got %d", len(rows))
	}
	if len(store.cleared) != 1 || store.cleared[0] != "run-1" {
		t.Fatalf("cleared = %v", store.cleared)
	}
	if len(store.reconcile) != 1 {
		t.Fatalf("reconcile calls = %d", len(store.reconcile))
	}
	p := store.reconcile[0]
	if p.ModelCode != cmdbcontract.ModelCloudHost || p.Source != "sync:src-1" || p.Env != "prod" || p.Grace != time.Minute ||
		len(p.ManualKeys) != 1 || p.ManualKeys[0] != "owner_note" || len(p.UniqueCodes) != 1 || p.SkipDisappear {
		t.Fatalf("reconcile params = %+v", p)
	}
	if len(store.relations) != 1 || store.relations[0] != "run-1|sync:src-1" {
		t.Fatalf("relations = %v", store.relations)
	}
	if len(store.finished) != 1 || store.finished[0].status != cmdbcontract.TaskSucceeded || store.finished[0].counts.Created != 2 || store.finished[0].counts.Discovered != 3 {
		t.Fatalf("finished = %+v", store.finished)
	}
}

func TestWorker_SinkFiltering(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	sink := newStagingSink(store, store, "r", 10)
	ctx := context.Background()
	if err := sink.Put(ctx, &Resource{ModelCode: cmdbcontract.ModelCloudHost, ExternalID: "i-1", Name: "  ", Attrs: map[string]any{"instance_id": "i-1", "owner_note": "manual", "zzz": 1, "private_ip": nil}}); err != nil {
		t.Fatal(err)
	}
	if err := sink.Put(ctx, &Resource{ModelCode: "nope", ExternalID: "x"}); err != nil {
		t.Fatal(err)
	}
	if err := sink.Put(ctx, &Resource{ModelCode: cmdbcontract.ModelCloudHost}); err == nil {
		t.Fatal("missing external_id must be rejected")
	}
	if err := sink.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	rows := store.staging["r"]
	if len(rows) != 1 || rows[0].Name != "i-1" || len(rows[0].Attrs) != 1 || rows[0].Attrs["instance_id"] != "i-1" {
		t.Fatalf("filtered rows = %+v", rows)
	}
}

func TestWorker_EmptyDiscoverySkipsDisappear(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	store.source = &Source{ID: "src-1", Provider: cmdbcontract.ProviderVMware, Credential: []byte("ct")}
	store.task = &Task{ID: "t1", SourceID: "src-1"}
	uc := newWorker(store, &fakeCollector{provider: cmdbcontract.ProviderVMware})
	if _, err := uc.RunOnce(context.Background(), "w"); err != nil {
		t.Fatal(err)
	}
	// VMware 可写两类型：即使本次为空也逐类型对账（但跳过消失判定）。
	if len(store.reconcile) != 2 || !store.reconcile[0].SkipDisappear || !store.reconcile[1].SkipDisappear {
		t.Fatalf("reconcile = %+v", store.reconcile)
	}
	if store.finished[0].status != cmdbcontract.TaskSucceeded || store.finished[0].counts.Discovered != 0 {
		t.Fatalf("finished = %+v", store.finished)
	}
}

// TestWorker_PerModelEmptyGuard 主机采到了、虚拟机一条没有（类型停用被 sink 丢弃 / 账号
// 失去可见性）：只有采到数据的类型做消失判定，0 行的类型跳过——否则全部虚拟机被整批退役。
func TestWorker_PerModelEmptyGuard(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	store.source = &Source{ID: "src-1", Provider: cmdbcontract.ProviderVMware, Credential: []byte("ct")}
	store.task = &Task{ID: "t1", SourceID: "src-1"}
	store.active = map[string]bool{cmdbcontract.ModelVirtualMachine: true}
	delete(store.fields, cmdbcontract.ModelVirtualMachine) // 类型停用：采集行在入暂存前被丢弃
	col := &fakeCollector{provider: cmdbcontract.ProviderVMware, items: []*Resource{
		{ModelCode: cmdbcontract.ModelPhysicalServer, ExternalID: "h-1", Attrs: map[string]any{"ip": "10.0.0.1"}},
		{ModelCode: cmdbcontract.ModelVirtualMachine, ExternalID: "vm-1", Attrs: map[string]any{"ip": "10.0.1.1"}},
	}}
	if _, err := newWorker(store, col).RunOnce(context.Background(), "w"); err != nil {
		t.Fatal(err)
	}
	if len(store.reconcile) != 2 {
		t.Fatalf("reconcile = %+v", store.reconcile)
	}
	for _, p := range store.reconcile {
		if want := p.ModelCode == cmdbcontract.ModelVirtualMachine; p.SkipDisappear != want {
			t.Fatalf("model %s SkipDisappear = %v, want %v", p.ModelCode, p.SkipDisappear, want)
		}
	}
	if store.finished[0].status != cmdbcontract.TaskSucceeded || store.finished[0].counts.Discovered != 1 {
		t.Fatalf("finished = %+v", store.finished)
	}
}

func TestWorker_CollectorErrorFailsTask(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	store.source = aliyunSource()
	store.task = &Task{ID: "t1", SourceID: "src-1"}
	uc := newWorker(store, &fakeCollector{
		provider: cmdbcontract.ProviderAliyun, err: errors.New("api down"),
		items: []*Resource{{ModelCode: cmdbcontract.ModelCloudHost, ExternalID: "i-1", Attrs: map[string]any{"instance_id": "i-1"}}},
	})
	if n, err := uc.RunOnce(context.Background(), "w"); n != 1 || err != nil {
		t.Fatalf("RunOnce = %d err=%v", n, err)
	}
	if len(store.reconcile) != 0 {
		t.Fatal("collector failure must not reconcile (would mark everything missing)")
	}
	f := store.finished[0]
	if f.status != cmdbcontract.TaskFailed || f.err == "" {
		t.Fatalf("finished = %+v", f)
	}
}

func TestWorker_UnknownProviderAndMissingCredential(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	store.source = &Source{ID: "src-1", Provider: "gcp", Credential: []byte("ct")}
	store.task = &Task{ID: "t1", SourceID: "src-1"}
	uc := newWorker(store)
	if _, err := uc.RunOnce(context.Background(), "w"); err != nil {
		t.Fatal(err)
	}
	if f := store.finished[0]; f.status != cmdbcontract.TaskFailed || !contains([]string{f.err}, "no collector registered for provider: gcp") {
		t.Fatalf("finished = %+v", f)
	}
	store2 := newFakeStore()
	store2.source = &Source{ID: "src-1", Provider: cmdbcontract.ProviderAliyun}
	store2.task = &Task{ID: "t1", SourceID: "src-1"}
	if _, err := newWorker(store2, &fakeCollector{provider: cmdbcontract.ProviderAliyun}).RunOnce(context.Background(), "w"); err != nil {
		t.Fatal(err)
	}
	if f := store2.finished[0]; f.status != cmdbcontract.TaskFailed || f.err != ErrCredentialMissing.Error() {
		t.Fatalf("finished = %+v", f)
	}
}

func TestWorker_LeaseLostCancelsCollect(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	store.source = aliyunSource()
	store.task = &Task{ID: "t1", SourceID: "src-1"}
	store.renewErr = ErrLeaseLost
	col := &fakeCollector{provider: cmdbcontract.ProviderAliyun, block: true}
	uc := newWorker(store, col) // lease 30ms → 10ms 续约即失主
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = uc.RunOnce(context.Background(), "w")
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("lease loss must cancel a blocked collector")
	}
	if f := store.finished[0]; f.status != cmdbcontract.TaskFailed || !contains([]string{f.err}, ErrLeaseLost.Error()) {
		t.Fatalf("finished = %+v", f)
	}
}

func TestWorker_RenewsLease(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	store.source = aliyunSource()
	store.task = &Task{ID: "t1", SourceID: "src-1"}
	slow := &slowCollector{provider: cmdbcontract.ProviderAliyun, delay: 80 * time.Millisecond}
	if _, err := newWorker(store, slow).RunOnce(context.Background(), "w"); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.renews < 2 {
		t.Fatalf("expected periodic renewals, got %d", store.renews)
	}
}

// TestWorker_RenewToleratesTransientError 一次续约抖动不中止同步（快速重试续上）；
// 持续失败则在租约临近到期前放弃，任务失败。
func TestWorker_RenewToleratesTransientError(t *testing.T) {
	t.Parallel()
	opts := WorkerOptions{Lease: 600 * time.Millisecond, CollectTimeout: 5 * time.Second, StagingBatch: 2}
	store := newFakeStore()
	store.source = aliyunSource()
	store.task = &Task{ID: "t1", SourceID: "src-1"}
	store.renewFail = 1
	slow := &slowCollector{provider: cmdbcontract.ProviderAliyun, delay: 450 * time.Millisecond}
	if _, err := newWorker(store, slow).WithOptions(opts).RunOnce(context.Background(), "w"); err != nil {
		t.Fatal(err)
	}
	if f := store.finished[0]; f.status != cmdbcontract.TaskSucceeded {
		t.Fatalf("one renew blip must not fail the sync: %+v", f)
	}
	if store.renews < 2 {
		t.Fatalf("failed renew must be retried, renews = %d", store.renews)
	}

	down := newFakeStore()
	down.source = aliyunSource()
	down.task = &Task{ID: "t1", SourceID: "src-1"}
	down.renewFail = 1 << 30
	blocked := &fakeCollector{provider: cmdbcontract.ProviderAliyun, block: true}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = newWorker(down, blocked).RunOnce(context.Background(), "w")
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("persistent renew failure must abort before the lease runs out")
	}
	if f := down.finished[0]; f.status != cmdbcontract.TaskFailed || !strings.Contains(f.err, "db blip") {
		t.Fatalf("finished = %+v", f)
	}
}

type slowCollector struct {
	provider string
	delay    time.Duration
}

func (c *slowCollector) Provider() string { return c.provider }

func (c *slowCollector) Discover(ctx context.Context, _ *Source, _ string, _ Sink) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(c.delay):
		return nil
	}
}

// ---- 调度器 ----

type fakeScheduler struct {
	enq, stale, purge int
	err               error
}

func (f *fakeScheduler) EnqueueDue(context.Context, time.Time) (int, error) { f.enq++; return 2, f.err }

func (f *fakeScheduler) MarkStale(context.Context, int32) (int, error) { f.stale++; return 1, nil }

func (f *fakeScheduler) PurgeStaging(context.Context, time.Duration) (int, error) {
	f.purge++
	return 0, nil
}

func TestScheduler_TickOnce(t *testing.T) {
	t.Parallel()
	repo := &fakeScheduler{}
	notifier := &fakeNotifier{}
	uc := NewSchedulerUsecase(repo, notifier, testLogger()).WithOptions(SchedulerOptions{StaleMultiplier: 0, StagingRetention: 0})
	now := time.Now()
	if n, err := uc.TickOnce(context.Background(), now); n != 2 || err != nil {
		t.Fatalf("TickOnce = %d err=%v", n, err)
	}
	// 同一分钟内再 tick：不重复做维护。
	if _, err := uc.TickOnce(context.Background(), now.Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	if repo.enq != 2 || repo.stale != 1 || repo.purge != 1 {
		t.Fatalf("enq=%d stale=%d purge=%d", repo.enq, repo.stale, repo.purge)
	}
	if len(notifier.events) != 1 || notifier.events[0].Type != EventSyncStale || notifier.events[0].DedupKey == "" {
		t.Fatalf("stale event = %+v", notifier.events)
	}
	if _, err := uc.TickOnce(context.Background(), now.Add(2*time.Minute)); err != nil || repo.stale != 2 {
		t.Fatalf("maintenance must run again after a minute: stale=%d err=%v", repo.stale, err)
	}
	repo.err = errors.New("db down")
	if _, err := uc.TickOnce(context.Background(), now.Add(3*time.Minute)); err == nil {
		t.Fatal("enqueue error must propagate")
	}
}

func TestWorker_EmitsEvents(t *testing.T) {
	t.Parallel()
	// 失败 → critical 事件，按源去重一个周期。
	store := newFakeStore()
	store.source = aliyunSource()
	store.source.IntervalSeconds = 600
	store.task = &Task{ID: "t1", SourceID: "src-1"}
	n := &fakeNotifier{}
	uc := newWorkerWithNotifier(store, n, &fakeCollector{provider: cmdbcontract.ProviderAliyun, err: errors.New("api down")})
	if _, err := uc.RunOnce(context.Background(), "w"); err != nil {
		t.Fatal(err)
	}
	if len(n.events) != 1 || n.events[0].Type != EventSyncFailed || n.events[0].Severity != SeverityCritical ||
		n.events[0].DedupKey != EventSyncFailed+":src-1" || n.events[0].DedupTTL != 600*time.Second || n.events[0].Payload["error"] != "collect aliyun: api down" {
		t.Fatalf("failed event = %+v", n.events)
	}
	// 成功且有退役 → warning 事件；无变化 → 不通知。
	store2 := newFakeStore()
	store2.source = aliyunSource()
	store2.task = &Task{ID: "t2", SourceID: "src-1"}
	store2.counts = Counts{Disappeared: 2, Retired: 1}
	n2 := &fakeNotifier{}
	col := &fakeCollector{provider: cmdbcontract.ProviderAliyun, items: []*Resource{{ModelCode: cmdbcontract.ModelCloudHost, ExternalID: "i-1", Attrs: map[string]any{"instance_id": "i-1"}}}}
	if _, err := newWorkerWithNotifier(store2, n2, col).RunOnce(context.Background(), "w"); err != nil {
		t.Fatal(err)
	}
	if len(n2.events) != 1 || n2.events[0].Type != EventSyncDisappeared || n2.events[0].Payload["retired"] != int32(1) {
		t.Fatalf("disappeared event = %+v", n2.events)
	}
	store3 := newFakeStore()
	store3.source = aliyunSource()
	store3.task = &Task{ID: "t3", SourceID: "src-1"}
	n3 := &fakeNotifier{}
	if _, err := newWorkerWithNotifier(store3, n3, col).RunOnce(context.Background(), "w"); err != nil {
		t.Fatal(err)
	}
	if len(n3.events) != 0 {
		t.Fatalf("quiet run must not notify: %+v", n3.events)
	}
}

func TestWorker_FieldMapAndChangedEvent(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	store.source = &Source{
		ID: "src-1", Provider: cmdbcontract.ProviderAliyun, Credential: []byte("ct"),
		Config: []byte(`{"region_id":"r","access_key_id":"k","field_map":{"primary_ip":"private_ip"}}`),
	}
	store.task = &Task{ID: "t1", SourceID: "src-1"}
	store.counts = Counts{Created: 1}
	n := &fakeNotifier{}
	col := &fakeCollector{provider: cmdbcontract.ProviderAliyun, items: []*Resource{
		{ModelCode: cmdbcontract.ModelCloudHost, ExternalID: "i-1", Attrs: map[string]any{"instance_id": "i-1", "primary_ip": "10.0.0.9"}},
	}}
	uc := newWorkerWithNotifier(store, n, col)
	uc.WithOptions(WorkerOptions{StagingBatch: 100})
	// 在 Clear 之前截获暂存：用 Append 记录。
	store.staging = map[string][]*Resource{}
	if _, err := uc.RunOnce(context.Background(), "w"); err != nil {
		t.Fatal(err)
	}
	if len(n.events) != 1 || n.events[0].Type != EventSyncChanged || n.events[0].Severity != SeverityInfo || n.events[0].Payload["created"] != int32(1) {
		t.Fatalf("changed event = %+v", n.events)
	}
}

func TestStagingSink_FieldMap(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	sink := newStagingSink(store, store, "r", 10)
	sink.fieldMap = map[string]string{"primary_ip": "private_ip"}
	if err := sink.Put(context.Background(), &Resource{ModelCode: cmdbcontract.ModelCloudHost, ExternalID: "i-1", Attrs: map[string]any{"instance_id": "i-1", "primary_ip": "10.0.0.9"}}); err != nil {
		t.Fatal(err)
	}
	if err := sink.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	rows := store.staging["r"]
	if len(rows) != 1 || rows[0].Attrs["private_ip"] != "10.0.0.9" || rows[0].Attrs["primary_ip"] != nil {
		t.Fatalf("field map not applied: %+v", rows)
	}
}

type fakeMaintenance struct {
	ensured []string
	dropped []time.Time
	fail    bool
}

func (f *fakeMaintenance) EnsurePartition(_ context.Context, table string, month time.Time) error {
	if f.fail {
		return errors.New("lock timeout")
	}
	f.ensured = append(f.ensured, table+":"+month.Format("2006-01"))
	return nil
}

func (f *fakeMaintenance) DropPartitionsBefore(_ context.Context, _ string, before time.Time) (int, error) {
	f.dropped = append(f.dropped, before)
	return 1, nil
}

func TestMaintenance_RunOnce(t *testing.T) {
	t.Parallel()
	repo := &fakeMaintenance{}
	uc := NewMaintenanceUsecase(repo, testLogger()).WithOptions(MaintenanceOptions{PartitionsAhead: 2, HistoryRetention: 30 * 24 * time.Hour})
	now := time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC)
	if err := uc.RunOnce(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if len(repo.ensured) != 3 || repo.ensured[0] != "cmdb_ci_history:2026-11" || repo.ensured[2] != "cmdb_ci_history:2027-01" {
		t.Fatalf("ensured = %v", repo.ensured)
	}
	if len(repo.dropped) != 1 || !repo.dropped[0].Equal(now.Add(-30*24*time.Hour)) {
		t.Fatalf("dropped = %v", repo.dropped)
	}
	// 保留期 0 = 不删除；单项失败返回错误但不中断其它月份。
	repo2 := &fakeMaintenance{fail: true}
	uc2 := NewMaintenanceUsecase(repo2, testLogger()).WithOptions(MaintenanceOptions{PartitionsAhead: 1, HistoryRetention: 0})
	if err := uc2.RunOnce(context.Background(), now); err == nil || len(repo2.dropped) != 0 {
		t.Fatalf("expected ensure error and no drop, got err=%v dropped=%v", err, repo2.dropped)
	}
}

type fakeWebhook struct {
	target  *WebhookTarget
	touches int
	limited bool
	queued  bool
}

func (f *fakeWebhook) GetWebhook(context.Context, string) (*WebhookTarget, error) {
	if f.target == nil {
		return nil, ErrWebhookSourceNotFound
	}
	return f.target, nil
}

func (f *fakeWebhook) TouchWebhook(context.Context, string, time.Duration) (bool, error) {
	f.touches++
	return !f.limited, nil
}

func (f *fakeWebhook) EnqueueTask(context.Context, string, string) (string, bool, error) {
	return "t1", f.queued, nil
}

func TestWebhookUsecase_Trigger(t *testing.T) {
	t.Parallel()
	sum := sha256.Sum256([]byte("tok"))
	repo := &fakeWebhook{target: &WebhookTarget{ID: "s", Enabled: true, TokenHash: sum[:]}, queued: true}
	uc := NewWebhookUsecase(repo)
	if _, _, err := uc.Trigger(context.Background(), "s", "bad"); !errors.Is(err, ErrWebhookUnauthorized) || repo.touches != 0 {
		t.Fatalf("bad token: err=%v touches=%d", err, repo.touches)
	}
	if id, queued, err := uc.Trigger(context.Background(), "s", "tok"); err != nil || id != "t1" || !queued {
		t.Fatalf("ok trigger: %s %v %v", id, queued, err)
	}
	repo.limited = true
	if _, _, err := uc.Trigger(context.Background(), "s", "tok"); !errors.Is(err, ErrWebhookRateLimited) {
		t.Fatalf("rate limit: %v", err)
	}
	repo.target.Enabled = false
	if _, _, err := uc.Trigger(context.Background(), "s", "tok"); !errors.Is(err, ErrWebhookSourceNotFound) {
		t.Fatalf("disabled: %v", err)
	}
}
