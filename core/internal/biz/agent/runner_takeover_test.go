package biz

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	bizassistant "github.com/lanyulei/reborn/backend/core/internal/biz/assistant"
)

// ---- 运行器测试桩：只实现运行器用到的方法，其余走嵌入的 nil 接口（被调用即 panic，等于断言「不该调用」） ----

type stubTasks struct {
	TaskRepo
	mu sync.Mutex
	// renew 第 n 次续租（从 1 起）的结果；nil 表示恒成功。
	renew  func(n int) (bool, error)
	renews int
	// current Get 返回的任务当前状态。
	current *Task

	finished  []string // status|result
	requeued  []string // result
	requeueOK []bool   // 放回队列时所用 ctx 是否仍可用
	staleArgs []int
}

func (s *stubTasks) Renew(context.Context, string, string, time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.renews++
	if s.renew == nil {
		return true, nil
	}
	return s.renew(s.renews)
}

func (s *stubTasks) Get(context.Context, string) (*Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current == nil {
		return nil, ErrTaskNotFound
	}
	cp := *s.current
	return &cp, nil
}

func (s *stubTasks) Finish(_ context.Context, _, _, status, result string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.finished = append(s.finished, status+"|"+result)
	return true, nil
}

func (s *stubTasks) Requeue(ctx context.Context, _, _ string, _ time.Duration, result string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requeued = append(s.requeued, result)
	s.requeueOK = append(s.requeueOK, ctx.Err() == nil)
	return true, nil
}

func (s *stubTasks) BindRun(context.Context, string, string, string, string) error { return nil }

func (s *stubTasks) ReleaseStale(_ context.Context, limit int) (int, error) {
	s.staleArgs = append(s.staleArgs, limit)
	return 0, nil
}

func (s *stubTasks) ExpireQueued(context.Context, time.Time, int) (int, error) { return 0, nil }

func (s *stubTasks) Claim(context.Context, int, time.Duration) ([]*Task, error) { return nil, nil }

func (s *stubTasks) Purge(context.Context, time.Time, int) (int, error) { return 2, nil }

type stubRuns struct {
	RunRepo
	mu      sync.Mutex
	stored  map[string]*Run
	created []*Run
	saved   []string // 每次 SaveRunning 写入的状态
	closed  []string // runID|status
}

func (s *stubRuns) Get(_ context.Context, id string) (*Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.stored[id]
	if !ok {
		return nil, ErrRunNotFound
	}
	cp := *r
	return &cp, nil
}

func (s *stubRuns) Create(_ context.Context, r *Run) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.created = append(s.created, r)
	return nil
}

func (s *stubRuns) SaveRunning(_ context.Context, r *Run) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saved = append(s.saved, r.Status)
	return true, nil
}

func (s *stubRuns) Close(_ context.Context, id, status, _ string, _ time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = append(s.closed, id+"|"+status)
	return true, nil
}

func (s *stubRuns) RecentAutoOutcome(context.Context, string, time.Duration) (int, int, error) {
	return 0, 0, nil
}

func (s *stubRuns) PurgeBefore(context.Context, time.Time, int) (int, error) { return 3, nil }

type stubProfiles struct {
	ProfileRepo
	profile *Profile
	err     error
}

func (s stubProfiles) Get(context.Context, string) (*Profile, error) { return s.profile, s.err }

type stubBudgets struct{}

func (stubBudgets) Add(context.Context, string, string, int32, int64, int64) error { return nil }
func (stubBudgets) Get(context.Context, string, string) (*Budget, error)           { return &Budget{}, nil }

func (stubBudgets) Total(context.Context, string) (*Budget, error) { return &Budget{}, nil }

type stubIncidents struct {
	Incidents
	inc *IncidentSnapshot
}

func (s stubIncidents) Snapshot(context.Context, string) (*IncidentSnapshot, error) {
	return s.inc, nil
}

type stubAccounts struct{ Accounts }

func (stubAccounts) IsMachine(context.Context, string) (bool, error) { return true, nil }
func (stubAccounts) IssueToken(context.Context, string, time.Duration) (string, time.Time, error) {
	return "token", time.Time{}, nil
}

// stubConvs 会话桩；Purge 不实现：保留期清理不应再单独清会话。
type stubConvs struct {
	Conversations
	calls []*bizassistant.ToolCall
}

func (stubConvs) Create(context.Context, string, string) (string, error) { return "conv-new", nil }
func (s stubConvs) Transcript(context.Context, string) ([]*bizassistant.Message, []*bizassistant.ToolCall, error) {
	return nil, s.calls, nil
}

type stubTools struct{}

func (stubTools) Find(name string) *bizassistant.ToolSpec { return &bizassistant.ToolSpec{Name: name} }
func (stubTools) All() []bizassistant.ToolSpec            { return nil }

type stubInvoker struct {
	mu    sync.Mutex
	tools []string
}

func (s *stubInvoker) Invoke(_ context.Context, spec bizassistant.ToolSpec, _ json.RawMessage, _ string) (*bizassistant.InvokeResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tools = append(s.tools, spec.Name)
	return &bizassistant.InvokeResult{StatusCode: 200, Body: json.RawMessage(`{}`)}, nil
}

type stubHeadless struct {
	err   error
	calls int
}

func (s *stubHeadless) RunHeadless(context.Context, bizassistant.HeadlessSpec) (*bizassistant.HeadlessResult, error) {
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	return &bizassistant.HeadlessResult{Content: `{"facts":[]}`, Steps: 1, EndReason: bizassistant.HeadlessCompleted}, nil
}

type runnerFixture struct {
	uc       *RunnerUsecase
	tasks    *stubTasks
	runs     *stubRuns
	invoker  *stubInvoker
	headless *stubHeadless
	profile  *Profile
	task     *Task
}

const (
	fixProfileID = "aaaaaaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa"
	fixAccountID = "bbbbbbbb-bbbb-7bbb-8bbb-bbbbbbbbbbbb"
	fixTaskID    = "cccccccc-cccc-7ccc-8ccc-cccccccccccc"
	fixIncident  = "22222222-2222-7222-8222-222222222222"
)

// newRunnerFixture 一条已领取的人工召唤诊断任务，故障进行中（要取故障租约）。
func newRunnerFixture() *runnerFixture {
	profile := &Profile{ID: fixProfileID, Spec: ProfileSpec{
		Name: "诊断", Kind: KindDiagnose, AgentUserID: fixAccountID, Enabled: true, Budget: DefaultBudget(), Auto: DefaultAuto(),
		Stages: []StageSpec{{Key: StageCollect, MaxSteps: 3}, {Key: StageConclude, MaxSteps: 3}},
	}}
	f := &runnerFixture{
		tasks: &stubTasks{}, runs: &stubRuns{stored: map[string]*Run{}}, invoker: &stubInvoker{}, headless: &stubHeadless{}, profile: profile,
		task: &Task{ID: fixTaskID, ProfileID: fixProfileID, Kind: KindDiagnose, Source: SourceManual, IncidentID: fixIncident, Status: TaskRunning, Attempts: 1, LockToken: "mine"},
	}
	cfg := NewConfig(nil)
	cfg.Enabled = true
	f.uc = NewRunnerUsecase(&Deps{
		Profiles: stubProfiles{profile: profile}, Tasks: f.tasks, Runs: f.runs, Budgets: stubBudgets{}, Headless: f.headless,
		Conversations: stubConvs{}, Tools: stubTools{}, Invoker: f.invoker, Accounts: stubAccounts{},
		Incidents: stubIncidents{inc: &IncidentSnapshot{ID: fixIncident, Number: "INC-1", Active: true, OccurTime: time.Now().Add(-time.Minute)}},
		Config:    cfg, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	return f
}

// 第 6 条：续租遇到数据库错误不是「任务已取消」——放回队列重试，运行保持 running。
func TestRunTask_RenewErrorIsRetriedNotCancelled(t *testing.T) {
	f := newRunnerFixture()
	f.tasks.renew = func(int) (bool, error) { return false, errors.New("conn reset by peer") }
	f.uc.runTask(context.Background(), f.task)
	if len(f.tasks.finished) != 0 {
		t.Fatalf("a transient renew error must not finish the task: %v", f.tasks.finished)
	}
	if len(f.tasks.requeued) != 1 || strings.Contains(f.tasks.requeued[0], "conn reset") {
		t.Fatalf("task must be requeued with a generic message: %v", f.tasks.requeued)
	}
	if len(f.runs.saved) != 1 || f.runs.saved[0] != RunRunning || len(f.runs.closed) != 0 {
		t.Fatalf("run must stay running for the retry: saved=%v closed=%v", f.runs.saved, f.runs.closed)
	}
	if f.headless.calls != 0 {
		t.Fatalf("no stage may start without the lease renewed: %d", f.headless.calls)
	}
}

// 第 9 条：没续上且任务仍在进行中 = 已易主。运行归新持有者续跑，本实例不写运行、不写任务、不释放故障租约。
func TestRunTask_TakenOverLeavesRunToNewOwner(t *testing.T) {
	for _, status := range []string{TaskRunning, TaskQueued} {
		f := newRunnerFixture()
		f.tasks.renew = func(int) (bool, error) { return false, nil }
		f.tasks.current = &Task{ID: fixTaskID, Status: status, LockToken: "theirs"}
		f.uc.runTask(context.Background(), f.task)
		if len(f.runs.saved) != 0 || len(f.runs.closed) != 0 {
			t.Fatalf("%s: run now belongs to the new owner and must not be written: saved=%v closed=%v", status, f.runs.saved, f.runs.closed)
		}
		if len(f.tasks.finished) != 0 || len(f.tasks.requeued) != 0 {
			t.Fatalf("%s: task must be left to the new owner: finished=%v requeued=%v", status, f.tasks.finished, f.tasks.requeued)
		}
		if len(f.invoker.tools) != 1 || f.invoker.tools[0] != toolAcquireLease {
			t.Fatalf("%s: the incident lease is shared with the new owner and must not be released: %v", status, f.invoker.tools)
		}
	}
}

// 第 9 条的另一半：任务确实被用户取消（已到终态）时行为不变——按取消收尾并释放故障租约。
func TestRunTask_UserCancelStillClosesAsCancelled(t *testing.T) {
	f := newRunnerFixture()
	f.tasks.renew = func(int) (bool, error) { return false, nil }
	f.tasks.current = &Task{ID: fixTaskID, Status: TaskCancelled}
	f.uc.runTask(context.Background(), f.task)
	if len(f.runs.saved) != 1 || f.runs.saved[0] != RunCancelled {
		t.Fatalf("run must be saved as cancelled: %v", f.runs.saved)
	}
	if len(f.tasks.finished) != 1 || !strings.HasPrefix(f.tasks.finished[0], TaskCancelled+"|") {
		t.Fatalf("task must be finished as cancelled: %v", f.tasks.finished)
	}
	if len(f.invoker.tools) != 2 || f.invoker.tools[1] != toolReleaseLease {
		t.Fatalf("lease must be released: %v", f.invoker.tools)
	}
}

// 第 7 条 + 第 16 条：运行前阶段失败时，即使运行 ctx 已取消（停机）也要把任务放回队列；
// 内部错误原文不写进 result。
func TestRunTask_PreRunFailureRequeuesDuringShutdown(t *testing.T) {
	f := newRunnerFixture()
	f.uc.d.Profiles = stubProfiles{err: errors.New("pgx: FATAL secret-detail")}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f.uc.runTask(ctx, f.task)
	if len(f.tasks.requeued) != 1 || !f.tasks.requeueOK[0] {
		t.Fatalf("requeue must run on a context that survives shutdown: %v %v", f.tasks.requeued, f.tasks.requeueOK)
	}
	if strings.Contains(f.tasks.requeued[0], "secret-detail") || f.tasks.requeued[0] == "" {
		t.Fatalf("internal error text must not reach task.result: %q", f.tasks.requeued[0])
	}
}

// 第 16 条：阶段出错的原文同样不进 result。
func TestRunTask_StageErrorTextNotInResult(t *testing.T) {
	f := newRunnerFixture()
	f.headless.err = errors.New("dial tcp 10.0.0.8:5432: secret-detail")
	f.uc.runTask(context.Background(), f.task)
	if len(f.tasks.requeued) != 1 || strings.Contains(f.tasks.requeued[0], "secret-detail") {
		t.Fatalf("stage error text must not reach task.result: %v", f.tasks.requeued)
	}
}

// 第 15 条：重试期间 Profile 换了机器账号，旧运行（会话归旧账号）不能续跑——收口后另起一条。
func TestOpenRun_AgentUserChangedStartsNewRun(t *testing.T) {
	f := newRunnerFixture()
	old := &Run{ID: "run-old", TaskID: fixTaskID, ProfileID: fixProfileID, AgentUserID: "old-account", ConversationID: "conv-old", Status: RunRunning}
	f.runs.stored[old.ID] = old
	f.task.RunID = old.ID
	inc := &IncidentSnapshot{ID: fixIncident, Number: "INC-1"}
	run, resumed, err := f.uc.openRun(context.Background(), f.task, f.profile, inc, KindDiagnose, time.Now())
	if err != nil || resumed {
		t.Fatalf("a run owned by another machine account must not be resumed: resumed=%v err=%v", resumed, err)
	}
	if run.ID == old.ID || run.AgentUserID != fixAccountID || run.ConversationID != "conv-new" {
		t.Fatalf("new run = %+v", run)
	}
	if len(f.runs.closed) != 1 || f.runs.closed[0] != "run-old|"+RunCancelled {
		t.Fatalf("the superseded run must be closed: %v", f.runs.closed)
	}
	// 账号没变：照常续跑，不收口。
	old.AgentUserID = fixAccountID
	f.runs.closed = nil
	run, resumed, err = f.uc.openRun(context.Background(), f.task, f.profile, inc, KindDiagnose, time.Now())
	if err != nil || !resumed || run.ID != old.ID || len(f.runs.closed) != 0 {
		t.Fatalf("same account must resume: resumed=%v run=%+v err=%v closed=%v", resumed, run, err, f.runs.closed)
	}
}

// 第 16 条：保留期清理只调运行仓储（运行与会话同事务删除），不再单独清会话。
func TestPurge_RunsAndConversationsInOneRepoCall(t *testing.T) {
	f := newRunnerFixture()
	n, err := f.uc.Purge(context.Background()) // stubConvs 没有 Purge：被调用即 panic
	if err != nil || n != 5 {
		t.Fatalf("Purge = %d %v, want 5 (3 runs + 2 tasks)", n, err)
	}
}

// 第 8 条：回收失联任务不再传实例本地时间。
func TestRun_ReleaseStaleUsesDatabaseClock(t *testing.T) {
	f := newRunnerFixture()
	if _, err := f.uc.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.tasks.staleArgs) != 1 || f.tasks.staleArgs[0] != housekeepingBatch {
		t.Fatalf("ReleaseStale args = %v", f.tasks.staleArgs)
	}
}

// 第 10 条：next_step 按故障域接口的上限（2000）截断，三条路径一致。
func TestNextStepClippedToAPILimit(t *testing.T) {
	long := strings.Repeat("步", maxNextStep+500)
	s := newState(false, DefaultAuto())
	d := s.decide(context.Background(), &bizassistant.ToolCall{ToolName: toolAddFinding, Write: true}, json.RawMessage(`{"finding":{"title":"根因","next_step":"`+long+`"}}`))
	var args struct {
		Finding struct {
			NextStep string `json:"nextStep"`
		} `json:"finding"`
	}
	if err := json.Unmarshal(d.Rewrite, &args); err != nil || utf8.RuneCountInString(args.Finding.NextStep) != maxNextStep {
		t.Fatalf("decide: next_step = %d runes, err %v", utf8.RuneCountInString(args.Finding.NextStep), err)
	}
	if f := parseFinding(json.RawMessage(`{"finding":{"title":"根因","next_step":"` + long + `"}}`)); f == nil || utf8.RuneCountInString(f.NextStep) != maxNextStep {
		t.Fatalf("parseFinding: %+v", f)
	}
	// 运行器代写（干跑记为捕获）：入参里的 nextStep 同样不超限。
	dry := newState(true, DefaultAuto())
	dry.finding = &Finding{Title: "根因", NextStep: long}
	(&RunnerUsecase{}).writeFinding(context.Background(), dry, "")
	if len(dry.captured) != 1 || json.Unmarshal([]byte(dry.captured[0].Input), &args) != nil || utf8.RuneCountInString(args.Finding.NextStep) != maxNextStep {
		t.Fatalf("writeFinding: captured = %+v", dry.captured)
	}
}

// 第 12 条：字段类型不符时逐字段宽松处理，不整体丢弃。
func TestParseFinding_LenientPerField(t *testing.T) {
	f := parseFinding(json.RawMessage(`{"finding":{"title":"连接池耗尽","conclusion":["不是字符串"],"confidence":"85","evidence":"单条证据","nextStep":"扩容"}}`))
	if f == nil {
		t.Fatal("a finding with mistyped fields must still be parsed")
	}
	if f.Title != "连接池耗尽" || f.Conclusion != "" || f.Confidence != 85 || len(f.Evidence) != 1 || f.Evidence[0] != "单条证据" || f.NextStep != "扩容" {
		t.Fatalf("parseFinding = %+v", f)
	}
	if f := parseFinding(json.RawMessage(`{"finding":{"title":"t","confidence":92.7,"evidence":["a",3,"","b"]}}`)); f == nil || f.Confidence != 92 || len(f.Evidence) != 2 {
		t.Fatalf("parseFinding numbers / mixed evidence = %+v", f)
	}
	for _, raw := range []string{`{"finding":"字符串"}`, `{"finding":{"title":7}}`, `[1]`, `{}`} {
		if parseFinding(json.RawMessage(raw)) != nil {
			t.Fatalf("%s must yield nil", raw)
		}
	}
}

// 第 13 条：发现写入成功后、阶段落库前崩溃——续跑时从转录里 AddFinding 的入参恢复发现内容。
func TestRebuildState_RestoresFindingFromTranscript(t *testing.T) {
	event := "33333333-3333-7333-8333-333333333333"
	calls := []*bizassistant.ToolCall{
		{ToolName: toolAddFinding, Status: bizassistant.ToolStatusFailed, Input: json.RawMessage(`{"finding":{"title":"失败的那次"}}`)},
		{
			ToolName: toolAddFinding, Status: bizassistant.ToolStatusSucceeded,
			Input:  json.RawMessage(`{"id":"` + fixIncident + `","finding":{"title":"连接池耗尽","conclusion":"c","confidence":88,"evidence":["e1"],"nextStep":"扩容","runId":"r"}}`),
			Output: json.RawMessage(`{"item":{"id":"` + event + `"}}`),
		},
	}
	uc := &RunnerUsecase{d: &Deps{Conversations: stubConvs{calls: calls}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}}
	st := newState(false, DefaultAuto())
	uc.rebuildState(context.Background(), st)
	if !st.findingDone || st.run.FindingEventID != event {
		t.Fatalf("finding must be marked done from the transcript: done=%v event=%q", st.findingDone, st.run.FindingEventID)
	}
	if st.finding == nil || st.finding.Title != "连接池耗尽" || st.finding.Confidence != 88 || st.finding.NextStep != "扩容" || len(st.finding.Evidence) != 1 {
		t.Fatalf("finding content must be restored from the AddFinding input: %+v", st.finding)
	}
	// 运行记录上已有发现时以它为准。
	st = newState(false, DefaultAuto())
	st.run.Finding = &Finding{Title: "已落库的"}
	uc.rebuildState(context.Background(), st)
	if st.finding == nil || st.finding.Title != "已落库的" {
		t.Fatalf("the saved finding wins: %+v", st.finding)
	}
	// 转录里没有成功的写入：不凭空造出发现。
	uc.d.Conversations = stubConvs{calls: calls[:1]}
	st = newState(false, DefaultAuto())
	uc.rebuildState(context.Background(), st)
	if st.findingDone || st.finding != nil {
		t.Fatalf("no successful AddFinding → no finding: done=%v finding=%+v", st.findingDone, st.finding)
	}
}

// 第 14 条：截尾同时受字节预算约束——行较长时只留 200 行仍会超过通用截断的阈值，最新的日志照样被切掉。
func TestTailLogs_ByteBudget(t *testing.T) {
	lines := make([]string, 0, 300)
	for i := range 300 {
		lines = append(lines, fmt.Sprintf("行%s #%03d", strings.Repeat("x", 400), i))
	}
	raw, _ := json.Marshal(map[string]any{"content": strings.Join(lines, "\n")})
	out := tailLogs(raw)
	if out == nil || len(out) >= 32<<10 {
		t.Fatalf("tail must fit under the generic truncation threshold: %d bytes", len(out))
	}
	var v map[string]string
	if err := json.Unmarshal(out, &v); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(v["content"], "#299") || strings.Contains(v["content"], "#100") {
		t.Fatalf("the newest lines must survive: …%s", v["content"][len(v["content"])-20:])
	}
	if !utf8.ValidString(v["content"]) {
		t.Fatal("content must stay valid UTF-8")
	}
	// 行数不到 200 但字节超预算：同样截尾（此前原样返回，随后被通用截断砍掉尾部）。
	raw, _ = json.Marshal(map[string]any{"content": strings.Join(lines[:150], "\n")})
	if out = tailLogs(raw); out == nil || len(out) >= 32<<10 {
		t.Fatalf("few but long lines must be trimmed by bytes: %d", len(out))
	}
	// 最后一行自己就超出预算：保留它的尾部，按字符边界切。
	raw, _ = json.Marshal(map[string]any{"content": "head\n" + strings.Repeat("汉", 20000) + "尾"})
	out = tailLogs(raw)
	if err := json.Unmarshal(out, &v); err != nil || !strings.HasSuffix(v["content"], "尾") || !utf8.ValidString(v["content"]) || len(v["content"]) > logTailMaxBytes+200 {
		t.Fatalf("oversized last line: err=%v len=%d", err, len(v["content"]))
	}
	if strings.ContainsRune(v["content"], utf8.RuneError) {
		t.Fatal("must cut on a rune boundary")
	}
}
