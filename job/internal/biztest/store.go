// Package biztest 提供 biz 仓储接口的内存实现，供引擎的用例与 HTTP 处理器单元测试使用
// （不依赖数据库）。语义与 internal/data 的 SQL 实现保持一致：领取顺序与区域过滤、令牌围栏、
// 事务式的「锁行 → 判定 → 落库」、互斥位。只用于测试。
package biztest

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/lanyulei/reborn/backend/job/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/jobcontract"
)

// Execution 内存里的一条执行记录（job_execution + 其主机与日志）。
type Execution struct {
	ID              string
	TemplateID      string
	Exclusive       bool
	Status          int32
	Spec            []byte
	RunnerZone      string
	CancelRequested bool
	RunnerID        string
	LeaseToken      string
	LeaseUntil      time.Time
	LogSeq          int64
	LogBytes        int64
	LogTruncated    bool
	CommitID        string
	Error           string
	HostOK          int32
	HostFailed      int32
	HostUnreachable int32
	Started         bool
	Finished        bool
	// Notified 模拟 job_execution.notify_time 非空（core 已发过结束通知）；恢复为执行中时清掉。
	Notified bool
	Hosts    []HostRow
	Logs     map[int64]string
	LogTimes map[int64]time.Time
	seq      int
}

// HostRow 内存里的一行执行主机。
type HostRow struct {
	biz.Host
	Status      int32
	OK          int32
	Changed     int32
	Failed      int32
	Unreachable int32
	Skipped     int32
}

// Store 内存仓储；实现 biz 的全部仓储接口。并发安全：状态在锁内读写，测试取状态也走加锁的方法
// （节点在端到端测试里一边跑一边上报，直接读字段会与写入竞争）。Err / MaterialErr 是测试在两次请求
// 之间设置的故障开关。
type Store struct {
	mu          sync.Mutex
	now         time.Time
	runners     map[string]*biz.Runner
	heartbeats  map[string]biz.Heartbeat
	lastSeen    map[string]time.Time
	execs       map[string]*Execution
	credentials map[string]*biz.Credential
	projects    map[string]*biz.Project
	payloads    map[string][]byte
	next        int
	// Err 非空时全部仓储方法返回它（模拟数据库故障）。
	Err error
	// MaterialErr 非空时 MaterialRepo 的方法返回它（模拟组装任务时的瞬时故障）。
	MaterialErr error
}

var (
	_ biz.RunnerRepo    = (*Store)(nil)
	_ biz.QueueRepo     = (*Store)(nil)
	_ biz.MaterialRepo  = (*Store)(nil)
	_ biz.PayloadRepo   = (*Store)(nil)
	_ biz.ProgressRepo  = (*Store)(nil)
	_ biz.HousekeepRepo = (*Store)(nil)
)

// NewStore new a Store；时钟从一个固定时刻起，由 Advance 推进。
func NewStore() *Store {
	return &Store{
		now:     time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		runners: map[string]*biz.Runner{}, heartbeats: map[string]biz.Heartbeat{}, lastSeen: map[string]time.Time{},
		execs: map[string]*Execution{}, credentials: map[string]*biz.Credential{}, projects: map[string]*biz.Project{},
		payloads: map[string][]byte{},
	}
}

// Advance 推进时钟。
func (s *Store) Advance(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = s.now.Add(d)
}

// AddRunner 登记一个节点。
func (s *Store) AddRunner(r biz.Runner) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runners[r.ID] = &r
}

// RemoveRunner 删除节点。
func (s *Store) RemoveRunner(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.runners, id)
}

// SetRunnerEnabled 启用 / 停用节点。
func (s *Store) SetRunnerEnabled(id string, enabled bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runners[id].Enabled = enabled
}

// AddCredential 登记凭据。
func (s *Store) AddCredential(id string, c biz.Credential) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.credentials[id] = &c
}

// RemoveCredential 删除凭据。
func (s *Store) RemoveCredential(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.credentials, id)
}

// AddProject 登记项目。
func (s *Store) AddProject(id string, p biz.Project) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.projects[id] = &p
}

// RemoveProject 删除项目。
func (s *Store) RemoveProject(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.projects, id)
}

// SetPayload 登记执行的分发文件内容（job_execution_payload）。
func (s *Store) SetPayload(executionID string, content []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.payloads[executionID] = append([]byte{}, content...)
}

// Payload implements biz.PayloadRepo.
func (s *Store) Payload(_ context.Context, executionID string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.MaterialErr != nil {
		return nil, s.MaterialErr
	}
	content, ok := s.payloads[executionID]
	if !ok {
		return nil, biz.ErrNotFound
	}
	return append([]byte{}, content...), nil
}

// AddExecution 建一条排队中的执行（按调用顺序排队）。
func (s *Store) AddExecution(e Execution) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e.Status == 0 {
		e.Status = jobcontract.StatusQueued
	}
	e.Logs, e.LogTimes = map[int64]string{}, map[int64]time.Time{}
	s.next++
	e.seq = s.next
	s.execs[e.ID] = &e
}

// Get 返回执行的一份拷贝；不存在返回 nil。
func (s *Store) Get(id string) *Execution {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.execs[id]
	if !ok {
		return nil
	}
	out := *e
	out.Hosts = append([]HostRow(nil), e.Hosts...)
	out.Logs = make(map[int64]string, len(e.Logs))
	for k, v := range e.Logs {
		out.Logs[k] = v
	}
	return &out
}

// Update 在锁内修改一条执行（测试用来模拟 core 的写入，如请求取消）。
func (s *Store) Update(id string, fn func(e *Execution)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(s.execs[id])
}

// Find implements biz.RunnerRepo.
func (s *Store) Find(_ context.Context, id string) (*biz.Runner, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return nil, s.Err
	}
	r, ok := s.runners[id]
	if !ok {
		return nil, biz.ErrNotFound
	}
	out := *r
	return &out, nil
}

// Heartbeat implements biz.RunnerRepo.
func (s *Store) Heartbeat(_ context.Context, id string, hb biz.Heartbeat) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return false, s.Err
	}
	r, ok := s.runners[id]
	if !ok {
		return false, biz.ErrNotFound
	}
	s.heartbeats[id], s.lastSeen[id] = hb, s.now
	return r.Enabled, nil
}

// HeartbeatRecord 一个节点最近一次心跳及其落库时刻。
type HeartbeatRecord struct {
	Heartbeat biz.Heartbeat
	LastSeen  time.Time
}

// HeartbeatsOf 返回全部节点最近一次心跳的快照（节点 ID → 心跳）。测试断言用：持锁拷贝，节点在别的
// 协程里上报时读到的也是完整的一份。
func (s *Store) HeartbeatsOf() map[string]HeartbeatRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]HeartbeatRecord, len(s.heartbeats))
	for id, hb := range s.heartbeats {
		out[id] = HeartbeatRecord{Heartbeat: hb, LastSeen: s.lastSeen[id]}
	}
	return out
}

// ClaimNext implements biz.QueueRepo.
func (s *Store) ClaimNext(_ context.Context, runnerID, zone, leaseToken string, lease time.Duration) (*biz.Claimed, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return nil, s.Err
	}
	queued := make([]*Execution, 0, len(s.execs))
	for _, e := range s.execs {
		if e.Status == jobcontract.StatusQueued && !e.CancelRequested && (e.RunnerZone == "" || e.RunnerZone == zone) {
			queued = append(queued, e)
		}
	}
	if len(queued) == 0 {
		return nil, nil
	}
	sort.Slice(queued, func(i, j int) bool { return queued[i].seq < queued[j].seq })
	e := queued[0]
	e.Status, e.RunnerID, e.LeaseToken = jobcontract.StatusRunning, runnerID, leaseToken
	e.LeaseUntil, e.Started = s.now.Add(lease), true
	return &biz.Claimed{ID: e.ID, Spec: append([]byte(nil), e.Spec...)}, nil
}

// Fail implements biz.QueueRepo.
func (s *Store) Fail(_ context.Context, executionID, leaseToken, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.execs[executionID]; ok && e.Status == jobcontract.StatusRunning && e.LeaseToken == leaseToken {
		e.Status, e.Finished, e.Error = jobcontract.StatusFailed, true, reason
	}
	return nil
}

// Release implements biz.QueueRepo。与 data 层一致：领取之后已被请求取消的执行落为已取消，不回队列。
func (s *Store) Release(_ context.Context, executionID, leaseToken string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.execs[executionID]; ok && e.Status == jobcontract.StatusRunning && e.LeaseToken == leaseToken {
		if e.CancelRequested {
			e.Status, e.Finished = jobcontract.StatusCanceled, true
		} else {
			e.Status = jobcontract.StatusQueued
		}
		e.RunnerID, e.LeaseToken, e.LeaseUntil, e.Started = "", "", time.Time{}, false
	}
	return nil
}

// Hosts implements biz.MaterialRepo.
func (s *Store) Hosts(_ context.Context, executionID string) ([]biz.Host, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.MaterialErr != nil {
		return nil, s.MaterialErr
	}
	e, ok := s.execs[executionID]
	if !ok {
		return nil, nil
	}
	out := make([]biz.Host, 0, len(e.Hosts))
	for _, h := range e.Hosts {
		out = append(out, h.Host)
	}
	return out, nil
}

// Credential implements biz.MaterialRepo.
func (s *Store) Credential(_ context.Context, id string) (*biz.Credential, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.MaterialErr != nil {
		return nil, s.MaterialErr
	}
	c, ok := s.credentials[id]
	if !ok {
		return nil, biz.ErrNotFound
	}
	out := *c
	return &out, nil
}

// Project implements biz.MaterialRepo.
func (s *Store) Project(_ context.Context, id string) (*biz.Project, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.MaterialErr != nil {
		return nil, s.MaterialErr
	}
	p, ok := s.projects[id]
	if !ok {
		return nil, biz.ErrNotFound
	}
	out := *p
	return &out, nil
}

func state(e *Execution) biz.ExecState {
	return biz.ExecState{
		Status: e.Status, RunnerID: e.RunnerID, LeaseToken: e.LeaseToken, LogSeq: e.LogSeq, LogBytes: e.LogBytes,
		LogTruncated: e.LogTruncated, CommitID: e.CommitID, CancelRequested: e.CancelRequested,
	}
}

// exclusiveTaken 报告同一模板是否已有别的未结束的互斥执行（uk_job_execution_exclusive）。
func (s *Store) exclusiveTaken(e *Execution) bool {
	if !e.Exclusive {
		return false
	}
	for _, o := range s.execs {
		if o.ID != e.ID && o.Exclusive && o.TemplateID == e.TemplateID && jobcontract.Active(o.Status) {
			return true
		}
	}
	return false
}

// Report implements biz.ProgressRepo.
func (s *Store) Report(_ context.Context, executionID string, decide func(cur biz.ExecState) (*biz.ReportMutation, error)) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return false, s.Err
	}
	e, ok := s.execs[executionID]
	if !ok {
		return false, biz.ErrNotFound
	}
	m, err := decide(state(e))
	if err != nil {
		return false, err
	}
	revived := false
	if m.Revive && e.Status == jobcontract.StatusUnknown && !s.exclusiveTaken(e) {
		e.Status, e.Finished, e.Error, e.Notified, revived = jobcontract.StatusRunning, false, "", false, true
	}
	for _, c := range m.Chunks {
		if _, dup := e.Logs[c.Seq]; !dup {
			e.Logs[c.Seq], e.LogTimes[c.Seq] = c.Content, s.now
		}
	}
	e.LogSeq, e.LogBytes, e.LogTruncated, e.CommitID = m.LogSeq, m.LogBytes, m.LogTruncated, m.CommitID
	e.LeaseUntil = s.now.Add(m.Lease)
	return revived, nil
}

// Finish implements biz.ProgressRepo.
func (s *Store) Finish(_ context.Context, executionID string, decide func(cur biz.ExecState) (*biz.FinishMutation, error)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return s.Err
	}
	e, ok := s.execs[executionID]
	if !ok {
		return biz.ErrNotFound
	}
	m, err := decide(state(e))
	if err != nil || m == nil {
		return err
	}
	for _, u := range m.Hosts {
		for i := range e.Hosts {
			if e.Hosts[i].Address == u.Address {
				h := &e.Hosts[i]
				h.Status, h.OK, h.Changed, h.Failed, h.Unreachable, h.Skipped = u.Status, u.OK, u.Changed, u.Failed, u.Unreachable, u.Skipped
			}
		}
	}
	e.HostOK, e.HostFailed, e.HostUnreachable = 0, 0, 0
	for _, h := range e.Hosts {
		switch h.Status {
		case jobcontract.HostOK, jobcontract.HostChanged:
			e.HostOK++
		case jobcontract.HostFailed:
			e.HostFailed++
		case jobcontract.HostUnreachable:
			e.HostUnreachable++
		}
	}
	e.Status, e.Finished, e.Error = m.Status, true, m.Error
	if m.CommitID != "" {
		e.CommitID = m.CommitID
	}
	return nil
}

// ExpireLeases implements biz.HousekeepRepo.
func (s *Store) ExpireLeases(_ context.Context, reason string, limit int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return 0, s.Err
	}
	n := 0
	for _, e := range s.execs {
		if n < limit && e.Status == jobcontract.StatusRunning && e.LeaseUntil.Before(s.now) {
			e.Status, e.Finished, e.Error = jobcontract.StatusUnknown, true, reason
			n++
		}
	}
	return n, nil
}

// PurgeLogs implements biz.HousekeepRepo.
func (s *Store) PurgeLogs(_ context.Context, retention time.Duration, limit int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return 0, s.Err
	}
	n := 0
	for _, e := range s.execs {
		if jobcontract.Active(e.Status) {
			continue
		}
		for seq, at := range e.LogTimes {
			if n < limit && at.Before(s.now.Add(-retention)) {
				delete(e.Logs, seq)
				delete(e.LogTimes, seq)
				n++
			}
		}
	}
	return n, nil
}

// Opener 测试用的凭据「解密器」：密文 = "enc:" + 明文；Fail 为 true 时一律失败（主密钥缺失）。
type Opener struct {
	Fail bool
}

var _ biz.SecretOpener = (*Opener)(nil)

// Seal 生成 Opener 可解的「密文」。
func Seal(plain string) []byte { return []byte("enc:" + plain) }

// Open implements biz.SecretOpener.
func (o *Opener) Open(ciphertext []byte) (string, error) {
	const prefix = "enc:"
	if o.Fail || len(ciphertext) < len(prefix) || string(ciphertext[:len(prefix)]) != prefix {
		return "", errors.New("cannot decrypt")
	}
	return string(ciphertext[len(prefix):]), nil
}

// Observer 记录指标调用的计数器。并发安全。
type Observer struct {
	mu     sync.Mutex
	Calls  map[string]int
	Events map[string]int
}

var _ biz.Observer = (*Observer)(nil)

// NewObserver new an Observer.
func NewObserver() *Observer {
	return &Observer{Calls: map[string]int{}, Events: map[string]int{}}
}

// ObserveCall implements biz.Observer.
func (o *Observer) ObserveCall(_ context.Context, op, result string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.Calls[op+"/"+result]++
}

// AddExecutions implements biz.Observer.
func (o *Observer) AddExecutions(_ context.Context, event string, n int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.Events[event] += n
}

// Call 返回某个「操作/结果」的计数。
func (o *Observer) Call(key string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.Calls[key]
}

// Event 返回某个事件的计数。
func (o *Observer) Event(name string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.Events[name]
}
