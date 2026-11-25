package biz

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// ShellIO 终端会话的输入输出。
type ShellIO struct {
	Stdin  io.Reader
	Stdout io.Writer
	Resize <-chan k8scontract.ExecResize
}

// ShellConn 一条已与集群建立的终端连接。
type ShellConn interface {
	// Run 泵送输入输出直到容器内的 shell 退出（返回 nil）、出错或 ctx 取消。
	Run(ctx context.Context, sio ShellIO) error
}

// Shell 以用户身份打开容器终端（data/kube 实现）。能否进入容器由集群判定。
type Shell interface {
	Open(ctx context.Context, c *Cluster, id Identity, p k8scontract.ExecParams) (ShellConn, error)
}

// ExecSession 终端会话记录（k8s_exec_session）。
type ExecSession struct {
	ID          string
	Identity    Identity
	ClusterID   string
	ClusterName string
	Params      k8scontract.ExecParams
}

// ExecRepo 终端会话记录的写入。
type ExecRepo interface {
	Start(ctx context.Context, s *ExecSession) error
	Finish(ctx context.Context, id, reason string, bytesIn, bytesOut int64) error
	// Record 追加一块录制内容（data 为空时只更新 truncated 标记）。
	Record(ctx context.Context, id string, seq int, data []byte, truncated bool) error
}

// Sink 向已接入的客户端写出一帧（类型 + 载荷）；返回错误表示连接已不可用。
type Sink func(typ byte, payload []byte) error

// EncodeFrame 编码一帧：首字节为类型，其余为载荷。
func EncodeFrame(typ byte, payload []byte) []byte {
	out := make([]byte, 1+len(payload))
	out[0] = typ
	copy(out[1:], payload)
	return out
}

// DecodeFrame 解码一帧；空帧或超长帧返回错误。payload 引用入参，不拷贝。
func DecodeFrame(b []byte) (typ byte, payload []byte, err error) {
	if len(b) == 0 {
		return 0, nil, errors.New("empty frame")
	}
	if len(b) > k8scontract.ExecMaxFrameSize+1 {
		return 0, nil, errors.New("frame too large")
	}
	return b[0], b[1:], nil
}

// Ring 定长环形缓冲：只保留最近写入的 size 字节（断线期间的终端输出）。非并发安全。
type Ring struct {
	buf   []byte
	start int
	n     int
}

// NewRing 构造容量为 size 的环形缓冲。
func NewRing(size int) *Ring { return &Ring{buf: make([]byte, size)} }

// Write 追加数据，超出容量时丢弃最旧的字节。
func (r *Ring) Write(p []byte) {
	size := len(r.buf)
	if size == 0 {
		return
	}
	if len(p) >= size {
		copy(r.buf, p[len(p)-size:])
		r.start, r.n = 0, size
		return
	}
	for _, b := range p {
		r.buf[(r.start+r.n)%size] = b
		if r.n < size {
			r.n++
		} else {
			r.start = (r.start + 1) % size
		}
	}
}

// Bytes 按写入顺序返回缓冲内容的拷贝。
func (r *Ring) Bytes() []byte {
	out := make([]byte, r.n)
	size := len(r.buf)
	for i := range r.n {
		out[i] = r.buf[(r.start+i)%size]
	}
	return out
}

// Reset 清空。
func (r *Ring) Reset() { r.start, r.n = 0, 0 }

// ExecLimits 终端会话的时间限制（测试可缩短）。
type ExecLimits struct {
	DetachGrace time.Duration
	IdleTimeout time.Duration
	MaxDuration time.Duration
	Tick        time.Duration
	// RecordMax 单个会话录制的输出字节上限；0 = 不录制。
	RecordMax int64
}

func defaultExecLimits() ExecLimits {
	return ExecLimits{
		DetachGrace: k8scontract.ExecDetachGrace, IdleTimeout: k8scontract.ExecIdleTimeout,
		MaxDuration: k8scontract.ExecMaxDuration, Tick: 2 * time.Second,
		RecordMax: k8scontract.DefaultMaxRecordBytes,
	}
}

// session 一个终端会话。mu 保护 sink / attach / detachedAt / ring / reason；计数与活跃时间用原子量，
// 避免输入输出两条路径互相等锁。
type session struct {
	id        string
	userID    string
	clusterID string
	started   time.Time

	stdinW *io.PipeWriter
	resize chan k8scontract.ExecResize
	cancel context.CancelFunc
	done   chan struct{}

	bytesIn    atomic.Int64
	bytesOut   atomic.Int64
	lastActive atomic.Int64 // UnixNano

	mu         sync.Mutex
	sink       Sink
	attach     uint64 // 每次接入 +1；旧接入的 Detach 据此变为空操作
	detachedAt time.Time
	ring       *Ring
	reason     string

	// rec 录制器；nil = 不录制。创建后只读，自带锁。
	rec *recorder
}

// Write implements io.Writer：终端输出。已接入则直接下发，否则进入环形缓冲等待接回。
func (s *session) Write(p []byte) (int, error) {
	s.bytesOut.Add(int64(len(p)))
	if s.rec != nil {
		s.rec.output(p)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sink != nil {
		if err := s.sink(k8scontract.FrameOutput, p); err == nil {
			return len(p), nil
		}
		s.sink, s.detachedAt = nil, time.Now()
	}
	s.ring.Write(p)
	return len(p), nil
}

func (s *session) end(reason string) {
	s.mu.Lock()
	if s.reason == "" {
		s.reason = reason
	}
	s.mu.Unlock()
	s.cancel()
}

// ExecHandle 一次接入：service 层把 WebSocket 的读循环接到它上面。
type ExecHandle struct {
	s      *session
	attach uint64
}

// SessionID 会话 ID。
func (h *ExecHandle) SessionID() string { return h.s.id }

// Done 会话结束时关闭。
func (h *ExecHandle) Done() <-chan struct{} { return h.s.done }

// Input 键盘输入。
func (h *ExecHandle) Input(p []byte) error {
	h.s.bytesIn.Add(int64(len(p)))
	h.s.lastActive.Store(time.Now().UnixNano())
	_, err := h.s.stdinW.Write(p)
	return err
}

// 终端窗口大小的上限。客户端给出的大小会进录制、回放时用它调整终端：不设上限，一个伪造的 65535×65535
// 就能让回放页面申请几十 GB 的缓冲。
const (
	maxTermCols = 500
	maxTermRows = 200
)

// ClampTermSize 把窗口大小收进 [1, 上限]。
func ClampTermSize(cols, rows uint16) (uint16, uint16) {
	return min(max(cols, 1), maxTermCols), min(max(rows, 1), maxTermRows)
}

// Resize 窗口大小变化；通道满时丢弃（下一次变化会覆盖）。
func (h *ExecHandle) Resize(r k8scontract.ExecResize) {
	r.Cols, r.Rows = ClampTermSize(r.Cols, r.Rows)
	if h.s.rec != nil {
		h.s.rec.resize(r.Cols, r.Rows)
	}
	select {
	case h.s.resize <- r:
	default:
	}
}

// Detach 连接中断：会话保留一段时间等待接回。被新的接入顶替后为空操作。
func (h *ExecHandle) Detach() {
	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	if h.s.attach == h.attach && h.s.sink != nil {
		h.s.sink, h.s.detachedAt = nil, time.Now()
	}
}

// Close 客户端主动结束会话。
func (h *ExecHandle) Close() { h.s.end(k8scontract.ExecEndClientClose) }

// ExecManager 终端会话管理：并发上限、断线保留与接回、空闲与最长时长、会话记录与审计。
type ExecManager struct {
	registry *Registry
	prober   *Prober
	scopes   *ScopeSyncer // nil = 不检查授权同步（仅单元测试）
	shell    Shell
	repo     ExecRepo
	auditor  *Auditor
	obs      Observer
	log      *slog.Logger
	limits   ExecLimits

	mu       sync.Mutex
	sessions map[string]*session
	wg       sync.WaitGroup
	closed   bool
}

// NewExecManager new an ExecManager.
func NewExecManager(registry *Registry, prober *Prober, scopes *ScopeSyncer, shell Shell, repo ExecRepo, auditor *Auditor, obs Observer, log *slog.Logger) *ExecManager {
	m := &ExecManager{
		registry: registry, prober: prober, scopes: scopes, shell: shell, repo: repo, auditor: auditor, obs: obs, log: log,
		limits: defaultExecLimits(), sessions: map[string]*session{},
	}
	registry.OnDrop(m.dropCluster)
	return m
}

// WithLimits 覆盖时间限制（测试用）。
func (m *ExecManager) WithLimits(l ExecLimits) { m.limits = l }

// Attach 新建会话，或接回 p.SessionID 指定的会话（仅发起用户可接回）。成功后 sink 立即收到一帧状态
// （ready / resumed），接回时随后回放断线期间的输出。
func (m *ExecManager) Attach(ctx context.Context, clusterID string, id Identity, p k8scontract.ExecParams, sink Sink) (*ExecHandle, error) {
	if p.SessionID != "" {
		return m.resume(clusterID, id, p.SessionID, sink)
	}
	c, err := m.registry.Owned(clusterID)
	if err != nil {
		return nil, err
	}
	if !m.prober.Online(c.ID) {
		return nil, ErrClusterOffline("cluster %s is unreachable", c.Name)
	}
	if m.scopes != nil && !m.scopes.Ready(c) {
		// 与其余入口同一道门：集群内的绑定尚未与当前授权一致时不放行（否则可能按已收回的授权进入容器）。
		return nil, ErrClusterOffline("cluster %s: authorisation sync pending, retry in a few seconds", c.Name)
	}
	if c.ExecDisabled {
		return nil, errExecDisabled
	}
	if p.Namespace == "" || p.Pod == "" {
		return nil, errInvalid("namespace and pod are required")
	}
	if p.Kind == 0 {
		p.Kind = k8scontract.ExecKindShell
	}
	if p.Kind != k8scontract.ExecKindShell && p.Kind != k8scontract.ExecKindDebug {
		return nil, ErrUnsupported("unsupported terminal kind %d", p.Kind)
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, ErrClusterOffline("service is shutting down")
	}
	count := 0
	for _, s := range m.sessions {
		if s.userID == id.UserID {
			count++
		}
	}
	m.mu.Unlock()
	if count >= k8scontract.MaxExecSessions {
		return nil, errTooManyStreams
	}

	audit := &AuditEvent{
		Ref:  k8scontract.Ref{Version: "v1", Resource: "pods", Namespace: p.Namespace, Name: p.Pod},
		Kind: "Pod", Action: k8scontract.ActionExec,
	}
	if p.Kind == k8scontract.ExecKindDebug {
		audit.Action = k8scontract.ActionDebug
	}
	sid, err := idgen.NewID() // 先于 Open：之后的失败路径都必须关闭已建立的连接
	if err != nil {
		return nil, err
	}
	conn, err := m.shell.Open(ctx, c, id, p)
	if err != nil {
		audit.Err = err
		audit.Patch = map[string]any{"container": p.Container}
		m.auditor.Record(ctx, c, id, []*AuditEvent{audit})
		return nil, err
	}
	audit.Patch = map[string]any{"container": p.Container, "sessionId": sid}

	sctx, cancel := context.WithCancel(context.WithoutCancel(ctx)) // 会话生命周期独立于建立它的那条连接
	stdinR, stdinW := io.Pipe()
	s := &session{
		id: sid, userID: id.UserID, clusterID: c.ID, started: time.Now(),
		stdinW: stdinW, resize: make(chan k8scontract.ExecResize, 4), cancel: cancel, done: make(chan struct{}),
		ring: NewRing(k8scontract.ExecReplayBytes), sink: sink, attach: 1,
	}
	s.lastActive.Store(time.Now().UnixNano())
	if m.limits.RecordMax > 0 {
		s.rec = newRecorder(m.limits.RecordMax, s.started)
	}
	if p.Cols > 0 && p.Rows > 0 {
		p.Cols, p.Rows = ClampTermSize(p.Cols, p.Rows)
		s.resize <- k8scontract.ExecResize{Cols: p.Cols, Rows: p.Rows}
		if s.rec != nil {
			s.rec.resize(p.Cols, p.Rows)
		}
	}
	// Open 可能很慢（握手、启动调试容器）：其间服务可能开始退出、同一用户的其他会话也可能建立。
	// 登记之前复查，否则并发的接入能越过每用户上限，或在 Shutdown 之后留下无人回收的会话。
	var refuse error
	m.mu.Lock()
	count = 0
	for _, o := range m.sessions {
		if o.userID == id.UserID {
			count++
		}
	}
	switch {
	case m.closed:
		refuse = ErrClusterOffline("service is shutting down")
	case count >= k8scontract.MaxExecSessions:
		refuse = errTooManyStreams
	default:
		m.sessions[sid] = s
		m.wg.Add(1)
	}
	m.mu.Unlock()
	if refuse != nil {
		cancel()
		discardShell(conn)
		return nil, refuse
	}
	m.auditor.Record(ctx, c, id, []*AuditEvent{audit})
	rec := &ExecSession{ID: sid, Identity: id, ClusterID: c.ID, ClusterName: c.Name, Params: p}
	// 会话记录的写入有时限：数据库卡顿时不能卡在这里——会话已登记，却没有管理循环（空闲 / 最长时长靠它收尾）。
	wctx, cancel := context.WithTimeout(sctx, 5*time.Second)
	defer cancel()
	if err := m.repo.Start(wctx, rec); err != nil {
		m.log.Error("write exec session failed", "session", sid, "error", err)
	}
	m.obs.StreamDelta("exec", 1)
	if err := sendStatus(sink, k8scontract.ExecStatus{State: k8scontract.ExecReady, SessionID: sid}); err != nil {
		s.mu.Lock()
		s.sink, s.detachedAt = nil, time.Now()
		s.mu.Unlock()
	}
	go m.run(sctx, s, conn, stdinR) // 退出路径：shell 结束、限制到期或 Shutdown 取消 sctx；wg 在 Shutdown 中收齐
	return &ExecHandle{s: s, attach: 1}, nil
}

func (m *ExecManager) resume(clusterID string, id Identity, sessionID string, sink Sink) (*ExecHandle, error) {
	m.mu.Lock()
	s := m.sessions[sessionID]
	m.mu.Unlock()
	// 不存在、不属于该用户、不在该集群，一律按不存在应答：不泄露他人会话是否存在。
	if s == nil || s.userID != id.UserID || s.clusterID != clusterID {
		return nil, ErrNotFound("terminal session not found or already closed")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// 新接入失败时原样恢复：此前仍连着的客户端不应因为一次失败的接回而被踢掉。
	prevSink, prevDetached := s.sink, s.detachedAt
	replay := s.ring.Bytes()
	s.ring.Reset()
	fail := func() (*ExecHandle, error) {
		s.ring.Write(replay)
		s.sink, s.detachedAt = prevSink, prevDetached
		return nil, ErrClusterOffline("terminal connection lost while resuming")
	}
	if err := sendStatus(sink, k8scontract.ExecStatus{State: k8scontract.ExecResumed, SessionID: s.id}); err != nil {
		return fail()
	}
	for rest := replay; len(rest) > 0; {
		n := min(len(rest), 32<<10)
		if err := sink(k8scontract.FrameOutput, rest[:n]); err != nil {
			return fail()
		}
		rest = rest[n:]
	}
	s.attach++
	s.sink, s.detachedAt = sink, time.Time{}
	s.lastActive.Store(time.Now().UnixNano())
	return &ExecHandle{s: s, attach: s.attach}, nil
}

func sendStatus(sink Sink, st k8scontract.ExecStatus) error {
	raw, err := json.Marshal(st)
	if err != nil {
		return err
	}
	return sink(k8scontract.FrameStatus, raw)
}

func (m *ExecManager) run(ctx context.Context, s *session, conn ShellConn, stdin *io.PipeReader) {
	defer m.wg.Done()
	result := make(chan error, 1)
	go func() { // 退出路径：conn.Run 在 ctx 取消后返回
		defer func() {
			if p := recover(); p != nil {
				m.log.Error("terminal session panicked", "session", s.id, "panic", p)
				result <- NewError(500, k8scontract.ReasonUnspecified, "terminal session failed unexpectedly")
			}
		}()
		result <- conn.Run(ctx, ShellIO{Stdin: stdin, Stdout: s, Resize: s.resize})
	}()
	ticker := time.NewTicker(m.limits.Tick)
	defer ticker.Stop()
	var runErr error
loop:
	for {
		select {
		case runErr = <-result:
			break loop
		case now := <-ticker.C:
			if reason := m.expired(s, now); reason != "" {
				s.end(reason)
			}
			m.flushRecord(ctx, s, now, false)
		}
	}
	s.cancel()
	_ = stdin.CloseWithError(io.EOF)
	_ = s.stdinW.Close()

	s.mu.Lock()
	reason := s.reason
	if reason == "" {
		reason = k8scontract.ExecEndContainerExit
		if runErr != nil {
			reason = k8scontract.ExecEndError
		}
		s.reason = reason
	}
	sink := s.sink
	s.sink = nil
	s.mu.Unlock()
	if sink != nil {
		st := k8scontract.ExecStatus{State: k8scontract.ExecClosed, SessionID: s.id, Reason: reason}
		if runErr != nil && reason == k8scontract.ExecEndError {
			st.Message = AsErrorMessage(runErr)
		}
		_ = sendStatus(sink, st)
	}
	m.mu.Lock()
	delete(m.sessions, s.id)
	m.mu.Unlock()
	m.obs.StreamDelta("exec", -1)
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	m.flushRecord(fctx, s, time.Now(), true)
	if err := m.repo.Finish(fctx, s.id, reason, s.bytesIn.Load(), s.bytesOut.Load()); err != nil {
		m.log.Error("finish exec session failed", "session", s.id, "error", err)
	}
	close(s.done)
}

// AsErrorMessage 给终端用户看的错误文本：业务错误取 message，其余不外泄细节。
func AsErrorMessage(err error) string { return AsError(err).Message }

func (m *ExecManager) expired(s *session, now time.Time) string {
	if now.Sub(s.started) >= m.limits.MaxDuration {
		return k8scontract.ExecEndMaxDuration
	}
	if now.Sub(time.Unix(0, s.lastActive.Load())) >= m.limits.IdleTimeout {
		return k8scontract.ExecEndIdleTimeout
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sink == nil && !s.detachedAt.IsZero() && now.Sub(s.detachedAt) >= m.limits.DetachGrace {
		return k8scontract.ExecEndDetachTimeout
	}
	return ""
}

// dropCluster 归属转移或集群移除：结束其上的全部会话。
func (m *ExecManager) dropCluster(clusterID string) {
	m.mu.Lock()
	var hit []*session
	for _, s := range m.sessions {
		if s.clusterID == clusterID {
			hit = append(hit, s)
		}
	}
	m.mu.Unlock()
	for _, s := range hit {
		s.end(k8scontract.ExecEndShutdown)
	}
}

// Shutdown 结束全部会话并等待收尾（会话记录落库）；ctx 到期则不再等待。
func (m *ExecManager) Shutdown(ctx context.Context) {
	m.mu.Lock()
	m.closed = true
	all := make([]*session, 0, len(m.sessions))
	for _, s := range m.sessions {
		all = append(all, s)
	}
	m.mu.Unlock()
	for _, s := range all {
		s.end(k8scontract.ExecEndShutdown)
	}
	done := make(chan struct{})
	go func() { // 退出路径：全部会话的 run 返回
		m.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

// flushRecord 把会话的录制缓冲落库（管理循环与会话结束时调用，不在输出路径上）。落库失败只记日志：
// 该块丢失，录制标记为不完整由后续的块带上。
func (m *ExecManager) flushRecord(ctx context.Context, s *session, now time.Time, force bool) {
	if s.rec == nil {
		return
	}
	seq, chunk, truncated, ok := s.rec.take(now, force)
	if !ok {
		return
	}
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := m.repo.Record(wctx, s.id, seq, chunk, truncated); err != nil {
		m.log.Error("write exec record failed", "session", s.id, "seq", seq, "error", err)
		s.rec.lost() // 这一块丢了：录制不完整，随下一次落库标记出来
	}
}

// discardShell 关闭一条已建立但不会被使用的终端连接：以已取消的上下文运行一次，让实现走完自己的清理路径。
func discardShell(conn ShellConn) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pr, pw := io.Pipe()
	_ = pw.Close()
	_ = conn.Run(ctx, ShellIO{Stdin: pr, Stdout: io.Discard})
}
