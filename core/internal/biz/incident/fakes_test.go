package biz

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"time"

	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
)

// 本文件是故障域用例单元测试共用的内存桩：只实现被测路径会碰到的方法，其余靠内嵌接口（调用即空指针 panic，
// 暴露测试覆盖之外的调用）。

type fakeTxm struct{}

func (fakeTxm) WithinTx(_ context.Context, fn func(tx Tx) error) error { return fn(fakeTx{}) }

type fakeTx struct{}

func (fakeTx) Exec(context.Context, string, ...any) (int64, error) { return 1, nil }
func (fakeTx) QueryRow(context.Context, string, ...any) txrun.Row  { return nil }

type fakeCursor struct {
	CursorRepo
	seq  int64
	sets []int64
	// seen 已处理序号；marks 每次 MarkSeenTx 的入参；pruned 每次 PruneBelow 的入参。
	seen   map[int64]struct{}
	marks  [][]int64
	pruned []int64
}

func (c *fakeCursor) Get(context.Context, string) (int64, error) { return c.seq, nil }
func (c *fakeCursor) SetTx(_ context.Context, _ Tx, _ string, seq int64) error {
	c.sets = append(c.sets, seq)
	c.seq = seq
	return nil
}

func (c *fakeCursor) MarkSeenTx(_ context.Context, _ Tx, seqs []int64) error {
	if c.seen == nil {
		c.seen = map[int64]struct{}{}
	}
	for _, s := range seqs {
		c.seen[s] = struct{}{}
	}
	c.marks = append(c.marks, seqs)
	return nil
}

func (c *fakeCursor) Seen(_ context.Context, from, to int64) (map[int64]struct{}, error) {
	out := map[int64]struct{}{}
	for s := range c.seen {
		if s > from && s <= to {
			out[s] = struct{}{}
		}
	}
	return out, nil
}

func (c *fakeCursor) PruneBelow(_ context.Context, seq int64) error {
	c.pruned = append(c.pruned, seq)
	return nil
}

type feedCall struct {
	after, until int64
	limit        int
}

type fakeAlerts struct {
	Alerts
	lookback   []*AlertSignal
	forward    []*AlertSignal
	rangeCalls []feedCall
	feedCalls  []feedCall
}

func (a *fakeAlerts) FeedRange(_ context.Context, after, until int64, limit int) ([]*AlertSignal, error) {
	a.rangeCalls = append(a.rangeCalls, feedCall{after: after, until: until, limit: limit})
	return a.lookback, nil
}

func (a *fakeAlerts) Feed(_ context.Context, after int64, limit int) ([]*AlertSignal, error) {
	a.feedCalls = append(a.feedCalls, feedCall{after: after, limit: limit})
	return a.forward, nil
}

type fakeRules struct{ RuleRepo }

func (fakeRules) ListEnabled(context.Context) ([]*Rule, error) { return nil, nil }

type fakeSettingRepo struct{ SettingRepo }

func (fakeSettingRepo) Get(context.Context) (*Settings, error) { return &Settings{}, nil }

var errBoom = errors.New("boom")

type fakeLinks struct {
	AlertLinkRepo
	// failAlert 查询该告警的活跃故障时报错（模拟单条处理失败）。
	failAlert string
}

func (l fakeLinks) ActiveIncident(_ context.Context, alertID string) (string, error) {
	if alertID != "" && alertID == l.failAlert {
		return "", errBoom
	}
	return "", nil
}

func (fakeLinks) ListAll(context.Context, string, int) ([]*AlertLink, error) { return nil, nil }

type fakeProposals struct {
	ProposalRepo
	get       *Proposal
	lockCalls int
	// list ListByStatusTx 返回的建议；saves 每次 SaveTx 的 (id, from, to)；created 每次 CreateTx 的建议。
	list    []*Proposal
	saves   []proposalSave
	created []*Proposal
}

type proposalSave struct {
	id       string
	from, to int32
}

func (p *fakeProposals) Get(context.Context, string) (*Proposal, error) { return p.get, nil }
func (p *fakeProposals) LockTx(context.Context, Tx, string) (*Proposal, error) {
	p.lockCalls++
	return p.get, nil
}

func (p *fakeProposals) ListByStatusTx(context.Context, Tx, string, []int32) ([]*Proposal, error) {
	return p.list, nil
}

func (p *fakeProposals) SaveTx(_ context.Context, _ Tx, pr *Proposal, from int32) (bool, error) {
	p.saves = append(p.saves, proposalSave{id: pr.ID, from: from, to: pr.Status})
	return true, nil
}

func (p *fakeProposals) CreateTx(_ context.Context, _ Tx, pr *Proposal) error {
	p.created = append(p.created, pr)
	return nil
}

type fakeIncidents struct {
	IncidentReader
	inc *Incident
}

func (r fakeIncidents) Get(context.Context, string) (*Incident, error) { return r.inc, nil }

type fakeEvents struct{ EventRepo }

func (fakeEvents) AppendTx(context.Context, Tx, *Event) error { return nil }

type fakePlaybooks struct {
	PlaybookRepo
	pb *Playbook
}

func (p fakePlaybooks) Get(context.Context, string) (*Playbook, error) { return p.pb, nil }

type fakeFlows struct{ Workflows }

func (fakeFlows) Startable(_ context.Context, key string) (string, bool, error) {
	return "流程 " + key, true, nil
}

type fakeActions struct {
	ActionRepo
	retries  []time.Duration
	finished []int32
}

func (a *fakeActions) Retry(_ context.Context, _ *Action, backoff time.Duration, _ string) error {
	a.retries = append(a.retries, backoff)
	return nil
}

func (a *fakeActions) FinishTx(_ context.Context, _ Tx, _ *Action, status int32, _ string) (bool, error) {
	a.finished = append(a.finished, status)
	return true, nil
}

type fakeWriter struct {
	IncidentWriter
	inc *Incident
}

func (w fakeWriter) LockTx(context.Context, Tx, string) (*Incident, error) { return w.inc, nil }
func (w fakeWriter) SaveTx(context.Context, Tx, *Incident) error           { return nil }

type fakeMembers struct{ MemberRepo }

func (fakeMembers) List(context.Context, string) ([]*Member, error) { return nil, nil }
func (fakeMembers) ListMany(context.Context, []string) (map[string][]*Member, error) {
	return map[string][]*Member{}, nil
}

type fakeAdmins struct{ Admins }

func (fakeAdmins) IsAdmin(context.Context, string) (bool, error) { return true, nil }

func testLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

func fireSignal(seq int64, alertID string) *AlertSignal {
	return &AlertSignal{Seq: seq, Kind: "fire", Alert: AlertRef{ID: alertID, Status: 1}}
}

// 以下桩供走完整写路径的用例使用（开故障 / 合并 / 重开 / 指派指挥 / 候选转正 / 改进项 / 动作 worker）。

// fakeDirectory 用户目录：ActiveUserIDs 原样返回给定的可用用户，UserNames 返回给定的显示名。
type fakeDirectory struct {
	Directory
	active []string
	names  map[string]string
}

// ActiveUserIDs 与真实实现同口径：从入参里过滤出可用的用户，保持入参顺序并去重。
func (d fakeDirectory) ActiveUserIDs(_ context.Context, ids []string) ([]string, error) {
	return slices.DeleteFunc(uniq(ids), func(id string) bool { return !slices.Contains(d.active, id) }), nil
}

func (d fakeDirectory) UserNames(context.Context, []string) (map[string]string, error) {
	return d.names, nil
}

// fakeNotify 记下写出的通知事件。
type fakeNotify struct{ events []NotifyEvent }

func (n *fakeNotify) NotifyEvent(_ context.Context, _ Tx, ev NotifyEvent) (*NotifyEvent, error) {
	n.events = append(n.events, ev)
	return &ev, nil
}

// fakeWebhookRepo 启用的 Webhook 订阅。
type fakeWebhookRepo struct {
	WebhookRepo
	enabled []*Webhook
}

func (r fakeWebhookRepo) ListEnabled(context.Context) ([]*Webhook, error) { return r.enabled, nil }

// fakeOutbox 记下入箱的出站箱行。
type fakeOutbox struct {
	OutboxRepo
	items []*OutboxItem
}

func (o *fakeOutbox) EnqueueTx(_ context.Context, _ Tx, items []*OutboxItem) error {
	o.items = append(o.items, items...)
	return nil
}

// mapWriter 按 ID 返回故障的写桩：记录建出的故障与每次写回。
type mapWriter struct {
	IncidentWriter
	byID    map[string]*Incident
	created []*Incident
	saved   []*Incident
}

func (w *mapWriter) LockTx(_ context.Context, _ Tx, id string) (*Incident, error) {
	return w.byID[id], nil
}

func (w *mapWriter) NextNumberTx(context.Context, Tx, time.Time) (int32, error) {
	return 1, nil
}

func (w *mapWriter) CreateTx(_ context.Context, _ Tx, in *Incident) error {
	w.created = append(w.created, in)
	return nil
}

func (w *mapWriter) SaveTx(_ context.Context, _ Tx, in *Incident) error {
	w.saved = append(w.saved, in)
	return nil
}

// recordingEvents 记下追加的时间线；Recent 返回 tail（时间升序）。
type recordingEvents struct {
	EventRepo
	appended []*Event
	tail     []*Event
}

func (e *recordingEvents) AppendTx(_ context.Context, _ Tx, ev *Event) error {
	e.appended = append(e.appended, ev)
	return nil
}

func (e *recordingEvents) Recent(context.Context, string, []string, int) ([]*Event, error) {
	return e.tail, nil
}

// stubLinks 带告警归属的关联桩：owner 为告警 → 所属活跃故障；attached 记下每次关联的告警。
type stubLinks struct {
	AlertLinkRepo
	owner    map[string]string
	links    []*AlertLink
	attached []string
}

func (l *stubLinks) ActiveIncident(_ context.Context, alertID string) (string, error) {
	return l.owner[alertID], nil
}

func (l *stubLinks) AttachTx(_ context.Context, _ Tx, link *AlertLink) (bool, error) {
	l.attached = append(l.attached, link.AlertID)
	return true, nil
}

func (l *stubLinks) ListAll(context.Context, string, int) ([]*AlertLink, error) { return l.links, nil }

func (l *stubLinks) Count(context.Context, string) (int, error)                 { return len(l.links), nil }
func (l *stubLinks) MoveAllTx(context.Context, Tx, string, string) (int, error) { return 0, nil }
func (l *stubLinks) ReleaseAllTx(context.Context, Tx, string) error             { return nil }

// stubAlerts 监控域告警桩：byID 为告警表，firing 为「仍在告警中」的条数。
type stubAlerts struct {
	Alerts
	byID   map[string]*AlertRef
	firing int
	noted  []string
	// mutedFiring 仍在告警中、但被屏蔽 / 抑制的条数：CountFiring 不计，AnyUnresolved 计。
	mutedFiring int
	// listCalls 每次 ListByIDs 的入参；readerCalls Reader 被调用的次数；unreadable 该用户看不到的分组。
	listCalls   [][]string
	readerCalls int
	unreadable  map[string]bool
}

// AnyUnresolved 与适配器同口径：告警中的都算，不论是否被屏蔽 / 抑制。
func (a *stubAlerts) AnyUnresolved(context.Context, []string) (bool, error) {
	return a.firing+a.mutedFiring > 0, nil
}

func (a *stubAlerts) Reader(context.Context, string) (func(*AlertRef) bool, error) {
	a.readerCalls++
	return func(r *AlertRef) bool { return !a.unreadable[r.GroupID] }, nil
}

func (a *stubAlerts) ListByIDs(_ context.Context, ids []string) ([]*AlertRef, error) {
	a.listCalls = append(a.listCalls, slices.Clone(ids))
	out := make([]*AlertRef, 0, len(ids))
	for _, id := range ids {
		if r := a.byID[id]; r != nil {
			out = append(out, r)
		}
	}
	return out, nil
}
func (a *stubAlerts) CountFiring(context.Context, []string) (int, error) { return a.firing, nil }
func (a *stubAlerts) Note(_ context.Context, alertID, _, _ string) error {
	a.noted = append(a.noted, alertID)
	return nil
}

// stubRules 规则读取桩。
type stubRules struct {
	RuleRepo
	rule *Rule
}

func (r stubRules) Get(context.Context, string) (*Rule, error) { return r.rule, nil }
func (r stubRules) ListByIDs(_ context.Context, ids []string) ([]*Rule, error) {
	if r.rule == nil || !slices.Contains(ids, r.rule.ID) {
		return nil, nil
	}
	return []*Rule{r.rule}, nil
}

// stubCandidates 候选故障桩：statuses 记下每次置状态；setErr 模拟并发处置后的围栏未命中。
type stubCandidates struct {
	CandidateRepo
	c        *Candidate
	setErr   error
	statuses []int32
}

func (c *stubCandidates) Get(context.Context, string) (*Candidate, error) { return c.c, nil }
func (c *stubCandidates) SetStatusTx(_ context.Context, _ Tx, _ string, status int32, _ string) error {
	c.statuses = append(c.statuses, status)
	return c.setErr
}

// stubItems 改进项桩：row 模拟库里当前这一行。SaveTx 与 data 层一样整体覆盖，两个窄更新只改目标列；
// 其余字段记录调用。
type stubItems struct {
	ActionItemRepo
	item     *ActionItem
	row      *ActionItem
	ticketed []*ActionItem
	saves    int
	tickets  []string
	marked   []string
	// markOK 为 false 时 MarkDoneTx 围栏未命中（期间已被手动完成）。
	markOK bool
}

func (s *stubItems) Get(context.Context, string) (*ActionItem, error) { return s.item, nil }
func (s *stubItems) ListTicketed(context.Context, int) ([]*ActionItem, error) {
	return s.ticketed, nil
}

func (s *stubItems) SaveTx(_ context.Context, _ Tx, it *ActionItem) error {
	s.saves++
	*s.row = *it
	return nil
}

func (s *stubItems) SetTicketTx(_ context.Context, _ Tx, id, ticketID, ticketNumber string) error {
	s.tickets = append(s.tickets, id+":"+ticketID+":"+ticketNumber)
	s.row.TicketID, s.row.TicketNumber = ticketID, ticketNumber
	return nil
}

func (s *stubItems) MarkDoneTx(_ context.Context, _ Tx, id string) (bool, error) {
	s.marked = append(s.marked, id)
	if !s.markOK {
		return false, nil
	}
	s.row.Status = ItemDone
	return true, nil
}

// stubTickets 工单域桩；onCreate / onView 在对应调用时执行（模拟跨域调用期间发生的并发编辑）。
type stubTickets struct {
	Tickets
	view     *TicketInfo
	created  []TicketCreate
	onCreate func()
	onView   func()
}

func (t *stubTickets) Template(context.Context, string, string) (*Named, error) {
	return &Named{ID: "tpl", Name: "默认模板"}, nil
}

func (t *stubTickets) Create(_ context.Context, p TicketCreate) (*TicketInfo, error) {
	t.created = append(t.created, p)
	if t.onCreate != nil {
		t.onCreate()
	}
	return &TicketInfo{ID: "t1", Number: "T-1"}, nil
}

func (t *stubTickets) View(context.Context, string, string) (*TicketInfo, error) {
	if t.onView != nil {
		t.onView()
	}
	return t.view, nil
}

// claimActions 动作发件箱桩：Claim 返回给定的一批；failDone 里的动作落账时报错；
// finished 记下成功落账的动作，retries 记下每个动作每次 Retry 的等待时长。
type claimActions struct {
	ActionRepo
	items    []*Action
	failDone map[string]error
	finished []string
	retries  map[string][]time.Duration
}

func (a *claimActions) Claim(context.Context, int, time.Duration) ([]*Action, error) {
	return a.items, nil
}
func (a *claimActions) EnqueueTx(context.Context, Tx, *Action) error { return nil }
func (a *claimActions) FinishTx(_ context.Context, _ Tx, act *Action, _ int32, _ string) (bool, error) {
	if err := a.failDone[act.ID]; err != nil {
		return false, err
	}
	a.finished = append(a.finished, act.ID)
	return true, nil
}

func (a *claimActions) Retry(_ context.Context, act *Action, backoff time.Duration, _ string) error {
	if a.retries == nil {
		a.retries = map[string][]time.Duration{}
	}
	a.retries[act.ID] = append(a.retries[act.ID], backoff)
	return nil
}

// inspectIncidents 记下巡检的筛选条件；List 返回给定的故障。
type inspectIncidents struct {
	IncidentReader
	inc      *Incident
	queries  []InspectQuery
	listed   []*Incident
	listCall int
}

func (r *inspectIncidents) Get(context.Context, string) (*Incident, error) { return r.inc, nil }
func (r *inspectIncidents) Inspect(_ context.Context, q InspectQuery, _ int) ([]*Incident, error) {
	r.queries = append(r.queries, q)
	return nil, nil
}

func (r *inspectIncidents) List(context.Context, IncidentFilter, int64, int64) ([]*Incident, int64, error) {
	r.listCall++
	return r.listed, int64(len(r.listed)), nil
}

func (r *inspectIncidents) ListByIDs(_ context.Context, ids []string) ([]*Incident, error) {
	var out []*Incident
	for _, it := range append([]*Incident{r.inc}, r.listed...) {
		if it != nil && slices.Contains(ids, it.ID) {
			out = append(out, it)
		}
	}
	return out, nil
}

// fencedProposals SaveTx 围栏未命中（返回 false）的建议桩。
type fencedProposals struct{ *fakeProposals }

func (p *fencedProposals) SaveTx(_ context.Context, _ Tx, pr *Proposal, from int32) (bool, error) {
	p.saves = append(p.saves, proposalSave{id: pr.ID, from: from, to: pr.Status})
	return false, nil
}

// fakeJobs 作业域桩：记录提交。
type fakeJobs struct {
	Jobs
	submits []JobSubmit
}

func (j *fakeJobs) Submit(_ context.Context, _ string, in JobSubmit) (*JobExecution, error) {
	j.submits = append(j.submits, in)
	return &JobExecution{ID: "e1"}, nil
}

// fakeWorkdays 工作日历桩：每个工作日按 24 小时算。
type fakeWorkdays struct{}

func (fakeWorkdays) AddWorkdays(_ context.Context, from time.Time, days int) (time.Time, error) {
	return from.Add(time.Duration(days) * 24 * time.Hour), nil
}
