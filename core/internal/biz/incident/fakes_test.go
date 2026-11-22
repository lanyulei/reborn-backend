package biz

import (
	"context"
	"errors"
	"log/slog"
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

type fakeAdmins struct{ Admins }

func (fakeAdmins) IsAdmin(context.Context, string) (bool, error) { return true, nil }

func testLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

func fireSignal(seq int64, alertID string) *AlertSignal {
	return &AlertSignal{Seq: seq, Kind: "fire", Alert: AlertRef{ID: alertID, Status: 1}}
}
