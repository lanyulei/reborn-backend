package biz

import (
	"context"
	stderrors "errors"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	bizworkflow "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"

	"github.com/go-kratos/kratos/v3/errors"
)

// txStub 直接执行回调的事务管理器；rollbacks 记下回滚（回调返回错误）的次数。
type txStub struct{ rollbacks int }

func (m *txStub) WithinTx(_ context.Context, fn func(tx txrun.Tx) error) error {
	err := fn(nil)
	if err != nil {
		m.rollbacks++
	}
	return err
}

// insightStub 待通知的 SLA 超时（按 skip 过滤）与统计的调用计数。
type insightStub struct {
	TicketInsight
	breaches   []*SLABreach
	claims     [][]string
	statsCalls int
}

func (s *insightStub) ClaimBreachesTx(_ context.Context, _ txrun.Tx, _ int, skip []string) ([]*SLABreach, error) {
	s.claims = append(s.claims, slices.Clone(skip))
	var out []*SLABreach
	for _, b := range s.breaches {
		if !slices.Contains(skip, b.TicketID) {
			out = append(out, b)
		}
	}
	return out, nil
}

func (s *insightStub) Stats(_ context.Context, since time.Time, zone string) (*Stats, error) {
	s.statsCalls++
	return &Stats{Total: int64(s.statsCalls), Trend: []TrendPoint{{Day: since.Format(time.DateOnly)}}}, nil
}

// flowsStub 流程侧：各实例当前待处理的人、流程管理员；broken 中的实例一读就报错。
type flowsStub struct {
	WorkflowForms
	pending, admins map[string][]string
	broken          map[string]bool
}

func (f flowsStub) PendingUsers(_ context.Context, instanceID string) ([]string, error) {
	if f.broken[instanceID] {
		return nil, stderrors.New("instance state is unreadable")
	}
	return f.pending[instanceID], nil
}

func (f flowsStub) Admins(_ context.Context, instanceID string) ([]string, error) {
	return f.admins[instanceID], nil
}

// notifyStub 记录写入的通知事件；标题为 reject 的按业务错误拒收。
type notifyStub struct{ events []biznotify.Event }

func (n *notifyStub) NotifyEvent(_ context.Context, _ txrun.Tx, ev biznotify.Event) (*biznotify.Event, error) {
	if ev.Title == "N4 reject" {
		return nil, errors.BadRequest("NOTIFY_INVALID", "rejected")
	}
	n.events = append(n.events, ev)
	return &ev, nil
}

func newSLAUsecase(insight *insightStub, flows flowsStub, notify *notifyStub, txs *txStub) *Usecase {
	return &Usecase{txs: txs, ext: Extensions{
		Insight: insight, Flows: flows, Notify: notify, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}}
}

// 一张工单处理失败不拖住整批：它被排除后其余照常通知；无人待处理时落到流程管理员；
// 实在无人可通知的不报错；事件带明确收件人、不设分组键（否则会被通知域聚合掉收件人）。
func TestNotifySLA(t *testing.T) {
	insight := &insightStub{breaches: []*SLABreach{
		{TicketID: "t1", Number: "N1", Title: "a", InstanceID: "i1", Kind: "response", Escalate: []string{"boss"}},
		{TicketID: "t2", Number: "N2", Title: "b", InstanceID: "broken", Kind: "resolve"},
		{TicketID: "t3", Number: "N3", Title: "c", InstanceID: "i3", Kind: "resolve"},
		{TicketID: "t4", Number: "N4", Title: "reject", InstanceID: "i1", Kind: "resolve"},
		{TicketID: "t5", Number: "N5", Title: "e", InstanceID: "i5", Kind: "resolve"},
	}}
	flows := flowsStub{
		pending: map[string][]string{"i1": {"handler", "boss"}},
		admins:  map[string][]string{"i3": {"admin"}},
		broken:  map[string]bool{"broken": true},
	}
	notify, txs := &notifyStub{}, &txStub{}
	uc := newSLAUsecase(insight, flows, notify, txs)
	n, err := uc.NotifySLA(context.Background())
	if err != nil {
		t.Fatalf("NotifySLA() error = %v", err)
	}
	// 第一批因 t2 回滚（事件随之作废，这里的桩不模拟回滚，故只看第二批）；第二批排除 t2。
	if txs.rollbacks != 1 || len(insight.claims) != 2 || !slices.Equal(insight.claims[1], []string{"t2"}) {
		t.Fatalf("rollbacks = %d, claims = %v; want one rollback and a retry that skips t2", txs.rollbacks, insight.claims)
	}
	if n != 4 {
		t.Fatalf("NotifySLA() = %d, want 4 (the batch without the broken ticket)", n)
	}
	second := notify.events[1:] // 第一批在 t2 之前只写了 t1 的一条
	want := map[string][]string{"N1 a": {"boss", "handler"}, "N3 c": {"admin"}}
	if len(second) != len(want) {
		t.Fatalf("events = %+v, want %d", second, len(want))
	}
	for _, ev := range second {
		if !slices.Equal(ev.Recipients, want[ev.Title]) || ev.GroupKey != "" || ev.EventType != EventSLABreached {
			t.Errorf("event %q: recipients %v group %q", ev.Title, ev.Recipients, ev.GroupKey)
		}
	}
}

// 处理失败的工单再多也不拖住其余的：单轮新暂缓到上限即报错，暂缓名单跨轮保留，
// 下一轮直接绕开已知有问题的，最终把正常的那张通知出去。
func TestNotifySLAParksFailures(t *testing.T) {
	insight := &insightStub{}
	for _, id := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		insight.breaches = append(insight.breaches, &SLABreach{TicketID: id, InstanceID: "broken"})
	}
	insight.breaches = append(insight.breaches, &SLABreach{TicketID: "z", Number: "N9", Title: "ok", InstanceID: "i1", Kind: "resolve"})
	txs, notify := &txStub{}, &notifyStub{}
	flows := flowsStub{broken: map[string]bool{"broken": true}, pending: map[string][]string{"i1": {"handler"}}}
	uc := newSLAUsecase(insight, flows, notify, txs)
	if _, err := uc.NotifySLA(context.Background()); err == nil || txs.rollbacks != slaMaxSkips {
		t.Fatalf("round 1: error = %v after %d rollbacks, want an error after %d", err, txs.rollbacks, slaMaxSkips)
	}
	if _, err := uc.NotifySLA(context.Background()); err != nil {
		t.Fatalf("round 2: error = %v, want the remaining failures parked and the healthy ticket notified", err)
	}
	if len(notify.events) == 0 || notify.events[len(notify.events)-1].Title != "N9 ok" {
		t.Fatalf("events = %+v, want the healthy ticket notified", notify.events)
	}
	before := txs.rollbacks
	if _, err := uc.NotifySLA(context.Background()); err != nil || txs.rollbacks != before {
		t.Fatalf("round 3: error = %v, rollbacks %d -> %d; parked tickets must be skipped up front", err, before, txs.rollbacks)
	}
}

// 统计结果短时缓存：同一窗口与时区的重复查询不再落到数据库；时区不同各算各的；未知时区拒绝。
func TestStatsCache(t *testing.T) {
	insight := &insightStub{}
	uc := &Usecase{ext: Extensions{Insight: insight}}
	ctx := context.Background()
	first, err := uc.Stats(ctx, 7, "Asia/Shanghai")
	if err != nil {
		t.Fatalf("Stats() error = %v", err)
	}
	if again, _ := uc.Stats(ctx, 7, "Asia/Shanghai"); again != first || insight.statsCalls != 1 {
		t.Fatalf("second Stats() hit the store again (calls = %d)", insight.statsCalls)
	}
	if first.Timezone != "Asia/Shanghai" {
		t.Errorf("Timezone = %q", first.Timezone)
	}
	// 窗口从该时区 6 天前的零点起算。
	loc, _ := time.LoadLocation("Asia/Shanghai")
	if want := time.Now().In(loc).AddDate(0, 0, -6).Format(time.DateOnly); first.Trend[0].Day != want {
		t.Errorf("window starts on %s, want %s", first.Trend[0].Day, want)
	}
	if _, err := uc.Stats(ctx, 7, "UTC"); err != nil || insight.statsCalls != 2 {
		t.Fatalf("Stats(other zone) calls = %d, err %v", insight.statsCalls, err)
	}
	if _, err := uc.Stats(ctx, 30, ""); err != nil || insight.statsCalls != 3 {
		t.Fatalf("Stats(other window) calls = %d, err %v", insight.statsCalls, err)
	}
	if _, err := uc.Stats(ctx, 7, "Mars/Olympus"); !errors.Is(err, ErrTimezoneUnknown) {
		t.Fatalf("Stats(unknown zone) error = %v, want ErrTimezoneUnknown", err)
	}
}

var _ bizworkflow.VariableOwner = (*StatusSyncer)(nil)
