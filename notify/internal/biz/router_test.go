package biz

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// routerTxStub 记录批事务内的全部写（无真实事务：错误即视为整批失败）。
type routerTxStub struct {
	marked      map[string]int32
	deferred    map[string]time.Time
	dispatches  []*Dispatch
	aggs        []AggIncrement
	suppressed  map[string]bool
	subscribers []Subscriber
	subsErr     error
	subsCalls   int
	users       map[string]string // 用户ID → 邮箱（显式收件解析）
}

func newRouterTxStub() *routerTxStub {
	return &routerTxStub{marked: map[string]int32{}, deferred: map[string]time.Time{}, suppressed: map[string]bool{}}
}

func (s *routerTxStub) MarkEvents(_ context.Context, marks []EventMark) error {
	for _, m := range marks {
		s.marked[m.ID] = m.Status
	}
	return nil
}

func (s *routerTxStub) DeferEvent(_ context.Context, id string, _ time.Time, next time.Time) error {
	s.deferred[id] = next
	return nil
}

func (s *routerTxStub) InsertDispatches(_ context.Context, ds []*Dispatch) error {
	s.dispatches = append(s.dispatches, ds...)
	return nil
}

func (s *routerTxStub) UpsertAggGroups(_ context.Context, incs []AggIncrement) (map[string]AggVerdict, error) {
	s.aggs = append(s.aggs, incs...)
	out := map[string]AggVerdict{}
	for _, inc := range incs {
		out[inc.Key] = AggVerdict{Suppressed: s.suppressed[inc.Key]}
	}
	return out, nil
}

func (s *routerTxStub) ListSubscribers(context.Context, string, string) ([]Subscriber, error) {
	s.subsCalls++
	return s.subscribers, s.subsErr
}

func (s *routerTxStub) LookupUserEmails(_ context.Context, ids []string) (map[string]string, error) {
	out := map[string]string{}
	for _, id := range ids {
		if email, ok := s.users[id]; ok {
			out[id] = email
		}
	}
	return out, nil
}

// eventSourceStub 把给定事件交给回调（模拟一批声明）。
type eventSourceStub struct {
	events []*Event
	tx     *routerTxStub
}

func (s *eventSourceStub) ProcessPending(ctx context.Context, _ int, fn func(context.Context, RouterTx, []*Event) error) (int, error) {
	if len(s.events) == 0 {
		return 0, nil
	}
	if err := fn(ctx, s.tx, s.events); err != nil {
		return 0, err
	}
	return len(s.events), nil
}

func newRouter(t *testing.T, snap *Snapshot, events ...*Event) (*RouterUsecase, *routerTxStub) {
	t.Helper()
	tx := newRouterTxStub()
	uc := NewRouterUsecase(&eventSourceStub{events: events, tx: tx}, newTestCache(t, snap), testLogger())
	return uc, tx
}

func TestRouterUsecase_RouteBatch(t *testing.T) {
	t.Parallel()
	routes := []*Route{
		{
			ID: routeID, BizID: "default", EventPattern: "monitor.*", SeverityMin: 1, Priority: 5, Enabled: true,
			ChannelIDs: []string{chEmail, chHook}, Recipient: RouteRecipient{Fixed: []string{"ops@example.com"}, Subscription: true},
		},
		{
			ID: routeAgg, BizID: "default", EventPattern: "host.down", SeverityMin: 1, Priority: 5, Enabled: true,
			ChannelIDs: []string{chHook}, Recipient: RouteRecipient{Fixed: []string{"x"}}, AggregateWindowS: 60,
		},
	}
	silences := []*Silence{{ID: "s", BizID: "default", EventPattern: "monitor.silent", SeverityMax: 4, Enabled: true}}
	snap := testSnapshot(routes, silences, nil)

	silent := testEvent("e-silent", "monitor.silent", 2)
	noRoute := testEvent("e-noroute", "resource.change", 2)
	direct := testEvent("e-direct", "monitor.alert.firing", 3)
	direct.Recipients = []string{"explicit@example.com", "018f3e1a-0a09-7000-8000-000000000001", "$group:oncall", "bogus"}
	aggB1 := testEvent("e-agg-b1", "host.down", 2)
	aggB1.GroupKey = "room-b"
	aggA1 := testEvent("e-agg-a1", "host.down", 2)
	aggA1.GroupKey = "room-a"
	aggA2 := testEvent("e-agg-a2", "host.down", 2)
	aggA2.GroupKey = "room-a"

	uc, tx := newRouter(t, snap, silent, noRoute, direct, aggB1, aggA1, aggA2)
	tx.subscribers = []Subscriber{{UserID: "u1", Email: "u1@example.com", Prefs: map[string]any{"email": true}}}
	tx.users = map[string]string{"018f3e1a-0a09-7000-8000-000000000001": "byid@example.com"}
	tx.suppressed[AggKey("default", "room-b")] = true

	n, err := uc.RouteOnce(context.Background(), 500)
	if err != nil || n != 6 {
		t.Fatalf("RouteOnce() = %d, %v", n, err)
	}
	if tx.marked["e-silent"] != EventStatusSuppressed || tx.marked["e-noroute"] != EventStatusFailed || tx.marked["e-direct"] != EventStatusRouted {
		t.Fatalf("event statuses = %v", tx.marked)
	}
	// 直发：显式收件替代 email 策略（邮箱 1 + 用户 ID 解析 1 = 2 个），webhook 1 个 → 3 个 dispatch；
	// $group 标识无机器人渠道可用、bogus 无法解析 → 忽略。
	if len(tx.dispatches) != 3 {
		t.Fatalf("dispatches = %d, want 3: %+v", len(tx.dispatches), tx.dispatches)
	}
	emails := map[string]bool{}
	for _, d := range tx.dispatches {
		if d.EventID != "e-direct" || d.Status != DispatchPending || d.NextRetryTime.IsZero() || len(d.SendKey) != 32 || d.ID == "" {
			t.Fatalf("dispatch = %+v", d)
		}
		if d.ChannelID == chEmail {
			emails[d.Recipient] = true
		}
	}
	if !emails["explicit@example.com"] || !emails["byid@example.com"] || emails["ops@example.com"] {
		t.Fatalf("explicit recipients (email + user id) must override policy, got %v", emails)
	}
	if tx.subsCalls != 1 {
		t.Fatalf("subscriber lookups = %d, want 1 (cached per batch)", tx.subsCalls)
	}
	// 聚合：按键排序（room-a 先于 room-b），同键合并计数与样本；抑制组事件置 suppressed。
	if len(tx.aggs) != 2 || tx.aggs[0].Key != AggKey("default", "room-a") || tx.aggs[0].Count != 2 || len(tx.aggs[0].Samples) != 2 ||
		tx.aggs[1].Key != AggKey("default", "room-b") || tx.aggs[1].Count != 1 || tx.aggs[0].RouteID != routeAgg || tx.aggs[0].WindowS != 60 {
		t.Fatalf("agg increments = %+v", tx.aggs)
	}
	if tx.aggs[0].FirstEventID != "e-agg-a1" {
		t.Fatalf("first event of room-a = %s", tx.aggs[0].FirstEventID)
	}
	if tx.marked["e-agg-a1"] != EventStatusRouted || tx.marked["e-agg-a2"] != EventStatusRouted || tx.marked["e-agg-b1"] != EventStatusSuppressed {
		t.Fatalf("aggregated statuses = %v", tx.marked)
	}
}

func TestRouterUsecase_TransientAndPoison(t *testing.T) {
	t.Parallel()
	routes := []*Route{{
		ID: routeID, BizID: "default", EventPattern: "*", SeverityMin: 1, Priority: 5, Enabled: true,
		ChannelIDs: []string{chEmail}, Recipient: RouteRecipient{Subscription: true},
	}}
	snap := testSnapshot(routes, nil, nil)
	fresh := testEvent("e-fresh", "a.b", 1)
	exhausted := testEvent("e-exhausted", "a.b", 1)
	exhausted.PublishAttempts = 4
	uc, tx := newRouter(t, snap, fresh, exhausted)
	tx.subsErr = errors.New("db timeout")
	if _, err := uc.RouteOnce(context.Background(), 10); err != nil {
		t.Fatalf("transient subscriber error must not abort batch: %v", err)
	}
	if _, ok := tx.deferred["e-fresh"]; !ok || tx.marked["e-fresh"] != 0 {
		t.Fatalf("fresh event should be deferred, marked=%v deferred=%v", tx.marked, tx.deferred)
	}
	if tx.marked["e-exhausted"] != EventStatusFailed {
		t.Fatalf("exhausted event should fail, got %v", tx.marked["e-exhausted"])
	}
	// 订阅无邮箱 → 无目标 → failed（配置错误，不毒化批）。
	tx2 := newRouterTxStub()
	uc2 := NewRouterUsecase(&eventSourceStub{events: []*Event{testEvent("e-nt", "a.b", 1)}, tx: tx2}, newTestCache(t, snap), testLogger())
	if _, err := uc2.RouteOnce(context.Background(), 10); err != nil || tx2.marked["e-nt"] != EventStatusFailed {
		t.Fatalf("no-target event: err=%v marked=%v", err, tx2.marked)
	}
}

func TestRouterUsecase_NoSnapshot(t *testing.T) {
	t.Parallel()
	uc := NewRouterUsecase(&eventSourceStub{}, NewConfigCache(&snapshotRepoStub{snap: testSnapshot(nil, nil, nil)}, &bucketSyncStub{}, testLogger()), testLogger())
	if _, err := uc.RouteOnce(context.Background(), 1); err == nil || !strings.Contains(err.Error(), "snapshot") {
		t.Fatalf("RouteOnce without snapshot error = %v", err)
	}
}
