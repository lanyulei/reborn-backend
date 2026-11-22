package biz

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

func newTestCorrelator(cursor *fakeCursor, alerts *fakeAlerts, links fakeLinks) *CorrelatorUsecase {
	d := &Deps{Txm: fakeTxm{}, Cursor: cursor, Alerts: alerts, Rules: fakeRules{}, Links: links, Logger: testLogger()}
	settings := NewSettingsUsecase(fakeSettingRepo{})
	inc := &IncidentUsecase{d: d, settings: settings, now: time.Now}
	return NewCorrelatorUsecase(d, inc, settings)
}

// 回看窗口里的事件不参与推进游标：即使窗口被触发事件填满，游标也要越过它们走到前向读取的最大 seq。
func TestCorrelatorRunAdvancesCursorPastLookback(t *testing.T) {
	t.Parallel()
	cursor := &fakeCursor{seq: 10}
	alerts := &fakeAlerts{
		lookback: []*AlertSignal{fireSignal(8, "a8"), fireSignal(9, "a9"), fireSignal(10, "a10")},
		forward:  []*AlertSignal{fireSignal(11, "a11"), fireSignal(12, "a12")},
	}
	uc := newTestCorrelator(cursor, alerts, fakeLinks{})
	n, err := uc.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if n != 5 {
		t.Fatalf("processed = %d, want 5", n)
	}
	if len(alerts.rangeCalls) != 1 || alerts.rangeCalls[0] != (feedCall{after: 0, until: 10, limit: feedLookback}) {
		t.Fatalf("lookback read = %+v, want (0, 10] limit %d", alerts.rangeCalls, feedLookback)
	}
	if len(alerts.feedCalls) != 1 || alerts.feedCalls[0] != (feedCall{after: 10, limit: feedBatch}) {
		t.Fatalf("forward read = %+v, want after 10 limit %d", alerts.feedCalls, feedBatch)
	}
	if cursor.seq != 12 || len(cursor.sets) != 1 {
		t.Fatalf("cursor = %d (sets %v), want 12 set once", cursor.seq, cursor.sets)
	}
	// 回看补处理的逐条记录，前向处理的与游标一起记录。
	if len(cursor.marks) != 4 || !slices.Equal(cursor.marks[3], []int64{11, 12}) {
		t.Fatalf("seen marks = %v, want three single lookback marks then [11 12]", cursor.marks)
	}
}

// 回看窗口里已处理过的序号不再重放（否则解除关联的告警会被重新归并、已关闭的故障被重开、候选永不过期）；
// 只补没见过的，并把补处理的记为已处理。
func TestCorrelatorRunSkipsSeenLookback(t *testing.T) {
	t.Parallel()
	cursor := &fakeCursor{seq: 10, seen: map[int64]struct{}{8: {}, 9: {}}}
	alerts := &fakeAlerts{
		lookback: []*AlertSignal{fireSignal(8, "a8"), fireSignal(9, "a9"), fireSignal(10, "a10")},
		forward:  []*AlertSignal{fireSignal(11, "a11")},
	}
	uc := newTestCorrelator(cursor, alerts, fakeLinks{})
	n, err := uc.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if n != 2 {
		t.Fatalf("processed = %d, want 2 (unseen seq 10 + forward seq 11)", n)
	}
	for _, seq := range []int64{8, 9, 10, 11} {
		if _, ok := cursor.seen[seq]; !ok {
			t.Fatalf("seq %d must be recorded as seen, got %v", seq, cursor.seen)
		}
	}
	if cursor.seq != 11 {
		t.Fatalf("cursor = %d, want 11", cursor.seq)
	}
	// 游标 - 回看窗口 ≤ 0：没有可清理的记录，不调用清理。
	if len(cursor.pruned) != 0 {
		t.Fatalf("prune calls = %v, want none while cursor is within the lookback window", cursor.pruned)
	}
}

// 游标越过回看窗口后，每轮清理滑出窗口的已处理记录。
func TestCorrelatorRunPrunesSeenBelowLookback(t *testing.T) {
	t.Parallel()
	cursor := &fakeCursor{seq: 2000}
	alerts := &fakeAlerts{forward: []*AlertSignal{fireSignal(2001, "a")}}
	uc := newTestCorrelator(cursor, alerts, fakeLinks{})
	if _, err := uc.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if !slices.Equal(cursor.pruned, []int64{2001 - feedLookback}) {
		t.Fatalf("prune calls = %v, want [%d]", cursor.pruned, 2001-feedLookback)
	}
}

// 单条处理失败：游标停在它之前，错误上抛（循环据此记录与退避），不再静默吞掉。
func TestCorrelatorRunStopsBeforeFailedSignal(t *testing.T) {
	t.Parallel()
	cursor := &fakeCursor{seq: 10}
	alerts := &fakeAlerts{
		lookback: []*AlertSignal{fireSignal(10, "a10")},
		forward: []*AlertSignal{
			fireSignal(11, "a11"),
			{Seq: 12, Kind: "resolve", Alert: AlertRef{ID: "bad"}},
			fireSignal(13, "a13"),
		},
	}
	uc := newTestCorrelator(cursor, alerts, fakeLinks{failAlert: "bad"})
	n, err := uc.Run(context.Background())
	if !errors.Is(err, errBoom) {
		t.Fatalf("Run() error = %v, want errBoom", err)
	}
	if n != 2 {
		t.Fatalf("processed = %d, want 2 (lookback + seq 11)", n)
	}
	if cursor.seq != 11 {
		t.Fatalf("cursor = %d, want 11 (stop before the failed seq 12)", cursor.seq)
	}
}

// 两段都为空：不读规则与设置，游标不动。
func TestCorrelatorRunIdle(t *testing.T) {
	t.Parallel()
	cursor := &fakeCursor{seq: 42}
	uc := newTestCorrelator(cursor, &fakeAlerts{}, fakeLinks{})
	n, err := uc.Run(context.Background())
	if err != nil || n != 0 {
		t.Fatalf("Run() = %d, %v; want 0, nil", n, err)
	}
	if len(cursor.sets) != 0 {
		t.Fatalf("cursor must not move on an idle run, got sets %v", cursor.sets)
	}
}
