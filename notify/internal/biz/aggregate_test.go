package biz

import (
	"context"
	"testing"
	"time"
)

type aggRepoStub struct {
	due     []string
	groups  map[string]*AggGroup
	closed  []string
	tx      *routerTxStub
	closeOK bool
}

func (s *aggRepoStub) ListDueGroups(context.Context, int) ([]string, error) { return s.due, nil }
func (s *aggRepoStub) RouteOf(_ context.Context, key string) (string, error) {
	if g, ok := s.groups[key]; ok {
		return g.RouteID, nil
	}
	return "", nil
}

func (s *aggRepoStub) CloseGroup(ctx context.Context, key string, threshold, _ int32, fn func(context.Context, RouterTx, *AggGroup) error) (bool, error) {
	g, ok := s.groups[key]
	if !ok || !s.closeOK {
		return false, nil
	}
	g.Suppressed = g.Count > int(threshold)
	s.closed = append(s.closed, key)
	return true, fn(ctx, s.tx, g)
}

func TestSweeperUsecase_SweepOnce(t *testing.T) {
	t.Parallel()
	routes := []*Route{{
		ID: routeAgg, BizID: "default", EventPattern: "host.down", SeverityMin: 1, Priority: 5, Enabled: true,
		ChannelIDs: []string{chHook, chEmail}, Recipient: RouteRecipient{Fixed: []string{"ops@example.com"}},
		AggregateWindowS: 60, SuppressThreshold: 2, SuppressDurationM: 30,
	}}
	snap := testSnapshot(routes, nil, nil)
	tx := newRouterTxStub()
	orphanSince := time.Unix(1_800_000_000, 0)
	repo := &aggRepoStub{
		due: []string{"default|small", "default|storm", "default|orphan", "default|gone"},
		groups: map[string]*AggGroup{
			"default|small": {
				Key: "default|small", BizID: "default", EventType: "host.down", RouteID: routeAgg, FirstEventID: "e1", Count: 2,
				Samples: []AggSample{{EventID: "e1", Title: "a"}, {EventID: "e2", Title: "b"}},
			},
			"default|storm": {
				Key: "default|storm", BizID: "default", EventType: "host.down", RouteID: routeAgg, FirstEventID: "e9", Count: 500,
				Samples: []AggSample{{EventID: "e9", Title: "x"}},
			},
			"default|orphan": {
				Key: "default|orphan", BizID: "default", EventType: "host.down", RouteID: "deleted-route", FirstEventID: "e5", Count: 30,
				FirstEventTime: orphanSince, Samples: []AggSample{{EventID: "e5", Title: "o"}},
			},
		},
		tx: tx, closeOK: true,
	}
	uc := NewSweeperUsecase(repo, newTestCache(t, snap), testLogger())
	n, err := uc.SweepOnce(context.Background(), 10)
	if err != nil || n != 3 {
		t.Fatalf("SweepOnce() = %d, %v; want 3 closed", n, err)
	}
	// small：2 渠道 → 2 条汇总 dispatch（含样本）；storm：超阈值 → 摘要模式；orphan：路由缺失无 dispatch，
	// 整组事件（不止样本）按 biz + group_key + 窗口起点改标 failed。
	if len(tx.dispatches) != 4 {
		t.Fatalf("summary dispatches = %d, want 4", len(tx.dispatches))
	}
	if len(tx.failedGroups) != 1 || tx.failedGroups[0] != (failedGroup{bizID: "default", groupKey: "orphan", since: orphanSince}) {
		t.Fatalf("orphan group must fail all its events by group membership, got %+v", tx.failedGroups)
	}
	if len(tx.marked) != 0 {
		t.Fatalf("sweeper must not mark individual sample events, got %v", tx.marked)
	}
	for _, d := range tx.dispatches {
		c, err := DecodeContent(d.Content)
		if err != nil || c.Agg == nil {
			t.Fatalf("summary content = %s, %v", d.Content, err)
		}
		switch d.EventID {
		case "e1":
			if c.Agg.Count != 2 || len(c.Agg.Samples) != 2 || c.Agg.Digest {
				t.Fatalf("small summary agg = %+v", c.Agg)
			}
		case "e9":
			if c.Agg.Count != 500 || !c.Agg.Digest || len(c.Agg.Samples) != 0 {
				t.Fatalf("storm summary agg = %+v", c.Agg)
			}
		default:
			t.Fatalf("unexpected summary for event %s", d.EventID)
		}
	}
}

func TestSweeperUsecase_NothingDue(t *testing.T) {
	t.Parallel()
	uc := NewSweeperUsecase(&aggRepoStub{}, newTestCache(t, testSnapshot(nil, nil, nil)), testLogger())
	if n, err := uc.SweepOnce(context.Background(), 10); err != nil || n != 0 {
		t.Fatalf("SweepOnce(empty) = %d, %v", n, err)
	}
	_ = time.Now()
}
