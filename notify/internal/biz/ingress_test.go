package biz

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

type ingressRepoStub struct {
	saved []*Event
	ttl   time.Duration
}

func (s *ingressRepoStub) SaveEvents(_ context.Context, _ *Producer, events []*Event, ttl time.Duration) ([]IngressResult, error) {
	s.saved = append(s.saved, events...)
	s.ttl = ttl
	out := make([]IngressResult, len(events))
	for i, ev := range events {
		out[i] = IngressResult{EventID: ev.ID}
	}
	return out, nil
}

func TestIngressUsecase_Accept(t *testing.T) {
	t.Parallel()
	repo := &ingressRepoStub{}
	uc := NewIngressUsecase(repo).WithDedupTTL(time.Hour)
	p := &Producer{BizID: "monitor", EventWhitelist: []string{"monitor.*"}}
	events := []*Event{
		{BizID: "monitor", EventType: "monitor.alert.firing", Severity: 3, Title: "cpu", Payload: []byte(`{"a":1}`), DedupKey: "k1"},
		{BizID: "monitor", EventType: "monitor.host.down", Severity: 4, Title: "down"},
	}
	results, err := uc.Accept(context.Background(), p, events)
	if err != nil || len(results) != 2 {
		t.Fatalf("Accept() = %v, %v", results, err)
	}
	for _, ev := range repo.saved {
		if ev.ID == "" || ev.Source != EventSourceAPI || ev.Status != EventStatusReceived || ev.PublishState != PublishPending || ev.CreateTime.IsZero() {
			t.Fatalf("event not enriched: %+v", ev)
		}
	}
	if repo.ttl != time.Hour {
		t.Fatalf("dedup ttl = %v", repo.ttl)
	}
	// scope 与校验失败整批拒绝、不落库。
	repo.saved = nil
	bad := []struct {
		name string
		ev   *Event
		want error
	}{
		{"other biz", &Event{BizID: "core", EventType: "monitor.x", Severity: 1, Title: "t"}, ErrForbidden},
		{"not whitelisted", &Event{BizID: "monitor", EventType: "user.status", Severity: 1, Title: "t"}, ErrForbidden},
		{"severity", &Event{BizID: "monitor", EventType: "monitor.x", Severity: 9, Title: "t"}, ErrEventInvalid},
		{"title", &Event{BizID: "monitor", EventType: "monitor.x", Severity: 1, Title: strings.Repeat("x", 256)}, ErrEventInvalid},
		{"payload json", &Event{BizID: "monitor", EventType: "monitor.x", Severity: 1, Title: "t", Payload: []byte("{")}, ErrEventInvalid},
		{"payload size", &Event{BizID: "monitor", EventType: "monitor.x", Severity: 1, Title: "t", Payload: make([]byte, 32*1024+1)}, ErrEventInvalid},
		{"recipients", &Event{BizID: "monitor", EventType: "monitor.x", Severity: 1, Title: "t", Recipients: make([]string, 21)}, ErrEventInvalid},
	}
	for _, c := range bad {
		if _, err := uc.Accept(context.Background(), p, []*Event{c.ev}); !errors.Is(err, c.want) {
			t.Errorf("%s: error = %v, want %v", c.name, err, c.want)
		}
	}
	if len(repo.saved) != 0 {
		t.Fatalf("rejected events must not be saved, got %d", len(repo.saved))
	}
}

func TestConfigCache_Refresh(t *testing.T) {
	t.Parallel()
	repo := &snapshotRepoStub{snap: testSnapshot(nil, nil, nil)}
	sync := &bucketSyncStub{}
	c := NewConfigCache(repo, sync, testLogger())
	if c.Current() != nil {
		t.Fatal("cache must be empty before load")
	}
	if err := c.Refresh(context.Background()); err != nil || c.Current() == nil || sync.synced != 1 {
		t.Fatalf("first refresh = %v, synced=%d", err, sync.synced)
	}
	first := c.Current()
	// 版本未变：不重载。
	repo.snap = testSnapshot(nil, nil, nil)
	if err := c.Refresh(context.Background()); err != nil || c.Current() != first {
		t.Fatalf("unchanged versions must keep snapshot (%v)", err)
	}
	// 版本变化：换入新快照并同步桶。
	repo.snap.Versions = ConfigVersions{"notify_channel": 2}
	if err := c.Refresh(context.Background()); err != nil || c.Current() == first || sync.synced != 2 {
		t.Fatalf("changed versions must reload: err=%v synced=%d", err, sync.synced)
	}
}
