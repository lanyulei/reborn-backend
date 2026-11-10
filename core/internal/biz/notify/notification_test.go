package biz

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"

	"github.com/go-kratos/kratos/v3/errors"
)

// notificationTxStub 事务内直插桩：记录补全后的事件（Exec 不参与）。
type notificationTxStub struct {
	inserted []*Event
	fail     bool
	gate     map[string]string
	expired  bool
}

func (s *notificationTxStub) InsertEventTx(_ context.Context, _ txrun.Tx, event *Event) error {
	if s.fail {
		return errors.New(500, "stub", "insert failed")
	}
	cp := *event
	cp.Payload = append([]byte(nil), event.Payload...)
	cp.Recipients = append([]string(nil), event.Recipients...)
	s.inserted = append(s.inserted, &cp)
	return nil
}

// notifyTx 事务句柄桩（仅满足签名）。
type notifyTx struct{}

func (notifyTx) Exec(context.Context, string, ...any) (int64, error) { return 1, nil }
func (notifyTx) QueryRow(context.Context, string, ...any) txrun.Row  { return nil }

// GateDedupTx 内存门闩：首次占位，后续同键返回首次 id；expired 置位模拟过期。
func (s *notificationTxStub) GateDedupTx(_ context.Context, _ txrun.Tx, bizID, key, eventID string, _ time.Duration) (string, error) {
	if s.gate == nil {
		s.gate = map[string]string{}
	}
	k := bizID + "|" + key
	if w, ok := s.gate[k]; ok && !s.expired {
		return w, nil
	}
	s.gate[k] = eventID
	return eventID, nil
}

func TestNotificationUsecase_NotifyEventEnrich(t *testing.T) {
	t.Parallel()
	stub := &notificationTxStub{}
	uc := NewNotificationUsecase(stub, nil)
	ctx := context.Background()

	event, err := uc.NotifyEvent(ctx, notifyTx{}, Event{
		EventType: "permission.change",
		Title:     "角色权限变更",
		Payload:   []byte(`{"role_id":"r1"}`),
		GroupKey:  "permission.change.r1",
	})
	if err != nil {
		t.Fatalf("NotifyEvent() error = %v", err)
	}
	// 恒定覆写：管道状态机字段不信任调用方。
	if event.ID == "" || event.CreateTime.IsZero() || !event.NextPublishTime.Equal(event.CreateTime) {
		t.Fatalf("NotifyEvent() id/times = %q/%v", event.ID, event.CreateTime)
	}
	if event.Source != EventSourceInternal || event.Status != EventStatusReceived ||
		event.PublishState != 0 || event.PublishAttempts != 0 {
		t.Fatalf("NotifyEvent() pipeline fields = %+v", event)
	}
	// 缺省补全：biz 空取 default，severity 0 取 warning。
	if event.BizID != DefaultBizID || event.Severity != 2 {
		t.Fatalf("NotifyEvent() defaults = %q/%d", event.BizID, event.Severity)
	}
	// 业务语义字段原样透传。
	if event.EventType != "permission.change" || event.GroupKey != "permission.change.r1" ||
		string(event.Payload) != `{"role_id":"r1"}` {
		t.Fatalf("NotifyEvent() passthrough = %+v", event)
	}
	if len(stub.inserted) != 1 || stub.inserted[0].ID != event.ID {
		t.Fatalf("InsertEventTx got %+v, want the returned event", stub.inserted)
	}
}

func TestNotificationUsecase_NotifyEventExplicitFields(t *testing.T) {
	t.Parallel()
	stub := &notificationTxStub{}
	uc := NewNotificationUsecase(stub, nil)

	event, err := uc.NotifyEvent(context.Background(), notifyTx{}, Event{
		BizID:      "core",
		EventType:  "resource.change",
		Severity:   3,
		Title:      "资源变更",
		DedupKey:   "rc-1",
		GroupKey:   "resource.change.r2",
		Recipients: []string{"ops@example.com"},
	})
	if err != nil {
		t.Fatalf("NotifyEvent() error = %v", err)
	}
	if event.BizID != "core" || event.Severity != 3 || event.DedupKey != "rc-1" ||
		len(event.Recipients) != 1 || event.Recipients[0] != "ops@example.com" {
		t.Fatalf("NotifyEvent() explicit fields = %+v", event)
	}
}

func TestNotificationUsecase_NotifyEventInvalid(t *testing.T) {
	t.Parallel()
	stub := &notificationTxStub{}
	uc := NewNotificationUsecase(stub, nil)
	ctx := context.Background()

	cases := []struct {
		name  string
		event Event
	}{
		{"empty event type", Event{Title: "t"}},
		{"wildcard event type", Event{EventType: "monitor.*", Title: "t"}},
		{"uppercase event type", Event{EventType: "Monitor.Alert", Title: "t"}},
		{"severity below range", Event{EventType: "a.b", Severity: -1, Title: "t"}},
		{"severity above range", Event{EventType: "a.b", Severity: 5, Title: "t"}},
		{"empty title", Event{EventType: "a.b"}},
		{"title too long", Event{EventType: "a.b", Title: strings.Repeat("标", 128)}},
		{"payload too large", Event{EventType: "a.b", Title: "t", Payload: make([]byte, 32*1024+1)}},
		{"group key too long", Event{EventType: "a.b", Title: "t", GroupKey: strings.Repeat("g", 129)}},
		{"dedup key too long", Event{EventType: "a.b", Title: "t", DedupKey: strings.Repeat("d", 129)}},
		{"payload not json", Event{EventType: "a.b", Title: "t", Payload: []byte(`{oops`)}},
		{"too many recipients", Event{EventType: "a.b", Title: "t", Recipients: make([]string, 21)}},
	}
	for _, tc := range cases {
		if _, err := uc.NotifyEvent(ctx, notifyTx{}, tc.event); !errors.Is(err, ErrNotificationInvalid) {
			t.Errorf("NotifyEvent(%s) error = %v, want ErrNotificationInvalid", tc.name, err)
		}
	}
	if len(stub.inserted) != 0 {
		t.Fatalf("invalid events must not insert, got %d", len(stub.inserted))
	}

	// 直插原语失败原样透传（事务由调用方回滚）。
	stub.fail = true
	if _, err := uc.NotifyEvent(ctx, notifyTx{}, Event{EventType: "a.b", Title: "t"}); err == nil ||
		errors.Is(err, ErrNotificationInvalid) {
		t.Fatalf("NotifyEvent(insert failure) error = %v, want raw insert error", err)
	}
}

// TestNotificationUsecase_DedupGate 带 dedup_key 的直插先过门闩：首次插入；有效期内
// 同键返回首次事件（Deduplicated、不插入）；过期后重新接纳。无 dedup_key 不触门闩。
func TestNotificationUsecase_DedupGate(t *testing.T) {
	t.Parallel()
	stub := &notificationTxStub{}
	uc := NewNotificationUsecase(stub, nil)
	ctx := context.Background()
	first, err := uc.NotifyEvent(ctx, notifyTx{}, Event{EventType: "user.status", Title: "t", DedupKey: "u-1"})
	if err != nil || first.Deduplicated || len(stub.inserted) != 1 {
		t.Fatalf("first insert = %+v, %v, inserted %d", first, err, len(stub.inserted))
	}
	second, err := uc.NotifyEvent(ctx, notifyTx{}, Event{EventType: "user.status", Title: "t", DedupKey: "u-1"})
	if err != nil || !second.Deduplicated || second.ID != first.ID || len(stub.inserted) != 1 {
		t.Fatalf("duplicate = %+v, %v, inserted %d; want dedup to first", second, err, len(stub.inserted))
	}
	// 不同 biz 同键互不影响。
	other, err := uc.NotifyEvent(ctx, notifyTx{}, Event{BizID: "monitor", EventType: "user.status", Title: "t", DedupKey: "u-1"})
	if err != nil || other.Deduplicated || len(stub.inserted) != 2 {
		t.Fatalf("other biz = %+v, %v", other, err)
	}
	stub.expired = true
	third, err := uc.NotifyEvent(ctx, notifyTx{}, Event{EventType: "user.status", Title: "t", DedupKey: "u-1"})
	if err != nil || third.Deduplicated || third.ID == first.ID || len(stub.inserted) != 3 {
		t.Fatalf("after expiry = %+v, %v", third, err)
	}
	// 无 dedup_key：每次都插入。
	for i := 0; i < 2; i++ {
		if ev, err := uc.NotifyEvent(ctx, notifyTx{}, Event{EventType: "user.status", Title: "t"}); err != nil || ev.Deduplicated {
			t.Fatal(err)
		}
	}
	if len(stub.inserted) != 5 {
		t.Fatalf("inserted = %d, want 5", len(stub.inserted))
	}
}
