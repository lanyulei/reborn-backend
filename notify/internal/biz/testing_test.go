package biz

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

// 测试脚手架：内存 ConfigRepo / RateBucketSyncer，构造已加载的 ConfigCache。

type snapshotRepoStub struct {
	snap *Snapshot
}

func (s *snapshotRepoStub) LoadVersions(context.Context) (ConfigVersions, error) {
	return s.snap.Versions, nil
}
func (s *snapshotRepoStub) LoadSnapshot(context.Context) (*Snapshot, error) { return s.snap, nil }

type bucketSyncStub struct{ synced int }

func (b *bucketSyncStub) SyncBuckets(_ context.Context, _ []*Channel) error {
	b.synced++
	return nil
}

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newTestCache(t *testing.T, snap *Snapshot) *ConfigCache {
	t.Helper()
	c := NewConfigCache(&snapshotRepoStub{snap: snap}, &bucketSyncStub{}, testLogger())
	if err := c.Load(context.Background()); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	return c
}

// 固定测试 ID（uuid 形态）。
const (
	chEmail   = "018f3e1a-0a01-7000-8000-000000000001"
	chHook    = "018f3e1a-0a01-7000-8000-000000000002"
	chRobot   = "018f3e1a-0a01-7000-8000-000000000003"
	chOff     = "018f3e1a-0a01-7000-8000-000000000004"
	routeID   = "018f3e1a-0a02-7000-8000-000000000001"
	routeAgg  = "018f3e1a-0a02-7000-8000-000000000002"
	producer1 = "018f3e1a-0a03-7000-8000-000000000001"
)

func testChannels() []*Channel {
	return []*Channel{
		{
			ID: chEmail, BizID: "default", Type: ChannelTypeEmail, Name: "email", Enabled: true,
			Email: &EmailConfig{Host: "smtp.example.com", Port: 25, FromAddr: "noreply@example.com"},
		},
		{
			ID: chHook, BizID: "default", Type: ChannelTypeWebhook, Name: "hook", Enabled: true,
			Webhook: &WebhookConfig{URL: "https://hooks.example.com/x", Method: "POST", TimeoutS: 3}, Secret: "hook-secret",
		},
		{ID: chRobot, BizID: "default", Type: ChannelTypeDingtalk, Name: "ding", Enabled: true, Robot: &RobotConfig{}},
		{
			ID: chOff, BizID: "default", Type: ChannelTypeEmail, Name: "email-off", Enabled: false,
			Email: &EmailConfig{Host: "smtp.example.com", FromAddr: "x@example.com"},
		},
	}
}

func testSnapshot(routes []*Route, silences []*Silence, templates []*Template) *Snapshot {
	return NewSnapshot(ConfigVersions{"notify_channel": 1}, testChannels(), routes, silences, templates,
		[]*Producer{{ID: producer1, BizID: "default", Name: "engine", Enabled: true, TokenHash: []byte("h")}}, time.Now())
}

func testEvent(id, eventType string, severity int32) *Event {
	return &Event{
		ID: id, BizID: "default", EventType: eventType, Severity: severity, Title: "t-" + id,
		Source: EventSourceAPI, Status: EventStatusReceived, CreateTime: time.Now().UTC(),
	}
}
