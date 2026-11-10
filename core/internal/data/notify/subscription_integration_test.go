//go:build integration

package data

import (
	"context"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"

	"github.com/go-kratos/kratos/v3/errors"
)

// 集成测试固定用户 ID（合法 UUID，无外键约束，不须预置用户行）。
const (
	subAlice = "018f3e1a-0304-7000-8000-000000000001"
	subBob   = "018f3e1a-0304-7000-8000-000000000002"
)

// newSubscriptionDO 返回带固定 ID 与审计时间的订阅 DO（id 显式传入，主键不共享）。
func newSubscriptionDO(id, userID, eventPattern string) *biz.Subscription {
	return &biz.Subscription{
		ID:           id,
		UserID:       userID,
		BizID:        biz.DefaultBizID,
		EventPattern: eventPattern,
		ChannelPrefs: biz.ChannelPrefs{
			"email":    biz.SubscriptionPref{Subscribed: true},
			"dingtalk": biz.SubscriptionPref{Target: "$group:ops"},
		},
		Enabled:    true,
		CreateTime: time.Now().UTC().Truncate(time.Microsecond),
		UpdateTime: time.Now().UTC().Truncate(time.Microsecond),
	}
}

func TestIntegration_SubscriptionCRUD(t *testing.T) {
	d := newIntegrationNotifyData(t)
	repo := NewSubscriptionRepo(d)
	ctx := context.Background()

	// 新建：偏好紧凑 jsonb 往返（bool + string 两种取值形态）。
	alert := newSubscriptionDO("018f3e1a-0304-7000-8000-0000000000a1", subAlice, "monitor.alert.*")
	out, err := repo.UpsertSubscription(ctx, alert)
	if err != nil {
		t.Fatalf("UpsertSubscription() error = %v", err)
	}
	if out.ID != alert.ID {
		t.Fatalf("UpsertSubscription(insert) id = %q, want %q", out.ID, alert.ID)
	}
	if len(out.ChannelPrefs) != 2 ||
		!out.ChannelPrefs["email"].Subscribed ||
		out.ChannelPrefs["dingtalk"].Target != "$group:ops" {
		t.Fatalf("UpsertSubscription() prefs = %+v", out.ChannelPrefs)
	}

	// 同自然键再次保存：整组替换（保留原 id/create_time，刷新 update_time）。
	time.Sleep(time.Millisecond)                                                                     // 保证 update_time 推进可观测
	repeat := newSubscriptionDO("018f3e1a-0304-7000-8000-00000000dead", subAlice, "monitor.alert.*") // 冲突路径应忽略候选 id
	repeat.Enabled = false
	repeat.ChannelPrefs = biz.ChannelPrefs{"wecom": biz.SubscriptionPref{Subscribed: true}}
	out2, err := repo.UpsertSubscription(ctx, repeat)
	if err != nil {
		t.Fatalf("UpsertSubscription(repeat) error = %v", err)
	}
	if out2.ID != out.ID {
		t.Fatalf("UpsertSubscription(repeat) id = %q, want %q（自然键替换）", out2.ID, out.ID)
	}
	if out2.Enabled || len(out2.ChannelPrefs) != 1 || !out2.ChannelPrefs["wecom"].Subscribed {
		t.Fatalf("UpsertSubscription(repeat) result = %+v", out2)
	}
	// 同一微秒内两次 Upsert 时 update_time 可与 create_time 相等（PG now() 微秒精度），只要求不早于。
	if out2.UpdateTime.Before(out.CreateTime) {
		t.Fatalf("UpsertSubscription(repeat) updateTime = %v, want >= %v", out2.UpdateTime, out.CreateTime)
	}

	// 同用户不同 pattern、不同用户同 pattern：均新行（自然键含 user 维度）。
	perm, err := repo.UpsertSubscription(ctx, newSubscriptionDO("018f3e1a-0304-7000-8000-0000000000a2", subAlice, "permission.change"))
	if err != nil {
		t.Fatalf("UpsertSubscription(other pattern) error = %v", err)
	}
	bobSub, err := repo.UpsertSubscription(ctx, newSubscriptionDO("018f3e1a-0304-7000-8000-0000000000b1", subBob, "monitor.alert.*"))
	if err != nil {
		t.Fatalf("UpsertSubscription(bob) error = %v", err)
	}
	if perm.ID == out.ID || bobSub.ID == out.ID {
		t.Fatal("UpsertSubscription() id 复用了既有行")
	}

	// Get：未删除行可见、缺失 NotFound。
	got, err := repo.GetSubscription(ctx, out.ID)
	if err != nil || got.UserID != subAlice {
		t.Fatalf("GetSubscription() = (%+v, %v)", got, err)
	}
	if _, err := repo.GetSubscription(ctx, missingChannelID); !errors.Is(err, biz.ErrSubscriptionNotFound) {
		t.Fatalf("GetSubscription(missing) error = %v, want ErrSubscriptionNotFound", err)
	}

	// 列表：按目标用户隔离 + keyword（event_pattern 模糊）/ enabled / 分页。
	items, total, err := repo.ListSubscriptions(ctx, biz.SubscriptionFilter{UserID: subAlice}, 1, 20)
	if err != nil {
		t.Fatalf("ListSubscriptions(alice) error = %v", err)
	}
	if total != 2 || len(items) != 2 {
		t.Fatalf("ListSubscriptions(alice) = %d items, total %d, want 2/2", len(items), total)
	}
	items, total, err = repo.ListSubscriptions(ctx, biz.SubscriptionFilter{UserID: subAlice, Keyword: "permission"}, 1, 20)
	if err != nil {
		t.Fatalf("ListSubscriptions(keyword) error = %v", err)
	}
	if total != 1 || items[0].EventPattern != "permission.change" {
		t.Fatalf("ListSubscriptions(keyword) = %+v", items)
	}
	enabled := true
	items, total, err = repo.ListSubscriptions(ctx, biz.SubscriptionFilter{UserID: subAlice, Enabled: &enabled}, 1, 20)
	if err != nil {
		t.Fatalf("ListSubscriptions(enabled) error = %v", err)
	}
	if total != 1 || items[0].EventPattern != "permission.change" {
		t.Fatalf("ListSubscriptions(enabled) = %+v", items)
	}
	items, total, err = repo.ListSubscriptions(ctx, biz.SubscriptionFilter{UserID: subAlice}, 2, 1)
	if err != nil {
		t.Fatalf("ListSubscriptions(paged) error = %v", err)
	}
	if total != 2 || len(items) != 1 {
		t.Fatalf("ListSubscriptions(paged) = %d items, total %d, want 1/2", len(items), total)
	}

	// 软删：读不可见、重复删 NotFound、同自然键重建新行。
	if err := repo.DeleteSubscription(ctx, out.ID); err != nil {
		t.Fatalf("DeleteSubscription() error = %v", err)
	}
	if _, err := repo.GetSubscription(ctx, out.ID); !errors.Is(err, biz.ErrSubscriptionNotFound) {
		t.Fatalf("GetSubscription(deleted) error = %v, want ErrSubscriptionNotFound", err)
	}
	if err := repo.DeleteSubscription(ctx, out.ID); !errors.Is(err, biz.ErrSubscriptionNotFound) {
		t.Fatalf("DeleteSubscription(twice) error = %v, want ErrSubscriptionNotFound", err)
	}
	rebuilt, err := repo.UpsertSubscription(ctx, newSubscriptionDO("018f3e1a-0304-7000-8000-0000000000a3", subAlice, "monitor.alert.*"))
	if err != nil {
		t.Fatalf("UpsertSubscription(rebuild) error = %v", err)
	}
	if rebuilt.ID == out.ID {
		t.Fatal("UpsertSubscription(rebuild) 复用了软删行 id")
	}
	if rebuilt.Enabled != true || len(rebuilt.ChannelPrefs) != 2 {
		t.Fatalf("UpsertSubscription(rebuild) = %+v", rebuilt)
	}

	// 订阅不参与配置轮询：cfg_version 无 notify_subscription 行（§4.1 缓存集）。
	var n int
	if err := d.Pool.QueryRow(ctx,
		"SELECT count(*) FROM notify_cfg_version WHERE table_name = 'notify_subscription'").Scan(&n); err != nil {
		t.Fatalf("count cfg_version rows: %v", err)
	}
	if n != 0 {
		t.Fatalf("notify_cfg_version has %d notify_subscription rows, want 0（订阅不轮询）", n)
	}
}
