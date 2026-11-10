package biz

import (
	"context"
	"testing"

	"github.com/go-kratos/kratos/v3/errors"
)

// subscriptionRepoStub 订阅仓储内存桩：承载用例断言所需的最小语义
// （自然键 Upsert 替换、软删不可见、NotFound）。
type subscriptionRepoStub struct {
	subs map[string]*Subscription
}

func newSubscriptionRepoStub() *subscriptionRepoStub {
	return &subscriptionRepoStub{subs: make(map[string]*Subscription)}
}

// UpsertSubscription implements biz.SubscriptionRepo：同自然键（未删除行）
// 替换偏好与启停并保留原 id/创建时间，否则新建；返回权威行。
func (s *subscriptionRepoStub) UpsertSubscription(_ context.Context, sub *Subscription) (*Subscription, error) {
	for _, existing := range s.subs {
		if existing.UserID == sub.UserID && existing.BizID == sub.BizID &&
			existing.EventPattern == sub.EventPattern {
			existing.ChannelPrefs = sub.ChannelPrefs
			existing.Enabled = sub.Enabled
			return cloneSub(existing), nil
		}
	}
	cp := cloneSub(sub)
	s.subs[cp.ID] = cp
	return cloneSub(cp), nil
}

func cloneSub(sub *Subscription) *Subscription {
	cp := *sub
	cp.ChannelPrefs = make(ChannelPrefs, len(sub.ChannelPrefs))
	for k, v := range sub.ChannelPrefs {
		cp.ChannelPrefs[k] = v
	}
	return &cp
}

func (s *subscriptionRepoStub) DeleteSubscription(_ context.Context, id string) error {
	if _, ok := s.subs[id]; !ok {
		return ErrSubscriptionNotFound
	}
	delete(s.subs, id)
	return nil
}

func (s *subscriptionRepoStub) GetSubscription(_ context.Context, id string) (*Subscription, error) {
	sub, ok := s.subs[id]
	if !ok {
		return nil, ErrSubscriptionNotFound
	}
	return cloneSub(sub), nil
}

func (s *subscriptionRepoStub) ListSubscriptions(_ context.Context, filter SubscriptionFilter, page, pageSize int64) ([]*Subscription, int64, error) {
	var matched []*Subscription
	for _, sub := range s.subs {
		if sub.UserID != filter.UserID {
			continue
		}
		if filter.Enabled != nil && sub.Enabled != *filter.Enabled {
			continue
		}
		matched = append(matched, cloneSub(sub))
	}
	total := int64(len(matched))
	start := (page - 1) * pageSize
	if start >= total {
		return nil, total, nil
	}
	end := min(start+pageSize, total)
	return matched[start:end], total, nil
}

// newPrefs 返回合法渠道偏好（email 开关 + 钉钉目标群）。
func newPrefs() ChannelPrefs {
	return ChannelPrefs{
		"email":    SubscriptionPref{Subscribed: true},
		"dingtalk": SubscriptionPref{Target: "$group:ops"},
	}
}

// newSubscriptionUC 组装订阅用例。
func newSubscriptionUC() (*SubscriptionUsecase, *subscriptionRepoStub) {
	repo := newSubscriptionRepoStub()
	return NewSubscriptionUsecase(repo), repo
}

// newSubscription 返回合法订阅入参。
func newSubscription(userID, eventPattern string) *Subscription {
	return &Subscription{
		UserID:       userID,
		EventPattern: eventPattern,
		ChannelPrefs: newPrefs(),
		Enabled:      true,
	}
}

func TestSubscriptionUsecase_Upsert(t *testing.T) {
	t.Parallel()
	uc, _ := newSubscriptionUC()
	ctx := context.Background()
	userID := "018f3e1a-7b1c-7cc1-9d2a-3f5e7a9b1001"

	out, err := uc.Upsert(ctx, newSubscription(userID, "monitor.alert.*"))
	if err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}
	if !uuidV7Pattern.MatchString(out.ID) {
		t.Fatalf("Upsert() id = %q, want UUIDv7", out.ID)
	}
	if out.BizID != DefaultBizID {
		t.Fatalf("Upsert() bizID = %q, want default", out.BizID)
	}
	if out.CreateTime.IsZero() || out.UpdateTime.IsZero() {
		t.Fatal("Upsert() timestamps not set")
	}

	// 同自然键再次保存 = 整组替换偏好与启停（同 id）。
	repeat := newSubscription(userID, "monitor.alert.*")
	repeat.Enabled = false
	repeat.ChannelPrefs = ChannelPrefs{"email": SubscriptionPref{Subscribed: true}}
	out2, err := uc.Upsert(ctx, repeat)
	if err != nil {
		t.Fatalf("Upsert(repeat) error = %v", err)
	}
	if out2.ID != out.ID {
		t.Fatalf("Upsert(repeat) id = %q, want %q（自然键替换）", out2.ID, out.ID)
	}
	if out2.Enabled || len(out2.ChannelPrefs) != 1 {
		t.Fatalf("Upsert(repeat) result = %+v", out2)
	}

	// 同用户不同 pattern = 新订阅（不冲突）。
	other, err := uc.Upsert(ctx, newSubscription(userID, "permission.change"))
	if err != nil {
		t.Fatalf("Upsert(other pattern) error = %v", err)
	}
	if other.ID == out.ID {
		t.Fatal("Upsert(other pattern) id 重用")
	}

	// 缺 user_id 拒绝。
	anon := newSubscription("", "*")
	if _, err := uc.Upsert(ctx, anon); !errors.Is(err, ErrSubscriptionInvalid) {
		t.Fatalf("Upsert(no user) error = %v, want ErrSubscriptionInvalid", err)
	}
}

func TestSubscriptionUsecase_UpsertPatternInvalid(t *testing.T) {
	t.Parallel()
	uc, _ := newSubscriptionUC()
	userID := "018f3e1a-7b1c-7cc1-9d2a-3f5e7a9b1001"

	for _, pattern := range []string{"", "Monitor.Alert", "monitor.*.firing", "monitor alert"} {
		sub := newSubscription(userID, pattern)
		if _, err := uc.Upsert(context.Background(), sub); !errors.Is(err, ErrSubscriptionInvalid) {
			t.Errorf("Upsert(pattern %q) error = %v, want ErrSubscriptionInvalid", pattern, err)
		}
	}
	for _, pattern := range []string{"*", "monitor.alert.*", "permission.change"} {
		sub := newSubscription(userID, pattern)
		if _, err := uc.Upsert(context.Background(), sub); err != nil {
			t.Errorf("Upsert(pattern %q) error = %v, want nil", pattern, err)
		}
	}
}

func TestSubscriptionUsecase_UpsertPrefsInvalid(t *testing.T) {
	t.Parallel()
	uc, _ := newSubscriptionUC()
	userID := "018f3e1a-7b1c-7cc1-9d2a-3f5e7a9b1001"

	cases := []struct {
		name  string
		prefs ChannelPrefs
	}{
		{"empty", ChannelPrefs{}},
		{"webhook not subscribable", ChannelPrefs{"webhook": SubscriptionPref{Subscribed: true}}},
		{"unknown type", ChannelPrefs{"sms": SubscriptionPref{Subscribed: true}}},
		{"negative pref", ChannelPrefs{"email": SubscriptionPref{}}},
		{"both set", ChannelPrefs{"dingtalk": SubscriptionPref{Subscribed: true, Target: "$group:ops"}}},
		{"email with target", ChannelPrefs{"email": SubscriptionPref{Target: "a@b.c"}}},
	}
	for _, tc := range cases {
		sub := newSubscription(userID, "*")
		sub.ChannelPrefs = tc.prefs
		if _, err := uc.Upsert(context.Background(), sub); !errors.Is(err, ErrSubscriptionInvalid) {
			t.Errorf("Upsert(%s) error = %v, want ErrSubscriptionInvalid", tc.name, err)
		}
	}

	// 合法形态：三类机器人开关/目标群 + email 开关混排。
	for _, prefs := range []ChannelPrefs{
		{"email": SubscriptionPref{Subscribed: true}},
		{"dingtalk": SubscriptionPref{Target: "$group:ops"}},
		{"wecom": SubscriptionPref{Subscribed: true}},
		{
			"email":    SubscriptionPref{Subscribed: true},
			"dingtalk": SubscriptionPref{Target: "$group:ops"},
			"feishu":   SubscriptionPref{Subscribed: true},
			"wecom":    SubscriptionPref{Target: "$group:dev"},
		},
	} {
		sub := newSubscription(userID, "*")
		sub.ChannelPrefs = prefs
		if _, err := uc.Upsert(context.Background(), sub); err != nil {
			t.Errorf("Upsert(prefs %v) error = %v, want nil", prefs, err)
		}
	}
}

func TestSubscriptionUsecase_DeleteAndList(t *testing.T) {
	t.Parallel()
	uc, _ := newSubscriptionUC()
	ctx := context.Background()
	alice := "018f3e1a-7b1c-7cc1-9d2a-3f5e7a9b1001"
	bob := "018f3e1a-7b1c-7cc1-9d2a-3f5e7a9b1002"

	for _, pattern := range []string{"monitor.alert.*", "permission.change", "*"} {
		if _, err := uc.Upsert(ctx, newSubscription(alice, pattern)); err != nil {
			t.Fatalf("Upsert(%q) error = %v", pattern, err)
		}
	}
	if _, err := uc.Upsert(ctx, newSubscription(bob, "monitor.alert.*")); err != nil {
		t.Fatalf("Upsert(bob) error = %v", err)
	}

	// 列表严格按目标用户隔离。
	items, total, err := uc.List(ctx, SubscriptionFilter{UserID: alice}, 0, 0)
	if err != nil {
		t.Fatalf("List(alice) error = %v", err)
	}
	if total != 3 || len(items) != 3 {
		t.Fatalf("List(alice) = %d items, total %d, want 3/3", len(items), total)
	}

	// 删除后不可见、重复删 NotFound。
	victim := items[0].ID
	if err := uc.Delete(ctx, victim); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, err := uc.Get(ctx, victim); !errors.Is(err, ErrSubscriptionNotFound) {
		t.Fatalf("Get(deleted) error = %v, want ErrSubscriptionNotFound", err)
	}
	if err := uc.Delete(ctx, victim); !errors.Is(err, ErrSubscriptionNotFound) {
		t.Fatalf("Delete(twice) error = %v, want ErrSubscriptionNotFound", err)
	}
	items, total, err = uc.List(ctx, SubscriptionFilter{UserID: alice}, 1, 20)
	if err != nil {
		t.Fatalf("List(after delete) error = %v", err)
	}
	if total != 2 || len(items) != 2 {
		t.Fatalf("List(after delete) = %d items, total %d, want 2/2", len(items), total)
	}

	// 软删同键重建（桩即删除，新行无冲突）。
	rebuilt, err := uc.Upsert(ctx, newSubscription(alice, items[0].EventPattern))
	if err != nil {
		t.Fatalf("Upsert(rebuild) error = %v", err)
	}
	if rebuilt.ID == victim {
		t.Fatal("Upsert(rebuild) 复用了已删 id")
	}
}
