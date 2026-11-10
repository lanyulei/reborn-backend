package biz

import (
	"reflect"
	"testing"
)

func TestResolveTargets(t *testing.T) {
	t.Parallel()
	snap := testSnapshot(nil, nil, nil)
	subs := []Subscriber{
		{UserID: "u1", Email: "u1@example.com", Prefs: map[string]any{"email": true, "dingtalk": "$group:ops"}},
		{UserID: "u2", Email: "", Prefs: map[string]any{"email": true}},
		{UserID: "u3", Email: "u3@example.com", Prefs: map[string]any{"dingtalk": true}},
	}
	route := &Route{
		ChannelIDs: []string{chEmail, chHook, chRobot, chOff, "missing"},
		Recipient:  RouteRecipient{Fixed: []string{"ops@example.com", " ops@example.com "}, Subscription: true},
	}

	got := ResolveTargets(snap, route, nil, subs)
	want := []Target{
		{chEmail, "ops@example.com"},
		{chEmail, "u1@example.com"},
		{chHook, "https://hooks.example.com/x"},
		{chRobot, "ops@example.com"}, // 机器人：fixed 非空用 fixed
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ResolveTargets() = %v, want %v", got, want)
	}

	// 显式收件：email 以其替代策略；webhook 不受影响；机器人无 fixed 走订阅目标群/隐含 $bot。
	route2 := &Route{ChannelIDs: []string{chEmail, chHook, chRobot}, Recipient: RouteRecipient{Subscription: true}}
	got = ResolveTargets(snap, route2, ClassifyRecipients([]string{"a@example.com", "a@example.com"}, nil), subs)
	want = []Target{{chEmail, "a@example.com"}, {chHook, "https://hooks.example.com/x"}, {chRobot, "$group:ops"}, {chRobot, "$bot"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ResolveTargets(explicit) = %v, want %v", got, want)
	}
	// 显式群标识替代机器人策略；显式用户 ID 经用户表解析为邮箱。
	explicit := ClassifyRecipients([]string{"$group:oncall", "018F3E1A-0A09-7000-8000-000000000001", "unknown-user", "018f3e1a-0a09-7000-8000-000000000002", " "},
		map[string]string{"018f3e1a-0a09-7000-8000-000000000001": "byid@example.com"})
	if !reflect.DeepEqual(explicit.Emails, []string{"byid@example.com"}) || !reflect.DeepEqual(explicit.Groups, []string{"$group:oncall"}) ||
		!reflect.DeepEqual(explicit.Unresolved, []string{"unknown-user", "018f3e1a-0a09-7000-8000-000000000002"}) {
		t.Fatalf("ClassifyRecipients = %+v", explicit)
	}
	got = ResolveTargets(snap, route2, explicit, subs)
	want = []Target{{chEmail, "byid@example.com"}, {chHook, "https://hooks.example.com/x"}, {chRobot, "$group:oncall"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ResolveTargets(groups+ids) = %v, want %v", got, want)
	}
	if ids := UserIDs([]string{"a@b.c", "018f3e1a-0a09-7000-8000-000000000001", "$x"}); !reflect.DeepEqual(ids, []string{"018f3e1a-0a09-7000-8000-000000000001"}) {
		t.Fatalf("UserIDs = %v", ids)
	}
	// 无订阅、无 fixed：email 无目标，机器人隐含 $bot。
	got = ResolveTargets(snap, &Route{ChannelIDs: []string{chEmail, chRobot}}, nil, nil)
	if !reflect.DeepEqual(got, []Target{{chRobot, "$bot"}}) {
		t.Fatalf("ResolveTargets(none) = %v", got)
	}
}
