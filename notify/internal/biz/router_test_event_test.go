package biz

import (
	"context"
	"testing"
)

// 连通性测试事件只投递被测渠道：此前命中路由的全部启用渠道都会真实投递（路由绑了通配模式时，
// 测 A 渠道会把测试消息发到别的群机器人与外部 webhook）。
func TestRouterUsecase_TestEventOnlyReachesTestedChannel(t *testing.T) {
	t.Parallel()
	routes := []*Route{{
		ID: routeID, BizID: "default", EventPattern: "*", SeverityMin: 1, Priority: 5, Enabled: true,
		ChannelIDs: []string{chEmail, chHook, chRobot}, Recipient: RouteRecipient{Fixed: []string{"ops@example.com"}},
		AggregateWindowS: 60,
	}}
	snap := testSnapshot(routes, nil, nil)

	tested := testEvent("e-test", "notify.test", 2)
	tested.GroupKey = "g"
	tested.Payload = []byte(`{"test":true,"channel_id":"` + chHook + `"}`)
	other := testEvent("e-test-missing", "notify.test", 2)
	other.Payload = []byte(`{"test":true,"channel_id":"018f3e1a-0a01-7000-8000-000000000009"}`)
	regular := testEvent("e-regular", "notify.test", 2)
	regular.GroupKey = "g"

	uc, tx := newRouter(t, snap, tested, other, regular)
	if _, err := uc.RouteOnce(context.Background(), 500); err != nil {
		t.Fatalf("RouteOnce() error = %v", err)
	}
	// 被测渠道在路由里：只有它的一条任务（不进聚合，尽管路由开了聚合且事件带 group_key）。
	if len(tx.aggs) != 1 {
		t.Fatalf("only the regular event may be aggregated, aggs = %+v", tx.aggs)
	}
	if len(tx.dispatches) != 1 || tx.dispatches[0].EventID != "e-test" || tx.dispatches[0].ChannelID != chHook {
		t.Fatalf("the test event must produce exactly one dispatch to the tested channel, dispatches = %+v", tx.dispatches)
	}
	if tx.marked["e-test"] != EventStatusRouted || tx.marked["e-regular"] != EventStatusRouted {
		t.Fatalf("event statuses = %v", tx.marked)
	}
	// 被测渠道不在路由里：不发往别的渠道，事件按无目标记 failed。
	if tx.marked["e-test-missing"] != EventStatusFailed {
		t.Fatalf("a test event whose channel is not on the route must fail, statuses = %v", tx.marked)
	}
	for _, d := range tx.dispatches {
		if d.ChannelID != chHook {
			t.Fatalf("no other channel may receive the test event, got %+v", d)
		}
	}
}
