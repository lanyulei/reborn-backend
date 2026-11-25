package biz

import (
	"context"
	"testing"
)

// eagerTestedChannel 优化前的实现（无条件解码）：作等价性对照。
func eagerTestedChannel(ev *Event) string {
	obj := ev.PayloadObject()
	if obj == nil {
		return ""
	}
	if t, ok := obj["test"].(bool); !ok || !t {
		return ""
	}
	id, _ := obj["channel_id"].(string)
	return id
}

// 测试事件识别按需解码后，对任意 payload 的判定必须与无条件解码时逐一相同；原始字节不可能
// 含 test 键时不解码。
func TestTestedChannel_EquivalentToEagerDecode(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		payload    string
		wantDecode bool
	}{
		{"nil payload", "", false},
		{"plain payload", `{"host":"web-1","env":"prod"}`, false},
		{"test event", `{"test":true,"channel_id":"` + chHook + `"}`, true},
		{"jsonb formatted test event", `{"test": true, "channel_id": "` + chHook + `"}`, true},
		{"test false", `{"test":false,"channel_id":"` + chHook + `"}`, true},
		{"test not bool", `{"test":"true","channel_id":"` + chHook + `"}`, true},
		{"test without channel", `{"test":true}`, true},
		{"channel not string", `{"test":true,"channel_id":7}`, true},
		{"nested test key", `{"labels":{"test":true,"channel_id":"` + chHook + `"}}`, true},
		{"test only as value", `{"kind":"test","channel_id":"` + chHook + `"}`, true},
		{"unquoted substring", `{"contest":true,"latest":1}`, false},
		{"escaped key", `{"\u0074est":true,"channel_id":"` + chHook + `"}`, true},
		{"escaped last letter", `{"tes\u0074":true,"channel_id":"` + chEmail + `"}`, true},
		{"unrelated unicode escape", `{"msg":"\u4e2d\u6587"}`, true},
		{"other escapes only", `{"msg":"a\tb\n\"q\""}`, false},
		{"non-object", `[{"test":true}]`, true},
		{"invalid json with marker", `{"test":true`, true},
		{"invalid json", `{"host":`, false},
	}
	for _, c := range cases {
		ev := testEvent("e", "notify.test", 2)
		if c.payload != "" {
			ev.Payload = []byte(c.payload)
		}
		p := lazyPayload{ev: ev}
		got, want := testedChannel(&p), eagerTestedChannel(ev)
		if got != want {
			t.Errorf("%s: testedChannel = %q, eager decode = %q", c.name, got, want)
		}
		if p.decoded != c.wantDecode {
			t.Errorf("%s: decoded = %v, want %v", c.name, p.decoded, c.wantDecode)
		}
	}
}

// 选路只在轮到带 tag_match 的候选路由时才取 payload，结果与先解码再匹配相同。
func TestSnapshotMatchRoute_PayloadOnDemand(t *testing.T) {
	t.Parallel()
	routes := []*Route{
		{ID: "r-plain", BizID: "plain", EventPattern: "*", SeverityMin: 1, Priority: 5, Enabled: true},
		{
			ID: "r-tag", BizID: "tagged", EventPattern: "monitor.*", SeverityMin: 2, Priority: 1, Enabled: true,
			TagMatch: map[string]string{"env": "prod"},
		},
		{
			ID: "r-tag-off", BizID: "tagged", EventPattern: "*", SeverityMin: 1, Priority: 2, Enabled: false,
			TagMatch: map[string]string{"env": "prod"},
		},
		{ID: "r-fallback", BizID: "tagged", EventPattern: "*", SeverityMin: 1, Priority: 9, Enabled: true},
	}
	snap := testSnapshot(routes, nil, nil)
	cases := []struct {
		name      string
		biz       string
		eventType string
		severity  int32
		payload   string
		want      string
		wantCalls int
	}{
		{"biz without tag routes never decodes", "plain", "monitor.alert.firing", 3, `{"env":"prod"}`, "r-plain", 0},
		{"tag route candidate decodes once", "tagged", "monitor.alert.firing", 3, `{"env":"prod"}`, "r-tag", 1},
		{"tag mismatch falls through", "tagged", "monitor.alert.firing", 3, `{"env":"dev"}`, "r-fallback", 1},
		{"tag route filtered by severity", "tagged", "monitor.alert.firing", 1, `{"env":"prod"}`, "r-fallback", 0},
		{"tag route filtered by pattern", "tagged", "host.down", 3, `{"env":"prod"}`, "r-fallback", 0},
		{"tag route with nil payload", "tagged", "monitor.alert.firing", 3, "", "r-fallback", 1},
		{"unknown biz", "nobody", "monitor.alert.firing", 3, `{"env":"prod"}`, "", 0},
	}
	for _, c := range cases {
		ev := testEvent("e", c.eventType, c.severity)
		ev.BizID = c.biz
		if c.payload != "" {
			ev.Payload = []byte(c.payload)
		}
		calls := 0
		p := lazyPayload{ev: ev}
		got := snap.matchRoute(c.biz, c.eventType, c.severity, func() map[string]any {
			calls++
			return p.object()
		})
		eager := snap.MatchRoute(c.biz, c.eventType, c.severity, ev.PayloadObject())
		if got != eager {
			t.Errorf("%s: on-demand route = %v, eager route = %v", c.name, got, eager)
		}
		if id := routeIDOf(got); id != c.want {
			t.Errorf("%s: route = %q, want %q", c.name, id, c.want)
		}
		if calls != c.wantCalls {
			t.Errorf("%s: payload requested %d times, want %d", c.name, calls, c.wantCalls)
		}
	}
}

func routeIDOf(r *Route) string {
	if r == nil {
		return ""
	}
	return r.ID
}

// lazyPayload 至多解码一次：tag_match 与测试事件识别共用结果。
func TestLazyPayload_DecodesAtMostOnce(t *testing.T) {
	t.Parallel()
	ev := testEvent("e", "notify.test", 2)
	ev.Payload = []byte(`{"test":true,"channel_id":"` + chHook + `"}`)
	p := lazyPayload{ev: ev}
	first := p.object()
	ev.Payload = []byte(`{"test":false}`) // 再解码就会得到另一个结果
	if got := testedChannel(&p); got != chHook || first == nil {
		t.Fatalf("testedChannel after cached decode = %q, want %q", got, chHook)
	}
}

// 按需解码不得绕过「测试事件只投被测渠道」：路由带 tag_match（payload 已为选路解码）与键被
// \u 转义（字面量预筛不命中）两种情形下，测试事件仍只到被测渠道。
func TestRouterUsecase_TestEventStillIsolatedWithLazyPayload(t *testing.T) {
	t.Parallel()
	routes := []*Route{{
		ID: routeID, BizID: "default", EventPattern: "*", SeverityMin: 1, Priority: 5, Enabled: true,
		ChannelIDs: []string{chEmail, chHook}, Recipient: RouteRecipient{Fixed: []string{"ops@example.com"}},
		TagMatch: map[string]string{"test": "true"},
	}, {
		ID: routeAgg, BizID: "default", EventPattern: "*", SeverityMin: 1, Priority: 9, Enabled: true,
		ChannelIDs: []string{chEmail, chHook}, Recipient: RouteRecipient{Fixed: []string{"ops@example.com"}},
	}}
	tagged := testEvent("e-tagged", "notify.test", 2)
	tagged.Payload = []byte(`{"test":true,"channel_id":"` + chHook + `"}`)
	escaped := testEvent("e-escaped", "notify.test", 2)
	escaped.Payload = []byte(`{"\u0074est":true,"channel_id":"` + chEmail + `"}`)
	regular := testEvent("e-regular", "notify.test", 2)
	regular.Payload = []byte(`{"host":"web-1"}`)

	uc, tx := newRouter(t, testSnapshot(routes, nil, nil), tagged, escaped, regular)
	if _, err := uc.RouteOnce(context.Background(), 500); err != nil {
		t.Fatalf("RouteOnce() error = %v", err)
	}
	got := map[string][]string{}
	for _, d := range tx.dispatches {
		got[d.EventID] = append(got[d.EventID], d.ChannelID)
	}
	if len(got["e-tagged"]) != 1 || got["e-tagged"][0] != chHook {
		t.Fatalf("tagged test event reached %v, want only %s", got["e-tagged"], chHook)
	}
	if len(got["e-escaped"]) != 1 || got["e-escaped"][0] != chEmail {
		t.Fatalf("escaped-key test event reached %v, want only %s", got["e-escaped"], chEmail)
	}
	if len(got["e-regular"]) != 2 {
		t.Fatalf("regular event must reach every channel of the route, got %v", got["e-regular"])
	}
}
