package biz

import (
	"fmt"
	"strings"
	"testing"
)

// benchPayloadRoutes 不带 tag_match 的路由集：选路不读 payload，不需要解码。
func benchPayloadRoutes() []*Route {
	return []*Route{
		{ID: "r-k8s-pod", BizID: "default", EventPattern: "k8s.pod.*", SeverityMin: 3, Priority: 1, Enabled: true},
		{ID: "r-monitor", BizID: "default", EventPattern: "monitor.alert.*", SeverityMin: 2, Priority: 5, Enabled: true},
		{ID: "r-host", BizID: "default", EventPattern: "host.*", SeverityMin: 1, Priority: 5, Enabled: true},
		{ID: "r-k8s", BizID: "default", EventPattern: "k8s.*", SeverityMin: 2, Priority: 8, Enabled: true},
		{ID: "r-catch-all", BizID: "default", EventPattern: "*", SeverityMin: 1, Priority: 9, Enabled: true},
		{ID: "r-disabled", BizID: "default", EventPattern: "*", SeverityMin: 1, Priority: 2, Enabled: false},
	}
}

// benchPayload 一条 monitor.alert.firing 事件的 payload（对象里嵌数组与标签），补齐到 size 字节。
// 不含字面量 "test" 与 \u 转义——当前实现的字节预筛据此直接判定非测试事件，不必解码。
func benchPayload(size int) []byte {
	const head = `{"status":"firing","receiver":"ops","alerts":[{"status":"firing","labels":{"alertname":"HostDown",` +
		`"instance":"web-1","severity":"critical","env":"prod"},"annotations":{"summary":"host web-1 down",` +
		`"description":"node exporter unreachable"}}],"pad":"`
	return []byte(head + strings.Repeat("x", size-len(head)-2) + `"}`)
}

// BenchmarkRouterMatchRoutePayload 路由匹配前的 payload 解析（AGENTS §6：按需解码改写的基准对比）。
// 一个 op = router 对一个事件做的 payload 相关工作：
//   - legacy：HEAD 的 processBatch，选路前无条件 ev.PayloadObject()（每个事件解码一次）；
//   - current：按需解码（只有轮到带 tag_match 的路由才取 payload）+ 测试事件识别的字节预筛。
//
// 夹具是不带 tag_match 的路由集，此时当前实现一次都不解码。只比较，不断言性能阈值。
func BenchmarkRouterMatchRoutePayload(b *testing.B) {
	snap := testSnapshot(benchPayloadRoutes(), nil, nil)
	for _, size := range []int{512, 4096} {
		ev := testEvent("e", "monitor.alert.firing", 3)
		ev.Payload = benchPayload(size)
		decodes := 0
		legacy := func() *Route {
			decodes++
			return snap.MatchRoute(ev.BizID, ev.EventType, ev.Severity, ev.PayloadObject())
		}
		current := func() *Route {
			p := lazyPayload{ev: ev}
			route := snap.matchRoute(ev.BizID, ev.EventType, ev.Severity, p.object)
			testedChannel(&p)
			if p.decoded {
				decodes++
			}
			return route
		}
		if l, c := legacy(), current(); l == nil || l != c {
			b.Fatalf("legacy route = %v, current route = %v", l, c)
		}
		b.Run(fmt.Sprintf("payload=%dB", size), func(b *testing.B) {
			for _, v := range []struct {
				name string
				run  func() *Route
			}{{name: "legacy=eager-decode", run: legacy}, {name: "current=on-demand", run: current}} {
				b.Run(v.name, func(b *testing.B) {
					decodes = 0
					b.ReportAllocs()
					for range b.N {
						if v.run() == nil {
							b.Fatal("no matching route")
						}
					}
					b.ReportMetric(float64(decodes)/float64(b.N), "decodes/op")
				})
			}
		})
	}
}
