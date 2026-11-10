package server

// 事件查询 e2e：只读运行表（种子经 testfake 预置，模拟管道落库），四维过滤
// 与分页经完整 HTTP 链路（auth → authz → validate → service）。

import (
	"net/http"
	"strings"
	"testing"
	"time"

	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
)

func TestHTTPServer_NotifyEventEndToEnd(t *testing.T) {
	t.Parallel()
	ts, signer, store := newTestHTTPServerWithStore(t)
	bearer := testBearer(t, signer, e2eActorID)

	// 匿名请求被 auth 中间件拦截。
	code, _ := doRequest(t, http.MethodGet, ts.URL+"/api/v1/notify/event?page=1&page_size=10", "")
	if code != http.StatusUnauthorized {
		t.Fatalf("anonymous list: status = %d, want 401", code)
	}

	// 预置三行运行数据（core 无事件写路径）。
	base := time.Now().UTC().Add(-time.Hour)
	store.SeedEvent(&biznotify.Event{
		ID: "018f3e1a-0304-7000-8000-0000000000f1", BizID: "monitor",
		EventType: "monitor.alert.firing", Severity: 3, Title: "CPU 告警",
		Payload: []byte(`{"host":"web-1","cpu":92.5}`), DedupKey: "alert-web-1",
		Recipients: []string{"ops@example.com"}, Source: biznotify.EventSourceAPI,
		Status: biznotify.EventStatusRouted, PublishState: 1,
		NextPublishTime: base, CreateTime: base,
	})
	store.SeedEvent(&biznotify.Event{
		ID: "018f3e1a-0304-7000-8000-0000000000f2", BizID: "core",
		EventType: "permission.change", Severity: 2, Title: "角色权限变更",
		Source: biznotify.EventSourceInternal, Status: biznotify.EventStatusReceived,
		NextPublishTime: base.Add(time.Minute), CreateTime: base.Add(time.Minute),
	})
	store.SeedEvent(&biznotify.Event{
		ID: "018f3e1a-0304-7000-8000-0000000000f3", BizID: "monitor",
		EventType: "monitor.host.down", Severity: 4, Title: "主机失联",
		Source: biznotify.EventSourceAPI, Status: biznotify.EventStatusSuppressed, PublishState: 1,
		NextPublishTime: base.Add(2 * time.Minute), CreateTime: base.Add(2 * time.Minute),
	})

	// 全量：倒序 + Struct payload 原生 JSON 对象渲染 + 排障字段。
	code, body := doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/event?page=1&page_size=10", bearer, "")
	if code != http.StatusOK {
		t.Fatalf("list events: status = %d body = %s", code, body)
	}
	for _, want := range []string{
		`"total":"3"`, `"host":"web-1"`, `"recipients":["ops@example.com"]`,
		`"source":"api"`, `"source":"internal"`, `"publishState":1`,
		`"status":3`, // suppressed 行倒序在首
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("list events body %q missing %s", body, want)
		}
	}

	// 四维过滤：biz + status + event_type 精确 + 时间区间。
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/event?page=1&page_size=10&bizId=monitor&status=3", bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"total":"1"`) || !strings.Contains(body, "monitor.host.down") {
		t.Fatalf("list events(biz+status): status = %d body = %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/event?page=1&page_size=10&eventType=permission.change", bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"total":"1"`) {
		t.Fatalf("list events(eventType): status = %d body = %s", code, body)
	}
	start := base.Add(90 * time.Second).UTC().Format(time.RFC3339)
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/event?page=1&page_size=10&createTimeStart="+start, bearer, "")
	// ≥ base+90s：仅剩 +2min 的 host.down 行。
	if code != http.StatusOK || !strings.Contains(body, `"total":"1"`) ||
		!strings.Contains(body, "monitor.host.down") {
		t.Fatalf("list events(time range): status = %d body = %s", code, body)
	}

	// 非法过滤值：status=9 → protovalidate 400。
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/event?page=1&page_size=10&status=9", bearer, "")
	if code != http.StatusBadRequest {
		t.Fatalf("bad status: status = %d body = %q, want 400", code, body)
	}
	// 时间区间倒挂 → CEL 400。
	end := base.Add(-2 * time.Hour).UTC().Format(time.RFC3339)
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/event?page=1&page_size=10&createTimeStart="+start+"&createTimeEnd="+end, bearer, "")
	if code != http.StatusBadRequest {
		t.Fatalf("inverted time range: status = %d body = %q, want 400", code, body)
	}
	// 分页越界：空页 + total 不变。
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/event?page=9&page_size=10", bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"total":"3"`) || strings.Contains(body, `"items":[{"`) {
		t.Fatalf("list events(paged out): status = %d body = %s", code, body)
	}
}
