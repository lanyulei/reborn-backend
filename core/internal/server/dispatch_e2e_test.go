package server

// 发送任务查询 e2e：只读运行表（种子经 testfake 预置，模拟管道 router 生成），
// 四维过滤与分页经完整 HTTP 链路（auth → authz → validate → service）。

import (
	"net/http"
	"strings"
	"testing"
	"time"

	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
)

func TestHTTPServer_NotifyDispatchEndToEnd(t *testing.T) {
	t.Parallel()
	ts, signer, store := newTestHTTPServerWithStore(t)
	bearer := testBearer(t, signer, e2eActorID)

	// 匿名请求被 auth 中间件拦截。
	code, _ := doRequest(t, http.MethodGet, ts.URL+"/api/v1/notify/dispatch?page=1&page_size=10", "")
	if code != http.StatusUnauthorized {
		t.Fatalf("anonymous list: status = %d, want 401", code)
	}

	// 预置三行运行数据（core 无发送任务写路径）。
	base := time.Now().UTC().Add(-time.Hour)
	retryAt := base.Add(5 * time.Minute)
	eventID := "018f3e1a-0304-7000-8000-0000000000e1"
	store.SeedDispatch(&biznotify.Dispatch{
		ID: "018f3e1a-0304-7000-8000-0000000000f1", EventID: eventID,
		ChannelID: "018f3e1a-0304-7000-8000-0000000000c1", Recipient: "ops@example.com",
		TemplateID: "018f3e1a-0304-7000-8000-0000000000t1",
		Content:    []byte(`{"subject":"CPU 告警","body":"web-1 cpu 92.5%"}`),
		Status:     biznotify.DispatchStatusFailed, Attempts: 2, NextRetry: &retryAt,
		LastError: "smtp: 421 service unavailable", SendKey: "9f86d081884c7d659a2f",
		CreateTime: base,
	})
	store.SeedDispatch(&biznotify.Dispatch{
		ID: "018f3e1a-0304-7000-8000-0000000000f2", EventID: eventID,
		ChannelID: "018f3e1a-0304-7000-8000-0000000000c2",
		Recipient: "https://hook.example.com/ops",
		Status:    biznotify.DispatchStatusSent, Attempts: 1, SendKey: "2c26b46b68ffc68ff9",
		CreateTime: base.Add(time.Minute),
	})
	store.SeedDispatch(&biznotify.Dispatch{
		ID:        "018f3e1a-0304-7000-8000-0000000000f3",
		EventID:   "018f3e1a-0304-7000-8000-0000000000e2",
		ChannelID: "018f3e1a-0304-7000-8000-0000000000c1", Recipient: "ops-group",
		Status: biznotify.DispatchStatusDead, Attempts: 6, SendKey: "5891b5b522d5b5b5b5",
		LastError: "dingtalk: token invalid", CreateTime: base.Add(2 * time.Minute),
	})

	// 全量：倒序 + Struct content 原生 JSON 对象渲染 + 重试/死信排障字段。
	code, body := doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/dispatch?page=1&page_size=10", bearer, "")
	if code != http.StatusOK {
		t.Fatalf("list dispatches: status = %d body = %s", code, body)
	}
	for _, want := range []string{
		`"total":"3"`, `"subject":"CPU 告警"`, `"recipient":"ops@example.com"`,
		`"status":4`, `"attempts":2`, `"lastError":"smtp: 421 service unavailable"`,
		`"sendKey":"9f86d081884c7d659a2f"`,
		`"status":5`, // dead 行倒序在首
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("list dispatches body %q missing %s", body, want)
		}
	}

	// 四维过滤：事件下钻 + 渠道 + status + 时间区间。
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/dispatch?page=1&page_size=10&eventId="+eventID, bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"total":"2"`) {
		t.Fatalf("list dispatches(event): status = %d body = %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/dispatch?page=1&page_size=10&channelId=018f3e1a-0304-7000-8000-0000000000c2", bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"total":"1"`) ||
		!strings.Contains(body, "hook.example.com") {
		t.Fatalf("list dispatches(channel): status = %d body = %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/dispatch?page=1&page_size=10&status=4", bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"total":"1"`) ||
		!strings.Contains(body, "smtp: 421 service unavailable") {
		t.Fatalf("list dispatches(status): status = %d body = %s", code, body)
	}
	start := base.Add(90 * time.Second).UTC().Format(time.RFC3339)
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/dispatch?page=1&page_size=10&createTimeStart="+start, bearer, "")
	// ≥ base+90s：仅剩 +2min 的 dead 行。
	if code != http.StatusOK || !strings.Contains(body, `"total":"1"`) ||
		!strings.Contains(body, "ops-group") {
		t.Fatalf("list dispatches(time range): status = %d body = %s", code, body)
	}

	// 非法过滤值：status=9 → protovalidate 400；eventId 非 uuid → 400。
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/dispatch?page=1&page_size=10&status=9", bearer, "")
	if code != http.StatusBadRequest {
		t.Fatalf("bad status: status = %d body = %q, want 400", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/dispatch?page=1&page_size=10&eventId=not-an-uuid", bearer, "")
	if code != http.StatusBadRequest {
		t.Fatalf("bad eventId: status = %d body = %q, want 400", code, body)
	}
	// 时间区间倒挂 → CEL 400。
	end := base.Add(-2 * time.Hour).UTC().Format(time.RFC3339)
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/dispatch?page=1&page_size=10&createTimeStart="+start+"&createTimeEnd="+end, bearer, "")
	if code != http.StatusBadRequest {
		t.Fatalf("inverted time range: status = %d body = %q, want 400", code, body)
	}
	// 分页越界：空页 + total 不变。
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/dispatch?page=9&page_size=10", bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"total":"3"`) || strings.Contains(body, `"items":[{"`) {
		t.Fatalf("list dispatches(paged out): status = %d body = %s", code, body)
	}
}
