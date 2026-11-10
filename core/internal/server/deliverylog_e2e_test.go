package server

// 投递明细查询 e2e：只读运行表（种子经 testfake 预置，模拟管道 sender 追加），
// 四维过滤与分页经完整 HTTP 链路（auth → authz → validate → service）。

import (
	"net/http"
	"strings"
	"testing"
	"time"

	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
)

func TestHTTPServer_NotifyDeliveryLogEndToEnd(t *testing.T) {
	t.Parallel()
	ts, signer, store := newTestHTTPServerWithStore(t)
	bearer := testBearer(t, signer, e2eActorID)

	// 匿名请求被 auth 中间件拦截。
	code, _ := doRequest(t, http.MethodGet, ts.URL+"/api/v1/notify/delivery-log?page=1&page_size=10", "")
	if code != http.StatusUnauthorized {
		t.Fatalf("anonymous list: status = %d, want 401", code)
	}

	// 预置三行运行数据（core 无投递明细写路径）。
	base := time.Now().UTC().Add(-time.Hour)
	dispatchID := "018f3e1a-0304-7000-8000-0000000000d1"
	store.SeedDeliveryLog(&biznotify.DeliveryLog{
		ID: "018f3e1a-0304-7000-8000-0000000000g1", DispatchID: dispatchID,
		ChannelID: "018f3e1a-0304-7000-8000-0000000000c1",
		Attempt:   1, OK: false, LatencyMS: 5021,
		Error: "smtp: 421 service unavailable", CreateTime: base,
	})
	store.SeedDeliveryLog(&biznotify.DeliveryLog{
		ID: "018f3e1a-0304-7000-8000-0000000000g2", DispatchID: dispatchID,
		ChannelID: "018f3e1a-0304-7000-8000-0000000000c1",
		Attempt:   2, OK: true, LatencyMS: 830,
		ProviderMsgID: "<20261110@a.example.com>", CreateTime: base.Add(time.Minute),
	})
	store.SeedDeliveryLog(&biznotify.DeliveryLog{
		ID:         "018f3e1a-0304-7000-8000-0000000000g3",
		DispatchID: "018f3e1a-0304-7000-8000-0000000000d2",
		ChannelID:  "018f3e1a-0304-7000-8000-0000000000c2",
		Attempt:    1, OK: true, LatencyMS: 120,
		ProviderMsgID: "ding-msg-9f2c", CreateTime: base.Add(2 * time.Minute),
	})

	// 全量：倒序 + 排障字段完整渲染。
	code, body := doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/delivery-log?page=1&page_size=10", bearer, "")
	if code != http.StatusOK {
		t.Fatalf("list delivery logs: status = %d body = %s", code, body)
	}
	for _, want := range []string{
		`"total":"3"`, `"attempt":1`, `"ok":false`, `"latencyMs":5021`,
		`"error":"smtp: 421 service unavailable"`,
		`"providerMsgId":"ding-msg-9f2c"`, `"ok":true`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("list delivery logs body %q missing %s", body, want)
		}
	}

	// 四维过滤：任务下钻 + 渠道 + ok + 时间区间。
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/delivery-log?page=1&page_size=10&dispatchId="+dispatchID, bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"total":"2"`) {
		t.Fatalf("list delivery logs(dispatch): status = %d body = %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/delivery-log?page=1&page_size=10&channelId=018f3e1a-0304-7000-8000-0000000000c2", bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"total":"1"`) ||
		!strings.Contains(body, "ding-msg-9f2c") {
		t.Fatalf("list delivery logs(channel): status = %d body = %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/delivery-log?page=1&page_size=10&ok=false", bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"total":"1"`) ||
		!strings.Contains(body, "smtp: 421 service unavailable") {
		t.Fatalf("list delivery logs(ok=false): status = %d body = %s", code, body)
	}
	start := base.Add(90 * time.Second).UTC().Format(time.RFC3339)
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/delivery-log?page=1&page_size=10&createTimeStart="+start, bearer, "")
	// ≥ base+90s：仅剩 +2min 的钉钉成功行。
	if code != http.StatusOK || !strings.Contains(body, `"total":"1"`) ||
		!strings.Contains(body, "ding-msg-9f2c") {
		t.Fatalf("list delivery logs(time range): status = %d body = %s", code, body)
	}

	// 非法过滤值：dispatchId 非 uuid → protovalidate 400。
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/delivery-log?page=1&page_size=10&dispatchId=not-an-uuid", bearer, "")
	if code != http.StatusBadRequest {
		t.Fatalf("bad dispatchId: status = %d body = %q, want 400", code, body)
	}
	// 时间区间倒挂 → CEL 400。
	end := base.Add(-2 * time.Hour).UTC().Format(time.RFC3339)
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/delivery-log?page=1&page_size=10&createTimeStart="+start+"&createTimeEnd="+end, bearer, "")
	if code != http.StatusBadRequest {
		t.Fatalf("inverted time range: status = %d body = %q, want 400", code, body)
	}
	// 分页越界：空页 + total 不变。
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/delivery-log?page=9&page_size=10", bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"total":"3"`) || strings.Contains(body, `"items":[{"`) {
		t.Fatalf("list delivery logs(paged out): status = %d body = %s", code, body)
	}
}
