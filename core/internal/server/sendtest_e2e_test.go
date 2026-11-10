package server

// 渠道测试事件 e2e：SendTestEvent 经完整 HTTP 链路（auth → authz →
// validate → service → biz → 落库原语），并以 ListEvents 读回验证管理面
// 可观测闭环（core 不发送——投递随管道 P1 生效）。

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
)

// jsonField 提取两级嵌套字段的字符串值（e2e 响应局部取值辅助）。
func jsonField(t *testing.T, body, outer, inner string) string {
	t.Helper()
	var parsed struct {
		Channel struct {
			ID string `json:"id"`
		} `json:"channel"`
		Event struct {
			ID string `json:"id"`
		} `json:"event"`
	}
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		t.Fatalf("unmarshal %q: %v", body, err)
	}
	switch outer {
	case "channel":
		if parsed.Channel.ID == "" {
			t.Fatalf("body %q missing channel.id", body)
		}
		return parsed.Channel.ID
	case "event":
		if parsed.Event.ID == "" {
			t.Fatalf("body %q missing event.id", body)
		}
		return parsed.Event.ID
	}
	t.Fatalf("unknown outer field %q", outer)
	return ""
}

func TestHTTPServer_NotifySendTestEventEndToEnd(t *testing.T) {
	t.Parallel()
	ts, signer, store := newTestHTTPServerWithStore(t)
	bearer := testBearer(t, signer, e2eActorID)

	// 匿名请求被 auth 中间件拦截。
	code, _ := doRequest(t, http.MethodPost,
		ts.URL+"/api/v1/notify/channel/018f3e1a-0304-7000-8000-0000000000c1:test", "")
	if code != http.StatusUnauthorized {
		t.Fatalf("anonymous send test: status = %d, want 401", code)
	}

	// 建渠道（HTTP 写路径）。
	code, body := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/notify/channel", bearer,
		`{"type":"NOTIFY_CHANNEL_TYPE_EMAIL","name":"email-test",`+
			`"email":{"host":"smtp.example.com","port":465,"username":"ops","fromAddr":"ops@example.com"},`+
			`"secret":"smtp-password"}`)
	if code != http.StatusOK {
		t.Fatalf("create channel: status = %d body = %s", code, body)
	}
	channelID := jsonField(t, body, "channel", "id")

	// 缺省 spec：响应为已落库事件（notify.test / internal / received-pending）。
	code, body = doRequestWithToken(t, http.MethodPost,
		ts.URL+"/api/v1/notify/channel/"+channelID+":test", bearer, `{}`)
	if code != http.StatusOK {
		t.Fatalf("send test event: status = %d body = %s", code, body)
	}
	for _, want := range []string{
		`"eventType":"notify.test"`, `"source":"internal"`, `"status":1`, `"publishState":0`,
		`"title":"通知渠道连通性测试"`, `"severity":2`, `"test":true`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("send test event body %q missing %s", body, want)
		}
	}

	// 可观测闭环：ListEvents 按类型过滤读回同一事件。
	eventID := jsonField(t, body, "event", "id")
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/event?page=1&page_size=10&eventType=notify.test", bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"total":"1"`) ||
		!strings.Contains(body, `"id":"`+eventID+`"`) {
		t.Fatalf("list events(notify.test): status = %d body = %s", code, body)
	}

	// 自定义 spec 往返：类型/标题/收件/载荷。
	code, body = doRequestWithToken(t, http.MethodPost,
		ts.URL+"/api/v1/notify/channel/"+channelID+":test", bearer,
		`{"eventType":"monitor.alert.test","title":"自定义测试","recipients":["ops@example.com"],`+
			`"payload":{"order":"A-1"}}`)
	if code != http.StatusOK || !strings.Contains(body, `"eventType":"monitor.alert.test"`) ||
		!strings.Contains(body, `"recipients":["ops@example.com"]`) ||
		!strings.Contains(body, `"order":"A-1"`) {
		t.Fatalf("send custom test event: status = %d body = %s", code, body)
	}

	// 停用实例 → 409（路由本就跳过，测试无意义）。
	code, body = doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/notify/channel/"+channelID, bearer,
		`{"enabled":false}`)
	if code != http.StatusOK {
		t.Fatalf("disable channel: status = %d body = %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodPost,
		ts.URL+"/api/v1/notify/channel/"+channelID+":test", bearer, `{}`)
	if code != http.StatusConflict {
		t.Fatalf("send test event(disabled): status = %d body = %q, want 409", code, body)
	}

	// 不存在渠道 → 404。
	code, body = doRequestWithToken(t, http.MethodPost,
		ts.URL+"/api/v1/notify/channel/018f3e1a-0304-7000-8000-0000000000cf:test", bearer, `{}`)
	if code != http.StatusNotFound {
		t.Fatalf("send test event(missing): status = %d body = %q, want 404", code, body)
	}

	// 非法参数：event_type 大写 → protovalidate 400；id 非 uuid → 400。
	code, body = doRequestWithToken(t, http.MethodPost,
		ts.URL+"/api/v1/notify/channel/"+channelID+":test", bearer,
		`{"eventType":"Monitor.Alert"}`)
	if code != http.StatusBadRequest {
		t.Fatalf("bad event type: status = %d body = %q, want 400", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodPost,
		ts.URL+"/api/v1/notify/channel/not-an-uuid:test", bearer, `{}`)
	if code != http.StatusBadRequest {
		t.Fatalf("bad channel id: status = %d body = %q, want 400", code, body)
	}

	// 落库即原始数据（testfake 与读路径同一存储）：两行测试事件。
	items, total, err := store.ListEvents(context.Background(), biznotify.EventFilter{}, 1, 10)
	if err != nil || total != 2 || len(items) != 2 {
		t.Fatalf("store events = (%d items, %v), want 2", len(items), err)
	}
}
