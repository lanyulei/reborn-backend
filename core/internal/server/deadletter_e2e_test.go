package server

// 死信重放 e2e：查询 + 标记重放（唯一写动作）经完整 HTTP 链路（auth →
// authz → validate → service）；种子经 testfake 预置（模拟 sender 在
// dispatch 重试超限后写入）。

import (
	"net/http"
	"strings"
	"testing"
	"time"

	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
)

func TestHTTPServer_NotifyDeadLetterEndToEnd(t *testing.T) {
	t.Parallel()
	ts, signer, store := newTestHTTPServerWithStore(t)
	bearer := testBearer(t, signer, e2eActorID)

	// 匿名请求被 auth 中间件拦截（读路径与写路径各自验证）。
	code, _ := doRequest(t, http.MethodGet, ts.URL+"/api/v1/notify/dead-letter?page=1&page_size=10", "")
	if code != http.StatusUnauthorized {
		t.Fatalf("anonymous list: status = %d, want 401", code)
	}
	code, _ = doRequest(t, http.MethodPost,
		ts.URL+"/api/v1/notify/dead-letter/018f3e1a-0304-7000-8000-0000000000a1:replay", "")
	if code != http.StatusUnauthorized {
		t.Fatalf("anonymous replay: status = %d, want 401", code)
	}

	// 预置三行运行数据（core 无死信写入路径，仅标记重放）。
	base := time.Now().UTC().Add(-time.Hour)
	eventID := "018f3e1a-0304-7000-8000-0000000000e1"
	replayable := "018f3e1a-0304-7000-8000-0000000000a1"
	store.SeedDeadLetter(&biznotify.DeadLetter{
		ID:         replayable,
		DispatchID: "018f3e1a-0304-7000-8000-0000000000d1", EventID: eventID,
		ChannelID: "018f3e1a-0304-7000-8000-0000000000c1", Recipient: "ops@example.com",
		LastError: "smtp: 421 service unavailable", Attempts: 6,
		ReplayState: biznotify.ReplayStateNone, CreateTime: base, UpdateTime: base,
	})
	store.SeedDeadLetter(&biznotify.DeadLetter{
		ID:         "018f3e1a-0304-7000-8000-0000000000a2",
		DispatchID: "018f3e1a-0304-7000-8000-0000000000d2", EventID: eventID,
		ChannelID: "018f3e1a-0304-7000-8000-0000000000c2",
		Recipient: "https://hook.example.com/ops",
		LastError: "dial tcp: i/o timeout", Attempts: 6,
		ReplayState: biznotify.ReplayStatePending,
		CreateTime:  base.Add(time.Minute), UpdateTime: base.Add(5 * time.Minute),
	})
	store.SeedDeadLetter(&biznotify.DeadLetter{
		ID:         "018f3e1a-0304-7000-8000-0000000000a3",
		DispatchID: "018f3e1a-0304-7000-8000-0000000000d3",
		EventID:    "018f3e1a-0304-7000-8000-0000000000e2",
		ChannelID:  "018f3e1a-0304-7000-8000-0000000000c1", Recipient: "ops-group",
		Attempts: 6, ReplayState: biznotify.ReplayStateReplayed,
		CreateTime: base.Add(2 * time.Minute), UpdateTime: base.Add(10 * time.Minute),
	})

	// 全量：倒序 + 排障字段 + 状态三态渲染。
	code, body := doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/dead-letter?page=1&page_size=10", bearer, "")
	if code != http.StatusOK {
		t.Fatalf("list dead letters: status = %d body = %s", code, body)
	}
	for _, want := range []string{
		`"total":"3"`, `"attempts":6`, `"recipient":"ops@example.com"`,
		`"lastError":"smtp: 421 service unavailable"`,
		`"replayState":1`, `"replayState":2`, `"replayState":3`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("list dead letters body %q missing %s", body, want)
		}
	}

	// 四维过滤：事件下钻 + replay_state + 渠道 + 时间区间。
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/dead-letter?page=1&page_size=10&eventId="+eventID, bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"total":"2"`) {
		t.Fatalf("list dead letters(event): status = %d body = %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/dead-letter?page=1&page_size=10&replayState=1", bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"total":"1"`) ||
		!strings.Contains(body, "smtp: 421 service unavailable") {
		t.Fatalf("list dead letters(none): status = %d body = %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/dead-letter?page=1&page_size=10&channelId=018f3e1a-0304-7000-8000-0000000000c2", bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"total":"1"`) {
		t.Fatalf("list dead letters(channel): status = %d body = %s", code, body)
	}
	start := base.Add(90 * time.Second).UTC().Format(time.RFC3339)
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/dead-letter?page=1&page_size=10&createTimeStart="+start, bearer, "")
	// ≥ base+90s：仅剩 +2min 的 replayed 行。
	if code != http.StatusOK || !strings.Contains(body, `"total":"1"`) ||
		!strings.Contains(body, `"replayState":3`) {
		t.Fatalf("list dead letters(time range): status = %d body = %s", code, body)
	}

	// 标记重放：none → pending，返回权威行。
	code, body = doRequestWithToken(t, http.MethodPost,
		ts.URL+"/api/v1/notify/dead-letter/"+replayable+":replay", bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"replayState":2`) ||
		!strings.Contains(body, `"lastError":"smtp: 421 service unavailable"`) {
		t.Fatalf("replay dead letter: status = %d body = %s, want 200 + pending row", code, body)
	}
	// 状态机：重复标记 / 已 pending / 已 replayed 均 409。
	for _, id := range []string{
		replayable,
		"018f3e1a-0304-7000-8000-0000000000a2",
		"018f3e1a-0304-7000-8000-0000000000a3",
	} {
		code, body = doRequestWithToken(t, http.MethodPost,
			ts.URL+"/api/v1/notify/dead-letter/"+id+":replay", bearer, "")
		if code != http.StatusConflict {
			t.Fatalf("replay dead letter(%s): status = %d body = %q, want 409", id, code, body)
		}
	}
	// 不存在 → 404。
	code, body = doRequestWithToken(t, http.MethodPost,
		ts.URL+"/api/v1/notify/dead-letter/018f3e1a-0304-7000-8000-0000000000af:replay", bearer, "")
	if code != http.StatusNotFound {
		t.Fatalf("replay missing dead letter: status = %d body = %q, want 404", code, body)
	}

	// 非法参数：replayState=9 → protovalidate 400；id 非 uuid → 400；时间
	// 区间倒挂 → CEL 400。
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/dead-letter?page=1&page_size=10&replayState=9", bearer, "")
	if code != http.StatusBadRequest {
		t.Fatalf("bad replayState: status = %d body = %q, want 400", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodPost,
		ts.URL+"/api/v1/notify/dead-letter/not-an-uuid:replay", bearer, "")
	if code != http.StatusBadRequest {
		t.Fatalf("bad replay id: status = %d body = %q, want 400", code, body)
	}
	end := base.Add(-2 * time.Hour).UTC().Format(time.RFC3339)
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/dead-letter?page=1&page_size=10&createTimeStart="+start+"&createTimeEnd="+end, bearer, "")
	if code != http.StatusBadRequest {
		t.Fatalf("inverted time range: status = %d body = %q, want 400", code, body)
	}
	// 分页越界：空页 + total 不变。
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/dead-letter?page=9&page_size=10", bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"total":"3"`) || strings.Contains(body, `"items":[{"`) {
		t.Fatalf("list dead letters(paged out): status = %d body = %s", code, body)
	}
}
