package server

// 生产者管理 e2e：创建签发（token 仅一次）、spec 整组替换、轮换、软删与列表，
// 全程 token/哈希不出现在任何读响应。

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestHTTPServer_NotifyProducerEndToEnd(t *testing.T) {
	t.Parallel()
	ts, signer := newTestHTTPServer(t)
	bearer := testBearer(t, signer, e2eActorID)

	// 匿名请求被 auth 中间件拦截。
	code, _ := doRequest(t, http.MethodGet, ts.URL+"/api/v1/notify/producer?page=1&page_size=10", "")
	if code != http.StatusUnauthorized {
		t.Fatalf("anonymous list: status = %d, want 401", code)
	}

	// 创建：token 明文仅本响应出现一次。
	code, body := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/notify/producer", bearer,
		`{"spec":{"name":"monitor-engine","eventWhitelist":["monitor.alert.*","monitor.host.down"],`+
			`"expireTime":"`+time.Now().UTC().Add(time.Hour).Format(time.RFC3339)+`"}}`)
	if code != http.StatusOK {
		t.Fatalf("create producer: status = %d body = %s", code, body)
	}
	var created struct {
		Producer struct {
			Id             string   `json:"id"`
			Name           string   `json:"name"`
			EventWhitelist []string `json:"eventWhitelist"`
		} `json:"producer"`
		Token string `json:"token"`
	}
	if err := json.Unmarshal([]byte(body), &created); err != nil {
		t.Fatalf("create producer unmarshal %q: %v", body, err)
	}
	if created.Producer.Id == "" || created.Producer.Name != "monitor-engine" ||
		len(created.Producer.EventWhitelist) != 2 {
		t.Fatalf("create producer = %q", body)
	}
	if !strings.HasPrefix(created.Token, "npt_") || len(created.Token) != 47 {
		t.Fatalf("create producer token = %q, want npt_ + 43 chars", created.Token)
	}
	for _, want := range []string{`"bizId":"default"`, `"enabled":true`} {
		if !strings.Contains(body, want) {
			t.Fatalf("create producer body %q missing %s", body, want)
		}
	}

	// 非法白名单条目：protovalidate 400。
	code, body = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/notify/producer", bearer,
		`{"spec":{"name":"bad-engine","eventWhitelist":["monitor.*.firing"]}}`)
	if code != http.StatusBadRequest {
		t.Fatalf("bad whitelist: status = %d body = %q, want 400", code, body)
	}

	// 更新：spec 整组替换（不传 expireTime 即清除）+ enabled 独立启停。
	code, body = doRequestWithToken(t, http.MethodPut,
		ts.URL+"/api/v1/notify/producer/"+created.Producer.Id, bearer,
		`{"spec":{"name":"monitor-engine-v2","eventWhitelist":["permission.change"]},"enabled":false}`)
	if code != http.StatusOK {
		t.Fatalf("update producer: status = %d body = %s", code, body)
	}

	// 详情：替换生效，且响应不含 token 字段。
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/producer/"+created.Producer.Id, bearer, "")
	if code != http.StatusOK {
		t.Fatalf("get producer: status = %d body = %s", code, body)
	}
	for _, want := range []string{
		`"name":"monitor-engine-v2"`, `"permission.change"`, `"enabled":false`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("get producer body %q missing %s", body, want)
		}
	}
	if strings.Contains(body, "token") || strings.Contains(body, "npt_") {
		t.Fatalf("get producer body %q must not echo token", body)
	}

	// 轮换：新 token 与初发不同，同样仅本次响应出现。
	code, body = doRequestWithToken(t, http.MethodPost,
		ts.URL+"/api/v1/notify/producer/"+created.Producer.Id+"/token:rotate", bearer, `{}`)
	if code != http.StatusOK {
		t.Fatalf("rotate producer token: status = %d body = %s", code, body)
	}
	var rotated struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal([]byte(body), &rotated); err != nil {
		t.Fatalf("rotate unmarshal %q: %v", body, err)
	}
	if rotated.Token == created.Token || !strings.HasPrefix(rotated.Token, "npt_") {
		t.Fatalf("rotate token = %q, want fresh npt_ token", rotated.Token)
	}

	// 同 biz 重名：409 哨兵。
	code, body = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/notify/producer", bearer,
		`{"spec":{"name":"monitor-engine-v2"}}`)
	if code != http.StatusConflict {
		t.Fatalf("dup name: status = %d body = %q, want 409", code, body)
	}

	// 列表：keyword 过滤 + 读响应无 token；再删 + 详情 404。
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/producer?page=1&page_size=10&keyword=v2", bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"total":"1"`) {
		t.Fatalf("list producers: status = %d body = %s", code, body)
	}
	if strings.Contains(body, "npt_") {
		t.Fatalf("list producers body %q must not echo token", body)
	}
	code, body = doRequestWithToken(t, http.MethodDelete,
		ts.URL+"/api/v1/notify/producer/"+created.Producer.Id, bearer, "")
	if code != http.StatusOK {
		t.Fatalf("delete producer: status = %d body = %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/notify/producer/"+created.Producer.Id, bearer, "")
	if code != http.StatusNotFound {
		t.Fatalf("get deleted producer: status = %d body = %q, want 404", code, body)
	}
	// 软删后轮换：404。
	code, _ = doRequestWithToken(t, http.MethodPost,
		ts.URL+"/api/v1/notify/producer/"+created.Producer.Id+"/token:rotate", bearer, `{}`)
	if code != http.StatusNotFound {
		t.Fatalf("rotate deleted producer: status = %d, want 404", code)
	}
}
