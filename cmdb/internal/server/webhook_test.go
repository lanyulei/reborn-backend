package server

import (
	"context"
	"crypto/sha256"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/cmdb/internal/biz"
	"github.com/lanyulei/reborn/backend/cmdb/internal/conf"
	"github.com/lanyulei/reborn/backend/cmdb/internal/service"
	"github.com/lanyulei/reborn/backend/kit/cmdbcontract"
)

type fakeWebhookRepo struct {
	targets map[string]*biz.WebhookTarget
	touched map[string]time.Time
	active  map[string]string
	trigger string
}

func (f *fakeWebhookRepo) GetWebhook(_ context.Context, id string) (*biz.WebhookTarget, error) {
	t, ok := f.targets[id]
	if !ok {
		return nil, biz.ErrWebhookSourceNotFound
	}
	return t, nil
}

func (f *fakeWebhookRepo) TouchWebhook(_ context.Context, id string, min time.Duration) (bool, error) {
	if last, ok := f.touched[id]; ok && time.Since(last) < min {
		return false, nil
	}
	f.touched[id] = time.Now()
	return true, nil
}

func (f *fakeWebhookRepo) EnqueueTask(_ context.Context, id, trigger string) (string, bool, error) {
	f.trigger = trigger
	if cur, ok := f.active[id]; ok {
		return cur, false, nil
	}
	f.active[id] = "task-" + id[:8]
	return f.active[id], true, nil
}

// 回调接入端到端：令牌校验、404 口径、限流、重复入队、角色门控。
func TestWebhook_Trigger(t *testing.T) {
	const (
		src   = "11111111-1111-1111-1111-111111111111"
		off   = "22222222-2222-2222-2222-222222222222"
		token = "deadbeef"
	)
	sum := sha256.Sum256([]byte(token))
	repo := &fakeWebhookRepo{
		targets: map[string]*biz.WebhookTarget{src: {ID: src, Enabled: true, TokenHash: sum[:]}, off: {ID: off, Enabled: false, TokenHash: sum[:]}},
		touched: map[string]time.Time{}, active: map[string]string{},
	}
	svc := service.NewWebhookService(biz.NewWebhookUsecase(repo))
	srv := NewHTTPServer(&conf.Server{Http: &conf.Server_HTTP{}}, Roles{RoleWebhook: {}}, testLogger(), pinger{}, svc)
	ts := httptest.NewServer(srv)
	defer ts.Close()
	call := func(id, tok, body string) (int, string) {
		req, _ := http.NewRequest(http.MethodPost, ts.URL+cmdbcontract.WebhookPath(id), strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if tok != "" {
			req.Header.Set(cmdbcontract.WebhookTokenHeader, tok)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	if code, body := call(src, "", `{"event":"x"}`); code != http.StatusUnauthorized || !strings.Contains(body, "WEBHOOK_UNAUTHORIZED") {
		t.Fatalf("missing token: %d %s", code, body)
	}
	if code, _ := call(src, "wrong", `{}`); code != http.StatusUnauthorized {
		t.Fatalf("wrong token must be 401: %d", code)
	}
	if code, body := call(off, token, `{}`); code != http.StatusNotFound || !strings.Contains(body, "WEBHOOK_SOURCE_NOT_FOUND") {
		t.Fatalf("disabled source: %d %s", code, body)
	}
	if code, _ := call("33333333-3333-3333-3333-333333333333", token, `{}`); code != http.StatusNotFound {
		t.Fatalf("unknown source must be 404: %d", code)
	}
	if code, _ := call("not-a-uuid", token, `{}`); code != http.StatusBadRequest {
		t.Fatalf("invalid id must be 400: %d", code)
	}
	// 首次：入队；厂商任意 JSON 体可接受。
	code, body := call(src, token, `{"source":"aliyun.ecs","data":{"id":"i-1"},"list":[1,2]}`)
	if code != http.StatusOK || !strings.Contains(body, `"queued":true`) || repo.trigger != cmdbcontract.TriggerWebhook {
		t.Fatalf("first trigger: %d %s trigger=%s", code, body, repo.trigger)
	}
	// 60s 内再次：限流。
	if code, body := call(src, token, `{}`); code != http.StatusTooManyRequests || !strings.Contains(body, "WEBHOOK_RATE_LIMITED") {
		t.Fatalf("rate limited: %d %s", code, body)
	}
	// 限流窗口过去但任务仍在：queued=false 并返回当前任务。
	repo.touched[src] = time.Now().Add(-2 * time.Minute)
	if code, body := call(src, token, ``); code != http.StatusOK || !strings.Contains(body, `"queued":false`) || !strings.Contains(body, "task-11111111") {
		t.Fatalf("already active: %d %s", code, body)
	}
	// 未启用 webhook 角色：路由不存在。
	noRole := httptest.NewServer(NewHTTPServer(&conf.Server{Http: &conf.Server_HTTP{}}, Roles{RoleWorker: {}}, testLogger(), pinger{}, svc))
	defer noRole.Close()
	req, _ := http.NewRequest(http.MethodPost, noRole.URL+cmdbcontract.WebhookPath(src), strings.NewReader(`{}`))
	req.Header.Set(cmdbcontract.WebhookTokenHeader, token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("webhook route must be absent without role: %d", resp.StatusCode)
	}
}
