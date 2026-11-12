package server

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// 头像：自助上传（免授权）→ /me 回填 avatar 地址 → 匿名可读文件；校验与 404 口径。
func TestHTTPServer_Avatar(t *testing.T) {
	t.Parallel()
	ts, svc := newTestHTTPServerWithServices(t)
	admin := testBearer(t, svc.signer, e2eActorID)
	user := createUser(t, ts, admin, "avatar_user")
	// 只授予一个无关接口的普通用户也能上传自己的头像（自助接口免授权）。
	grantAssistantRole(t, ts, admin, user.Id, "avatar_role", "/system.v1.UserService/ListUsers")
	bearer := testBearer(t, svc.signer, user.Id)
	png := base64.StdEncoding.EncodeToString([]byte("\x89PNG\r\n\x1a\nfake"))

	code, body := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/me/avatar", bearer, `{"data":"`+png+`","mime":"image/png"}`)
	if code != http.StatusOK || !strings.Contains(body, `"url":"/api/v1/user/`+user.Id+`/avatar?v=`) {
		t.Fatalf("upload avatar: %d %s", code, body)
	}
	var up struct {
		URL string `json:"url"`
	}
	_ = json.Unmarshal([]byte(body), &up)
	// /me 回填头像地址。
	if code, body := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/me", bearer, ""); code != http.StatusOK || !strings.Contains(body, `"avatar":"`+up.URL+`"`) {
		t.Fatalf("profile avatar: %d %s", code, body)
	}
	// 匿名读取文件：正确的 Content-Type 与安全头。
	resp, err := http.Get(ts.URL + up.URL)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "image/png" || resp.Header.Get("Content-Security-Policy") != "sandbox" {
		t.Fatalf("get avatar: %d %s csp=%q", resp.StatusCode, resp.Header.Get("Content-Type"), resp.Header.Get("Content-Security-Policy"))
	}
	// 未设置头像的用户 → 404；非法 ID → 400。
	if code, body := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/user/"+e2eActorID+"/avatar", "", ""); code != http.StatusNotFound || !strings.Contains(body, "AVATAR_NOT_FOUND") {
		t.Fatalf("missing avatar: %d %s", code, body)
	}
	if code, _ := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/user/not-a-uuid/avatar", "", ""); code != http.StatusBadRequest {
		t.Fatalf("invalid id must be 400: %d", code)
	}
	// 校验：SVG 拒绝、空内容拒绝、未登录拒绝。
	if code, _ := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/me/avatar", bearer, `{"data":"`+png+`","mime":"image/svg+xml"}`); code != http.StatusBadRequest {
		t.Fatalf("svg must be rejected: %d", code)
	}
	if code, _ := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/me/avatar", bearer, `{"data":"","mime":"image/png"}`); code != http.StatusBadRequest {
		t.Fatalf("empty data must be rejected: %d", code)
	}
	if code, _ := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/me/avatar", "", `{"data":"`+png+`","mime":"image/png"}`); code != http.StatusUnauthorized {
		t.Fatalf("anonymous upload must be 401: %d", code)
	}
	// 重新上传不同内容 → 指纹变化（URL 不同）；用户列表接口不受影响仍可查。
	jpg := base64.StdEncoding.EncodeToString([]byte("\xff\xd8\xff fake jpeg"))
	code, body = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/me/avatar", bearer, `{"data":"`+jpg+`","mime":"image/jpeg"}`)
	if code != http.StatusOK || strings.Contains(body, up.URL) {
		t.Fatalf("re-upload must change fingerprint: %d %s", code, body)
	}
	resp, err = http.Get(ts.URL + "/api/v1/user/" + user.Id + "/avatar")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.Header.Get("Content-Type") != "image/jpeg" {
		t.Fatalf("content type after re-upload = %s", resp.Header.Get("Content-Type"))
	}
}
