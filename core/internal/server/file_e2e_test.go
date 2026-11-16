package server

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

// 附件：上传（内容嗅探 + 白名单）→ 元数据 → 下载（附件方式 + 安全响应头）→ 访问隔离。
func TestHTTPServer_FileEndToEnd(t *testing.T) {
	t.Parallel()
	ts, signer := newTestHTTPServer(t)
	owner := testBearer(t, signer, e2eActorID)
	other := createUser(t, ts, owner, "mallory")
	otherBearer := testBearer(t, signer, other.Id)

	content := []byte("%PDF-1.7\n1 0 obj\n<<>>\nendobj\n")
	upload := func(token, filename string, data []byte) (int, string) {
		t.Helper()
		body, err := json.Marshal(map[string]string{"filename": filename, "data": base64.StdEncoding.EncodeToString(data)})
		if err != nil {
			t.Fatalf("marshal upload: %v", err)
		}
		return doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/file", token, string(body))
	}

	// 匿名上传被拒（上传免接口授权，但不免登录）。
	if code, _ := upload("", "a.pdf", content); code != http.StatusUnauthorized {
		t.Fatalf("anonymous upload: status = %d, want 401", code)
	}

	code, body := upload(owner, `季度"报告".pdf`, content)
	if code != http.StatusOK {
		t.Fatalf("upload: status = %d body = %s", code, body)
	}
	var uploaded struct {
		File struct {
			Id       string `json:"id"`
			Filename string `json:"filename"`
			Size     string `json:"size"`
			Mime     string `json:"mime"`
		} `json:"file"`
	}
	if err := json.Unmarshal([]byte(body), &uploaded); err != nil {
		t.Fatalf("upload unmarshal %q: %v", body, err)
	}
	if uploaded.File.Id == "" || uploaded.File.Mime != "application/pdf" || uploaded.File.Size != "29" {
		t.Fatalf("uploaded = %+v", uploaded.File)
	}
	for _, leak := range []string{"objectKey", "driver", "sha256"} {
		if strings.Contains(body, leak) {
			t.Fatalf("upload response must not expose %s: %s", leak, body)
		}
	}

	// 类型按内容判定：伪装成 .pdf 的 HTML 被拒；文件名带路径被拒。
	if code, body := upload(owner, "evil.pdf", []byte("<html><script>alert(1)</script></html>")); code != http.StatusBadRequest || !strings.Contains(body, "FILE_TYPE_NOT_ALLOWED") {
		t.Fatalf("html upload: status = %d body = %q", code, body)
	}
	if code, body := upload(owner, "../x.pdf", content); code != http.StatusBadRequest || !strings.Contains(body, "FILE_NAME_INVALID") {
		t.Fatalf("path filename: status = %d body = %q", code, body)
	}

	// 元数据与下载：上传者可读；他人按不存在处理。
	fileURL := ts.URL + "/api/v1/file/" + uploaded.File.Id
	if code, body := doRequestWithToken(t, http.MethodGet, fileURL, owner, ""); code != http.StatusOK || !strings.Contains(body, `"mime":"application/pdf"`) {
		t.Fatalf("get file: status = %d body = %s", code, body)
	}
	req, err := http.NewRequest(http.MethodGet, fileURL+"/content", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+owner)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read download: %v", err)
	}
	if resp.StatusCode != http.StatusOK || string(got) != string(content) {
		t.Fatalf("download: status = %d body = %q", resp.StatusCode, got)
	}
	disposition := resp.Header.Get("Content-Disposition")
	if !strings.HasPrefix(disposition, "attachment;") || strings.ContainsAny(disposition, "\r\n") {
		t.Fatalf("Content-Disposition = %q, want attachment without CR/LF", disposition)
	}
	if resp.Header.Get("Content-Type") != "application/pdf" || resp.Header.Get("X-Content-Type-Options") != "nosniff" ||
		resp.Header.Get("Content-Security-Policy") != "sandbox" || !strings.Contains(resp.Header.Get("Cache-Control"), "no-store") {
		t.Fatalf("download headers = %v", resp.Header)
	}
	for _, path := range []string{"", "/content"} {
		if code, body := doRequestWithToken(t, http.MethodGet, fileURL+path, otherBearer, ""); code != http.StatusNotFound || !strings.Contains(body, "FILE_NOT_FOUND") {
			t.Fatalf("other user reads temp file%s: status = %d body = %q", path, code, body)
		}
	}
	if code, _ := doRequestWithToken(t, http.MethodGet, fileURL+"/content", "", ""); code != http.StatusUnauthorized {
		t.Fatalf("anonymous download: status = %d, want 401", code)
	}
}
