package service

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// handler 返回后 net/http 会回收 ResponseWriter（内部缓冲置 nil）：
// close 之后迟到的写（如心跳）必须放弃而不是 panic。见 serveStream 的 defer sw.close()。
func TestSSEWriterClose(t *testing.T) {
	rec := httptest.NewRecorder()
	sw := &sseWriter{w: rec, rc: http.NewResponseController(rec)}

	if err := sw.event(k8scontract.EventPing, "1", k8scontract.Empty{}); err != nil {
		t.Fatalf("first event should succeed: %v", err)
	}
	if !sw.isStarted() {
		t.Fatal("stream should be started after the first event")
	}

	sw.close()
	err := sw.event(k8scontract.EventPing, "2", k8scontract.Empty{})
	if err == nil || !strings.Contains(err.Error(), "stream closed") {
		t.Fatalf("event after close should fail with stream closed, got %v", err)
	}
	if strings.Count(rec.Body.String(), "event: ") != 1 {
		t.Fatalf("closed writer must not emit events, body: %q", rec.Body.String())
	}
}
