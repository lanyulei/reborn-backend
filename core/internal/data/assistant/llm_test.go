package data

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/assistant"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
)

func newTestChatModel(url string) *ChatModelClient {
	return NewChatModel(&conf.Assistant{BaseUrl: url, ApiKey: "k", Model: "m"})
}

func userRequest() biz.ChatRequest {
	return biz.ChatRequest{Messages: []*biz.Message{{Role: biz.RoleUser, Content: "hi"}}}
}

const sseChunk = `data: {"id":"1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant","content":"你好"},"finish_reason":null}]}` + "\n\n"

// 上游发出首个分片后不再出数据：空闲看门狗中止请求并报模型不可用，而不是挂到整轮超时。
func TestChatModel_StreamIdleTimeout(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, sseChunk)
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(func() { close(release); ts.Close() })
	m := newTestChatModel(ts.URL)
	m.idleTimeout = 200 * time.Millisecond

	started := time.Now()
	_, err := m.Stream(context.Background(), userRequest(), func(biz.Delta) error { return nil })
	var rejected *rejectedError
	if !errors.Is(err, biz.ErrLLMUnavailable) || errors.As(err, &rejected) {
		t.Fatalf("Stream() error = %v, want ErrLLMUnavailable", err)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("Stream() returned after %v, idle watchdog did not fire", elapsed)
	}
}

// 端点不认识 stream_options：去掉后重试一次成功，此后不再携带。
func TestChatModel_StreamOptionsFallback(t *testing.T) {
	t.Parallel()
	var withOptions, total atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		total.Add(1)
		if strings.Contains(string(body), "stream_options") {
			withOptions.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"error":{"message":"unknown field stream_options","type":"invalid_request_error"}}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, sseChunk+"data: [DONE]\n\n")
	}))
	t.Cleanup(ts.Close)
	m := newTestChatModel(ts.URL)

	for i := range 2 {
		out, err := m.Stream(context.Background(), userRequest(), func(biz.Delta) error { return nil })
		if err != nil || out.Content != "你好" {
			t.Fatalf("Stream() #%d = %+v, %v", i+1, out, err)
		}
	}
	if withOptions.Load() != 1 || total.Load() != 3 {
		t.Fatalf("requests with stream_options = %d, total = %d; want 1 and 3", withOptions.Load(), total.Load())
	}
}

// 请求本身被拒绝（4xx）与上游故障（5xx）分开归类。
func TestChatModel_ErrorClassification(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		status   int
		rejected bool
	}{
		{http.StatusBadRequest, true},
		{http.StatusTooManyRequests, false},
		{http.StatusUnauthorized, false},
		{http.StatusBadGateway, false},
	} {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(tc.status)
			_, _ = io.WriteString(w, `{"error":{"message":"x","type":"t"}}`)
		}))
		m := newTestChatModel(ts.URL)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		_, err := m.Stream(ctx, userRequest(), func(biz.Delta) error { return nil })
		cancel()
		ts.Close()
		if !errors.Is(err, biz.ErrLLMUnavailable) {
			t.Fatalf("status %d: error = %v, want a model error", tc.status, err)
		}
		var rejected *rejectedError
		if got := errors.As(err, &rejected); got != tc.rejected {
			t.Fatalf("status %d: rejected = %v, want %v (err = %v)", tc.status, got, tc.rejected, err)
		}
	}
}
