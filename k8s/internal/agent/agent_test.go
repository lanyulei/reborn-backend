package agent

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

func TestFileStore(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "state")
	s := NewFileStore(dir)
	ctx := context.Background()
	if tok, err := s.Load(ctx); err != nil || tok != "" {
		t.Fatalf("empty store: %q %v", tok, err)
	}
	if err := s.Save(ctx, "abc.def"); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(filepath.Join(dir, "token"))
	if err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("token file mode = %v, %v", fi.Mode().Perm(), err)
	}
	if tok, _ := s.Load(ctx); tok != "abc.def" {
		t.Fatalf("loaded %q", tok)
	}
	if err := s.Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx); err != nil {
		t.Fatalf("delete twice: %v", err)
	}
}

// 令牌持续被拒绝（401）：达到次数与时长门槛后删除本地令牌并停止；一两次 401 不删（可能来自前置代理）。
// 集群被停用（403）：保留令牌、继续重试。不并行：改了包级的门槛。
func TestRunTokenRejection(t *testing.T) {
	oldLimit, oldWindow, oldBackoff := rejectLimit, rejectWindow, rejectBackoff
	rejectLimit, rejectWindow, rejectBackoff = 3, 0, 5*time.Millisecond
	defer func() { rejectLimit, rejectWindow, rejectBackoff = oldLimit, oldWindow, oldBackoff }()

	var hits atomic.Int32
	status := atomic.Int32{}
	status.Store(http.StatusForbidden)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == k8scontract.AgentConnectPath {
			if hits.Add(1) == 4 { // 前 3 次：集群被停用；之后：令牌无效
				status.Store(http.StatusUnauthorized)
			}
		}
		http.Error(w, "rejected", int(status.Load()))
	}))
	defer srv.Close()
	store := NewFileStore(t.TempDir())
	_ = store.Save(context.Background(), "some-token")
	a, err := New(Config{ServerURL: srv.URL, Version: "test"}, store, http.NotFoundHandler(), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := a.Run(ctx); !errors.Is(err, ErrTokenRejected) {
		t.Fatalf("Run = %v, want ErrTokenRejected", err)
	}
	// 3 次 403（不计入）+ 3 次 401 才放弃。
	if n := hits.Load(); n != 6 {
		t.Fatalf("connect attempts = %d, want 6", n)
	}
	if tok, _ := store.Load(context.Background()); tok != "" {
		t.Fatal("a token rejected repeatedly must be removed from the store")
	}
}

// 单次 401 之后恢复正常：令牌必须还在。
func TestRunKeepsTokenAfterSingleRejection(t *testing.T) {
	oldLimit, oldWindow, oldBackoff := rejectLimit, rejectWindow, rejectBackoff
	rejectLimit, rejectWindow, rejectBackoff = 3, 0, 5*time.Millisecond
	defer func() { rejectLimit, rejectWindow, rejectBackoff = oldLimit, oldWindow, oldBackoff }()

	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits.Add(1) == 1 {
			http.Error(w, "rejected", http.StatusUnauthorized)
			return
		}
		http.Error(w, "busy", http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	store := NewFileStore(t.TempDir())
	_ = store.Save(context.Background(), "some-token")
	a, err := New(Config{ServerURL: srv.URL, Version: "test"}, store, http.NotFoundHandler(), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := a.Run(ctx); err != nil {
		t.Fatalf("Run = %v, want nil (stopped by context)", err)
	}
	if tok, _ := store.Load(context.Background()); tok != "some-token" {
		t.Fatalf("token after a single rejection = %q, must be kept", tok)
	}
}

func TestRunWithoutAnyToken(t *testing.T) {
	t.Parallel()
	a, err := New(Config{ServerURL: "http://127.0.0.1:1"}, NewFileStore(t.TempDir()), http.NotFoundHandler(), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Run(context.Background()); err == nil {
		t.Fatal("Run must fail without a stored or register token")
	}
}

func TestNewRejectsBadServerURL(t *testing.T) {
	t.Parallel()
	for _, u := range []string{"", "ws://x", "https://user:pw@x", "x"} {
		if _, err := New(Config{ServerURL: u}, NewFileStore(t.TempDir()), nil, slog.New(slog.DiscardHandler)); err == nil {
			t.Errorf("New(%q) must fail", u)
		}
	}
}
