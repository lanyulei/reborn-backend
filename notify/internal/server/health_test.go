package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/notify/internal/biz"
)

type cfgRepoStub struct {
	versions biz.ConfigVersions
	fail     bool
}

func (s *cfgRepoStub) LoadVersions(context.Context) (biz.ConfigVersions, error) {
	if s.fail {
		return nil, errors.New("db down")
	}
	return s.versions, nil
}

func (s *cfgRepoStub) LoadSnapshot(context.Context) (*biz.Snapshot, error) {
	if s.fail {
		return nil, errors.New("db down")
	}
	return biz.NewSnapshot(s.versions, nil, nil, nil, nil, nil, time.Now()), nil
}

type bucketStub struct{}

func (bucketStub) SyncBuckets(context.Context, []*biz.Channel) error { return nil }

type pingStub struct{ err error }

func (p pingStub) Ping() error { return p.err }

// TestHealth_ConfigStale 配置刷新长期失败 → 503（旧快照仍在用但已脱离控制面）。
func TestHealth_ConfigStale(t *testing.T) {
	t.Parallel()
	repo := &cfgRepoStub{versions: biz.ConfigVersions{"notify_channel": 1}}
	cache := biz.NewConfigCache(repo, bucketStub{}, testLogger())
	h := NewHealth(cache, pingStub{}, nil)
	now := time.Now()
	h.now = func() time.Time { return now }
	get := func() int {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
		return rec.Code
	}
	if get() != http.StatusServiceUnavailable {
		t.Fatal("unloaded config must be unhealthy")
	}
	if err := cache.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	if get() != http.StatusOK {
		t.Fatal("fresh config must be healthy")
	}
	repo.fail = true
	for i := 0; i < 3; i++ {
		_ = cache.Refresh(context.Background())
	}
	now = now.Add(configStaleAfter + time.Second)
	if get() != http.StatusServiceUnavailable {
		t.Fatal("stale config must be unhealthy")
	}
	if st := cache.Status(); st.Failures != 3 || st.LoadedAt.IsZero() {
		t.Fatalf("status = %+v", st)
	}
	repo.fail = false
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	now = time.Now() // 恢复后的 lastOK 取真实时钟
	if get() != http.StatusOK {
		t.Fatal("recovered refresh must be healthy again")
	}
}

type backboneStub struct{ err error }

func (b backboneStub) Name() string               { return "nats" }
func (b backboneStub) Ping(context.Context) error { return b.err }

// TestHealth_Backbone 骨干不可达 → 503。
func TestHealth_Backbone(t *testing.T) {
	t.Parallel()
	repo := &cfgRepoStub{versions: biz.ConfigVersions{"notify_channel": 1}}
	cache := biz.NewConfigCache(repo, bucketStub{}, testLogger())
	if err := cache.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	bb := &backboneStub{}
	h := NewHealth(cache, pingStub{}, bb)
	get := func() int {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
		return rec.Code
	}
	if get() != http.StatusOK {
		t.Fatal("healthy backbone")
	}
	bb.err = errors.New("nats disconnected")
	if get() != http.StatusServiceUnavailable {
		t.Fatal("unreachable backbone must be unhealthy")
	}
}
