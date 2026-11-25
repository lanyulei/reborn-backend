package kube

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// chartArchive 一个最小的 Chart 包：<name>/Chart.yaml。
func chartArchive(t *testing.T, name, version string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)
	body := "apiVersion: v2\nkind: Chart\nname: " + name + "\nversion: " + version + "\n"
	if err := tw.WriteHeader(&tar.Header{Typeflag: tar.TypeReg, Name: name + "/Chart.yaml", Size: int64(len(body)), Mode: 0o644}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte(body)); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

type fixedRepos struct{ repo *HelmRepo }

func (f fixedRepos) HelmRepo(context.Context, string) (*HelmRepo, error) { return f.repo, nil }

// httpChartRepo 一个经典 HTTP 仓库：index.yaml 里只有 Chart demo 的 0.1.0。
type httpChartRepo struct {
	tgz []byte
	mu  sync.Mutex
	get []string
}

func (h *httpChartRepo) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	h.get = append(h.get, r.URL.Path)
	h.mu.Unlock()
	switch r.URL.Path {
	case "/index.yaml":
		_, _ = io.WriteString(w, "apiVersion: v1\nentries:\n  demo:\n    - {name: demo, version: 0.1.0, urls: [demo-0.1.0.tgz]}\n")
	case "/demo-0.1.0.tgz":
		_, _ = w.Write(h.tgz)
	default:
		http.NotFound(w, r)
	}
}

func newChartRepo(t *testing.T) (*httpChartRepo, func() *Manager) {
	t.Helper()
	repo := &httpChartRepo{tgz: chartArchive(t, "demo", "0.1.0")}
	srv := httptest.NewServer(repo)
	t.Cleanup(srv.Close)
	// 每次调用给出一个全新的 Manager：索引缓存是冷的，下载名额与生产一样是 helmFetchParallel 个。
	return repo, func() *Manager {
		m := &Manager{}
		m.EnableApps(fixedRepos{repo: &HelmRepo{ID: uid1, Name: "demo", Type: k8scontract.HelmRepoHTTP, URL: srv.URL, Enabled: true}})
		return m
	}
}

// 下载 Chart 时不能持着下载名额去取索引：索引未缓存时它的下载自己也要占一个名额。名额只剩一个的那次
// 请求必须成功（持着名额等索引的话，索引的下载永远拿不到名额，请求挂到超时）。
func TestLoadChartDoesNotHoldFetchSlotWhileFetchingIndex(t *testing.T) {
	t.Parallel()
	repo, fresh := newChartRepo(t)
	m := fresh()
	for range helmFetchParallel - 1 { // 占住其余名额，只剩一个
		release, err := m.apps.acquireFetch(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer release()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ch, r, data, err := m.loadChartData(ctx, uid1, "demo", "0.1.0")
	if err != nil {
		t.Fatalf("load chart with one free slot: %v", err)
	}
	if ch == nil || ch.Metadata == nil || ch.Metadata.Name != "demo" || ch.Metadata.Version != "0.1.0" || r.Name != "demo" || !bytes.Equal(data, repo.tgz) {
		t.Fatalf("chart = %+v repo = %+v data = %d bytes", ch.Metadata, r, len(data))
	}
	if m.apps.cached(uid1+"/0") == nil { // 索引缓存的键 = 仓库 ID / 版本（这里是 0）
		t.Fatal("the index must be cached after the download")
	}
}

// 报告里的场景：索引缓存过期后， helmFetchParallel 个以上的人同时打开 Chart / 安装，全部成功。
func TestLoadChartConcurrentColdIndex(t *testing.T) {
	t.Parallel()
	repo, fresh := newChartRepo(t)
	m := fresh()
	const callers = 3 * helmFetchParallel
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() { // 退出路径：loadChartData 受 ctx 约束
			defer wg.Done()
			_, _, data, err := m.loadChartData(ctx, uid1, "demo", "0.1.0")
			if err == nil && !bytes.Equal(data, repo.tgz) {
				err = fmt.Errorf("chart data differs from the repository (%d bytes)", len(data))
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent load: %v", err)
		}
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	indexes := 0
	for _, p := range repo.get {
		if p == "/index.yaml" {
			indexes++
		}
	}
	if indexes != 1 {
		t.Fatalf("index.yaml was downloaded %d times; concurrent requests must share one download", indexes)
	}
}
