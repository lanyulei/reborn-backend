package kube

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	authv1 "k8s.io/api/authorization/v1"
)

// selfCheckFixture 假集群：权限询问一律允许（denyResource 除外），内置 ClusterRole 都存在（missingRole 除外）；
// 每个请求停留 hold，用来观察并发度。
type selfCheckFixture struct {
	denyResource, missingRole string
	hold                      time.Duration

	mu            sync.Mutex
	running, peak int
}

func (f *selfCheckFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.running++
	f.peak = max(f.peak, f.running)
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.running--
		f.mu.Unlock()
	}()
	select {
	case <-time.After(f.hold):
	case <-r.Context().Done():
		return
	}
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews":
		var review authv1.SelfSubjectAccessReview
		if err := decodeClusterBody(r, &review); err != nil || review.Spec.ResourceAttributes == nil {
			http.Error(w, "bad review", http.StatusBadRequest)
			return
		}
		review.Status.Allowed = review.Spec.ResourceAttributes.Resource != f.denyResource
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(review)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/apis/rbac.authorization.k8s.io/v1/clusterroles/"):
		name := strings.TrimPrefix(r.URL.Path, "/apis/rbac.authorization.k8s.io/v1/clusterroles/")
		if name == f.missingRole {
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404,"message":"clusterrole not found"}`)
			return
		}
		// 固定的响应：客户端只看有没有，响应内容不取自请求。
		_, _ = io.WriteString(w, `{"kind":"ClusterRole","apiVersion":"rbac.authorization.k8s.io/v1","metadata":{"name":"role"}}`)
	default:
		clusterNotFound(w)
	}
}

// 自检项有界并发发出：用时不再是各项之和；结果的顺序与逐项串行时一致（版本 → 各权限项 → 内置角色）。
func TestSelfCheckRunsInParallelAndKeepsOrder(t *testing.T) {
	t.Parallel()
	f := &selfCheckFixture{hold: 30 * time.Millisecond}
	u := fakeClusterUser(t, f)
	got := u.c.selfCheck(context.Background(), "v1.35.0")
	if len(got) != 1+len(selfChecks)+3 {
		t.Fatalf("items = %d, want %d", len(got), 1+len(selfChecks)+3)
	}
	for i, ck := range selfChecks {
		if got[1+i].Name != ck.name {
			t.Fatalf("item %d = %q, want %q (order must not depend on completion order)", 1+i, got[1+i].Name, ck.name)
		}
	}
	for i, role := range []string{"view", "edit", "admin"} {
		if name := got[1+len(selfChecks)+i].Name; name != "built-in clusterrole "+role {
			t.Fatalf("role item %d = %q", i, name)
		}
	}
	for _, item := range got {
		if !item.OK {
			t.Fatalf("%s must pass: %s", item.Name, item.Message)
		}
	}
	f.mu.Lock()
	peak := f.peak
	f.mu.Unlock()
	if peak < 2 || peak > selfCheckParallel {
		t.Fatalf("peak concurrency = %d, want 2..%d", peak, selfCheckParallel)
	}
}

// 并发不改变判定：被拒绝的权限、缺失的内置角色、超时没答完的请求，都照常记为未通过——
// 不可达 / 不合格的集群不会因为并发被判成通过。
func TestSelfCheckFailuresStayFailures(t *testing.T) {
	t.Parallel()
	f := &selfCheckFixture{denyResource: "groups", missingRole: "edit"}
	u := fakeClusterUser(t, f)
	failed := map[string]bool{}
	for _, item := range u.c.selfCheck(context.Background(), "v1.35.0") {
		if !item.OK {
			if item.Message == "" {
				t.Fatalf("%s failed without a message", item.Name)
			}
			failed[item.Name] = true
		}
	}
	if len(failed) != 2 || !failed["impersonate groups"] || !failed["built-in clusterrole edit"] {
		t.Fatalf("failed items = %v, want exactly the denied permission and the missing role", failed)
	}

	slow := &selfCheckFixture{hold: time.Second}
	su := fakeClusterUser(t, slow)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	got := su.c.selfCheck(ctx, "v1.35.0")
	for _, item := range got[1:] {
		if item.OK {
			t.Fatalf("%s did not get an answer within the budget and must not pass", item.Name)
		}
	}
}

// 等待缓存同步的缺省上限必须明显短于 core 调数据面的 30 秒：否则用户拿到的是 core 的通用超时，
// 而不是数据面明确的「缓存建立中」。
func TestDefaultSyncTimeoutBelowPlaneTimeout(t *testing.T) {
	t.Parallel()
	const corePlaneTimeout = 30 * time.Second
	if got := (Options{}).withDefaults().SyncTimeout; got > corePlaneTimeout-5*time.Second {
		t.Fatalf("default SyncTimeout = %s, must leave clear headroom under core's %s", got, corePlaneTimeout)
	}
	if got := (Options{SyncTimeout: 7 * time.Second}).withDefaults().SyncTimeout; got != 7*time.Second {
		t.Fatalf("an explicit SyncTimeout must be kept: %s", got)
	}
}
