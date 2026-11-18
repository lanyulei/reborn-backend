package kube

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	uid1  = "018f3e1a-0000-7000-8000-000000000001"
	role1 = "018f3e1a-0000-7000-8000-0000000000a1"
	role2 = "018f3e1a-0000-7000-8000-0000000000a2"
)

// recorder 记录到达「集群」的每个请求头。
type recorder struct {
	mu   sync.Mutex
	reqs []http.Header
}

func (r *recorder) RoundTrip(req *http.Request) (*http.Response, error) {
	r.mu.Lock()
	r.reqs = append(r.reqs, req.Header.Clone())
	r.mu.Unlock()
	return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("{}")), Header: http.Header{}, Request: req}, nil
}

func newReq(ctx context.Context) *http.Request {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://cluster.invalid/api/v1/pods", nil)
	return req
}

func TestUserRoundTripperImpersonation(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	rt := userRoundTripper{next: bearerRoundTripper{token: "platform-token", next: rec}}

	// 1. 没有身份的上下文：请求不会发出。
	if _, err := rt.RoundTrip(newReq(context.Background())); !errors.Is(err, errNoIdentity) {
		t.Fatalf("request without identity must be refused, got %v", err)
	}
	// 2. 身份字段不合法（会进请求头）：同样拒绝。
	for _, bad := range []biz.Identity{
		{UserID: ""},
		{UserID: "admin"},
		{UserID: uid1 + "\r\nImpersonate-Group: system:masters"},
		{UserID: uid1, RoleIDs: []string{"system:masters"}},
	} {
		ctx := context.WithValue(context.Background(), identityKey{}, bad)
		if _, err := rt.RoundTrip(newReq(ctx)); !errors.Is(err, errNoIdentity) {
			t.Fatalf("identity %+v must be refused, got %v", bad, err)
		}
	}
	if len(rec.reqs) != 0 {
		t.Fatalf("%d refused requests reached the cluster", len(rec.reqs))
	}

	// 3. 正常身份：带上且只带上由身份推出的头；调用方塞进来的 Impersonate-* 与 Authorization 被丢弃。
	id := biz.Identity{UserID: uid1, RoleIDs: []string{role1, role2}}
	req := newReq(context.WithValue(context.Background(), identityKey{}, id))
	req.Header.Set("Impersonate-User", "system:admin")
	req.Header.Add("Impersonate-Group", "system:masters")
	req.Header.Set("impersonate-extra-scopes", "x")
	req.Header.Set("Impersonate-Uid", "0")
	req.Header.Set("Authorization", "Bearer attacker")
	if _, err := rt.RoundTrip(req); err != nil {
		t.Fatal(err)
	}
	got := rec.reqs[0]
	if got.Get("Impersonate-User") != "reborn:"+uid1 {
		t.Errorf("Impersonate-User = %q", got.Get("Impersonate-User"))
	}
	want := []string{"reborn:role:" + role1, "reborn:role:" + role2, "system:authenticated"}
	if strings.Join(got.Values("Impersonate-Group"), ",") != strings.Join(want, ",") {
		t.Errorf("Impersonate-Group = %v, want %v", got.Values("Impersonate-Group"), want)
	}
	for k := range got {
		if strings.HasPrefix(k, "Impersonate-") && k != "Impersonate-User" && k != "Impersonate-Group" {
			t.Errorf("smuggled header survived: %s", k)
		}
	}
	if got.Get("Authorization") != "Bearer platform-token" {
		t.Errorf("Authorization = %q", got.Get("Authorization"))
	}
	if req.Header.Get("Impersonate-User") != "system:admin" {
		t.Error("the caller's request must not be mutated")
	}

	// 4. 超管标记只来自身份。
	admin := biz.Identity{UserID: uid1, IsAdmin: true}
	if _, err := rt.RoundTrip(newReq(context.WithValue(context.Background(), identityKey{}, admin))); err != nil {
		t.Fatal(err)
	}
	if g := rec.reqs[1].Values("Impersonate-Group"); strings.Join(g, ",") != "reborn:admin,system:authenticated" {
		t.Errorf("admin groups = %v", g)
	}
}

func TestPlatformRoundTripperStripsImpersonation(t *testing.T) {
	t.Parallel()
	rec := &recorder{}
	rt := platformRoundTripper{next: bearerRoundTripper{token: "t", next: rec}}
	// 即使上下文里带着用户身份、请求上带着 Impersonate 头，平台身份的请求也不带任何 Impersonate-*。
	req := newReq(context.WithValue(context.Background(), identityKey{}, biz.Identity{UserID: uid1}))
	req.Header.Set("Impersonate-User", "x")
	if _, err := rt.RoundTrip(req); err != nil {
		t.Fatal(err)
	}
	for k := range rec.reqs[0] {
		if strings.HasPrefix(k, "Impersonate-") {
			t.Fatalf("platform request carries %s", k)
		}
	}
}

// 端到端地经 client-go 走一遍：用户身份的客户端发出的每个请求都带 Impersonate 头；
// 平台身份的客户端一个都不带。同时覆盖 Agent 集群的拨号接缝。
func TestClientsCarryIdentity(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	seen := map[string][]http.Header{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen[r.URL.Path] = append(seen[r.URL.Path], r.Header.Clone())
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"PodList","apiVersion":"v1","metadata":{},"items":[]}`))
	}))
	defer srv.Close()
	addr := srv.Listener.Addr().String()
	m := NewManager(nil, nil, nil, dialerFunc(func(ctx context.Context, id string) (net.Conn, error) {
		if id != "agent-cluster" {
			return nil, ErrAgentOffline
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	}), Options{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer m.Close()
	if err := m.Connect(context.Background(), &biz.Cluster{ID: "agent-cluster", Name: "a", AccessMode: k8scontract.AccessAgent}); err != nil {
		t.Fatal(err)
	}
	c, err := m.conn("agent-cluster")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.asUser(context.Background(), biz.Identity{UserID: "not-a-uuid"}); err == nil {
		t.Fatal("asUser must reject a malformed identity")
	}
	u, err := c.asUser(context.Background(), biz.Identity{UserID: uid1, RoleIDs: []string{role1}})
	if err != nil {
		t.Fatal(err)
	}
	pods := schema.GroupVersionResource{Version: "v1", Resource: "pods"}
	if _, err := u.dyn.Resource(pods).Namespace("user-ns").List(u.ctx, metav1.ListOptions{}); err != nil {
		t.Fatal(err)
	}
	// 用户客户端 + 丢了身份的上下文：必须失败，而不是悄悄以平台身份发出。
	if _, err := u.dyn.Resource(pods).Namespace("leak-ns").List(context.Background(), metav1.ListOptions{}); err == nil {
		t.Fatal("user client with a bare context must fail")
	}
	if _, err := c.platDyn.Resource(pods).Namespace("plat-ns").List(u.ctx, metav1.ListOptions{}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if h := seen["/api/v1/namespaces/user-ns/pods"]; len(h) != 1 || h[0].Get("Impersonate-User") != "reborn:"+uid1 {
		t.Fatalf("user request headers: %v", h)
	}
	if len(seen["/api/v1/namespaces/leak-ns/pods"]) != 0 {
		t.Fatal("identity-less user request reached the cluster")
	}
	if h := seen["/api/v1/namespaces/plat-ns/pods"]; len(h) != 1 || h[0].Get("Impersonate-User") != "" {
		t.Fatalf("platform request headers: %v", h)
	}
	// 没有隧道的 Agent 集群：不可达。
	_ = m.Connect(context.Background(), &biz.Cluster{ID: "offline", Name: "o", AccessMode: k8scontract.AccessAgent})
	if _, err := m.Probe(context.Background(), "offline"); err == nil || !strings.Contains(err.Error(), "agent is not connected") {
		t.Fatalf("offline agent cluster: %v", err)
	}
}

type dialerFunc func(ctx context.Context, clusterID string) (net.Conn, error)

func (f dialerFunc) DialCluster(ctx context.Context, id string) (net.Conn, error) { return f(ctx, id) }

func TestDirectTransportsValidation(t *testing.T) {
	t.Parallel()
	tok := k8scontract.Credential{Token: "t"}
	bad := []directSpec{
		{apiServer: "http://10.0.0.1:6443", cred: tok},          // 明文
		{apiServer: "https://user:pw@10.0.0.1:6443", cred: tok}, // 地址内嵌凭据
		{apiServer: "10.0.0.1:6443", cred: tok},
		{apiServer: "https://10.0.0.1:6443"}, // 没有凭据
		{apiServer: "https://10.0.0.1:6443", cred: tok, caCert: "not pem"},
		{apiServer: "https://10.0.0.1:6443", cred: tok, proxyURL: "socks5://x"},
		{apiServer: "https://10.0.0.1:6443", cred: k8scontract.Credential{ClientCert: "x", ClientKey: "y"}},
	}
	for _, s := range bad {
		if _, _, _, _, err := directTransports(s); err == nil {
			t.Errorf("spec %+v must be rejected", s)
		}
	}
	host, h2, h1, token, err := directTransports(directSpec{apiServer: " https://10.0.0.1:6443/ ", cred: tok, proxyURL: "http://proxy:3128"})
	if err != nil || host != "https://10.0.0.1:6443" || token != "t" || h2.Proxy == nil || h1.TLSClientConfig.NextProtos[0] != "http/1.1" {
		t.Fatalf("valid spec: %q %v", host, err)
	}
}

func TestMapError(t *testing.T) {
	t.Parallel()
	gr := schema.GroupResource{Resource: "pods"}
	cases := []struct {
		err    error
		code   int
		reason string
	}{
		{apierrors.NewBadRequest("bad"), 400, k8scontract.ReasonInvalidArgument},
		{apierrors.NewForbidden(gr, "p", errors.New("no")), 403, k8scontract.ReasonForbidden},
		{apierrors.NewNotFound(gr, "p"), 404, k8scontract.ReasonNotFound},
		{apierrors.NewConflict(gr, "p", errors.New("changed")), 409, k8scontract.ReasonConflict},
		{apierrors.NewAlreadyExists(gr, "p"), 409, k8scontract.ReasonConflict},
		{apierrors.NewInvalid(schema.GroupKind{Kind: "Pod"}, "p", nil), 422, k8scontract.ReasonRejected},
		{apierrors.NewTooManyRequests("pdb", 1), 409, k8scontract.ReasonConflict},
		{apierrors.NewUnauthorized("bad token"), 502, k8scontract.ReasonUpstream},
		{apierrors.NewInternalError(errors.New("boom")), 502, k8scontract.ReasonUpstream},
		{ErrAgentOffline, 503, k8scontract.ReasonClusterOffline},
		{&net.OpError{Op: "dial", Err: errors.New("connection refused")}, 503, k8scontract.ReasonClusterOffline},
		{context.DeadlineExceeded, 502, k8scontract.ReasonUpstream},
		{errNoIdentity, 500, k8scontract.ReasonUnspecified},
		{biz.ErrCacheSyncing("x"), 503, k8scontract.ReasonCacheSyncing},
	}
	for _, tc := range cases {
		be := biz.AsError(mapError(tc.err))
		if be.Code != tc.code || be.Reason != tc.reason {
			t.Errorf("%v → %d %s, want %d %s", tc.err, be.Code, be.Reason, tc.code, tc.reason)
		}
	}
	msg := biz.AsError(mapError(apierrors.NewForbidden(gr, "p", errors.New("User \"reborn:x\" cannot get")))).Message
	if !strings.Contains(msg, "cannot get") {
		t.Errorf("the cluster's message must be kept: %q", msg)
	}
	if mapError(nil) != nil {
		t.Error("nil stays nil")
	}
}

func pod(js string) map[string]any {
	var m map[string]any
	if err := json.Unmarshal([]byte(js), &m); err != nil {
		panic(err)
	}
	return m
}

func TestPodStatus(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, js, state, text string
		ready, total          int
		restarts              int64
	}{
		{"running and ready", `{"spec":{"containers":[{}]},"status":{"phase":"Running","containerStatuses":[{"ready":true,"restartCount":2,"state":{"running":{}}}]}}`, sOK, "Running", 1, 1, 2},
		{"running but not ready", `{"spec":{"containers":[{},{}]},"status":{"phase":"Running","containerStatuses":[{"ready":true,"state":{"running":{}}},{"ready":false,"state":{"running":{}}}]}}`, sAbnormal, "NotReady", 1, 2, 0},
		{"crash loop", `{"spec":{"containers":[{}]},"status":{"phase":"Running","containerStatuses":[{"ready":false,"restartCount":7,"state":{"waiting":{"reason":"CrashLoopBackOff","message":"back-off 5m"}}}]}}`, sAbnormal, "CrashLoopBackOff", 0, 1, 7},
		{"image pull", `{"spec":{"containers":[{}]},"status":{"phase":"Pending","containerStatuses":[{"ready":false,"state":{"waiting":{"reason":"ImagePullBackOff"}}}]}}`, sAbnormal, "ImagePullBackOff", 0, 1, 0},
		{"oom killed", `{"spec":{"containers":[{}]},"status":{"phase":"Running","containerStatuses":[{"ready":false,"restartCount":1,"state":{"terminated":{"reason":"OOMKilled","exitCode":137}}}]}}`, sAbnormal, "OOMKilled", 0, 1, 1},
		{"exit code without reason", `{"spec":{"containers":[{}]},"status":{"phase":"Failed","containerStatuses":[{"state":{"terminated":{"exitCode":2}}}]}}`, sAbnormal, "ExitCode:2", 0, 1, 0},
		{"creating", `{"spec":{"containers":[{}]},"status":{"phase":"Pending","containerStatuses":[{"state":{"waiting":{"reason":"ContainerCreating"}}}]}}`, sPending, "ContainerCreating", 0, 1, 0},
		{"unschedulable", `{"spec":{"containers":[{}]},"status":{"phase":"Pending","conditions":[{"type":"PodScheduled","status":"False","reason":"Unschedulable","message":"0/3 nodes are available"}]}}`, sAbnormal, "Unschedulable", 0, 1, 0},
		{"init in progress", `{"spec":{"initContainers":[{"name":"a"},{"name":"b"}],"containers":[{}]},"status":{"phase":"Pending","initContainerStatuses":[{"name":"a","state":{"terminated":{"exitCode":0}}},{"name":"b","state":{"running":{}}}]}}`, sPending, "Init:1/2", 0, 1, 0},
		{"init failed", `{"spec":{"initContainers":[{"name":"a"}],"containers":[{}]},"status":{"phase":"Pending","initContainerStatuses":[{"name":"a","restartCount":3,"state":{"waiting":{"reason":"CrashLoopBackOff"}}}]}}`, sAbnormal, "Init:CrashLoopBackOff", 0, 1, 3},
		{"sidecar started does not block", `{"spec":{"initContainers":[{"name":"s","restartPolicy":"Always"}],"containers":[{}]},"status":{"phase":"Running","initContainerStatuses":[{"name":"s","started":true,"state":{"running":{}}}],"containerStatuses":[{"ready":true,"state":{"running":{}}}]}}`, sOK, "Running", 1, 1, 0},
		{"completed", `{"spec":{"containers":[{}]},"status":{"phase":"Succeeded","containerStatuses":[{"state":{"terminated":{"reason":"Completed","exitCode":0}}}]}}`, sDone, "Completed", 0, 1, 0},
		{"terminating", `{"metadata":{"deletionTimestamp":"2026-01-01T00:00:00Z"},"spec":{"containers":[{}]},"status":{"phase":"Running","containerStatuses":[{"ready":true,"state":{"running":{}}}]}}`, sPending, "Terminating", 1, 1, 0},
		{"evicted", `{"spec":{"containers":[{}]},"status":{"phase":"Failed","reason":"Evicted","message":"node was low on memory"}}`, sAbnormal, "Evicted", 0, 1, 0},
		{"node lost", `{"metadata":{"deletionTimestamp":"2026-01-01T00:00:00Z"},"spec":{"containers":[{}]},"status":{"phase":"Running","reason":"NodeLost"}}`, sUnknown, "Unknown", 0, 1, 0},
	}
	for _, tc := range cases {
		st, text, _, ready, total, restarts := PodStatus(pod(tc.js))
		if st != tc.state || text != tc.text || ready != tc.ready || total != tc.total || restarts != tc.restarts {
			t.Errorf("%s: got (%s, %s, %d/%d, %d restarts), want (%s, %s, %d/%d, %d)", tc.name, st, text, ready, total, restarts, tc.state, tc.text, tc.ready, tc.total, tc.restarts)
		}
	}
	if _, _, reason, _, _, _ := PodStatus(pod(cases[2].js)); reason != "back-off 5m" {
		t.Errorf("abnormal reason = %q", reason)
	}
}

func TestConverterDropsContent(t *testing.T) {
	t.Parallel()
	secret := &unstructured.Unstructured{Object: pod(`{"apiVersion":"v1","kind":"Secret","type":"Opaque",
		"metadata":{"name":"s","namespace":"ns","uid":"u","labels":{"a":"b"},"creationTimestamp":"2026-01-01T00:00:00Z",
		  "managedFields":[{"manager":"kubectl"}],"annotations":{"kubectl.kubernetes.io/last-applied-configuration":"{\"data\":{\"p\":\"c2VjcmV0\"}}"},
		  "ownerReferences":[{"uid":"owner-1"}]},
		"data":{"p":"c2VjcmV0"}}`)}
	e := converterFor(k8scontract.GVRSecret)(secret)
	raw, _ := json.Marshal(e)
	if strings.Contains(string(raw), "c2VjcmV0") || strings.Contains(string(raw), "managedFields") || strings.Contains(string(raw), "kubectl") {
		t.Fatalf("cache entry keeps content it must not: %s", raw)
	}
	if e.Row.Name != "s" || e.Row.Namespace != "ns" || len(e.Owners) != 1 || e.Owners[0] != "owner-1" || e.Row.CreateTime.IsZero() {
		t.Fatalf("entry: %+v", e)
	}
	dep := &unstructured.Unstructured{Object: pod(`{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"d","generation":2},
		"spec":{"replicas":3,"template":{"spec":{"containers":[{"image":"nginx:1"}]}}},"status":{"observedGeneration":2,"readyReplicas":1,"updatedReplicas":3}}`)}
	d := converterFor(k8scontract.GVRDeployment)(dep)
	if d.Row.State != sAbnormal || d.Row.Cells["ready"] != "1/3" || d.Row.Cells["images"].([]string)[0] != "nginx:1" {
		t.Fatalf("deployment row: %+v", d.Row)
	}
	for gvr := range views {
		if !k8scontract.Cached(gvr) {
			t.Errorf("view for uncached resource %v", gvr)
		}
	}
	for _, gvr := range k8scontract.CachedResources() {
		if _, ok := views[gvr]; !ok {
			t.Errorf("cached resource %v has no view", gvr)
		}
	}
}

func TestEntryStore(t *testing.T) {
	t.Parallel()
	s := newEntryStore(k8scontract.GVRConfigMap)
	obj := func(ns, name string) *unstructured.Unstructured {
		u := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap"}}
		u.SetNamespace(ns)
		u.SetName(name)
		return u
	}
	subA, subAll := s.subscribe("a", nil), s.subscribe("", nil)
	_ = s.Replace([]any{obj("a", "1"), obj("b", "1")}, "10")
	select {
	case <-s.ready:
	default:
		t.Fatal("store must be ready after the first Replace")
	}
	_ = s.Add(obj("a", "2"))
	_ = s.Update(obj("a", "2"))
	_ = s.Delete(obj("b", "1"))
	_ = s.Delete(obj("b", "missing"))
	if s.size() != 2 || len(s.snapshot(nil)) != 2 || len(s.snapshot([]string{"a"})) != 2 || len(s.snapshot([]string{"b"})) != 0 {
		t.Fatalf("size=%d", s.size())
	}
	// a 的订阅者：全量替换 1 次 + a 的两次变更；b 的变更不通知它。
	if n := subA.pending.Load(); n != 3 {
		t.Errorf("namespace subscriber notified %d times, want 3", n)
	}
	if n := subAll.pending.Load(); n != 5 {
		t.Errorf("all-namespaces subscriber notified %d times, want 5", n)
	}
	s.unsubscribe(subA)
	_ = s.Add(obj("a", "3"))
	if subA.pending.Load() != 3 {
		t.Error("unsubscribed subscriber still notified")
	}
}

func TestReadLogLineAndPump(t *testing.T) {
	t.Parallel()
	long := strings.Repeat("x", k8scontract.MaxLogLineBytes+500)
	input := "2026-01-01T00:00:00.000000001Z first\n" +
		"2026-01-01T00:00:00.000000001Z second same stamp\r\n" +
		"2026-01-01T00:00:01Z " + long + "\n" +
		"no timestamp line\n" +
		"2026-01-01T00:00:02Z bad \xff utf8\n" +
		"2026-01-01T00:00:03Z last without newline"
	type got struct {
		id string
		l  k8scontract.LogLine
	}
	run := func(cursor, keyword string) []got {
		var out []got
		err := pumpLog(context.Background(), strings.NewReader(input), biz.NewLogTracker(cursor), keyword, "p", "c",
			func(event, id string, data any) error {
				out = append(out, got{id, data.(k8scontract.LogLine)})
				return nil
			})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	all := run("", "")
	if len(all) != 6 {
		t.Fatalf("lines = %d", len(all))
	}
	if all[0].l.Text != "first" || all[1].l.Text != "second same stamp" || all[0].id == all[1].id {
		t.Errorf("first two lines: %+v", all[:2])
	}
	if !all[2].l.Truncated || len(all[2].l.Text) != k8scontract.MaxLogLineBytes {
		t.Errorf("long line: truncated=%v len=%d", all[2].l.Truncated, len(all[2].l.Text))
	}
	if all[3].l.Text != "no timestamp line" || !all[3].l.Time.Equal(all[2].l.Time) || all[3].l.Truncated {
		t.Errorf("line without timestamp: %+v", all[3].l)
	}
	if !strings.Contains(all[4].l.Text, "bad ") || strings.Contains(all[4].l.Text, "\xff") {
		t.Errorf("invalid utf8 must be replaced: %q", all[4].l.Text)
	}
	if all[5].l.Text != "last without newline" || all[5].l.Pod != "p" || all[5].l.Container != "c" {
		t.Errorf("last line: %+v", all[5].l)
	}
	// 从第二行（与第一行同时间戳）续接：只收到其后的行，且游标与首次一致。
	resumed := run(all[1].id, "")
	if len(resumed) != 4 || resumed[0].id != all[2].id {
		t.Fatalf("resume from line 2: %d lines, first id %s", len(resumed), resumed[0].id)
	}
	// 关键字过滤不改变游标（序号按原始行计）。
	filtered := run("", "last")
	if len(filtered) != 1 || filtered[0].id != all[5].id {
		t.Fatalf("keyword: %+v", filtered)
	}
	// 对端断开：停止。
	err := pumpLog(context.Background(), strings.NewReader(input), biz.NewLogTracker(""), "", "", "",
		func(string, string, any) error { return io.ErrClosedPipe })
	if !errors.Is(err, errEmit) {
		t.Fatalf("emit failure: %v", err)
	}
}

func TestRestoreMaskedSecret(t *testing.T) {
	t.Parallel()
	cur := pod(`{"apiVersion":"v1","kind":"Secret","data":{"keep":"a2VlcA==","change":"b2xk","drop":"ZHJvcA=="}}`)
	obj := pod(`{"apiVersion":"v1","kind":"Secret","data":{"keep":"******","change":"bmV3","added":"******"},"stringData":{"x":"******","y":"plain"}}`)
	restoreMaskedSecret(obj, cur)
	data := obj["data"].(map[string]any)
	if data["keep"] != "a2VlcA==" || data["change"] != "bmV3" {
		t.Errorf("data: %v", data)
	}
	if _, ok := data["added"]; ok {
		t.Error("a masked value for a new key must be dropped, not stored as the mask text")
	}
	if _, ok := data["drop"]; ok {
		t.Error("a key removed by the user stays removed")
	}
	sd := obj["stringData"].(map[string]any)
	if _, ok := sd["x"]; ok || sd["y"] != "plain" {
		t.Errorf("stringData: %v", sd)
	}
	cm := pod(`{"apiVersion":"v1","kind":"ConfigMap","data":{"k":"******"}}`)
	restoreMaskedSecret(cm, cur)
	if cm["data"].(map[string]any)["k"] != "******" {
		t.Error("non-secrets are left alone")
	}
}

func TestSplitManifestAndVersion(t *testing.T) {
	t.Parallel()
	docs, err := splitManifest("---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: a}\n---\n\n---\n{\"apiVersion\":\"v1\",\"kind\":\"Secret\",\"metadata\":{\"name\":\"b\"}}\n")
	if err != nil || len(docs) != 2 || docs[1].GetKind() != "Secret" {
		t.Fatalf("split: %d %v", len(docs), err)
	}
	for _, bad := range []string{"", "---\n", "kind: ConfigMap\n", "apiVersion: v1\nkind: List\nitems: []\n", "a: [unclosed\n"} {
		if _, err := splitManifest(bad); err == nil {
			t.Errorf("manifest %q must be rejected", bad)
		}
	}
	many := strings.Repeat("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: a}\n---\n", k8scontract.MaxManifestDocs+1)
	if _, err := splitManifest(many); err == nil {
		t.Error("too many documents must be rejected")
	}
	for v, want := range map[string]int{"v1.35.1": 35, "v1.32.4-eks-abc": 32, "v1.33+k3s1": 33, "1.31.0": 31} {
		if major, minor, ok := serverMinor(v); !ok || major != 1 || minor != want {
			t.Errorf("serverMinor(%s) = %d.%d %v", v, major, minor, ok)
		}
	}
	if _, _, ok := serverMinor("garbage"); ok {
		t.Error("garbage version must not parse")
	}
	_ = time.Second
}
