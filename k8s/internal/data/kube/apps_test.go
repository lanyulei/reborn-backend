package kube

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestParseIndex(t *testing.T) {
	t.Parallel()
	ix, err := parseIndex([]byte(`apiVersion: v1
entries:
  web:
    - {name: web, version: 1.2.0, urls: [web-1.2.0.tgz], created: "2026-01-02T03:04:05Z"}
    - {name: web, version: 1.10.0, urls: [web-1.10.0.tgz]}
    - {name: web, version: "bad version", urls: [x.tgz]}
    - {name: web, version: 1.9.0-rc.1, urls: [web-rc.tgz]}
  "../evil":
    - {name: x, version: 1.0.0, urls: [x.tgz]}
  empty: []
`))
	if err != nil {
		t.Fatal(err)
	}
	if len(ix.entries) != 1 {
		t.Fatalf("entries = %v", ix.entries)
	}
	var got []string
	for _, e := range ix.entries["web"] {
		got = append(got, e.Version)
	}
	if strings.Join(got, ",") != "1.10.0,1.9.0-rc.1,1.2.0" {
		t.Fatalf("versions = %v (semver order, invalid dropped)", got)
	}
	for _, bad := range []string{"entries: [1,2", "foo: bar", ""} {
		if _, err := parseIndex([]byte(bad)); err == nil {
			t.Errorf("parseIndex(%q) must fail", bad)
		}
	}
}

func TestChartURL(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"web-1.0.0.tgz":                   "https://charts.example.com/stable/web-1.0.0.tgz",
		"charts/web-1.0.0.tgz":            "https://charts.example.com/stable/charts/web-1.0.0.tgz",
		"/abs/web.tgz":                    "https://charts.example.com/abs/web.tgz",
		"https://cdn.example.net/web.tgz": "https://cdn.example.net/web.tgz",
	}
	for ref, want := range cases {
		got, err := chartURL("https://charts.example.com/stable", ref)
		if err != nil || got != want {
			t.Errorf("chartURL(%q) = %q, %v; want %q", ref, got, err, want)
		}
	}
	for _, ref := range []string{"file:///etc/passwd", "oci://x/y", "ftp://x/y"} {
		if got, err := chartURL("https://charts.example.com", ref); err == nil {
			t.Errorf("chartURL(%q) = %q, want error", ref, got)
		}
	}
}

func TestFetchLimitsAndCredentials(t *testing.T) {
	t.Parallel()
	var auth []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = append(auth, r.Header.Get("Authorization"))
		switch r.URL.Path {
		case "/big":
			_, _ = w.Write([]byte(strings.Repeat("x", 2000)))
		case "/missing":
			http.NotFound(w, r)
		default:
			_, _ = w.Write([]byte("ok"))
		}
	}))
	defer srv.Close()
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = append(auth, "other:"+r.Header.Get("Authorization"))
		_, _ = w.Write([]byte("ok"))
	}))
	defer other.Close()

	a := repoAccess{typ: k8scontract.HelmRepoHTTP, url: srv.URL, username: "u", password: "p"}
	// 测试服务器在回环地址：本包的测试打开了回环开关（见 review_test.go 的 init）；生产缺省拒绝回环。
	c, err := repoHTTPClient(a)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if data, err := fetch(ctx, c, a, srv.URL+"/ok", 1000); err != nil || string(data) != "ok" {
		t.Fatalf("fetch ok = %q, %v", data, err)
	}
	if auth[len(auth)-1] == "" {
		t.Fatal("credentials must be sent to the repository host")
	}
	if _, err := fetch(ctx, c, a, srv.URL+"/big", 1000); err == nil || !strings.Contains(err.Error(), "limit") {
		t.Fatalf("oversized body: err = %v", err)
	}
	if _, err := fetch(ctx, c, a, srv.URL+"/missing", 1000); err == nil {
		t.Fatal("404 must fail")
	}
	if _, err := fetch(ctx, c, a, other.URL+"/x", 1000); err != nil {
		t.Fatal(err)
	}
	if last := auth[len(auth)-1]; last != "other:" {
		t.Fatalf("credentials leaked to another host: %q", last)
	}
	if _, err := fetch(ctx, c, a, "file:///etc/passwd", 1000); err == nil {
		t.Fatal("non-http scheme must fail")
	}
}

func TestGuardDial(t *testing.T) {
	t.Parallel()
	for _, addr := range []string{"169.254.169.254:80", "[fe80::1]:443", "0.0.0.0:80", "224.0.0.1:80", "nothost:80"} {
		if guardDial("tcp", addr, nil) == nil {
			t.Errorf("guardDial(%s) must refuse", addr)
		}
	}
	for _, addr := range []string{"10.0.0.8:443", "127.0.0.1:8080", "93.184.216.34:443", "[2001:db8::1]:443"} {
		if err := guardDial("tcp", addr, nil); err != nil {
			t.Errorf("guardDial(%s) = %v", addr, err)
		}
	}
}

func TestHelmError(t *testing.T) {
	t.Parallel()
	cases := []struct {
		msg    string
		reason string
	}{
		{`secrets is forbidden: User "reborn:x" cannot list resource "secrets"`, k8scontract.ReasonForbidden},
		{"cannot re-use a name that is still in use", k8scontract.ReasonConflict},
		{"another operation in progress", k8scontract.ReasonConflict},
		{"template: web/templates/x.yaml:3: function \"nope\" not defined", k8scontract.ReasonRejected},
	}
	for _, c := range cases {
		be := biz.AsError(helmError(&plainErr{c.msg}))
		if be.Reason != c.reason {
			t.Errorf("helmError(%q).Reason = %s, want %s", c.msg, be.Reason, c.reason)
		}
	}
	if helmError(nil) != nil {
		t.Fatal("nil in, nil out")
	}
}

type plainErr struct{ s string }

func (e *plainErr) Error() string { return e.s }

func TestSummarizeQuotas(t *testing.T) {
	t.Parallel()
	q := func(ns, name string, hard, used map[corev1.ResourceName]string) corev1.ResourceQuota {
		conv := func(m map[corev1.ResourceName]string) corev1.ResourceList {
			out := corev1.ResourceList{}
			for k, v := range m {
				out[k] = resource.MustParse(v)
			}
			return out
		}
		return corev1.ResourceQuota{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
			Status:     corev1.ResourceQuotaStatus{Hard: conv(hard), Used: conv(used)},
		}
	}
	out := summarizeQuotas([]corev1.ResourceQuota{
		q("b", "default", map[corev1.ResourceName]string{"pods": "10", "requests.cpu": "2"}, map[corev1.ResourceName]string{"pods": "5", "requests.cpu": "500m"}),
		q("b", "tight", map[corev1.ResourceName]string{"pods": "6"}, map[corev1.ResourceName]string{"pods": "5"}),
		q("a", "default", map[corev1.ResourceName]string{"pods": "0"}, map[corev1.ResourceName]string{"pods": "0"}),
	})
	if len(out) != 2 || out[0].Namespace != "a" || out[1].Namespace != "b" {
		t.Fatalf("namespaces = %+v", out)
	}
	if out[0].Items[0].Ratio != 0 {
		t.Fatalf("zero hard must give ratio 0: %+v", out[0].Items)
	}
	b := out[1]
	if strings.Join(b.Quotas, ",") != "default,tight" || len(b.Items) != 2 {
		t.Fatalf("b = %+v", b)
	}
	if b.Items[0].Resource != "pods" || b.Items[0].Hard != "6" || b.Items[0].Ratio < 0.83 {
		t.Fatalf("pods must take the tightest quota: %+v", b.Items[0])
	}
	if b.Items[1].Resource != "requests.cpu" || b.Items[1].Used != "500m" || b.Items[1].Ratio != 0.25 {
		t.Fatalf("cpu = %+v", b.Items[1])
	}
}

// 用户身份的 Helm 客户端：身份由 identityRT 放进请求上下文，Impersonate-* 头仍只由 userRoundTripper 添加；
// 调用方自带的 Impersonate-* 头不得生效。
func TestIdentityRT(t *testing.T) {
	t.Parallel()
	id := biz.Identity{UserID: "018f3e1a-0cd1-7000-8000-000000000001", RoleIDs: []string{"018f3e1a-0cd1-7000-8000-000000000002"}}
	var got http.Header
	next := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		got = r.Header
		return &http.Response{StatusCode: 200, Body: http.NoBody, Header: http.Header{}}, nil
	})
	rt := identityRT{id: id, next: userRoundTripper{next: next}}
	req, _ := http.NewRequest(http.MethodGet, "http://cluster.invalid/api", nil)
	req.Header.Set("Impersonate-User", "system:admin")
	req.Header.Add("Impersonate-Group", "system:masters")
	if _, err := rt.RoundTrip(req); err != nil {
		t.Fatal(err)
	}
	if got.Get("Impersonate-User") != k8scontract.ImpersonateUser(id.UserID) {
		t.Fatalf("Impersonate-User = %q", got.Get("Impersonate-User"))
	}
	groups := strings.Join(got.Values("Impersonate-Group"), ",")
	if strings.Contains(groups, "system:masters") || !strings.Contains(groups, k8scontract.RoleGroup(id.RoleIDs[0])) {
		t.Fatalf("Impersonate-Group = %q", groups)
	}
	bad := identityRT{id: biz.Identity{UserID: "not-a-uuid"}, next: userRoundTripper{next: next}}
	if _, err := bad.RoundTrip(req); err == nil {
		t.Fatal("invalid identity must be refused")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// Helm 写操作的审计恰好一条：按时完成随响应返回（由调用方落库），超时则返回 Pending、结束时经 sink 补记。
func TestHelmAsyncAuditExactlyOnce(t *testing.T) {
	t.Parallel()
	id := biz.Identity{UserID: "018f3e1a-0cd1-7000-8000-000000000001"}
	newUser := func(sink biz.AuditSink) *userClients {
		m := &Manager{apps: &appsState{sink: sink}, helm: newHelmOps()}
		c := &conn{m: m, cluster: &biz.Cluster{ID: "c1", Name: "c1"}}
		c.ctx, c.cancel = context.WithCancel(context.Background())
		t.Cleanup(c.cancel)
		return &userClients{ctx: context.Background(), id: id, c: c}
	}
	sunk := make(chan []*biz.AuditEvent, 4)
	sink := func(_ context.Context, _ *biz.Cluster, _ biz.Identity, ev []*biz.AuditEvent) { sunk <- ev }

	// 按时完成
	audit := helmAudit(k8scontract.ActionHelmInstall, "ns", "web", map[string]any{})
	resp, audits, err := newUser(sink).helmAsyncWait(audit, time.Second, func(context.Context, *userClients) helmResult {
		return helmResult{resp: k8scontract.HelmApplyResponse{Revision: 7, Status: "deployed"}}
	})
	if err != nil || len(audits) != 1 || audits[0].Err != nil || resp.(k8scontract.HelmApplyResponse).Revision != 7 {
		t.Fatalf("fast path: %+v %v %v", resp, audits, err)
	}
	if audits[0].Patch.(map[string]any)["revision"] != 7 {
		t.Fatalf("revision must be recorded in the audit: %+v", audits[0].Patch)
	}
	select {
	case ev := <-sunk:
		t.Fatalf("fast path must not also go through the sink: %+v", ev)
	case <-time.After(100 * time.Millisecond):
	}

	// 按时失败：错误与审计一起返回
	wantErr := biz.ErrForbidden("nope")
	_, audits, err = newUser(sink).helmAsyncWait(helmAudit(k8scontract.ActionHelmInstall, "ns", "web", nil), time.Second,
		func(context.Context, *userClients) helmResult { return helmResult{err: wantErr} })
	if err == nil || len(audits) != 1 || audits[0].Err == nil {
		t.Fatalf("failed path: %v %v", audits, err)
	}

	// 超时：返回 Pending 且不带审计；操作结束后经 sink 补记一条
	release := make(chan struct{})
	resp, audits, err = newUser(sink).helmAsyncWait(helmAudit(k8scontract.ActionHelmUpgrade, "ns", "slow", nil), 50*time.Millisecond,
		func(context.Context, *userClients) helmResult {
			<-release
			return helmResult{err: biz.ErrInvalid("late failure")}
		})
	if err != nil || audits != nil || !resp.(k8scontract.HelmApplyResponse).Pending {
		t.Fatalf("pending path: %+v %v %v", resp, audits, err)
	}
	close(release)
	select {
	case ev := <-sunk:
		if len(ev) != 1 || ev[0].Action != k8scontract.ActionHelmUpgrade || ev[0].Err == nil {
			t.Fatalf("late audit: %+v", ev[0])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("late completion must be audited through the sink")
	}

	// 请求中断（客户端断开）同样不取消操作
	rctx, rcancel := context.WithCancel(context.Background())
	u := newUser(sink)
	u.ctx = rctx
	rcancel()
	started := make(chan struct{})
	resp, _, _ = u.helmAsyncWait(helmAudit(k8scontract.ActionHelmUninstall, "ns", "x", nil), time.Second,
		func(ctx context.Context, _ *userClients) helmResult {
			close(started)
			time.Sleep(50 * time.Millisecond)
			return helmResult{err: ctx.Err()}
		})
	<-started
	if ev := <-sunk; len(ev) != 1 || ev[0].Err != nil {
		t.Fatalf("operation must outlive the request: %+v (resp %+v)", ev[0], resp)
	}
}
