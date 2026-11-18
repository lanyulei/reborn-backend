package kube

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
)

// 测试用的仓库服务器都在回环地址上：打开开发开关（生产缺省拒绝，见 TestHelmRepoDialAndRedirect）。
func init() { helmAllowLoopback = true }

// reborn:ns-admin 的规则：没有 RBAC 的写权限，也没有 escalate / bind / impersonate。
func TestNsAdminRules(t *testing.T) {
	t.Parallel()
	admin := []rbacv1.PolicyRule{
		{APIGroups: []string{""}, Resources: []string{"pods", "secrets"}, Verbs: []string{"get", "list", "watch", "create", "delete"}},
		{APIGroups: []string{"rbac.authorization.k8s.io"}, Resources: []string{"roles", "rolebindings"}, Verbs: []string{"get", "list", "watch", "create", "update", "patch", "delete", "deletecollection"}},
		{APIGroups: []string{""}, Resources: []string{"serviceaccounts"}, Verbs: []string{"impersonate"}},
		{APIGroups: []string{"*"}, Resources: []string{"*"}, Verbs: []string{"*"}},
		{APIGroups: []string{"apps"}, Resources: []string{"deployments"}, Verbs: []string{"*"}},
		{APIGroups: []string{"authorization.k8s.io"}, Resources: []string{"localsubjectaccessreviews"}, Verbs: []string{"create"}},
	}
	got := nsAdminRules(admin)
	for _, r := range got {
		rbac := false
		for _, g := range r.APIGroups {
			if g == rbacv1.GroupName || g == "*" {
				rbac = true
			}
		}
		for _, v := range r.Verbs {
			switch v {
			case "escalate", "bind", "impersonate", "*":
				t.Errorf("rule %v keeps privileged verb %s", r, v)
			case "get", "list", "watch":
			default:
				if rbac {
					t.Errorf("rule %v can write RBAC objects (%s)", r, v)
				}
			}
		}
	}
	// 普通规则原样保留；只含 impersonate 的规则整条去掉。
	if len(got) != 5 || len(got[0].Verbs) != 5 {
		t.Fatalf("rules = %+v", got)
	}
	if strings.Join(got[3].Verbs, ",") != "get,list,watch,create,update,patch,delete,deletecollection" {
		t.Fatalf("wildcard verbs on a non-rbac group must expand to regular verbs: %v", got[3].Verbs)
	}
}

func TestPlatformOwnedAndSubjects(t *testing.T) {
	t.Parallel()
	if !platformOwned("reborn:1234:edit") || platformOwned("team-binding") {
		t.Fatal("ownership is decided by the reborn: name prefix")
	}
	if platformOwned(agentBindingName) {
		t.Fatal("the agent's own binding must never be reconciled away")
	}
	subj := func(kind, name string) rbacv1.Subject { return rbacv1.Subject{Kind: kind, Name: name} }
	if !anyPlatformSubject([]rbacv1.Subject{subj("User", "alice"), subj("User", "reborn:018f")}) ||
		!anyPlatformSubject([]rbacv1.Subject{subj("Group", "reborn:role:1")}) {
		t.Fatal("bindings naming platform users or groups must be detected")
	}
	if anyPlatformSubject([]rbacv1.Subject{subj("ServiceAccount", "reborn:x"), subj("User", "bob")}) {
		t.Fatal("service accounts and ordinary users are not platform subjects")
	}
}

// Secret / ConfigMap 的缓存只取元数据：平台凭据发出的请求必须带 PartialObjectMetadata 的 Accept 头。
func TestMetadataClientRequestsMetadataOnly(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var accepts []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		accepts = append(accepts, r.Header.Get("Accept"))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"kind":"PartialObjectMetadataList","apiVersion":"meta.k8s.io/v1","metadata":{"resourceVersion":"1"},"items":[{"metadata":{"name":"s1","namespace":"a","uid":"u1","labels":{"k":"v"}}}]}`)
	}))
	defer srv.Close()
	mc, err := metadata.NewForConfig(&rest.Config{Host: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	list, err := mc.Resource(schema.GroupVersionResource{Version: "v1", Resource: "secrets"}).List(context.Background(), metav1.ListOptions{})
	if err != nil || len(list.Items) != 1 {
		t.Fatalf("list: %v %v", list, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(accepts) == 0 || !strings.Contains(accepts[0], "as=PartialObjectMetadataList") {
		t.Fatalf("Accept = %q, want a PartialObjectMetadataList request", accepts)
	}
	// 元数据对象能转成缓存条目，且条目里没有内容字段。
	s := newEntryStore(k8scontract.GVRSecret)
	c := s.toCached(&list.Items[0])
	if c == nil || c.entry.Row.Name != "s1" || c.entry.Row.Namespace != "a" || c.entry.Labels["k"] != "v" {
		t.Fatalf("cached = %+v", c)
	}
	if _, ok := c.entry.Row.Cells["type"]; ok {
		t.Fatal("the secret type is not part of the metadata and must not be a column")
	}
	if !k8scontract.MetadataOnly(k8scontract.GVRSecret) || !k8scontract.MetadataOnly(k8scontract.GVRConfigMap) {
		t.Fatal("secrets and configmaps must be metadata-only")
	}
}

// 部分命名空间可读的订阅者：其余命名空间的变更不通知。
func TestSubscribeOnlyAllowedNamespaces(t *testing.T) {
	t.Parallel()
	s := newEntryStore(k8scontract.GVRConfigMap)
	obj := func(ns, name string) *unstructured.Unstructured {
		u := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap"}}
		u.SetNamespace(ns)
		u.SetName(name)
		return u
	}
	sub := s.subscribe("", []string{"a"})
	_ = s.Add(obj("b", "1"))
	_ = s.Add(obj("b", "2"))
	if n := sub.pending.Load(); n != 0 {
		t.Fatalf("changes in a namespace the subscriber cannot read were counted: %d", n)
	}
	_ = s.Add(obj("a", "1"))
	if n := sub.pending.Load(); n != 1 {
		t.Fatalf("pending = %d, want 1", n)
	}
}

// 权限询问缓存：clear 之前发出的询问不得在 clear 之后写回。
func TestAccessCacheGeneration(t *testing.T) {
	t.Parallel()
	a := newAccessCache()
	a.mu.Lock()
	gen := a.gen
	a.mu.Unlock()
	a.clear() // 授权被收回
	a.mu.Lock()
	if a.gen == gen {
		t.Fatal("clear must advance the generation")
	}
	if a.gen == gen { // 模拟 allowed() 里的写回条件
		a.m["k"] = accessEntry{allowed: true, expire: time.Now().Add(time.Minute)}
	}
	n := len(a.m)
	a.mu.Unlock()
	if n != 0 {
		t.Fatal("a stale answer was stored after clear")
	}
}

func TestListRejectsNamespaceTraversal(t *testing.T) {
	t.Parallel()
	for _, ns := range []string{"../nodes", "a/b", "kube-system/../../x", "A"} {
		if nsPattern.MatchString(ns) {
			t.Errorf("namespace %q must be rejected", ns)
		}
	}
	if !nsPattern.MatchString("kube-system") {
		t.Fatal("ordinary namespaces must pass")
	}
}

// 经隧道的非流式响应有字节上限；watch / 日志跟随 / 协议升级不设上限。
func TestCappedTransport(t *testing.T) {
	t.Parallel()
	big := strings.Repeat("x", 4096)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, big) }))
	defer srv.Close()
	rt := cappedTransport{next: http.DefaultTransport, limit: 1024}
	get := func(path string) (int, error) {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+path, nil)
		resp, err := rt.RoundTrip(req)
		if err != nil {
			return 0, err
		}
		defer func() { _ = resp.Body.Close() }()
		raw, err := io.ReadAll(resp.Body)
		return len(raw), err
	}
	if n, err := get("/api/v1/pods"); !errors.Is(err, errResponseTooLarge) || n > 1024 {
		t.Fatalf("oversized list response: read %d, err %v", n, err)
	}
	for _, p := range []string{"/api/v1/pods?watch=true", "/api/v1/namespaces/a/pods/p/log?follow=true"} {
		if n, err := get(p); err != nil || n != len(big) {
			t.Fatalf("%s: streaming responses must not be capped (read %d, err %v)", p, n, err)
		}
	}
}

// Helm 仓库的拨号与重定向：回环 / 链路本地被拒；https 不得降级；主机一变去掉认证头。
func TestHelmRepoDialAndRedirect(t *testing.T) {
	t.Parallel()
	for _, ip := range []string{"127.0.0.1", "::1", "169.254.169.254", "0.0.0.0", "fe80::1"} {
		if guardIP(net.ParseIP(ip), false) == nil {
			t.Errorf("%s must be refused", ip)
		}
	}
	if guardIP(net.ParseIP("10.1.2.3"), false) != nil {
		t.Fatal("private addresses are allowed by design")
	}
	if guardIP(net.ParseIP("127.0.0.1"), true) != nil {
		t.Fatal("loopback is allowed only with the explicit development switch")
	}
	mk := func(raw string) *http.Request {
		u, _ := url.Parse(raw)
		return &http.Request{URL: u, Header: http.Header{"Authorization": {"Basic x"}}}
	}
	first := mk("https://charts.example.com/index.yaml")
	if err := checkRepoRedirect(mk("http://charts.example.com/x"), []*http.Request{first}); err == nil {
		t.Fatal("https to http redirect must be refused")
	}
	other := mk("https://cdn.example.com/x")
	if err := checkRepoRedirect(other, []*http.Request{first}); err != nil || other.Header.Get("Authorization") != "" {
		t.Fatalf("credentials must be dropped when the host changes: err %v header %q", err, other.Header.Get("Authorization"))
	}
	sub := mk("https://sub.charts.example.com/x")
	_ = checkRepoRedirect(sub, []*http.Request{first})
	if sub.Header.Get("Authorization") != "" {
		t.Fatal("a subdomain is a different host: credentials must be dropped")
	}
	same := mk("https://charts.example.com/y")
	if err := checkRepoRedirect(same, []*http.Request{first}); err != nil || same.Header.Get("Authorization") == "" {
		t.Fatal("same-host redirects keep the credentials")
	}
}

// 清单里的 Secret 内容掩码，其余文档原样。
func TestMaskManifestSecrets(t *testing.T) {
	t.Parallel()
	manifest := `---
# Source: demo/templates/secret.yaml
apiVersion: v1
kind: Secret
metadata:
  name: db
data:
  password: c3VwZXItc2VjcmV0
stringData:
  token: plain-token
---
# Source: demo/templates/cm.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: cfg
data:
  mode: "Secret"   # 这里出现 Secret 字样，但不是 Secret 对象
`
	out := maskManifestSecrets(manifest)
	for _, leak := range []string{"c3VwZXItc2VjcmV0", "plain-token"} {
		if strings.Contains(out, leak) {
			t.Fatalf("secret value %q leaked:\n%s", leak, out)
		}
	}
	if strings.Count(out, k8scontract.MaskedValue) != 2 || !strings.Contains(out, "# Source: demo/templates/secret.yaml") {
		t.Fatalf("masked manifest:\n%s", out)
	}
	if !strings.Contains(out, `mode: "Secret"   # 这里出现 Secret 字样，但不是 Secret 对象`) {
		t.Fatalf("non-secret documents must be returned verbatim:\n%s", out)
	}
	if plain := "kind: Deployment\nmetadata: {name: x}\n"; maskManifestSecrets(plain) != plain {
		t.Fatal("manifests without secrets are returned verbatim")
	}
}

// Helm 在途操作：每集群并发上限、关停后拒绝新操作、集群移除时取消。
func TestHelmOps(t *testing.T) {
	t.Parallel()
	h := newHelmOps()
	var dones []func()
	for i := range maxHelmOpsPerCluster {
		_, done, err := h.begin("c1", time.Minute)
		if err != nil {
			t.Fatalf("op %d: %v", i, err)
		}
		dones = append(dones, done)
	}
	if _, _, err := h.begin("c1", time.Minute); err == nil {
		t.Fatal("the per-cluster limit must be enforced")
	}
	ctx2, done2, err := h.begin("c2", time.Minute)
	if err != nil {
		t.Fatal("other clusters are not affected by the limit")
	}
	h.cancelCluster("c2")
	if ctx2.Err() == nil {
		t.Fatal("dropping a cluster must cancel its operations")
	}
	done2()
	for _, d := range dones {
		d()
	}
	h.shutdown(time.Second)
	if _, _, err := h.begin("c1", time.Minute); err == nil {
		t.Fatal("no new operations after shutdown")
	}
}

func TestReservedNamespace(t *testing.T) {
	t.Parallel()
	for _, n := range []string{"kube-system", "kube-anything", "default", "reborn-system"} {
		if !reservedNamespace(n) {
			t.Errorf("%s must be reserved", n)
		}
	}
	if reservedNamespace("team-a") {
		t.Fatal("ordinary names are not reserved")
	}
	c := &conn{}
	if _, err := c.createNamespace(context.Background(), &k8scontract.NamespaceRequest{Name: "kube-system"}, "x"); biz.AsError(err).Code != 400 {
		t.Fatalf("reserved name: %v", err)
	}
	if _, err := c.createNamespace(context.Background(), &k8scontract.NamespaceRequest{Name: "team-a", Labels: map[string]string{"istio-injection": "disabled"}}, "x"); biz.AsError(err).Code != 400 {
		t.Fatalf("labels must be refused on the plane too: %v", err)
	}
}
