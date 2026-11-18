package kube

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestKubeHandlerIdentity 集群代理转给集群的请求：身份头只由平台按 id 设置，入站的凭据头、
// Impersonate-* 与平台内部头一律不出现；目标主机恒为集群地址。
func TestKubeHandlerIdentity(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var got *http.Request
	cluster := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		got = r.Clone(context.Background())
		mu.Unlock()
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer cluster.Close()

	m := NewManager(nil, nil, nil, OfflineDialer{}, Options{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	cl := &biz.Cluster{ID: "c1", Name: "c1"}
	c := &conn{m: m, cluster: cl, host: cluster.URL}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	defer c.cancel()
	base := http.DefaultTransport
	c.userRT = userRoundTripper{next: bearerRoundTripper{token: "platform-token", next: base}}
	c.userH1 = c.userRT
	m.conns[cl.ID] = c

	id := biz.Identity{UserID: uid1, RoleIDs: []string{role1}}
	h, err := m.KubeHandler(cl, id)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "http://evil.example/api/v1/namespaces/a/pods?limit=5", nil)
	req.Host = "evil.example"
	req.Header.Set("Authorization", "Bearer user-supplied")
	req.Header.Set("Impersonate-User", "system:admin")
	req.Header.Add("Impersonate-Group", "system:masters")
	req.Header.Set("Impersonate-Extra-Scopes", "x")
	req.Header.Set("Impersonate-Uid", "0")
	req.Header.Set("Cookie", "session=1")
	req.Header.Set(k8scontract.IdentityHeader, "forged")
	req.Header.Set(k8scontract.SignatureHeader, "forged")
	req.Header.Set("X-Remote-User", "root")
	req.Header.Set("X-Forwarded-For", "1.2.3.4")
	req.Header.Set("Accept", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "ok") {
		t.Fatalf("response = %d %s", rec.Code, rec.Body.String())
	}
	mu.Lock()
	defer mu.Unlock()
	if got == nil {
		t.Fatal("request did not reach the cluster")
	}
	if got.URL.Path != "/api/v1/namespaces/a/pods" || got.URL.RawQuery != "limit=5" || got.Host != strings.TrimPrefix(cluster.URL, "http://") {
		t.Fatalf("target = %s %s host=%s", got.URL.Path, got.URL.RawQuery, got.Host)
	}
	if v := got.Header.Get("Authorization"); v != "Bearer platform-token" {
		t.Fatalf("Authorization = %q", v)
	}
	if v := got.Header.Get("Impersonate-User"); v != k8scontract.ImpersonateUser(uid1) {
		t.Fatalf("Impersonate-User = %q", v)
	}
	groups := got.Header.Values("Impersonate-Group")
	want := k8scontract.ImpersonateGroups([]string{role1}, false)
	if strings.Join(groups, ",") != strings.Join(want, ",") {
		t.Fatalf("Impersonate-Group = %v, want %v", groups, want)
	}
	for _, name := range []string{"Impersonate-Extra-Scopes", "Impersonate-Uid", "Cookie", k8scontract.IdentityHeader, k8scontract.SignatureHeader, "X-Remote-User", "X-Forwarded-For"} {
		if v := got.Header.Values(name); len(v) != 0 {
			t.Errorf("header %s leaked to the cluster: %v", name, v)
		}
	}
	if got.Header.Get("Accept") != "application/json" {
		t.Error("ordinary headers must pass through")
	}

	// 不合法的身份拿不到处理函数。
	if _, err := m.KubeHandler(cl, biz.Identity{UserID: "system:admin"}); err == nil {
		t.Fatal("invalid identity must be refused")
	}
}

func tarOf(t *testing.T, entries ...tar.Header) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for i := range entries {
		h := entries[i]
		body := ""
		if h.Typeflag == tar.TypeReg {
			body = "data-" + h.Name
			h.Size = int64(len(body))
		}
		h.Mode = 0o4755
		if err := tw.WriteHeader(&h); err != nil {
			t.Fatal(err)
		}
		_, _ = tw.Write([]byte(body))
	}
	_ = tw.Close()
	return buf.Bytes()
}

func TestRelayTar(t *testing.T) {
	t.Parallel()
	// 单个文件：只下发内容。
	var out bytes.Buffer
	var name string
	var isTar bool
	begin := func(n string, tarred bool, _ int64) io.Writer { name, isTar = n, tarred; return &out }
	n, err := relayTar(bytes.NewReader(tarOf(t, tar.Header{Typeflag: tar.TypeReg, Name: "app.log"})), "app.log", 1<<20, begin)
	if err != nil || name != "app.log" || isTar || out.String() != "data-app.log" || n != int64(out.Len()) {
		t.Fatalf("single file: %v name=%s tar=%v body=%q n=%d", err, name, isTar, out.String(), n)
	}

	// 目录：重新编码，丢弃不安全的条目。
	out.Reset()
	src := tarOf(t,
		tar.Header{Typeflag: tar.TypeDir, Name: "log/"},
		tar.Header{Typeflag: tar.TypeReg, Name: "log/a.txt"},
		tar.Header{Typeflag: tar.TypeReg, Name: "../escape.txt"},
		tar.Header{Typeflag: tar.TypeReg, Name: "/etc/passwd"},
		tar.Header{Typeflag: tar.TypeReg, Name: "log/../../up.txt"},
		tar.Header{Typeflag: tar.TypeReg, Name: "other/b.txt"},
		tar.Header{Typeflag: tar.TypeSymlink, Name: "log/evil", Linkname: "/etc/shadow"},
		tar.Header{Typeflag: tar.TypeSymlink, Name: "log/evil2", Linkname: "../../etc/shadow"},
		tar.Header{Typeflag: tar.TypeSymlink, Name: "log/ok", Linkname: "a.txt"},
		tar.Header{Typeflag: tar.TypeLink, Name: "log/hard", Linkname: "etc/passwd"},
		tar.Header{Typeflag: tar.TypeChar, Name: "log/dev"},
		tar.Header{Typeflag: tar.TypeReg, Name: "log/sub/c.txt"},
	)
	if _, err := relayTar(bytes.NewReader(src), "log", 1<<20, begin); err != nil {
		t.Fatal(err)
	}
	if name != "log.tar" || !isTar {
		t.Fatalf("directory: name=%s tar=%v", name, isTar)
	}
	tr := tar.NewReader(&out)
	var names []string
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if h.Mode&^0o777 != 0 {
			t.Errorf("%s keeps special mode bits %o", h.Name, h.Mode)
		}
		names = append(names, h.Name)
	}
	if strings.Join(names, " ") != "log/ log/a.txt log/ok log/sub/c.txt" {
		t.Fatalf("relayed entries = %v", names)
	}

	// 超过上限：中止。
	if _, err := relayTar(bytes.NewReader(tarOf(t, tar.Header{Typeflag: tar.TypeReg, Name: "big"})), "big", 3, begin); !errors.Is(err, errOverLimit) {
		t.Fatalf("over limit: %v", err)
	}
	if _, err := relayTar(bytes.NewReader(src), "log", 20, begin); !errors.Is(err, errOverLimit) {
		t.Fatalf("directory over limit: %v", err)
	}
}

func certPEM(t *testing.T, notAfter time.Time) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "demo.example.com"}, NotBefore: notAfter.Add(-365 * 24 * time.Hour), NotAfter: notAfter}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func checksOf(set *biz.FindingSet) map[string]int32 {
	out := map[string]int32{}
	for _, f := range set.Result(k8scontract.InspectionSummary{}).Findings {
		out[f.Check+"/"+f.Kind+"/"+f.Name] = f.Severity
	}
	return out
}

func TestInspectChecks(t *testing.T) {
	t.Parallel()
	now := time.Now()
	yes := true
	one, three := int32(1), int32(3)

	// 证书。
	set := biz.NewFindingSet()
	inspectTLS(set, "a", "expired", certPEM(t, now.Add(-time.Hour)), now)
	inspectTLS(set, "a", "soon", certPEM(t, now.Add(10*24*time.Hour)), now)
	inspectTLS(set, "a", "fine", certPEM(t, now.Add(200*24*time.Hour)), now)
	inspectTLS(set, "a", "garbage", []byte("not a cert"), now)
	inspectTLS(set, "a", "key-only", []byte("-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n"), now)
	got := checksOf(set)
	if len(got) != 2 || got["cert_expiring/Secret/expired"] != k8scontract.FindingCritical || got["cert_expiring/Secret/soon"] != k8scontract.FindingWarning {
		t.Fatalf("cert findings = %v", got)
	}

	// Pod：归并到 Deployment；latest / 无标签 / 无请求 / 无限制 / 无探针 / 频繁重启。
	set = biz.NewFindingSet()
	bad := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "a", Name: "web-7d764666f9-abcde", OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "web-7d764666f9", Controller: &yes}}},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "nginx"}, {Name: "side", Image: "registry.local:5000/side:latest"}}},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "main", RestartCount: 42}}},
	}
	good := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "a", Name: "ok-0", OwnerReferences: []metav1.OwnerReference{{Kind: "StatefulSet", Name: "ok", Controller: &yes}}},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "main", Image: "registry.local:5000/app@sha256:abc", ReadinessProbe: &corev1.Probe{},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m"), corev1.ResourceMemory: resource.MustParse("16Mi")},
				Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("32Mi")},
			},
		}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	done := bad.DeepCopy()
	done.Name, done.Status.Phase = "finished", corev1.PodSucceeded
	done.OwnerReferences = nil
	inspectPod(set, bad)
	inspectPod(set, bad) // 第二个副本
	inspectPod(set, good)
	inspectPod(set, done)
	got = checksOf(set)
	for _, k := range []string{"no_requests/Deployment/web", "no_limits/Deployment/web", "image_latest/Deployment/web", "no_probe/Deployment/web", "pod_restarts/Pod/web-7d764666f9-abcde"} {
		if _, ok := got[k]; !ok {
			t.Errorf("missing finding %s in %v", k, got)
		}
	}
	if len(got) != 5 {
		t.Errorf("unexpected findings: %v", got)
	}
	for image, want := range map[string]bool{"nginx": true, "nginx:latest": true, "nginx:1.27": false, "reg:5000/nginx": true, "reg:5000/nginx:1": false, "nginx@sha256:abc": false} {
		if imageUntagged(image) != want {
			t.Errorf("imageUntagged(%q) = %v", image, !want)
		}
	}

	// 副本数、PVC、节点。
	set = biz.NewFindingSet()
	inspectReplicas(set, "Deployment", "a", "single", &one)
	inspectReplicas(set, "Deployment", "a", "ha", &three)
	inspectReplicas(set, "StatefulSet", "a", "nil", nil)
	inspectPVC(set, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: "a", Name: "old", CreationTimestamp: metav1.NewTime(now.Add(-3 * time.Hour))}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending}}, now)
	inspectPVC(set, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: "a", Name: "new", CreationTimestamp: metav1.NewTime(now)}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending}}, now)
	inspectPVC(set, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: "a", Name: "bound", CreationTimestamp: metav1.NewTime(now.Add(-3 * time.Hour))}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}}, now)
	node := func(name, kubelet string, ready corev1.ConditionStatus, pressure bool) *corev1.Node {
		n := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
		n.Status.NodeInfo.KubeletVersion = kubelet
		n.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: ready}}
		if pressure {
			n.Status.Conditions = append(n.Status.Conditions, corev1.NodeCondition{Type: corev1.NodeDiskPressure, Status: corev1.ConditionTrue})
		}
		return n
	}
	inspectNode(set, node("ok", "v1.35.1", corev1.ConditionTrue, false), 35)
	inspectNode(set, node("down", "v1.34.0", corev1.ConditionFalse, false), 35)
	inspectNode(set, node("disk", "v1.35.1", corev1.ConditionTrue, true), 35)
	inspectNode(set, node("old", "v1.33.2", corev1.ConditionTrue, false), 35)
	inspectNode(set, node("newer", "v1.36.0", corev1.ConditionTrue, false), 35)
	got = checksOf(set)
	want := map[string]int32{
		"single_replica/Deployment/single":      k8scontract.FindingInfo,
		"pvc_pending/PersistentVolumeClaim/old": k8scontract.FindingWarning,
		"node_unhealthy/Node/down":              k8scontract.FindingCritical,
		"node_unhealthy/Node/disk":              k8scontract.FindingWarning,
		"version_skew/Node/old":                 k8scontract.FindingWarning,
		"version_skew/Node/newer":               k8scontract.FindingWarning,
	}
	if len(got) != len(want) {
		t.Fatalf("findings = %v", got)
	}
	for k, sev := range want {
		if got[k] != sev {
			t.Errorf("%s = %d, want %d", k, got[k], sev)
		}
	}
	if minorOf("v1.35.1") != 35 || minorOf("1.32.0-eks") != 32 || minorOf("garbage") != -1 {
		t.Error("minorOf")
	}
}

func TestDebugImageAllowList(t *testing.T) {
	t.Parallel()
	m := &Manager{}
	if img, err := m.debugImage(""); err != nil || img != DefaultDebugImage {
		t.Fatalf("default = %q %v", img, err)
	}
	if _, err := m.debugImage("evil/image:1"); err == nil {
		t.Fatal("image outside the allow-list must be refused")
	}
	m.SetDebugImages([]string{"alpine:3.20", "busybox:1.36"})
	if img, _ := m.debugImage(""); img != "alpine:3.20" {
		t.Fatalf("first configured image is the default, got %q", img)
	}
	if img, err := m.debugImage("busybox:1.36"); err != nil || img != "busybox:1.36" {
		t.Fatalf("listed image: %q %v", img, err)
	}
	if _, err := m.debugImage(DefaultDebugImage); err == nil {
		t.Fatal("built-in default is not allowed once a list is configured")
	}
}
