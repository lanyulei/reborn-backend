package biz

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

func TestKubePolicy(t *testing.T) {
	t.Parallel()
	denied := []struct{ method, path string }{
		{"GET", ""},
		{"GET", "/"},
		{"GET", "/metrics"},
		{"GET", "/logs/kube-apiserver.log"},
		{"GET", "/debug/pprof/"},
		{"GET", "/api/../metrics"},
		{"GET", "/api/v1/../../metrics"},
		{"GET", "/apis/./x"},
		{"GET", "//api/v1"},
		{"GET", "/api//v1/pods"},
		{"GET", "/apix/v1"},
		{"GET", "api/v1"},
		{"GET", "/api/v1/pods\x00"},
		{"GET", `/api\v1`},
		{"POST", "/api/v1/namespaces/a/pods/p/portforward"},
		{"GET", "/api/v1/namespaces/a/pods/p/proxy/"},
		{"GET", "/api/v1/namespaces/a/pods/p/proxy/secret"},
		{"GET", "/api/v1/namespaces/a/services/s/proxy/x"},
		{"GET", "/api/v1/nodes/n/proxy/metrics"},
		{"GET", "/api/v1/proxy/nodes/n"},
	}
	for _, c := range denied {
		if _, err := KubePolicy(c.method, c.path, "", false); err == nil {
			t.Errorf("%s %q must be denied", c.method, c.path)
		}
	}
	cases := []struct {
		method, path, query string
		audit               string
		write               bool
	}{
		{"GET", "/version", "", "", false},
		{"GET", "/api", "", "", false},
		{"GET", "/apis/apps/v1", "", "", false},
		{"GET", "/openapi/v3", "", "", false},
		{"GET", "/api/v1/namespaces/a/pods", "watch=true", "", false},
		{"GET", "/api/v1/namespaces/a/pods/p/log", "follow=true", "", false},
		{"POST", "/api/v1/namespaces/a/pods", "", k8scontract.ActionCreate, true},
		{"PATCH", "/apis/apps/v1/namespaces/a/deployments/web", "", k8scontract.ActionUpdate, true},
		{"PUT", "/apis/apps/v1/namespaces/a/deployments/web/scale", "", k8scontract.ActionUpdate, true},
		{"DELETE", "/api/v1/namespaces/a", "", k8scontract.ActionDelete, true},
		{"POST", "/api/v1/namespaces/a/pods", "dryRun=All", "", false},
		{"POST", "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews", "", "", false},
		{"GET", "/api/v1/namespaces/a/pods/p/exec", "command=sh", k8scontract.ActionExec, false},
		{"POST", "/api/v1/namespaces/a/pods/p/attach", "", k8scontract.ActionExec, false},
	}
	for _, c := range cases {
		act, err := KubePolicy(c.method, c.path, c.query, false)
		if err != nil || act.Audit != c.audit || act.KubeWrite() != c.write {
			t.Errorf("%s %s?%s = %+v, %v; want audit %q write %v", c.method, c.path, c.query, act, err, c.audit, c.write)
		}
	}
	if _, err := KubePolicy("GET", "/api/v1/namespaces/a/pods/p/exec", "", true); err != errExecDisabled {
		t.Errorf("exec on an exec-disabled cluster: %v", err)
	}
	if act, _ := KubePolicy("PATCH", "/apis/apps/v1/namespaces/a/deployments/web/status", "", false); act.Target.Namespace != "a" ||
		act.Target.Resource != "deployments" || act.Target.Name != "web" || act.Target.Group != "apps" || act.Target.Subresource != "status" {
		t.Errorf("target = %+v", act.Target)
	}
	if act, _ := KubePolicy("DELETE", "/api/v1/namespaces/a", "", false); act.Target.Resource != "namespaces" || act.Target.Name != "a" || act.Target.Namespace != "" {
		t.Errorf("namespace target = %+v", act.Target)
	}
}

func TestKubeIsChange(t *testing.T) {
	t.Parallel()
	for path, want := range map[string]bool{
		"/api/v1/namespaces/a/pods":                              true,
		"/api/v1/namespaces/a/pods/p/exec":                       false,
		"/api/v1/namespaces/a/pods/p/eviction":                   true,
		"/apis/authorization.k8s.io/v1/selfsubjectaccessreviews": false,
		"/version": false,
	} {
		if got := k8scontract.KubeIsChange(http.MethodPost, path); got != want {
			t.Errorf("KubeIsChange(POST %s) = %v", path, got)
		}
	}
	if k8scontract.KubeIsChange(http.MethodGet, "/api/v1/namespaces/a/pods") {
		t.Error("GET is not a change")
	}
}

func TestFilePaths(t *testing.T) {
	t.Parallel()
	for _, p := range []string{"", "etc", "/etc/../root", "/etc/./x", "/etc//x", "/etc/", "/a\nb", "/", "/a/.."} {
		if CheckContainerPath(p, false) == nil {
			t.Errorf("path %q must be rejected", p)
		}
	}
	for _, p := range []string{"/etc/hosts", "/var/log", "/tmp/a b/c.txt"} {
		if err := CheckContainerPath(p, false); err != nil {
			t.Errorf("path %q: %v", p, err)
		}
	}
	if CheckContainerPath("/", true) != nil {
		t.Error("root is a valid upload directory")
	}
	for _, n := range []string{"", ".", "..", "a/b", `a\b`, "../x", "x\x00", "a\tb", strings.Repeat("a", 256)} {
		if CheckFileName(n) == nil {
			t.Errorf("file name %q must be rejected", n)
		}
	}
	if err := CheckFileName("report 2026.tar.gz"); err != nil {
		t.Error(err)
	}
}

func TestSafeTar(t *testing.T) {
	t.Parallel()
	for _, n := range []string{"/etc/passwd", "../x", "log/../../x", "log/../x", "other/file", "", "logs", "log\x00", ".."} {
		if SafeTarEntry(n, "log") {
			t.Errorf("entry %q must be dropped", n)
		}
	}
	for _, n := range []string{"log", "log/", "log/a.txt", "log/sub/b", "./log/a"} {
		if !SafeTarEntry(n, "log") {
			t.Errorf("entry %q must be kept", n)
		}
	}
	for entry, target := range map[string]string{"log/a": "/etc/passwd", "log/b": "../../etc/passwd", "log/sub/c": "../../../x", "log/d": ""} {
		if SafeTarLink(entry, target, "log") {
			t.Errorf("link %s -> %s must be dropped", entry, target)
		}
	}
	if !SafeTarLink("log/sub/c", "../a.txt", "log") || !SafeTarLink("log/a", "b", "log") {
		t.Error("links inside the tree must be kept")
	}
}

func decodeChunk(t *testing.T, chunk []byte) [][]any {
	t.Helper()
	zr, err := gzip.NewReader(bytes.NewReader(chunk))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(zr)
	var out [][]any
	for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte{'\n'}) {
		var row []any
		if err := json.Unmarshal(line, &row); err != nil {
			t.Fatalf("bad record line %q: %v", line, err)
		}
		out = append(out, row)
	}
	return out
}

func TestRecorder(t *testing.T) {
	t.Parallel()
	start := time.Now()
	r := newRecorder(10, start)
	if _, _, _, ok := r.take(start, true); ok {
		t.Fatal("empty recorder must have nothing to flush")
	}
	r.resize(80, 24)
	r.output([]byte("hello"))
	// 未到落库条件：不取。
	if _, _, _, ok := r.take(start.Add(time.Second), false); ok {
		t.Fatal("small fresh buffer must not flush yet")
	}
	seq, chunk, truncated, ok := r.take(start.Add(k8scontract.RecordFlushInterval+time.Second), false)
	if !ok || seq != 1 || truncated {
		t.Fatalf("flush = %d %v %v", seq, truncated, ok)
	}
	rows := decodeChunk(t, chunk)
	if len(rows) != 2 || rows[0][1] != "r" || rows[0][2] != "80x24" || rows[1][1] != "o" {
		t.Fatalf("rows = %v", rows)
	}
	if b, _ := base64.StdEncoding.DecodeString(rows[1][2].(string)); string(b) != "hello" {
		t.Fatalf("output = %q", b)
	}
	// 超过上限：停止录制并标记；之后的输出不再进入。
	r.output([]byte("world!")) // 5 + 6 > 10
	r.output([]byte("x"))
	_, chunk, truncated, ok = r.take(start, true)
	if !ok || chunk != nil || !truncated {
		t.Fatalf("truncation flag must be flushed on its own: chunk=%v truncated=%v ok=%v", chunk, truncated, ok)
	}
	if _, _, _, ok := r.take(start, true); ok {
		t.Fatal("nothing left to flush")
	}
}

func TestFindingSet(t *testing.T) {
	t.Parallel()
	s := NewFindingSet()
	f := Finding{Check: k8scontract.CheckNoRequests, Severity: k8scontract.FindingWarning, Namespace: "a", Kind: "Deployment", Name: "web"}
	s.Add(f)
	s.Add(f) // 同一对象同一检查项只记一次（多副本）
	for i := 0; i < k8scontract.MaxFindingsPerCheck+10; i++ {
		s.Add(Finding{Check: k8scontract.CheckImageLatest, Severity: k8scontract.FindingWarning, Namespace: "a", Kind: "Pod", Name: strings.Repeat("p", i+1)})
	}
	s.Add(Finding{Check: k8scontract.CheckNodeUnhealthy, Severity: k8scontract.FindingCritical, Kind: "Node", Name: "n1"})
	s.Add(Finding{Check: k8scontract.CheckSingleReplica, Severity: k8scontract.FindingInfo, Namespace: "a", Kind: "Deployment", Name: "web"})
	res := s.Result(k8scontract.InspectionSummary{Pods: 3})
	if res.Summary.Critical != 1 || res.Summary.Warning != 1+k8scontract.MaxFindingsPerCheck+10 || res.Summary.Info != 1 || res.Summary.Pods != 3 {
		t.Fatalf("summary = %+v", res.Summary)
	}
	if len(res.Findings) != 1+k8scontract.MaxFindingsPerCheck+1+1 {
		t.Fatalf("stored findings = %d", len(res.Findings))
	}
	if res.Summary.Checks[0].Check != k8scontract.CheckNodeUnhealthy {
		t.Fatalf("checks must be ordered by severity: %+v", res.Summary.Checks)
	}
	if got := k8scontract.InspectionScore(1, 12, 3); got != 100-5-12-0 {
		t.Fatalf("score = %d", got)
	}
	if got := k8scontract.InspectionScore(100, 100, 1000); got != 10 {
		t.Fatalf("score floor = %d", got)
	}
}
