package kube

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const diagLogPath = "GET /api/v1/namespaces/a/pods/web/log"

// diagStatus 一个容器状态：waiting（reason）并重启过 restarts 次。
func diagStatus(name, reason string, restarts int) map[string]any {
	return map[string]any{"name": name, "ready": false, "restartCount": restarts, "state": map[string]any{"waiting": map[string]any{"reason": reason}}}
}

// diagPodSpec 由状态推出容器清单（同名同序）。
func diagPodSpec(statuses ...map[string]any) []any {
	spec := make([]any, 0, len(statuses))
	for _, st := range statuses {
		spec = append(spec, map[string]any{"name": st["name"], "image": "nginx"})
	}
	return spec
}

// diagPodFixture 假集群：Pod web 有 initContainers 与 containers 两组容器，日志接口对任何容器都回固定的一行。
func diagPodFixture(t *testing.T, seen *seenRequests, inits, containers []map[string]any) *userClients {
	t.Helper()
	pod := map[string]any{
		"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"name": "web", "namespace": "a", "uid": "u-1"},
		"spec":   map[string]any{"initContainers": diagPodSpec(inits...), "containers": diagPodSpec(containers...)},
		"status": map[string]any{"phase": "Pending", "initContainerStatuses": inits, "containerStatuses": containers},
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.add(r)
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/a/pods/web":
			_ = json.NewEncoder(w).Encode(pod)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/a/pods/web/log":
			// 固定的一行：读了哪个容器由 seenRequests 记录，响应内容不取自请求。
			w.Header().Set("Content-Type", "text/plain")
			_, _ = io.WriteString(w, "log line\n")
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/a/events":
			_ = json.NewEncoder(w).Encode(corev1.EventList{TypeMeta: metav1.TypeMeta{Kind: "EventList", APIVersion: "v1"}})
		default:
			clusterNotFound(w)
		}
	})
	return fakeClusterUser(t, handler)
}

// logReads 实际发生的日志读取：「容器名 / 是否上一次」。
func logReads(t *testing.T, seen *seenRequests) []string {
	t.Helper()
	var out []string
	for _, r := range seen.on(diagLogPath) {
		out = append(out, r.query.Get("container")+"/previous="+r.query.Get("previous"))
	}
	return out
}

// 诊断读日志的名额（至多 4 个容器）只算真正读了日志的容器：waiting 且没重启过的容器没有日志可读，
// 原先却占名额——排在它们后面的 CrashLoopBackOff 容器就读不到日志。
func TestDiagnosePodLogQuotaCountsOnlyReads(t *testing.T) {
	t.Parallel()
	idle := func(n int) []map[string]any {
		out := make([]map[string]any, 0, n)
		for i := range n {
			out = append(out, diagStatus(fmt.Sprintf("idle%d", i), "CreateContainerConfigError", 0))
		}
		return out
	}
	crash := diagStatus("crash", "CrashLoopBackOff", 3)

	// 普通容器：4 个 waiting / 0 次重启的排在前面。
	seen := &seenRequests{}
	u := diagPodFixture(t, seen, nil, append(idle(4), crash))
	out, err := u.diagnosePod(podRef("a", "web"))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := logReads(t, seen), []string{"crash/previous=", "crash/previous=true"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("log reads = %v, want %v", got, want)
	}
	if len(out.Containers) != 5 {
		t.Fatalf("containers = %d", len(out.Containers))
	}
	for i := range 4 {
		if c := out.Containers[i]; c.LogTail != nil || c.LogError != "" {
			t.Errorf("container %d has no logs to read: %+v", i, c)
		}
	}
	if c := out.Containers[4]; !reflect.DeepEqual(c.LogTail, []string{"log line"}) || !reflect.DeepEqual(c.PreviousLogTail, []string{"log line"}) || c.LogError != "" {
		t.Fatalf("the crashing container must have its logs read: %+v", c)
	}

	// 计数器在 init 容器组与普通容器组之间共享：4 个没日志可读的 init 容器同样不能占名额。
	seen = &seenRequests{}
	u = diagPodFixture(t, seen, idle(4), []map[string]any{crash})
	out, err = u.diagnosePod(podRef("a", "web"))
	if err != nil {
		t.Fatal(err)
	}
	if got := logReads(t, seen); len(got) != 2 || got[0] != "crash/previous=" || got[1] != "crash/previous=true" {
		t.Fatalf("log reads = %v", got)
	}
	if last := out.Containers[len(out.Containers)-1]; last.Name != "crash" || len(last.LogTail) != 1 {
		t.Fatalf("crashing container after idle init containers: %+v", last)
	}
}

// 上限不变：至多 4 个容器读日志。
func TestDiagnosePodLogQuotaStillCapped(t *testing.T) {
	t.Parallel()
	crashing := make([]map[string]any, 0, 6)
	for i := range 6 {
		crashing = append(crashing, diagStatus(fmt.Sprintf("c%d", i), "CrashLoopBackOff", i+1))
	}
	seen := &seenRequests{}
	u := diagPodFixture(t, seen, nil, crashing)
	out, err := u.diagnosePod(podRef("a", "web"))
	if err != nil {
		t.Fatal(err)
	}
	if reads := logReads(t, seen); len(reads) != 8 { // 4 个容器 ×（本次 + 上一次）
		t.Fatalf("log reads = %d (%v), want 8", len(reads), reads)
	}
	for i, c := range out.Containers {
		if (len(c.LogTail) > 0) != (i < 4) {
			t.Errorf("container %d (%s): logs read = %v", i, c.Name, len(c.LogTail) > 0)
		}
	}
}
