package kube

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// eventPage 一页事件：编号从 from 起共 n 条，时间随编号递增。集群按 etcd 键序返回（旧的在前）。
func eventPage(from, n int, continueToken string) corev1.EventList {
	list := corev1.EventList{TypeMeta: metav1.TypeMeta{Kind: "EventList", APIVersion: "v1"}, Items: make([]corev1.Event, 0, n)}
	list.Continue = continueToken
	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for i := from; i < from+n; i++ {
		list.Items = append(list.Items, corev1.Event{
			ObjectMeta:     metav1.ObjectMeta{Name: fmt.Sprintf("web.%x", i), Namespace: "a"},
			InvolvedObject: corev1.ObjectReference{Kind: "Pod", Name: "web", Namespace: "a"},
			Reason:         fmt.Sprintf("R%d", i),
			LastTimestamp:  metav1.NewTime(base.Add(time.Duration(i) * time.Second)),
		})
	}
	return list
}

// 对象详情的事件按 continue 翻页后再排序截断：原先只读第一页（该对象最旧的 500 条），
// 事件多的对象最新的反而看不到。
func TestEventsFollowPagination(t *testing.T) {
	t.Parallel()
	const total = 2*eventsPageSize + 100 // 1100 条：两整页 + 100（最后一页不满）
	seen := &seenRequests{}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.add(r)
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/namespaces/a/events" {
			clusterNotFound(w)
			return
		}
		from, token := 0, "p1"
		switch c := r.URL.Query().Get("continue"); c {
		case "":
		case "p1":
			from, token = eventsPageSize, "p2"
		case "p2":
			from, token = 2*eventsPageSize, ""
		default:
			t.Errorf("unexpected continue token %q", c)
		}
		_ = json.NewEncoder(w).Encode(eventPage(from, min(eventsPageSize, total-from), token))
	})
	u := fakeClusterUser(t, handler)

	out, err := u.events(podRef("a", "web"))
	if err != nil {
		t.Fatal(err)
	}
	// 至多 200 条，最近的在前：都来自最后一页（原先拿到的最旧一页里一条都没有）。
	if len(out.Items) != 200 || out.Items[0].Reason != fmt.Sprintf("R%d", total-1) || out.Items[len(out.Items)-1].Reason != fmt.Sprintf("R%d", total-200) {
		t.Fatalf("items = %d, first %q, last %q", len(out.Items), out.Items[0].Reason, out.Items[len(out.Items)-1].Reason)
	}
	if first := out.Items[0]; first.Object != "Pod/web" || first.LastTime.IsZero() || !first.LastTime.After(out.Items[1].LastTime) {
		t.Fatalf("first item = %+v", first)
	}
	requests := seen.on("GET /api/v1/namespaces/a/events")
	if len(requests) != 3 {
		t.Fatalf("event list requests = %d, want 3 pages", len(requests))
	}
	for i, token := range []string{"", "p1", "p2"} {
		q := requests[i].query
		if q.Get("continue") != token || q.Get("limit") != "500" {
			t.Errorf("page %d: continue=%q limit=%q", i, q.Get("continue"), q.Get("limit"))
		}
		if want := "involvedObject.name=web,involvedObject.kind=Pod,involvedObject.namespace=a"; q.Get("fieldSelector") != want {
			t.Errorf("page %d: fieldSelector = %q, want %q", i, q.Get("fieldSelector"), want)
		}
	}
}

// 翻页有明确上限：集群一直给得出下一页时，读满 maxObjectEvents 条（maxObjectEvents / eventsPageSize 页）即止。
func TestEventsStopAtLimit(t *testing.T) {
	t.Parallel()
	seen := &seenRequests{}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.add(r)
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/events" { // 集群级对象的事件：路径不带命名空间
			clusterNotFound(w)
			return
		}
		page := 0
		if c := r.URL.Query().Get("continue"); c != "" {
			if _, err := fmt.Sscanf(c, "p%d", &page); err != nil {
				t.Errorf("continue token %q: %v", c, err)
			}
		}
		_ = json.NewEncoder(w).Encode(eventPage(page*eventsPageSize, eventsPageSize, fmt.Sprintf("p%d", page+1))) // 永远还有下一页
	})
	u := fakeClusterUser(t, handler)

	out, err := u.events(k8scontract.Ref{Version: "v1", Resource: "nodes", Name: "n1"})
	if err != nil {
		t.Fatal(err)
	}
	pages := maxObjectEvents / eventsPageSize
	if requests := seen.on("GET /api/v1/events"); len(requests) != pages {
		t.Fatalf("event list requests = %d, want at most %d pages", len(requests), pages)
	}
	// 读到的是最旧的 maxObjectEvents 条，再取最近的 200 条。
	if len(out.Items) != 200 || out.Items[0].Reason != fmt.Sprintf("R%d", maxObjectEvents-1) {
		t.Fatalf("items = %d, first %q", len(out.Items), out.Items[0].Reason)
	}
}
