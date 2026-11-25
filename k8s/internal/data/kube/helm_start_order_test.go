package kube

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// 执行变更单时「已开始」必须在拿到 Helm 并发名额之后才记：名额暂满（429）或进程正在关停（503）时集群
// 没有被改动，不能留下 started——否则变更单被记为失败，而不是放回队列稍后重试（见 biz.ChangeRunner）。
func TestHelmChangeMarksStartedAfterSlotAcquired(t *testing.T) {
	t.Parallel()
	id := biz.Identity{UserID: "018f3e1a-0cd1-7000-8000-000000000001"}
	newUser := func() (*userClients, *helmOps) {
		t.Helper()
		h := newHelmOps()
		t.Cleanup(h.cancel)
		m := &Manager{apps: &appsState{}, helm: h}
		return &userClients{ctx: context.Background(), id: id, c: &conn{m: m, cluster: &biz.Cluster{ID: "c1", Name: "c1"}}}, h
	}
	var order []string
	run := func(context.Context, *userClients) helmResult {
		order = append(order, "run")
		return helmResult{resp: k8scontract.HelmApplyResponse{Revision: 1}}
	}

	// 名额暂满：既不记 started，也不执行。
	u, h := newUser()
	u.started = func() error { order = append(order, "started"); return nil }
	for range maxHelmOpsPerCluster {
		_, done, err := h.begin("c1", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		defer done()
	}
	_, audits, err := u.helmAsyncWait(helmAudit(k8scontract.ActionHelmInstall, "ns", "web", nil), time.Second, run)
	if be := biz.AsError(err); be.Code != http.StatusTooManyRequests {
		t.Fatalf("cluster at capacity: %v", err)
	}
	if len(order) != 0 || audits != nil {
		t.Fatalf("nothing may run or be marked as started before a slot is acquired: %v %v", order, audits)
	}

	// 进程关停：同样。
	u, h = newUser()
	h.cancel()
	u.started = func() error { order = append(order, "started"); return nil }
	_, audits, err = u.helmAsyncWait(helmAudit(k8scontract.ActionHelmInstall, "ns", "web", nil), time.Second, run)
	if be := biz.AsError(err); be.Code != http.StatusServiceUnavailable || be.Reason != k8scontract.ReasonClusterOffline {
		t.Fatalf("shutting down: %v", err)
	}
	if len(order) != 0 || audits != nil {
		t.Fatalf("nothing may run or be marked as started while shutting down: %v %v", order, audits)
	}

	// 拿到名额：先记 started，再执行，各一次。
	u, _ = newUser()
	u.started = func() error { order = append(order, "started"); return nil }
	if _, audits, err = u.helmAsyncWait(helmAudit(k8scontract.ActionHelmInstall, "ns", "web", nil), time.Second, run); err != nil || len(audits) != 1 {
		t.Fatalf("normal path: %v %v", audits, err)
	}
	if want := []string{"started", "run"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("order = %v, want %v", order, want)
	}

	// 记不下 started（数据库不可用）：不执行，占到的名额要还回去。
	u, h = newUser()
	dbDown := errors.New("db down")
	u.started = func() error { return dbDown }
	_, _, err = u.helmAsyncWait(helmAudit(k8scontract.ActionHelmInstall, "ns", "web", nil), time.Second, run)
	if !errors.Is(err, dbDown) || len(order) != 2 {
		t.Fatalf("a failed start must stop the operation: %v (order %v)", err, order)
	}
	for range maxHelmOpsPerCluster {
		_, done, err := h.begin("c1", time.Minute)
		if err != nil {
			t.Fatalf("the slot taken before the failed start was not released: %v", err)
		}
		done()
	}
}
