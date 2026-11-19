package kube

import (
	"strings"
	"testing"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

func TestQuickTouched(t *testing.T) {
	t.Parallel()
	want := map[string]string{
		k8scontract.OpScale:       "spec.replicas",
		k8scontract.OpRollback:    "spec.template",
		k8scontract.OpPause:       "spec.paused",
		k8scontract.OpCronSuspend: "spec.suspend",
		k8scontract.OpNodeCordon:  "spec.unschedulable",
		k8scontract.OpSetImage:    "spec.template.spec.initContainers[name=init].image",
	}
	for op, path := range want {
		got := QuickTouched(op, "initContainers", "init")
		if len(got) != 1 || biz.FieldPath(got[0]) != path {
			t.Errorf("%s touches %v, want %s", op, got, path)
		}
	}
	// pod-resize 涉及的字段随请求而定（planResize），不在这张固定表里。
	for _, op := range []string{k8scontract.OpRestart, k8scontract.OpCronTrigger, k8scontract.OpPodEvict, k8scontract.OpPodResize, k8scontract.StreamDrain} {
		if QuickTouched(op, "", "") != nil {
			t.Errorf("%s must not touch existing fields", op)
		}
	}
	// 每个需审批的一元写操作要么按对象执行，要么有快捷计划，要么是 Helm。
	u := &userClients{}
	for _, op := range []string{
		k8scontract.OpScale, k8scontract.OpRestart, k8scontract.OpSetImage, k8scontract.OpRollback, k8scontract.OpPause,
		k8scontract.OpCronTrigger, k8scontract.OpCronSuspend, k8scontract.OpPodEvict, k8scontract.OpPodResize, k8scontract.OpNodeCordon, k8scontract.StreamDrain,
	} {
		body := `{"ref":{"version":"v1","resource":"x","name":"n"},"container":"c","image":"i","name":"n"}`
		if qp, err := u.quickPlanFor(op, []byte(body)); err != nil || qp == nil || qp.action == "" {
			t.Errorf("%s has no quick plan: %v", op, err)
		}
		if !k8scontract.IsWriteOp(op) && !k8scontract.IsWriteStream(op) {
			t.Errorf("%s must be gated", op)
		}
	}
	if _, err := u.quickPlanFor(k8scontract.OpSecretReveal, nil); err == nil {
		t.Error("reads must not be plannable")
	}
}

func TestNamespaceObjects(t *testing.T) {
	t.Parallel()
	objs, err := NamespaceObjects(&k8scontract.NamespaceRequest{
		Name: "team-a", Labels: map[string]string{"team": "a"},
		Quota:        map[string]string{"requests.cpu": "4", "pods": "50"},
		LimitDefault: map[string]string{"cpu": "500m"}, LimitDefaultRequest: map[string]string{"memory": "128Mi"},
	})
	if err != nil {
		t.Fatal(err)
	}
	kinds := make([]string, 0, len(objs))
	for _, o := range objs {
		kinds = append(kinds, o.GetKind())
		if o.GetKind() != "Namespace" && o.GetNamespace() != "team-a" {
			t.Errorf("%s must live in the requested namespace", o.GetKind())
		}
	}
	if strings.Join(kinds, ",") != "Namespace,ResourceQuota,LimitRange" {
		t.Fatalf("kinds = %v", kinds)
	}
	if objs, err = NamespaceObjects(&k8scontract.NamespaceRequest{Name: "bare"}); err != nil || len(objs) != 1 {
		t.Fatalf("bare namespace: %v %v", objs, err)
	}
	bad := []k8scontract.NamespaceRequest{
		{Name: "Bad_Name"},
		{Name: "kube-x", Quota: map[string]string{"count/roles.rbac.authorization.k8s.io": "1"}},
		{Name: "a", Quota: map[string]string{"pods": "lots"}},
		{Name: "a", Quota: map[string]string{"pods": "-1"}},
		{Name: "a", LimitDefault: map[string]string{"nvidia.com/gpu": "1"}},
		{Name: "a", Labels: map[string]string{"pod-security.kubernetes.io/enforce": "privileged"}},
	}
	for i := range bad {
		if _, err := NamespaceObjects(&bad[i]); err == nil {
			t.Errorf("request %+v must be rejected", bad[i])
		}
	}
}
