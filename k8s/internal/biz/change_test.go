package biz

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

func obj(t *testing.T, s string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		t.Fatal(err)
	}
	return m
}

const deployJSON = `{"apiVersion":"apps/v1","kind":"Deployment",
 "metadata":{"name":"web","namespace":"a","resourceVersion":"10","uid":"u1","labels":{"app":"web"},"annotations":{"deployment.kubernetes.io/revision":"3"},"managedFields":[{}]},
 "spec":{"replicas":2,"paused":false,"template":{"spec":{"containers":[{"name":"web","image":"nginx:1"},{"name":"side","image":"envoy:1"}]}}},
 "status":{"readyReplicas":2}}`

func TestDiffFieldsAndVerify(t *testing.T) {
	t.Parallel()
	base := ChangeBase(obj(t, deployJSON))
	if _, ok := base["status"]; ok {
		t.Fatal("status must be stripped")
	}
	desired := ChangeBase(obj(t, deployJSON))
	desired["spec"].(map[string]any)["replicas"] = float64(5)
	desired["metadata"].(map[string]any)["labels"].(map[string]any)["tier"] = "front"
	delete(desired["spec"].(map[string]any), "paused")
	patch, checks := DiffFields(base, desired)
	paths := make([]string, 0, len(checks))
	for _, c := range checks {
		paths = append(paths, FieldPath(c.Path))
	}
	if got := strings.Join(paths, ","); got != "metadata.labels.tier,spec.paused,spec.replicas" {
		t.Fatalf("touched fields = %s", got)
	}
	if spec := patch["spec"].(map[string]any); spec["replicas"] != float64(5) || spec["paused"] != nil {
		t.Fatalf("patch = %v", patch)
	}
	if _, has := patch["spec"].(map[string]any)["paused"]; !has {
		t.Fatal("a removed key must be an explicit null in the patch")
	}

	// 审批期间：别人加了标签、控制器改了状态与注解、int64 形式的数值——都不算冲突。
	live := obj(t, deployJSON)
	live["metadata"].(map[string]any)["labels"].(map[string]any)["team"] = "x"
	live["metadata"].(map[string]any)["annotations"].(map[string]any)["deployment.kubernetes.io/revision"] = "4"
	live["metadata"].(map[string]any)["resourceVersion"] = "99"
	live["status"] = map[string]any{"readyReplicas": int64(1)}
	live["spec"].(map[string]any)["replicas"] = int64(2)
	if err := VerifyChecks(live, checks); err != nil {
		t.Fatalf("untouched fields changed, must not conflict: %v", err)
	}
	next := ApplyPatch(live, patch)
	spec := next["spec"].(map[string]any)
	if spec["replicas"] != float64(5) || next["metadata"].(map[string]any)["labels"].(map[string]any)["team"] != "x" {
		t.Fatalf("patch must keep concurrent unrelated changes: %v", next)
	}
	if _, has := spec["paused"]; has {
		t.Fatal("patch must delete the removed key")
	}
	if live["spec"].(map[string]any)["replicas"] != int64(2) {
		t.Fatal("ApplyPatch must not modify its input")
	}

	// 涉及的字段被改动 → 冲突，并指出字段与前后值。
	live["spec"].(map[string]any)["replicas"] = int64(3)
	err := VerifyChecks(live, checks)
	var fc *FieldConflict
	if !errors.As(err, &fc) || fc.Path != "spec.replicas" || fc.Was != "2" || fc.Now != "3" {
		t.Fatalf("conflict = %v", err)
	}
	// 提交时不存在的字段被别人加上了 → 冲突。
	live["spec"].(map[string]any)["replicas"] = int64(2)
	live["metadata"].(map[string]any)["labels"].(map[string]any)["tier"] = "back"
	if err := VerifyChecks(live, checks); err == nil || !strings.Contains(err.Error(), "metadata.labels.tier") {
		t.Fatalf("added field must conflict: %v", err)
	}
}

// 列表是一个整体：只改一个容器的镜像，补丁替换整个 containers；别人改了另一个容器也算冲突。
func TestDiffFieldsListGranularity(t *testing.T) {
	t.Parallel()
	base := ChangeBase(obj(t, deployJSON))
	desired := ChangeBase(obj(t, deployJSON))
	cs := desired["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)
	cs[0].(map[string]any)["image"] = "nginx:2"
	_, checks := DiffFields(base, desired)
	if len(checks) != 1 || FieldPath(checks[0].Path) != "spec.template.spec.containers" {
		t.Fatalf("checks = %+v", checks)
	}
	live := obj(t, deployJSON)
	lc := live["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)
	lc[1].(map[string]any)["image"] = "envoy:2"
	if err := VerifyChecks(live, checks); err == nil {
		t.Fatal("a change to another element of the same list is a conflict (documented limitation)")
	}
	// 快捷操作用按名称选元素的路径，粒度是单个容器的镜像。
	one := []k8scontract.FieldCheck{{Path: []string{"spec", "template", "spec", "containers", "name=web", "image"}, Value: "nginx:1"}}
	if err := VerifyChecks(live, one); err != nil {
		t.Fatalf("other container changed, must not conflict: %v", err)
	}
	lc[0].(map[string]any)["image"] = "nginx:9"
	if err := VerifyChecks(live, one); err == nil || !strings.Contains(err.Error(), "containers[name=web].image") {
		t.Fatalf("err = %v", err)
	}
}

func TestVerifyChecksMasksSecrets(t *testing.T) {
	t.Parallel()
	live := obj(t, `{"apiVersion":"v1","kind":"Secret","data":{"password":"bmV3"}}`)
	err := VerifyChecks(live, []k8scontract.FieldCheck{{Path: []string{"data", "password"}, Value: "b2xk"}})
	if err == nil || strings.Contains(err.Error(), "bmV3") || strings.Contains(err.Error(), "b2xk") {
		t.Fatalf("secret values must be masked in conflicts: %v", err)
	}
}

func TestFieldPathAndLookup(t *testing.T) {
	t.Parallel()
	if got := FieldPath([]string{"metadata", "annotations", "a.b/c"}); got != `metadata.annotations["a.b/c"]` {
		t.Fatalf("path = %s", got)
	}
	m := obj(t, deployJSON)
	if v, ok := LookupField(m, []string{"spec", "template", "spec", "containers", "name=side", "image"}); !ok || v != "envoy:1" {
		t.Fatalf("lookup = %v %v", v, ok)
	}
	if _, ok := LookupField(m, []string{"spec", "template", "spec", "containers", "name=nope"}); ok {
		t.Fatal("missing element must not be found")
	}
	if _, ok := LookupField(m, []string{"spec", "replicas", "x"}); ok {
		t.Fatal("descending into a scalar must fail")
	}
}

func TestTransientChangeError(t *testing.T) {
	t.Parallel()
	for _, err := range []error{ErrClusterOffline("x"), errNotOwner, ErrCacheSyncing("x"), context.DeadlineExceeded} {
		if !TransientChangeError(err) {
			t.Errorf("%v must be transient", err)
		}
	}
	for _, err := range []error{ErrForbidden("x"), NewError(409, k8scontract.ReasonConflict, "x"), NewError(422, k8scontract.ReasonRejected, "x"), errors.New("boom")} {
		if TransientChangeError(err) {
			t.Errorf("%v must not be retried", err)
		}
	}
}

func TestChangeIdentity(t *testing.T) {
	t.Parallel()
	id := ChangeIdentity(&Change{ID: "c1", Requester: k8scontract.ChangeRequester{UserID: "u", Username: "n", IsAdmin: false}}, time.Unix(100, 0))
	if id.ChangeID != "c1" || id.UserID != "u" || id.RoleIDs == nil || id.Emergency {
		t.Fatalf("identity = %+v", id)
	}
}
