package kube

import (
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// 没启用 status 子资源的 CRD，PUT 是整体替换：更新时必须带上现有的 status，否则 status 被清空。
// YAML 里自带的 status 不采纳（prepare 已去掉），带上的是集群里的现值，且是拷贝（不与现状共享底层 map）。
func TestKeepStatus(t *testing.T) {
	t.Parallel()
	cur := &unstructured.Unstructured{Object: map[string]any{
		"spec":   map[string]any{"size": int64(1)},
		"status": map[string]any{"phase": "Ready", "conditions": []any{map[string]any{"type": "Ready"}}},
	}}
	obj := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{"size": int64(2)}}}
	keepStatus(obj, cur)
	if !reflect.DeepEqual(obj.Object["status"], cur.Object["status"]) {
		t.Fatalf("status must be carried over: %v", obj.Object["status"])
	}
	obj.Object["status"].(map[string]any)["phase"] = "Changed"
	if cur.Object["status"].(map[string]any)["phase"] != "Ready" {
		t.Fatal("the carried status must be a copy")
	}

	// 现状没有 status：不凭空加一个。
	bare := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{}}}
	keepStatus(bare, &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{}}})
	if _, ok := bare.Object["status"]; ok {
		t.Fatal("no status must be invented")
	}
}

// 变更单执行时的「现状 + 补丁」同样保留现状的 status，并带现状的 resourceVersion / uid（并发修改即 409）。
func TestPatchedKeepsLiveStatus(t *testing.T) {
	t.Parallel()
	live := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "example.io/v1", "kind": "Widget",
		"metadata": map[string]any{"name": "w", "namespace": "a", "resourceVersion": "7", "uid": "u-1", "managedFields": []any{map[string]any{"manager": "x"}}},
		"spec":     map[string]any{"size": int64(1)},
		"status":   map[string]any{"phase": "Ready"},
	}}
	next := patched(live, map[string]any{"spec": map[string]any{"size": int64(3)}})
	if !reflect.DeepEqual(next.Object["status"], map[string]any{"phase": "Ready"}) {
		t.Fatalf("status must survive the patch: %v", next.Object["status"])
	}
	if size, _, _ := unstructured.NestedInt64(next.Object, "spec", "size"); size != 3 {
		t.Fatalf("patch not applied: %d", size)
	}
	if next.GetResourceVersion() != "7" || string(next.GetUID()) != "u-1" {
		t.Fatalf("resourceVersion / uid must come from the live object: %q %q", next.GetResourceVersion(), next.GetUID())
	}
	if _, ok, _ := unstructured.NestedFieldNoCopy(next.Object, "metadata", "managedFields"); ok {
		t.Fatal("managedFields must not be sent back")
	}
	if size, _, _ := unstructured.NestedInt64(live.Object, "spec", "size"); size != 1 {
		t.Fatal("the live object must not be modified")
	}
}
