package kube

import (
	"slices"
	"strings"
	"testing"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// 缓存列表声明的筛选必须与视图一致：没有状态列的资源（Service / Ingress / ConfigMap / Secret /
// StorageClass）行的状态恒为空，原先照样声明支持 state，前端选任何状态都只能得到空列表。
func TestCachedFiltersFollowView(t *testing.T) {
	t.Parallel()
	want := map[k8scontract.GVR]string{
		k8scontract.GVRPod:     "node,state,owner", // Pod：节点、状态、所属，顺序不变
		k8scontract.GVRService: "owner", k8scontract.GVRIngress: "owner", k8scontract.GVRConfigMap: "owner",
		k8scontract.GVRSecret: "owner", k8scontract.GVRStorageClass: "owner",
		k8scontract.GVRDeployment: "state,owner", k8scontract.GVRNode: "state,owner", k8scontract.GVRNamespace: "state,owner",
	}
	for _, gvr := range k8scontract.CachedResources() {
		expect, known := want[gvr]
		if !known {
			expect = "state,owner"
		}
		if got := strings.Join(cachedFilters(gvr), ","); got != expect {
			t.Errorf("%s: filters = %q, want %q", gvr.Resource, got, expect)
		}
		// 声明与实际一致：声明了 state ⇔ 视图有状态列 ⇔ 行确实带状态（按状态筛选筛得出结果）。
		filters := cachedFilters(gvr)
		stateful := slices.ContainsFunc(views[gvr].columns, func(c k8scontract.Column) bool { return c.Type == k8scontract.ColState })
		if slices.Contains(filters, k8scontract.FilterState) != stateful {
			t.Errorf("%s: the state filter is advertised iff the view has a state column", gvr.Resource)
		}
		e := converterFor(gvr)(&unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod"}})
		if (e.Row.State != "") != stateful {
			t.Errorf("%s: row state = %q, stateful = %v", gvr.Resource, e.Row.State, stateful)
		}
		if _, total := biz.SelectEntries([]*biz.Entry{e}, biz.ListQuery{State: k8scontract.StateOK}); (total == 1) != (stateful && e.Row.State == k8scontract.StateOK) {
			t.Errorf("%s: filtering by state=ok matched %d of 1 rows (row state %q)", gvr.Resource, total, e.Row.State)
		}
		if !slices.Contains(filters, k8scontract.FilterOwner) {
			t.Errorf("%s: the owner filter must always be offered", gvr.Resource)
		}
	}
	// 返回值每次新建：调用方改动不会污染后续请求。
	first := cachedFilters(k8scontract.GVRPod)
	first[0] = "mutated"
	if got := cachedFilters(k8scontract.GVRPod)[0]; got != k8scontract.FilterNode {
		t.Fatalf("filters slice is shared between calls: %q", got)
	}
}
