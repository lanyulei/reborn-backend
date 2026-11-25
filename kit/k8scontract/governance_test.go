package k8scontract

import (
	"reflect"
	"testing"
)

// Namespace 自身的子资源（/status、/finalize）是「namespaces 资源 + 名称 + 子资源」，
// 不能解析成「命名空间 foo 下名为 status 的资源」。
func TestParseKubePath(t *testing.T) {
	t.Parallel()
	cases := []struct {
		path string
		want KubeRequest
	}{
		{"/api/v1/namespaces/foo/status", KubeRequest{Version: "v1", Resource: "namespaces", Name: "foo", Subresource: "status", Resourceful: true}},
		{"/api/v1/namespaces/foo/finalize", KubeRequest{Version: "v1", Resource: "namespaces", Name: "foo", Subresource: "finalize", Resourceful: true}},
		{"/api/v1/namespaces/foo", KubeRequest{Version: "v1", Resource: "namespaces", Name: "foo", Resourceful: true}},
		{"/api/v1/namespaces", KubeRequest{Version: "v1", Resource: "namespaces", Resourceful: true}},
		{"/api/v1/namespaces/foo/pods", KubeRequest{Version: "v1", Namespace: "foo", Resource: "pods", Resourceful: true}},
		{"/api/v1/namespaces/foo/pods/bar/eviction", KubeRequest{Version: "v1", Namespace: "foo", Resource: "pods", Name: "bar", Subresource: "eviction", Resourceful: true}},
		// 其他组里复数名恰好叫 status / finalize 的命名空间级资源：仍按命名空间限定解析（没有歧义可依）。
		{"/apis/example.com/v1/namespaces/foo/status", KubeRequest{Group: "example.com", Version: "v1", Namespace: "foo", Resource: "status", Resourceful: true}},
		{"/apis/apps/v1/namespaces/foo/deployments/web/status", KubeRequest{Group: "apps", Version: "v1", Namespace: "foo", Resource: "deployments", Name: "web", Subresource: "status", Resourceful: true}},
		{"/version", KubeRequest{}},
	}
	for _, c := range cases {
		if got := ParseKubePath(c.path); !reflect.DeepEqual(got, c.want) {
			t.Errorf("ParseKubePath(%q) = %+v, want %+v", c.path, got, c.want)
		}
	}
	// 需审批的集群上，经限时 kubeconfig 改 Namespace 的 status / finalize 仍被当作变更拦下（core 的审批门禁）。
	for _, p := range []string{"/api/v1/namespaces/foo/status", "/api/v1/namespaces/foo/finalize"} {
		if !KubeIsChange("PUT", p) {
			t.Errorf("KubeIsChange(PUT %s) = false, want true", p)
		}
	}
}
