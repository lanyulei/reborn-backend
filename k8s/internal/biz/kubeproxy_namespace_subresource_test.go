package biz

import (
	"testing"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// 经集群代理修改 Namespace 自身的子资源（status / finalize）：是写操作，审计目标记在 namespaces/<名称> 上
// （原先记成「命名空间 foo 下的资源 status」）。
func TestKubePolicyNamespaceSubresource(t *testing.T) {
	t.Parallel()
	for _, sub := range []string{"status", "finalize"} {
		act, err := KubePolicy("PUT", "/api/v1/namespaces/foo/"+sub, "", false)
		if err != nil {
			t.Fatalf("%s: %v", sub, err)
		}
		if !act.KubeWrite() || act.Audit != k8scontract.ActionUpdate {
			t.Errorf("%s: %+v must be audited as an update", sub, act)
		}
		want := k8scontract.KubeRequest{
			Version: "v1", Resource: "namespaces", Name: "foo", Subresource: sub, Resourceful: true,
		}
		if act.Target != want {
			t.Errorf("%s: target = %+v, want %+v", sub, act.Target, want)
		}
	}
	// 读请求不审计。
	if act, err := KubePolicy("GET", "/api/v1/namespaces/foo/status", "", false); err != nil || act.Audit != "" {
		t.Errorf("GET: %+v %v", act, err)
	}
}
