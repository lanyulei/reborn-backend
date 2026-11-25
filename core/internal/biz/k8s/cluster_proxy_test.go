package biz

import (
	"context"
	"testing"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/k8s/v1"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// fakeClusterStore 集群读写的内存实现：记下最近一次写入的内容。
type fakeClusterStore struct {
	cluster *Cluster
	updated *Cluster
}

func (s *fakeClusterStore) List(context.Context, ClusterFilter, int64, int64) ([]*Cluster, int64, error) {
	return []*Cluster{s.cluster}, 1, nil
}

func (s *fakeClusterStore) Get(_ context.Context, id string) (*Cluster, error) {
	if s.cluster == nil || s.cluster.ID != id {
		return nil, ErrClusterNotFound
	}
	c := *s.cluster
	return &c, nil
}

func (s *fakeClusterStore) Options(context.Context, bool, []string) ([]*ClusterOption, error) {
	return []*ClusterOption{{ID: s.cluster.ID, Name: s.cluster.Name}}, nil
}

func (s *fakeClusterStore) Create(_ context.Context, c *Cluster, _ []byte, _ *AuditEntry) error {
	cp := *c
	s.cluster = &cp
	return nil
}

func (s *fakeClusterStore) Update(_ context.Context, c *Cluster, _ []byte, _ int32, _ *AuditEntry) error {
	cp := *c
	s.updated = &cp
	return nil
}

func (s *fakeClusterStore) Delete(context.Context, string, *AuditEntry) error { return nil }

func (s *fakeClusterStore) ReplaceRegister(context.Context, string, string, []byte, time.Time, *AuditEntry) error {
	return nil
}

func (s *fakeClusterStore) RevokeAll(context.Context, string, *AuditEntry) error { return nil }

// capturingPlane 记下发给数据面的「测试连接」请求。
type capturingPlane struct {
	fakeChangePlane
	tests []kc.ClusterTestRequest
}

func (p *capturingPlane) Call(ctx context.Context, id kc.Identity, clusterID, op string, req any) ([]byte, error) {
	if r, ok := req.(kc.ClusterTestRequest); ok {
		p.tests = append(p.tests, r)
	}
	return p.fakeChangePlane.Call(ctx, id, clusterID, op, req)
}

// 编辑集群 / 测试连接时代理地址的账号。已保存 http://alice:pw@proxy:8080，界面回显的是打码后的
// http://REDACTED@proxy:8080。
func TestClusterUpdateProxyPlaceholder(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const stored, placeholder = "http://alice:pw@proxy:8080", "http://REDACTED@proxy:8080" //nolint:gosec // 测试数据
	newUsecase := func() (*ClusterUsecase, *fakeClusterStore, *capturingPlane) {
		f := newChangeFakes()
		store := &fakeClusterStore{cluster: &Cluster{
			ID: "c1", Name: "prod-1", AccessMode: kc.AccessDirect, APIServer: "https://10.0.0.1:6443", ProxyURL: stored, HasCredential: true, Enabled: true,
		}}
		plane := &capturingPlane{fakeChangePlane: fakeChangePlane{f}}
		return NewClusterUsecase(store, store, store, f, f, f, NewPlaneUsecase(plane, f, f, f, store, nil), &conf.K8S{}), store, plane
	}
	in := ClusterInput{Name: "prod-1", AccessMode: kc.AccessDirect, APIServer: "https://10.0.0.1:6443", ProxyURL: placeholder}
	cred := &CredentialInput{Token: "new-token"}

	// 地址没变、表单带回占位符：落库的是已保存的原文，不出现占位符。
	uc, store, _ := newUsecase()
	if _, err := uc.Update(ctx, alice, "c1", in, cred, 1); err != nil {
		t.Fatal(err)
	}
	if store.updated == nil || store.updated.ProxyURL != stored {
		t.Fatalf("saved proxy = %+v, want the stored account", store.updated)
	}
	// 任何响应里代理账号仍然打码。
	if masked := MaskProxyURL(store.updated.ProxyURL); masked != placeholder || masked == stored {
		t.Fatalf("the proxy account must still be masked in responses: %q", masked)
	}

	// 换了代理地址、表单带回的还是占位符（即便重填了集群凭据）：拒绝，不落库。
	uc, store, _ = newUsecase()
	changed := in
	changed.ProxyURL = "http://REDACTED@proxy:9090"
	if _, err := uc.Update(ctx, alice, "c1", changed, cred, 1); !isReason(err, v1.ErrorReason_K8S_CLUSTER_INVALID) {
		t.Fatalf("Update(changed address with placeholder) error = %v, want K8S_CLUSTER_INVALID", err)
	}
	if store.updated != nil {
		t.Fatalf("a rejected update must not be written: %+v", store.updated)
	}

	// 换了代理地址并重新填了账号：按输入。
	uc, store, _ = newUsecase()
	changed.ProxyURL = "http://bob:pw2@proxy:9090"
	if _, err := uc.Update(ctx, alice, "c1", changed, cred, 1); err != nil {
		t.Fatal(err)
	}
	if store.updated.ProxyURL != changed.ProxyURL {
		t.Fatalf("saved proxy = %q, want the entered account", store.updated.ProxyURL)
	}

	// 测试连接：同样的口径。占位符 + 新地址不会拿着「REDACTED」去测新代理。
	uc, _, plane := newUsecase()
	// 带上重填的集群凭据：排除「地址变了须重填凭据」那道门，被拒的原因只能是占位符。
	if _, err := uc.Test(ctx, alice, TestInput{ClusterID: "c1", APIServer: in.APIServer, ProxyURL: "http://REDACTED@proxy:9090", Credential: cred}); !isReason(err, v1.ErrorReason_K8S_CLUSTER_INVALID) {
		t.Fatalf("Test(changed address with placeholder) error = %v, want K8S_CLUSTER_INVALID", err)
	}
	if len(plane.tests) != 0 {
		t.Fatalf("a rejected test must not reach the data plane: %+v", plane.tests)
	}
	// 地址没变：发给数据面的是已保存的原文，并沿用已保存的集群凭据。
	if _, err := uc.Test(ctx, alice, TestInput{ClusterID: "c1", APIServer: in.APIServer, ProxyURL: placeholder}); err != nil {
		t.Fatal(err)
	}
	if len(plane.tests) != 1 || plane.tests[0].ProxyURL != stored || plane.tests[0].ClusterID != "c1" {
		t.Fatalf("test requests = %+v", plane.tests)
	}
}
