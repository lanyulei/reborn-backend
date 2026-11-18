//go:build integration

package data

import (
	"context"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/k8s"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// 审计与变更单的列表按数据范围过滤（objectScopeWhere）：命名空间授权只见该命名空间；集群级行要求
// 全部命名空间；别的集群不可见；变更单另外总能看到自己提交的。已批准变更单的发起人身份可刷新。
func TestScopedListsAndRequesterRefresh(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	if _, err := e.pool.Exec(ctx, "TRUNCATE TABLE k8s_change"); err != nil {
		t.Fatal(err)
	}
	c1, c2 := e.create("scope-a", kc.AccessAgent, nil), e.create("scope-b", kc.AccessAgent, nil)
	audit := func(clusterID, ns string) {
		id, _ := idgen.NewID()
		if _, err := e.pool.Exec(ctx,
			`INSERT INTO k8s_audit (id, user_id, cluster_id, namespace, resource, name, action, result) VALUES ($1, $2, $3, $4, 'pods', 'p', 'delete', 1)`,
			id, uAlice, clusterID, ns); err != nil {
			t.Fatal(err)
		}
	}
	audit(c1.ID, "team-a")
	audit(c1.ID, "team-b")
	audit(c1.ID, "") // 集群级对象
	audit(c2.ID, "team-a")
	count := func(access biz.Access) int64 {
		_, total, err := e.audits.List(ctx, biz.AuditFilter{Start: time.Now().Add(-time.Hour), End: time.Now().Add(time.Hour), Access: access, Resource: "pods"}, 1, 50)
		if err != nil {
			t.Fatal(err)
		}
		return total
	}
	nsOnly := biz.NewAccess([]*biz.RoleScope{{ClusterID: c1.ID, Namespace: "team-a"}})
	allNS := biz.NewAccess([]*biz.RoleScope{{ClusterID: c1.ID, Namespace: kc.AllNamespaces}})
	if got := count(nsOnly); got != 1 {
		t.Fatalf("namespace-scoped audits = %d, want 1", got)
	}
	if got := count(allNS); got != 3 {
		t.Fatalf("all-namespaces audits on one cluster = %d, want 3", got)
	}
	if got := count(biz.NewAccess(nil)); got != 0 {
		t.Fatalf("no grants: audits = %d, want 0", got)
	}
	if got := count(biz.Access{All: true}); got != 4 {
		t.Fatalf("admin audits = %d, want 4", got)
	}

	changes := NewChangeRepo(e.data)
	mk := func(clusterID, ns, requester string) string {
		id, _ := idgen.NewID()
		c := &biz.Change{ID: id, Kind: kc.ChangeKindOp, ClusterID: clusterID, ClusterName: "x", Namespace: ns, Action: "scale", Op: kc.OpScale,
			Summary: "s", Display: &kc.ChangeDisplay{}, Status: kc.ChangePending, RequesterID: requester, RequesterName: "u"}
		if err := changes.Create(ctx, c, nil); err != nil {
			t.Fatal(err)
		}
		return id
	}
	bob := "018f3e1a-0000-7000-8000-0000000000b0"
	mk(c1.ID, "team-a", uAlice)
	mk(c1.ID, "team-b", uAlice)
	own := mk(c2.ID, "team-z", bob)
	_, total, err := changes.List(ctx, biz.ChangeFilter{Access: &nsOnly, OrRequesterID: bob}, 1, 50)
	if err != nil || total != 2 { // team-a 范围内的一张 + 自己提交的一张
		t.Fatalf("scoped changes = %d, %v; want 2", total, err)
	}

	// 发起人身份刷新：只对已批准的变更单生效，保留用户名。
	if _, err := e.pool.Exec(ctx, `UPDATE k8s_change SET status = $2, approve_time = now(), expire_time = now() + interval '1 hour',
		requester = '{"userId":"u","username":"bob","roleIds":["r1"],"isAdmin":true}' WHERE id = $1`, own, kc.ChangeApproved); err != nil {
		t.Fatal(err)
	}
	items, err := changes.Approved(ctx, 10)
	if err != nil || len(items) != 1 || items[0].ID != own || items[0].RequesterID != bob {
		t.Fatalf("approved = %+v, %v", items, err)
	}
	if err := changes.RefreshRequester(ctx, own, &kc.ChangeRequester{RoleIDs: []string{"r2"}, IsAdmin: false}); err != nil {
		t.Fatal(err)
	}
	var got string
	if err := e.pool.QueryRow(ctx, "SELECT requester::text FROM k8s_change WHERE id = $1", own).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != `{"userId": "u", "isAdmin": false, "roleIds": ["r2"], "username": "bob"}` {
		t.Fatalf("refreshed requester = %s", got)
	}
}
