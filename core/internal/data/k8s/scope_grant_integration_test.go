//go:build integration

package data

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/k8s/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/k8s"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"

	kerrors "github.com/go-kratos/kratos/v3/errors"
	"github.com/jackc/pgx/v5"
)

// 自助授权把一条授权并入角色的数据范围：不动其它条目、同一集群同一命名空间取较高档位、不会降级；与管理端的
// 整组替换走同一把角色锁；并发的多次并入一条都不丢；引用了不存在的集群返回 ErrScopeInvalid 的说明。
func TestScopeRepoGrant(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	c1, c2 := e.create("grant-a", kc.AccessAgent, nil), e.create("grant-b", kc.AccessAgent, nil)
	audit := entry(kc.AuditResourceRoleScope, kc.ActionScopeReplace)
	item := func(clusterID, ns string, level int32) *biz.RoleScope {
		return &biz.RoleScope{ClusterID: clusterID, Namespace: ns, Level: level}
	}
	want := func(roleID string, expect ...*biz.RoleScope) {
		t.Helper()
		got, err := e.scopes.Get(ctx, roleID)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(expect) {
			t.Fatalf("scopes = %+v, want %+v", got, expect)
		}
		for i := range expect {
			if got[i].ClusterID != expect[i].ClusterID || got[i].Namespace != expect[i].Namespace || got[i].Level != expect[i].Level {
				t.Fatalf("scopes = %+v, want %+v", got, expect)
			}
		}
	}

	// 管理端先给了一部分；并入不丢、同条目取较高档位、不降级。
	v0 := e.cfg(kc.CfgRoleScope)
	if err := e.scopes.Replace(ctx, roleA, []*biz.RoleScope{item(c1.ID, "team-a", kc.LevelView)}, audit); err != nil {
		t.Fatal(err)
	}
	for _, it := range []*biz.RoleScope{item(c1.ID, "team-b", kc.LevelEdit), item(c1.ID, "team-a", kc.LevelEdit), item(c1.ID, "team-a", kc.LevelView)} {
		if err := e.scopes.Grant(ctx, roleA, it, audit); err != nil {
			t.Fatal(err)
		}
	}
	want(roleA, item(c1.ID, "team-a", kc.LevelEdit), item(c1.ID, "team-b", kc.LevelEdit))
	if v := e.cfg(kc.CfgRoleScope); v != v0+4 { // 一次整组替换 + 三次并入，每次写入 bump 一次
		t.Fatalf("cfg version = %d, want %d", v, v0+4)
	}

	// 并发并入：一条都不丢。
	const workers = 8
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := range workers {
		wg.Add(1)
		go func(i int) { // 退出路径：Grant 返回即结束
			defer wg.Done()
			errs <- e.scopes.Grant(ctx, roleB, item(c2.ID, fmt.Sprintf("ns-%d", i), kc.LevelView), audit)
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got, err := e.scopes.Get(ctx, roleB); err != nil || len(got) != workers {
		t.Fatalf("concurrent grants: %d scopes, %v; want %d", len(got), err, workers)
	}

	// 与管理端的整组替换互斥：别的写入拿住角色的锁时，并入必须等它提交之后才读出、写回。
	done := make(chan error, 1)
	if err := dbx.WithTx(ctx, e.data.Pool, func(tx pgx.Tx) error {
		if err := lockRoleScope(ctx, tx, roleB); err != nil {
			return err
		}
		go func() { done <- e.scopes.Grant(ctx, roleB, item(c2.ID, "late", kc.LevelView), audit) }()
		select {
		case err := <-done:
			t.Errorf("a grant must wait while another writer holds the role lock: %v", err)
		case <-time.After(300 * time.Millisecond):
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got, err := e.scopes.Get(ctx, roleB); err != nil || len(got) != workers+1 {
		t.Fatalf("after the lock holder committed: %d scopes, %v; want %d", len(got), err, workers+1)
	}

	// 集群不存在：并入失败，已有的数据范围不变。
	err := e.scopes.Grant(ctx, roleA, item("00000000-0000-7000-8000-000000000000", "team-z", kc.LevelView), audit)
	if kerrors.FromError(err).Reason != v1.ErrorReason_K8S_SCOPE_INVALID.String() {
		t.Fatalf("grant on a missing cluster: %v, want K8S_SCOPE_INVALID", err)
	}
	want(roleA, item(c1.ID, "team-a", kc.LevelEdit), item(c1.ID, "team-b", kc.LevelEdit))
}
