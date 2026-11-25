//go:build integration

package data

import (
	"context"
	"fmt"
	"slices"
	"testing"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
)

// 批量插入与逐行插入写出同样的行：三类授权按上限行数（64 / 128 / 256）全量替换后逐类回读一致、
// 物化策略行数一致、每行主键各不相同；同一次保存里有重复资源码时撞唯一索引、整体回滚（原授权不变）。
func TestIntegration_ReplaceForRoleBatch(t *testing.T) {
	d := newIntegrationData(t)
	repo := NewPermissionRepo(d)
	txs := basedata.NewTxManager(d)
	ctx := context.Background()
	role := &biz.Role{ID: "018f3e1a-0100-7000-8000-0000000000b1", Name: "批量", Code: "batch", Status: biz.RoleStatusEnabled}
	if err := NewRoleRepo(d).CreateRole(ctx, role); err != nil {
		t.Fatalf("CreateRole() error = %v", err)
	}
	replace := func(grants biz.GrantSet, operations []string) error {
		return txs.WithinTx(ctx, func(tx txrun.Tx) error {
			return repo.ReplaceForRole(ctx, tx, role.ID, grants, operations)
		})
	}
	codes := func(prefix string, n int) []string {
		out := make([]string, 0, n)
		for i := range n {
			out = append(out, fmt.Sprintf("%s.%04d", prefix, i))
		}
		return out
	}
	want := biz.GrantSet{Menus: codes("menu", 64), Elements: codes("element", 128), Apis: codes("api", 256)}
	if err := replace(want, want.Apis); err != nil {
		t.Fatalf("ReplaceForRole() error = %v", err)
	}
	got, err := repo.ListGrantsByRole(ctx, role.ID)
	if err != nil {
		t.Fatalf("ListGrantsByRole() error = %v", err)
	}
	slices.Sort(got.Menus)
	slices.Sort(got.Elements)
	slices.Sort(got.Apis)
	if !slices.Equal(got.Menus, want.Menus) || !slices.Equal(got.Elements, want.Elements) || !slices.Equal(got.Apis, want.Apis) {
		t.Fatalf("grants read back differ: %d/%d/%d", len(got.Menus), len(got.Elements), len(got.Apis))
	}
	var grantRows, grantIDs, permRows, permIDs int
	if err := d.Pool.QueryRow(ctx,
		`SELECT (SELECT count(*) FROM system_grant WHERE role_id = $1::uuid), (SELECT count(DISTINCT id) FROM system_grant WHERE role_id = $1::uuid),
		        (SELECT count(*) FROM system_permission WHERE role_id = $1::uuid), (SELECT count(DISTINCT id) FROM system_permission WHERE role_id = $1::uuid)`,
		role.ID).Scan(&grantRows, &grantIDs, &permRows, &permIDs); err != nil {
		t.Fatal(err)
	}
	if grantRows != 448 || grantIDs != 448 || permRows != 256 || permIDs != 256 {
		t.Fatalf("rows grant %d (ids %d), permission %d (ids %d)", grantRows, grantIDs, permRows, permIDs)
	}

	// 重复资源码：唯一索引冲突，整体回滚。
	if err := replace(biz.GrantSet{Menus: []string{"dup", "dup"}}, nil); err == nil {
		t.Fatal("ReplaceForRole(duplicate codes) error = nil, want unique violation")
	}
	after, err := repo.ListGrantsByRole(ctx, role.ID)
	if err != nil || len(after.Menus) != 64 || len(after.Elements) != 128 || len(after.Apis) != 256 {
		t.Fatalf("grants after failed replace = %d/%d/%d, %v", len(after.Menus), len(after.Elements), len(after.Apis), err)
	}
}
