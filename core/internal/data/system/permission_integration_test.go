//go:build integration

package data

import (
	"context"
	"slices"
	"testing"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
)

// TestIntegration_Permission 权限仓储全链路：直接授权（system_grant）与
// 物化策略（system_permission）单事务全量替换、ListGrantsByRole 三类回读、
// 策略面 JOIN 解析角色 code、软删角色剔除、DeleteRole 级联清两表。
func TestIntegration_Permission(t *testing.T) {
	d := newIntegrationData(t)
	repo := NewPermissionRepo(d)
	roles := NewRoleRepo(d)
	txs := basedata.NewTxManager(d)
	ctx := context.Background()
	// replace 在事务内调用 ReplaceForRole（事务句柄由用例层传入的现行签名）
	replace := func(roleID string, grants biz.GrantSet, operations []string) error {
		return txs.WithinTx(ctx, func(tx txrun.Tx) error {
			return repo.ReplaceForRole(ctx, tx, roleID, grants, operations)
		})
	}

	admin := &biz.Role{ID: "018f3e1a-0100-7000-8000-000000000001", Name: "管理员", Code: "admin", Status: biz.RoleStatusEnabled}
	viewer := &biz.Role{ID: "018f3e1a-0100-7000-8000-000000000002", Name: "查看", Code: "viewer", Status: biz.RoleStatusEnabled}
	for _, r := range []*biz.Role{admin, viewer} {
		if err := roles.CreateRole(ctx, r); err != nil {
			t.Fatalf("CreateRole(%s) error = %v", r.Code, err)
		}
	}

	// 初始空态。
	policies, err := repo.ListPolicies(ctx)
	if err != nil || len(policies) != 0 {
		t.Fatalf("ListPolicies() = %v, %v; want empty", policies, err)
	}
	grants, err := repo.ListGrantsByRole(ctx, admin.ID)
	if err != nil || len(grants.Menus) != 0 || len(grants.Elements) != 0 || len(grants.Apis) != 0 {
		t.Fatalf("ListGrantsByRole() = %+v, %v; want empty", grants, err)
	}

	// 全量替换：菜单 + 直接接口授权；物化策略由调用方给出（仓储按给定操作集原样落库，
	// 展开口径属 biz 层，这里直接给出期望的操作集）。
	listRoles := biz.OperationCode("/system.v1.RoleService/ListRoles")
	listUsers := biz.OperationCode("/system.v1.UserService/ListUsers")
	setRolePerms := biz.OperationCode("/system.v1.RoleService/SetRolePermissions")
	adminGrants := biz.GrantSet{Menus: []string{"system.user"}, Apis: []string{listRoles}}
	operations := []string{listRoles, listUsers}
	slices.Sort(operations)
	if err := replace(admin.ID, adminGrants, operations); err != nil {
		t.Fatalf("ReplaceForRole() error = %v", err)
	}
	grants, err = repo.ListGrantsByRole(ctx, admin.ID)
	if err != nil {
		t.Fatalf("ListGrantsByRole() error = %v", err)
	}
	if !slices.Equal(grants.Menus, []string{"system.user"}) || len(grants.Elements) != 0 ||
		!slices.Equal(grants.Apis, []string{listRoles}) {
		t.Fatalf("ListGrantsByRole() = %+v, want menu system.user + api ListRoles", grants)
	}
	policies, err = repo.ListPolicies(ctx)
	if err != nil {
		t.Fatalf("ListPolicies() error = %v", err)
	}
	if len(policies) != len(operations) {
		t.Fatalf("ListPolicies() = %d 条, want %d (materialized union)", len(policies), len(operations))
	}
	for _, op := range operations {
		if !containsPolicy(policies, "admin", op) {
			t.Fatalf("ListPolicies() missing materialized %s for admin: %+v", op, policies)
		}
	}

	// 策略面：JOIN 解析角色 code，含多角色输出。
	viewerGrants := biz.GrantSet{Elements: []string{"system.role.grant"}}
	viewerOps := []string{setRolePerms}
	if err := replace(viewer.ID, viewerGrants, viewerOps); err != nil {
		t.Fatalf("ReplaceForRole(viewer) error = %v", err)
	}
	policies, err = repo.ListPolicies(ctx)
	if err != nil {
		t.Fatalf("ListPolicies() error = %v", err)
	}
	if !containsPolicy(policies, "admin", listUsers) || !containsPolicy(policies, "admin", listRoles) {
		t.Fatalf("ListPolicies() missing admin entries: %+v", policies)
	}
	if !containsPolicy(policies, "viewer", setRolePerms) {
		t.Fatalf("ListPolicies() missing viewer element expansion: %+v", policies)
	}

	// 覆盖语义：第二次替换后旧授权（菜单）消失，仅剩新集合。
	apiOnly := biz.GrantSet{Apis: []string{listRoles}}
	if err := replace(admin.ID, apiOnly, []string{listRoles}); err != nil {
		t.Fatalf("ReplaceForRole(replace) error = %v", err)
	}
	grants, err = repo.ListGrantsByRole(ctx, admin.ID)
	if err != nil || len(grants.Menus) != 0 || !slices.Equal(grants.Apis, []string{listRoles}) {
		t.Fatalf("ListGrantsByRole(replaced) = %+v, %v; want api-only", grants, err)
	}

	// DeleteRole 级联：直接授权与策略同时清理。
	if err := roles.DeleteRole(ctx, viewer.ID); err != nil {
		t.Fatalf("DeleteRole(viewer) error = %v", err)
	}
	if grants, err = repo.ListGrantsByRole(ctx, viewer.ID); err != nil || !emptyGrants(grants) {
		t.Fatalf("ListGrantsByRole(after role delete) = %+v, %v; want empty", grants, err)
	}
	if policies, err = repo.ListPolicies(ctx); err != nil || len(policies) != 1 {
		t.Fatalf("ListPolicies(after role delete) = %+v, %v; want admin-only", policies, err)
	}

	// 清空。
	if err := replace(admin.ID, biz.GrantSet{}, nil); err != nil {
		t.Fatalf("ReplaceForRole(clear) error = %v", err)
	}
	if grants, err = repo.ListGrantsByRole(ctx, admin.ID); err != nil || !emptyGrants(grants) {
		t.Fatalf("ListGrantsByRole(cleared) = %+v, %v; want empty", grants, err)
	}
	if policies, err = repo.ListPolicies(ctx); err != nil || len(policies) != 0 {
		t.Fatalf("ListPolicies(cleared) = %+v, %v; want empty", policies, err)
	}
}

// emptyGrants 报告三类直接授权是否均为空（GrantSet 含切片字段不可 ==）。
func emptyGrants(g biz.GrantSet) bool {
	return len(g.Menus) == 0 && len(g.Elements) == 0 && len(g.Apis) == 0
}

// containsPolicy 报告策略集合是否含 (角色 code, 操作 code) 精确条目。
func containsPolicy(policies []biz.PermissionPolicy, roleCode, operation string) bool {
	return slices.ContainsFunc(policies, func(p biz.PermissionPolicy) bool {
		return p.RoleCode == roleCode && p.Operation == operation
	})
}
