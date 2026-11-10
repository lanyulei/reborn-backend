//go:build integration

package data

import (
	"context"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"

	"github.com/go-kratos/kratos/v3/errors"
)

func newTestRole(id, code string) *biz.Role {
	return &biz.Role{
		ID:         id,
		Name:       "Role-" + code,
		Code:       code,
		Status:     biz.RoleStatusEnabled,
		CreateTime: time.Now().UTC(),
		UpdateTime: time.Now().UTC(),
	}
}

func TestIntegration_RoleCRUD(t *testing.T) {
	d := newIntegrationData(t)
	repo := NewRoleRepo(d)
	ctx := context.Background()

	role := newTestRole("018f3e1a-0002-7000-8000-000000000001", "admin")
	if err := repo.CreateRole(ctx, role); err != nil {
		t.Fatalf("CreateRole() error = %v", err)
	}

	// code 冲突映射。
	dup := newTestRole("018f3e1a-0002-7000-8000-000000000002", "admin")
	if err := repo.CreateRole(ctx, dup); !errors.Is(err, biz.ErrRoleCodeExists) {
		t.Fatalf("CreateRole(dup code) error = %v, want ErrRoleCodeExists", err)
	}

	got, err := repo.GetRole(ctx, role.ID)
	if err != nil {
		t.Fatalf("GetRole() error = %v", err)
	}
	if got.Code != "admin" || got.Status != biz.RoleStatusEnabled {
		t.Fatalf("GetRole() = %+v", got)
	}
	if _, err := repo.GetRole(ctx, missingID); !errors.Is(err, biz.ErrRoleNotFound) {
		t.Fatalf("GetRole(missing) error = %v, want ErrRoleNotFound", err)
	}

	// 部分更新。
	name := "超级管理员"
	status := biz.RoleStatusDisabled
	if err := repo.UpdateRole(ctx, role.ID, biz.RoleUpdate{Name: &name, Status: &status}); err != nil {
		t.Fatalf("UpdateRole() error = %v", err)
	}
	got, err = repo.GetRole(ctx, role.ID)
	if err != nil {
		t.Fatalf("GetRole() error = %v", err)
	}
	if got.Name != name || got.Status != status {
		t.Fatalf("UpdateRole() result = %+v", got)
	}
	if err := repo.UpdateRole(ctx, missingID, biz.RoleUpdate{Name: &name}); !errors.Is(err, biz.ErrRoleNotFound) {
		t.Fatalf("UpdateRole(missing) error = %v, want ErrRoleNotFound", err)
	}

	// 列表过滤 + ByIDs。
	another := newTestRole("018f3e1a-0002-7000-8000-000000000003", "viewer")
	if err := repo.CreateRole(ctx, another); err != nil {
		t.Fatalf("CreateRole(viewer) error = %v", err)
	}
	roles, total, err := repo.ListRoles(ctx, biz.RoleFilter{}, 1, 10)
	if err != nil {
		t.Fatalf("ListRoles() error = %v", err)
	}
	if total != 2 || len(roles) != 2 {
		t.Fatalf("ListRoles() total = %d, len = %d", total, len(roles))
	}
	roles, total, err = repo.ListRoles(ctx, biz.RoleFilter{Keyword: "view"}, 1, 10)
	if err != nil {
		t.Fatalf("ListRoles(keyword) error = %v", err)
	}
	if total != 1 || roles[0].Code != "viewer" {
		t.Fatalf("ListRoles(keyword) = %d %+v", total, roles)
	}
	enabled := biz.RoleStatusEnabled
	_, total, err = repo.ListRoles(ctx, biz.RoleFilter{Status: &enabled}, 1, 10)
	if err != nil {
		t.Fatalf("ListRoles(status) error = %v", err)
	}
	if total != 1 {
		t.Fatalf("ListRoles(enabled) total = %d, want 1", total)
	}

	// ByIDs：命中去重 + 缺失不计。
	byIDs, err := repo.ListRolesByIDs(ctx, []string{role.ID, another.ID, role.ID, missingID})
	if err != nil {
		t.Fatalf("ListRolesByIDs() error = %v", err)
	}
	if len(byIDs) != 2 {
		t.Fatalf("ListRolesByIDs() = %+v, want 2 roles", byIDs)
	}
	if byIDs, err = repo.ListRolesByIDs(ctx, nil); err != nil || len(byIDs) != 0 {
		t.Fatalf("ListRolesByIDs(nil) = %+v, err = %v", byIDs, err)
	}

	// 删除（软删）后不可见。
	if err := repo.DeleteRole(ctx, another.ID); err != nil {
		t.Fatalf("DeleteRole() error = %v", err)
	}
	if _, err := repo.GetRole(ctx, another.ID); !errors.Is(err, biz.ErrRoleNotFound) {
		t.Fatalf("GetRole(deleted) error = %v, want ErrRoleNotFound", err)
	}
	if err := repo.DeleteRole(ctx, another.ID); !errors.Is(err, biz.ErrRoleNotFound) {
		t.Fatalf("DeleteRole(deleted) error = %v, want ErrRoleNotFound", err)
	}
}

func TestIntegration_UserRoleBinding(t *testing.T) {
	d := newIntegrationData(t)
	users := NewUserRepo(d)
	roles := NewRoleRepo(d)
	bindings := NewUserRoleRepo(d)
	ctx := context.Background()

	user := newTestUser("018f3e1a-0003-7000-8000-000000000001", "alice")
	admin := newTestRole("018f3e1a-0003-7000-8000-000000000002", "admin")
	viewer := newTestRole("018f3e1a-0003-7000-8000-000000000003", "viewer")
	for _, create := range []func() error{
		func() error { return users.CreateUser(ctx, user) },
		func() error { return roles.CreateRole(ctx, admin) },
		func() error { return roles.CreateRole(ctx, viewer) },
	} {
		if err := create(); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	// 全量覆盖绑定。
	if err := bindings.BindUserRoles(ctx, user.ID, []string{admin.ID, viewer.ID}); err != nil {
		t.Fatalf("BindUserRoles() error = %v", err)
	}
	bound, err := bindings.ListUserRoles(ctx, user.ID)
	if err != nil {
		t.Fatalf("ListUserRoles() error = %v", err)
	}
	if len(bound) != 2 || bound[0].Code != "admin" || bound[1].Code != "viewer" {
		t.Fatalf("ListUserRoles() = %+v, want [admin viewer]", bound)
	}

	// 再绑定：旧绑定物理删除，仅剩新集合。
	if err := bindings.BindUserRoles(ctx, user.ID, []string{viewer.ID}); err != nil {
		t.Fatalf("BindUserRoles(rebind) error = %v", err)
	}
	bound, err = bindings.ListUserRoles(ctx, user.ID)
	if err != nil {
		t.Fatalf("ListUserRoles() error = %v", err)
	}
	if len(bound) != 1 || bound[0].Code != "viewer" {
		t.Fatalf("ListUserRoles(rebind) = %+v, want [viewer]", bound)
	}

	// 空集合清空绑定。
	if err := bindings.BindUserRoles(ctx, user.ID, nil); err != nil {
		t.Fatalf("BindUserRoles(clear) error = %v", err)
	}
	bound, err = bindings.ListUserRoles(ctx, user.ID)
	if err != nil {
		t.Fatalf("ListUserRoles(clear) error = %v", err)
	}
	if len(bound) != 0 {
		t.Fatalf("ListUserRoles(clear) = %+v, want empty", bound)
	}

	// 重新绑定后删除用户：绑定级联物理删除。
	if err := bindings.BindUserRoles(ctx, user.ID, []string{admin.ID}); err != nil {
		t.Fatalf("BindUserRoles() error = %v", err)
	}
	if err := users.DeleteUser(ctx, user.ID); err != nil {
		t.Fatalf("DeleteUser() error = %v", err)
	}
	bound, err = bindings.ListUserRoles(ctx, user.ID)
	if err != nil {
		t.Fatalf("ListUserRoles(after user delete) error = %v", err)
	}
	if len(bound) != 0 {
		t.Fatalf("bindings survived user deletion: %+v", bound)
	}

	// 删除角色：其全部绑定物理删除。
	user2 := newTestUser("018f3e1a-0003-7000-8000-000000000004", "bob")
	if err := users.CreateUser(ctx, user2); err != nil {
		t.Fatalf("CreateUser(bob) error = %v", err)
	}
	if err := bindings.BindUserRoles(ctx, user2.ID, []string{admin.ID}); err != nil {
		t.Fatalf("BindUserRoles(bob) error = %v", err)
	}
	if err := roles.DeleteRole(ctx, admin.ID); err != nil {
		t.Fatalf("DeleteRole() error = %v", err)
	}
	bound, err = bindings.ListUserRoles(ctx, user2.ID)
	if err != nil {
		t.Fatalf("ListUserRoles(after role delete) error = %v", err)
	}
	if len(bound) != 0 {
		t.Fatalf("bindings survived role deletion: %+v", bound)
	}
}
