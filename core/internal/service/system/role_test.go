package service

import (
	"context"
	"slices"
	"testing"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/system/v1"
	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"

	"github.com/go-kratos/kratos/v3/errors"
)

func newRoleService(t *testing.T) (*RoleService, *testfake.Store) {
	t.Helper()
	store := testfake.NewStore()
	authorizer, err := testfake.NewAuthorizer(store, time.Minute)
	if err != nil {
		t.Fatalf("testfake.NewAuthorizer() error = %v", err)
	}
	hasher, err := testfake.NewHasher()
	if err != nil {
		t.Fatalf("testfake.NewHasher() error = %v", err)
	}
	return NewRoleService(
		biz.NewRoleUsecase(store, store, authorizer),
		biz.NewPermissionUsecase(store, store, authorizer, biznotify.NewNotificationUsecase(store, nil), testfake.NewTxManager()),
		biz.NewUserUsecase(store, store, store, store, hasher, store),
	), store
}

func TestRoleService_CreateRole(t *testing.T) {
	t.Parallel()
	svc, _ := newRoleService(t)

	resp, err := svc.CreateRole(context.Background(), &v1.CreateRoleRequest{Name: "管理员", Code: "admin"})
	if err != nil {
		t.Fatalf("CreateRole() error = %v", err)
	}
	if resp.GetRole().GetStatus() != biz.RoleStatusEnabled {
		t.Fatalf("CreateRole() status = %d, want default enabled", resp.GetRole().GetStatus())
	}
	if resp.GetRole().GetId() == "" {
		t.Fatal("CreateRole() id is empty")
	}
}

func TestRoleService_CreateRoleDuplicateCode(t *testing.T) {
	t.Parallel()
	svc, _ := newRoleService(t)
	ctx := context.Background()
	if _, err := svc.CreateRole(ctx, &v1.CreateRoleRequest{Name: "管理员", Code: "admin"}); err != nil {
		t.Fatalf("CreateRole() error = %v", err)
	}
	_, err := svc.CreateRole(ctx, &v1.CreateRoleRequest{Name: "管理员2", Code: "admin"})
	if !errors.Is(err, biz.ErrRoleCodeExists) {
		t.Fatalf("CreateRole(duplicate) error = %v, want ErrRoleCodeExists", err)
	}
}

func TestRoleService_UpdateAndGetRole(t *testing.T) {
	t.Parallel()
	svc, _ := newRoleService(t)
	ctx := context.Background()
	created, err := svc.CreateRole(ctx, &v1.CreateRoleRequest{Name: "管理员", Code: "admin"})
	if err != nil {
		t.Fatalf("CreateRole() error = %v", err)
	}

	name := "超级管理员"
	if _, err := svc.UpdateRole(ctx, &v1.UpdateRoleRequest{Id: created.GetRole().GetId(), Name: &name}); err != nil {
		t.Fatalf("UpdateRole() error = %v", err)
	}
	got, err := svc.GetRole(ctx, &v1.GetRoleRequest{Id: created.GetRole().GetId()})
	if err != nil {
		t.Fatalf("GetRole() error = %v", err)
	}
	if got.GetRole().GetName() != name || got.GetRole().GetCode() != "admin" {
		t.Fatalf("GetRole() = %+v, want updated name and immutable code", got.GetRole())
	}
}

// TestRoleService_Permissions 权限委托链：Set（未知菜单码/接口码 400 且不
// 落库）→ Get 回读直接授权 → 级联物化策略 → ListOperations 枚举与 proto
// 常量一致。
func TestRoleService_Permissions(t *testing.T) {
	t.Parallel()
	svc, store := newRoleService(t)
	seedUserRow(t, store, "u-admin", "root", true)
	ctx := claimsContext("u-admin")
	created, err := svc.CreateRole(ctx, &v1.CreateRoleRequest{Name: "运维", Code: "ops"})
	if err != nil {
		t.Fatalf("CreateRole() error = %v", err)
	}
	roleID := created.GetRole().GetId()

	// 未登记菜单码 → GRANT_INVALID，且不落库。
	if _, err := svc.SetRolePermissions(ctx, &v1.SetRolePermissionsRequest{
		Id:    roleID,
		Menus: []string{"system.nope"},
	}); !errors.Is(err, biz.ErrGrantInvalid) {
		t.Fatalf("SetRolePermissions(unknown menu) error = %v, want ErrGrantInvalid", err)
	}
	// 未枚举接口 code（含格式合法但不在目录内的）→ OPERATION_INVALID，且不落库。
	unknownCode := biz.OperationCode("/system.v1.UserService/Nope")
	if _, err := svc.SetRolePermissions(ctx, &v1.SetRolePermissionsRequest{
		Id:   roleID,
		Apis: []string{biz.OperationCode(v1.OperationUserServiceListUsers), unknownCode},
	}); !errors.Is(err, ErrOperationInvalid) {
		t.Fatalf("SetRolePermissions(unknown op) error = %v, want ErrOperationInvalid", err)
	}
	got, err := svc.GetRolePermissions(ctx, &v1.GetRolePermissionsRequest{Id: roleID})
	if err != nil {
		t.Fatalf("GetRolePermissions() error = %v", err)
	}
	if len(got.GetMenus()) != 0 || len(got.GetElements()) != 0 || len(got.GetApis()) != 0 {
		t.Fatalf("rejected Set leaked into store: %+v", got)
	}

	// 合法三类直接授权：去重后落库，回读一致。
	listUsers := biz.OperationCode(v1.OperationUserServiceListUsers)
	if _, err := svc.SetRolePermissions(ctx, &v1.SetRolePermissionsRequest{
		Id:       roleID,
		Menus:    []string{"system.user", "system.user"},
		Elements: []string{"system.role.grant"},
		Apis:     []string{listUsers, listUsers},
	}); err != nil {
		t.Fatalf("SetRolePermissions() error = %v", err)
	}
	got, err = svc.GetRolePermissions(ctx, &v1.GetRolePermissionsRequest{Id: roleID})
	if err != nil {
		t.Fatalf("GetRolePermissions() error = %v", err)
	}
	if !slices.Equal(got.GetMenus(), []string{"system.user"}) ||
		!slices.Equal(got.GetElements(), []string{"system.role.grant"}) ||
		!slices.Equal(got.GetApis(), []string{listUsers}) {
		t.Fatalf("GetRolePermissions() = %+v, want deduped direct grants", got)
	}

	// 物化口径：策略仅来自 apis 直接授权（Enforce 即刻可见）；菜单/元素
	// 授权不级联物化（SetRolePermissions 属 system.role.grant 元素，未直接
	// 授予 apis，不得出现在策略中）。
	policies, err := store.ListPolicies(ctx)
	if err != nil {
		t.Fatalf("ListPolicies() error = %v", err)
	}
	if len(policies) != 1 || policies[0].RoleCode != "ops" || policies[0].Operation != listUsers {
		t.Fatalf("policies = %+v, want exactly one [ops %s] (apis only, no cascade)", policies, listUsers)
	}
	cascadeOp := biz.OperationCode(v1.OperationRoleServiceSetRolePermissions)
	if slices.ContainsFunc(policies, func(p biz.PermissionPolicy) bool { return p.Operation == cascadeOp }) {
		t.Fatalf("element grant materialized policy %s, want no cascade", cascadeOp)
	}

	// 角色不存在 → 404 哨兵透传。
	if _, err := svc.GetRolePermissions(ctx, &v1.GetRolePermissionsRequest{Id: "018f3e1a-0000-7000-8000-000000000001"}); !errors.Is(err, biz.ErrRoleNotFound) {
		t.Fatalf("GetRolePermissions(missing role) error = %v, want ErrRoleNotFound", err)
	}

	// 操作全集：与 service 层枚举一致（抽查新增接口在内，且带中文名）。
	listed, err := svc.ListOperations(ctx, &v1.ListOperationsRequest{})
	if err != nil {
		t.Fatalf("ListOperations() error = %v", err)
	}
	idx := slices.IndexFunc(listed.GetOperations(), func(op *v1.Operation) bool {
		return op.GetName() == v1.OperationUserServiceCreateUser
	})
	if idx < 0 {
		t.Fatalf("ListOperations() missing %s", v1.OperationUserServiceCreateUser)
	}
	gotOp := listed.GetOperations()[idx]
	if gotOp.GetTitle() != "创建用户" {
		t.Fatalf("ListOperations() title = %q, want 创建用户", gotOp.GetTitle())
	}
	if gotOp.GetCode() != biz.OperationCode(v1.OperationUserServiceCreateUser) {
		t.Fatalf("ListOperations() code = %q, want md5(name)", gotOp.GetCode())
	}
}

// TestRoleService_BuiltinGuards 内置角色写操作守卫：编辑 / 授权仅超管（普通操作者
// 403 PERMISSION_DENIED，缺少身份按无效令牌拒绝）；删除对任何人返回
// ROLE_BUILTIN_PROTECTED；非内置角色不受影响。
func TestRoleService_BuiltinGuards(t *testing.T) {
	t.Parallel()
	svc, store := newRoleService(t)
	const adminID, opID = "u-admin", "u-op"
	seedUserRow(t, store, adminID, "root", true)
	seedUserRow(t, store, opID, "operator", false)
	if err := store.CreateRole(context.Background(), &biz.Role{ID: "role-default", Name: "默认角色", Code: "default", Builtin: true}); err != nil {
		t.Fatalf("seed builtin role: %v", err)
	}
	opCtx, adminCtx := claimsContext(opID), claimsContext(adminID)
	name := "默认角色-改"

	denied := func(step string, err error) {
		t.Helper()
		if errors.Code(err) != 403 || errors.Reason(err) != v1.ErrorReason_PERMISSION_DENIED.String() {
			t.Errorf("%s: error = %v, want 403 PERMISSION_DENIED", step, err)
		}
	}

	// 普通操作者：编辑 / 授权被拒。
	_, err := svc.UpdateRole(opCtx, &v1.UpdateRoleRequest{Id: "role-default", Name: &name})
	denied("UpdateRole(builtin, operator)", err)
	_, err = svc.SetRolePermissions(opCtx, &v1.SetRolePermissionsRequest{Id: "role-default", Menus: []string{"system.user"}})
	denied("SetRolePermissions(builtin, operator)", err)
	// 缺少身份：无效令牌。
	if _, err := svc.UpdateRole(context.Background(), &v1.UpdateRoleRequest{Id: "role-default", Name: &name}); !errors.Is(err, biz.ErrTokenInvalid) {
		t.Errorf("UpdateRole(builtin, no claims) error = %v, want ErrTokenInvalid", err)
	}
	// 超管：编辑 / 授权通过。
	if _, err := svc.UpdateRole(adminCtx, &v1.UpdateRoleRequest{Id: "role-default", Name: &name}); err != nil {
		t.Fatalf("UpdateRole(builtin, admin) error = %v", err)
	}
	if _, err := svc.SetRolePermissions(adminCtx, &v1.SetRolePermissionsRequest{Id: "role-default", Menus: []string{"system.user"}}); err != nil {
		t.Fatalf("SetRolePermissions(builtin, admin) error = %v", err)
	}
	got, err := svc.GetRole(adminCtx, &v1.GetRoleRequest{Id: "role-default"})
	if err != nil || got.GetRole().GetName() != name || !got.GetRole().GetBuiltin() {
		t.Fatalf("GetRole(builtin) = %+v, err = %v; want renamed + builtin=true", got.GetRole(), err)
	}
	// 删除：超管同样被拒，且角色仍在。
	if _, err := svc.DeleteRole(adminCtx, &v1.DeleteRoleRequest{Id: "role-default"}); !errors.Is(err, biz.ErrRoleBuiltin) {
		t.Fatalf("DeleteRole(builtin, admin) error = %v, want ErrRoleBuiltin", err)
	}
	if _, err := svc.GetRole(adminCtx, &v1.GetRoleRequest{Id: "role-default"}); err != nil {
		t.Fatalf("builtin role vanished after rejected delete: %v", err)
	}

	// 非内置角色：普通操作者（接口级 RBAC 由中间件负责，service 不加额外守卫）可编辑 / 删除。
	created, err := svc.CreateRole(opCtx, &v1.CreateRoleRequest{Name: "运维", Code: "ops"})
	if err != nil {
		t.Fatalf("CreateRole() error = %v", err)
	}
	if _, err := svc.UpdateRole(opCtx, &v1.UpdateRoleRequest{Id: created.GetRole().GetId(), Name: &name}); err != nil {
		t.Fatalf("UpdateRole(normal, operator) error = %v", err)
	}
	if _, err := svc.DeleteRole(opCtx, &v1.DeleteRoleRequest{Id: created.GetRole().GetId()}); err != nil {
		t.Fatalf("DeleteRole(normal, operator) error = %v", err)
	}
}

// TestRoleService_GrantCeiling 授权上限守卫：非超管只能授予自己已持有的
// 菜单/元素/接口授权与 biz 范围（目标角色已有项可保留）；超管不受限；
// 缺少身份按无效令牌拒绝。
func TestRoleService_GrantCeiling(t *testing.T) {
	t.Parallel()
	svc, store := newRoleService(t)
	const adminID, opID = "u-admin", "u-op"
	seedUserRow(t, store, adminID, "root", true)
	seedUserRow(t, store, opID, "operator", false)
	opCtx, adminCtx := claimsContext(opID), claimsContext(adminID)
	listUsers := biz.OperationCode(v1.OperationUserServiceListUsers)
	createUser := biz.OperationCode(v1.OperationUserServiceCreateUser)

	mine, err := svc.CreateRole(adminCtx, &v1.CreateRoleRequest{Name: "运维", Code: "ops", BizScope: []string{"a", "b"}})
	if err != nil {
		t.Fatalf("CreateRole() error = %v", err)
	}
	mineID := mine.GetRole().GetId()
	if _, err := svc.SetRolePermissions(adminCtx, &v1.SetRolePermissionsRequest{
		Id: mineID, Menus: []string{"system.user"}, Apis: []string{listUsers},
	}); err != nil {
		t.Fatalf("SetRolePermissions(admin) error = %v", err)
	}
	if err := store.BindUserRoles(context.Background(), opID, []string{mineID}); err != nil {
		t.Fatalf("bind operator role: %v", err)
	}

	denied := func(step string, err error) {
		t.Helper()
		if errors.Code(err) != 403 || errors.Reason(err) != v1.ErrorReason_PERMISSION_DENIED.String() {
			t.Errorf("%s: error = %v, want 403 PERMISSION_DENIED", step, err)
		}
	}

	// 给自己所在角色追加未持有的接口/菜单/元素 → 403。
	_, err = svc.SetRolePermissions(opCtx, &v1.SetRolePermissionsRequest{Id: mineID, Menus: []string{"system.user"}, Apis: []string{listUsers, createUser}})
	denied("operator adds api", err)
	_, err = svc.SetRolePermissions(opCtx, &v1.SetRolePermissionsRequest{Id: mineID, Menus: []string{"system.user", "system.role"}})
	denied("operator adds menu", err)
	_, err = svc.SetRolePermissions(opCtx, &v1.SetRolePermissionsRequest{Id: mineID, Elements: []string{"system.role.grant"}})
	denied("operator adds element", err)
	// 持有范围内的授予与收缩放行。
	other, err := svc.CreateRole(adminCtx, &v1.CreateRoleRequest{Name: "只读", Code: "ro", BizScope: []string{"a"}})
	if err != nil {
		t.Fatalf("CreateRole() error = %v", err)
	}
	otherID := other.GetRole().GetId()
	if _, err := svc.SetRolePermissions(opCtx, &v1.SetRolePermissionsRequest{Id: otherID, Menus: []string{"system.user"}, Apis: []string{listUsers}}); err != nil {
		t.Fatalf("operator grants within ceiling: %v", err)
	}
	// 目标角色已有的超限项可保留。
	if _, err := svc.SetRolePermissions(adminCtx, &v1.SetRolePermissionsRequest{Id: otherID, Apis: []string{listUsers, createUser}}); err != nil {
		t.Fatalf("SetRolePermissions(admin) error = %v", err)
	}
	if _, err := svc.SetRolePermissions(opCtx, &v1.SetRolePermissionsRequest{Id: otherID, Apis: []string{createUser}}); err != nil {
		t.Fatalf("operator keeps existing grant: %v", err)
	}
	// 缺少身份：无效令牌。
	if _, err := svc.SetRolePermissions(context.Background(), &v1.SetRolePermissionsRequest{Id: otherID}); !errors.Is(err, biz.ErrTokenInvalid) {
		t.Errorf("SetRolePermissions(no claims) error = %v, want ErrTokenInvalid", err)
	}

	// biz 范围：扩到自身范围之外或清空（=不限）→ 403；范围内放行。
	_, err = svc.UpdateRole(opCtx, &v1.UpdateRoleRequest{Id: otherID, BizScope: &v1.BizScope{Items: []string{"a", "c"}}})
	denied("operator widens biz scope", err)
	_, err = svc.UpdateRole(opCtx, &v1.UpdateRoleRequest{Id: mineID, BizScope: &v1.BizScope{}})
	denied("operator clears biz scope", err)
	if _, err := svc.UpdateRole(opCtx, &v1.UpdateRoleRequest{Id: otherID, BizScope: &v1.BizScope{Items: []string{"a", "b"}}}); err != nil {
		t.Fatalf("operator sets biz scope within own: %v", err)
	}
	if _, err := svc.UpdateRole(adminCtx, &v1.UpdateRoleRequest{Id: otherID, BizScope: &v1.BizScope{}}); err != nil {
		t.Fatalf("admin clears biz scope: %v", err)
	}
}
