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
	return NewRoleService(
		biz.NewRoleUsecase(store, store, authorizer),
		biz.NewPermissionUsecase(store, store, authorizer, biznotify.NewNotificationUsecase(store, nil), testfake.NewTxManager()),
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
	ctx := context.Background()
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
