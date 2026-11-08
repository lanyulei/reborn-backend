package biz

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

// failingUserReader 让 GetUser 还认工故障，验证 fail closed。
type failingUserReader struct{ userStoreStub }

func (s *failingUserReader) GetUser(context.Context, string) (*User, error) {
	return nil, errors.New("boom")
}

// newAuthzUsecase 装配判定用例：用户/绑定/直接授权用 userStoreStub，
// 策略走真实 Authorizer。
func newAuthzUsecase(t *testing.T, stub *userStoreStub, policies []PermissionPolicy) *AuthzUsecase {
	t.Helper()
	src := &policyStoreStub{policies: policies}
	authorizer := newTestAuthorizer(t, src, time.Minute)
	return NewAuthzUsecase(stub, stub, stub, authorizer)
}

// TestAuthzUsecase_AdminBypass is_admin 超级管理员跳过接口权限认证：
// 无任何角色与策略也放行。
func TestAuthzUsecase_AdminBypass(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	stub := newUserStoreStub()
	stub.users["root"] = &User{ID: "root", Username: "root", IsAdmin: true}
	uc := newAuthzUsecase(t, stub, nil)

	allowed, err := uc.Allowed(ctx, "root", "/system.v1.UserService/DeleteUser")
	if err != nil {
		t.Fatalf("Allowed(admin) error = %v", err)
	}
	if !allowed {
		t.Fatal("Allowed(admin) = false, want true")
	}
}

// TestAuthzUsecase_DeniedByDefault 普通用户无策略一律拒绝（deny-by-default）。
func TestAuthzUsecase_DeniedByDefault(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	stub := newUserStoreStub()
	stub.users["alice"] = &User{ID: "alice", Username: "alice"}
	stub.roles["r1"] = &Role{ID: "r1", Code: "ops", Status: RoleStatusEnabled}
	stub.bindings["alice"] = []string{"r1"}
	uc := newAuthzUsecase(t, stub, nil)

	allowed, err := uc.Allowed(ctx, "alice", "/system.v1.UserService/ListUsers")
	if err != nil {
		t.Fatalf("Allowed() error = %v", err)
	}
	if allowed {
		t.Fatal("Allowed(no policy) = true, want false")
	}
}

// TestAuthzUsecase_MultiRoleUnion 多角色并集：任一启用角色命中即放行，
// 未授权操作仍拒绝（一用户多角色的显式需求）。
func TestAuthzUsecase_MultiRoleUnion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	stub := newUserStoreStub()
	stub.users["alice"] = &User{ID: "alice", Username: "alice"}
	stub.roles["r1"] = &Role{ID: "r1", Code: "user_viewer", Status: RoleStatusEnabled}
	stub.roles["r2"] = &Role{ID: "r2", Code: "role_viewer", Status: RoleStatusEnabled}
	stub.bindings["alice"] = []string{"r1", "r2"}
	uc := newAuthzUsecase(t, stub, []PermissionPolicy{
		{RoleCode: "user_viewer", Operation: "/system.v1.UserService/ListUsers"},
		{RoleCode: "role_viewer", Operation: "/system.v1.RoleService/ListRoles"},
	})

	for _, op := range []string{
		"/system.v1.UserService/ListUsers", // 角色 1 授权
		"/system.v1.RoleService/ListRoles", // 角色 2 授权（并集）
	} {
		if allowed, err := uc.Allowed(ctx, "alice", op); err != nil || !allowed {
			t.Errorf("Allowed(%s) = %v, %v; want true, nil", op, allowed, err)
		}
	}
	if allowed, err := uc.Allowed(ctx, "alice", "/system.v1.UserService/CreateUser"); err != nil || allowed {
		t.Errorf("Allowed(ungranted) = %v, %v; want false, nil", allowed, err)
	}
}

// TestAuthzUsecase_DisabledRoleSkipped 停用角色的授权不生效：
// status=0 的角色即使策略命中也跳过。
func TestAuthzUsecase_DisabledRoleSkipped(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	stub := newUserStoreStub()
	stub.users["alice"] = &User{ID: "alice", Username: "alice"}
	stub.roles["r1"] = &Role{ID: "r1", Code: "ops", Status: RoleStatusDisabled}
	stub.bindings["alice"] = []string{"r1"}
	uc := newAuthzUsecase(t, stub, []PermissionPolicy{
		{RoleCode: "ops", Operation: "/system.v1.UserService/ListUsers"},
	})

	if allowed, err := uc.Allowed(ctx, "alice", "/system.v1.UserService/ListUsers"); err != nil || allowed {
		t.Fatalf("Allowed(disabled role) = %v, %v; want false, nil", allowed, err)
	}
}

// TestAuthzUsecase_UserNotFoundDenied 用户不存在（含已删）：明确拒绝而非报错
// （令牌有效但主体已消失的窗口由授权收口）。
func TestAuthzUsecase_UserNotFoundDenied(t *testing.T) {
	t.Parallel()
	stub := newUserStoreStub()
	uc := newAuthzUsecase(t, stub, nil)

	allowed, err := uc.Allowed(context.Background(), "ghost", "/system.v1.UserService/ListUsers")
	if err != nil {
		t.Fatalf("Allowed(missing user) error = %v", err)
	}
	if allowed {
		t.Fatal("Allowed(missing user) = true, want false")
	}
}

// TestAuthzUsecase_StoreErrorFailsClosed 仓储故障必须返回错误（中间件 500），
// 绝不放行。
func TestAuthzUsecase_StoreErrorFailsClosed(t *testing.T) {
	t.Parallel()
	stub := &failingUserReader{}
	src := &policyStoreStub{}
	uc := NewAuthzUsecase(stub, stub, stub, newTestAuthorizer(t, src, time.Minute))

	allowed, err := uc.Allowed(context.Background(), "anyone", "/system.v1.UserService/ListUsers")
	if err == nil {
		t.Fatal("Allowed(store failure) error = nil, want non-nil")
	}
	if allowed {
		t.Fatal("Allowed(store failure) = true, want false (fail closed)")
	}
}

// TestAuthzUsecase_EffectiveOperations 有效授权并集：多启用角色去重并集
// （字典序）、停用角色排除、is_admin 与用户不存在均返回空集（与 Allowed
// 同口径，供 /me 暴露给前端）。
func TestAuthzUsecase_EffectiveOperations(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	stub := newUserStoreStub()
	stub.users["alice"] = &User{ID: "alice", Username: "alice"}
	stub.roles["r1"] = &Role{ID: "r1", Code: "user_viewer", Status: RoleStatusEnabled}
	stub.roles["r2"] = &Role{ID: "r2", Code: "ops", Status: RoleStatusEnabled}
	stub.roles["r3"] = &Role{ID: "r3", Code: "disabled_role", Status: RoleStatusDisabled}
	stub.bindings["alice"] = []string{"r1", "r2", "r3"}
	uc := newAuthzUsecase(t, stub, []PermissionPolicy{
		{RoleCode: "user_viewer", Operation: "/system.v1.UserService/ListUsers"},
		{RoleCode: "ops", Operation: "/system.v1.RoleService/ListRoles"},
		{RoleCode: "disabled_role", Operation: "/system.v1.UserService/DeleteUser"},
	})

	got, err := uc.EffectiveOperations(ctx, "alice")
	if err != nil {
		t.Fatalf("EffectiveOperations() error = %v", err)
	}
	want := []string{"/system.v1.RoleService/ListRoles", "/system.v1.UserService/ListUsers"}
	if !slices.Equal(got, want) {
		t.Fatalf("EffectiveOperations() = %v, want %v", got, want)
	}

	// is_admin 超管直通无需枚举；用户不存在（含已软删）空集而非报错。
	adminStub := newUserStoreStub()
	adminStub.users["root"] = &User{ID: "root", Username: "root", IsAdmin: true}
	adminUC := newAuthzUsecase(t, adminStub, nil)
	if got, err := adminUC.EffectiveOperations(ctx, "root"); err != nil || len(got) != 0 {
		t.Fatalf("EffectiveOperations(admin) = %v, %v; want empty, nil", got, err)
	}
	if got, err := adminUC.EffectiveOperations(ctx, "ghost"); err != nil || len(got) != 0 {
		t.Fatalf("EffectiveOperations(missing user) = %v, %v; want empty, nil", got, err)
	}
}
