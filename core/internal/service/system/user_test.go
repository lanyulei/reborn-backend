package service

import (
	"context"
	"testing"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/system/v1"
	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"

	"github.com/go-kratos/kratos/v3/errors"
)

func newUserService(t *testing.T) (*UserService, *testfake.Store) {
	t.Helper()
	store := testfake.NewStore()
	hasher, err := testfake.NewHasher()
	if err != nil {
		t.Fatalf("testfake.NewHasher() error = %v", err)
	}
	uc := biz.NewUserUsecase(store, store, store, store, hasher, store)
	return NewUserService(uc), store
}

func TestUserService_CreateUser(t *testing.T) {
	t.Parallel()
	svc, _ := newUserService(t)

	resp, err := svc.CreateUser(context.Background(), &v1.CreateUserRequest{
		Username: "alice",
		Password: "plain-secret",
		Nickname: "Alice",
		Gender:   "female",
	})
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}
	// 未指定 status 默认启用。
	if resp.GetUser().GetStatus() != biz.StatusEnabled {
		t.Fatalf("CreateUser() status = %d, want default enabled", resp.GetUser().GetStatus())
	}
	if resp.GetUser().GetId() == "" || resp.GetUser().GetCreateTime() == nil {
		t.Fatalf("CreateUser() = %+v, want id and create_time filled", resp.GetUser())
	}
}

func TestUserService_CreateUserDuplicate(t *testing.T) {
	t.Parallel()
	svc, _ := newUserService(t)
	ctx := context.Background()
	req := &v1.CreateUserRequest{
		Username: "alice", Password: "plain-secret", Nickname: "Alice", Gender: "unknown",
	}
	if _, err := svc.CreateUser(ctx, req); err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}
	_, err := svc.CreateUser(ctx, req)
	if !errors.Is(err, biz.ErrUserExists) {
		t.Fatalf("CreateUser(duplicate) error = %v, want ErrUserExists", err)
	}
}

func TestUserService_UpdateUser_OptionalPassthrough(t *testing.T) {
	t.Parallel()
	svc, _ := newUserService(t)
	ctx := context.Background()
	created, err := svc.CreateUser(ctx, &v1.CreateUserRequest{
		Username: "alice", Password: "plain-secret", Nickname: "Alice", Gender: "unknown",
	})
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}

	nickname := "Alice Chen"
	if _, err := svc.UpdateUser(ctx, &v1.UpdateUserRequest{
		Id: created.GetUser().GetId(), Nickname: &nickname,
	}); err != nil {
		t.Fatalf("UpdateUser() error = %v", err)
	}
	updated, err := svc.GetUser(ctx, &v1.GetUserRequest{Id: created.GetUser().GetId()})
	if err != nil {
		t.Fatalf("GetUser() error = %v", err)
	}
	if updated.GetUser().GetNickname() != nickname {
		t.Fatalf("nickname = %q, want %q", updated.GetUser().GetNickname(), nickname)
	}
	if updated.GetUser().GetUsername() != "alice" {
		t.Fatalf("username = %q, want unchanged", updated.GetUser().GetUsername())
	}
}

func TestUserService_ListUsers(t *testing.T) {
	t.Parallel()
	svc, _ := newUserService(t)
	ctx := context.Background()
	for _, name := range []string{"alice", "bob"} {
		if _, err := svc.CreateUser(ctx, &v1.CreateUserRequest{
			Username: name, Password: "plain-secret", Nickname: name, Gender: "unknown",
		}); err != nil {
			t.Fatalf("CreateUser(%s) error = %v", name, err)
		}
	}
	resp, err := svc.ListUsers(ctx, &v1.ListUsersRequest{Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("ListUsers() error = %v", err)
	}
	if resp.GetTotal() != 2 || len(resp.GetItems()) != 2 {
		t.Fatalf("ListUsers() total = %d, items = %d, want 2/2", resp.GetTotal(), len(resp.GetItems()))
	}
}

func TestUserService_BindAndListRoles(t *testing.T) {
	t.Parallel()
	userSvc, store := newUserService(t)
	authorizer, err := testfake.NewAuthorizer(store, time.Minute)
	if err != nil {
		t.Fatalf("testfake.NewAuthorizer() error = %v", err)
	}
	roleSvc := NewRoleService(biz.NewRoleUsecase(store, store, authorizer), biz.NewPermissionUsecase(store, store, authorizer, biznotify.NewNotificationUsecase(store, nil), testfake.NewTxManager()), userSvc.uc)
	ctx := context.Background()

	user, err := userSvc.CreateUser(ctx, &v1.CreateUserRequest{
		Username: "alice", Password: "plain-secret", Nickname: "Alice", Gender: "unknown",
	})
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}
	role, err := roleSvc.CreateRole(ctx, &v1.CreateRoleRequest{Name: "管理员", Code: "admin"})
	if err != nil {
		t.Fatalf("CreateRole() error = %v", err)
	}

	if _, err := userSvc.BindUserRoles(ctx, &v1.BindUserRolesRequest{
		Id: user.GetUser().GetId(), RoleIds: []string{role.GetRole().GetId()},
	}); err != nil {
		t.Fatalf("BindUserRoles() error = %v", err)
	}
	roles, err := userSvc.ListUserRoles(ctx, &v1.ListUserRolesRequest{Id: user.GetUser().GetId()})
	if err != nil {
		t.Fatalf("ListUserRoles() error = %v", err)
	}
	if len(roles.GetItems()) != 1 || roles.GetItems()[0].GetCode() != "admin" {
		t.Fatalf("ListUserRoles() = %+v, want one admin role", roles.GetItems())
	}
}

// seedUserRow 直接预置用户行（跳过密码哈希，仅供守卫断言）。
func seedUserRow(t *testing.T, store *testfake.Store, id, username string, admin bool) {
	t.Helper()
	if err := store.CreateUser(context.Background(), &biz.User{
		ID: id, Username: username, Nickname: username, Gender: "unknown", IsAdmin: admin, Status: biz.StatusEnabled,
	}); err != nil {
		t.Fatalf("seed user %s: %v", username, err)
	}
}

// TestUserService_AdminGuards 用户管理写操作的操作者侧守卫：
//   - 普通操作者不能授予/撤销 is_admin（含给自己），不能停用/删除/重置超管；
//   - 超管不能停用/删除自己、不能改自己的 is_admin（值未变的回填放行）；
//   - 缺少身份（无 claims）按无效令牌拒绝。
func TestUserService_AdminGuards(t *testing.T) {
	t.Parallel()
	svc, store := newUserService(t)
	const adminID, opID, bobID = "u-admin", "u-op", "u-bob"
	seedUserRow(t, store, adminID, "root", true)
	seedUserRow(t, store, opID, "operator", false)
	seedUserRow(t, store, bobID, "bob", false)
	opCtx, adminCtx := claimsContext(opID), claimsContext(adminID)
	yes, no := true, false

	denied := func(name string, err error) {
		t.Helper()
		if errors.Code(err) != 403 || errors.Reason(err) != v1.ErrorReason_PERMISSION_DENIED.String() {
			t.Errorf("%s: error = %v, want 403 PERMISSION_DENIED", name, err)
		}
	}
	self := func(name string, err error) {
		t.Helper()
		if !errors.Is(err, biz.ErrUserSelfOperation) {
			t.Errorf("%s: error = %v, want ErrUserSelfOperation", name, err)
		}
	}

	// 普通操作者：提权与触碰超管一律 403。
	_, err := svc.UpdateUser(opCtx, &v1.UpdateUserRequest{Id: opID, IsAdmin: &yes})
	denied("operator grants self is_admin", err)
	_, err = svc.UpdateUser(opCtx, &v1.UpdateUserRequest{Id: bobID, IsAdmin: &yes})
	denied("operator grants other is_admin", err)
	_, err = svc.CreateUser(opCtx, &v1.CreateUserRequest{Username: "evil", Password: "plain-secret", Nickname: "e", Gender: "unknown", IsAdmin: true})
	denied("operator creates admin", err)
	_, err = svc.UpdateUserStatus(opCtx, &v1.UpdateUserStatusRequest{Id: adminID, Status: biz.StatusDisabled})
	denied("operator disables admin", err)
	_, err = svc.DeleteUser(opCtx, &v1.DeleteUserRequest{Id: adminID})
	denied("operator deletes admin", err)
	_, err = svc.ResetUserPassword(opCtx, &v1.ResetUserPasswordRequest{Id: adminID, Password: "new-secret-123"})
	denied("operator resets admin password", err)
	// 普通操作者对普通用户的常规管理不受影响（含改自己昵称）。
	nick := "Op"
	if _, err := svc.UpdateUser(opCtx, &v1.UpdateUserRequest{Id: opID, Nickname: &nick}); err != nil {
		t.Fatalf("operator updates own nickname: %v", err)
	}
	if _, err := svc.UpdateUserStatus(opCtx, &v1.UpdateUserStatusRequest{Id: bobID, Status: biz.StatusDisabled}); err != nil {
		t.Fatalf("operator disables bob: %v", err)
	}

	// 超管：自我锁死路径拒绝；值未变的 is_admin 回填放行。
	_, err = svc.UpdateUserStatus(adminCtx, &v1.UpdateUserStatusRequest{Id: adminID, Status: biz.StatusDisabled})
	self("admin disables self", err)
	_, err = svc.DeleteUser(adminCtx, &v1.DeleteUserRequest{Id: adminID})
	self("admin deletes self", err)
	_, err = svc.UpdateUser(adminCtx, &v1.UpdateUserRequest{Id: adminID, IsAdmin: &no})
	self("admin demotes self", err)
	if _, err := svc.UpdateUser(adminCtx, &v1.UpdateUserRequest{Id: adminID, IsAdmin: &yes}); err != nil {
		t.Fatalf("admin re-submits unchanged is_admin: %v", err)
	}
	if _, err := svc.UpdateUser(adminCtx, &v1.UpdateUserRequest{Id: bobID, IsAdmin: &yes}); err != nil {
		t.Fatalf("admin grants bob is_admin: %v", err)
	}
	if _, err := svc.ResetUserPassword(adminCtx, &v1.ResetUserPasswordRequest{Id: adminID, Password: "new-secret-123"}); err != nil {
		t.Fatalf("admin resets own password: %v", err)
	}
	if _, err := svc.DeleteUser(adminCtx, &v1.DeleteUserRequest{Id: bobID}); err != nil {
		t.Fatalf("admin deletes bob (now admin): %v", err)
	}

	// 目标不存在：404 优先于守卫口径之外的判断。
	if _, err := svc.DeleteUser(adminCtx, &v1.DeleteUserRequest{Id: "missing"}); !errors.Is(err, biz.ErrUserNotFound) {
		t.Fatalf("delete missing: error = %v, want ErrUserNotFound", err)
	}
	// 无身份：拒绝。
	if _, err := svc.DeleteUser(context.Background(), &v1.DeleteUserRequest{Id: opID}); !errors.Is(err, biz.ErrTokenInvalid) {
		t.Fatalf("delete without claims: error = %v, want ErrTokenInvalid", err)
	}
}
