package service

import (
	"context"
	"testing"
	"time"

	v1 "github.com/lanyulei/monitor/backend/core/api/system/v1"
	"github.com/lanyulei/monitor/backend/core/internal/biz"
	"github.com/lanyulei/monitor/backend/core/internal/testfake"

	"github.com/go-kratos/kratos/v3/errors"
)

func newUserService(t *testing.T) (*UserService, *testfake.Store) {
	t.Helper()
	store := testfake.NewStore()
	hasher, err := testfake.NewHasher()
	if err != nil {
		t.Fatalf("testfake.NewHasher() error = %v", err)
	}
	uc := biz.NewUserUsecase(store, store, store, store, hasher)
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
	roleSvc := NewRoleService(biz.NewRoleUsecase(store, store, authorizer), biz.NewPermissionUsecase(store, store, authorizer))
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
