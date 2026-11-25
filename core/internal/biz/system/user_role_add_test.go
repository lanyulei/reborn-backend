package biz

import (
	"context"
	"errors"
	"slices"
	"testing"
)

// AddUserRole 桩：追加一条绑定，已绑定则不动（与 data 层的 ON CONFLICT DO NOTHING 同口径）。
func (s *userStoreStub) AddUserRole(_ context.Context, userID, roleID string) error {
	if !slices.Contains(s.bindings[userID], roleID) {
		s.bindings[userID] = append(s.bindings[userID], roleID)
	}
	return nil
}

// AddRole 只追加：既有绑定原样保留、重复追加幂等；用户或角色不存在返回对应哨兵且不写入；
// 不吊销会话（与 BindRoles 的副作用口径一致）。
func TestUserUsecase_AddRole(t *testing.T) {
	t.Parallel()
	uc, stub := newUserUsecase(t)
	ctx := context.Background()

	user, err := uc.Create(ctx, &User{Username: "alice", Password: "plain-secret", Gender: GenderUnknown})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	r1, r2 := &Role{ID: "role-1", Code: "a"}, &Role{ID: "role-2", Code: "b"}
	stub.roles[r1.ID], stub.roles[r2.ID] = r1, r2
	if err := uc.BindRoles(ctx, user.ID, []string{r1.ID}); err != nil {
		t.Fatalf("BindRoles() error = %v", err)
	}

	if err := uc.AddRole(ctx, "missing", r2.ID); !errors.Is(err, ErrUserNotFound) {
		t.Fatalf("AddRole(missing user) error = %v, want ErrUserNotFound", err)
	}
	if err := uc.AddRole(ctx, user.ID, "missing"); !errors.Is(err, ErrRoleNotFound) {
		t.Fatalf("AddRole(missing role) error = %v, want ErrRoleNotFound", err)
	}
	if got := stub.bindings[user.ID]; !slices.Equal(got, []string{r1.ID}) {
		t.Fatalf("bindings after rejected adds = %v, want [role-1]", got)
	}

	for range 3 { // 幂等：重复追加不重复、不动其它绑定
		if err := uc.AddRole(ctx, user.ID, r2.ID); err != nil {
			t.Fatalf("AddRole() error = %v", err)
		}
	}
	if got := stub.bindings[user.ID]; !slices.Equal(got, []string{r1.ID, r2.ID}) {
		t.Fatalf("bindings = %v, want [role-1 role-2]", got)
	}
	if err := uc.AddRole(ctx, user.ID, r1.ID); err != nil { // 已绑定的角色
		t.Fatalf("AddRole(bound) error = %v", err)
	}
	if got := stub.bindings[user.ID]; !slices.Equal(got, []string{r1.ID, r2.ID}) {
		t.Fatalf("bindings = %v, want [role-1 role-2]", got)
	}
	if len(stub.revoked) != 0 {
		t.Fatalf("revoked = %v, want no session revocation", stub.revoked)
	}
}
