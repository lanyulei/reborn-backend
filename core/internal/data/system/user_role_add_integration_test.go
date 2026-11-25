//go:build integration

package data

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"

	rootbiz "github.com/lanyulei/reborn/backend/core/internal/biz"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
)

// boundRoleCodes 用户当前绑定的角色标识（按 code 升序）。
func boundRoleCodes(t *testing.T, repo *userRoleRepo, userID string) []string {
	t.Helper()
	bound, err := repo.ListUserRoles(context.Background(), userID)
	if err != nil {
		t.Fatalf("ListUserRoles() error = %v", err)
	}
	codes := make([]string, 0, len(bound))
	for _, r := range bound {
		codes = append(codes, r.Code)
	}
	return codes
}

// 追加一条绑定：其它绑定不动、重复追加不报错也不多出一行；用户或角色不存在 / 已软删时不写入；
// 并发追加不同角色一条都不丢（单语句 + uk_system_user_role）。
func TestIntegration_UserRoleAdd(t *testing.T) {
	d := newIntegrationData(t)
	users := NewUserRepo(d)
	roles := NewRoleRepo(d)
	bindings := NewUserRoleRepo(d)
	ctx := context.Background()

	user := newTestUser("018f3e1a-0031-7000-8000-000000000001", "alice")
	gone := newTestUser("018f3e1a-0031-7000-8000-000000000002", "bob")
	admin := newTestRole("018f3e1a-0031-7000-8000-000000000011", "admin")
	viewer := newTestRole("018f3e1a-0031-7000-8000-000000000012", "viewer")
	dropped := newTestRole("018f3e1a-0031-7000-8000-000000000013", "dropped")
	for _, seed := range []func() error{
		func() error { return users.CreateUser(ctx, user) },
		func() error { return users.CreateUser(ctx, gone) },
		func() error { return roles.CreateRole(ctx, admin) },
		func() error { return roles.CreateRole(ctx, viewer) },
		func() error { return roles.CreateRole(ctx, dropped) },
		func() error { return bindings.BindUserRoles(ctx, user.ID, []string{admin.ID}) },
		func() error { return users.DeleteUser(ctx, gone.ID) },
		func() error { return roles.DeleteRole(ctx, dropped.ID) },
	} {
		if err := seed(); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	for range 3 {
		if err := bindings.AddUserRole(ctx, user.ID, viewer.ID); err != nil {
			t.Fatalf("AddUserRole() error = %v", err)
		}
	}
	if err := bindings.AddUserRole(ctx, user.ID, admin.ID); err != nil { // 已绑定
		t.Fatalf("AddUserRole(bound) error = %v", err)
	}
	if got := boundRoleCodes(t, bindings, user.ID); !slices.Equal(got, []string{"admin", "viewer"}) {
		t.Fatalf("bound = %v, want [admin viewer]", got)
	}

	// 不存在 / 已软删的用户或角色：不写入、不报错。
	for _, pair := range [][2]string{{user.ID, missingID}, {missingID, viewer.ID}, {user.ID, dropped.ID}, {gone.ID, viewer.ID}} {
		if err := bindings.AddUserRole(ctx, pair[0], pair[1]); err != nil {
			t.Fatalf("AddUserRole(%v) error = %v", pair, err)
		}
	}
	var rows int
	if err := d.Pool.QueryRow(ctx, "SELECT count(*) FROM system_user_role").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 2 {
		t.Fatalf("system_user_role rows = %d, want 2", rows)
	}

	// 并发：16 个角色各被追加两次。
	const n = 16
	want := []string{"admin", "viewer"}
	ids := make([]string, n)
	for i := range n {
		r := newTestRole(fmt.Sprintf("018f3e1a-0031-7000-8000-0000000001%02x", i), fmt.Sprintf("extra-%02d", i))
		if err := roles.CreateRole(ctx, r); err != nil {
			t.Fatalf("seed role: %v", err)
		}
		ids[i] = r.ID
		want = append(want, r.Code)
	}
	errs := make([]error, 2*n)
	var wg sync.WaitGroup
	for i := range 2 * n {
		wg.Go(func() { errs[i] = bindings.AddUserRole(ctx, user.ID, ids[i%n]) })
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatalf("concurrent AddUserRole() error = %v", err)
		}
	}
	slices.Sort(want)
	if got := boundRoleCodes(t, bindings, user.ID); !slices.Equal(got, want) {
		t.Fatalf("bound = %v, want %v", got, want)
	}
}

// 多实例同时为同一用户首次做自助授权（Ensure），同时另有角色被追加给他：只建出一个专属角色，
// 用户原有的绑定与并发追加的绑定一条不丢。
func TestIntegration_K8sSelfRolesEnsureConcurrent(t *testing.T) {
	d := newIntegrationData(t)
	userRepo := NewUserRepo(d)
	roleRepo := NewRoleRepo(d)
	bindings := NewUserRoleRepo(d)
	ctx := context.Background()

	userUC := biz.NewUserUsecase(userRepo, userRepo, bindings, roleRepo, nil, nil)
	self := rootbiz.NewK8sSelfRoles(biz.NewRoleUsecase(roleRepo, roleRepo, nil), userUC)

	user := newTestUser("018f3e1a-0032-7000-8000-000000000001", "alice")
	if err := userRepo.CreateUser(ctx, user); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	const n = 8
	want := []string{rootbiz.K8sSelfRoleCode(user.ID)}
	pre := make([]string, 0, 2)
	extra := make([]string, 0, n)
	for i := range n + 2 {
		r := newTestRole(fmt.Sprintf("018f3e1a-0032-7000-8000-0000000001%02x", i), fmt.Sprintf("role-%02d", i))
		if err := roleRepo.CreateRole(ctx, r); err != nil {
			t.Fatalf("seed role: %v", err)
		}
		want = append(want, r.Code)
		if i < 2 {
			pre = append(pre, r.ID)
		} else {
			extra = append(extra, r.ID)
		}
	}
	if err := bindings.BindUserRoles(ctx, user.ID, pre); err != nil {
		t.Fatalf("seed bindings: %v", err)
	}

	got := make([]string, n)
	errs := make([]error, 2*n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range n {
		wg.Go(func() {
			<-start
			got[i], errs[i] = self.Ensure(ctx, user.ID, user.Username)
		})
		wg.Go(func() {
			<-start
			errs[n+i] = userUC.AddRole(ctx, user.ID, extra[i])
		})
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("call %d error = %v", i, err)
		}
	}
	for _, id := range got {
		if id == "" || id != got[0] {
			t.Fatalf("Ensure() ids = %v, want one shared role", got)
		}
	}
	var selfRoles int
	if err := d.Pool.QueryRow(ctx, "SELECT count(*) FROM system_role WHERE code = $1::text AND delete_time IS NULL",
		rootbiz.K8sSelfRoleCode(user.ID)).Scan(&selfRoles); err != nil {
		t.Fatal(err)
	}
	if selfRoles != 1 {
		t.Fatalf("self-service roles = %d, want 1", selfRoles)
	}
	slices.Sort(want)
	if codes := boundRoleCodes(t, bindings, user.ID); !slices.Equal(codes, want) {
		t.Fatalf("bound = %v, want %v", codes, want)
	}
	// 再来一次仍是同一个角色，绑定不变。
	if id, err := self.Ensure(ctx, user.ID, user.Username); err != nil || id != got[0] {
		t.Fatalf("Ensure(again) = %q, %v, want %q", id, err, got[0])
	}
	if codes := boundRoleCodes(t, bindings, user.ID); !slices.Equal(codes, want) {
		t.Fatalf("bound after repeat = %v, want %v", codes, want)
	}
}
