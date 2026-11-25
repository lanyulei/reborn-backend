package biz

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	bizsystem "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"
)

// selfRolesEnv 用内存仓储装配自助授权角色适配器，并预置一个用户与他已有的两个角色。
func selfRolesEnv(t *testing.T) (k8sSelfRoles, *bizsystem.UserUsecase, *testfake.Store, string) {
	t.Helper()
	ctx := context.Background()
	store := testfake.NewStore()
	const userID = "018f3e1a-00aa-7000-8000-000000000001"
	now := time.Now().UTC()
	if err := store.CreateUser(ctx, &bizsystem.User{ID: userID, Username: "alice", Status: bizsystem.StatusEnabled, CreateTime: now, UpdateTime: now}); err != nil {
		t.Fatal(err)
	}
	for _, code := range []string{"dev", "ops"} {
		if err := store.CreateRole(ctx, &bizsystem.Role{ID: "role-" + code, Name: code, Code: code, Status: bizsystem.RoleStatusEnabled}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.BindUserRoles(ctx, userID, []string{"role-dev", "role-ops"}); err != nil {
		t.Fatal(err)
	}
	users := bizsystem.NewUserUsecase(store, store, store, store, nil, store)
	roles := bizsystem.NewRoleUsecase(store, store, nil)
	return k8sSelfRoles{roles: roles, users: users}, users, store, userID
}

func boundCodes(t *testing.T, store *testfake.Store, userID string) []string {
	t.Helper()
	bound, err := store.ListUserRoles(context.Background(), userID)
	if err != nil {
		t.Fatal(err)
	}
	codes := make([]string, 0, len(bound))
	for _, r := range bound {
		codes = append(codes, r.Code)
	}
	return codes
}

// Ensure 幂等：第一次创建并绑定专属角色，之后复用同一个角色、不重复绑定；用户原有的角色绑定始终都在。
func TestK8sSelfRolesEnsureIdempotent(t *testing.T) {
	t.Parallel()
	s, _, store, userID := selfRolesEnv(t)
	ctx := context.Background()

	first, err := s.Ensure(ctx, userID, "alice")
	if err != nil || first == "" {
		t.Fatalf("Ensure() = %q, %v", first, err)
	}
	for range 3 {
		again, err := s.Ensure(ctx, userID, "alice")
		if err != nil || again != first {
			t.Fatalf("Ensure(again) = %q, %v, want %q", again, err, first)
		}
	}
	want := []string{"dev", K8sSelfRoleCode(userID), "ops"}
	if got := boundCodes(t, store, userID); !slices.Equal(got, want) {
		t.Fatalf("bound roles = %v, want %v", got, want)
	}
}

// 并发的首次 Ensure 与同时发生的其它角色追加互不覆盖：只建出一个专属角色，所有绑定一条不丢。
// （整组替换的旧写法在这里会丢：各自读到的「现有绑定」里没有对方刚写的那条。）
func TestK8sSelfRolesEnsureConcurrentKeepsBindings(t *testing.T) {
	t.Parallel()
	s, users, store, userID := selfRolesEnv(t)
	ctx := context.Background()

	const n = 8
	extra := make([]string, 0, n)
	for i := range n {
		code := fmt.Sprintf("extra-%d", i)
		if err := store.CreateRole(ctx, &bizsystem.Role{ID: "role-" + code, Name: code, Code: code, Status: bizsystem.RoleStatusEnabled}); err != nil {
			t.Fatal(err)
		}
		extra = append(extra, code)
	}

	ids := make([]string, n)
	errs := make([]error, 2*n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range n {
		wg.Go(func() {
			<-start
			ids[i], errs[i] = s.Ensure(ctx, userID, "alice")
		})
		wg.Go(func() {
			<-start
			errs[n+i] = users.AddRole(ctx, userID, "role-"+extra[i])
		})
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("call %d error = %v", i, err)
		}
	}
	for _, id := range ids {
		if id == "" || id != ids[0] {
			t.Fatalf("Ensure() ids = %v, want one shared role", ids)
		}
	}
	want := append([]string{"dev", "ops", K8sSelfRoleCode(userID)}, extra...)
	slices.Sort(want)
	if got := boundCodes(t, store, userID); !slices.Equal(got, want) {
		t.Fatalf("bound roles = %v, want %v", got, want)
	}
}

// 专属角色被停用时不绑定、不报成功；用户不存在时返回错误。
func TestK8sSelfRolesEnsureRejects(t *testing.T) {
	t.Parallel()
	s, _, store, userID := selfRolesEnv(t)
	ctx := context.Background()

	if _, err := s.Ensure(ctx, "018f3e1a-00aa-7000-8000-0000000000ff", "ghost"); err == nil {
		t.Fatal("Ensure(missing user) error = nil")
	}
	if err := store.CreateRole(ctx, &bizsystem.Role{ID: "role-self", Name: "self", Code: K8sSelfRoleCode(userID), Status: bizsystem.RoleStatusDisabled}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Ensure(ctx, userID, "alice"); err == nil {
		t.Fatal("Ensure(disabled role) error = nil")
	}
	if got := boundCodes(t, store, userID); !slices.Equal(got, []string{"dev", "ops"}) {
		t.Fatalf("bound roles = %v, want the original two", got)
	}
}
