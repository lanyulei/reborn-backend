package biz

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v3/errors"
)

// adminPermStub 记录管理面调用并派生策略面（ListPolicies 从 byRole 生成），
// 保证 SetGrants 后 Reload 读到与写路径一致的数据。
type adminPermStub struct {
	roles    map[string]string   // roleID -> code（解析策略 subject）
	byRole   map[string][]string // roleID -> 物化 operations
	grants   map[string]GrantSet // roleID -> 直接授权
	replaced []replacedRecord    // 每次 ReplaceForRole 的入参快照
}

// replacedRecord 记录一次全量替换的直接授权与物化策略，供断言。
type replacedRecord struct {
	grants     GrantSet
	operations []string
}

func (s *adminPermStub) ListPolicies(context.Context) ([]PermissionPolicy, error) {
	out := make([]PermissionPolicy, 0, 8)
	for roleID, ops := range s.byRole {
		for _, op := range ops {
			out = append(out, PermissionPolicy{RoleCode: s.roles[roleID], Operation: op})
		}
	}
	slices.SortFunc(out, func(a, b PermissionPolicy) int {
		if c := len(a.RoleCode) - len(b.RoleCode); c != 0 {
			return c
		}
		return 0
	})
	return out, nil
}

func (s *adminPermStub) ListGrantsByRole(_ context.Context, roleID string) (GrantSet, error) {
	g := s.grants[roleID]
	return GrantSet{
		Menus:    slices.Clone(g.Menus),
		Elements: slices.Clone(g.Elements),
		Apis:     slices.Clone(g.Apis),
	}, nil
}

func (s *adminPermStub) ReplaceForRole(_ context.Context, roleID string, grants GrantSet, operations []string) error {
	s.replaced = append(s.replaced, replacedRecord{
		grants: GrantSet{
			Menus:    slices.Clone(grants.Menus),
			Elements: slices.Clone(grants.Elements),
			Apis:     slices.Clone(grants.Apis),
		},
		operations: slices.Clone(operations),
	})
	if s.byRole == nil {
		s.byRole = map[string][]string{}
	}
	if s.grants == nil {
		s.grants = map[string]GrantSet{}
	}
	s.byRole[roleID] = slices.Clone(operations)
	s.grants[roleID] = GrantSet{
		Menus:    slices.Clone(grants.Menus),
		Elements: slices.Clone(grants.Elements),
		Apis:     slices.Clone(grants.Apis),
	}
	return nil
}

func newPermissionUsecase(t *testing.T, stub *userStoreStub, admin *adminPermStub) (*PermissionUsecase, *Authorizer) {
	t.Helper()
	authorizer := newTestAuthorizer(t, admin, time.Minute)
	return NewPermissionUsecase(stub, admin, authorizer), authorizer
}

// TestPermissionUsecase_GetGrants 角色不存在透传 404；存在时委托仓储回读。
func TestPermissionUsecase_GetGrants(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	stub := newUserStoreStub()
	stub.roles["r1"] = &Role{ID: "r1", Code: "ops"}
	admin := &adminPermStub{
		roles:  map[string]string{"r1": "ops"},
		grants: map[string]GrantSet{"r1": {Menus: []string{"system.user"}}},
	}
	uc, _ := newPermissionUsecase(t, stub, admin)

	if _, err := uc.GetGrants(ctx, "missing"); !errors.Is(err, ErrRoleNotFound) {
		t.Fatalf("GetGrants(missing role) error = %v, want ErrRoleNotFound", err)
	}
	got, err := uc.GetGrants(ctx, "r1")
	if err != nil || !slices.Equal(got.Menus, []string{"system.user"}) {
		t.Fatalf("GetGrants() = %+v, %v; want menus [system.user]", got, err)
	}
}

// TestPermissionUsecase_SetGrants 全量覆盖 + 物化口径语义：
// 菜单/页面元素授权只落可见性（system_grant），不产生任何接口授权；
// 接口策略（system_permission）仅与 apis 直接授权一一对应；未知菜单/元素码
// 拒绝且不落库；apis 授权后本实例 Enforce 即刻可见；空集清空。
func TestPermissionUsecase_SetGrants(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	stub := newUserStoreStub()
	stub.roles["r1"] = &Role{ID: "r1", Code: "ops"}
	admin := &adminPermStub{roles: map[string]string{"r1": "ops"}}
	uc, authorizer := newPermissionUsecase(t, stub, admin)

	// 角色不存在：不触碰仓储。
	if err := uc.SetGrants(ctx, "missing", GrantSet{Menus: []string{"system.user"}}); !errors.Is(err, ErrRoleNotFound) {
		t.Fatalf("SetGrants(missing role) error = %v, want ErrRoleNotFound", err)
	}
	if len(admin.replaced) != 0 {
		t.Fatalf("SetGrants(missing role) touched store: %v", admin.replaced)
	}

	// 未登记菜单码 → ErrGrantInvalid，且不落库。
	if err := uc.SetGrants(ctx, "r1", GrantSet{Menus: []string{"system.nope"}}); !errors.Is(err, ErrGrantInvalid) {
		t.Fatalf("SetGrants(unknown menu) error = %v, want ErrGrantInvalid", err)
	}
	if err := uc.SetGrants(ctx, "r1", GrantSet{Elements: []string{"system.user.nope"}}); !errors.Is(err, ErrGrantInvalid) {
		t.Fatalf("SetGrants(unknown element) error = %v, want ErrGrantInvalid", err)
	}
	if len(admin.replaced) != 0 {
		t.Fatalf("rejected SetGrants leaked into store: %v", admin.replaced)
	}

	// 菜单 + 元素授权：可见性去重排序落库，物化策略必须为空（不级联）。
	if err := uc.SetGrants(ctx, "r1", GrantSet{
		Menus:    []string{"system.user", "system.role", "system.role"},
		Elements: []string{"system.user.create"},
	}); err != nil {
		t.Fatalf("SetGrants(visibility) error = %v", err)
	}
	rec := admin.replaced[0]
	if !slices.Equal(rec.grants.Menus, []string{"system.role", "system.user"}) {
		t.Fatalf("SetGrants() persisted menus %v, want deduped+sorted", rec.grants.Menus)
	}
	if !slices.Equal(rec.grants.Elements, []string{"system.user.create"}) {
		t.Fatalf("SetGrants() persisted elements %v", rec.grants.Elements)
	}
	if len(rec.operations) != 0 {
		t.Fatalf("SetGrants(visibility) operations = %v, want empty (menus/elements must not grant APIs)", rec.operations)
	}
	// 可见性授权不放行任何接口（deny-by-default 仍生效）。
	ok, err := authorizer.Enforce(ctx, "ops", OperationCode("/system.v1.UserService/ListUsers"))
	if err != nil || ok {
		t.Fatalf("Enforce after visibility-only grants = %v, %v; want false, nil", ok, err)
	}

	// apis 直接授权：物化策略与 apis 一一对应（去重排序），本实例即刻生效。
	if err := uc.SetGrants(ctx, "r1", GrantSet{
		Menus: []string{"system.user"},
		Apis: []string{
			"ffff0000000000000000000000000002",
			"ffff0000000000000000000000000001",
			"ffff0000000000000000000000000002",
		},
	}); err != nil {
		t.Fatalf("SetGrants(apis) error = %v", err)
	}
	apiRec := admin.replaced[1]
	if !slices.Equal(apiRec.operations, []string{
		"ffff0000000000000000000000000001",
		"ffff0000000000000000000000000002",
	}) {
		t.Fatalf("SetGrants(apis) operations = %v, want apis only", apiRec.operations)
	}
	if ok, _ := authorizer.Enforce(ctx, "ops", "ffff0000000000000000000000000001"); !ok {
		t.Fatal("Enforce after api grant = false, want true")
	}

	// 清空语义：空集合落库且策略不再命中。
	if err := uc.SetGrants(ctx, "r1", GrantSet{}); err != nil {
		t.Fatalf("SetGrants(clear) error = %v", err)
	}
	if ok, _ := authorizer.Enforce(ctx, "ops", "ffff0000000000000000000000000001"); ok {
		t.Fatal("Enforce after clear = true, want false")
	}
}
