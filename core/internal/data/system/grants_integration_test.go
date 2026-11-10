//go:build integration

package data

import (
	"context"
	"slices"
	"testing"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
)

// TestIntegration_ListGrantsByRoles 多角色直接授权单条查询并集：DISTINCT 去重
// + 字典序，空输入不查库返回空集。
func TestIntegration_ListGrantsByRoles(t *testing.T) {
	d := newIntegrationData(t)
	roles := NewRoleRepo(d)
	perms := NewPermissionRepo(d)
	txs := basedata.NewTxManager(d)
	ctx := context.Background()

	ops := newTestRole("018f3e1a-0005-7000-8000-000000000001", "ops")
	dev := newTestRole("018f3e1a-0005-7000-8000-000000000002", "dev")
	for _, r := range []*biz.Role{ops, dev} {
		if err := roles.CreateRole(ctx, r); err != nil {
			t.Fatalf("seed role %s: %v", r.Code, err)
		}
	}
	seed := func(roleID string, grants biz.GrantSet) {
		t.Helper()
		if err := txs.WithinTx(ctx, func(tx txrun.Tx) error {
			return perms.ReplaceForRole(ctx, tx, roleID, grants, grants.Apis)
		}); err != nil {
			t.Fatalf("ReplaceForRole(%s): %v", roleID, err)
		}
	}
	seed(ops.ID, biz.GrantSet{Menus: []string{"system.user", "system.role"}, Elements: []string{"system.user.create"}, Apis: []string{"ffff0000000000000000000000000001"}})
	seed(dev.ID, biz.GrantSet{Menus: []string{"system.role", "notify.channel"}, Elements: []string{"system.user.create", "notify.channel.test"}})

	got, err := perms.ListGrantsByRoles(ctx, []string{ops.ID, dev.ID})
	if err != nil {
		t.Fatalf("ListGrantsByRoles() error = %v", err)
	}
	wantMenus := []string{"notify.channel", "system.role", "system.user"}
	wantElements := []string{"notify.channel.test", "system.user.create"}
	if !slices.Equal(got.Menus, wantMenus) || !slices.Equal(got.Elements, wantElements) ||
		!slices.Equal(got.Apis, []string{"ffff0000000000000000000000000001"}) {
		t.Fatalf("ListGrantsByRoles() = %+v, want menus %v elements %v", got, wantMenus, wantElements)
	}
	// 单角色结果与 ListGrantsByRole 一致。
	single, err := perms.ListGrantsByRole(ctx, dev.ID)
	if err != nil {
		t.Fatalf("ListGrantsByRole() error = %v", err)
	}
	batch, err := perms.ListGrantsByRoles(ctx, []string{dev.ID})
	if err != nil {
		t.Fatalf("ListGrantsByRoles(single) error = %v", err)
	}
	if !slices.Equal(single.Menus, batch.Menus) || !slices.Equal(single.Elements, batch.Elements) {
		t.Fatalf("single %+v != batch %+v", single, batch)
	}
	if empty, err := perms.ListGrantsByRoles(ctx, nil); err != nil || len(empty.Menus)+len(empty.Elements)+len(empty.Apis) != 0 {
		t.Fatalf("ListGrantsByRoles(nil) = %+v, %v; want empty", empty, err)
	}
}

// TestIntegration_KeywordLikeEscaped 关键字中的 LIKE 元字符按字面匹配：`_`
// 不再匹配任意单字符，`%` 不再命中全表。
func TestIntegration_KeywordLikeEscaped(t *testing.T) {
	d := newIntegrationData(t)
	users := NewUserRepo(d)
	ctx := context.Background()
	for i, name := range []string{"alice", "bob_x", "bobyx", "pct%name"} {
		u := newTestUser("018f3e1a-0006-7000-8000-00000000000"+string(rune('1'+i)), name)
		u.Phone = "1390000000" + string(rune('1'+i)) // 工厂按用户名末字符派生手机号，此处显式唯一
		if err := users.CreateUser(ctx, u); err != nil {
			t.Fatalf("seed user %s: %v", name, err)
		}
	}
	cases := map[string][]string{
		"_":   {"bob_x"},
		"%":   {"pct%name"},
		"bob": {"bob_x", "bobyx"},
	}
	for keyword, want := range cases {
		got, total, err := users.ListUsers(ctx, biz.UserFilter{Keyword: keyword}, 1, 10)
		if err != nil {
			t.Fatalf("ListUsers(%q) error = %v", keyword, err)
		}
		names := make([]string, 0, len(got))
		for _, u := range got {
			names = append(names, u.Username)
		}
		slices.Sort(names)
		if total != int64(len(want)) || !slices.Equal(names, want) {
			t.Errorf("ListUsers(%q) = %v (total %d), want %v", keyword, names, total, want)
		}
	}
}
