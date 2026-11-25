//go:build integration

package data

import (
	"context"
	"slices"
	"testing"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
)

// 启动检查的查询：只选出「启用 + 超管 + 口令列仍是迁移预置串」的账号；改过密码、已停用、非超管、已删除的都不算。
func TestIntegration_PresetPasswordAdmins(t *testing.T) {
	d := newIntegrationData(t)
	repo := NewUserRepo(d)
	ctx := context.Background()
	seed := func(id, username string, admin bool, status int32, password string) {
		t.Helper()
		u := newTestUser(id, username)
		u.IsAdmin, u.Status, u.Password = admin, status, password
		if err := repo.CreateUser(ctx, u); err != nil {
			t.Fatalf("CreateUser(%s) error = %v", username, err)
		}
	}
	preset := biz.PresetAdminPasswordHash
	seed("018f3e1a-0001-7000-8000-0000000000a1", "admin1", true, biz.StatusEnabled, preset)
	seed("018f3e1a-0001-7000-8000-0000000000a2", "admin2", true, biz.StatusEnabled, newTestUser("x", "y2").Password) // 已改密
	seed("018f3e1a-0001-7000-8000-0000000000a3", "admin3", true, biz.StatusDisabled, preset)                         // 已停用
	seed("018f3e1a-0001-7000-8000-0000000000a4", "plain4", false, biz.StatusEnabled, preset)                         // 非超管
	seed("018f3e1a-0001-7000-8000-0000000000a5", "admin5", true, biz.StatusEnabled, preset)                          // 稍后删除
	if err := repo.DeleteUser(ctx, "018f3e1a-0001-7000-8000-0000000000a5"); err != nil {
		t.Fatalf("DeleteUser() error = %v", err)
	}

	got, err := repo.PresetPasswordAdmins(ctx)
	if err != nil || !slices.Equal(got, []string{"admin1"}) {
		t.Fatalf("PresetPasswordAdmins() = %v, %v; want [admin1]", got, err)
	}
	// 改密后不再选中。
	changed := newTestUser("x", "y1").Password
	if err := repo.UpdateUser(ctx, "018f3e1a-0001-7000-8000-0000000000a1", biz.UserUpdate{PasswordHash: &changed}); err != nil {
		t.Fatalf("UpdateUser(password) error = %v", err)
	}
	if got, err := repo.PresetPasswordAdmins(ctx); err != nil || len(got) != 0 {
		t.Fatalf("PresetPasswordAdmins(after change) = %v, %v; want none", got, err)
	}
}
