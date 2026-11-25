package biz

import (
	"context"
	"os"
	"strings"
	"testing"
)

// presetAdminPlain 是迁移 0001 注释里写明的内置超管初始口令（公开的种子数据，不是任何环境的凭据）。
const presetAdminPlain = "Admin123!"

// seedPresetAdmin 预置一个与迁移种子行同形态的内置超管（口令列即预置 PHC 串）。
func seedPresetAdmin(stub *userStoreStub) *User {
	u := &User{ID: "user-admin", Username: "admin", Password: PresetAdminPasswordHash, IsAdmin: true, Status: StatusEnabled}
	stub.users[u.ID] = u
	return u
}

// 常量必须与迁移里的字面量逐字节一致：两边漂移时「仍是初始口令」的提示会静默失效。
func TestPresetAdminPasswordHashMatchesMigration(t *testing.T) {
	t.Parallel()
	sql, err := os.ReadFile("../../../migrations/0001_system_init.up.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	if !strings.Contains(string(sql), "'"+PresetAdminPasswordHash+"'") {
		t.Fatal("PresetAdminPasswordHash no longer matches the seeded admin password in 0001_system_init.up.sql")
	}
}

// 口令仍是迁移预置的初始口令 → 登录与当前登录者信息都带出改密提示；登录照常成功、不受任何限制。
// 改密后（哈希必然不同）提示消失。普通用户（非预置哈希）恒为 false。
func TestAuthUsecase_PasswordChangeRequired(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	uc, stub, _, _ := newAuthUsecase(t)
	admin := seedPresetAdmin(stub)
	alice := seedLoginUser(t, stub, "alice", StatusEnabled)

	user, pair, err := uc.Login(ctx, "admin", presetAdminPlain, "", "")
	if err != nil || pair == nil || pair.AccessToken == "" {
		t.Fatalf("Login(preset password) = (%+v, %v), want a normal successful login", pair, err)
	}
	if !user.PasswordChangeRequired || user.Password != "" {
		t.Fatalf("Login(preset password) user = %+v, want PasswordChangeRequired and no password", user)
	}
	profile, _, err := uc.Profile(ctx, admin.ID)
	if err != nil || !profile.PasswordChangeRequired {
		t.Fatalf("Profile(preset password) = (required=%v, %v), want PasswordChangeRequired", profile.PasswordChangeRequired, err)
	}

	if user, _, err := uc.Login(ctx, "alice", "plain-secret", "", ""); err != nil || user.PasswordChangeRequired {
		t.Fatalf("Login(ordinary user) = (%+v, %v), want no hint", user, err)
	}
	if profile, _, err := uc.Profile(ctx, alice.ID); err != nil || profile.PasswordChangeRequired {
		t.Fatalf("Profile(ordinary user) = (%+v, %v), want no hint", profile, err)
	}

	// 改密：哪怕新口令与初始口令相同，盐重取后哈希也不同——提示消失。
	users := NewUserUsecase(stub, stub, stub, stub, testHasher(t), stub)
	if err := users.ChangeMyPassword(ctx, admin.ID, presetAdminPlain, "N3w-Passw0rd!"); err != nil {
		t.Fatalf("ChangeMyPassword() error = %v", err)
	}
	user, _, err = uc.Login(ctx, "admin", "N3w-Passw0rd!", "", "")
	if err != nil || user.PasswordChangeRequired {
		t.Fatalf("Login(after change) = (%+v, %v), want no hint", user, err)
	}
	if profile, _, err := uc.Profile(ctx, admin.ID); err != nil || profile.PasswordChangeRequired {
		t.Fatalf("Profile(after change) = (%+v, %v), want no hint", profile, err)
	}
}

// credFailingReader 让按用户名的读路径失败（其余读照常）。
type credFailingReader struct {
	*userStoreStub
	err error
}

func (r credFailingReader) GetUserByUsername(context.Context, string) (*User, error) {
	return nil, r.err
}

// 提示位只是提示：核对口令哈希的那次读失败时，「当前登录者」接口照常返回（提示按否）。
func TestAuthUsecase_ProfileSurvivesPresetCheckFailure(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	uc, stub, _, _ := newAuthUsecase(t)
	admin := seedPresetAdmin(stub)
	for name, err := range map[string]error{"infrastructure": context.DeadlineExceeded, "deleted meanwhile": ErrUserNotFound} {
		uc.userReader = credFailingReader{userStoreStub: stub, err: err}
		profile, _, perr := uc.Profile(ctx, admin.ID)
		if perr != nil || profile.ID != admin.ID || profile.PasswordChangeRequired {
			t.Errorf("%s: Profile() = (%+v, %v), want the profile without the hint", name, profile, perr)
		}
	}
}
