//go:build integration

package data

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/lanyulei/monitor/backend/core/internal/biz"
	"github.com/lanyulei/monitor/backend/core/internal/conf"

	"github.com/go-kratos/kratos/v3/errors"
)

// missingID 是不存在于库中的合法 UUID。
const missingID = "018f3e1a-ffff-7000-8000-000000000000"

// newIntegrationData 构造真实连接池；未配置 TEST_DATABASE_SOURCE 时跳过。
func newIntegrationData(t *testing.T) *Data {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_SOURCE")
	if dsn == "" {
		t.Skip("TEST_DATABASE_SOURCE not set, skipping data integration tests")
	}
	d, cleanup, err := NewData(&conf.Data{Database: &conf.Data_Database{Source: dsn}})
	if err != nil {
		t.Fatalf("NewData() error = %v", err)
	}
	t.Cleanup(cleanup)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// 单条语句同时截断六张表（存在外键依赖，逐表 TRUNCATE 会被拒绝）。
	if _, err := d.pool.Exec(ctx, "TRUNCATE TABLE system_permission, system_grant, system_refresh_token, system_user_role, system_role, \"system_user\""); err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
	return d
}

// newTestUser 返回带固定 ID 与审计时间的用户 DO（密码为合法 PHC 格式占位）。
func newTestUser(id, username string) *biz.User {
	return &biz.User{
		ID:         id,
		Username:   username,
		Password:   "$argon2id$v=19$m=19456,t=2,p=1$c2FsdHNhbHRzYWx0c2FsdA$c2FsdHNhbHRzYWx0c2FsdA",
		Nickname:   "Nickname-" + username,
		Email:      username + "@example.com",
		Phone:      "1380000000" + username[len(username)-1:],
		Gender:     biz.GenderFemale,
		Status:     biz.StatusEnabled,
		CreateTime: time.Now().UTC(),
		UpdateTime: time.Now().UTC(),
	}
}

func TestIntegration_UserCRUD(t *testing.T) {
	d := newIntegrationData(t)
	repo := NewUserRepo(d)
	ctx := context.Background()
	user := newTestUser("018f3e1a-0001-7000-8000-000000000001", "alice")

	if err := repo.CreateUser(ctx, user); err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}

	// 唯一冲突映射：username / email / phone。
	dup := newTestUser("018f3e1a-0001-7000-8000-000000000002", "alice2")
	dup.Username = "alice"
	if err := repo.CreateUser(ctx, dup); !errors.Is(err, biz.ErrUserExists) {
		t.Fatalf("CreateUser(dup username) error = %v, want ErrUserExists", err)
	}
	dup.Username = "bob"
	dup.Email = user.Email
	if err := repo.CreateUser(ctx, dup); !errors.Is(err, biz.ErrUserEmailExists) {
		t.Fatalf("CreateUser(dup email) error = %v, want ErrUserEmailExists", err)
	}
	dup.Email = ""
	dup.Phone = user.Phone
	if err := repo.CreateUser(ctx, dup); !errors.Is(err, biz.ErrUserPhoneExists) {
		t.Fatalf("CreateUser(dup phone) error = %v, want ErrUserPhoneExists", err)
	}

	// 查询：不返回密码；email/phone 正常回填。
	got, err := repo.GetUser(ctx, user.ID)
	if err != nil {
		t.Fatalf("GetUser() error = %v", err)
	}
	if got.Username != "alice" || got.Email != "alice@example.com" || got.Phone == "" {
		t.Fatalf("GetUser() = %+v", got)
	}
	if got.Password != "" {
		t.Fatal("GetUser() must not return password")
	}
	if got.Status != biz.StatusEnabled || got.Gender != biz.GenderFemale {
		t.Fatalf("GetUser() status/gender = %d/%q", got.Status, got.Gender)
	}

	// 按用户名查询（认证链路专用）：出参包含 password 哈希，其余读路径不返回。
	byName, err := repo.GetUserByUsername(ctx, "alice")
	if err != nil {
		t.Fatalf("GetUserByUsername() error = %v", err)
	}
	if byName.ID != user.ID || byName.Password != user.Password {
		t.Fatalf("GetUserByUsername() = %+v, want id %s with password hash", byName, user.ID)
	}
	if _, err := repo.GetUserByUsername(ctx, "missing-name"); !errors.Is(err, biz.ErrUserNotFound) {
		t.Fatalf("GetUserByUsername(missing) error = %v, want ErrUserNotFound", err)
	}

	// 空邮箱/手机号用户：落库 NULL、出参归一空串。
	bare := newTestUser("018f3e1a-0001-7000-8000-000000000003", "carol")
	bare.Email, bare.Phone = "", ""
	if err := repo.CreateUser(ctx, bare); err != nil {
		t.Fatalf("CreateUser(bare) error = %v", err)
	}
	gotBare, err := repo.GetUser(ctx, bare.ID)
	if err != nil {
		t.Fatalf("GetUser(bare) error = %v", err)
	}
	if gotBare.Email != "" || gotBare.Phone != "" {
		t.Fatalf("GetUser(bare) email/phone = %q/%q, want empty", gotBare.Email, gotBare.Phone)
	}

	// 部分更新：nickname 变更 + email 清空（空串 → NULL）。
	nickname := "Alice Chen"
	empty := ""
	if err := repo.UpdateUser(ctx, user.ID, biz.UserUpdate{Nickname: &nickname, Email: &empty}); err != nil {
		t.Fatalf("UpdateUser() error = %v", err)
	}
	got, err = repo.GetUser(ctx, user.ID)
	if err != nil {
		t.Fatalf("GetUser() error = %v", err)
	}
	if got.Nickname != nickname || got.Email != "" {
		t.Fatalf("UpdateUser() result = %+v", got)
	}

	// 更新不存在用户。
	if err := repo.UpdateUser(ctx, missingID, biz.UserUpdate{Nickname: &nickname}); !errors.Is(err, biz.ErrUserNotFound) {
		t.Fatalf("UpdateUser(missing) error = %v, want ErrUserNotFound", err)
	}

	// 列表：keyword / status 过滤与分页。
	users, total, err := repo.ListUsers(ctx, biz.UserFilter{Keyword: "ali"}, 1, 10)
	if err != nil {
		t.Fatalf("ListUsers() error = %v", err)
	}
	if total != 1 || len(users) != 1 || users[0].Username != "alice" {
		t.Fatalf("ListUsers(keyword) = %d %+v", total, users)
	}
	// keyword 亦模糊匹配手机号（alice phone=1380000000e，carol phone 为空）。
	users, total, err = repo.ListUsers(ctx, biz.UserFilter{Keyword: "1380000000"}, 1, 10)
	if err != nil {
		t.Fatalf("ListUsers(keyword by phone) error = %v", err)
	}
	if total != 1 || len(users) != 1 || users[0].Username != "alice" {
		t.Fatalf("ListUsers(keyword by phone) = %d %+v, want alice only", total, users)
	}
	disabled := biz.StatusDisabled
	_, total, err = repo.ListUsers(ctx, biz.UserFilter{Status: &disabled}, 1, 10)
	if err != nil {
		t.Fatalf("ListUsers(status) error = %v", err)
	}
	if total != 0 {
		t.Fatalf("ListUsers(disabled) total = %d, want 0", total)
	}
	_, total, err = repo.ListUsers(ctx, biz.UserFilter{}, 1, 1)
	if err != nil {
		t.Fatalf("ListUsers(paged) error = %v", err)
	}
	if total != 2 {
		t.Fatalf("ListUsers(paged) total = %d, want 2", total)
	}

	// 删除（软删）后不可见，且 username 可复用。
	if err := repo.DeleteUser(ctx, user.ID); err != nil {
		t.Fatalf("DeleteUser() error = %v", err)
	}
	if _, err := repo.GetUser(ctx, user.ID); !errors.Is(err, biz.ErrUserNotFound) {
		t.Fatalf("GetUser(deleted) error = %v, want ErrUserNotFound", err)
	}
	if _, err := repo.GetUserByUsername(ctx, "alice"); !errors.Is(err, biz.ErrUserNotFound) {
		t.Fatalf("GetUserByUsername(deleted) error = %v, want ErrUserNotFound", err)
	}
	if err := repo.DeleteUser(ctx, user.ID); !errors.Is(err, biz.ErrUserNotFound) {
		t.Fatalf("DeleteUser(deleted) error = %v, want ErrUserNotFound", err)
	}
	reuse := newTestUser("018f3e1a-0001-7000-8000-000000000004", "alice")
	reuse.Email, reuse.Phone = "", ""
	if err := repo.CreateUser(ctx, reuse); err != nil {
		t.Fatalf("CreateUser(reuse username) error = %v", err)
	}
}
