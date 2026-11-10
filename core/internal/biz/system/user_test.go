package biz

import (
	"context"
	"encoding/base64"
	"regexp"
	"slices"
	"testing"

	"github.com/lanyulei/reborn/backend/core/internal/conf"

	"github.com/go-kratos/kratos/v3/errors"
)

// uuidV7Pattern 校验 UUIDv7：版本位为 7、variant 位为 10xx。
var uuidV7Pattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func testHasher(t *testing.T) *PasswordHasher {
	t.Helper()
	h, err := NewPasswordHasher(&conf.Security{Password: &conf.Security_Password{
		MemoryKb: 19456, Iterations: 2, Parallelism: 1,
	}})
	if err != nil {
		t.Fatalf("NewPasswordHasher() error = %v", err)
	}
	return h
}

// TestStatusConstantsLocked 锁定状态编码契约：与库表 "system_user".status 注释
// 及 proto 的 in: [1,2,3,4] 对齐，防止常量被意外改号后静默漂移。
func TestStatusConstantsLocked(t *testing.T) {
	t.Parallel()
	if StatusEnabled != 1 || StatusDisabled != 2 || StatusPaused != 3 || StatusResigned != 4 {
		t.Fatalf("user status constants drifted: enabled=%d disabled=%d paused=%d resigned=%d, want 1/2/3/4",
			StatusEnabled, StatusDisabled, StatusPaused, StatusResigned)
	}
	if RoleStatusEnabled != 1 || RoleStatusDisabled != 0 {
		t.Fatalf("role status constants drifted: enabled=%d disabled=%d, want 1/0",
			RoleStatusEnabled, RoleStatusDisabled)
	}
}

// userStoreStub 是 biz 单测用的极简内存仓储（仅覆盖被测分支）。
type userStoreStub struct {
	users    map[string]*User
	bindings map[string][]string
	roles    map[string]*Role
	grants   map[string]GrantSet
	lastHash string
	revoked  []string // DeleteByUser 的 userID 调用记录（会话吊销断言）
}

// DeleteByUser 刷新令牌吊销桩（RefreshTokenWriter 仅此方法被 UserUsecase 消费）。
func (s *userStoreStub) DeleteByUser(_ context.Context, userID string) error {
	s.revoked = append(s.revoked, userID)
	return nil
}

// Save/Rotate 补齐 RefreshTokenWriter 接口（UserUsecase 不调用）。
func (s *userStoreStub) Save(context.Context, *RefreshToken) error           { return nil }
func (s *userStoreStub) Rotate(context.Context, string, *RefreshToken) error { return nil }

func newUserStoreStub() *userStoreStub {
	return &userStoreStub{
		users:    map[string]*User{},
		bindings: map[string][]string{},
		roles:    map[string]*Role{},
		grants:   map[string]GrantSet{},
	}
}

func (s *userStoreStub) GetUser(_ context.Context, id string) (*User, error) {
	u, ok := s.users[id]
	if !ok {
		return nil, ErrUserNotFound
	}
	return u, nil
}

// GetUserByUsername 认证链路专用：出参含 password 哈希（对齐 data 层语义，
// 返回克隆——Login 成功路径会清空出参密码，不得污染仓储内部状态）。
func (s *userStoreStub) GetUserByUsername(_ context.Context, username string) (*User, error) {
	for _, u := range s.users {
		if u.Username == username {
			cp := *u
			return &cp, nil
		}
	}
	return nil, ErrUserNotFound
}

func (s *userStoreStub) ListUsers(_ context.Context, _ UserFilter, _, _ int64) ([]*User, int64, error) {
	users := make([]*User, 0, len(s.users))
	for _, u := range s.users {
		users = append(users, u)
	}
	return users, int64(len(users)), nil
}

func (s *userStoreStub) CreateUser(_ context.Context, user *User) error {
	for _, u := range s.users {
		if u.Username == user.Username {
			return ErrUserExists
		}
	}
	s.users[user.ID] = user
	s.lastHash = user.Password
	return nil
}

func (s *userStoreStub) UpdateUser(_ context.Context, id string, upd UserUpdate) error {
	u, ok := s.users[id]
	if !ok {
		return ErrUserNotFound
	}
	if upd.Nickname != nil {
		u.Nickname = *upd.Nickname
	}
	if upd.Status != nil {
		u.Status = *upd.Status
	}
	if upd.PasswordHash != nil {
		u.Password = *upd.PasswordHash
	}
	return nil
}

func (s *userStoreStub) DeleteUser(_ context.Context, id string) error {
	if _, ok := s.users[id]; !ok {
		return ErrUserNotFound
	}
	delete(s.users, id)
	delete(s.bindings, id)
	return nil
}

func (s *userStoreStub) GetRole(_ context.Context, id string) (*Role, error) {
	r, ok := s.roles[id]
	if !ok {
		return nil, ErrRoleNotFound
	}
	return r, nil
}

func (s *userStoreStub) ListRoles(_ context.Context, _ RoleFilter, _, _ int64) ([]*Role, int64, error) {
	roles := make([]*Role, 0, len(s.roles))
	for _, r := range s.roles {
		roles = append(roles, r)
	}
	return roles, int64(len(roles)), nil
}

func (s *userStoreStub) ListRolesByIDs(_ context.Context, ids []string) ([]*Role, error) {
	roles := make([]*Role, 0, len(ids))
	for _, id := range ids {
		if r, ok := s.roles[id]; ok {
			roles = append(roles, r)
		}
	}
	return roles, nil
}

func (s *userStoreStub) CreateRole(_ context.Context, role *Role) error {
	for _, r := range s.roles {
		if r.Code == role.Code {
			return ErrRoleCodeExists
		}
	}
	s.roles[role.ID] = role
	return nil
}

func (s *userStoreStub) UpdateRole(_ context.Context, id string, upd RoleUpdate) error {
	r, ok := s.roles[id]
	if !ok {
		return ErrRoleNotFound
	}
	if upd.Name != nil {
		r.Name = *upd.Name
	}
	if upd.Status != nil {
		r.Status = *upd.Status
	}
	return nil
}

func (s *userStoreStub) DeleteRole(_ context.Context, id string) error {
	if _, ok := s.roles[id]; !ok {
		return ErrRoleNotFound
	}
	delete(s.roles, id)
	return nil
}

func (s *userStoreStub) BindUserRoles(_ context.Context, userID string, roleIDs []string) error {
	s.bindings[userID] = roleIDs
	return nil
}

func (s *userStoreStub) ListUserRoles(_ context.Context, userID string) ([]*Role, error) {
	roles := make([]*Role, 0, len(s.bindings[userID]))
	for _, id := range s.bindings[userID] {
		if r, ok := s.roles[id]; ok {
			roles = append(roles, r)
		}
	}
	return roles, nil
}

// ListGrantsByRole 直接授权读取（AuthzUsecase 装配用）：返回三类集合副本，
// 未登记角色为空集（对齐 data 层语义）。
func (s *userStoreStub) ListGrantsByRole(_ context.Context, roleID string) (GrantSet, error) {
	g, ok := s.grants[roleID]
	if !ok {
		return GrantSet{}, nil
	}
	return GrantSet{
		Menus:    slices.Clone(g.Menus),
		Elements: slices.Clone(g.Elements),
		Apis:     slices.Clone(g.Apis),
	}, nil
}

// ListGrantsByRoles 多角色直接授权并集（对齐 data 层 DISTINCT + 字典序）。
func (s *userStoreStub) ListGrantsByRoles(_ context.Context, roleIDs []string) (GrantSet, error) {
	var union GrantSet
	for _, id := range roleIDs {
		g := s.grants[id]
		union.Menus = append(union.Menus, g.Menus...)
		union.Elements = append(union.Elements, g.Elements...)
		union.Apis = append(union.Apis, g.Apis...)
	}
	return GrantSet{
		Menus:    sortedUnique(union.Menus),
		Elements: sortedUnique(union.Elements),
		Apis:     sortedUnique(union.Apis),
	}, nil
}

func newUserUsecase(t *testing.T) (*UserUsecase, *userStoreStub) {
	t.Helper()
	stub := newUserStoreStub()
	return NewUserUsecase(stub, stub, stub, stub, testHasher(t), stub), stub
}

func TestUserUsecase_Create(t *testing.T) {
	t.Parallel()
	uc, stub := newUserUsecase(t)

	user, err := uc.Create(context.Background(), &User{
		Username: "alice",
		Password: "plain-secret",
		Nickname: "Alice",
		Gender:   GenderFemale,
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if !uuidV7Pattern.MatchString(user.ID) {
		t.Fatalf("Create() id = %q, want UUIDv7", user.ID)
	}
	// 明文不落库：仓储收到的是 Argon2id PHC 串。
	if stub.lastHash == "plain-secret" {
		t.Fatal("Create() persisted plaintext password")
	}
	if !regexp.MustCompile(`^\$argon2id\$`).MatchString(stub.lastHash) {
		t.Fatalf("Create() persisted password = %q, want argon2id PHC", stub.lastHash)
	}
	// 出参不携带密码。
	if user.Password != "" {
		t.Fatalf("Create() returned password %q, want empty", user.Password)
	}
}

func TestUserUsecase_CreateDuplicate(t *testing.T) {
	t.Parallel()
	uc, _ := newUserUsecase(t)
	ctx := context.Background()
	if _, err := uc.Create(ctx, &User{Username: "alice", Password: "plain-secret", Gender: GenderUnknown}); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	_, err := uc.Create(ctx, &User{Username: "alice", Password: "plain-secret", Gender: GenderUnknown})
	if !errors.Is(err, ErrUserExists) {
		t.Fatalf("Create(duplicate) error = %v, want ErrUserExists", err)
	}
}

func TestUserUsecase_ResetPassword(t *testing.T) {
	t.Parallel()
	uc, stub := newUserUsecase(t)
	ctx := context.Background()
	user, err := uc.Create(ctx, &User{Username: "alice", Password: "plain-secret", Gender: GenderUnknown})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	stub.lastHash = ""

	if err := uc.ResetPassword(ctx, user.ID, "new-secret-123"); err != nil {
		t.Fatalf("ResetPassword() error = %v", err)
	}
	if !regexp.MustCompile(`^\$argon2id\$`).MatchString(stub.users[user.ID].Password) {
		t.Fatalf("ResetPassword() stored %q, want argon2id PHC", stub.users[user.ID].Password)
	}
	if stub.users[user.ID].Password == "new-secret-123" {
		t.Fatal("ResetPassword() stored plaintext")
	}
}

func TestUserUsecase_UpdateStatus(t *testing.T) {
	t.Parallel()
	uc, stub := newUserUsecase(t)
	ctx := context.Background()
	user, err := uc.Create(ctx, &User{Username: "alice", Password: "plain-secret", Gender: GenderUnknown})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if err := uc.UpdateStatus(ctx, user.ID, StatusDisabled); err != nil {
		t.Fatalf("UpdateStatus() error = %v", err)
	}
	if got := stub.users[user.ID].Status; got != StatusDisabled {
		t.Fatalf("status = %d, want %d", got, StatusDisabled)
	}
}

func TestUserUsecase_BindRoles(t *testing.T) {
	t.Parallel()
	uc, stub := newUserUsecase(t)
	ctx := context.Background()

	user, err := uc.Create(ctx, &User{Username: "alice", Password: "plain-secret", Gender: GenderUnknown})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	r1, r2, r3 := &Role{ID: "role-1", Code: "a"}, &Role{ID: "role-2", Code: "b"}, &Role{ID: "role-3", Code: "c"}
	stub.roles[r1.ID], stub.roles[r2.ID], stub.roles[r3.ID] = r1, r2, r3

	// 用户不存在 → 404。
	if err := uc.BindRoles(ctx, "missing", []string{r1.ID}); !errors.Is(err, ErrUserNotFound) {
		t.Fatalf("BindRoles(missing user) error = %v, want ErrUserNotFound", err)
	}
	// 角色不存在 → 404。
	if err := uc.BindRoles(ctx, user.ID, []string{r1.ID, "missing"}); !errors.Is(err, ErrRoleNotFound) {
		t.Fatalf("BindRoles(missing role) error = %v, want ErrRoleNotFound", err)
	}
	// 全量覆盖：第二次绑定后仅剩新集合。
	if err := uc.BindRoles(ctx, user.ID, []string{r1.ID, r2.ID}); err != nil {
		t.Fatalf("BindRoles() error = %v", err)
	}
	if err := uc.BindRoles(ctx, user.ID, []string{r2.ID, r3.ID}); err != nil {
		t.Fatalf("BindRoles() error = %v", err)
	}
	if got := stub.bindings[user.ID]; len(got) != 2 || got[0] != r2.ID || got[1] != r3.ID {
		t.Fatalf("bindings = %v, want [role-2 role-3]", got)
	}
	// 空集合清空绑定。
	if err := uc.BindRoles(ctx, user.ID, nil); err != nil {
		t.Fatalf("BindRoles(clear) error = %v", err)
	}
	if got := stub.bindings[user.ID]; len(got) != 0 {
		t.Fatalf("bindings = %v, want empty", got)
	}
}

func TestUserUsecase_ListRoles(t *testing.T) {
	t.Parallel()
	uc, stub := newUserUsecase(t)
	ctx := context.Background()
	if _, err := uc.ListRoles(ctx, "missing"); !errors.Is(err, ErrUserNotFound) {
		t.Fatalf("ListRoles(missing user) error = %v, want ErrUserNotFound", err)
	}

	user, err := uc.Create(ctx, &User{Username: "alice", Password: "plain-secret", Gender: GenderUnknown})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	stub.bindings[user.ID] = []string{"role-1"}
	stub.roles["role-1"] = &Role{ID: "role-1", Code: "admin"}

	roles, err := uc.ListRoles(ctx, user.ID)
	if err != nil {
		t.Fatalf("ListRoles() error = %v", err)
	}
	if len(roles) != 1 || roles[0].ID != "role-1" {
		t.Fatalf("ListRoles() = %v, want role-1", roles)
	}
}

func TestNormalizePage(t *testing.T) {
	t.Parallel()
	cases := []struct {
		page, pageSize, wantPage, wantPageSize int64
	}{
		{0, 0, 1, 10},
		{1, 0, 1, 10},
		{-1, 5, 1, 5},
		{2, 200, 2, 100},
		{3, 50, 3, 50},
	}
	for _, tc := range cases {
		page, pageSize := normalizePage(tc.page, tc.pageSize)
		if page != tc.wantPage || pageSize != tc.wantPageSize {
			t.Errorf("normalizePage(%d, %d) = (%d, %d), want (%d, %d)",
				tc.page, tc.pageSize, page, pageSize, tc.wantPage, tc.wantPageSize)
		}
	}
}

func TestNewID(t *testing.T) {
	t.Parallel()
	seen := make(map[string]struct{}, 1000)
	for range 1000 {
		id, err := newID()
		if err != nil {
			t.Fatalf("newID() error = %v", err)
		}
		if !uuidV7Pattern.MatchString(id) {
			t.Fatalf("newID() = %q, want UUIDv7", id)
		}
		if _, ok := seen[id]; ok {
			t.Fatalf("newID() duplicated id %q", id)
		}
		seen[id] = struct{}{}
	}
}

// TestUserUsecase_PasswordB64Endpoints Create/ResetPassword 的 b64$ 编码路径：
// 哈希的是解码后的明文，编码串本身绝不落库。
func TestUserUsecase_PasswordB64Endpoints(t *testing.T) {
	t.Parallel()
	uc, stub := newUserUsecase(t)
	ctx := context.Background()
	b64pw := "b64$" + base64.StdEncoding.EncodeToString([]byte("plain-secret"))

	user, err := uc.Create(ctx, &User{Username: "alice", Password: b64pw, Gender: GenderUnknown})
	if err != nil {
		t.Fatalf("Create(b64 password) error = %v", err)
	}
	if stub.lastHash == b64pw || !regexp.MustCompile(`^\$argon2id\$`).MatchString(stub.lastHash) {
		t.Fatalf("Create(b64 password) persisted %q, want argon2id PHC of decoded plaintext", stub.lastHash)
	}

	if err := uc.ResetPassword(ctx, user.ID, b64pw); err != nil {
		t.Fatalf("ResetPassword(b64 password) error = %v", err)
	}
	if stub.users[user.ID].Password == b64pw || !regexp.MustCompile(`^\$argon2id\$`).MatchString(stub.users[user.ID].Password) {
		t.Fatalf("ResetPassword(b64 password) stored %q, want argon2id PHC", stub.users[user.ID].Password)
	}
	// 编码后过短：拒绝且不触碰存量哈希。
	before := stub.users[user.ID].Password
	if err := uc.ResetPassword(ctx, user.ID,
		"b64$"+base64.StdEncoding.EncodeToString([]byte("1234567"))); !errors.Is(err, ErrPasswordLength) {
		t.Fatalf("ResetPassword(decoded short) error = %v, want ErrPasswordLength", err)
	}
	if stub.users[user.ID].Password != before {
		t.Fatal("ResetPassword(failed decode) must not modify stored hash")
	}
}

// TestUserUsecase_SecurityChangesRevokeSessions 账号安全态变更（重置密码/
// 停用/撤销超管）须吊销该用户全部刷新令牌；启用与授予超管不吊销。
func TestUserUsecase_SecurityChangesRevokeSessions(t *testing.T) {
	t.Parallel()
	uc, stub := newUserUsecase(t)
	ctx := context.Background()
	u, err := uc.Create(ctx, &User{Username: "alice", Password: "plain-secret", Nickname: "A", Gender: GenderFemale, Status: StatusEnabled})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	revoked := func() int { return len(stub.revoked) }

	if err := uc.ResetPassword(ctx, u.ID, "new-secret-123"); err != nil || revoked() != 1 {
		t.Fatalf("ResetPassword() error = %v, revoked = %d; want nil, 1", err, revoked())
	}
	if err := uc.UpdateStatus(ctx, u.ID, StatusDisabled); err != nil || revoked() != 2 {
		t.Fatalf("UpdateStatus(disabled) error = %v, revoked = %d; want nil, 2", err, revoked())
	}
	if err := uc.UpdateStatus(ctx, u.ID, StatusEnabled); err != nil || revoked() != 2 {
		t.Fatalf("UpdateStatus(enabled) error = %v, revoked = %d; want nil, unchanged 2", err, revoked())
	}
	grant, revoke := true, false
	if err := uc.Update(ctx, u.ID, UserUpdate{IsAdmin: &grant}); err != nil || revoked() != 2 {
		t.Fatalf("Update(is_admin=true) error = %v, revoked = %d; want nil, unchanged 2", err, revoked())
	}
	if err := uc.Update(ctx, u.ID, UserUpdate{IsAdmin: &revoke}); err != nil || revoked() != 3 {
		t.Fatalf("Update(is_admin=false) error = %v, revoked = %d; want nil, 3", err, revoked())
	}
	for _, id := range stub.revoked {
		if id != u.ID {
			t.Fatalf("revoked user = %q, want %q", id, u.ID)
		}
	}
	// 目标不存在：不吊销。
	if err := uc.ResetPassword(ctx, "missing", "new-secret-123"); !errors.Is(err, ErrUserNotFound) || revoked() != 3 {
		t.Fatalf("ResetPassword(missing) error = %v, revoked = %d; want ErrUserNotFound, 3", err, revoked())
	}
}

// TestNormalizePageClamp 超大页码夹到上限，(page-1)*pageSize 不溢出为负。
func TestNormalizePageClamp(t *testing.T) {
	t.Parallel()
	page, size := normalizePage(1<<62, 100)
	if page != maxPage || size != 100 || (page-1)*size < 0 {
		t.Fatalf("normalizePage(huge) = (%d, %d), offset %d", page, size, (page-1)*size)
	}
}
