//go:build integration

package data

import (
	"context"
	"sync"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"

	"github.com/go-kratos/kratos/v3/errors"
)

// 登录签发的刷新令牌是条件插入（IssuedForHash）：签发与「重置口令 / 停用」并发时，旧口令换来的会话
// 不得落在吊销之后而存活。刷新轮换（不带 IssuedForHash）照旧无条件插入。
func TestIntegration_RefreshTokenSaveChecksCredentials(t *testing.T) {
	d := newIntegrationData(t)
	users, tokens := NewUserRepo(d), NewRefreshTokenRepo(d)
	ctx := context.Background()
	const (
		userID = "018f3e1a-0002-7000-8000-0000000000a1"
		prefix = "018f3e1a-0003-7000-8000-0000000000"
	)
	user := newTestUser(userID, "login-race")
	if err := users.CreateUser(ctx, user); err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}
	// 按 ID 读取的 GetUser 不带口令哈希（出参永不回显）；登录用的是按用户名读取，哈希从这里取。
	saved, err := users.GetUserByUsername(ctx, "login-race")
	if err != nil || saved.Password == "" {
		t.Fatalf("GetUserByUsername() = %+v, %v; want the stored password hash", saved, err)
	}
	staleHash := saved.Password
	issue := func(suffix, hash string) error {
		rt := newRefreshToken(prefix+suffix, userID, "raw-"+suffix, time.Hour)
		rt.IssuedForHash = hash
		return tokens.Save(ctx, rt)
	}

	// 口令未变：正常签发。
	if err := issue("a2", staleHash); err != nil {
		t.Fatalf("Save(unchanged password) error = %v", err)
	}
	// 管理员重置了口令之后，拿着旧哈希的签发被拒绝，且没有落行。
	newHash := "argon2id-reset-hash"
	if err := users.UpdateUser(ctx, userID, biz.UserUpdate{PasswordHash: &newHash}); err != nil {
		t.Fatalf("UpdateUser(password) error = %v", err)
	}
	if err := issue("a3", staleHash); !errors.Is(err, biz.ErrTokenInvalid) {
		t.Fatalf("Save(stale password) error = %v, want ErrTokenInvalid", err)
	}
	if _, err := tokens.FindByHash(ctx, hashRaw("raw-a3")); !errors.Is(err, biz.ErrTokenInvalid) {
		t.Fatalf("a session issued with a stale password must not be stored, lookup error = %v", err)
	}
	// 刷新轮换路径（不带校验）不受影响。
	if err := tokens.Save(ctx, newRefreshToken(prefix+"a4", userID, "raw-a4", time.Hour)); err != nil {
		t.Fatalf("Save(rotation path) error = %v", err)
	}
	// 账号被停用后：即使口令哈希对得上也不签发。
	disabled := biz.StatusDisabled
	if err := users.UpdateUser(ctx, userID, biz.UserUpdate{Status: &disabled}); err != nil {
		t.Fatalf("UpdateUser(disabled) error = %v", err)
	}
	if err := issue("a5", newHash); !errors.Is(err, biz.ErrTokenInvalid) {
		t.Fatalf("Save(disabled account) error = %v, want ErrTokenInvalid", err)
	}
}

// 同一角色的多次并发全量替换（角色此前没有任何授权行，DELETE 都不影响行、随后各自插入）被事务级
// 咨询锁串行化：不再撞唯一索引报错，最终状态恰为一组授权。
func TestIntegration_PermissionReplaceConcurrent(t *testing.T) {
	d := newIntegrationData(t)
	repo, roles, txs := NewPermissionRepo(d), NewRoleRepo(d), basedata.NewTxManager(d)
	ctx := context.Background()
	role := &biz.Role{ID: "018f3e1a-0100-7000-8000-0000000000e1", Name: "并发授权", Code: "concurrent-grants", Status: biz.RoleStatusEnabled}
	if err := roles.CreateRole(ctx, role); err != nil {
		t.Fatalf("CreateRole() error = %v", err)
	}
	listRoles, listUsers := biz.OperationCode("/system.v1.RoleService/ListRoles"), biz.OperationCode("/system.v1.UserService/ListUsers")

	const workers = 8
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := range workers {
		wg.Go(func() {
			apis := []string{listRoles}
			if i%2 == 1 {
				apis = append(apis, listUsers)
			}
			errs <- txs.WithinTx(ctx, func(tx txrun.Tx) error {
				return repo.ReplaceForRole(ctx, tx, role.ID, biz.GrantSet{Menus: []string{"system.user"}, Apis: apis}, apis)
			})
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("ReplaceForRole() error = %v", err)
		}
	}
	grants, err := repo.ListGrantsByRole(ctx, role.ID)
	if err != nil || len(grants.Menus) != 1 || len(grants.Apis) < 1 || len(grants.Apis) > 2 {
		t.Fatalf("ListGrantsByRole() = %+v, %v; want exactly one writer's grant set", grants, err)
	}
	policies, err := repo.ListPolicies(ctx)
	if err != nil {
		t.Fatalf("ListPolicies() error = %v", err)
	}
	n := 0
	for _, p := range policies {
		if p.RoleCode == role.Code {
			n++
		}
	}
	if n != len(grants.Apis) {
		t.Fatalf("materialized policies = %d, want %d (consistent with the direct grants)", n, len(grants.Apis))
	}
}
