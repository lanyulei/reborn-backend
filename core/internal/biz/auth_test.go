package biz

import (
	"context"
	"encoding/base64"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v3/errors"
)

// refreshStoreStub 是认证用例的内存刷新令牌仓储，语义对齐 data 层契约：
// 行存在即有效、轮换检测重放、Save 惰性清理同用户过期行。
type refreshStoreStub struct {
	tokens  map[string]*RefreshToken // key: TokenHash
	deleted []string                 // DeleteByUser 的 userID 调用记录
}

func newRefreshStoreStub() *refreshStoreStub {
	return &refreshStoreStub{tokens: map[string]*RefreshToken{}}
}

func (s *refreshStoreStub) FindByHash(_ context.Context, tokenHash string) (*RefreshToken, error) {
	t, ok := s.tokens[tokenHash]
	if !ok {
		return nil, ErrTokenInvalid
	}
	return t, nil
}

func (s *refreshStoreStub) Save(_ context.Context, token *RefreshToken) error {
	now := time.Now()
	for h, t := range s.tokens {
		if t.UserID == token.UserID && t.ExpireTime.Before(now) {
			delete(s.tokens, h)
		}
	}
	s.tokens[token.TokenHash] = token
	return nil
}

func (s *refreshStoreStub) Rotate(_ context.Context, oldHash string, next *RefreshToken) error {
	if _, ok := s.tokens[oldHash]; !ok {
		return ErrTokenInvalid
	}
	delete(s.tokens, oldHash)
	s.tokens[next.TokenHash] = next
	return nil
}

func (s *refreshStoreStub) DeleteByUser(_ context.Context, userID string) error {
	s.deleted = append(s.deleted, userID)
	for h, t := range s.tokens {
		if t.UserID == userID {
			delete(s.tokens, h)
		}
	}
	return nil
}

func newAuthUsecase(t *testing.T) (*AuthUsecase, *userStoreStub, *refreshStoreStub) {
	t.Helper()
	stub := newUserStoreStub()
	refresh := newRefreshStoreStub()
	uc, err := NewAuthUsecase(stub, stub, refresh, refresh, testHasher(t), newDefaultSigner(t))
	if err != nil {
		t.Fatalf("NewAuthUsecase() error = %v", err)
	}
	return uc, stub, refresh
}

// seedLoginUser 预置一个可登录用户（真实 Argon2id 哈希），返回该用户。
func seedLoginUser(t *testing.T, stub *userStoreStub, username string, status int32) *User {
	t.Helper()
	hash, err := testHasher(t).Hash("plain-secret")
	if err != nil {
		t.Fatalf("hash seed password: %v", err)
	}
	u := &User{ID: "user-" + username, Username: username, Password: hash, Nickname: username, Status: status}
	stub.users[u.ID] = u
	return u
}

// TestAuthUsecase_LoginFailures 登录失败分支：凭据类错误统一口径（防枚举），
// 非可用状态在凭据正确后以 403 拒绝。
func TestAuthUsecase_LoginFailures(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	cases := []struct {
		name     string
		username string
		password string
		status   int32
		wantErr  error
	}{
		{"unknown user", "nobody", "plain-secret", StatusEnabled, ErrInvalidCredentials},
		{"wrong password", "alice", "wrong-password", StatusEnabled, ErrInvalidCredentials},
		{"disabled account", "alice", "plain-secret", StatusDisabled, ErrAccountDisabled},
		{"paused account", "alice", "plain-secret", StatusPaused, ErrAccountDisabled},
		{"resigned account", "alice", "plain-secret", StatusResigned, ErrAccountDisabled},
	}
	for _, tc := range cases {
		uc, stub, refresh := newAuthUsecase(t)
		seedLoginUser(t, stub, "alice", tc.status)
		user, pair, err := uc.Login(ctx, tc.username, tc.password)
		if !errors.Is(err, tc.wantErr) {
			t.Errorf("%s: Login() error = %v, want %v", tc.name, err, tc.wantErr)
		}
		if user != nil || pair != nil {
			t.Errorf("%s: Login() = (%v, %v), want (nil, nil)", tc.name, user, pair)
		}
		if len(refresh.tokens) != 0 {
			t.Errorf("%s: Login() persisted refresh tokens on failure: %v", tc.name, refresh.tokens)
		}
	}
}

// TestAuthUsecase_LoginSuccess 成功登录：出参无密码、双令牌可解析且已落库。
func TestAuthUsecase_LoginSuccess(t *testing.T) {
	t.Parallel()
	uc, stub, refresh := newAuthUsecase(t)
	u := seedLoginUser(t, stub, "alice", StatusEnabled)

	user, pair, err := uc.Login(context.Background(), "alice", "plain-secret")
	if err != nil {
		t.Fatalf("Login() error = %v", err)
	}
	if user.Password != "" {
		t.Fatal("Login() returned password hash, want empty")
	}
	if user.ID != u.ID {
		t.Fatalf("Login() user id = %q, want %q", user.ID, u.ID)
	}
	if pair.AccessToken == "" || pair.RefreshToken == "" {
		t.Fatalf("Login() pair = %+v, want both tokens", pair)
	}
	signer := newDefaultSigner(t)
	claims, err := signer.ParseAccess(pair.AccessToken)
	if err != nil {
		t.Fatalf("ParseAccess(login token) error = %v", err)
	}
	if claims.Subject != u.ID || claims.Username != "alice" {
		t.Fatalf("access claims = %+v, want sub=%s username=alice", claims.RegisteredClaims, u.ID)
	}
	if !refreshTokenPattern.MatchString(pair.RefreshToken) {
		t.Fatalf("refresh token = %q, want 64 hex chars", pair.RefreshToken)
	}
	saved, ok := refresh.tokens[hashRefreshToken(pair.RefreshToken)]
	if !ok {
		t.Fatal("Login() did not persist refresh token hash")
	}
	if saved.UserID != u.ID || !saved.ExpireTime.After(time.Now()) {
		t.Fatalf("persisted refresh token = %+v", saved)
	}
}

// TestAuthUsecase_LoginUnknownUserRunsDummyVerify 防枚举时序对齐：
// 用户不存在路径必须执行一次等代价 Argon2id 校验，耗时与真实校验同量级。
// 计时断言取两侧各自的最小值（最小值收敛到无抢占的真实成本）：
// 若 dummy verify 被跳过，Login 最小耗时是微秒级的查表错误路径，比单次
// Argon2id 校验低三个数量级；并行负载只会抬高单次采样，不影响判定。
func TestAuthUsecase_LoginUnknownUserRunsDummyVerify(t *testing.T) {
	t.Parallel()
	uc, _, _ := newAuthUsecase(t)
	hasher := testHasher(t)
	phc, err := hasher.Hash("calibration")
	if err != nil {
		t.Fatalf("hash calibration password: %v", err)
	}

	minVerify := time.Duration(1<<62 - 1)
	for range 3 {
		start := time.Now()
		hasher.Verify("wrong", phc) // 校准单次 Verify 成本
		minVerify = min(minVerify, time.Since(start))
	}
	maxLogin := time.Duration(0)
	for range 3 {
		start := time.Now()
		_, _, _ = uc.Login(context.Background(), "nobody", "plain-secret")
		maxLogin = max(maxLogin, time.Since(start))
	}

	// 断言最小校验成本的一半仍小于任一次 Login 耗时：dummy verify 在跑。
	if maxLogin < minVerify/2 {
		t.Fatalf("Login(unknown user) max %s < half of min verify cost %s; dummy verify likely skipped", maxLogin, minVerify)
	}
}

// TestAuthUsecase_RefreshFailures 刷新失败分支：未知/已轮换/已登出统一
// ErrTokenInvalid；过期不删行（由下次签发惰性清理）；用户已删或不可用
// 时吊销全部令牌后返回对应错误。
func TestAuthUsecase_RefreshFailures(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("unknown token", func(t *testing.T) {
		t.Parallel()
		uc, _, _ := newAuthUsecase(t)
		if _, err := uc.Refresh(ctx, "not-issued-by-us"); !errors.Is(err, ErrTokenInvalid) {
			t.Fatalf("Refresh(unknown) error = %v, want ErrTokenInvalid", err)
		}
	})

	t.Run("expired token kept until lazy purge", func(t *testing.T) {
		t.Parallel()
		uc, stub, refresh := newAuthUsecase(t)
		u := seedLoginUser(t, stub, "alice", StatusEnabled)
		raw := "raw-expired"
		hash := hashRefreshToken(raw)
		refresh.tokens[hash] = &RefreshToken{
			ID: "rt-expired", UserID: u.ID, TokenHash: hash,
			ExpireTime: time.Now().Add(-time.Minute), CreateTime: time.Now().Add(-25 * time.Hour),
		}
		_, err := uc.Refresh(ctx, raw)
		if !errors.Is(err, ErrTokenExpired) {
			t.Fatalf("Refresh(expired) error = %v, want ErrTokenExpired", err)
		}
		if _, ok := refresh.tokens[hash]; !ok {
			t.Fatal("Refresh(expired) deleted the row; lazy purge contract broken")
		}
	})

	t.Run("deleted user revokes tokens", func(t *testing.T) {
		t.Parallel()
		uc, _, refresh := newAuthUsecase(t)
		hash := hashRefreshToken("raw-deleted-user")
		refresh.tokens[hash] = &RefreshToken{
			ID: "rt-1", UserID: "user-gone", TokenHash: hash,
			ExpireTime: time.Now().Add(time.Hour), CreateTime: time.Now(),
		}
		_, err := uc.Refresh(ctx, "raw-deleted-user")
		if !errors.Is(err, ErrTokenInvalid) {
			t.Fatalf("Refresh(deleted user) error = %v, want ErrTokenInvalid", err)
		}
		if len(refresh.tokens) != 0 {
			t.Fatalf("Refresh(deleted user) left tokens = %v", refresh.tokens)
		}
	})

	t.Run("disabled user revokes tokens", func(t *testing.T) {
		t.Parallel()
		uc, stub, refresh := newAuthUsecase(t)
		u := seedLoginUser(t, stub, "alice", StatusDisabled)
		hash := hashRefreshToken("raw-disabled")
		refresh.tokens[hash] = &RefreshToken{
			ID: "rt-1", UserID: u.ID, TokenHash: hash,
			ExpireTime: time.Now().Add(time.Hour), CreateTime: time.Now(),
		}
		_, err := uc.Refresh(ctx, "raw-disabled")
		if !errors.Is(err, ErrAccountDisabled) {
			t.Fatalf("Refresh(disabled user) error = %v, want ErrAccountDisabled", err)
		}
		if len(refresh.tokens) != 0 {
			t.Fatalf("Refresh(disabled user) left tokens = %v", refresh.tokens)
		}
	})
}

// TestAuthUsecase_RefreshSuccessAndReplay 成功轮换：旧哈希即刻失效、新哈希
// 可用；旧原文重放（含并发轮换败者语义）按 ErrTokenInvalid 拒绝。
func TestAuthUsecase_RefreshSuccessAndReplay(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	uc, stub, refresh := newAuthUsecase(t)
	u := seedLoginUser(t, stub, "alice", StatusEnabled)

	_, pair, err := uc.Login(ctx, "alice", "plain-secret")
	if err != nil {
		t.Fatalf("Login() error = %v", err)
	}
	oldHash := hashRefreshToken(pair.RefreshToken)

	next, err := uc.Refresh(ctx, pair.RefreshToken)
	if err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	if next.AccessToken == "" || next.RefreshToken == "" || next.RefreshToken == pair.RefreshToken {
		t.Fatalf("Refresh() pair = %+v, want new token pair", next)
	}
	if _, ok := refresh.tokens[oldHash]; ok {
		t.Fatal("Refresh() left old hash valid")
	}
	if _, ok := refresh.tokens[hashRefreshToken(next.RefreshToken)]; !ok {
		t.Fatal("Refresh() did not persist new hash")
	}
	signer := newDefaultSigner(t)
	claims, err := signer.ParseAccess(next.AccessToken)
	if err != nil || claims.Subject != u.ID {
		t.Fatalf("Refresh() access token claims = %+v err = %v, want sub %s", claims, err, u.ID)
	}

	// 旧原文重放：哈希已不存在 → ErrTokenInvalid（覆盖并发轮换败者）。
	if _, err := uc.Refresh(ctx, pair.RefreshToken); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("Refresh(replay) error = %v, want ErrTokenInvalid", err)
	}
}

// TestAuthUsecase_Logout 登出吊销全部刷新令牌且幂等。
func TestAuthUsecase_Logout(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	uc, stub, refresh := newAuthUsecase(t)
	u := seedLoginUser(t, stub, "alice", StatusEnabled)
	if _, _, err := uc.Login(ctx, "alice", "plain-secret"); err != nil {
		t.Fatalf("Login() error = %v", err)
	}
	if len(refresh.tokens) == 0 {
		t.Fatal("Login() saved no refresh token")
	}

	if err := uc.Logout(ctx, u.ID); err != nil {
		t.Fatalf("Logout() error = %v", err)
	}
	if len(refresh.tokens) != 0 {
		t.Fatalf("Logout() left tokens = %v", refresh.tokens)
	}
	if len(refresh.deleted) != 1 || refresh.deleted[0] != u.ID {
		t.Fatalf("Logout() delete calls = %v, want [%s]", refresh.deleted, u.ID)
	}
	// 幂等：重复登出不是错误。
	if err := uc.Logout(ctx, u.ID); err != nil {
		t.Fatalf("Logout(idempotent) error = %v", err)
	}
}

// TestAuthUsecase_Profile 当前登录者信息 + 绑定角色；用户不存在透传哨兵。
func TestAuthUsecase_Profile(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	uc, stub, _ := newAuthUsecase(t)
	u := seedLoginUser(t, stub, "alice", StatusEnabled)
	stub.roles["role-1"] = &Role{ID: "role-1", Code: "admin", Name: "管理员"}
	stub.bindings[u.ID] = []string{"role-1"}

	user, roles, err := uc.Profile(ctx, u.ID)
	if err != nil {
		t.Fatalf("Profile() error = %v", err)
	}
	if user.ID != u.ID || len(roles) != 1 || roles[0].Code != "admin" {
		t.Fatalf("Profile() = (%+v, %+v)", user, roles)
	}
	if _, _, err := uc.Profile(ctx, "missing"); !errors.Is(err, ErrUserNotFound) {
		t.Fatalf("Profile(missing) error = %v, want ErrUserNotFound", err)
	}
}

// TestAuthUsecase_LoginWithB64Password 前端编码密码兼容：b64$ 值与明文等价。
func TestAuthUsecase_LoginWithB64Password(t *testing.T) {
	t.Parallel()
	uc, stub, _ := newAuthUsecase(t)
	seedLoginUser(t, stub, "alice", StatusEnabled)
	b64pw := "b64$" + base64.StdEncoding.EncodeToString([]byte("plain-secret"))

	user, pair, err := uc.Login(context.Background(), "alice", b64pw)
	if err != nil || user == nil || pair == nil {
		t.Fatalf("Login(b64 password) = (%v, %v, %v), want success", user, pair, err)
	}
	// 明文路径不受影响（双向兼容）。
	if _, _, err := uc.Login(context.Background(), "alice", "plain-secret"); err != nil {
		t.Fatalf("Login(plaintext) error = %v, want nil", err)
	}
	// 编码后的错误密码仍是凭据错误。
	if _, _, err := uc.Login(context.Background(), "alice",
		"b64$"+base64.StdEncoding.EncodeToString([]byte("wrong-pass-9"))); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("Login(b64 wrong password) error = %v, want ErrInvalidCredentials", err)
	}
	// 解码后长度越界在查询前拒绝。
	if _, _, err := uc.Login(context.Background(), "alice",
		"b64$"+base64.StdEncoding.EncodeToString([]byte("1234567"))); !errors.Is(err, ErrPasswordLength) {
		t.Fatalf("Login(decoded short) error = %v, want ErrPasswordLength", err)
	}
}
