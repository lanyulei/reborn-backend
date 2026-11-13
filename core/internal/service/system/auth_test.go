package service

import (
	"context"
	"io"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/system/v1"
	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"

	jwtx "github.com/go-kratos/kratos/contrib/middleware/jwt/v3"
	"github.com/go-kratos/kratos/v3/errors"
	"github.com/golang-jwt/jwt/v5"
)

// newAuthService 构造真实用例链 + 内存仓储的 AuthService 与配套用例；
// authorizer 一并返回供测试走真实授权链（PermissionUsecase.Replace 后
// 快照即时刷新）。
func newAuthService(t *testing.T) (*AuthService, *biz.AuthUsecase, *testfake.Store, *biz.Authorizer) {
	t.Helper()
	store := testfake.NewStore()
	hasher, err := testfake.NewHasher()
	if err != nil {
		t.Fatalf("testfake.NewHasher() error = %v", err)
	}
	signer, err := testfake.NewSigner()
	if err != nil {
		t.Fatalf("testfake.NewSigner() error = %v", err)
	}
	uc, err := biz.NewAuthUsecase(store, store, store, store, hasher, signer,
		store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("biz.NewAuthUsecase() error = %v", err)
	}
	authorizer, err := testfake.NewAuthorizer(store, time.Minute)
	if err != nil {
		t.Fatalf("testfake.NewAuthorizer() error = %v", err)
	}
	authz := biz.NewAuthzUsecase(store, store, store, authorizer)
	return NewAuthService(uc, authz, biz.NewAvatarUsecase(store, store, testfake.NewAvatarStore())), uc, store, authorizer
}

// seedAuthUser 预置可登录用户（真实 Argon2id 哈希）。
func seedAuthUser(t *testing.T, store *testfake.Store, username string) *biz.User {
	t.Helper()
	hasher, err := testfake.NewHasher()
	if err != nil {
		t.Fatalf("testfake.NewHasher() error = %v", err)
	}
	hash, err := hasher.Hash("plain-secret")
	if err != nil {
		t.Fatalf("hash seed password: %v", err)
	}
	u := &biz.User{ID: "user-" + username, Username: username, Password: hash, Nickname: username, Status: biz.StatusEnabled}
	if err := store.CreateUser(context.Background(), u); err != nil {
		t.Fatalf("seed user %s: %v", username, err)
	}
	return u
}

// claimsContext 模拟鉴权中间件注入后的请求 ctx。
func claimsContext(userID string) context.Context {
	return jwtx.NewContext(context.Background(), &biz.AccessClaims{
		RegisteredClaims: jwt.RegisteredClaims{Subject: userID},
	})
}

func TestAuthService_Login(t *testing.T) {
	t.Parallel()
	svc, _, store, _ := newAuthService(t)
	seedAuthUser(t, store, "alice")
	ctx := context.Background()

	resp, err := svc.Login(ctx, &v1.LoginRequest{Username: "alice", Password: "plain-secret"})
	if err != nil {
		t.Fatalf("Login() error = %v", err)
	}
	if resp.GetAccessToken() == "" || resp.GetRefreshToken() == "" || resp.GetExpiresIn() != 900 {
		t.Fatalf("Login() tokens = (%q, %q, %d), want non-empty and 15m", resp.GetAccessToken(), resp.GetRefreshToken(), resp.GetExpiresIn())
	}
	if resp.GetUser().GetUsername() != "alice" || resp.GetUser().GetId() == "" {
		t.Fatalf("Login() user = %+v", resp.GetUser())
	}

	if _, err := svc.Login(ctx, &v1.LoginRequest{Username: "alice", Password: "wrong-password"}); !errors.Is(err, biz.ErrInvalidCredentials) {
		t.Fatalf("Login(wrong password) error = %v, want ErrInvalidCredentials", err)
	}
	if _, err := svc.Login(ctx, &v1.LoginRequest{Username: "nobody", Password: "plain-secret"}); !errors.Is(err, biz.ErrInvalidCredentials) {
		t.Fatalf("Login(unknown user) error = %v, want ErrInvalidCredentials", err)
	}

	// 登录审计：三次尝试各落一条，成功/密码错误回填 user id，用户不存在为空。
	logs, total, err := store.ListLoginLogs(ctx, biz.LoginLogFilter{}, 1, 10)
	if err != nil {
		t.Fatalf("ListLoginLogs() error = %v", err)
	}
	if total != 3 || len(logs) != 3 {
		t.Fatalf("ListLoginLogs() = (%d total, %d items), want (3, 3)", total, len(logs))
	}
	byResult := map[int32]biz.LoginLog{}
	for _, l := range logs {
		byResult[l.Result] = *l
	}
	for _, want := range []struct {
		result  int32
		wantUID bool
	}{
		{biz.LoginResultSuccess, true},
		{biz.LoginResultWrongPassword, true},
		{biz.LoginResultUserNotFound, false},
	} {
		l, ok := byResult[want.result]
		if !ok {
			t.Fatalf("login log result %d missing, got %v", want.result, byResult)
		}
		if want.wantUID && l.UserID == "" {
			t.Errorf("login log result %d: user id empty, want backfilled", want.result)
		}
		if !want.wantUID && l.UserID != "" {
			t.Errorf("login log result %d: user id = %q, want empty", want.result, l.UserID)
		}
	}
}

func TestAuthService_RefreshToken(t *testing.T) {
	t.Parallel()
	svc, uc, store, _ := newAuthService(t)
	seedAuthUser(t, store, "alice")
	ctx := context.Background()
	_, pair, err := uc.Login(ctx, "alice", "plain-secret", "", "")
	if err != nil {
		t.Fatalf("Login() error = %v", err)
	}

	resp, err := svc.RefreshToken(ctx, &v1.RefreshTokenRequest{RefreshToken: pair.RefreshToken})
	if err != nil {
		t.Fatalf("RefreshToken() error = %v", err)
	}
	if resp.GetAccessToken() == "" || resp.GetRefreshToken() == "" || resp.GetExpiresIn() != 900 {
		t.Fatalf("RefreshToken() = (%q, %q, %d)", resp.GetAccessToken(), resp.GetRefreshToken(), resp.GetExpiresIn())
	}

	// 旧令牌已轮换：重放按无效拒绝。
	if _, err := svc.RefreshToken(ctx, &v1.RefreshTokenRequest{RefreshToken: pair.RefreshToken}); !errors.Is(err, biz.ErrTokenInvalid) {
		t.Fatalf("RefreshToken(replay) error = %v, want ErrTokenInvalid", err)
	}
}

// TestAuthService_LogoutClaims 用户身份提取契约：仅接受中间件注入的
// AccessClaims 且 Subject 非空，其余一律按无效令牌拒绝。
func TestAuthService_LogoutClaims(t *testing.T) {
	t.Parallel()
	svc, _, store, _ := newAuthService(t)
	u := seedAuthUser(t, store, "alice")

	if _, err := svc.Logout(claimsContext(u.ID), &v1.LogoutRequest{}); err != nil {
		t.Fatalf("Logout(valid claims) error = %v", err)
	}

	cases := []struct {
		name string
		ctx  context.Context
	}{
		{"no claims", context.Background()},
		{"wrong claims type", jwtx.NewContext(context.Background(), &jwt.RegisteredClaims{Subject: u.ID})},
		{"empty subject", claimsContext("")},
	}
	for _, tc := range cases {
		if _, err := svc.Logout(tc.ctx, &v1.LogoutRequest{}); !errors.Is(err, biz.ErrTokenInvalid) {
			t.Errorf("%s: Logout() error = %v, want ErrTokenInvalid", tc.name, err)
		}
	}
}

func TestAuthService_GetLoginProfile(t *testing.T) {
	t.Parallel()
	svc, _, store, _ := newAuthService(t)
	u := seedAuthUser(t, store, "alice")
	role := &biz.Role{ID: "role-admin", Code: "admin", Name: "管理员", Status: biz.RoleStatusEnabled}
	if err := store.CreateRole(context.Background(), role); err != nil {
		t.Fatalf("seed role: %v", err)
	}
	if err := store.BindUserRoles(context.Background(), u.ID, []string{role.ID}); err != nil {
		t.Fatalf("bind role: %v", err)
	}

	resp, err := svc.GetLoginProfile(claimsContext(u.ID), &v1.GetLoginProfileRequest{})
	if err != nil {
		t.Fatalf("GetLoginProfile() error = %v", err)
	}
	if resp.GetUser().GetId() != u.ID || resp.GetUser().GetUsername() != "alice" {
		t.Fatalf("GetLoginProfile() user = %+v", resp.GetUser())
	}
	if len(resp.GetRoles()) != 1 || resp.GetRoles()[0].GetCode() != "admin" {
		t.Fatalf("GetLoginProfile() roles = %+v", resp.GetRoles())
	}

	// claims 缺失与用户不存在分别按无效令牌/404 拒绝。
	if _, err := svc.GetLoginProfile(context.Background(), &v1.GetLoginProfileRequest{}); !errors.Is(err, biz.ErrTokenInvalid) {
		t.Fatalf("GetLoginProfile(no claims) error = %v, want ErrTokenInvalid", err)
	}
	if _, err := svc.GetLoginProfile(claimsContext("user-missing"), &v1.GetLoginProfileRequest{}); !errors.Is(err, biz.ErrUserNotFound) {
		t.Fatalf("GetLoginProfile(missing user) error = %v, want ErrUserNotFound", err)
	}
}

// TestAuthService_GetLoginProfileGrants permissions/menus/elements 出参契约：
// 走真实授权链（PermissionUsecase.SetGrants 提交后快照即时刷新），普通用户
// permissions 返回级联物化后的操作 code 并集（字典序），menus/elements 返回
// 直接授权并集；is_admin 超管三者均为空（前端按 is_admin 放行）。
func TestAuthService_GetLoginProfileGrants(t *testing.T) {
	t.Parallel()
	svc, _, store, authorizer := newAuthService(t)
	u := seedAuthUser(t, store, "alice")
	role := &biz.Role{ID: "role-ops", Code: "ops", Name: "运维", Status: biz.RoleStatusEnabled}
	if err := store.CreateRole(context.Background(), role); err != nil {
		t.Fatalf("seed role: %v", err)
	}
	if err := store.BindUserRoles(context.Background(), u.ID, []string{role.ID}); err != nil {
		t.Fatalf("bind role: %v", err)
	}
	grants := biz.GrantSet{
		Menus:    []string{"system.user"},
		Elements: []string{"system.role.grant"},
		Apis:     []string{biz.OperationCode("/system.v1.RoleService/ListRoles")},
	}
	permUC := biz.NewPermissionUsecase(store, store, authorizer, biznotify.NewNotificationUsecase(store, nil), testfake.NewTxManager())
	if err := permUC.SetGrants(context.Background(), role.ID, grants, ""); err != nil {
		t.Fatalf("grant permissions: %v", err)
	}

	resp, err := svc.GetLoginProfile(claimsContext(u.ID), &v1.GetLoginProfileRequest{})
	if err != nil {
		t.Fatalf("GetLoginProfile() error = %v", err)
	}
	// 接口权限只来自 apis 直接授权（菜单/元素仅控可见性，不级联）。
	want := slices.Clone(grants.Apis)
	slices.Sort(want)
	if got := resp.GetPermissions(); !slices.Equal(got, want) {
		t.Fatalf("GetLoginProfile() permissions = %v, want %v", got, want)
	}
	if got := resp.GetMenus(); !slices.Equal(got, grants.Menus) {
		t.Fatalf("GetLoginProfile() menus = %v, want %v", got, grants.Menus)
	}
	if got := resp.GetElements(); !slices.Equal(got, grants.Elements) {
		t.Fatalf("GetLoginProfile() elements = %v, want %v", got, grants.Elements)
	}

	// is_admin 超管：无需枚举，返回空集合。
	root := seedAuthUser(t, store, "root")
	root.IsAdmin = true
	if err := store.UpdateUser(context.Background(), root.ID, biz.UserUpdate{IsAdmin: &root.IsAdmin}); err != nil {
		t.Fatalf("promote root: %v", err)
	}
	resp, err = svc.GetLoginProfile(claimsContext(root.ID), &v1.GetLoginProfileRequest{})
	if err != nil {
		t.Fatalf("GetLoginProfile(admin) error = %v", err)
	}
	if got := resp.GetPermissions(); len(got) != 0 {
		t.Fatalf("GetLoginProfile(admin) permissions = %v, want empty", got)
	}
	if got := resp.GetMenus(); len(got) != 0 {
		t.Fatalf("GetLoginProfile(admin) menus = %v, want empty", got)
	}
	if got := resp.GetElements(); len(got) != 0 {
		t.Fatalf("GetLoginProfile(admin) elements = %v, want empty", got)
	}
}

// TestParseIP 客户端 IP 解析：合法 v4/v6（含方括号、空白）规范化，非法串
// （超长/伪造/主机名）返回空，不回退原串入库。
func TestParseIP(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		" 10.1.2.3 ":            "10.1.2.3",
		"[2001:db8::1]":         "2001:db8::1",
		"2001:DB8:0:0::1":       "2001:db8::1",
		"evil.example.com":      "",
		"10.1.2.3, 10.0.0.1":    "",
		strings.Repeat("a", 60): "",
		"":                      "",
	}
	for in, want := range cases {
		if got := parseIP(in); got != want {
			t.Errorf("parseIP(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestResolveClientIP 客户端 IP 判定：公网直连忽略 XFF（客户端可伪造）；
// 对端为回环/内网代理时取 XFF 最右段（最近一跳代理追加），最左段不采信。
func TestResolveClientIP(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		remote string
		xff    []string
		want   string
	}{
		{"direct public ignores xff", "203.0.113.9:4711", []string{"1.2.3.4"}, "203.0.113.9"},
		{"no xff", "203.0.113.9:4711", nil, "203.0.113.9"},
		{"loopback proxy single hop", "127.0.0.1:5000", []string{"198.51.100.7"}, "198.51.100.7"},
		{"spoofed leftmost ignored", "10.0.0.2:5000", []string{"1.2.3.4, 198.51.100.7"}, "198.51.100.7"},
		{"multiple headers rightmost", "10.0.0.2:5000", []string{"1.2.3.4", "198.51.100.7"}, "198.51.100.7"},
		{"garbage xff falls back to peer", "127.0.0.1:5000", []string{"not-an-ip"}, "127.0.0.1"},
		{"proxy without xff", "[::1]:5000", nil, "::1"},
		{"unparseable remote", "bogus", []string{"1.2.3.4"}, ""},
	}
	for _, tc := range cases {
		if got := resolveClientIP(tc.remote, tc.xff); got != tc.want {
			t.Errorf("%s: resolveClientIP() = %q, want %q", tc.name, got, tc.want)
		}
	}
}
