package service

import (
	"context"

	v1 "github.com/lanyulei/monitor/backend/core/api/system/v1"
	"github.com/lanyulei/monitor/backend/core/internal/biz"

	jwtx "github.com/go-kratos/kratos/contrib/middleware/jwt/v3"
)

// AuthService 实现 api/system/v1 生成的 AuthServiceServer。
type AuthService struct {
	v1.UnimplementedAuthServiceServer

	uc    *biz.AuthUsecase
	authz *biz.AuthzUsecase
}

// NewAuthService new an AuthService.
func NewAuthService(uc *biz.AuthUsecase, authz *biz.AuthzUsecase) *AuthService {
	return &AuthService{uc: uc, authz: authz}
}

// Login implements proto AuthService/Login：委托认证用例校验凭据并签发双令牌。
func (s *AuthService) Login(ctx context.Context, req *v1.LoginRequest) (*v1.LoginResponse, error) {
	user, pair, err := s.uc.Login(ctx, req.GetUsername(), req.GetPassword())
	if err != nil {
		return nil, err
	}
	return &v1.LoginResponse{
		AccessToken:  pair.AccessToken,
		ExpiresIn:    pair.ExpiresIn,
		RefreshToken: pair.RefreshToken,
		User:         toProtoUser(user),
	}, nil
}

// RefreshToken implements proto AuthService/RefreshToken：旧刷新令牌原子轮换。
func (s *AuthService) RefreshToken(ctx context.Context, req *v1.RefreshTokenRequest) (*v1.RefreshTokenResponse, error) {
	pair, err := s.uc.Refresh(ctx, req.GetRefreshToken())
	if err != nil {
		return nil, err
	}
	return &v1.RefreshTokenResponse{
		AccessToken:  pair.AccessToken,
		ExpiresIn:    pair.ExpiresIn,
		RefreshToken: pair.RefreshToken,
	}, nil
}

// Logout implements proto AuthService/Logout：用户身份取自鉴权中间件注入的 claims。
func (s *AuthService) Logout(ctx context.Context, _ *v1.LogoutRequest) (*v1.LogoutResponse, error) {
	userID, err := UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.uc.Logout(ctx, userID); err != nil {
		return nil, err
	}
	return &v1.LogoutResponse{}, nil
}

// GetLoginProfile implements proto AuthService/GetLoginProfile：用户身份取自鉴权中间件注入的 claims。
func (s *AuthService) GetLoginProfile(ctx context.Context, _ *v1.GetLoginProfileRequest) (*v1.GetLoginProfileResponse, error) {
	userID, err := UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	user, roles, err := s.uc.Profile(ctx, userID)
	if err != nil {
		return nil, err
	}
	// 有效授权并集与 authz 中间件同口径，供前端菜单/路由可见性（/me 为
	// 免授权自助接口，普通用户无其它途径获知自身权限）。
	permissions, err := s.authz.EffectiveOperations(ctx, userID)
	if err != nil {
		return nil, err
	}
	menus, elements, err := s.authz.EffectiveGrants(ctx, userID)
	if err != nil {
		return nil, err
	}
	resp := &v1.GetLoginProfileResponse{
		User:        toProtoUser(user),
		Roles:       make([]*v1.Role, 0, len(roles)),
		Permissions: permissions,
		Menus:       menus,
		Elements:    elements,
	}
	for _, r := range roles {
		resp.Roles = append(resp.Roles, toProtoRole(r))
	}
	return resp, nil
}

// UserIDFromContext 从鉴权中间件注入的 JWT claims 提取用户 ID；
// claims 缺失或类型不符按无效令牌拒绝。鉴权身份一律以 Subject（用户 ID）为准。
// 除本包外亦供 server 层授权中间件复用（单一实现，勿另写副本）。
func UserIDFromContext(ctx context.Context) (string, error) {
	claims, ok := jwtx.FromContext(ctx)
	if !ok {
		return "", biz.ErrTokenInvalid
	}
	ac, ok := claims.(*biz.AccessClaims)
	if !ok || ac.Subject == "" {
		return "", biz.ErrTokenInvalid
	}
	return ac.Subject, nil
}
