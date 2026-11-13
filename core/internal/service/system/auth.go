package service

import (
	"context"
	"errors"
	"net"
	"strings"
	"unicode/utf8"

	"google.golang.org/genproto/googleapis/api/httpbody"

	v1 "github.com/lanyulei/reborn/backend/core/api/system/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"

	jwtx "github.com/go-kratos/kratos/contrib/middleware/jwt/v3"
	"github.com/go-kratos/kratos/v3/transport"
	khttp "github.com/go-kratos/kratos/v3/transport/http"
)

// AuthService 实现 api/system/v1 生成的 AuthServiceServer。
type AuthService struct {
	uc     *biz.AuthUsecase
	authz  *biz.AuthzUsecase
	avatar *biz.AvatarUsecase
}

// NewAuthService new an AuthService.
func NewAuthService(uc *biz.AuthUsecase, authz *biz.AuthzUsecase, avatar *biz.AvatarUsecase) *AuthService {
	return &AuthService{uc: uc, authz: authz, avatar: avatar}
}

// Login implements proto AuthService/Login：委托认证用例校验凭据并签发双令牌。
// 客户端 IP/User-Agent 在本层从传输上下文提取（biz 层不触 transport），
// 供登录审计日志落库。
func (s *AuthService) Login(ctx context.Context, req *v1.LoginRequest) (*v1.LoginResponse, error) {
	user, pair, err := s.uc.Login(ctx, req.GetUsername(), req.GetPassword(), clientIP(ctx), userAgent(ctx))
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
	unrestricted, bizScope, err := s.authz.BizScope(ctx, userID)
	if err != nil {
		return nil, err
	}
	resp := &v1.GetLoginProfileResponse{
		User:            toProtoUser(user),
		Roles:           make([]*v1.Role, 0, len(roles)),
		Permissions:     permissions,
		Menus:           menus,
		Elements:        elements,
		BizScope:        bizScope,
		BizUnrestricted: unrestricted,
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

// maxUserAgentBytes 是落库 User-Agent 的截断上限（varchar(255)，实际均为 ASCII）。
const maxUserAgentBytes = 255

// clientIP 从传输上下文提取客户端 IP（登录审计与登录失败限速的来源键）：
// 缺省取 RemoteAddr 的 host 部分；仅当直连对端是回环/内网地址（即前置
// 反向代理）时才采信 X-Forwarded-For，且只取最右一段——那是最近一跳代理
// 追加的对端地址，客户端无法伪造；最左段由客户端任意填写，采信它等于允许
// 伪造审计 IP 并绕过按 IP 限速。公网直连时 XFF 整体不可信，忽略。
// 多级代理（CDN → LB → 本服务）下取到的是上一级代理地址：无可信代理名单
// 配置，宁可保守。非 HTTP 传输取不到返回空串（DO 约定：空串=未取到，落库
// 为 NULL）。
//
// 两路取值均须经 net.ParseIP 解析成功并规范化：不校验则任意串直达 ip
// varchar(45)——超长触发 22001 让 best-effort 的登录审计行静默丢失。解析
// 失败按未取到处理，不回退使用原始串。
func clientIP(ctx context.Context) string {
	r, ok := khttp.RequestFromServerContext(ctx)
	if !ok || r == nil {
		return ""
	}
	return resolveClientIP(r.RemoteAddr, r.Header.Values("X-Forwarded-For"))
}

// resolveClientIP 是 clientIP 的纯判定部分（remoteAddr 为 host:port 或裸 host）。
func resolveClientIP(remoteAddr string, forwardedFor []string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	peer := parseIP(host)
	if p := net.ParseIP(peer); p != nil && (p.IsLoopback() || p.IsPrivate()) {
		// 多个 XFF 头按出现顺序拼接，最右段恒为最近一跳代理追加。
		hops := strings.Split(strings.Join(forwardedFor, ","), ",")
		if ip := parseIP(hops[len(hops)-1]); ip != "" {
			return ip
		}
	}
	return peer
}

// parseIP 解析并规范化 IP 字面量（去空白；兼容 "[v6]" 方括号形态）；非法
// 返回空串。
func parseIP(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimSuffix(strings.TrimPrefix(s, "["), "]")
	ip := net.ParseIP(s)
	if ip == nil {
		return ""
	}
	return ip.String()
}

// userAgent 从传输上下文提取客户端 User-Agent（经 transport 层统一 Header
// 接口），超长截断至落库上限；取不到返回空串。
func userAgent(ctx context.Context) string {
	tr, ok := transport.FromServerContext(ctx)
	if !ok || tr == nil {
		return ""
	}
	return truncate(tr.RequestHeader().Get("User-Agent"))
}

// truncate 按 UTF-8 边界截断字符串至 maxUserAgentBytes 字节。
func truncate(s string) string {
	if len(s) <= maxUserAgentBytes {
		return s
	}
	s = s[:maxUserAgentBytes]
	// 末字节非 rune 首字节说明切在多字节序列中间，回退至完整 rune 边界。
	for len(s) > 0 && !utf8.RuneStart(s[len(s)-1]) {
		s = s[:len(s)-1]
	}
	return s
}

// UploadMyAvatar implements proto AuthService/UploadMyAvatar：当前登录用户自助上传头像。
func (s *AuthService) UploadMyAvatar(ctx context.Context, req *v1.UploadMyAvatarRequest) (*v1.UploadMyAvatarResponse, error) {
	userID, err := UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	url, err := s.avatar.Upload(ctx, userID, req.GetData(), req.GetMime())
	if err != nil {
		return nil, err
	}
	return &v1.UploadMyAvatarResponse{Url: url}, nil
}

// GetUserAvatar implements proto AuthService/GetUserAvatar：以 HttpBody 返回头像
// （<img> 直载，匿名可读）。nosniff + CSP sandbox 与 logo 同口径防御。
func (s *AuthService) GetUserAvatar(ctx context.Context, req *v1.GetUserAvatarRequest) (*httpbody.HttpBody, error) {
	data, mime, err := s.avatar.Get(ctx, req.GetId())
	if err != nil {
		if errors.Is(err, biz.ErrAvatarNotFound) {
			return nil, v1.ErrorAvatarNotFound("用户未设置头像")
		}
		return nil, err
	}
	if tr, ok := transport.FromServerContext(ctx); ok {
		tr.ReplyHeader().Set("X-Content-Type-Options", "nosniff")
		tr.ReplyHeader().Set("Content-Security-Policy", "sandbox")
		tr.ReplyHeader().Set("Cache-Control", "public, max-age=31536000, immutable")
	}
	return &httpbody.HttpBody{ContentType: mime, Data: data}, nil
}
