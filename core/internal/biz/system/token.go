package biz

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/lanyulei/reborn/backend/core/internal/conf"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

// JWT 契约常量：HS256 对称签名；密钥长度按 RFC 8725 §3.5 对 HMAC-SHA256
// 的强度下限（≥32 字节）在构造时校验。
const (
	minJWTSecretLen = 32
	maxJWTSecretLen = 512
	minAccessTTL    = time.Minute
	// refreshEntropyBytes 刷新令牌原文熵源长度，hex 后为 64 字符。
	refreshEntropyBytes = 32
)

// AccessClaims 是访问令牌声明：标准声明（sub=用户ID / iss / jti / iat / exp）
// 外加 Username 自定义声明。实现 jwt.Claims，供鉴权中间件注入与业务层断言。
type AccessClaims struct {
	jwt.RegisteredClaims
	// Username 登录名，仅作展示/日志关联，鉴权身份一律以 Subject（用户 ID）为准。
	Username string `json:"username"`
}

// TokenSigner 是访问令牌（JWT/HS256）签发与校验器；签名密钥与有效期参数从
// 配置注入，构造时校验（装配阶段快速失败，禁止弱密钥静默生效）。
type TokenSigner struct {
	secret     []byte
	accessTTL  time.Duration
	refreshTTL time.Duration
	issuer     string
}

// NewTokenSigner 按配置构造签发器；jwt 段缺失、密钥长度越界、TTL 低于下限、
// refresh_ttl 不大于 access_ttl、issuer 为空均返回错误。
func NewTokenSigner(c *conf.Security) (*TokenSigner, error) {
	j := c.GetJwt()
	if j == nil {
		return nil, fmt.Errorf("jwt config is required")
	}
	secret := j.GetSecret()
	if len(secret) < minJWTSecretLen {
		return nil, fmt.Errorf("jwt secret must be %d-%d bytes (set KRATOS_SECURITY_JWT_SECRET)", minJWTSecretLen, maxJWTSecretLen)
	}
	if len(secret) > maxJWTSecretLen {
		return nil, fmt.Errorf("jwt secret must be %d-%d bytes", minJWTSecretLen, maxJWTSecretLen)
	}
	accessTTL := j.GetAccessTtl().AsDuration()
	if accessTTL < minAccessTTL {
		return nil, fmt.Errorf("jwt access_ttl must be at least %s", minAccessTTL)
	}
	refreshTTL := j.GetRefreshTtl().AsDuration()
	if refreshTTL <= accessTTL {
		return nil, fmt.Errorf("jwt refresh_ttl %s must be greater than access_ttl %s", refreshTTL, accessTTL)
	}
	if issuer := j.GetIssuer(); issuer == "" {
		return nil, fmt.Errorf("jwt issuer is required")
	}
	return &TokenSigner{
		secret:     []byte(secret),
		accessTTL:  accessTTL,
		refreshTTL: refreshTTL,
		issuer:     j.GetIssuer(),
	}, nil
}

// AccessTTL 返回访问令牌有效期。
func (s *TokenSigner) AccessTTL() time.Duration { return s.accessTTL }

// RefreshTTL 返回刷新令牌有效期。
func (s *TokenSigner) RefreshTTL() time.Duration { return s.refreshTTL }

// SignAccess 以 HS256 签发访问令牌；now 由调用方注入，便于测试确定性构造
// 过期/临期令牌。返回令牌串与其有效期（秒）。
func (s *TokenSigner) SignAccess(userID, username string, now time.Time) (string, int64, error) {
	claims := AccessClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    s.issuer,
			Subject:   userID,
			ID:        uuid.NewString(),
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(s.accessTTL)),
		},
		Username: username,
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(s.secret)
	if err != nil {
		return "", 0, fmt.Errorf("sign access token: %w", err)
	}
	return token, int64(s.accessTTL / time.Second), nil
}

// ParseAccess 解析并校验访问令牌；校验集与鉴权中间件完全一致
// （HS256 / 签发方 / 有效期），二者行为互为镜像。
func (s *TokenSigner) ParseAccess(token string) (*AccessClaims, error) {
	var claims AccessClaims
	if _, err := jwt.ParseWithClaims(token, &claims, s.KeyFunc(), s.ParserOptions()...); err != nil {
		return nil, fmt.Errorf("parse access token: %w", err)
	}
	return &claims, nil
}

// KeyFunc 返回中间件/解析器共用的密钥函数；仅接受 HMAC 签名方法族，
// 拒绝 alg 混淆（如伪造 RS256/none 头绕过密钥校验）。
func (s *TokenSigner) KeyFunc() jwt.Keyfunc {
	return func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method %v", t.Header["alg"])
		}
		return s.secret, nil
	}
}

// ClaimsFactory 返回访问令牌声明构造函数；jwt 中间件要求每次解析注入新实例
// （解析结果复用同一个 Claims 会跨请求串写）。
func (s *TokenSigner) ClaimsFactory() func() jwt.Claims {
	return func() jwt.Claims { return &AccessClaims{} }
}

// ParserOptions 返回与签发侧一致的解析校验选项：签名方法白名单 + 签发方校验。
func (s *TokenSigner) ParserOptions() []jwt.ParserOption {
	return []jwt.ParserOption{
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithIssuer(s.issuer),
	}
}

// NewRefreshToken 生成刷新令牌原文：crypto/rand 32 字节 hex（64 字符）。
// 原文仅签发响应中出现一次，服务端只保存其 SHA-256 哈希。
func (s *TokenSigner) NewRefreshToken() (string, error) {
	b := make([]byte, refreshEntropyBytes)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("read random refresh token: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// hashRefreshToken 计算刷新令牌原文的 SHA-256 hex（64 字符），是落库的唯一凭据。
func hashRefreshToken(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}
