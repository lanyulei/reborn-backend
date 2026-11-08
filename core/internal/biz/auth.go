package biz

import (
	"context"
	"fmt"
	"time"

	v1 "github.com/lanyulei/monitor/backend/core/api/system/v1"

	"github.com/go-kratos/kratos/v3/errors"
)

// dummyPasswordSeed 是防枚举时序对齐用的占位明文：仅用于构造一个与真实用户
// 哈希同参数的 Argon2id PHC 串，使「用户不存在」路径与真实校验路径等代价。
//
// 时序校准种子，泄漏无安全影响（哈希结果同样不构成凭据）。
//
//nolint:gosec // G101 误报：不是任何环境的凭据或密钥，仅是喂给 Argon2id 的
const dummyPasswordSeed = "timing-align-dummy-seed#2026"

// 认证域哨兵错误：跨层业务错误统一携带 Kratos code/reason，
// reason 与 api/system/v1 的 ErrorReason 枚举对齐（单一事实来源）。
var (
	// ErrInvalidCredentials 用户名或密码错误；不区分用户不存在与密码错误（防枚举）。
	ErrInvalidCredentials = errors.Unauthorized(v1.ErrorReason_INVALID_CREDENTIALS.String(), "invalid username or password")
	// ErrAccountDisabled 账号非可用状态（停用/暂停/离职）；仅在凭据或令牌校验通过后返回。
	ErrAccountDisabled = errors.Forbidden(v1.ErrorReason_ACCOUNT_DISABLED.String(), "account is not enabled")
	// ErrTokenInvalid 令牌缺失、无效或已被吊销。
	ErrTokenInvalid = errors.Unauthorized(v1.ErrorReason_TOKEN_INVALID.String(), "token is invalid")
	// ErrTokenExpired 令牌已过期；客户端可凭有效刷新令牌静默换发。
	ErrTokenExpired = errors.Unauthorized(v1.ErrorReason_TOKEN_EXPIRED.String(), "token has expired")
)

// RefreshToken 是刷新令牌领域模型（DO）；TokenHash 为原文的 SHA-256 hex，
// 原文仅在签发响应中出现一次，任何路径不得持久化原文。
type RefreshToken struct {
	ID         string
	UserID     string
	TokenHash  string
	ExpireTime time.Time
	CreateTime time.Time
}

// RefreshTokenReader 是刷新令牌只读仓储接口，由 data 层实现（依赖倒置）。
type RefreshTokenReader interface {
	// FindByHash 按哈希查询令牌；不存在返回 ErrTokenInvalid。
	FindByHash(ctx context.Context, tokenHash string) (*RefreshToken, error)
}

// RefreshTokenWriter 是刷新令牌写仓储接口，由 data 层实现（依赖倒置）。
// 轮换/登出/删号一律物理删除：行的存在即代表凭证有效，非业务实体不做软删。
type RefreshTokenWriter interface {
	// Save 插入新令牌，并同事务惰性清理该用户已过期的历史行。
	Save(ctx context.Context, token *RefreshToken) error
	// Rotate 原子轮换：删除旧哈希行（不存在或已被轮换返回 ErrTokenInvalid）并插入新行。
	Rotate(ctx context.Context, oldHash string, next *RefreshToken) error
	// DeleteByUser 删除该用户全部刷新令牌（登出/账号不可用）；幂等，删除 0 行不是错误。
	DeleteByUser(ctx context.Context, userID string) error
}

// TokenPair 是一次签发产出的双令牌：AccessToken 置于 Authorization: Bearer
// 使用；RefreshToken 原文仅本次返回，客户端须妥善保存。
type TokenPair struct {
	AccessToken  string
	ExpiresIn    int64
	RefreshToken string
}

// AuthUsecase 是认证用例：登录校验、双令牌签发/轮换/吊销与当前登录者查询。
type AuthUsecase struct {
	userReader    UserReader
	roles         UserRoleRepo
	refreshReader RefreshTokenReader
	refreshWriter RefreshTokenWriter
	hasher        *PasswordHasher
	signer        *TokenSigner
	dummyHash     string // 防枚举时序对齐用的占位 PHC 串，构造时一次性生成
}

// NewAuthUsecase new an AuthUsecase. 构造时预生成占位哈希（一次 Argon2id 开销），
// 避免「用户不存在」路径与真实校验路径的时序差异泄漏账号存在性。
func NewAuthUsecase(userReader UserReader, roles UserRoleRepo,
	refreshReader RefreshTokenReader, refreshWriter RefreshTokenWriter,
	hasher *PasswordHasher, signer *TokenSigner,
) (*AuthUsecase, error) {
	dummyHash, err := hasher.Hash(dummyPasswordSeed)
	if err != nil {
		return nil, fmt.Errorf("build dummy password hash: %w", err)
	}
	return &AuthUsecase{
		userReader:    userReader,
		roles:         roles,
		refreshReader: refreshReader,
		refreshWriter: refreshWriter,
		hasher:        hasher,
		signer:        signer,
		dummyHash:     dummyHash,
	}, nil
}

// Login 校验用户名密码并签发双令牌，返回登录用户（不含密码）与令牌对。
// 密码接受 b64$ 编码值或明文（解码后断言 8-64 位）。错误口径：用户不存在与
// 密码错误统一 ErrInvalidCredentials（用户不存在时仍执行一次等代价的 Argon2id
// 校验对齐时序）；凭据正确但账号非可用状态返回 ErrAccountDisabled。
// 出参任何路径不落密码与令牌。
func (uc *AuthUsecase) Login(ctx context.Context, username, password string) (*User, *TokenPair, error) {
	password, err := decodePassword(password)
	if err != nil {
		return nil, nil, err
	}
	u, err := uc.userReader.GetUserByUsername(ctx, username)
	if err != nil {
		if !errors.Is(err, ErrUserNotFound) {
			return nil, nil, fmt.Errorf("get user by username: %w", err)
		}
		uc.hasher.Verify(password, uc.dummyHash)
		return nil, nil, ErrInvalidCredentials
	}
	if !uc.hasher.Verify(password, u.Password) {
		return nil, nil, ErrInvalidCredentials
	}
	if u.Status != StatusEnabled {
		return nil, nil, ErrAccountDisabled
	}
	pair, err := uc.issueTokens(ctx, u, time.Now())
	if err != nil {
		return nil, nil, err
	}
	u.Password = ""
	return u, pair, nil
}

// Refresh 用刷新令牌原文换发新双令牌；旧令牌原子轮换即刻失效。
// 未知/已轮换/已登出的令牌返回 ErrTokenInvalid；已过期返回 ErrTokenExpired
// （过期行不即时删除，由该用户下次签发时惰性清理）；账号已删除或不可用时
// 吊销其全部令牌后分别返回 ErrTokenInvalid / ErrAccountDisabled。
func (uc *AuthUsecase) Refresh(ctx context.Context, raw string) (*TokenPair, error) {
	old, err := uc.refreshReader.FindByHash(ctx, hashRefreshToken(raw))
	if err != nil {
		if errors.Is(err, ErrTokenInvalid) {
			return nil, ErrTokenInvalid
		}
		return nil, fmt.Errorf("find refresh token: %w", err)
	}
	if old.ExpireTime.Before(time.Now()) {
		return nil, ErrTokenExpired
	}
	u, err := uc.userReader.GetUser(ctx, old.UserID)
	if err != nil {
		if !errors.Is(err, ErrUserNotFound) {
			return nil, fmt.Errorf("get user: %w", err)
		}
		if derr := uc.refreshWriter.DeleteByUser(ctx, old.UserID); derr != nil {
			return nil, fmt.Errorf("revoke refresh tokens of deleted user: %w", derr)
		}
		return nil, ErrTokenInvalid
	}
	if u.Status != StatusEnabled {
		if derr := uc.refreshWriter.DeleteByUser(ctx, u.ID); derr != nil {
			return nil, fmt.Errorf("revoke refresh tokens of disabled user: %w", derr)
		}
		return nil, ErrAccountDisabled
	}

	// 先签发（无状态、失败无害）后轮换：轮换成功前旧令牌始终有效，失败可重试；
	// 并发轮换的败者由 Rotate 返回 ErrTokenInvalid（检测到重放，强制重新登录）。
	now := time.Now()
	access, expiresIn, err := uc.signer.SignAccess(u.ID, u.Username, now)
	if err != nil {
		return nil, err
	}
	nextRaw, err := uc.signer.NewRefreshToken()
	if err != nil {
		return nil, err
	}
	nextID, err := newID()
	if err != nil {
		return nil, err
	}
	next := &RefreshToken{
		ID:         nextID,
		UserID:     u.ID,
		TokenHash:  hashRefreshToken(nextRaw),
		ExpireTime: now.Add(uc.signer.RefreshTTL()),
		CreateTime: now,
	}
	if err := uc.refreshWriter.Rotate(ctx, old.TokenHash, next); err != nil {
		if errors.Is(err, ErrTokenInvalid) {
			return nil, ErrTokenInvalid
		}
		return nil, fmt.Errorf("rotate refresh token: %w", err)
	}
	return &TokenPair{AccessToken: access, ExpiresIn: expiresIn, RefreshToken: nextRaw}, nil
}

// Logout 吊销当前用户全部刷新令牌。access token 无状态不做吊销，其剩余有效期
// （典型 ≤15m）为登出后的固有残留窗口，由短 access_ttl 缓解。
func (uc *AuthUsecase) Logout(ctx context.Context, userID string) error {
	if err := uc.refreshWriter.DeleteByUser(ctx, userID); err != nil {
		return fmt.Errorf("revoke refresh tokens: %w", err)
	}
	return nil
}

// Profile 查询当前登录用户信息及绑定角色；用户不存在返回 ErrUserNotFound。
// 有效授权并集由 AuthzUsecase（接口操作）与 PermissionUsecase（菜单/元素
// 直接授权）口径提供，认证域不再自行计算。
func (uc *AuthUsecase) Profile(ctx context.Context, userID string) (*User, []*Role, error) {
	u, err := uc.userReader.GetUser(ctx, userID)
	if err != nil {
		return nil, nil, err
	}
	roles, err := uc.roles.ListUserRoles(ctx, userID)
	if err != nil {
		return nil, nil, fmt.Errorf("list user roles: %w", err)
	}
	return u, roles, nil
}

// issueTokens 签发 access + refresh 双令牌并落库刷新令牌哈希。
func (uc *AuthUsecase) issueTokens(ctx context.Context, u *User, now time.Time) (*TokenPair, error) {
	access, expiresIn, err := uc.signer.SignAccess(u.ID, u.Username, now)
	if err != nil {
		return nil, err
	}
	raw, err := uc.signer.NewRefreshToken()
	if err != nil {
		return nil, err
	}
	id, err := newID()
	if err != nil {
		return nil, err
	}
	rt := &RefreshToken{
		ID:         id,
		UserID:     u.ID,
		TokenHash:  hashRefreshToken(raw),
		ExpireTime: now.Add(uc.signer.RefreshTTL()),
		CreateTime: now,
	}
	if err := uc.refreshWriter.Save(ctx, rt); err != nil {
		return nil, fmt.Errorf("save refresh token: %w", err)
	}
	return &TokenPair{AccessToken: access, ExpiresIn: expiresIn, RefreshToken: raw}, nil
}
