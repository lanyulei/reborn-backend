//go:build integration

package data

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"

	"github.com/go-kratos/kratos/v3/errors"
)

// hashRaw 与 biz 层 hashRefreshToken 同算法（SHA-256 hex），测试侧独立计算
// 以校验落库值确为原文哈希。
func hashRaw(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

// newRefreshToken 构造固定 ID 的刷新令牌 DO（哈希由原文派生）。
func newRefreshToken(id, userID, raw string, expireIn time.Duration) *biz.RefreshToken {
	now := time.Now().UTC()
	return &biz.RefreshToken{
		ID:         id,
		UserID:     userID,
		TokenHash:  hashRaw(raw),
		ExpireTime: now.Add(expireIn),
		CreateTime: now,
	}
}

// TestIntegration_RefreshTokenRepo 刷新令牌仓储契约：哈希查询、原子轮换
// （重放拒绝）、按用户吊销幂等、Save 惰性清理同用户过期行。
func TestIntegration_RefreshTokenRepo(t *testing.T) {
	d := newIntegrationData(t)
	users := NewUserRepo(d)
	repo := NewRefreshTokenRepo(d)
	ctx := context.Background()

	user := newTestUser("018f3e1a-0002-7000-8000-000000000001", "auth-user")
	if err := users.CreateUser(ctx, user); err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}

	// Save + FindByHash：落库哈希，出参字段完整。
	rt := newRefreshToken("018f3e1a-0003-7000-8000-000000000001", user.ID, "raw-alpha", time.Hour)
	if err := repo.Save(ctx, rt); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	got, err := repo.FindByHash(ctx, hashRaw("raw-alpha"))
	if err != nil {
		t.Fatalf("FindByHash() error = %v", err)
	}
	if got.ID != rt.ID || got.UserID != user.ID || got.TokenHash != rt.TokenHash {
		t.Fatalf("FindByHash() = %+v, want %+v", got, rt)
	}
	if _, err := repo.FindByHash(ctx, hashRaw("raw-unknown")); !errors.Is(err, biz.ErrTokenInvalid) {
		t.Fatalf("FindByHash(unknown) error = %v, want ErrTokenInvalid", err)
	}

	// Rotate：旧哈希即刻失效，新哈希可查；旧哈希重放（并发败者）拒绝。
	next := newRefreshToken("018f3e1a-0003-7000-8000-000000000002", user.ID, "raw-beta", time.Hour)
	if err := repo.Rotate(ctx, rt.TokenHash, next); err != nil {
		t.Fatalf("Rotate() error = %v", err)
	}
	if _, err := repo.FindByHash(ctx, rt.TokenHash); !errors.Is(err, biz.ErrTokenInvalid) {
		t.Fatalf("FindByHash(rotated old) error = %v, want ErrTokenInvalid", err)
	}
	if _, err := repo.FindByHash(ctx, next.TokenHash); err != nil {
		t.Fatalf("FindByHash(rotated new) error = %v", err)
	}
	if err := repo.Rotate(ctx, rt.TokenHash, next); !errors.Is(err, biz.ErrTokenInvalid) {
		t.Fatalf("Rotate(replay) error = %v, want ErrTokenInvalid", err)
	}

	// Save 惰性清理：先落一行已过期，再为新令牌 Save 时同用户过期行被清除。
	expired := newRefreshToken("018f3e1a-0003-7000-8000-000000000003", user.ID, "raw-stale", -time.Minute)
	if err := repo.Save(ctx, expired); err != nil {
		t.Fatalf("Save(expired row) error = %v", err)
	}
	fresh := newRefreshToken("018f3e1a-0003-7000-8000-000000000004", user.ID, "raw-fresh", time.Hour)
	if err := repo.Save(ctx, fresh); err != nil {
		t.Fatalf("Save(fresh) error = %v", err)
	}
	if _, err := repo.FindByHash(ctx, expired.TokenHash); !errors.Is(err, biz.ErrTokenInvalid) {
		t.Fatalf("FindByHash(purged expired) error = %v, want ErrTokenInvalid (lazy purge)", err)
	}
	if _, err := repo.FindByHash(ctx, fresh.TokenHash); err != nil {
		t.Fatalf("FindByHash(fresh) error = %v", err)
	}

	// DeleteByUser：清空该用户全部行，幂等。
	if err := repo.DeleteByUser(ctx, user.ID); err != nil {
		t.Fatalf("DeleteByUser() error = %v", err)
	}
	if _, err := repo.FindByHash(ctx, fresh.TokenHash); !errors.Is(err, biz.ErrTokenInvalid) {
		t.Fatalf("FindByHash(after logout) error = %v, want ErrTokenInvalid", err)
	}
	if err := repo.DeleteByUser(ctx, user.ID); err != nil {
		t.Fatalf("DeleteByUser(idempotent) error = %v", err)
	}

	// DeleteUser 联动删刷新令牌（删号即吊销全部凭证）。
	if err := repo.Save(ctx, fresh); err != nil {
		t.Fatalf("Save(reset fresh) error = %v", err)
	}
	if err := users.DeleteUser(ctx, user.ID); err != nil {
		t.Fatalf("DeleteUser() error = %v", err)
	}
	if _, err := repo.FindByHash(ctx, fresh.TokenHash); !errors.Is(err, biz.ErrTokenInvalid) {
		t.Fatalf("FindByHash(after user delete) error = %v, want ErrTokenInvalid", err)
	}
}
