package data

import (
	"context"
	"errors"
	"fmt"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

// 编译期断言锁住 biz 契约：接口签名漂移时在此处立即暴露（AGENTS.md §4.1）。
var (
	_ biz.RefreshTokenReader = (*refreshTokenRepo)(nil)
	_ biz.RefreshTokenWriter = (*refreshTokenRepo)(nil)
)

// refreshTokenPO 是 system_refresh_token 表的持久化对象；与 biz.RefreshToken
// 显式转换，禁止结构体复用跨层传递。轮换置 rotated_at 留墓碑（重放检测，
// 保留至原过期时间）；登出/删号/整族吊销物理删除，过期行由 Save/Rotate 惰性清理。
type refreshTokenPO struct {
	id         string
	userID     string
	familyID   string
	tokenHash  string
	expireTime time.Time
	createTime time.Time
	rotatedAt  *time.Time
}

// toEntity 将 PO 转换为领域模型（DO）。
func (p refreshTokenPO) toEntity() biz.RefreshToken {
	return biz.RefreshToken{
		ID:         p.id,
		UserID:     p.userID,
		FamilyID:   p.familyID,
		TokenHash:  p.tokenHash,
		ExpireTime: p.expireTime,
		CreateTime: p.createTime,
		RotatedAt:  p.rotatedAt,
	}
}

// refreshTokenRepo 是刷新令牌仓储的 PostgreSQL 实现：手写参数化 SQL，无 ORM/反射。
type refreshTokenRepo struct {
	data *basedata.Data
}

// NewRefreshTokenRepo new a refreshTokenRepo.
func NewRefreshTokenRepo(data *basedata.Data) *refreshTokenRepo {
	return &refreshTokenRepo{data: data}
}

// insertRefreshToken 在事务内插入令牌行（审计时间由用例统一生成并显式落库）。
// FamilyID 为空时族即自身（NULLIF → NULL，读取处 COALESCE(family_id, id)）。
func insertRefreshToken(ctx context.Context, tx pgx.Tx, t *biz.RefreshToken) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO system_refresh_token (id, user_id, family_id, token_hash, expire_time, create_time)
		 VALUES ($1, $2, NULLIF($3, '')::uuid, $4, $5, $6)`,
		t.ID, t.UserID, t.FamilyID, t.TokenHash, t.ExpireTime, t.CreateTime)
	if err != nil {
		// token_hash 唯一冲突（概率 2^-256）与外键违约均按基础设施错误原样上抛。
		return fmt.Errorf("insert refresh token: %w", err)
	}
	return nil
}

// FindByHash implements biz.RefreshTokenReader；不存在返回 ErrTokenInvalid。
// 已轮换的墓碑行同样返回（RotatedAt 非空），由用例判定并发竞态/重放。
func (r *refreshTokenRepo) FindByHash(ctx context.Context, tokenHash string) (*biz.RefreshToken, error) {
	var p refreshTokenPO
	err := r.data.Pool.QueryRow(ctx,
		`SELECT id, user_id, COALESCE(family_id, id), token_hash, expire_time, create_time, rotated_at
		 FROM system_refresh_token WHERE token_hash = $1`,
		tokenHash).
		Scan(&p.id, &p.userID, &p.familyID, &p.tokenHash, &p.expireTime, &p.createTime, &p.rotatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrTokenInvalid
		}
		return nil, fmt.Errorf("find refresh token: %w", err)
	}
	t := p.toEntity()
	return &t, nil
}

// Save implements biz.RefreshTokenWriter：插入新令牌，并同事务惰性清理该用户
// 已过期的历史行（无后台清理任务）。
func (r *refreshTokenRepo) Save(ctx context.Context, token *biz.RefreshToken) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			"DELETE FROM system_refresh_token WHERE user_id = $1 AND expire_time < now()", token.UserID); err != nil {
			return fmt.Errorf("purge expired refresh tokens: %w", err)
		}
		return insertRefreshToken(ctx, tx, token)
	})
}

// Rotate implements biz.RefreshTokenWriter：原子轮换——同事务把旧哈希行置为墓碑
// （rotated_at = next.CreateTime，保留至原过期时间供重放检测）并插入新行；
// 旧行不存在或已是墓碑（未知/已被轮换/已登出）返回 ErrTokenInvalid，天然覆盖
// 并发轮换竞争（仅一方能把 rotated_at 从 NULL 置位）。顺带惰性清理该用户
// 已过期的行（含过期墓碑），长期只刷新不登录的会话也不会无界累积。
func (r *refreshTokenRepo) Rotate(ctx context.Context, oldHash string, next *biz.RefreshToken) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			"UPDATE system_refresh_token SET rotated_at = $2 WHERE token_hash = $1 AND rotated_at IS NULL",
			oldHash, next.CreateTime)
		if err != nil {
			return fmt.Errorf("tombstone old refresh token: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return biz.ErrTokenInvalid
		}
		if _, err := tx.Exec(ctx,
			"DELETE FROM system_refresh_token WHERE user_id = $1 AND expire_time < now()", next.UserID); err != nil {
			return fmt.Errorf("purge expired refresh tokens: %w", err)
		}
		return insertRefreshToken(ctx, tx, next)
	})
}

// DeleteByFamily implements biz.RefreshTokenWriter：删除整族令牌（含墓碑）；
// 幂等。family_id 为 NULL 的行族即自身 id，故同时匹配 id。
func (r *refreshTokenRepo) DeleteByFamily(ctx context.Context, familyID string) error {
	if _, err := r.data.Pool.Exec(ctx,
		"DELETE FROM system_refresh_token WHERE family_id = $1 OR id = $1", familyID); err != nil {
		return fmt.Errorf("delete refresh tokens by family: %w", err)
	}
	return nil
}

// DeleteByUser implements biz.RefreshTokenWriter：删除该用户全部刷新令牌（含墓碑）；幂等，
// 删除 0 行不是错误。
func (r *refreshTokenRepo) DeleteByUser(ctx context.Context, userID string) error {
	if _, err := r.data.Pool.Exec(ctx,
		"DELETE FROM system_refresh_token WHERE user_id = $1", userID); err != nil {
		return fmt.Errorf("delete refresh tokens by user: %w", err)
	}
	return nil
}
