package data

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lanyulei/monitor/backend/core/internal/biz"

	"github.com/jackc/pgx/v5"
)

// 编译期断言锁住 biz 契约：接口签名漂移时在此处立即暴露（AGENTS.md §4.1）。
var (
	_ biz.RefreshTokenReader = (*refreshTokenRepo)(nil)
	_ biz.RefreshTokenWriter = (*refreshTokenRepo)(nil)
)

// refreshTokenPO 是 system_refresh_token 表的持久化对象；与 biz.RefreshToken
// 显式转换，禁止结构体复用跨层传递。该表不做软删（行的存在即代表凭证有效），
// 轮换/登出/删号物理删除，过期行由 Save 惰性清理。
type refreshTokenPO struct {
	id         string
	userID     string
	tokenHash  string
	expireTime time.Time
	createTime time.Time
}

// toEntity 将 PO 转换为领域模型（DO）。
func (p refreshTokenPO) toEntity() biz.RefreshToken {
	return biz.RefreshToken{
		ID:         p.id,
		UserID:     p.userID,
		TokenHash:  p.tokenHash,
		ExpireTime: p.expireTime,
		CreateTime: p.createTime,
	}
}

// refreshTokenRepo 是刷新令牌仓储的 PostgreSQL 实现：手写参数化 SQL，无 ORM/反射。
type refreshTokenRepo struct {
	data *Data
}

// NewRefreshTokenRepo new a refreshTokenRepo.
func NewRefreshTokenRepo(data *Data) *refreshTokenRepo {
	return &refreshTokenRepo{data: data}
}

// insertRefreshToken 在事务内插入令牌行（审计时间由用例统一生成并显式落库）。
func insertRefreshToken(ctx context.Context, tx pgx.Tx, t *biz.RefreshToken) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO system_refresh_token (id, user_id, token_hash, expire_time, create_time)
		 VALUES ($1, $2, $3, $4, $5)`,
		t.ID, t.UserID, t.TokenHash, t.ExpireTime, t.CreateTime)
	if err != nil {
		// token_hash 唯一冲突（概率 2^-256）与外键违约均按基础设施错误原样上抛。
		return fmt.Errorf("insert refresh token: %w", err)
	}
	return nil
}

// FindByHash implements biz.RefreshTokenReader；不存在返回 ErrTokenInvalid。
func (r *refreshTokenRepo) FindByHash(ctx context.Context, tokenHash string) (*biz.RefreshToken, error) {
	var p refreshTokenPO
	err := r.data.pool.QueryRow(ctx,
		"SELECT id, user_id, token_hash, expire_time, create_time FROM system_refresh_token WHERE token_hash = $1",
		tokenHash).
		Scan(&p.id, &p.userID, &p.tokenHash, &p.expireTime, &p.createTime)
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
	tx, err := r.data.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx,
		"DELETE FROM system_refresh_token WHERE user_id = $1 AND expire_time < now()", token.UserID); err != nil {
		return fmt.Errorf("purge expired refresh tokens: %w", err)
	}
	if err := insertRefreshToken(ctx, tx, token); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Rotate implements biz.RefreshTokenWriter：原子轮换——同事务删除旧哈希行并插入
// 新行；旧行不存在（未知/已被轮换/已登出）返回 ErrTokenInvalid，天然覆盖
// 并发轮换竞争（败者检测到令牌重放，强制重新登录）。
func (r *refreshTokenRepo) Rotate(ctx context.Context, oldHash string, next *biz.RefreshToken) error {
	tx, err := r.data.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, "DELETE FROM system_refresh_token WHERE token_hash = $1", oldHash)
	if err != nil {
		return fmt.Errorf("delete old refresh token: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrTokenInvalid
	}
	if err := insertRefreshToken(ctx, tx, next); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// DeleteByUser implements biz.RefreshTokenWriter：删除该用户全部刷新令牌；幂等，
// 删除 0 行不是错误。
func (r *refreshTokenRepo) DeleteByUser(ctx context.Context, userID string) error {
	if _, err := r.data.pool.Exec(ctx,
		"DELETE FROM system_refresh_token WHERE user_id = $1", userID); err != nil {
		return fmt.Errorf("delete refresh tokens by user: %w", err)
	}
	return nil
}
