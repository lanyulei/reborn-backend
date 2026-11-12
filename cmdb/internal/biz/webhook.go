package biz

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"time"

	v1 "github.com/lanyulei/reborn/backend/cmdb/api/webhook/v1"
	"github.com/lanyulei/reborn/backend/kit/cmdbcontract"

	"github.com/go-kratos/kratos/v3/errors"
)

// 回调接入哨兵错误（reason 来自 api/webhook/v1）。
var (
	ErrWebhookUnauthorized   = errors.Unauthorized(v1.ErrorReason_WEBHOOK_UNAUTHORIZED.String(), "invalid webhook token")
	ErrWebhookSourceNotFound = errors.NotFound(v1.ErrorReason_WEBHOOK_SOURCE_NOT_FOUND.String(), "sync source not found or webhook disabled")
	ErrWebhookRateLimited    = errors.New(429, v1.ErrorReason_WEBHOOK_RATE_LIMITED.String(), "webhook triggered too frequently")
)

// WebhookTarget 采集源回调视图。
type WebhookTarget struct {
	ID        string
	Enabled   bool
	TokenHash []byte
}

// WebhookRepo 回调接入仓储。
type WebhookRepo interface {
	// GetWebhook 取采集源（未删除）的启用状态与令牌哈希；不存在返回 ErrWebhookSourceNotFound。
	GetWebhook(ctx context.Context, sourceID string) (*WebhookTarget, error)
	// TouchWebhook 限流门闩：距上次回调 ≥ minInterval 时记录本次并返回 true（单语句条件更新）。
	TouchWebhook(ctx context.Context, sourceID string, minInterval time.Duration) (bool, error)
	// EnqueueTask 插入回调触发任务；已有未完成任务时返回该任务 ID 与 queued=false。
	EnqueueTask(ctx context.Context, sourceID, trigger string) (taskID string, queued bool, err error)
}

// WebhookUsecase 回调触发：鉴权（常量时间比较）→ 限流 → 入队。
type WebhookUsecase struct {
	repo WebhookRepo
}

// NewWebhookUsecase new a WebhookUsecase.
func NewWebhookUsecase(repo WebhookRepo) *WebhookUsecase { return &WebhookUsecase{repo: repo} }

// Trigger 处理一次回调。统一在"源不存在 / 未启用 / 已停用"时返回 404，令牌不匹配 401。
func (uc *WebhookUsecase) Trigger(ctx context.Context, sourceID, token string) (string, bool, error) {
	target, err := uc.repo.GetWebhook(ctx, sourceID)
	if err != nil {
		return "", false, err
	}
	if !target.Enabled || len(target.TokenHash) == 0 {
		return "", false, ErrWebhookSourceNotFound
	}
	if token == "" {
		return "", false, ErrWebhookUnauthorized
	}
	sum := sha256.Sum256([]byte(token))
	if subtle.ConstantTimeCompare(sum[:], target.TokenHash) != 1 {
		return "", false, ErrWebhookUnauthorized
	}
	ok, err := uc.repo.TouchWebhook(ctx, sourceID, cmdbcontract.WebhookMinInterval)
	if err != nil {
		return "", false, err
	}
	if !ok {
		return "", false, ErrWebhookRateLimited
	}
	return uc.repo.EnqueueTask(ctx, sourceID, cmdbcontract.TriggerWebhook)
}
