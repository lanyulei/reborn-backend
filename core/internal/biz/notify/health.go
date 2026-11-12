package biz

import (
	"context"
	"time"
)

// ChannelHealth 渠道共享熔断状态一行（notify_channel_health ⋈ notify_channel；管道
// sender 写 / core 只读）。OpenUntil 为 nil 表示未打开。
type ChannelHealth struct {
	ChannelID   string
	ChannelName string
	ChannelType string
	BizID       string
	Samples     int32
	Failures    int32
	OpenUntil   *time.Time
	Probing     bool
	UpdateTime  time.Time
}

// Open 报告熔断是否打开（截止在未来）。
func (h *ChannelHealth) Open(now time.Time) bool {
	return h.OpenUntil != nil && now.Before(*h.OpenUntil)
}

// ChannelHealthRepo 渠道健康只读仓储，由 data 层实现。
type ChannelHealthRepo interface {
	// ListChannelHealth 按 biz（空 = 全部）列出有发送记录的渠道健康行：打开优先、
	// 最近更新倒序；已删除渠道不返回。
	ListChannelHealth(ctx context.Context, bizID string) ([]*ChannelHealth, error)
}

// ChannelHealthUsecase 渠道健康查询用例（纯读）。
type ChannelHealthUsecase struct {
	repo ChannelHealthRepo
}

// NewChannelHealthUsecase new a ChannelHealthUsecase.
func NewChannelHealthUsecase(repo ChannelHealthRepo) *ChannelHealthUsecase {
	return &ChannelHealthUsecase{repo: repo}
}

// List 列出渠道健康。
func (uc *ChannelHealthUsecase) List(ctx context.Context, bizID string) ([]*ChannelHealth, error) {
	return uc.repo.ListChannelHealth(ctx, bizID)
}
