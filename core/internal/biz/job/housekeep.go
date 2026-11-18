package biz

import (
	"context"
	"time"
)

// 保留期。
const (
	// payloadRetention 分发文件的内容在执行结束后保留多久（之后只留名称、大小与摘要）。
	payloadRetention = 30 * 24 * time.Hour
	// fireRetention 触发记录保留多久。
	fireRetention = 180 * 24 * time.Hour
	// purgeBatch 一轮清理的行数上限。
	purgeBatch = 500
)

// Retention 过期数据清理（data 层实现）。
type Retention interface {
	// Purge 删除已结束且超过保留期的文件内容、超过保留期的触发记录（不含信号待发送的），各至多 limit 行；
	// 返回删除的行数。语句幂等，多实例并发安全。
	Purge(ctx context.Context, payloadAge, fireAge time.Duration, limit int) (int, error)
}

// HousekeepUsecase 作业中心的过期数据清理。
type HousekeepUsecase struct {
	repo Retention
}

// NewHousekeepUsecase new a HousekeepUsecase.
func NewHousekeepUsecase(repo Retention) *HousekeepUsecase { return &HousekeepUsecase{repo: repo} }

// Run 清理一批；删满一批时返回正数，循环随即再跑一轮。
func (uc *HousekeepUsecase) Run(ctx context.Context) (int, error) {
	n, err := uc.repo.Purge(ctx, payloadRetention, fireRetention, purgeBatch)
	if err != nil || n < purgeBatch {
		return 0, err
	}
	return n, nil
}
