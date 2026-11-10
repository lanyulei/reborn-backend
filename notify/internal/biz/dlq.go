package biz

import (
	"context"
	"log/slog"
	"time"
)

// DlqRepo 死信重放仓储：单事务 UPDATE…RETURNING 声明 pending 死信（SKIP LOCKED）
// 并经回调插入新 dispatch，崩溃整体回滚（评审 #16）。
type DlqRepo interface {
	Replay(ctx context.Context, limit int, build func(dl *DeadLetter) (*Dispatch, error)) (int, error)
}

// DlqUsecase 死信重放用例：控制台标记 pending → 新 dispatch（新 id、新 send_key）
// → replayed；渠道已不存在仍重放，由 sender 以不可重试错误死信（可观测）。
type DlqUsecase struct {
	repo DlqRepo
	log  *slog.Logger
}

// NewDlqUsecase new a DlqUsecase.
func NewDlqUsecase(repo DlqRepo, logger *slog.Logger) *DlqUsecase {
	return &DlqUsecase{repo: repo, log: logger}
}

// ReplayOnce 重放一批；返回条数。
func (uc *DlqUsecase) ReplayOnce(ctx context.Context, limit int) (int, error) {
	n, err := uc.repo.Replay(ctx, limit, func(dl *DeadLetter) (*Dispatch, error) {
		ds, err := buildDispatches(dl.EventID, []Target{{ChannelID: dl.ChannelID, Recipient: dl.Recipient}}, time.Now().UTC(), nil)
		if err != nil {
			return nil, err
		}
		return ds[0], nil
	})
	if n > 0 {
		uc.log.InfoContext(ctx, "dead letters replayed", "count", n)
	}
	return n, err
}
