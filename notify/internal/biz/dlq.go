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
// 汇总型死信（聚合 Count/Samples/Digest）重放时保留聚合上下文，否则一条 500 事件
// 的风暴汇总会被重放成 FirstEventID 的单事件通知。
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
		ds, err := buildDispatches(dl.EventID, []Target{{ChannelID: dl.ChannelID, Recipient: dl.Recipient}}, time.Now().UTC(), replayContent(dl.Content))
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

// replayContent 重放用内容：只保留聚合上下文，丢弃已渲染的 subject/body/模板版本——
// 重放按当前模板重新渲染（操作者常是修好模板再重放）。快照缺失或不可解码 → nil
// （按单事件重放）。
func replayContent(raw []byte) *DispatchContent {
	if len(raw) == 0 {
		return nil
	}
	c, err := DecodeContent(raw)
	if err != nil || c.Agg == nil {
		return nil
	}
	return &DispatchContent{Agg: c.Agg}
}
