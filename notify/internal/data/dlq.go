package data

import (
	"context"
	"fmt"

	"github.com/lanyulei/reborn/backend/notify/internal/biz"

	"github.com/jackc/pgx/v5"
)

var _ biz.DlqRepo = (*dlqRepo)(nil)

// dlqRepo 死信重放仓储。
type dlqRepo struct {
	data *Data
}

// NewDlqRepo new a dlqRepo.
func NewDlqRepo(data *Data) *dlqRepo { return &dlqRepo{data: data} }

// Replay implements biz.DlqRepo：单事务声明 pending 死信（UPDATE…RETURNING +
// SKIP LOCKED）→ 插入新 dispatch → 提交（评审 #16）。
func (r *dlqRepo) Replay(ctx context.Context, limit int, build func(dl *biz.DeadLetter) (*biz.Dispatch, error)) (int, error) {
	n := 0
	err := WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx,
			`UPDATE notify_dlq SET replay_state = 3, update_time = now()
			 WHERE id IN (SELECT id FROM notify_dlq WHERE replay_state = 2 ORDER BY update_time LIMIT $1 FOR UPDATE SKIP LOCKED)
			 RETURNING id, dispatch_id, event_id, channel_id, recipient, COALESCE(last_error, ''), attempts`, limit)
		if err != nil {
			return fmt.Errorf("claim dead letters: %w", err)
		}
		var dls []*biz.DeadLetter
		for rows.Next() {
			var dl biz.DeadLetter
			if err := rows.Scan(&dl.ID, &dl.DispatchID, &dl.EventID, &dl.ChannelID, &dl.Recipient, &dl.LastError, &dl.Attempts); err != nil {
				rows.Close()
				return fmt.Errorf("scan dead letter: %w", err)
			}
			dls = append(dls, &dl)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		dispatches := make([]*biz.Dispatch, 0, len(dls))
		for _, dl := range dls {
			d, err := build(dl)
			if err != nil {
				return err
			}
			dispatches = append(dispatches, d)
		}
		n = len(dispatches)
		return insertDispatches(ctx, tx, dispatches)
	})
	return n, err
}
