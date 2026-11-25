package data

import (
	"context"
	"fmt"
	"time"

	"github.com/lanyulei/reborn/backend/kit/dbx"
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
	err := dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx,
			`UPDATE notify_dlq SET replay_state = 3, update_time = now()
			 WHERE id IN (SELECT id FROM notify_dlq WHERE replay_state = 2 ORDER BY update_time LIMIT $1 FOR UPDATE SKIP LOCKED)
			 RETURNING id, dispatch_id, event_id, channel_id, recipient, COALESCE(last_error, ''), attempts, create_time`, limit)
		if err != nil {
			return fmt.Errorf("claim dead letters: %w", err)
		}
		var dls []*biz.DeadLetter
		for rows.Next() {
			var dl biz.DeadLetter
			if err := rows.Scan(&dl.ID, &dl.DispatchID, &dl.EventID, &dl.ChannelID, &dl.Recipient, &dl.LastError, &dl.Attempts, &dl.CreateTime); err != nil {
				rows.Close()
				return fmt.Errorf("scan dead letter: %w", err)
			}
			dls = append(dls, &dl)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if err := loadDeadContents(ctx, tx, dls); err != nil {
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

// loadDeadContents 一次往返取回一批死 dispatch 的 content 快照（聚合上下文），此前逐条 SELECT。
// 判定与逐条时相同：行的 create_time 不晚于该死信的 create_time 才算（死信时间 ≥ dispatch 创建
// 时间）；语句里的上界取本批最大值，只用于分区剪枝，逐行条件在内存里再比一次。源分区已清理 →
// 无行，Content 保持 nil，按单事件重放。
func loadDeadContents(ctx context.Context, tx pgx.Tx, dls []*biz.DeadLetter) error {
	if len(dls) == 0 {
		return nil
	}
	ids := make([]string, len(dls))
	var maxCreate time.Time
	for i, dl := range dls {
		ids[i] = dl.DispatchID
		if dl.CreateTime.After(maxCreate) {
			maxCreate = dl.CreateTime
		}
	}
	rows, err := tx.Query(ctx,
		"SELECT id::text, create_time, content FROM notify_dispatch WHERE id = ANY($1::uuid[]) AND create_time <= $2::timestamptz",
		ids, maxCreate)
	if err != nil {
		return fmt.Errorf("load dead dispatch content: %w", err)
	}
	defer rows.Close()
	type snapshot struct {
		createTime time.Time
		content    []byte
	}
	found := make(map[string][]snapshot, len(dls))
	for rows.Next() {
		var (
			id string
			sn snapshot
		)
		if err := rows.Scan(&id, &sn.createTime, &sn.content); err != nil {
			return fmt.Errorf("load dead dispatch content: %w", err)
		}
		found[id] = append(found[id], sn)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("load dead dispatch content: %w", err)
	}
	for _, dl := range dls {
		for _, sn := range found[dl.DispatchID] {
			if !sn.createTime.After(dl.CreateTime) {
				dl.Content = sn.content
				break
			}
		}
	}
	return nil
}
