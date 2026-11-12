package data

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/lanyulei/reborn/backend/cmdb/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/notifycontract"

	"github.com/jackc/pgx/v5"
)

var _ biz.Notifier = (*notifier)(nil)

// notifier 通知事件直插：与 core 内部事件同一语句（kit/notifycontract.InsertEventSQL），
// 带去重键时先过 notify_dedup 门闩（同一事务），非首次直接跳过。
type notifier struct {
	data *Data
}

// NewNotifier new a notifier.
func NewNotifier(data *Data) *notifier { return &notifier{data: data} }

// Notify implements biz.Notifier.
func (n *notifier) Notify(ctx context.Context, event biz.SyncEvent) error {
	id, err := idgen.NewID()
	if err != nil {
		return err
	}
	var payload any
	if len(event.Payload) > 0 {
		raw, err := json.Marshal(event.Payload)
		if err != nil {
			return fmt.Errorf("encode event payload: %w", err)
		}
		payload = raw
	}
	return dbx.WithTx(ctx, n.data.Pool, func(tx pgx.Tx) error {
		if event.DedupKey != "" && event.DedupTTL > 0 {
			var winner string
			if err := tx.QueryRow(ctx, notifycontract.DedupGateSQL, notifycontract.DefaultBizID, event.DedupKey, id, event.DedupTTL).Scan(&winner); err != nil {
				return fmt.Errorf("dedup gate: %w", err)
			}
			if winner != id {
				return nil
			}
		}
		_, err := tx.Exec(ctx, notifycontract.InsertEventSQL,
			id, notifycontract.DefaultBizID, event.Type, event.Severity, event.Title, payload,
			dbx.NilIfEmpty(event.DedupKey), nil, nil, notifycontract.EventSourceInternal,
			notifycontract.EventStatusReceived, notifycontract.PublishPending, 0)
		if err != nil {
			return fmt.Errorf("insert notify event: %w", err)
		}
		return nil
	})
}
