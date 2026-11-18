package data

import (
	"context"
	"errors"
	"fmt"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	"github.com/jackc/pgx/v5"
)

var _ biz.EngineEventRepo = (*engineEventRepo)(nil)

// 自监控事件发件箱（monitor_engine_event）：core 只写 status / attempts / last_error / done_time。
// 状态字面量与通知发件箱相同（1 待派发 / 2 已派发 / 3 已丢弃），失败退避的做法也相同——待派发行的
// done_time 记最近一次失败的时间（见 outbox.go 顶部的说明）。
type engineEventRepo struct {
	data *basedata.Data
}

// NewEngineEventRepo new an engineEventRepo.
func NewEngineEventRepo(data *basedata.Data) *engineEventRepo { return &engineEventRepo{data: data} }

// ClaimTx implements biz.EngineEventRepo。同一对象更早入箱且未终结的行存在时，后入箱的不领取：
// 「已恢复」不会抢在「持续不可用」之前发出。
func (r *engineEventRepo) ClaimTx(ctx context.Context, tx txrun.Tx, exclude []string) (*biz.EngineEvent, error) {
	var ev biz.EngineEvent
	err := tx.QueryRow(ctx,
		`SELECT e.id::text, e.type, e.object_id::text, e.dedup_key, e.detail, e.attempts
		 FROM monitor_engine_event e
		 WHERE e.status = $1 AND e.id <> ALL($2::uuid[]) AND `+backoffDue("e")+`
		   AND NOT EXISTS (
		     SELECT 1 FROM monitor_engine_event p
		     WHERE p.object_id = e.object_id AND p.status = $1 AND (p.create_time, p.id) < (e.create_time, e.id))
		 ORDER BY e.create_time, e.id LIMIT 1 FOR UPDATE OF e SKIP LOCKED`,
		mc.OutboxPending, dbx.TextArray(exclude), outboxBackoffBaseSeconds, outboxBackoffMaxSeconds).
		Scan(&ev.ID, &ev.Type, &ev.ObjectID, &ev.DedupKey, &ev.Detail, &ev.Attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim monitor engine event: %w", err)
	}
	return &ev, nil
}

// FinishTx implements biz.EngineEventRepo.
func (r *engineEventRepo) FinishTx(ctx context.Context, tx txrun.Tx, id string, status int32, reason string) error {
	return finishRow(ctx, tx, tableEngineEvent, id, status, reason)
}

// Fail implements biz.EngineEventRepo.
func (r *engineEventRepo) Fail(ctx context.Context, id, reason string) (bool, error) {
	return failRow(ctx, r.data, tableEngineEvent, id, reason)
}
