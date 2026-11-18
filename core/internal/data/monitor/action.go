package data

import (
	"context"
	"fmt"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

var _ biz.ActionRepo = (*actionRepo)(nil)

// actionRepo 事件动作仓储。引擎写入行，core 只改 status / attempts / next_time / last_error / done_time
// （迁移 0076 的列级授权）。
type actionRepo struct {
	data *basedata.Data
}

// NewActionRepo new an actionRepo.
func NewActionRepo(data *basedata.Data) *actionRepo { return &actionRepo{data: data} }

// Claim implements biz.ActionRepo。单条语句：行锁 + SKIP LOCKED 选出到期的待执行动作，尝试次数 +1 并把
// next_time 推到租约之后——语句结束即释放行锁，执行期间靠 next_time 挡住别的实例。走
// idx_monitor_alert_action_pending (next_time) WHERE status = 1。
func (r *actionRepo) Claim(ctx context.Context, limit int, lease time.Duration) ([]*biz.AlertAction, error) {
	rows, err := r.data.Pool.Query(ctx,
		`UPDATE monitor_alert_action a SET attempts = a.attempts + 1, next_time = now() + make_interval(secs => $3::float8)
		 WHERE a.id IN (
		   SELECT p.id FROM monitor_alert_action p WHERE p.status = $1 AND p.next_time <= now()
		     AND NOT (p.kind = $4 AND EXISTS (
		       SELECT 1 FROM monitor_alert_action t WHERE t.alert_id = p.alert_id AND t.kind = $5 AND t.status = $1))
		   ORDER BY p.next_time, p.id LIMIT $2 FOR UPDATE OF p SKIP LOCKED)
		 RETURNING a.id::text, a.alert_id::text, a.kind, a.payload, a.attempts`,
		mc.ActionPending, limit, lease.Seconds(), mc.ActionTicketNote, mc.ActionTicket)
	if err != nil {
		return nil, fmt.Errorf("claim monitor alert actions: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.AlertAction, 0, limit)
	for rows.Next() {
		var a biz.AlertAction
		var payload []byte
		if err := rows.Scan(&a.ID, &a.AlertID, &a.Kind, &payload, &a.Attempts); err != nil {
			return nil, fmt.Errorf("scan monitor alert action: %w", err)
		}
		if err := decodeJSON(payload, &a.Payload, "alert action payload"); err != nil {
			return nil, err
		}
		items = append(items, &a)
	}
	return items, rows.Err()
}

// FinishTx implements biz.ActionRepo。围栏：仍是待执行且尝试次数等于领取时的值。
func (r *actionRepo) FinishTx(ctx context.Context, tx txrun.Tx, a *biz.AlertAction, status int32, note string) (bool, error) {
	n, err := tx.Exec(ctx,
		`UPDATE monitor_alert_action SET status = $3, last_error = $4, done_time = now()
		 WHERE id = $1 AND status = $5 AND attempts = $2`, a.ID, a.Attempts, status, note, mc.ActionPending)
	if err != nil {
		return false, fmt.Errorf("finish monitor alert action: %w", err)
	}
	return n == 1, nil
}

// Retry implements biz.ActionRepo。同样的围栏；未命中（已被重新领取或已终结）是空操作。
func (r *actionRepo) Retry(ctx context.Context, a *biz.AlertAction, backoff time.Duration, reason string) error {
	if _, err := r.data.Pool.Exec(ctx,
		`UPDATE monitor_alert_action SET last_error = $3, next_time = now() + make_interval(secs => $4::float8)
		 WHERE id = $1 AND status = $5 AND attempts = $2`, a.ID, a.Attempts, reason, backoff.Seconds(), mc.ActionPending); err != nil {
		return fmt.Errorf("retry monitor alert action: %w", err)
	}
	return nil
}
