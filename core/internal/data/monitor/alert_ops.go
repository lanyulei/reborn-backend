package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	"github.com/google/wire"
	"github.com/jackc/pgx/v5"
)

var (
	_ biz.AlertEventRepo   = (*alertOpsRepo)(nil)
	_ biz.AlertTicketLinks = (*alertOpsRepo)(nil)
	_ biz.AlertLinkWriter  = (*alertOpsRepo)(nil)
	_ biz.AlertCIStats     = (*alertOpsRepo)(nil)
	_ biz.AlertRefReader   = (*alertOpsRepo)(nil)
	_ biz.AlertAckWriter   = (*alertOpsRepo)(nil)
	_ biz.AlertCmdWriter   = (*alertOpsRepo)(nil)
)

// AlertP2ProviderSet 告警事件第二、三期（事件操作、时间线）的 data providers。
var AlertP2ProviderSet = wire.NewSet(
	NewAlertOpsRepo,
	wire.Bind(new(biz.AlertEventRepo), new(*alertOpsRepo)),
	wire.Bind(new(biz.AlertTicketLinks), new(*alertOpsRepo)),
	wire.Bind(new(biz.AlertLinkWriter), new(*alertOpsRepo)),
	wire.Bind(new(biz.AlertCIStats), new(*alertOpsRepo)),
	wire.Bind(new(biz.AlertRefReader), new(*alertOpsRepo)),
	wire.Bind(new(biz.AlertAckWriter), new(*alertOpsRepo)),
	wire.Bind(new(biz.AlertCmdWriter), new(*alertOpsRepo)),
)

// alertOpsRepo 告警事件上由 core 负责的写入与配套查询。写权限以迁移 0076 的列级授权为界：
// monitor_alert 只写 ack_user / ack_time / ticket_id / workflow_instance_id / ai_summary；
// monitor_alert_event、monitor_alert_cmd 只 INSERT。事件的 status / closed_by 等由评估引擎写。
type alertOpsRepo struct {
	data *basedata.Data
}

// NewAlertOpsRepo new an alertOpsRepo.
func NewAlertOpsRepo(data *basedata.Data) *alertOpsRepo { return &alertOpsRepo{data: data} }

const insertAlertEventSQL = `INSERT INTO monitor_alert_event (id, alert_id, type, operator, detail)
	 VALUES ($1, $2, $3, $4, $5) RETURNING create_time`

func alertEventArgs(ev *biz.AlertEvent) ([]any, error) {
	detail, err := json.Marshal(ev.Detail)
	if err != nil {
		return nil, fmt.Errorf("encode alert event detail: %w", err)
	}
	return []any{ev.ID, ev.AlertID, ev.Type, dbx.NilIfEmpty(ev.Operator), detail}, nil
}

// Append implements biz.AlertEventRepo.
func (r *alertOpsRepo) Append(ctx context.Context, ev *biz.AlertEvent) error {
	args, err := alertEventArgs(ev)
	if err != nil {
		return err
	}
	if err := r.data.Pool.QueryRow(ctx, insertAlertEventSQL, args...).Scan(&ev.CreateTime); err != nil {
		return fmt.Errorf("insert monitor alert event: %w", err)
	}
	return nil
}

// AppendTx implements biz.AlertEventRepo.
func (r *alertOpsRepo) AppendTx(ctx context.Context, tx txrun.Tx, ev *biz.AlertEvent) error {
	args, err := alertEventArgs(ev)
	if err != nil {
		return err
	}
	if err := tx.QueryRow(ctx, insertAlertEventSQL, args...).Scan(&ev.CreateTime); err != nil {
		return fmt.Errorf("insert monitor alert event: %w", err)
	}
	return nil
}

// ListByAlert implements biz.AlertEventRepo（走 idx_monitor_alert_event_alert）。条数超过 limit 时保留的是最新的
// limit 条（事件详情的时间线只展示最近的一段），仍按时间升序返回。
func (r *alertOpsRepo) ListByAlert(ctx context.Context, alertID string, limit int) ([]*biz.AlertEvent, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT id::text, alert_id::text, type, COALESCE(operator::text, ''), detail, create_time
		 FROM monitor_alert_event WHERE alert_id = $1 ORDER BY create_time DESC, id DESC LIMIT $2`, alertID, limit)
	if err != nil {
		return nil, fmt.Errorf("list monitor alert events: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.AlertEvent, 0, 16)
	for rows.Next() {
		var ev biz.AlertEvent
		var detail []byte
		if err := rows.Scan(&ev.ID, &ev.AlertID, &ev.Type, &ev.Operator, &detail, &ev.CreateTime); err != nil {
			return nil, fmt.Errorf("scan monitor alert event: %w", err)
		}
		if err := decodeJSON(detail, &ev.Detail, "alert event detail"); err != nil {
			return nil, err
		}
		items = append(items, &ev)
	}
	slices.Reverse(items) // 倒序取的最新一段还原为升序
	return items, rows.Err()
}

// TicketLinks implements biz.AlertTicketLinks：各事件最近一条 ticket 条目（逐事件走时间线索引的尾部）。
func (r *alertOpsRepo) TicketLinks(ctx context.Context, alertIDs []string) (map[string]biz.TicketLink, error) {
	out := make(map[string]biz.TicketLink, len(alertIDs))
	if len(alertIDs) == 0 {
		return out, nil
	}
	rows, err := r.data.Pool.Query(ctx,
		`SELECT DISTINCT ON (alert_id) alert_id::text, COALESCE(operator::text, ''), detail
		 FROM monitor_alert_event WHERE alert_id = ANY($1::uuid[]) AND type = $2
		 ORDER BY alert_id, create_time DESC, id DESC`, alertIDs, mc.AlertEventTicket)
	if err != nil {
		return nil, fmt.Errorf("list monitor alert ticket links: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var alertID, operator string
		var raw []byte
		if err := rows.Scan(&alertID, &operator, &raw); err != nil {
			return nil, fmt.Errorf("scan monitor alert ticket link: %w", err)
		}
		var d mc.AlertEventDetail
		if err := decodeJSON(raw, &d, "alert event detail"); err != nil {
			return nil, err
		}
		out[alertID] = biz.TicketLink{TicketID: d.TicketID, Number: d.TicketNumber, Creator: operator, Auto: d.Auto}
	}
	return out, rows.Err()
}

// LinkTicketTx implements biz.AlertLinkWriter：条件更新（尚未关联工单才写）。
func (r *alertOpsRepo) LinkTicketTx(ctx context.Context, tx txrun.Tx, alertID, ticketID string) (bool, string, error) {
	n, err := tx.Exec(ctx, "UPDATE monitor_alert SET ticket_id = $2 WHERE id = $1 AND ticket_id IS NULL", alertID, ticketID)
	if err != nil {
		return false, "", fmt.Errorf("link monitor alert ticket: %w", err)
	}
	if n == 1 {
		return true, "", nil
	}
	var existing string
	err = tx.QueryRow(ctx, "SELECT COALESCE(ticket_id::text, '') FROM monitor_alert WHERE id = $1", alertID).Scan(&existing)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, "", biz.ErrAlertNotFound
	}
	if err != nil {
		return false, "", fmt.Errorf("read monitor alert ticket: %w", err)
	}
	return false, existing, nil
}

// SetWorkflowInstanceTx implements biz.AlertLinkWriter（已有值时不覆盖）。
func (r *alertOpsRepo) SetWorkflowInstanceTx(ctx context.Context, tx txrun.Tx, alertID, instanceID string) error {
	if _, err := tx.Exec(ctx,
		"UPDATE monitor_alert SET workflow_instance_id = $2 WHERE id = $1 AND workflow_instance_id IS NULL", alertID, instanceID); err != nil {
		return fmt.Errorf("set monitor alert workflow instance: %w", err)
	}
	return nil
}

// SetAISummaryTx implements biz.AlertLinkWriter.
func (r *alertOpsRepo) SetAISummaryTx(ctx context.Context, tx txrun.Tx, alertID, summary string) error {
	if _, err := tx.Exec(ctx, "UPDATE monitor_alert SET ai_summary = $2 WHERE id = $1", alertID, summary); err != nil {
		return fmt.Errorf("set monitor alert ai summary: %w", err)
	}
	return nil
}

// FiringByCI implements biz.AlertCIStats（走 idx_monitor_alert_ci）。
func (r *alertOpsRepo) FiringByCI(ctx context.Context, ciIDs []string, groups biz.GroupFilter) (map[string]int32, error) {
	out := make(map[string]int32, len(ciIDs))
	if len(ciIDs) == 0 || groups.Empty() {
		return out, nil
	}
	var a sqlArgs
	where := "a.ci_id = ANY(" + a.add(ciIDs) + "::uuid[]) AND a.status = " + a.add(mc.AlertFiring) + groupCond(&a, "a.group_id", groups)
	rows, err := r.data.Pool.Query(ctx, "SELECT a.ci_id::text, count(*) FROM monitor_alert a WHERE "+where+" GROUP BY a.ci_id", a.args...)
	if err != nil {
		return nil, fmt.Errorf("count firing monitor alerts by ci: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var n int32
		if err := rows.Scan(&id, &n); err != nil {
			return nil, fmt.Errorf("scan firing monitor alerts by ci: %w", err)
		}
		out[id] = n
	}
	return out, rows.Err()
}

// ListByIDs implements biz.AlertRefReader.
func (r *alertOpsRepo) ListByIDs(ctx context.Context, ids []string) ([]*biz.Alert, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := r.data.Pool.Query(ctx, "SELECT "+alertColumns+" FROM monitor_alert a WHERE a.id = ANY($1::uuid[])", ids)
	if err != nil {
		return nil, fmt.Errorf("list monitor alerts by ids: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.Alert, 0, len(ids))
	for rows.Next() {
		al, err := scanAlert(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("scan monitor alert: %w", err)
		}
		items = append(items, al)
	}
	return items, rows.Err()
}

// RuleNames implements biz.AlertRefReader.
func (r *alertOpsRepo) RuleNames(ctx context.Context, alertIDs []string) (map[string]string, error) {
	if len(alertIDs) == 0 {
		return map[string]string{}, nil
	}
	rows, err := r.data.Pool.Query(ctx, "SELECT id::text, rule_name FROM monitor_alert WHERE id = ANY($1::uuid[])", alertIDs)
	if err != nil {
		return nil, fmt.Errorf("list monitor alert rule names: %w", err)
	}
	return collectNames(rows, "monitor alert rule name")
}

// PendingClose implements biz.AlertRefReader（走 uk_monitor_alert_cmd_pending）。
func (r *alertOpsRepo) PendingClose(ctx context.Context, alertIDs []string) (map[string]bool, error) {
	out := make(map[string]bool, len(alertIDs))
	if len(alertIDs) == 0 {
		return out, nil
	}
	rows, err := r.data.Pool.Query(ctx,
		"SELECT alert_id::text FROM monitor_alert_cmd WHERE alert_id = ANY($1::uuid[]) AND kind = $2 AND status = $3",
		alertIDs, mc.AlertCmdClose, mc.CmdPending)
	if err != nil {
		return nil, fmt.Errorf("list pending monitor alert close cmds: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan pending monitor alert close cmd: %w", err)
		}
		out[id] = true
	}
	return out, rows.Err()
}

// AckTx implements biz.AlertAckWriter.
func (r *alertOpsRepo) AckTx(ctx context.Context, tx txrun.Tx, alertID, userID string) (bool, error) {
	n, err := tx.Exec(ctx,
		"UPDATE monitor_alert SET ack_user = $2, ack_time = now() WHERE id = $1 AND status = $3 AND ack_user IS NULL",
		alertID, userID, mc.AlertFiring)
	if err != nil {
		return false, fmt.Errorf("ack monitor alert: %w", err)
	}
	return n == 1, nil
}

// UnackTx implements biz.AlertAckWriter.
func (r *alertOpsRepo) UnackTx(ctx context.Context, tx txrun.Tx, alertID, expect string) (bool, error) {
	n, err := tx.Exec(ctx,
		"UPDATE monitor_alert SET ack_user = NULL, ack_time = NULL WHERE id = $1 AND status = $3 AND ack_user = $2",
		alertID, expect, mc.AlertFiring)
	if err != nil {
		return false, fmt.Errorf("unack monitor alert: %w", err)
	}
	return n == 1, nil
}

// TransferTx implements biz.AlertAckWriter。此前已认领的保留原认领时间。
func (r *alertOpsRepo) TransferTx(ctx context.Context, tx txrun.Tx, alertID, expect, to string) (bool, error) {
	n, err := tx.Exec(ctx,
		`UPDATE monitor_alert SET ack_user = $3, ack_time = COALESCE(ack_time, now())
		 WHERE id = $1 AND status = $4 AND ack_user IS NOT DISTINCT FROM $2::uuid`,
		alertID, dbx.NilIfEmpty(expect), to, mc.AlertFiring)
	if err != nil {
		return false, fmt.Errorf("transfer monitor alert: %w", err)
	}
	return n == 1, nil
}

// RequestClose implements biz.AlertCmdWriter。单语句：事件告警中才插入指令；已有待处理的关闭指令时
// 部分唯一索引（uk_monitor_alert_cmd_pending）让插入成为空操作。
func (r *alertOpsRepo) RequestClose(ctx context.Context, alertID, operator, reason string) (bool, error) {
	id, err := idgen.NewID()
	if err != nil {
		return false, fmt.Errorf("generate alert cmd id: %w", err)
	}
	var firing bool
	err = r.data.Pool.QueryRow(ctx,
		`WITH f AS (SELECT id FROM monitor_alert WHERE id = $2 AND status = $6),
		      ins AS (INSERT INTO monitor_alert_cmd (id, alert_id, kind, operator, reason)
		              SELECT $1, f.id, $3, $4, $5 FROM f
		              ON CONFLICT (alert_id, kind) WHERE status = 1 DO NOTHING)
		 SELECT EXISTS (SELECT 1 FROM f)`,
		id, alertID, mc.AlertCmdClose, dbx.NilIfEmpty(operator), reason, mc.AlertFiring).Scan(&firing)
	if err != nil {
		return false, fmt.Errorf("request monitor alert close: %w", err)
	}
	return firing, nil
}
