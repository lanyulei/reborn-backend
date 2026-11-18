package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/lanyulei/reborn/backend/kit/dbx"
	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/monitor/internal/biz"

	"github.com/jackc/pgx/v5"
)

var _ biz.CommandRepo = (*commandRepo)(nil)

// commandRepo 事件指令（monitor_alert_cmd）的消费。引擎对该表只有 SELECT / DELETE 与
// status、result、done_time 三列的 UPDATE。
type commandRepo struct {
	data *Data
}

// NewCommandRepo new a commandRepo.
func NewCommandRepo(data *Data) *commandRepo { return &commandRepo{data: data} }

// PendingCommands implements biz.CommandRepo：只取本版本会执行的指令（手动关闭）。将来新增的指令类型由认识
// 它的版本处理，不会占住这里的批次。
func (r *commandRepo) PendingCommands(ctx context.Context, limit int) ([]biz.AlertCommand, error) {
	rows, err := r.data.Pool.Query(ctx,
		"SELECT id::text, alert_id::text, kind FROM monitor_alert_cmd WHERE status = $1 AND kind = $3 ORDER BY create_time, id LIMIT $2",
		monitorcontract.CmdPending, limit, monitorcontract.AlertCmdClose)
	if err != nil {
		return nil, fmt.Errorf("list pending alert commands: %w", err)
	}
	cmds, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (biz.AlertCommand, error) {
		var c biz.AlertCommand
		err := row.Scan(&c.ID, &c.AlertID, &c.Kind)
		return c, err
	})
	if err != nil {
		return nil, fmt.Errorf("list pending alert commands: %w", err)
	}
	return cmds, nil
}

// CloseAlert implements biz.CommandRepo。锁序：指令行 → 评估任务行 → 告警行 → 判定状态行。评估的提交事务与
// 规划器的清理事务都先锁任务行，所以三者在任务行上串行，不构成死锁环；一条指令一个事务，只锁一个任务行。
// 事件已不在告警中（已恢复、已被关闭、已被清理，含等任务行锁期间被一轮评估判了恢复）→ 指令置未执行。
func (r *commandRepo) CloseAlert(ctx context.Context, cmd biz.AlertCommand, newID func() (string, error)) (bool, error) {
	closed := false
	err := dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		closed = false
		var (
			operator *string
			reason   string
		)
		err := tx.QueryRow(ctx,
			"SELECT operator::text, reason FROM monitor_alert_cmd WHERE id = $1 AND status = $2 FOR UPDATE SKIP LOCKED",
			cmd.ID, monitorcontract.CmdPending).Scan(&operator, &reason)
		if errors.Is(err, pgx.ErrNoRows) {
			return biz.ErrCommandGone
		}
		if err != nil {
			return fmt.Errorf("lock alert command: %w", err)
		}
		reject := func(why string) error {
			_, err := tx.Exec(ctx, "UPDATE monitor_alert_cmd SET status = $2, result = $3, done_time = now() WHERE id = $1",
				cmd.ID, monitorcontract.CmdRejected, why)
			if err != nil {
				return fmt.Errorf("reject alert command: %w", err)
			}
			return nil
		}
		var (
			ruleID, datasourceID string
			status               int32
		)
		err = tx.QueryRow(ctx, "SELECT rule_id::text, datasource_id::text, status FROM monitor_alert WHERE id = $1", cmd.AlertID).
			Scan(&ruleID, &datasourceID, &status)
		if errors.Is(err, pgx.ErrNoRows) {
			return reject("alert not found")
		}
		if err != nil {
			return fmt.Errorf("read alert: %w", err)
		}
		if status != monitorcontract.AlertFiring {
			return reject("alert is not firing")
		}
		// 任务行可能已不存在（规则正被清理）：那就没有在途的评估要串行。
		if _, err := tx.Exec(ctx, "SELECT 1 FROM monitor_eval_task WHERE rule_id = $1 AND datasource_id = $2 FOR UPDATE",
			ruleID, datasourceID); err != nil {
			return fmt.Errorf("lock eval task: %w", err)
		}
		var ticketed bool
		err = tx.QueryRow(ctx,
			`UPDATE monitor_alert SET status = $2, resolve_time = now(), closed_by = $4::uuid, close_reason = $5
			 WHERE id = $1 AND status = $3 RETURNING ticket_id IS NOT NULL`,
			cmd.AlertID, monitorcontract.AlertClosed, monitorcontract.AlertFiring, operator, reason).Scan(&ticketed)
		if errors.Is(err, pgx.ErrNoRows) {
			return reject("alert is not firing")
		}
		if err != nil {
			return fmt.Errorf("close alert: %w", err)
		}
		if _, err := tx.Exec(ctx, "DELETE FROM monitor_eval_state WHERE rule_id = $1 AND datasource_id = $2 AND alert_id = $3",
			ruleID, datasourceID, cmd.AlertID); err != nil {
			return fmt.Errorf("delete eval state of closed alert: %w", err)
		}
		eventID, err := newID()
		if err != nil {
			return err
		}
		detail, err := json.Marshal(monitorcontract.AlertEventDetail{Cause: monitorcontract.CloseCauseManual, Text: reason})
		if err != nil {
			return fmt.Errorf("encode alert event detail: %w", err)
		}
		if _, err := tx.Exec(ctx,
			"INSERT INTO monitor_alert_event (id, alert_id, type, operator, detail) VALUES ($1, $2, $3, $4::uuid, $5::jsonb)",
			eventID, cmd.AlertID, monitorcontract.AlertEventClose, operator, string(detail)); err != nil {
			return fmt.Errorf("record close event: %w", err)
		}
		if ticketed {
			actionID, err := newID()
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx,
				"INSERT INTO monitor_alert_action (id, alert_id, kind) VALUES ($1, $2, $3) ON CONFLICT (alert_id, kind) DO NOTHING",
				actionID, cmd.AlertID, monitorcontract.ActionTicketNote); err != nil {
				return fmt.Errorf("queue ticket note: %w", err)
			}
		}
		if _, err := tx.Exec(ctx, "UPDATE monitor_alert_cmd SET status = $2, done_time = now() WHERE id = $1",
			cmd.ID, monitorcontract.CmdDone); err != nil {
			return fmt.Errorf("finish alert command: %w", err)
		}
		closed = true
		return nil
	})
	return closed, err
}
