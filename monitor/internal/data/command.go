package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lanyulei/reborn/backend/kit/dbx"
	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/monitor/internal/biz"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.CommandRepo        = (*commandRepo)(nil)
	_ biz.CommandFailureRepo = (*commandRepo)(nil)
)

// commandRepo 事件指令（monitor_alert_cmd）的消费。引擎对该表只有 SELECT / DELETE 与
// status、result、done_time、first_failed_at、last_failed_at 五列的 UPDATE。
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

// RecordCommandFailure implements biz.CommandRepo。单语句，带「仍是待处理」围栏（别的实例刚处理掉就什么都不记）：
// RETURNING 在更新后的行上求值，同一条语句就得到「这一段连续失败是否已满 giveUp」。两个时刻都取数据库时钟。
// 多实例都来记也只会刷新 last_failed_at，first_failed_at 是这一段共有的起点。
func (r *commandRepo) RecordCommandFailure(ctx context.Context, id string, gap, giveUp time.Duration) (bool, error) {
	var exhausted bool
	err := r.data.Pool.QueryRow(ctx,
		`UPDATE monitor_alert_cmd
		    SET first_failed_at = CASE
		          WHEN first_failed_at IS NULL OR last_failed_at IS NULL OR last_failed_at < now() - $3::interval THEN now()
		          ELSE first_failed_at END,
		        last_failed_at = now()
		  WHERE id = $1::uuid AND status = $2
		RETURNING first_failed_at <= now() - $4::interval`,
		id, monitorcontract.CmdPending, gap, giveUp).Scan(&exhausted)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, biz.ErrCommandGone
	}
	if err != nil {
		return false, fmt.Errorf("record alert command failure: %w", err)
	}
	return exhausted, nil
}

// AbandonCommand implements biz.CommandRepo。三重围栏：FOR UPDATE SKIP LOCKED（别的实例正在处理这条指令就
// 立即返回，不排队）→ 仍是待处理 → first_failed_at 距今确已满 giveUp（事务内的数据库时钟）。锁住指令行之后
// 它不会再被别人改，后面的 UPDATE 仍带上同样的条件并核对行数。只动指令行与时间线，不碰告警与判定状态，
// 所以不需要像 CloseAlert 那样锁任务行。
func (r *commandRepo) AbandonCommand(ctx context.Context, cmd biz.AlertCommand, why string, giveUp time.Duration, newID func() (string, error)) (bool, error) {
	abandoned := false
	err := dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		abandoned = false
		var exhausted bool
		err := tx.QueryRow(ctx,
			`SELECT COALESCE(first_failed_at <= now() - $2::interval, false) FROM monitor_alert_cmd
			  WHERE id = $1::uuid AND status = $3 FOR UPDATE SKIP LOCKED`,
			cmd.ID, giveUp, monitorcontract.CmdPending).Scan(&exhausted)
		if errors.Is(err, pgx.ErrNoRows) || (err == nil && !exhausted) {
			// 已不是待处理、正被别的实例处理，或失败时段其实未满：什么都不写。
			return nil
		}
		if err != nil {
			return fmt.Errorf("lock alert command: %w", err)
		}
		tag, err := tx.Exec(ctx,
			`UPDATE monitor_alert_cmd SET status = $2, result = $3, done_time = now()
			  WHERE id = $1 AND status = $4 AND first_failed_at <= now() - $5::interval`,
			cmd.ID, monitorcontract.CmdRejected, why, monitorcontract.CmdPending, giveUp)
		if err != nil {
			return fmt.Errorf("abandon alert command: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return biz.ErrCommandGone
		}
		eventID, err := newID()
		if err != nil {
			return err
		}
		detail, err := json.Marshal(monitorcontract.AlertEventDetail{Text: why})
		if err != nil {
			return fmt.Errorf("encode alert event detail: %w", err)
		}
		// 操作人按既有约定取 NULL：这条记录由系统产生，不是下达关闭的那个人写的。
		if _, err := tx.Exec(ctx,
			"INSERT INTO monitor_alert_event (id, alert_id, type, operator, detail) VALUES ($1, $2, $3, NULL, $4::jsonb)",
			eventID, cmd.AlertID, monitorcontract.AlertEventActionFailed, string(detail)); err != nil {
			return fmt.Errorf("record abandoned alert command event: %w", err)
		}
		abandoned = true
		return nil
	})
	return abandoned, err
}
