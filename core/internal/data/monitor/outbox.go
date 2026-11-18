package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.OutboxRepo     = (*outboxRepo)(nil)
	_ biz.DispatchWriter = (*outboxRepo)(nil)
)

// 失败退避：第 n 次失败后要等 min(15s × 2^(n-1), 30 分钟) 才会被再次领取，十次失败前后约一个半小时
// ——依赖方短暂不可用（数据库抖动、值班 / 目录查询失败）不至于把次数用尽，而丢弃是不可恢复的。
// 发件箱没有「下次重试时间」列，core 能写的只有 status / attempts / last_error / done_time，所以待派发
// 行的 done_time 被用来记「最近一次失败的时间」（终结时再写成终结时间）。引擎只在行已终结时才读
// done_time（按保留期清理），重新置为待派发时把它与 attempts 一并清空。
const (
	outboxBackoffBaseSeconds = 15
	outboxBackoffMaxSeconds  = 1800
)

// 两张发件箱表（表名为代码内常量，不接受外部输入）：status / attempts / last_error / done_time 四列同名
// 同义，终结与记失败的语句共用。
const (
	tableNotifyOutbox = "monitor_notify_outbox"
	tableEngineEvent  = "monitor_engine_event"
)

// backoffDue 「这一行已过退避期」的条件（表别名 alias；$3 = 退避基数秒，$4 = 退避上限秒）。
func backoffDue(alias string) string {
	return `(` + alias + `.attempts = 0 OR ` + alias + `.done_time IS NULL
		        OR ` + alias + `.done_time + make_interval(secs => LEAST($3::float8 * power(2, ` + alias + `.attempts - 1), $4::float8)) <= now())`
}

// finishRow 在事务内把发件箱行置为已派发 / 已丢弃。只写 core 获准的列；以 status = 待派发 围栏。
func finishRow(ctx context.Context, tx txrun.Tx, table, id string, status int32, reason string) error {
	n, err := tx.Exec(ctx,
		"UPDATE "+table+" SET status = $2, last_error = $3, done_time = now() WHERE id = $1 AND status = $4",
		id, status, reason, mc.OutboxPending)
	if err != nil {
		return fmt.Errorf("finish %s: %w", table, err)
	}
	if n != 1 {
		return fmt.Errorf("finish %s %s: row is no longer pending", table, id)
	}
	return nil
}

// failRow 单语句记一次失败：失败次数 +1 并记下失败时间（退避的起点），达到上限即置为已丢弃。
func failRow(ctx context.Context, data *basedata.Data, table, id, reason string) (bool, error) {
	var status int32
	err := data.Pool.QueryRow(ctx,
		`UPDATE `+table+` SET attempts = attempts + 1, last_error = $2,
		   status = CASE WHEN attempts + 1 >= $3 THEN $4 ELSE status END, done_time = now()
		 WHERE id = $1 AND status = $5 RETURNING status`,
		id, reason, mc.OutboxMaxAttempts, mc.OutboxDropped, mc.OutboxPending).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil // 已被别处终结（引擎清理、另一实例完成）
	}
	if err != nil {
		return false, fmt.Errorf("record %s failure: %w", table, err)
	}
	return status == mc.OutboxDropped, nil
}

type outboxRepo struct {
	data *basedata.Data
}

// NewOutboxRepo new an outboxRepo.
func NewOutboxRepo(data *basedata.Data) *outboxRepo { return &outboxRepo{data: data} }

// ClaimTx implements biz.OutboxRepo。行锁 + SKIP LOCKED：别的实例正在处理的行直接跳过。
// 同一事件更早入箱且尚未结束的行存在时，后入箱的不领取——恢复通知不会抢在触发通知之前发出
// （更早的行即使正被别的实例锁着，其 status 仍是待派发，照样挡住后面的）。
func (r *outboxRepo) ClaimTx(ctx context.Context, tx txrun.Tx, exclude []string) (*biz.OutboxItem, error) {
	var it biz.OutboxItem
	err := tx.QueryRow(ctx,
		`SELECT o.id::text, o.alert_id::text, o.kind, o.seq, o.attempts, o.level
		 FROM monitor_notify_outbox o
		 WHERE o.status = $1 AND o.id <> ALL($2::uuid[])
		   AND `+backoffDue("o")+`
		   AND NOT EXISTS (
		     SELECT 1 FROM monitor_notify_outbox p
		     WHERE p.alert_id = o.alert_id AND p.status = $1 AND (p.create_time, p.id) < (o.create_time, o.id))
		 ORDER BY o.create_time, o.id LIMIT 1 FOR UPDATE OF o SKIP LOCKED`,
		mc.OutboxPending, dbx.TextArray(exclude), outboxBackoffBaseSeconds, outboxBackoffMaxSeconds).
		Scan(&it.ID, &it.AlertID, &it.Kind, &it.Seq, &it.Attempts, &it.Level)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim monitor notify outbox: %w", err)
	}
	return &it, nil
}

// FinishTx implements biz.OutboxRepo.
func (r *outboxRepo) FinishTx(ctx context.Context, tx txrun.Tx, id string, status int32, reason string) error {
	return finishRow(ctx, tx, tableNotifyOutbox, id, status, reason)
}

// Fail implements biz.OutboxRepo.
func (r *outboxRepo) Fail(ctx context.Context, id, reason string) (bool, error) {
	return failRow(ctx, r.data, tableNotifyOutbox, id, reason)
}

// InsertLogTx implements biz.DispatchWriter.
func (r *outboxRepo) InsertLogTx(ctx context.Context, tx txrun.Tx, l *biz.NotifyLog) error {
	recipients, err := json.Marshal(dbx.TextArray(l.Recipients))
	if err != nil {
		return fmt.Errorf("encode notify log recipients: %w", err)
	}
	events, err := json.Marshal(dbx.TextArray(l.NotifyEventIDs))
	if err != nil {
		return fmt.Errorf("encode notify log events: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO monitor_notify_log (id, alert_id, kind, seq, source, subscription_id, recipients, notify_event_ids, note, level)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		l.ID, l.AlertID, l.Kind, l.Seq, l.Source, dbx.NilIfEmpty(l.SubscriptionID), recipients, events, l.Note, l.Level); err != nil {
		return fmt.Errorf("insert monitor notify log: %w", err)
	}
	return nil
}

// TouchAlertTx implements biz.DispatchWriter。core 对 monitor_alert 只有这两列的写权限。
// notify_count 抬到本次的通知序号（幂等，重复执行不会多加）。
func (r *outboxRepo) TouchAlertTx(ctx context.Context, tx txrun.Tx, alertID string, seq int32) error {
	if _, err := tx.Exec(ctx,
		"UPDATE monitor_alert SET notify_count = GREATEST(notify_count, $2), last_notify_time = now() WHERE id = $1", alertID, seq); err != nil {
		return fmt.Errorf("touch monitor alert: %w", err)
	}
	return nil
}
