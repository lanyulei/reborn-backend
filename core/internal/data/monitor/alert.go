package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.EvalReader      = (*evalRepo)(nil)
	_ biz.AlertReader     = (*alertRepo)(nil)
	_ biz.AlertStats      = (*alertRepo)(nil)
	_ biz.NotifyLogReader = (*notifyLogRepo)(nil)
)

// 本文件全部是运行表（评估引擎写）的只读查询，以及 core 自己写的通知记录的读取。

type evalRepo struct {
	data *basedata.Data
}

// NewEvalRepo new an evalRepo.
func NewEvalRepo(data *basedata.Data) *evalRepo { return &evalRepo{data: data} }

// ListTasks implements biz.EvalReader。一条规则的任务数 = 其生效的数据源数，有界。
func (r *evalRepo) ListTasks(ctx context.Context, ruleIDs []string) (map[string][]*biz.EvalTask, error) {
	out := make(map[string][]*biz.EvalTask, len(ruleIDs))
	if len(ruleIDs) == 0 {
		return out, nil
	}
	rows, err := r.data.Pool.Query(ctx,
		`SELECT rule_id::text, datasource_id::text, last_status, last_eval_time, last_error, last_duration_ms, firing_count, next_eval_time
		 FROM monitor_eval_task WHERE rule_id = ANY($1::uuid[]) ORDER BY rule_id, datasource_id`, ruleIDs)
	if err != nil {
		return nil, fmt.Errorf("list monitor eval tasks: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var t biz.EvalTask
		if err := rows.Scan(&t.RuleID, &t.DatasourceID, &t.LastStatus, &t.LastEvalTime, &t.LastError, &t.LastDurationMS, &t.FiringCount, &t.NextEvalTime); err != nil {
			return nil, fmt.Errorf("scan monitor eval task: %w", err)
		}
		out[t.RuleID] = append(out[t.RuleID], &t)
	}
	return out, rows.Err()
}

// ListRecords implements biz.EvalReader。调用方保证给出时间下界（分区裁剪）。
func (r *evalRepo) ListRecords(ctx context.Context, filter biz.EvalRecordFilter, page, pageSize int64) ([]*biz.EvalRecord, int64, error) {
	var a sqlArgs
	where := "rule_id = " + a.add(filter.RuleID)
	if filter.DatasourceID != "" {
		where += " AND datasource_id = " + a.add(filter.DatasourceID)
	}
	if filter.Status != nil {
		where += " AND status = " + a.add(*filter.Status)
	}
	if filter.Start != nil {
		where += " AND eval_time >= " + a.add(*filter.Start)
	}
	if filter.End != nil {
		where += " AND eval_time <= " + a.add(*filter.End)
	}
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM monitor_eval_record WHERE "+where, a.args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count monitor eval records: %w", err)
	}
	limit, offset := a.add(pageSize), a.add((page-1)*pageSize)
	rows, err := r.data.Pool.Query(ctx,
		`SELECT id::text, rule_id::text, datasource_id::text, eval_time, duration_ms, status, error, series, anomalies, recovered, detail
		 FROM monitor_eval_record WHERE `+where+" ORDER BY eval_time DESC, id DESC LIMIT "+limit+" OFFSET "+offset, a.args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list monitor eval records: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.EvalRecord, 0, pageSize)
	for rows.Next() {
		var rec biz.EvalRecord
		var detail []byte
		if err := rows.Scan(&rec.ID, &rec.RuleID, &rec.DatasourceID, &rec.EvalTime, &rec.DurationMS, &rec.Status, &rec.Error,
			&rec.Series, &rec.Anomalies, &rec.Recovered, &detail); err != nil {
			return nil, 0, fmt.Errorf("scan monitor eval record: %w", err)
		}
		if err := decodeJSON(detail, &rec.Detail, "eval record detail"); err != nil {
			return nil, 0, err
		}
		items = append(items, &rec)
	}
	return items, total, rows.Err()
}

// alertColumns 告警事件查询列序（表别名 a）。
const alertColumns = `a.id::text, a.fingerprint, a.rule_id::text, a.rule_name, a.group_id::text, a.datasource_id::text, a.datasource_name,
	a.severity, a.status, a.labels, a.annotations, a.expr, a.value, a.recovery_value, a.first_fire_time, a.last_fire_time,
	a.resolve_time, a.muted, COALESCE(a.mute_id::text, ''), a.notify_count, a.last_notify_time, a.group_key,
	COALESCE(a.ack_user::text, ''), a.ack_time, COALESCE(a.closed_by::text, ''), a.close_reason, a.inhibited, a.inhibit_kind,
	COALESCE(a.inhibit_id::text, ''), COALESCE(a.inhibit_alert_id::text, ''), COALESCE(a.ci_id::text, ''), a.escalation_level,
	COALESCE(a.ticket_id::text, ''), COALESCE(a.workflow_instance_id::text, ''), a.ai_summary`

func scanAlert(scan func(dest ...any) error) (*biz.Alert, error) {
	var al biz.Alert
	var labels, annotations []byte
	if err := scan(&al.ID, &al.Fingerprint, &al.RuleID, &al.RuleName, &al.GroupID, &al.DatasourceID, &al.DatasourceName,
		&al.Severity, &al.Status, &labels, &annotations, &al.Expr, &al.Value, &al.RecoveryValue, &al.FirstFireTime, &al.LastFireTime,
		&al.ResolveTime, &al.Muted, &al.MuteID, &al.NotifyCount, &al.LastNotifyTime, &al.GroupKey,
		&al.AckUser, &al.AckTime, &al.ClosedBy, &al.CloseReason, &al.Inhibited, &al.InhibitKind,
		&al.InhibitID, &al.InhibitAlertID, &al.CIID, &al.EscalationLevel,
		&al.TicketID, &al.WorkflowInstanceID, &al.AISummary); err != nil {
		return nil, err
	}
	if err := decodeJSON(labels, &al.Labels, "alert labels"); err != nil {
		return nil, err
	}
	if err := decodeJSON(annotations, &al.Annotations, "alert annotations"); err != nil {
		return nil, err
	}
	return &al, nil
}

type alertRepo struct {
	data *basedata.Data
}

// NewAlertRepo new an alertRepo.
func NewAlertRepo(data *basedata.Data) *alertRepo { return &alertRepo{data: data} }

// alertWhere 把过滤条件翻译成 WHERE 子句（以 "TRUE" 起头，后接若干 " AND …"）。List / Scan /
// Aggregate 共用同一口径。标签过滤用 @>（labels 的 GIN 索引是 jsonb_path_ops）。
func alertWhere(a *sqlArgs, f biz.AlertFilter) (string, error) {
	where := "TRUE" + groupCond(a, "a.group_id", f.Groups)
	if len(f.Statuses) > 0 {
		where += " AND a.status = ANY(" + a.add(f.Statuses) + "::int[])"
	}
	if len(f.RuleIDs) > 0 {
		where += " AND a.rule_id = ANY(" + a.add(f.RuleIDs) + "::uuid[])"
	}
	if len(f.DatasourceIDs) > 0 {
		where += " AND a.datasource_id = ANY(" + a.add(f.DatasourceIDs) + "::uuid[])"
	}
	if len(f.Severities) > 0 {
		where += " AND a.severity = ANY(" + a.add(f.Severities) + "::int[])"
	}
	if f.Muted != nil {
		where += " AND a.muted = " + a.add(*f.Muted)
	}
	if f.Fingerprint != "" {
		where += " AND a.fingerprint = " + a.add(f.Fingerprint)
	}
	if f.Keyword != "" {
		// 接口契约（ListAlertsRequest.keyword）就是「规则名称模糊匹配」；标签按键值精确过滤走 Labels。
		// 不再对 labels::text 做 ILIKE：那会对每一行展开 jsonb（无索引可用）。规则名称走
		// idx_monitor_alert_rule_name_trgm（0085）。
		where += " AND a.rule_name ILIKE " + a.add(dbx.LikeContains(f.Keyword))
	}
	if len(f.Labels) > 0 {
		raw, err := json.Marshal(f.Labels)
		if err != nil {
			return "", fmt.Errorf("encode label filter: %w", err)
		}
		where += " AND a.labels @> " + a.add(raw) + "::jsonb"
	}
	if f.FireStart != nil {
		where += " AND a.first_fire_time >= " + a.add(*f.FireStart)
	}
	if f.FireEnd != nil {
		where += " AND a.first_fire_time <= " + a.add(*f.FireEnd)
	}
	if f.FireAfter != nil {
		// 键集游标；ID 为空用全零 uuid 作下界（任何真实 ID 都大于它）。走 idx_monitor_alert_first_fire。
		id := f.FireAfter.ID
		if id == "" {
			id = "00000000-0000-0000-0000-000000000000"
		}
		where += " AND (a.first_fire_time, a.id) > (" + a.add(f.FireAfter.FirstFireTime) + ", " + a.add(id) + "::uuid)"
	}
	if f.HistorySince != nil {
		// 未给时间范围：活跃事件全量，历史事件只回看一段（走 (status, last_fire_time) 索引）。
		where += fmt.Sprintf(" AND (a.status = %d OR a.last_fire_time >= %s)", mc.AlertFiring, a.add(*f.HistorySince))
	}
	if f.RecipientID != "" {
		raw, err := json.Marshal([]string{f.RecipientID})
		if err != nil {
			return "", fmt.Errorf("encode recipient filter: %w", err)
		}
		notified := "EXISTS (SELECT 1 FROM monitor_notify_log l WHERE l.alert_id = a.id AND l.recipients @> " + a.add(raw) + "::jsonb)"
		if f.OrAckUser {
			// 「我负责的」：通知过我的，或由我认领的（后者走 idx_monitor_alert_ack_user）。
			notified = "(" + notified + " OR a.ack_user = " + a.add(f.RecipientID) + "::uuid)"
		}
		where += " AND " + notified
	}
	if f.CIID != "" {
		where += " AND a.ci_id = " + a.add(f.CIID) + "::uuid"
	}
	if f.Acked != nil {
		where += " AND (a.ack_user IS NOT NULL) = " + a.add(*f.Acked)
	}
	if f.Inhibited != nil {
		where += " AND a.inhibited = " + a.add(*f.Inhibited)
	}
	if f.AckUserID != "" {
		where += " AND a.ack_user = " + a.add(f.AckUserID) + "::uuid"
	}
	// 处理状态与 biz.Alert.HandleState 同一口径：被抑制 > 已屏蔽 > 已认领 > 未认领。
	switch f.HandleState {
	case biz.HandleInhibited:
		where += " AND a.inhibited"
	case biz.HandleMuted:
		where += " AND a.muted AND NOT a.inhibited"
	case biz.HandleAcked:
		where += " AND a.ack_user IS NOT NULL AND NOT a.muted AND NOT a.inhibited"
	case biz.HandleUnacked:
		where += " AND a.ack_user IS NULL AND NOT a.muted AND NOT a.inhibited"
	}
	return where, nil
}

func (r *alertRepo) query(ctx context.Context, tail string, args []any, capHint int64) ([]*biz.Alert, error) {
	rows, err := r.data.Pool.Query(ctx, "SELECT "+alertColumns+" FROM monitor_alert a WHERE "+tail, args...)
	if err != nil {
		return nil, fmt.Errorf("list monitor alerts: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.Alert, 0, min(capHint, 256))
	for rows.Next() {
		al, err := scanAlert(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("scan monitor alert: %w", err)
		}
		items = append(items, al)
	}
	return items, rows.Err()
}

// List implements biz.AlertReader.
func (r *alertRepo) List(ctx context.Context, filter biz.AlertFilter, page, pageSize int64) ([]*biz.Alert, int64, error) {
	var a sqlArgs
	where, err := alertWhere(&a, filter)
	if err != nil {
		return nil, 0, err
	}
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM monitor_alert a WHERE "+where, a.args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count monitor alerts: %w", err)
	}
	limit, offset := a.add(pageSize), a.add((page-1)*pageSize)
	items, err := r.query(ctx, where+" ORDER BY a.last_fire_time DESC, a.id DESC LIMIT "+limit+" OFFSET "+offset, a.args, pageSize)
	return items, total, err
}

// Scan implements biz.AlertReader。带键集游标（FireAfter）时按 (first_fire_time, id) 升序——消费者据此翻页。
func (r *alertRepo) Scan(ctx context.Context, filter biz.AlertFilter, limit int) ([]*biz.Alert, error) {
	var a sqlArgs
	where, err := alertWhere(&a, filter)
	if err != nil {
		return nil, err
	}
	order := " ORDER BY a.last_fire_time DESC, a.id DESC"
	if filter.FireAfter != nil {
		order = " ORDER BY a.first_fire_time, a.id"
	}
	return r.query(ctx, where+order+" LIMIT "+a.add(limit), a.args, int64(limit))
}

// Get implements biz.AlertReader.
func (r *alertRepo) Get(ctx context.Context, id string) (*biz.Alert, error) {
	al, err := scanAlert(r.data.Pool.QueryRow(ctx, "SELECT "+alertColumns+" FROM monitor_alert a WHERE a.id = $1", id).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrAlertNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get monitor alert: %w", err)
	}
	return al, nil
}

// aggregateKeys 聚合维度 → 聚合键表达式（白名单；标签名走实参，不拼进 SQL）。
var aggregateKeys = map[string]string{
	biz.AggregateByRule:     "a.rule_id::text",
	biz.AggregateBySeverity: "a.severity::text",
	biz.AggregateByGroup:    "a.group_id::text",
}

// Aggregate implements biz.AlertStats.
func (r *alertRepo) Aggregate(ctx context.Context, by biz.AggregateBy, filter biz.AlertFilter, limit int) ([]*biz.AlertAggregate, int64, error) {
	var a sqlArgs
	where, err := alertWhere(&a, filter)
	if err != nil {
		return nil, 0, err
	}
	key, ok := aggregateKeys[by.Kind]
	name := "''"
	switch {
	case by.Kind == biz.AggregateByLabel:
		key = "COALESCE(a.labels ->> " + a.add(by.Label) + ", '')"
	case !ok:
		return nil, 0, fmt.Errorf("aggregate monitor alerts: unknown dimension %q", by.Kind)
	case by.Kind == biz.AggregateByRule:
		name = "max(a.rule_name)"
	}
	rows, err := r.data.Pool.Query(ctx,
		"SELECT "+key+" AS k, "+name+`, count(*), max(a.severity),
		        count(*) FILTER (WHERE a.severity = 1), count(*) FILTER (WHERE a.severity = 2),
		        count(*) FILTER (WHERE a.severity = 3), count(*) FILTER (WHERE a.severity = 4),
		        max(a.last_fire_time), (sum(count(*)) OVER ())::bigint
		 FROM monitor_alert a WHERE `+where+" GROUP BY k ORDER BY count(*) DESC, k LIMIT "+a.add(limit), a.args...)
	if err != nil {
		return nil, 0, fmt.Errorf("aggregate monitor alerts: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.AlertAggregate, 0, 16)
	var total int64
	for rows.Next() {
		var it biz.AlertAggregate
		if err := rows.Scan(&it.Key, &it.Name, &it.Count, &it.MaxSeverity,
			&it.SeverityCounts[0], &it.SeverityCounts[1], &it.SeverityCounts[2], &it.SeverityCounts[3], &it.LastFireTime, &total); err != nil {
			return nil, 0, fmt.Errorf("scan monitor alert aggregate: %w", err)
		}
		items = append(items, &it)
	}
	return items, total, rows.Err()
}

// MutedCounts implements biz.AlertStats.
func (r *alertRepo) MutedCounts(ctx context.Context, muteIDs []string) (map[string]int32, error) {
	out := make(map[string]int32, len(muteIDs))
	if len(muteIDs) == 0 {
		return out, nil
	}
	rows, err := r.data.Pool.Query(ctx,
		`SELECT mute_id::text, count(*) FROM monitor_alert WHERE status = $1 AND muted AND mute_id = ANY($2::uuid[]) GROUP BY mute_id`,
		mc.AlertFiring, muteIDs)
	if err != nil {
		return nil, fmt.Errorf("count muted monitor alerts: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var n int32
		if err := rows.Scan(&id, &n); err != nil {
			return nil, fmt.Errorf("scan muted monitor alert count: %w", err)
		}
		out[id] = n
	}
	return out, rows.Err()
}

type notifyLogRepo struct {
	data *basedata.Data
}

// NewNotifyLogRepo new a notifyLogRepo.
func NewNotifyLogRepo(data *basedata.Data) *notifyLogRepo { return &notifyLogRepo{data: data} }

// ListByAlert implements biz.NotifyLogReader。订阅名取自本域的订阅表（含已删除的订阅）。条数超过 limit 时
// 保留的是最新的 limit 条（持续告警的事件每轮重复提醒都会追加记录，最新的才是用户要看的），仍按时间升序返回。
func (r *notifyLogRepo) ListByAlert(ctx context.Context, alertID string, limit int) ([]*biz.NotifyLog, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT l.id::text, l.alert_id::text, l.kind, l.seq, l.source, COALESCE(l.subscription_id::text, ''), COALESCE(s.name, ''),
		        l.recipients, l.notify_event_ids, l.note, l.create_time, l.level
		 FROM monitor_notify_log l LEFT JOIN monitor_subscription s ON s.id = l.subscription_id
		 WHERE l.alert_id = $1 ORDER BY l.create_time DESC, l.id DESC LIMIT $2`, alertID, limit)
	if err != nil {
		return nil, fmt.Errorf("list monitor notify logs: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.NotifyLog, 0, 8)
	for rows.Next() {
		var l biz.NotifyLog
		var recipients, events []byte
		if err := rows.Scan(&l.ID, &l.AlertID, &l.Kind, &l.Seq, &l.Source, &l.SubscriptionID, &l.SubscriptionName,
			&recipients, &events, &l.Note, &l.CreateTime, &l.Level); err != nil {
			return nil, fmt.Errorf("scan monitor notify log: %w", err)
		}
		if err := decodeJSON(recipients, &l.Recipients, "notify log recipients"); err != nil {
			return nil, err
		}
		if err := decodeJSON(events, &l.NotifyEventIDs, "notify log events"); err != nil {
			return nil, err
		}
		items = append(items, &l)
	}
	slices.Reverse(items) // 倒序取的最新一段还原为升序
	return items, rows.Err()
}

// IsRecipient implements biz.NotifyLogReader.
func (r *notifyLogRepo) IsRecipient(ctx context.Context, alertID, userID string) (bool, error) {
	raw, err := json.Marshal([]string{userID})
	if err != nil {
		return false, fmt.Errorf("encode recipient filter: %w", err)
	}
	var ok bool
	if err := r.data.Pool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM monitor_notify_log WHERE alert_id = $1 AND recipients @> $2::jsonb)", alertID, raw).Scan(&ok); err != nil {
		return false, fmt.Errorf("check monitor notify recipient: %w", err)
	}
	return ok, nil
}

// SubscriptionHits implements biz.NotifyLogReader。逐订阅走 (subscription_id, create_time DESC) 索引：
// 最近一次命中取索引首行，次数只统计 since 之后的。
func (r *notifyLogRepo) SubscriptionHits(ctx context.Context, subscriptionIDs []string, since time.Time) (map[string]biz.SubscriptionHit, error) {
	out := make(map[string]biz.SubscriptionHit, len(subscriptionIDs))
	if len(subscriptionIDs) == 0 {
		return out, nil
	}
	rows, err := r.data.Pool.Query(ctx,
		`SELECT s.id::text,
		        (SELECT max(l.create_time) FROM monitor_notify_log l WHERE l.subscription_id = s.id),
		        (SELECT count(*) FROM monitor_notify_log l WHERE l.subscription_id = s.id AND l.create_time >= $2)
		 FROM unnest($1::uuid[]) AS s(id)`, subscriptionIDs, since)
	if err != nil {
		return nil, fmt.Errorf("list monitor subscription hits: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var last *time.Time
		var h biz.SubscriptionHit
		if err := rows.Scan(&id, &last, &h.Count); err != nil {
			return nil, fmt.Errorf("scan monitor subscription hit: %w", err)
		}
		if last == nil {
			continue
		}
		h.LastTime = *last
		out[id] = h
	}
	return out, rows.Err()
}
