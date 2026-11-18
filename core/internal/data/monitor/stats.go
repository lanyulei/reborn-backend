package data

import (
	"context"
	"fmt"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

var (
	_ biz.StatsReader = (*statsRepo)(nil)
	_ biz.StatsRanker = (*statsRepo)(nil)
)

// statsRepo 告警统计：全部是运行表的只读聚合。带时间范围的查询以「分组 + 首次触发时间」取数——有分组
// 过滤时走 idx_monitor_alert_group_fire (group_id, first_fire_time DESC)，不限分组（超级管理员）时走
// idx_monitor_alert_first_fire；范围由用例限制在 90 天内。
type statsRepo struct {
	data *basedata.Data
}

// NewStatsRepo new a statsRepo.
func NewStatsRepo(data *basedata.Data) *statsRepo { return &statsRepo{data: data} }

// statsCellLimit 趋势结果集的行数保险（91 天 × 至多 MaxGroups 个分组）。
const statsCellLimit = 92 * biz.MaxGroups

// rangeWhere 统计范围的 WHERE 子句：首次触发时间 [Start, End) + 分组过滤。
func rangeWhere(a *sqlArgs, r biz.StatsRange) string {
	return "a.first_fire_time >= " + a.add(r.Start) + " AND a.first_fire_time < " + a.add(r.End) + groupCond(a, "a.group_id", r.Groups)
}

// statsDims 拆分维度 → 键表达式（白名单）。
var statsDims = map[string]string{
	biz.StatsBySeverity: "a.severity::text",
	biz.StatsByGroup:    "a.group_id::text",
}

// Overview implements biz.StatsReader。三个子查询各走各的索引：活跃告警走 (group_id, status) /
// (status, last_fire_time)；今日新增走首次触发时间；今日恢复 / 关闭走 resolve_time 的部分索引。
func (r *statsRepo) Overview(ctx context.Context, groups biz.GroupFilter, dayStart, dayEnd time.Time) (*biz.AlertOverview, error) {
	var a sqlArgs
	firing := fmt.Sprintf("a.status = %d", mc.AlertFiring) + groupCond(&a, "a.group_id", groups)
	fired := rangeWhere(&a, biz.StatsRange{Groups: groups, Start: dayStart, End: dayEnd})
	ended := fmt.Sprintf("a.status <> %d AND a.resolve_time >= %s AND a.resolve_time < %s", mc.AlertFiring, a.add(dayStart), a.add(dayEnd)) +
		groupCond(&a, "a.group_id", groups)
	var o biz.AlertOverview
	err := r.data.Pool.QueryRow(ctx, `
		SELECT f.total, f.s1, f.s2, f.s3, f.s4, f.unacked, f.muted, f.inhibited, n.fired, e.resolved, e.closed
		FROM (SELECT count(*) AS total,
		             count(*) FILTER (WHERE a.severity = 1) AS s1, count(*) FILTER (WHERE a.severity = 2) AS s2,
		             count(*) FILTER (WHERE a.severity = 3) AS s3, count(*) FILTER (WHERE a.severity = 4) AS s4,
		             count(*) FILTER (WHERE a.ack_user IS NULL AND NOT a.muted AND NOT a.inhibited) AS unacked,
		             count(*) FILTER (WHERE a.muted) AS muted, count(*) FILTER (WHERE a.inhibited) AS inhibited
		      FROM monitor_alert a WHERE `+firing+`) f,
		     (SELECT count(*) AS fired FROM monitor_alert a WHERE `+fired+`) n,
		     (SELECT count(*) FILTER (WHERE a.status = `+fmt.Sprint(mc.AlertResolved)+`) AS resolved,
		             count(*) FILTER (WHERE a.status = `+fmt.Sprint(mc.AlertClosed)+`) AS closed
		      FROM monitor_alert a WHERE `+ended+`) e`, a.args...).
		Scan(&o.Firing, &o.FiringBySeverity[0], &o.FiringBySeverity[1], &o.FiringBySeverity[2], &o.FiringBySeverity[3],
			&o.Unacked, &o.Muted, &o.Inhibited, &o.TodayFired, &o.TodayResolved, &o.TodayClosed)
	if err != nil {
		return nil, fmt.Errorf("monitor alert overview: %w", err)
	}
	return &o, nil
}

// Trend implements biz.StatsReader。按 timezone 的自然日分桶（时区名走实参）。
func (r *statsRepo) Trend(ctx context.Context, sr biz.StatsRange, dim, timezone string) ([]biz.StatsCell, error) {
	key, ok := statsDims[dim]
	if !ok {
		return nil, fmt.Errorf("monitor alert trend: unknown dimension %q", dim)
	}
	var a sqlArgs
	where := rangeWhere(&a, sr)
	rows, err := r.data.Pool.Query(ctx,
		"SELECT to_char(a.first_fire_time AT TIME ZONE "+a.add(timezone)+", 'YYYY-MM-DD') AS d, "+key+` AS k, count(*)
		 FROM monitor_alert a WHERE `+where+" GROUP BY d, k ORDER BY d, k LIMIT "+a.add(statsCellLimit), a.args...)
	if err != nil {
		return nil, fmt.Errorf("monitor alert trend: %w", err)
	}
	defer rows.Close()
	cells := make([]biz.StatsCell, 0, 128)
	for rows.Next() {
		var c biz.StatsCell
		if err := rows.Scan(&c.Day, &c.Key, &c.Count); err != nil {
			return nil, fmt.Errorf("scan monitor alert trend: %w", err)
		}
		cells = append(cells, c)
	}
	return cells, rows.Err()
}

// Efficiency implements biz.StatsReader.
func (r *statsRepo) Efficiency(ctx context.Context, sr biz.StatsRange, dim string) ([]biz.EfficiencyRow, error) {
	key, ok := statsDims[dim]
	if !ok {
		return nil, fmt.Errorf("monitor alert efficiency: unknown dimension %q", dim)
	}
	var a sqlArgs
	where := rangeWhere(&a, sr)
	resolved := fmt.Sprintf("a.status = %d", mc.AlertResolved)
	rows, err := r.data.Pool.Query(ctx,
		"SELECT "+key+` AS k, count(*), count(a.ack_time), count(*) FILTER (WHERE `+resolved+`),
		        COALESCE(sum(extract(epoch FROM a.ack_time - a.first_fire_time)), 0)::float8,
		        COALESCE(sum(extract(epoch FROM a.resolve_time - a.first_fire_time)) FILTER (WHERE `+resolved+`), 0)::float8
		 FROM monitor_alert a WHERE `+where+" GROUP BY k ORDER BY k LIMIT "+a.add(biz.MaxGroups), a.args...)
	if err != nil {
		return nil, fmt.Errorf("monitor alert efficiency: %w", err)
	}
	defer rows.Close()
	out := make([]biz.EfficiencyRow, 0, 16)
	for rows.Next() {
		var row biz.EfficiencyRow
		if err := rows.Scan(&row.Key, &row.Alerts, &row.Acked, &row.Resolved, &row.AckSumS, &row.ResolveSumS); err != nil {
			return nil, fmt.Errorf("scan monitor alert efficiency: %w", err)
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// Top implements biz.StatsRanker。标签名走实参，不拼进 SQL；按资源 / 标签排行只统计有该资源 / 标签的事件。
// 按规则排行时，名称与分组取该规则在范围内最近一条事件上记下的（规则可能改过名、移过分组）：只对排进
// 前 limit 名的规则各取一行，走 idx_monitor_alert_rule (rule_id, first_fire_time DESC)。
func (r *statsRepo) Top(ctx context.Context, sr biz.StatsRange, by biz.TopBy, limit int) ([]biz.TopRow, int64, error) {
	var a sqlArgs
	where := rangeWhere(&a, sr)
	base := where
	key := ""
	switch by.Kind {
	case biz.TopByRule:
		key = "a.rule_id::text"
	case biz.TopByCI:
		key = "a.ci_id::text"
		where += " AND a.ci_id IS NOT NULL"
	case biz.TopByLabel:
		p := a.add(by.Label)
		key = "a.labels ->> " + p
		where += " AND a.labels ->> " + p + " IS NOT NULL"
	default:
		return nil, 0, fmt.Errorf("monitor alert top: unknown dimension %q", by.Kind)
	}
	ranked := "SELECT " + key + ` AS k, count(*) AS n, count(*) FILTER (WHERE a.status = ` + fmt.Sprint(mc.AlertFiring) + `) AS firing,
		        max(a.severity) AS sev, (sum(count(*)) OVER ())::bigint AS total
		 FROM monitor_alert a WHERE ` + where + " GROUP BY k ORDER BY count(*) DESC, k LIMIT " + a.add(limit)
	query := "SELECT t.k, '', '', t.n, t.firing, t.sev, t.total FROM (" + ranked + ") t ORDER BY t.n DESC, t.k"
	if by.Kind == biz.TopByRule {
		query = "SELECT t.k, x.rule_name, x.group_id, t.n, t.firing, t.sev, t.total FROM (" + ranked + `) t
		 CROSS JOIN LATERAL (
		   SELECT a.rule_name, a.group_id::text AS group_id FROM monitor_alert a
		   WHERE a.rule_id = t.k::uuid AND ` + base + ` ORDER BY a.first_fire_time DESC LIMIT 1) x
		 ORDER BY t.n DESC, t.k`
	}
	rows, err := r.data.Pool.Query(ctx, query, a.args...)
	if err != nil {
		return nil, 0, fmt.Errorf("monitor alert top: %w", err)
	}
	defer rows.Close()
	out := make([]biz.TopRow, 0, limit)
	var total int64
	for rows.Next() {
		var row biz.TopRow
		if err := rows.Scan(&row.Key, &row.Name, &row.GroupID, &row.Count, &row.Firing, &row.MaxSeverity, &total); err != nil {
			return nil, 0, fmt.Errorf("scan monitor alert top: %w", err)
		}
		out = append(out, row)
	}
	return out, total, rows.Err()
}

// Noise implements biz.StatsRanker。范围内的事件逐条用 LATERAL 取它的通知记录（idx_monitor_notify_log_alert）
// 与时间线里的屏蔽 / 抑制条目（idx_monitor_alert_event_alert）：代价随范围内的事件数增长（至多 90 天），
// 与两张表的总行数无关——哈希关联会把它们整表（或自范围起点之后的全部）扫一遍。
func (r *statsRepo) Noise(ctx context.Context, sr biz.StatsRange) (*biz.AlertNoise, error) {
	var a sqlArgs
	where := rangeWhere(&a, sr)
	var n biz.AlertNoise
	err := r.data.Pool.QueryRow(ctx, `
		SELECT count(*), count(*) FILTER (WHERE a.muted OR ev.muted), count(*) FILTER (WHERE a.inhibited OR ev.inhibited),
		       count(*) FILTER (WHERE a.notify_count > 0 OR lg.total > 0),
		       COALESCE(sum(lg.by_rule), 0)::bigint, COALESCE(sum(lg.by_sub), 0)::bigint, COALESCE(sum(lg.by_esc), 0)::bigint
		FROM monitor_alert a
		CROSS JOIN LATERAL (
		  SELECT count(*) AS total, count(*) FILTER (WHERE l.source = `+fmt.Sprint(mc.NotifySourceRule)+`) AS by_rule,
		         count(*) FILTER (WHERE l.source = `+fmt.Sprint(mc.NotifySourceSubscription)+`) AS by_sub,
		         count(*) FILTER (WHERE l.source = `+fmt.Sprint(mc.NotifySourceEscalation)+`) AS by_esc
		  FROM monitor_notify_log l WHERE l.alert_id = a.id) lg
		CROSS JOIN LATERAL (
		  SELECT COALESCE(bool_or(e.type = '`+mc.AlertEventMute+`'), false) AS muted,
		         COALESCE(bool_or(e.type = '`+mc.AlertEventInhibit+`'), false) AS inhibited
		  FROM monitor_alert_event e
		  WHERE e.alert_id = a.id AND e.type IN ('`+mc.AlertEventMute+`', '`+mc.AlertEventInhibit+`')) ev
		WHERE `+where, a.args...).
		Scan(&n.Alerts, &n.Muted, &n.Inhibited, &n.Notified, &n.RuleNotifications, &n.SubscriptionNotifications, &n.EscalationNotifications)
	if err != nil {
		return nil, fmt.Errorf("monitor alert noise: %w", err)
	}
	return &n, nil
}
