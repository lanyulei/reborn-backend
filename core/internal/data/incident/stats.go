package data

import (
	"context"
	"fmt"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/incident"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
)

var _ biz.StatsRepo = (*statsRepo)(nil)

// statsRepo 故障统计：全部是只读聚合，演练与已合并的不计；范围按开启时间。
type statsRepo struct {
	data *basedata.Data
}

// NewStatsRepo new a statsRepo.
func NewStatsRepo(data *basedata.Data) *statsRepo { return &statsRepo{data: data} }

// rangeWhere 统计范围的 WHERE（表别名 i）。
func rangeWhere(a *sqlArgs, r biz.StatsRange) string {
	where := "i.open_time >= " + a.add(r.Start) + " AND i.open_time < " + a.add(r.End) + " AND NOT i.is_drill AND i.status <> 6"
	if r.BusinessID != "" {
		where += " AND i.business_id = " + a.add(r.BusinessID)
	}
	return where
}

// Overview implements biz.StatsRepo.
func (r *statsRepo) Overview(ctx context.Context, businessID string, now time.Time) (*biz.OverviewStats, error) {
	var a sqlArgs
	biz0 := ""
	if businessID != "" {
		biz0 = " AND i.business_id = " + a.add(businessID)
	}
	// 候选故障数：候选行本身不存业务（业务到开出故障时才定），带业务过滤时只统计「规则固定指向该业务」的候选
	// ——此前不过滤，每个业务的概览里都是同一个全局数。按告警标签动态匹配业务的规则（business_label）在
	// 候选阶段还没有确定的业务，其候选不计入任何一个业务。规则的业务存在 spec 的 JSON 里，是文本：单独用一个
	// 文本参数比较，不复用上面按 uuid 推断的那个参数。
	candidates := "SELECT count(*)::int AS n FROM incident_candidate WHERE status = 1"
	if businessID != "" {
		candidates = "SELECT count(*)::int AS n FROM incident_candidate c JOIN incident_rule r ON r.id = c.rule_id" +
			" WHERE c.status = 1 AND lower(r.spec->'incident'->>'business_id') = lower(" + a.add(businessID) + "::text)"
	}
	dayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	ds, de, nowP := a.add(dayStart), a.add(dayStart.Add(24*time.Hour)), a.add(now)
	o := &biz.OverviewStats{ActiveBySev: map[int32]int32{}}
	var s1, s2, s3, s4 int32
	err := r.data.Pool.QueryRow(ctx, `
		SELECT act.total, act.s1, act.s2, act.s3, act.s4, act.unresponded, rv.pending, rv.overdue, pp.pending, td.opened, td.resolved, cd.n
		FROM (SELECT count(*)::int AS total,
		             count(*) FILTER (WHERE i.severity = 1)::int AS s1, count(*) FILTER (WHERE i.severity = 2)::int AS s2,
		             count(*) FILTER (WHERE i.severity = 3)::int AS s3, count(*) FILTER (WHERE i.severity = 4)::int AS s4,
		             count(*) FILTER (WHERE i.status = 1)::int AS unresponded
		      FROM incident_record i WHERE i.status <= 3 AND NOT i.is_drill`+biz0+`) act,
		     (SELECT count(*) FILTER (WHERE i.review_status IN (1, 2))::int AS pending,
		             count(*) FILTER (WHERE i.review_status IN (1, 2) AND i.review_due < `+nowP+`)::int AS overdue
		      FROM incident_record i WHERE NOT i.is_drill AND i.status <> 6`+biz0+`) rv,
		     (SELECT count(*)::int AS pending FROM incident_proposal p JOIN incident_record i ON i.id = p.incident_id WHERE p.status = 1 AND NOT i.is_drill`+biz0+`) pp,
		     (SELECT count(*) FILTER (WHERE i.open_time >= `+ds+` AND i.open_time < `+de+`)::int AS opened,
		             count(*) FILTER (WHERE i.resolve_time >= `+ds+` AND i.resolve_time < `+de+`)::int AS resolved
		      FROM incident_record i WHERE NOT i.is_drill AND i.status <> 6`+biz0+`) td,
		     (`+candidates+`) cd`, a.args...).
		Scan(&o.Active, &s1, &s2, &s3, &s4, &o.Unresponded, &o.ReviewPending, &o.ReviewOverdue, &o.ProposalPending, &o.TodayOpened, &o.TodayResolved, &o.Candidates)
	if err != nil {
		return nil, fmt.Errorf("incident overview: %w", err)
	}
	o.ActiveBySev[1], o.ActiveBySev[2], o.ActiveBySev[3], o.ActiveBySev[4] = s1, s2, s3, s4
	return o, nil
}

// Trend implements biz.StatsRepo（桶起点按 UTC）。
func (r *statsRepo) Trend(ctx context.Context, sr biz.StatsRange, bucket string) ([]*biz.TrendPoint, error) {
	unit := map[string]string{"day": "day", "week": "week", "month": "month"}[bucket]
	if unit == "" {
		return nil, fmt.Errorf("incident trend: unknown bucket %q", bucket)
	}
	var a sqlArgs
	where := rangeWhere(&a, sr)
	rows, err := r.data.Pool.Query(ctx,
		"SELECT date_trunc('"+unit+"', i.open_time AT TIME ZONE 'UTC') AS b, i.severity, count(*)::int, "+
			"COALESCE(sum(EXTRACT(EPOCH FROM (COALESCE(i.resolve_time, now()) - i.occur_time))), 0)::bigint FROM incident_record i WHERE "+where+
			" GROUP BY b, i.severity ORDER BY b", a.args...)
	if err != nil {
		return nil, fmt.Errorf("incident trend: %w", err)
	}
	defer rows.Close()
	var out []*biz.TrendPoint
	var cur *biz.TrendPoint
	for rows.Next() {
		var b time.Time
		var sev, n int32
		var dur int64
		if err := rows.Scan(&b, &sev, &n, &dur); err != nil {
			return nil, fmt.Errorf("incident trend: scan: %w", err)
		}
		if cur == nil || !cur.Bucket.Equal(b) {
			cur = &biz.TrendPoint{Bucket: b, BySev: map[int32]int32{}}
			out = append(out, cur)
		}
		cur.Total += n
		cur.BySev[sev] += n
		cur.Duration += dur
	}
	return out, rows.Err()
}

// Efficiency implements biz.StatsRepo：各等级一行 + Severity 0 的合计行。
func (r *statsRepo) Efficiency(ctx context.Context, sr biz.StatsRange) ([]*biz.EfficiencyRow, error) {
	var a sqlArgs
	where := rangeWhere(&a, sr)
	rows, err := r.data.Pool.Query(ctx, `
		SELECT COALESCE(g.severity, 0), count(*)::int,
		       COALESCE(avg(EXTRACT(EPOCH FROM (i.open_time - i.occur_time))), -1)::bigint,
		       COALESCE(avg(EXTRACT(EPOCH FROM (i.respond_time - i.open_time))) FILTER (WHERE i.respond_time IS NOT NULL), -1)::bigint,
		       COALESCE(avg(EXTRACT(EPOCH FROM (i.mitigate_time - i.occur_time))) FILTER (WHERE i.mitigate_time IS NOT NULL), -1)::bigint,
		       COALESCE(avg(EXTRACT(EPOCH FROM (i.resolve_time - i.occur_time))) FILTER (WHERE i.resolve_time IS NOT NULL), -1)::bigint,
		       count(*) FILTER (WHERE i.reopen_count > 0)::int
		FROM incident_record i CROSS JOIN LATERAL (VALUES (i.severity), (NULL::smallint)) AS g(severity)
		WHERE `+where+` GROUP BY g.severity ORDER BY 1`, a.args...)
	if err != nil {
		return nil, fmt.Errorf("incident efficiency: %w", err)
	}
	defer rows.Close()
	var out []*biz.EfficiencyRow
	for rows.Next() {
		var row biz.EfficiencyRow
		if err := rows.Scan(&row.Severity, &row.Count, &row.MTTD, &row.MTTA, &row.MTTM, &row.MTTR, &row.Reopened); err != nil {
			return nil, fmt.Errorf("incident efficiency: scan: %w", err)
		}
		out = append(out, &row)
	}
	return out, rows.Err()
}

// ReviewOverdue implements biz.StatsRepo：需要复盘的（review_due 非空）里仍待复盘且已过期的。
func (r *statsRepo) ReviewOverdue(ctx context.Context, sr biz.StatsRange, now time.Time) (int, int, error) {
	var a sqlArgs
	where := rangeWhere(&a, sr)
	var required, overdue int
	if err := r.data.Pool.QueryRow(ctx,
		"SELECT count(*) FILTER (WHERE i.review_due IS NOT NULL), count(*) FILTER (WHERE i.review_status IN (1, 2) AND i.review_due < "+a.add(now)+") FROM incident_record i WHERE "+where, a.args...).
		Scan(&required, &overdue); err != nil {
		return 0, 0, fmt.Errorf("incident review overdue: %w", err)
	}
	return required, overdue, nil
}

// Distribution implements biz.StatsRepo.
func (r *statsRepo) Distribution(ctx context.Context, sr biz.StatsRange) (*biz.DistributionStats, error) {
	out := &biz.DistributionStats{}
	group := func(keyExpr, nameExpr, join string, dst *[]biz.NamedCount) error {
		var a sqlArgs
		where := rangeWhere(&a, sr)
		rows, err := r.data.Pool.Query(ctx,
			"SELECT "+keyExpr+" AS k, "+nameExpr+" AS n, count(*)::int, COALESCE(sum(EXTRACT(EPOCH FROM (COALESCE(i.resolve_time, now()) - i.occur_time))), 0)::bigint "+
				"FROM incident_record i "+join+" WHERE "+where+" GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 20", a.args...)
		if err != nil {
			return fmt.Errorf("incident distribution: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var nc biz.NamedCount
			if err := rows.Scan(&nc.Key, &nc.Name, &nc.Count, &nc.Duration); err != nil {
				return fmt.Errorf("incident distribution: scan: %w", err)
			}
			*dst = append(*dst, nc)
		}
		return rows.Err()
	}
	if err := group("i.root_cause_code", "''", "", &out.RootCauses); err != nil {
		return nil, err
	}
	if err := group("i.source::text", "''", "", &out.Sources); err != nil {
		return nil, err
	}
	if err := group("COALESCE(i.business_id::text, '')", "COALESCE(b.name, '未归属')", "LEFT JOIN incident_business b ON b.id = i.business_id", &out.Businesses); err != nil {
		return nil, err
	}
	if err := group("COALESCE(i.rule_id::text, '')", "COALESCE(r.name, '')", "LEFT JOIN incident_rule r ON r.id = i.rule_id", &out.Rules); err != nil {
		return nil, err
	}
	var a sqlArgs
	where := rangeWhere(&a, sr)
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*)::int FROM incident_alert l JOIN incident_record i ON i.id = l.incident_id WHERE "+where, a.args...).Scan(&out.AlertLinked); err != nil {
		return nil, fmt.Errorf("incident distribution: alerts: %w", err)
	}
	return out, nil
}
