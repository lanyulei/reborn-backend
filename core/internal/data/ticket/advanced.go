package data

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/ticket"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// 编译期断言锁住 biz 契约。
var (
	_ biz.TicketOps     = (*ticketRepo)(nil)
	_ biz.TicketInsight = (*ticketRepo)(nil)
)

// LockFormTx implements biz.TicketOps.
func (r *ticketRepo) LockFormTx(ctx context.Context, tx txrun.Tx, id string) ([]biz.FormField, map[string]any, error) {
	var (
		rawForm, rawData []byte
		form             []biz.FormField
		data             map[string]any
	)
	if err := tx.QueryRow(ctx, "SELECT form, data FROM ticket_order WHERE id = $1 FOR NO KEY UPDATE", id).Scan(&rawForm, &rawData); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, biz.ErrTicketNotFound
		}
		return nil, nil, fmt.Errorf("lock ticket form: %w", err)
	}
	if err := json.Unmarshal(rawForm, &form); err != nil {
		return nil, nil, fmt.Errorf("decode ticket form: %w", err)
	}
	if err := json.Unmarshal(rawData, &data); err != nil {
		return nil, nil, fmt.Errorf("decode ticket data: %w", err)
	}
	return form, data, nil
}

// SetDataTx implements biz.TicketOps.
func (r *ticketRepo) SetDataTx(ctx context.Context, tx txrun.Tx, id string, data map[string]any) error {
	if data == nil {
		data = map[string]any{}
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("encode ticket data: %w", err)
	}
	if _, err := tx.Exec(ctx, "UPDATE ticket_order SET data = $2, update_time = now() WHERE id = $1", id, raw); err != nil {
		return fmt.Errorf("update ticket data: %w", err)
	}
	return nil
}

// MarkResponseTx implements biz.TicketOps：只在尚未响应时写入（首次响应时间不被后续动作覆盖）。
func (r *ticketRepo) MarkResponseTx(ctx context.Context, tx txrun.Tx, id string, at time.Time) error {
	if _, err := tx.Exec(ctx,
		"UPDATE ticket_order SET response_time = $2, update_time = now() WHERE id = $1 AND response_time IS NULL", id, at); err != nil {
		return fmt.Errorf("mark ticket response: %w", err)
	}
	return nil
}

// LinkResourcesTx implements biz.TicketOps：一条语句批量写入。
func (r *ticketRepo) LinkResourcesTx(ctx context.Context, tx txrun.Tx, id string, resources []biz.Resource) error {
	if len(resources) == 0 {
		return nil
	}
	ids, names, models := make([]string, len(resources)), make([]string, len(resources)), make([]string, len(resources))
	for i, rs := range resources {
		ids[i], names[i], models[i] = rs.ID, rs.Name, rs.ModelName
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO ticket_resource (ticket_id, ci_id, ci_name, model_name)
		 SELECT $1, u.id::uuid, left(u.name, 255), left(u.model, 64)
		 FROM unnest($2::text[], $3::text[], $4::text[]) AS u(id, name, model)`, id, ids, names, models); err != nil {
		return fmt.Errorf("link ticket resources: %w", err)
	}
	return nil
}

// Rate implements biz.TicketInsight：条件与写入在同一语句里（并发重复评价只生效一次）。
func (r *ticketRepo) Rate(ctx context.Context, id, creator string, rating int32, comment string) (bool, error) {
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE ticket_order SET rating = $3, rating_comment = $4, rating_time = now(), update_time = now()
		 WHERE id = $1 AND creator = $2 AND status IN (2, 3) AND rating IS NULL`, id, creator, rating, comment)
	if err != nil {
		return false, fmt.Errorf("rate ticket: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// ListResources implements biz.TicketInsight.
func (r *ticketRepo) ListResources(ctx context.Context, id string) ([]biz.Resource, error) {
	rows, err := r.data.Pool.Query(ctx,
		"SELECT ci_id, ci_name, model_name FROM ticket_resource WHERE ticket_id = $1 ORDER BY ci_name, ci_id", id)
	if err != nil {
		return nil, fmt.Errorf("list ticket resources: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (biz.Resource, error) {
		var rs biz.Resource
		return rs, row.Scan(&rs.ID, &rs.Name, &rs.ModelName)
	})
	if err != nil {
		return nil, fmt.Errorf("scan ticket resource: %w", err)
	}
	return out, nil
}

// statsTopTemplates 「按服务」只取工单量最多的几项。
const statsTopTemplates = 10

// Stats implements biz.TicketInsight：两条聚合查询，都只扫 since 之后提交的工单（走 create_time
// 索引）——一条按「状态 × 优先级 × 服务」分组算出各项计数（总览、按状态、按优先级、按服务都由
// 它在内存里汇总，分组数至多几百），一条按「提交日 × 结束日」分组得出趋势。
// SLA 的分母 = 已见分晓的工单（已达成或已超时），口径见 overdueCondition。
//
// 分日时区：zone 非空时在只读事务里把会话时区临时设为它（SET LOCAL，随事务结束复原，不影响
// 连接池里的其它使用者），再用 ::date 分日——比逐行 AT TIME ZONE 换算快得多；为空即数据库
// 会话时区（与工单编号里的日期同一口径）。
func (r *ticketRepo) Stats(ctx context.Context, since time.Time, zone string) (*biz.Stats, error) {
	st := &biz.Stats{ByStatus: map[int32]int64{}, ByPriority: map[int32]int64{}}
	err := dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if zone != "" {
			if _, err := tx.Exec(ctx, "SELECT set_config('TimeZone', $1, true)", zone); err != nil {
				var pgErr *pgconn.PgError
				if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.InvalidParameterValue {
					return biz.ErrTimezoneUnknown // 数据库的时区库里没有这个名称
				}
				return fmt.Errorf("set stats time zone: %w", err)
			}
		}
		if err := r.statsCounts(ctx, tx, since, st); err != nil {
			return err
		}
		return r.statsTrend(ctx, tx, since, st)
	})
	if err != nil {
		return nil, err
	}
	return st, nil
}

// statsCounts 按「状态 × 优先级 × 服务」分组的各项计数，汇总进 st。
func (r *ticketRepo) statsCounts(ctx context.Context, tx pgx.Tx, since time.Time, st *biz.Stats) error {
	rows, err := tx.Query(ctx,
		`SELECT status, priority, template_name, count(*),
			sum(extract(epoch FROM end_time - create_time)) FILTER (WHERE status = 2)::float8,
			count(end_time) FILTER (WHERE status = 2),
			count(*) FILTER (WHERE `+responseLate+` OR (response_due IS NOT NULL AND (response_time IS NOT NULL OR status IN (2, 3)))),
			count(*) FILTER (WHERE response_due IS NOT NULL AND (response_time IS NOT NULL OR status IN (2, 3)) AND NOT `+responseLate+`),
			count(*) FILTER (WHERE `+resolveLate+` OR (resolve_due IS NOT NULL AND status IN (2, 3))),
			count(*) FILTER (WHERE resolve_due IS NOT NULL AND status IN (2, 3) AND NOT `+resolveLate+`),
			count(rating), sum(rating)
		 FROM ticket_order WHERE create_time >= $1
		 GROUP BY status, priority, template_name ORDER BY template_name`, since)
	if err != nil {
		return fmt.Errorf("ticket stats: %w", err)
	}
	defer rows.Close()
	var (
		resolveSum        float64
		resolved, rateSum int64
		byTemplate        []biz.NamedCount // 按名称升序（数据库的排序规则），同名相邻
	)
	for rows.Next() {
		var (
			status, priority                      int16
			template                              string
			n, done, respTotal, respMet, resTotal int64
			resMet, rated                         int64
			seconds                               *float64
			ratings                               *int64
		)
		if err := rows.Scan(&status, &priority, &template, &n, &seconds, &done,
			&respTotal, &respMet, &resTotal, &resMet, &rated, &ratings); err != nil {
			return fmt.Errorf("scan ticket stats: %w", err)
		}
		st.Total += n
		st.ByStatus[int32(status)] += n
		st.ByPriority[int32(priority)] += n
		st.ResponseTotal, st.ResponseMet = st.ResponseTotal+respTotal, st.ResponseMet+respMet
		st.ResolveTotal, st.ResolveMet = st.ResolveTotal+resTotal, st.ResolveMet+resMet
		st.Rated, resolved = st.Rated+rated, resolved+done
		if seconds != nil {
			resolveSum += *seconds
		}
		if ratings != nil {
			rateSum += *ratings
		}
		if i := len(byTemplate) - 1; i >= 0 && byTemplate[i].Name == template {
			byTemplate[i].Count += n
		} else {
			byTemplate = append(byTemplate, biz.NamedCount{Name: template, Count: n})
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("ticket stats: %w", err)
	}
	if resolved > 0 {
		st.AvgResolveSeconds = int64(resolveSum / float64(resolved))
	}
	if st.Rated > 0 {
		st.AvgRating = float64(rateSum) / float64(st.Rated)
	}
	// 工单量降序；稳定排序，数量相同的保持名称升序。
	slices.SortStableFunc(byTemplate, func(a, b biz.NamedCount) int { return cmp.Compare(b.Count, a.Count) })
	st.ByTemplate = byTemplate[:min(len(byTemplate), statsTopTemplates)]
	return nil
}

// statsTrend 每日提交 / 结束的工单数（按会话时区的自然日），日期升序，只含有工单的日期。
func (r *ticketRepo) statsTrend(ctx context.Context, tx pgx.Tx, since time.Time, st *biz.Stats) error {
	rows, err := tx.Query(ctx,
		`SELECT to_char(day, 'YYYY-MM-DD'), to_char(end_day, 'YYYY-MM-DD'), count(*) FROM (
			SELECT create_time::date AS day, end_time::date AS end_day FROM ticket_order WHERE create_time >= $1
		 ) t GROUP BY day, end_day`, since)
	if err != nil {
		return fmt.Errorf("ticket trend: %w", err)
	}
	defer rows.Close()
	trend := map[string]*biz.TrendPoint{}
	point := func(day string) *biz.TrendPoint {
		p := trend[day]
		if p == nil {
			p = &biz.TrendPoint{Day: day}
			trend[day] = p
		}
		return p
	}
	for rows.Next() {
		var (
			day    string
			endDay *string
			n      int64
		)
		if err := rows.Scan(&day, &endDay, &n); err != nil {
			return fmt.Errorf("scan ticket trend: %w", err)
		}
		point(day).Created += n
		if endDay != nil { // 尚未结束的工单没有结束日
			point(*endDay).Closed += n
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("ticket trend: %w", err)
	}
	st.Trend = make([]biz.TrendPoint, 0, len(trend))
	for _, p := range trend {
		st.Trend = append(st.Trend, *p)
	}
	slices.SortFunc(st.Trend, func(a, b biz.TrendPoint) int { return strings.Compare(a.Day, b.Day) })
	return nil
}

// ClaimBreachesTx implements biz.TicketInsight：响应超时与解决超时各领取一批并打上标记。
// FOR UPDATE SKIP LOCKED：并发的巡检各取各的；标记与通知事件同事务提交。
func (r *ticketRepo) ClaimBreachesTx(ctx context.Context, tx txrun.Tx, limit int, skip []string) ([]*biz.SLABreach, error) {
	if skip == nil {
		skip = []string{}
	}
	var out []*biz.SLABreach
	for _, kind := range []struct {
		name, cond string
		bit        int
	}{
		{"response", "response_time IS NULL AND response_due IS NOT NULL AND response_due <= now()", 1},
		{"resolve", "resolve_due IS NOT NULL AND resolve_due <= now()", 2},
	} {
		if len(out) >= limit {
			break
		}
		// 单语句完成「挑选 + 打标记 + 返回」；逐行返回，标量扫描。
		sql := fmt.Sprintf(`WITH due AS (
				SELECT id FROM ticket_order
				WHERE status = 1 AND %s AND (sla_notified & %d) = 0 AND NOT (id = ANY($2::uuid[]))
				ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED)
			UPDATE ticket_order t SET sla_notified = t.sla_notified | %d FROM due WHERE t.id = due.id
			RETURNING t.id::text AS id, t.number, t.title, COALESCE(t.instance_id::text, '') AS instance, t.creator::text AS creator, t.sla_escalate`,
			kind.cond, kind.bit, kind.bit)
		breaches, err := claim(ctx, tx, sql, limit-len(out), skip, kind.name)
		if err != nil {
			return nil, err
		}
		out = append(out, breaches...)
	}
	return out, nil
}

// claim 执行一条领取语句。txrun.Tx 只暴露单行查询，这里把结果聚合成一行 JSON 取回。
func claim(ctx context.Context, tx txrun.Tx, sql string, limit int, skip []string, kind string) ([]*biz.SLABreach, error) {
	var raw []byte
	wrapped := "WITH claimed AS (" + sql + `) SELECT COALESCE(jsonb_agg(jsonb_build_object(
		'id', id, 'number', number, 'title', title, 'instance', instance, 'creator', creator, 'escalate', sla_escalate)), '[]'::jsonb)
		FROM claimed`
	if err := tx.QueryRow(ctx, wrapped, limit, skip).Scan(&raw); err != nil {
		return nil, fmt.Errorf("claim sla breaches: %w", err)
	}
	var rows []struct {
		ID       string   `json:"id"`
		Number   string   `json:"number"`
		Title    string   `json:"title"`
		Instance string   `json:"instance"`
		Creator  string   `json:"creator"`
		Escalate []string `json:"escalate"`
	}
	if err := json.Unmarshal(raw, &rows); err != nil {
		return nil, fmt.Errorf("decode sla breaches: %w", err)
	}
	out := make([]*biz.SLABreach, 0, len(rows))
	for _, row := range rows {
		out = append(out, &biz.SLABreach{
			TicketID: row.ID, Number: row.Number, Title: row.Title, InstanceID: row.Instance, Creator: row.Creator,
			Kind: kind, Escalate: row.Escalate,
		})
	}
	return out, nil
}
