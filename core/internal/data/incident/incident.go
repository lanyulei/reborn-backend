package data

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/incident"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.IncidentReader = (*incidentRepo)(nil)
	_ biz.IncidentWriter = (*incidentRepo)(nil)
	_ biz.MemberRepo     = (*memberRepo)(nil)
)

// incidentColumns 故障查询列序（表别名 i），末两列为派生字段。
const incidentColumns = `i.id::text, i.number, i.title, i.severity, i.status, i.review_status, i.source, i.source_ref, i.external_key,
	COALESCE(i.business_id::text, ''), COALESCE(i.rule_id::text, ''), i.correlation_key, i.summary, i.ai_summary, i.root_cause_code, i.root_cause,
	i.resolve_note, COALESCE(i.commander_id::text, ''), i.affected_ci_ids::text[], i.groups, i.is_drill, i.occur_time, i.open_time, i.respond_time,
	i.mitigate_time, i.first_resolve_time, i.resolve_time, i.close_time, i.reopen_count, COALESCE(i.merged_into::text, ''), i.escalation_level,
	i.escalate_time, i.last_activity_time, i.stale_notify_time, i.update_count, i.auto_approve, COALESCE(i.agent_owner::text, ''), i.agent_lease_until,
	i.agent_finding_count, i.review_due, i.review_remind, i.version, i.create_time, i.update_time, COALESCE(i.create_user::text, ''), COALESCE(i.update_user::text, ''),
	(SELECT count(*) FROM incident_alert l WHERE l.incident_id = i.id)::int,
	(SELECT count(*) FROM incident_proposal p WHERE p.incident_id = i.id AND p.status = 1)::int`

func scanIncident(scan func(dest ...any) error) (*biz.Incident, error) {
	var in biz.Incident
	if err := scan(&in.ID, &in.Number, &in.Title, &in.Severity, &in.Status, &in.ReviewStatus, &in.Source, &in.SourceRef, &in.ExternalKey,
		&in.BusinessID, &in.RuleID, &in.CorrelationKey, &in.Summary, &in.AISummary, &in.RootCauseCode, &in.RootCause,
		&in.ResolveNote, &in.CommanderID, &in.AffectedCIIDs, &in.Groups, &in.IsDrill, &in.OccurTime, &in.OpenTime, &in.RespondTime,
		&in.MitigateTime, &in.FirstResolve, &in.ResolveTime, &in.CloseTime, &in.ReopenCount, &in.MergedInto, &in.EscalationLevel,
		&in.EscalateTime, &in.LastActivity, &in.StaleNotifyTime, &in.UpdateCount, &in.AutoApprove, &in.AgentOwner, &in.AgentLeaseUntil,
		&in.AgentFindings, &in.ReviewDue, &in.ReviewRemind, &in.Version, &in.CreateTime, &in.UpdateTime, &in.CreateUser, &in.UpdateUser,
		&in.AlertCount, &in.PendingProposals); err != nil {
		return nil, err
	}
	return &in, nil
}

type incidentRepo struct {
	data *basedata.Data
}

// NewIncidentRepo new an incidentRepo.
func NewIncidentRepo(data *basedata.Data) *incidentRepo { return &incidentRepo{data: data} }

func (r *incidentRepo) query(ctx context.Context, q basedata.Queryer, what, tail string, args []any) ([]*biz.Incident, error) {
	rows, err := q.Query(ctx, "SELECT "+incidentColumns+" FROM incident_record i WHERE "+tail, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", what, err)
	}
	defer rows.Close()
	items := make([]*biz.Incident, 0, 32)
	for rows.Next() {
		in, err := scanIncident(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("%s: scan: %w", what, err)
		}
		items = append(items, in)
	}
	return items, rows.Err()
}

// Get implements biz.IncidentReader.
func (r *incidentRepo) Get(ctx context.Context, id string) (*biz.Incident, error) {
	in, err := scanIncident(basedata.QueryerFrom(ctx, r.data.Pool).QueryRow(ctx, "SELECT "+incidentColumns+" FROM incident_record i WHERE i.id = $1", id).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get incident: %w", err)
	}
	return in, nil
}

// where 把过滤条件翻译成 WHERE 子句。
func incidentWhere(a *sqlArgs, f biz.IncidentFilter) string {
	where := "TRUE"
	if len(f.Statuses) > 0 {
		where += " AND i.status = ANY(" + a.add(int32Array(f.Statuses)) + "::int[])"
	}
	if len(f.Severities) > 0 {
		where += " AND i.severity = ANY(" + a.add(int32Array(f.Severities)) + "::int[])"
	}
	if f.BusinessID != "" {
		where += " AND i.business_id = " + a.add(f.BusinessID)
	}
	if f.CommanderID != "" {
		where += " AND i.commander_id = " + a.add(f.CommanderID)
	}
	if f.MemberUserID != "" {
		p := a.add(f.MemberUserID)
		where += " AND (i.commander_id = " + p + " OR EXISTS (SELECT 1 FROM incident_member m WHERE m.incident_id = i.id AND m.user_id = " + p + "))"
	}
	if f.IsDrill != nil {
		where += " AND i.is_drill = " + a.add(*f.IsDrill)
	}
	if f.ReviewStatus != nil {
		where += " AND i.review_status = " + a.add(*f.ReviewStatus)
	}
	if f.ReviewOverdue {
		where += " AND i.review_status IN (1, 2) AND i.review_due < now()"
	}
	if f.OpenStart != nil {
		where += " AND i.open_time >= " + a.add(*f.OpenStart)
	}
	if f.OpenEnd != nil {
		where += " AND i.open_time < " + a.add(*f.OpenEnd)
	}
	if f.CIID != "" {
		where += " AND i.affected_ci_ids @> ARRAY[" + a.add(f.CIID) + "::uuid]"
	}
	if len(f.IDs) > 0 {
		where += " AND i.id = ANY(" + a.add(textArray(f.IDs)) + "::uuid[])"
	}
	if f.RuleID != "" {
		where += " AND i.rule_id = " + a.add(f.RuleID)
	}
	if f.CorrelationKey != "" {
		where += " AND i.correlation_key = " + a.add(f.CorrelationKey)
	}
	if f.Keyword != "" {
		p := a.add(dbx.LikeContains(f.Keyword))
		where += " AND (i.number ILIKE " + p + " OR i.title ILIKE " + p + ")"
	}
	if f.ExcludeDrill {
		where += " AND NOT i.is_drill AND i.status <> 6"
	}
	return where
}

// List implements biz.IncidentReader.
func (r *incidentRepo) List(ctx context.Context, f biz.IncidentFilter, page, pageSize int64) ([]*biz.Incident, int64, error) {
	var a sqlArgs
	where := incidentWhere(&a, f)
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM incident_record i WHERE "+where, a.args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count incidents: %w", err)
	}
	limit, offset := a.add(pageSize), a.add((page-1)*pageSize)
	items, err := r.query(ctx, r.data.Pool, "list incidents", where+" ORDER BY i.open_time DESC, i.id DESC LIMIT "+limit+" OFFSET "+offset, a.args)
	return items, total, err
}

// ListByIDs implements biz.IncidentReader.
func (r *incidentRepo) ListByIDs(ctx context.Context, ids []string) ([]*biz.Incident, error) {
	if len(ids) == 0 {
		return []*biz.Incident{}, nil
	}
	return r.query(ctx, r.data.Pool, "list incidents by ids", "i.id = ANY($1::uuid[])", []any{textArray(ids)})
}

// Inspect implements biz.IncidentReader.
func (r *incidentRepo) Inspect(ctx context.Context, q biz.InspectQuery, limit int) ([]*biz.Incident, error) {
	var a sqlArgs
	where := "TRUE"
	if len(q.Statuses) > 0 {
		where += " AND i.status = ANY(" + a.add(int32Array(q.Statuses)) + "::int[])"
	}
	if len(q.Severities) > 0 {
		where += " AND i.severity = ANY(" + a.add(int32Array(q.Severities)) + "::int[])"
	}
	if q.OpenBefore != nil {
		where += " AND i.open_time < " + a.add(*q.OpenBefore)
	}
	if q.EscalateTimeNull {
		where += " AND i.escalate_time IS NULL"
	}
	if q.EscalationLevelLT != nil {
		where += " AND i.escalation_level < " + a.add(*q.EscalationLevelLT)
	}
	if q.EscalateTimeBefore != nil {
		where += " AND (i.escalate_time IS NULL OR i.escalate_time < " + a.add(*q.EscalateTimeBefore) + ")"
	}
	if q.LastActivityBefore != nil {
		where += " AND i.last_activity_time < " + a.add(*q.LastActivityBefore)
	}
	if q.StaleNotifyBefore != nil {
		where += " AND (i.stale_notify_time IS NULL OR i.stale_notify_time < " + a.add(*q.StaleNotifyBefore) + ")"
	}
	if len(q.ReviewStatuses) > 0 {
		where += " AND i.review_status = ANY(" + a.add(int32Array(q.ReviewStatuses)) + "::int[])"
	}
	if q.ReviewDueBefore != nil {
		where += " AND i.review_due IS NOT NULL AND i.review_due < " + a.add(*q.ReviewDueBefore)
	}
	if q.ReviewRemindLT > 0 {
		where += " AND i.review_remind < " + a.add(q.ReviewRemindLT)
	}
	if q.ResolveBefore != nil {
		where += " AND i.resolve_time < " + a.add(*q.ResolveBefore)
	}
	if q.AgentLeaseBefore != nil {
		where += " AND i.agent_owner IS NOT NULL AND i.agent_lease_until < " + a.add(*q.AgentLeaseBefore)
	}
	if q.ExcludeDrill {
		where += " AND NOT i.is_drill"
	}
	return r.query(ctx, r.data.Pool, "inspect incidents", where+" ORDER BY i.open_time LIMIT "+a.add(limit), a.args)
}

// Search implements biz.IncidentReader：全文检索（tsvector）+ 标题相似度（pg_trgm）。
func (r *incidentRepo) Search(ctx context.Context, query, excludeID string, limit int) ([]*biz.Incident, []float32, error) {
	rows, err := r.data.Pool.Query(ctx,
		"SELECT "+incidentColumns+`, (ts_rank(i.search, plainto_tsquery('simple', $1)) + similarity(i.title, $1))::float4 AS score
		 FROM incident_record i
		 WHERE i.status <> 6 AND ($2 = '' OR i.id <> $2::uuid)
		   AND (i.search @@ plainto_tsquery('simple', $1) OR i.title % $1)
		 ORDER BY score DESC, i.open_time DESC LIMIT $3`, query, excludeID, limit)
	if err != nil {
		return nil, nil, fmt.Errorf("search incidents: %w", err)
	}
	defer rows.Close()
	var items []*biz.Incident
	var scores []float32
	for rows.Next() {
		var in biz.Incident
		var score float32
		dest := []any{
			&in.ID, &in.Number, &in.Title, &in.Severity, &in.Status, &in.ReviewStatus, &in.Source, &in.SourceRef, &in.ExternalKey,
			&in.BusinessID, &in.RuleID, &in.CorrelationKey, &in.Summary, &in.AISummary, &in.RootCauseCode, &in.RootCause,
			&in.ResolveNote, &in.CommanderID, &in.AffectedCIIDs, &in.Groups, &in.IsDrill, &in.OccurTime, &in.OpenTime, &in.RespondTime,
			&in.MitigateTime, &in.FirstResolve, &in.ResolveTime, &in.CloseTime, &in.ReopenCount, &in.MergedInto, &in.EscalationLevel,
			&in.EscalateTime, &in.LastActivity, &in.StaleNotifyTime, &in.UpdateCount, &in.AutoApprove, &in.AgentOwner, &in.AgentLeaseUntil,
			&in.AgentFindings, &in.ReviewDue, &in.ReviewRemind, &in.Version, &in.CreateTime, &in.UpdateTime, &in.CreateUser, &in.UpdateUser,
			&in.AlertCount, &in.PendingProposals, &score,
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, nil, fmt.Errorf("search incidents: scan: %w", err)
		}
		items = append(items, &in)
		scores = append(scores, score)
	}
	return items, scores, rows.Err()
}

// CountRecentByRule implements biz.IncidentReader.
func (r *incidentRepo) CountRecentByRule(ctx context.Context, ruleID string, since time.Time) (int, error) {
	var n int
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM incident_record WHERE rule_id = $1 AND open_time >= $2", ruleID, since).Scan(&n); err != nil {
		return 0, fmt.Errorf("count incidents by rule: %w", err)
	}
	return n, nil
}

// FindByExternalKey implements biz.IncidentReader.
func (r *incidentRepo) FindByExternalKey(ctx context.Context, sourceID, externalKey string) (*biz.Incident, error) {
	items, err := r.query(ctx, r.data.Pool, "find incident by external key", "i.source = 3 AND i.source_ref = $1 AND i.external_key = $2 LIMIT 1", []any{sourceID, externalKey})
	if err != nil || len(items) == 0 {
		return nil, err
	}
	return items[0], nil
}

// NextNumberTx implements biz.IncidentWriter：按日行锁自增。
func (r *incidentRepo) NextNumberTx(ctx context.Context, tx txrun.Tx, day time.Time) (int32, error) {
	var n int32
	if err := tx.QueryRow(ctx,
		`INSERT INTO incident_counter (day, n) VALUES ($1, 1)
		 ON CONFLICT (day) DO UPDATE SET n = incident_counter.n + 1 RETURNING n`, day.Format(time.DateOnly)).Scan(&n); err != nil {
		return 0, fmt.Errorf("next incident number: %w", err)
	}
	return n, nil
}

// mutableColumns 写回的可变列（CreateTx 与 SaveTx 共用列表，保证两边一致）。
var mutableColumns = []string{
	"title", "severity", "status", "review_status", "business_id", "summary", "ai_summary", "root_cause_code", "root_cause", "resolve_note",
	"commander_id", "affected_ci_ids", "groups", "is_drill", "occur_time", "respond_time", "mitigate_time", "first_resolve_time", "resolve_time",
	"close_time", "reopen_count", "merged_into", "escalation_level", "escalate_time", "last_activity_time", "stale_notify_time", "update_count",
	"auto_approve", "agent_owner", "agent_lease_until", "agent_finding_count", "review_due", "review_remind", "update_user",
}

func mutableArgs(in *biz.Incident) []any {
	return []any{
		in.Title, in.Severity, in.Status, in.ReviewStatus, nilIfEmpty(in.BusinessID), in.Summary, in.AISummary, in.RootCauseCode, in.RootCause, in.ResolveNote,
		nilIfEmpty(in.CommanderID), textArray(in.AffectedCIIDs), textArray(in.Groups), in.IsDrill, in.OccurTime, nilTime(in.RespondTime), nilTime(in.MitigateTime),
		nilTime(in.FirstResolve), nilTime(in.ResolveTime), nilTime(in.CloseTime), in.ReopenCount, nilIfEmpty(in.MergedInto), in.EscalationLevel,
		nilTime(in.EscalateTime), in.LastActivity, nilTime(in.StaleNotifyTime), in.UpdateCount, in.AutoApprove, nilIfEmpty(in.AgentOwner),
		nilTime(in.AgentLeaseUntil), in.AgentFindings, nilTime(in.ReviewDue), in.ReviewRemind, nilIfEmpty(in.UpdateUser),
	}
}

// CreateTx implements biz.IncidentWriter.
func (r *incidentRepo) CreateTx(ctx context.Context, tx txrun.Tx, in *biz.Incident) error {
	cols := append([]string{"id", "number", "source", "source_ref", "external_key", "rule_id", "correlation_key", "open_time", "create_user", "version"}, mutableColumns...)
	args := append([]any{in.ID, in.Number, in.Source, in.SourceRef, in.ExternalKey, nilIfEmpty(in.RuleID), in.CorrelationKey, in.OpenTime, nilIfEmpty(in.CreateUser), 1}, mutableArgs(in)...)
	ph := make([]string, 0, len(cols))
	for i := range cols {
		p := fmt.Sprintf("$%d", i+1)
		switch cols[i] {
		case "affected_ci_ids":
			p += "::uuid[]"
		case "groups":
			p += "::text[]"
		}
		ph = append(ph, p)
	}
	_, err := tx.Exec(ctx, "INSERT INTO incident_record ("+strings.Join(cols, ", ")+") VALUES ("+strings.Join(ph, ", ")+")", args...)
	if err != nil {
		if isUniqueViolation(err, "") {
			return biz.ErrVersionConflict
		}
		return fmt.Errorf("insert incident: %w", err)
	}
	in.Version = 1
	return nil
}

// LockTx implements biz.IncidentWriter.
func (r *incidentRepo) LockTx(ctx context.Context, tx txrun.Tx, id string) (*biz.Incident, error) {
	in, err := scanIncident(tx.QueryRow(ctx, "SELECT "+incidentColumns+" FROM incident_record i WHERE i.id = $1 FOR UPDATE OF i", id).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("lock incident: %w", err)
	}
	return in, nil
}

// SaveTx implements biz.IncidentWriter.
func (r *incidentRepo) SaveTx(ctx context.Context, tx txrun.Tx, in *biz.Incident) error {
	sets := make([]string, 0, len(mutableColumns))
	for i, c := range mutableColumns {
		p := fmt.Sprintf("$%d", i+3)
		switch c {
		case "affected_ci_ids":
			p += "::uuid[]"
		case "groups":
			p += "::text[]"
		}
		sets = append(sets, c+" = "+p)
	}
	args := append([]any{in.ID, in.Version}, mutableArgs(in)...)
	n, err := tx.Exec(ctx, "UPDATE incident_record SET "+strings.Join(sets, ", ")+", version = version + 1, update_time = now() WHERE id = $1 AND version = $2", args...)
	if err != nil {
		if isUniqueViolation(err, "") {
			return biz.ErrVersionConflict
		}
		return fmt.Errorf("save incident: %w", err)
	}
	if n != 1 {
		return biz.ErrVersionConflict
	}
	in.Version++
	return nil
}

// ---------------------------------------------------------------------------

type memberRepo struct {
	data *basedata.Data
}

// NewMemberRepo new a memberRepo.
func NewMemberRepo(data *basedata.Data) *memberRepo { return &memberRepo{data: data} }

const memberColumns = "incident_id::text, user_id::text, role, create_time"

func (r *memberRepo) query(ctx context.Context, q basedata.Queryer, where string, args ...any) ([]*biz.Member, error) {
	rows, err := q.Query(ctx, "SELECT "+memberColumns+" FROM incident_member WHERE "+where+" ORDER BY create_time, user_id", args...)
	if err != nil {
		return nil, fmt.Errorf("list incident members: %w", err)
	}
	defer rows.Close()
	var out []*biz.Member
	for rows.Next() {
		var m biz.Member
		if err := rows.Scan(&m.IncidentID, &m.UserID, &m.Role, &m.CreateTime); err != nil {
			return nil, fmt.Errorf("scan incident member: %w", err)
		}
		out = append(out, &m)
	}
	return out, rows.Err()
}

// List implements biz.MemberRepo.
func (r *memberRepo) List(ctx context.Context, incidentID string) ([]*biz.Member, error) {
	return r.query(ctx, basedata.QueryerFrom(ctx, r.data.Pool), "incident_id = $1", incidentID)
}

// ListMany implements biz.MemberRepo.
func (r *memberRepo) ListMany(ctx context.Context, incidentIDs []string) (map[string][]*biz.Member, error) {
	out := map[string][]*biz.Member{}
	if len(incidentIDs) == 0 {
		return out, nil
	}
	items, err := r.query(ctx, r.data.Pool, "incident_id = ANY($1::uuid[])", textArray(incidentIDs))
	if err != nil {
		return nil, err
	}
	for _, m := range items {
		out[m.IncidentID] = append(out[m.IncidentID], m)
	}
	return out, nil
}

// SetTx implements biz.MemberRepo.
func (r *memberRepo) SetTx(ctx context.Context, tx txrun.Tx, m *biz.Member, actor string) error {
	if _, err := tx.Exec(ctx,
		`INSERT INTO incident_member (incident_id, user_id, role, create_user) VALUES ($1, $2, $3, $4)
		 ON CONFLICT (incident_id, user_id) DO UPDATE SET role = EXCLUDED.role`, m.IncidentID, m.UserID, m.Role, nilIfEmpty(actor)); err != nil {
		return fmt.Errorf("set incident member: %w", err)
	}
	return nil
}

// RemoveTx implements biz.MemberRepo.
func (r *memberRepo) RemoveTx(ctx context.Context, tx txrun.Tx, incidentID, userID string) error {
	if _, err := tx.Exec(ctx, "DELETE FROM incident_member WHERE incident_id = $1 AND user_id = $2", incidentID, userID); err != nil {
		return fmt.Errorf("remove incident member: %w", err)
	}
	return nil
}
