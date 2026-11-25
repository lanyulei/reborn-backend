package data

import (
	"context"
	"errors"
	"fmt"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/incident"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.AlertLinkRepo = (*linkRepo)(nil)
	_ biz.ChangeRefRepo = (*changeRefRepo)(nil)
	_ biz.CandidateRepo = (*candidateRepo)(nil)
)

// ---------------------------------------------------------------------------
// 关联告警
// ---------------------------------------------------------------------------

type linkRepo struct {
	data *basedata.Data
}

// NewLinkRepo new a linkRepo.
func NewLinkRepo(data *basedata.Data) *linkRepo { return &linkRepo{data: data} }

const linkColumns = "l.incident_id::text, l.alert_id::text, l.method, l.released, l.rule_name, l.severity, COALESCE(l.ci_id::text, ''), l.first_fire_time, l.attach_time, COALESCE(l.attach_user::text, '')"

func scanLink(scan func(dest ...any) error) (*biz.AlertLink, error) {
	var l biz.AlertLink
	if err := scan(&l.IncidentID, &l.AlertID, &l.Method, &l.Released, &l.RuleName, &l.Severity, &l.CIID, &l.FirstFireTime, &l.AttachTime, &l.AttachUser); err != nil {
		return nil, err
	}
	return &l, nil
}

func (r *linkRepo) query(ctx context.Context, what, tail string, args ...any) ([]*biz.AlertLink, error) {
	rows, err := r.data.Pool.Query(ctx, "SELECT "+linkColumns+" FROM incident_alert l WHERE "+tail, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", what, err)
	}
	defer rows.Close()
	out := make([]*biz.AlertLink, 0, 32)
	for rows.Next() {
		l, err := scanLink(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("%s: scan: %w", what, err)
		}
		out = append(out, l)
	}
	return out, rows.Err()
}

// activeStatuses 活跃故障状态（待响应 / 处理中 / 已缓解）：告警同一时刻只属于一个活跃故障。
var activeStatuses = []int32{biz.StatusOpen, biz.StatusResponding, biz.StatusMitigated}

// AttachTx implements biz.AlertLinkRepo。
// 告警若仍挂在已非活跃的故障上（已恢复的故障不释放链接，见 Resolve），先释放那条旧链接：
// 否则下面的 INSERT 会撞 uk_incident_alert_active（一条告警只能有一个未释放的归属）。
// 本故障自身已有的链接不动——重复关联同一故障由主键冲突吸收。
func (r *linkRepo) AttachTx(ctx context.Context, tx txrun.Tx, l *biz.AlertLink) (bool, error) {
	if _, err := tx.Exec(ctx,
		`UPDATE incident_alert l SET released = true
		 WHERE l.alert_id = $2 AND NOT l.released AND l.incident_id <> $1
		   AND EXISTS (SELECT 1 FROM incident_record i WHERE i.id = l.incident_id AND NOT (i.status = ANY($3::int[])))`,
		l.IncidentID, l.AlertID, int32Array(activeStatuses)); err != nil {
		return false, fmt.Errorf("release stale alert links: %w", err)
	}
	n, err := tx.Exec(ctx,
		`INSERT INTO incident_alert (incident_id, alert_id, method, rule_name, severity, ci_id, first_fire_time, attach_user)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (incident_id, alert_id) DO NOTHING`,
		l.IncidentID, l.AlertID, l.Method, l.RuleName, l.Severity, nilIfEmpty(l.CIID), nilTime(l.FirstFireTime), nilIfEmpty(l.AttachUser))
	if err != nil {
		if isUniqueViolation(err, "uk_incident_alert_active") {
			return false, biz.ErrAlertLinked
		}
		return false, fmt.Errorf("attach alert: %w", err)
	}
	return n == 1, nil
}

// DetachTx implements biz.AlertLinkRepo.
func (r *linkRepo) DetachTx(ctx context.Context, tx txrun.Tx, incidentID, alertID string) (bool, error) {
	n, err := tx.Exec(ctx, "DELETE FROM incident_alert WHERE incident_id = $1 AND alert_id = $2", incidentID, alertID)
	if err != nil {
		return false, fmt.Errorf("detach alert: %w", err)
	}
	return n == 1, nil
}

// MoveAllTx implements biz.AlertLinkRepo：目标已有同一告警时丢弃源的那条。
// PostgreSQL 的数据修改语句只能出现在 WITH 子句里（不能作 FROM 子查询）——此前写成
// `FROM (DELETE … RETURNING *)` 是语法错误，「合并故障」因此整体不可用。
func (r *linkRepo) MoveAllTx(ctx context.Context, tx txrun.Tx, from, to string) (int, error) {
	n, err := tx.Exec(ctx,
		`WITH moved AS (
			DELETE FROM incident_alert WHERE incident_id = $1
			RETURNING alert_id, rule_name, severity, ci_id, first_fire_time, attach_time, attach_user
		 )
		 INSERT INTO incident_alert (incident_id, alert_id, method, released, rule_name, severity, ci_id, first_fire_time, attach_time, attach_user)
		 SELECT $2, alert_id, 4, false, rule_name, severity, ci_id, first_fire_time, attach_time, attach_user FROM moved
		 ON CONFLICT (incident_id, alert_id) DO NOTHING`, from, to)
	if err != nil {
		return 0, fmt.Errorf("move alerts: %w", err)
	}
	return int(n), nil
}

// ReleaseAllTx implements biz.AlertLinkRepo.
func (r *linkRepo) ReleaseAllTx(ctx context.Context, tx txrun.Tx, incidentID string) error {
	if _, err := tx.Exec(ctx, "UPDATE incident_alert SET released = true WHERE incident_id = $1 AND NOT released", incidentID); err != nil {
		return fmt.Errorf("release alerts: %w", err)
	}
	return nil
}

// List implements biz.AlertLinkRepo.
func (r *linkRepo) List(ctx context.Context, incidentID string, page, pageSize int64) ([]*biz.AlertLink, int64, error) {
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM incident_alert WHERE incident_id = $1", incidentID).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count incident alerts: %w", err)
	}
	items, err := r.query(ctx, "list incident alerts", "l.incident_id = $1 ORDER BY l.attach_time DESC, l.alert_id LIMIT $2 OFFSET $3", incidentID, pageSize, (page-1)*pageSize)
	return items, total, err
}

// ListAll implements biz.AlertLinkRepo.
func (r *linkRepo) ListAll(ctx context.Context, incidentID string, limit int) ([]*biz.AlertLink, error) {
	return r.query(ctx, "list all incident alerts", "l.incident_id = $1 ORDER BY l.attach_time, l.alert_id LIMIT $2", incidentID, limit)
}

// ActiveIncident implements biz.AlertLinkRepo：告警当前所属的**活跃**故障（待响应 / 处理中 / 已缓解）。
// 已恢复的故障不释放链接（其详情仍会列出这些告警），但不再占用告警——否则同一告警再次触发时
// 关联器会直接返回，既不自动重开也不开新故障、也不通知。
func (r *linkRepo) ActiveIncident(ctx context.Context, alertID string) (string, error) {
	var id string
	err := basedata.QueryerFrom(ctx, r.data.Pool).QueryRow(ctx,
		`SELECT l.incident_id::text FROM incident_alert l
		 JOIN incident_record i ON i.id = l.incident_id
		 WHERE l.alert_id = $1 AND NOT l.released AND i.status = ANY($2::int[])`, alertID, int32Array(activeStatuses)).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("active incident of alert: %w", err)
	}
	return id, nil
}

// IncidentIDsByAlert implements biz.AlertLinkRepo.
func (r *linkRepo) IncidentIDsByAlert(ctx context.Context, alertID string) ([]string, error) {
	rows, err := r.data.Pool.Query(ctx, "SELECT incident_id::text FROM incident_alert WHERE alert_id = $1 ORDER BY attach_time DESC", alertID)
	if err != nil {
		return nil, fmt.Errorf("incidents of alert: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// Count implements biz.AlertLinkRepo.
func (r *linkRepo) Count(ctx context.Context, incidentID string) (int, error) {
	var n int
	if err := basedata.QueryerFrom(ctx, r.data.Pool).QueryRow(ctx, "SELECT count(*) FROM incident_alert WHERE incident_id = $1", incidentID).Scan(&n); err != nil {
		return 0, fmt.Errorf("count incident alerts: %w", err)
	}
	return n, nil
}

// ---------------------------------------------------------------------------
// 变更引用
// ---------------------------------------------------------------------------

type changeRefRepo struct {
	data *basedata.Data
}

// NewChangeRefRepo new a changeRefRepo.
func NewChangeRefRepo(data *basedata.Data) *changeRefRepo { return &changeRefRepo{data: data} }

// UpsertTx implements biz.ChangeRefRepo.
func (r *changeRefRepo) UpsertTx(ctx context.Context, tx txrun.Tx, c *biz.ChangeRef) error {
	if err := tx.QueryRow(ctx,
		`INSERT INTO incident_change_ref (id, incident_id, domain, kind, ref_id, summary, ref_time, relation, create_user)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		 ON CONFLICT (incident_id, domain, ref_id) DO UPDATE SET relation = EXCLUDED.relation, summary = CASE WHEN EXCLUDED.summary <> '' THEN EXCLUDED.summary ELSE incident_change_ref.summary END,
		   ref_time = COALESCE(EXCLUDED.ref_time, incident_change_ref.ref_time), kind = EXCLUDED.kind, create_user = EXCLUDED.create_user, update_time = now()
		 RETURNING id::text, create_time, update_time`,
		c.ID, c.IncidentID, c.Domain, c.Kind, c.RefID, c.Summary, nilTime(c.RefTime), c.Relation, nilIfEmpty(c.CreateUser)).Scan(&c.ID, &c.CreateTime, &c.UpdateTime); err != nil {
		return fmt.Errorf("upsert incident change ref: %w", err)
	}
	return nil
}

// List implements biz.ChangeRefRepo.
func (r *changeRefRepo) List(ctx context.Context, incidentID string) ([]*biz.ChangeRef, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT id::text, domain, kind, ref_id, summary, ref_time, relation, COALESCE(create_user::text, ''), create_time, update_time
		 FROM incident_change_ref WHERE incident_id = $1 ORDER BY update_time DESC`, incidentID)
	if err != nil {
		return nil, fmt.Errorf("list incident change refs: %w", err)
	}
	defer rows.Close()
	var out []*biz.ChangeRef
	for rows.Next() {
		c := &biz.ChangeRef{IncidentID: incidentID}
		if err := rows.Scan(&c.ID, &c.Domain, &c.Kind, &c.RefID, &c.Summary, &c.RefTime, &c.Relation, &c.CreateUser, &c.CreateTime, &c.UpdateTime); err != nil {
			return nil, fmt.Errorf("scan incident change ref: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// 候选故障
// ---------------------------------------------------------------------------

type candidateRepo struct {
	data *basedata.Data
}

// NewCandidateRepo new a candidateRepo.
func NewCandidateRepo(data *basedata.Data) *candidateRepo { return &candidateRepo{data: data} }

const candidateColumns = "c.id::text, c.rule_id::text, c.correlation_key, c.alert_ids::text[], c.ci_ids::text[], c.status, COALESCE(c.incident_id::text, ''), c.first_time, c.update_time"

func scanCandidate(scan func(dest ...any) error) (*biz.Candidate, error) {
	var c biz.Candidate
	if err := scan(&c.ID, &c.RuleID, &c.CorrelationKey, &c.AlertIDs, &c.CIIDs, &c.Status, &c.IncidentID, &c.FirstTime, &c.UpdateTime); err != nil {
		return nil, err
	}
	return &c, nil
}

// LockOpenTx implements biz.CandidateRepo.
func (r *candidateRepo) LockOpenTx(ctx context.Context, tx txrun.Tx, ruleID, key string) (*biz.Candidate, error) {
	c, err := scanCandidate(tx.QueryRow(ctx, "SELECT "+candidateColumns+" FROM incident_candidate c WHERE c.rule_id = $1 AND c.correlation_key = $2 AND c.status = 1 FOR UPDATE", ruleID, key).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lock incident candidate: %w", err)
	}
	return c, nil
}

// UpsertTx implements biz.CandidateRepo.
func (r *candidateRepo) UpsertTx(ctx context.Context, tx txrun.Tx, c *biz.Candidate) error {
	if _, err := tx.Exec(ctx,
		`INSERT INTO incident_candidate (id, rule_id, correlation_key, alert_ids, ci_ids, status, first_time, update_time)
		 VALUES ($1, $2, $3, $4::uuid[], $5::uuid[], 1, $6, $7)
		 ON CONFLICT (id) DO UPDATE SET alert_ids = EXCLUDED.alert_ids, ci_ids = EXCLUDED.ci_ids, update_time = EXCLUDED.update_time`,
		c.ID, c.RuleID, c.CorrelationKey, textArray(c.AlertIDs), textArray(c.CIIDs), c.FirstTime, c.UpdateTime); err != nil {
		return fmt.Errorf("upsert incident candidate: %w", err)
	}
	return nil
}

// SetStatusTx implements biz.CandidateRepo.
func (r *candidateRepo) SetStatusTx(ctx context.Context, tx txrun.Tx, id string, status int32, incidentID string) error {
	n, err := tx.Exec(ctx, "UPDATE incident_candidate SET status = $2, incident_id = COALESCE($3::uuid, incident_id), update_time = now() WHERE id = $1 AND status = 1", id, status, nilIfEmpty(incidentID))
	if err != nil {
		return fmt.Errorf("set incident candidate status: %w", err)
	}
	if n == 0 {
		return biz.ErrCandidateMissing
	}
	return nil
}

// Get implements biz.CandidateRepo.
func (r *candidateRepo) Get(ctx context.Context, id string) (*biz.Candidate, error) {
	c, err := scanCandidate(r.data.Pool.QueryRow(ctx, "SELECT "+candidateColumns+" FROM incident_candidate c WHERE c.id = $1", id).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrCandidateMissing
	}
	if err != nil {
		return nil, fmt.Errorf("get incident candidate: %w", err)
	}
	return c, nil
}

// List implements biz.CandidateRepo.
func (r *candidateRepo) List(ctx context.Context, status int32, page, pageSize int64) ([]*biz.Candidate, int64, error) {
	var a sqlArgs
	where := "TRUE"
	if status > 0 {
		where += " AND c.status = " + a.add(status)
	}
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM incident_candidate c WHERE "+where, a.args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count incident candidates: %w", err)
	}
	rows, err := r.data.Pool.Query(ctx, "SELECT "+candidateColumns+" FROM incident_candidate c WHERE "+where+" ORDER BY c.update_time DESC LIMIT "+a.add(pageSize)+" OFFSET "+a.add((page-1)*pageSize), a.args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list incident candidates: %w", err)
	}
	defer rows.Close()
	var out []*biz.Candidate
	for rows.Next() {
		c, err := scanCandidate(rows.Scan)
		if err != nil {
			return nil, 0, fmt.Errorf("scan incident candidate: %w", err)
		}
		out = append(out, c)
	}
	return out, total, rows.Err()
}

// Expire implements biz.CandidateRepo.
func (r *candidateRepo) Expire(ctx context.Context, updateBefore time.Time) (int, error) {
	tag, err := r.data.Pool.Exec(ctx, "UPDATE incident_candidate SET status = 4, update_time = now() WHERE status = 1 AND update_time < $1", updateBefore)
	if err != nil {
		return 0, fmt.Errorf("expire incident candidates: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// Counts implements biz.CandidateRepo.
func (r *candidateRepo) Counts(ctx context.Context, sr biz.StatsRange) (int, int, error) {
	var total, promoted int
	if err := r.data.Pool.QueryRow(ctx,
		"SELECT count(*), count(*) FILTER (WHERE status = 2) FROM incident_candidate WHERE first_time >= $1 AND first_time < $2", sr.Start, sr.End).Scan(&total, &promoted); err != nil {
		return 0, 0, fmt.Errorf("count incident candidates: %w", err)
	}
	return total, promoted, nil
}
