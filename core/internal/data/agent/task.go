package data

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/agent"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/idgen"

	"github.com/jackc/pgx/v5"
)

var _ biz.TaskRepo = (*taskRepo)(nil)

type taskRepo struct {
	data *basedata.Data
}

// NewTaskRepo new a taskRepo.
func NewTaskRepo(data *basedata.Data) *taskRepo { return &taskRepo{data: data} }

const taskColumns = `t.id::text, COALESCE(t.profile_id::text, ''), t.kind, t.source, t.dry_run, t.incident_id::text, t.incident_number, t.incident_title, t.severity,
	COALESCE(t.business_id::text, ''), COALESCE(t.rule_id::text, ''), t.reason, t.period_key, t.status, t.attempts, t.next_time, COALESCE(t.lock_token::text, ''), t.locked_until,
	COALESCE(t.requester_id::text, ''), t.input, t.result, COALESCE(t.run_id::text, ''), t.queued_time, t.start_time, t.end_time, t.update_time`

func scanTask(scan func(dest ...any) error) (*biz.Task, error) {
	var t biz.Task
	if err := scan(&t.ID, &t.ProfileID, &t.Kind, &t.Source, &t.DryRun, &t.IncidentID, &t.IncidentNumber, &t.IncidentTitle, &t.Severity,
		&t.BusinessID, &t.RuleID, &t.Reason, &t.PeriodKey, &t.Status, &t.Attempts, &t.NextTime, &t.LockToken, &t.LockedUntil,
		&t.RequesterID, &t.Input, &t.Result, &t.RunID, &t.QueuedTime, &t.StartTime, &t.EndTime, &t.UpdateTime); err != nil {
		return nil, err
	}
	return &t, nil
}

const taskInsert = `INSERT INTO agent_task (id, profile_id, kind, source, dry_run, incident_id, incident_number, incident_title, severity, business_id, rule_id, reason, period_key,
	status, next_time, requester_id, input, queued_time, update_time)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, 'queued', $14, $15, $16, $14, $14)`

func taskArgs(t *biz.Task) []any {
	return []any{
		t.ID, nilIfEmpty(t.ProfileID), t.Kind, t.Source, t.DryRun, t.IncidentID, t.IncidentNumber, t.IncidentTitle, t.Severity, nilIfEmpty(t.BusinessID),
		nilIfEmpty(t.RuleID), t.Reason, t.PeriodKey, t.NextTime, nilIfEmpty(t.RequesterID), t.Input,
	}
}

// EnqueueTx implements biz.TaskRepo：诊断任务命中按故障唯一时改为更新已有任务的等级（等级上调不丢）；经验沉淀按
// 复盘版本（period_key）去重；只可能返回数据库错误。
func (r *taskRepo) EnqueueTx(ctx context.Context, tx biz.Tx, t *biz.Task) error {
	if t.Kind == biz.KindPostmortem {
		var n int
		if err := tx.QueryRow(ctx, "SELECT count(*) FROM agent_task WHERE incident_id = $1 AND kind = $2 AND period_key = $3", t.IncidentID, t.Kind, t.PeriodKey).Scan(&n); err != nil {
			return fmt.Errorf("check agent postmortem task: %w", err)
		}
		if n > 0 {
			return nil
		}
		if _, err := tx.Exec(ctx, taskInsert, taskArgs(t)...); err != nil {
			return fmt.Errorf("enqueue agent postmortem task: %w", err)
		}
		return nil
	}
	// 部分唯一索引不能直接做 ON CONFLICT 的推断目标（带 WHERE 的索引可以，但要重复谓词）；用显式的两步：先更新，再插入。
	n, err := tx.Exec(ctx,
		`UPDATE agent_task SET severity = LEAST(severity, $2), incident_title = $3, update_time = now()
		 WHERE incident_id = $1 AND kind = 'diagnose' AND NOT dry_run AND status IN ('queued', 'running')`, t.IncidentID, t.Severity, t.IncidentTitle)
	if err != nil {
		return fmt.Errorf("bump agent task: %w", err)
	}
	if n > 0 {
		return nil
	}
	if _, err := tx.Exec(ctx, taskInsert, taskArgs(t)...); err != nil {
		if isUniqueViolation(err, "uk_agent_task_incident_active") {
			return nil
		}
		return fmt.Errorf("enqueue agent task: %w", err)
	}
	return nil
}

// Create implements biz.TaskRepo.
func (r *taskRepo) Create(ctx context.Context, t *biz.Task) error {
	if _, err := r.data.Pool.Exec(ctx, taskInsert, taskArgs(t)...); err != nil {
		if isUniqueViolation(err, "uk_agent_task_incident_active") {
			return biz.ErrTaskExists
		}
		return fmt.Errorf("insert agent task: %w", err)
	}
	return nil
}

// Get implements biz.TaskRepo.
func (r *taskRepo) Get(ctx context.Context, id string) (*biz.Task, error) {
	t, err := scanTask(r.data.Pool.QueryRow(ctx, "SELECT "+taskColumns+" FROM agent_task t WHERE t.id = $1", id).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrTaskNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get agent task: %w", err)
	}
	return t, nil
}

// List implements biz.TaskRepo.
func (r *taskRepo) List(ctx context.Context, f biz.TaskFilter, page, pageSize int64) ([]*biz.Task, int64, error) {
	where := []string{"1 = 1"}
	args := make([]any, 0, 6)
	add := func(cond string, v any) {
		args = append(args, v)
		where = append(where, strings.ReplaceAll(cond, "?", "$"+strconv.Itoa(len(args))))
	}
	if f.ProfileID != "" {
		add("t.profile_id::text = ?", f.ProfileID)
	}
	if f.IncidentID != "" {
		add("t.incident_id::text = ?", f.IncidentID)
	}
	if f.Status != "" {
		add("t.status = ?", f.Status)
	}
	if f.Source > 0 {
		add("t.source = ?", f.Source)
	}
	clause := strings.Join(where, " AND ")
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM agent_task t WHERE "+clause, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count agent tasks: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}
	args = append(args, pageSize, (page-1)*pageSize)
	rows, err := r.data.Pool.Query(ctx, "SELECT "+taskColumns+" FROM agent_task t WHERE "+clause+
		" ORDER BY t.queued_time DESC, t.id DESC LIMIT $"+strconv.Itoa(len(args)-1)+" OFFSET $"+strconv.Itoa(len(args)), args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list agent tasks: %w", err)
	}
	defer rows.Close()
	out := make([]*biz.Task, 0, pageSize)
	for rows.Next() {
		t, err := scanTask(rows.Scan)
		if err != nil {
			return nil, 0, fmt.Errorf("scan agent task: %w", err)
		}
		out = append(out, t)
	}
	return out, total, rows.Err()
}

// ActiveByIncident implements biz.TaskRepo.
func (r *taskRepo) ActiveByIncident(ctx context.Context, incidentID, kind string) (*biz.Task, error) {
	if kind == "" {
		kind = biz.KindDiagnose
	}
	t, err := scanTask(r.data.Pool.QueryRow(ctx, "SELECT "+taskColumns+
		" FROM agent_task t WHERE t.incident_id = $1 AND t.kind = $2 AND NOT t.dry_run AND t.status IN ('queued', 'running') ORDER BY t.queued_time DESC LIMIT 1", incidentID, kind).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get active agent task: %w", err)
	}
	return t, nil
}

// Claim implements biz.TaskRepo：单语句行锁 + SKIP LOCKED 领取到期的排队任务（S1 优先、再按入队时间）。
func (r *taskRepo) Claim(ctx context.Context, limit int, lease time.Duration) ([]*biz.Task, error) {
	token, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	rows, err := r.data.Pool.Query(ctx,
		`UPDATE agent_task t SET status = 'running', attempts = t.attempts + 1, lock_token = $2, locked_until = now() + make_interval(secs => $3::float8),
		   start_time = COALESCE(t.start_time, now()), update_time = now()
		 WHERE t.id IN (SELECT q.id FROM agent_task q WHERE q.status = 'queued' AND q.next_time <= now() ORDER BY q.severity, q.queued_time, q.id LIMIT $1 FOR UPDATE SKIP LOCKED)
		 RETURNING `+strings.ReplaceAll(taskColumns, "t.", "t."), limit, token, lease.Seconds())
	if err != nil {
		return nil, fmt.Errorf("claim agent tasks: %w", err)
	}
	defer rows.Close()
	var out []*biz.Task
	for rows.Next() {
		t, err := scanTask(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("scan claimed agent task: %w", err)
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// UPDATE ... RETURNING 不保证按子查询的顺序返回：这里再按领取顺序（等级、入队时间）排一次。
	slices.SortFunc(out, func(a, b *biz.Task) int {
		if a.Severity != b.Severity {
			return int(a.Severity - b.Severity)
		}
		return a.QueuedTime.Compare(b.QueuedTime)
	})
	return out, nil
}

// Renew implements biz.TaskRepo.
func (r *taskRepo) Renew(ctx context.Context, id, token string, lease time.Duration) (bool, error) {
	tag, err := r.data.Pool.Exec(ctx, "UPDATE agent_task SET locked_until = now() + make_interval(secs => $3::float8), update_time = now() WHERE id = $1 AND lock_token = $2 AND status = 'running'",
		id, token, lease.Seconds())
	if err != nil {
		return false, fmt.Errorf("renew agent task: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// Finish implements biz.TaskRepo.
func (r *taskRepo) Finish(ctx context.Context, id, token, status, result string) (bool, error) {
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE agent_task SET status = $3, result = $4, end_time = now(), lock_token = NULL, locked_until = NULL, update_time = now()
		 WHERE id = $1 AND status = 'running' AND ($2 = '' OR lock_token::text = $2)`, id, token, status, result)
	if err != nil {
		return false, fmt.Errorf("finish agent task: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// Requeue implements biz.TaskRepo.
func (r *taskRepo) Requeue(ctx context.Context, id, token string, backoff time.Duration, result string) (bool, error) {
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE agent_task SET status = 'queued', result = $4, next_time = now() + make_interval(secs => $3::float8), lock_token = NULL, locked_until = NULL, update_time = now()
		 WHERE id = $1 AND status = 'running' AND lock_token::text = $2`, id, token, backoff.Seconds(), result)
	if err != nil {
		return false, fmt.Errorf("requeue agent task: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// BindRun implements biz.TaskRepo.
func (r *taskRepo) BindRun(ctx context.Context, id, token, runID, profileID string) error {
	if _, err := r.data.Pool.Exec(ctx, "UPDATE agent_task SET run_id = $3, profile_id = $4, update_time = now() WHERE id = $1 AND lock_token::text = $2", id, token, runID, profileID); err != nil {
		return fmt.Errorf("bind agent run: %w", err)
	}
	return nil
}

// Cancel implements biz.TaskRepo.
func (r *taskRepo) Cancel(ctx context.Context, id, result string) (bool, error) {
	tag, err := r.data.Pool.Exec(ctx,
		"UPDATE agent_task SET status = 'cancelled', result = $2, end_time = now(), lock_token = NULL, locked_until = NULL, update_time = now() WHERE id = $1 AND status IN ('queued', 'running')", id, result)
	if err != nil {
		return false, fmt.Errorf("cancel agent task: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// ExpireQueued implements biz.TaskRepo.
func (r *taskRepo) ExpireQueued(ctx context.Context, before time.Time, limit int) (int, error) {
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE agent_task SET status = 'expired', result = '排队超时', end_time = now(), update_time = now()
		 WHERE id IN (SELECT id FROM agent_task WHERE status = 'queued' AND queued_time < $1 AND attempts = 0 LIMIT $2)`, before, limit)
	if err != nil {
		return 0, fmt.Errorf("expire agent tasks: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// ReleaseStale implements biz.TaskRepo：租约过期的 running 放回队列（持有实例已崩溃）。
func (r *taskRepo) ReleaseStale(ctx context.Context, now time.Time, limit int) (int, error) {
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE agent_task SET status = 'queued', result = '运行实例失联，重新排队', next_time = now(), lock_token = NULL, locked_until = NULL, update_time = now()
		 WHERE id IN (SELECT id FROM agent_task WHERE status = 'running' AND locked_until < $1 LIMIT $2)`, now, limit)
	if err != nil {
		return 0, fmt.Errorf("release stale agent tasks: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// CountActiveByProfile implements biz.TaskRepo.
func (r *taskRepo) CountActiveByProfile(ctx context.Context, profileID string) (int, error) {
	var n int
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM agent_task WHERE profile_id = $1 AND status IN ('queued', 'running')", profileID).Scan(&n); err != nil {
		return 0, fmt.Errorf("count active agent tasks: %w", err)
	}
	return n, nil
}

// CountByStatus implements biz.TaskRepo.
func (r *taskRepo) CountByStatus(ctx context.Context, sr biz.StatsRange) (map[string]int32, error) {
	rows, err := r.data.Pool.Query(ctx,
		"SELECT status, count(*)::int FROM agent_task WHERE queued_time >= $1 AND queued_time < $2 AND ($3 = '' OR profile_id::text = $3) GROUP BY status",
		sr.Start, sr.End, sr.ProfileID)
	if err != nil {
		return nil, fmt.Errorf("count agent tasks by status: %w", err)
	}
	defer rows.Close()
	out := map[string]int32{}
	for rows.Next() {
		var status string
		var n int32
		if err := rows.Scan(&status, &n); err != nil {
			return nil, err
		}
		out[status] = n
	}
	return out, rows.Err()
}

// Purge implements biz.TaskRepo.
func (r *taskRepo) Purge(ctx context.Context, before time.Time, limit int) (int, error) {
	tag, err := r.data.Pool.Exec(ctx, "DELETE FROM agent_task WHERE id IN (SELECT id FROM agent_task WHERE status NOT IN ('queued', 'running') AND update_time < $1 LIMIT $2)", before, limit)
	if err != nil {
		return 0, fmt.Errorf("purge agent tasks: %w", err)
	}
	return int(tag.RowsAffected()), nil
}
