package data

import (
	"context"
	"errors"
	"fmt"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/agent"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.RunRepo    = (*runRepo)(nil)
	_ biz.BudgetRepo = (*budgetRepo)(nil)
)

// ---------------------------------------------------------------------------
// 运行
// ---------------------------------------------------------------------------

type runRepo struct {
	data *basedata.Data
}

// NewRunRepo new a runRepo.
func NewRunRepo(data *basedata.Data) *runRepo { return &runRepo{data: data} }

const runColumns = `r.id::text, r.task_id::text, r.profile_id::text, r.incident_id::text, r.agent_user_id::text, r.conversation_id::text, r.status, r.dry_run, r.stages,
	r.prompt_tokens, r.completion_tokens, r.tokens_estimated, r.finding, COALESCE(r.finding_event_id::text, ''), r.proposals, r.denied, r.captured, r.end_reason,
	r.feedback, COALESCE(r.feedback_user::text, ''), r.feedback_time, r.feedback_note, r.start_time, r.end_time, r.create_time`

func scanRun(scan func(dest ...any) error) (*biz.Run, error) {
	var r biz.Run
	var stages, finding, proposals, denied, captured []byte
	if err := scan(&r.ID, &r.TaskID, &r.ProfileID, &r.IncidentID, &r.AgentUserID, &r.ConversationID, &r.Status, &r.DryRun, &stages,
		&r.PromptTokens, &r.CompletionTokens, &r.TokensEstimated, &finding, &r.FindingEventID, &proposals, &denied, &captured, &r.EndReason,
		&r.Feedback, &r.FeedbackUser, &r.FeedbackTime, &r.FeedbackNote, &r.StartTime, &r.EndTime, &r.CreateTime); err != nil {
		return nil, err
	}
	if err := decodeJSON(stages, &r.Stages, "agent run stages"); err != nil {
		return nil, err
	}
	if len(finding) > 0 {
		r.Finding = &biz.Finding{}
		if err := decodeJSON(finding, r.Finding, "agent run finding"); err != nil {
			return nil, err
		}
	}
	if err := decodeJSON(proposals, &r.Proposals, "agent run proposals"); err != nil {
		return nil, err
	}
	if err := decodeJSON(denied, &r.Denied, "agent run denied"); err != nil {
		return nil, err
	}
	if err := decodeJSON(captured, &r.Captured, "agent run captured"); err != nil {
		return nil, err
	}
	return &r, nil
}

func runArgs(r *biz.Run) ([]any, error) {
	stages, err := encodeJSON(nonNil(r.Stages), "agent run stages")
	if err != nil {
		return nil, err
	}
	var finding any
	if r.Finding != nil {
		raw, err := encodeJSON(r.Finding, "agent run finding")
		if err != nil {
			return nil, err
		}
		finding = raw
	}
	proposals, err := encodeJSON(nonNil(r.Proposals), "agent run proposals")
	if err != nil {
		return nil, err
	}
	denied, err := encodeJSON(nonNil(r.Denied), "agent run denied")
	if err != nil {
		return nil, err
	}
	captured, err := encodeJSON(nonNil(r.Captured), "agent run captured")
	if err != nil {
		return nil, err
	}
	return []any{r.Status, stages, r.PromptTokens, r.CompletionTokens, r.TokensEstimated, finding, nilIfEmpty(r.FindingEventID), proposals, denied, captured, r.EndReason, nilTime(r.EndTime)}, nil
}

// nonNil nil 切片落空数组。
func nonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

// Create implements biz.RunRepo.
func (r *runRepo) Create(ctx context.Context, run *biz.Run) error {
	args, err := runArgs(run)
	if err != nil {
		return err
	}
	if _, err := r.data.Pool.Exec(ctx,
		`INSERT INTO agent_run (id, task_id, profile_id, incident_id, agent_user_id, conversation_id, dry_run, start_time, create_time,
		   status, stages, prompt_tokens, completion_tokens, tokens_estimated, finding, finding_event_id, proposals, denied, captured, end_reason, end_time)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)`,
		append([]any{run.ID, run.TaskID, run.ProfileID, run.IncidentID, run.AgentUserID, run.ConversationID, run.DryRun, run.StartTime}, args...)...); err != nil {
		return fmt.Errorf("insert agent run: %w", err)
	}
	return nil
}

// Get implements biz.RunRepo.
func (r *runRepo) Get(ctx context.Context, id string) (*biz.Run, error) {
	run, err := scanRun(r.data.Pool.QueryRow(ctx, "SELECT "+runColumns+" FROM agent_run r WHERE r.id = $1", id).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrRunNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get agent run: %w", err)
	}
	return run, nil
}

// SaveRunning implements biz.RunRepo.
func (r *runRepo) SaveRunning(ctx context.Context, run *biz.Run) (bool, error) {
	args, err := runArgs(run)
	if err != nil {
		return false, err
	}
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE agent_run SET status = $2, stages = $3, prompt_tokens = $4, completion_tokens = $5, tokens_estimated = $6, finding = $7, finding_event_id = $8,
		   proposals = $9, denied = $10, captured = $11, end_reason = $12, end_time = $13 WHERE id = $1 AND status = 'running'`, append([]any{run.ID}, args...)...)
	if err != nil {
		return false, fmt.Errorf("save running agent run: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// Close implements biz.RunRepo.
func (r *runRepo) Close(ctx context.Context, id, status, reason string, end time.Time) (bool, error) {
	tag, err := r.data.Pool.Exec(ctx,
		"UPDATE agent_run SET status = $2, end_reason = $3, end_time = $4 WHERE id = $1 AND status = 'running'", id, status, reason, end)
	if err != nil {
		return false, fmt.Errorf("close agent run: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// ListByIncident implements biz.RunRepo.
func (r *runRepo) ListByIncident(ctx context.Context, incidentID string, limit int) ([]*biz.Run, error) {
	rows, err := r.data.Pool.Query(ctx, "SELECT "+runColumns+" FROM agent_run r WHERE r.incident_id = $1 ORDER BY r.start_time DESC LIMIT $2", incidentID, limit)
	if err != nil {
		return nil, fmt.Errorf("list agent runs: %w", err)
	}
	defer rows.Close()
	out := make([]*biz.Run, 0, 8)
	for rows.Next() {
		run, err := scanRun(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("scan agent run: %w", err)
		}
		out = append(out, run)
	}
	return out, rows.Err()
}

// SetFeedback implements biz.RunRepo.
func (r *runRepo) SetFeedback(ctx context.Context, id string, feedback int32, userID, note string, at time.Time) error {
	if _, err := r.data.Pool.Exec(ctx, "UPDATE agent_run SET feedback = $2, feedback_user = $3, feedback_note = $4, feedback_time = $5 WHERE id = $1", id, feedback, userID, note, at); err != nil {
		return fmt.Errorf("set agent run feedback: %w", err)
	}
	return nil
}

// Stats implements biz.RunRepo.
func (r *runRepo) Stats(ctx context.Context, sr biz.StatsRange) (*biz.Overview, []biz.ProfileStat, error) {
	out := &biz.Overview{}
	err := r.data.Pool.QueryRow(ctx,
		`SELECT count(*)::int,
		        count(*) FILTER (WHERE status = 'succeeded')::int,
		        count(*) FILTER (WHERE status = 'no_finding')::int,
		        count(*) FILTER (WHERE status = 'failed')::int,
		        COALESCE(avg(EXTRACT(EPOCH FROM (end_time - start_time))) FILTER (WHERE end_time IS NOT NULL), 0),
		        COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (end_time - start_time))) FILTER (WHERE end_time IS NOT NULL), 0),
		        COALESCE(sum(prompt_tokens), 0)::bigint, COALESCE(sum(completion_tokens), 0)::bigint,
		        COALESCE(sum(jsonb_array_length(proposals)), 0)::int,
		        count(*) FILTER (WHERE feedback = 1)::int, count(*) FILTER (WHERE feedback = 2)::int
		 FROM agent_run WHERE start_time >= $1 AND start_time < $2 AND ($3 = '' OR profile_id::text = $3) AND NOT dry_run`,
		sr.Start, sr.End, sr.ProfileID).Scan(&out.Runs, &out.Succeeded, &out.NoFinding, &out.Failed, &out.AvgDurationS, &out.P95DurationS,
		&out.PromptTokens, &out.CompletionTokens, &out.Proposals, &out.Helpful, &out.Unhelpful)
	if err != nil {
		return nil, nil, fmt.Errorf("agent run stats: %w", err)
	}
	if err := r.data.Pool.QueryRow(ctx,
		`SELECT count(*)::int FROM incident_proposal p
		 WHERE p.proposer_type = 3 AND p.auto_eligible AND p.create_time >= $1 AND p.create_time < $2
		   AND ($3 = '' OR EXISTS (SELECT 1 FROM agent_run r WHERE r.incident_id = p.incident_id AND r.profile_id::text = $3))`,
		sr.Start, sr.End, sr.ProfileID).Scan(&out.AutoEligible); err != nil {
		return nil, nil, fmt.Errorf("agent auto eligible stats: %w", err)
	}
	rows, err := r.data.Pool.Query(ctx,
		`SELECT r.profile_id::text, COALESCE(p.name, ''), count(*)::int,
		        count(*) FILTER (WHERE r.status = 'succeeded')::int, count(*) FILTER (WHERE r.status = 'no_finding')::int, count(*) FILTER (WHERE r.status = 'failed')::int,
		        count(*) FILTER (WHERE r.feedback = 1)::int, count(*) FILTER (WHERE r.feedback = 2)::int,
		        COALESCE(sum(r.prompt_tokens), 0)::bigint, COALESCE(sum(r.completion_tokens), 0)::bigint,
		        COALESCE(avg(EXTRACT(EPOCH FROM (r.end_time - r.start_time))) FILTER (WHERE r.end_time IS NOT NULL), 0)
		 FROM agent_run r LEFT JOIN agent_profile p ON p.id = r.profile_id
		 WHERE r.start_time >= $1 AND r.start_time < $2 AND ($3 = '' OR r.profile_id::text = $3) AND NOT r.dry_run
		 GROUP BY r.profile_id, p.name ORDER BY count(*) DESC`, sr.Start, sr.End, sr.ProfileID)
	if err != nil {
		return nil, nil, fmt.Errorf("agent profile stats: %w", err)
	}
	defer rows.Close()
	var byProfile []biz.ProfileStat
	for rows.Next() {
		var s biz.ProfileStat
		if err := rows.Scan(&s.ProfileID, &s.ProfileName, &s.Tasks, &s.Succeeded, &s.NoFinding, &s.Failed, &s.Helpful, &s.Unhelpful, &s.PromptTokens, &s.CompletionTokens, &s.AvgDurationS); err != nil {
			return nil, nil, err
		}
		byProfile = append(byProfile, s)
	}
	return out, byProfile, rows.Err()
}

// agentRunEvidenceRE 建议证据里指向一次运行的条目：agent_run:<规范写法的 uuid>（前缀 10 个字符）。
const agentRunEvidenceRE = `^agent_run:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`

// RecentAutoOutcome implements biz.RunRepo：该 Profile 的运行提出的、自动批准的建议里，验证窗口已出结果的总数与失败数。
func (r *runRepo) RecentAutoOutcome(ctx context.Context, profileID string, window time.Duration) (int, int, error) {
	// 证据与运行的连接写成 r.id = <从证据里取出的 uuid>，能走 agent_run 主键；原来的
	// e = 'agent_run:' || r.id::text 要对 agent_run 的每一行算一遍表达式，而这条语句在每个开启自动通过的
	// Profile 任务结束时都会执行。两者等价：uuid 的文本形式恒为规范写法，所以「e 等于前缀 + r.id 的文本」
	// 当且仅当「e 符合 agentRunEvidenceRE 且其中的 uuid 等于 r.id」；CASE 保证不符合格式的证据不会被拿去
	// 转 uuid（得 NULL，连接不上，与原来一样不命中、不报错）。
	var total, failed int
	err := r.data.Pool.QueryRow(ctx,
		`SELECT count(*)::int, count(*) FILTER (WHERE p.status = 6 OR (p.verify_result ->> 'worse')::boolean)::int
		 FROM incident_proposal p
		 WHERE p.proposer_type = 3 AND p.auto AND p.status IN (5, 6) AND p.create_time >= now() - make_interval(secs => $2::float8)
		   AND jsonb_typeof(p.evidence) = 'array'
		   AND EXISTS (SELECT 1 FROM jsonb_array_elements_text(p.evidence) e
		               JOIN agent_run r ON r.id = CASE WHEN e ~ '`+agentRunEvidenceRE+`' THEN substring(e FROM 11)::uuid END
		               WHERE r.profile_id = $1)`,
		profileID, window.Seconds()).Scan(&total, &failed)
	if err != nil {
		return 0, 0, fmt.Errorf("agent auto outcome: %w", err)
	}
	return total, failed, nil
}

// PurgeBefore implements biz.RunRepo：运行与它的助手会话在同一事务里物理删除。先删运行、再另起事务删
// 会话的话，后一步失败时运行已经没了，再没有记录指向那些会话，它们会永久残留。
// 会话三张表的删除顺序与 assistant 域 conversationRepo.Purge 一致（工具调用 → 消息 → 会话）。
func (r *runRepo) PurgeBefore(ctx context.Context, before time.Time, limit int) (int, error) {
	var n int
	err := dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		convs, err := purgeRuns(ctx, tx, before, limit)
		if err != nil {
			return fmt.Errorf("purge agent runs: %w", err)
		}
		n = len(convs)
		if n == 0 {
			return nil
		}
		// dup: 与 assistant 域 conversationRepo.Purge 的三条删除语句逐字相同。各域的 data 包不互相 import，
		// 而会话必须与运行在同一事务里删除，没法复用那边自己开事务的 Purge；改那边的表结构 / 删除顺序时这里要同步。
		for _, stmt := range []string{
			"DELETE FROM assistant_tool_call WHERE conversation_id = ANY($1::uuid[])",
			"DELETE FROM assistant_message WHERE conversation_id = ANY($1::uuid[])",
			"DELETE FROM assistant_conversation WHERE id = ANY($1::uuid[])",
		} {
			if _, err := tx.Exec(ctx, stmt, convs); err != nil {
				return fmt.Errorf("purge agent run conversations: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

// purgeRuns 删除到期的运行，返回它们的会话 ID。
func purgeRuns(ctx context.Context, tx pgx.Tx, before time.Time, limit int) ([]string, error) {
	rows, err := tx.Query(ctx,
		"DELETE FROM agent_run WHERE id IN (SELECT id FROM agent_run WHERE end_time IS NOT NULL AND end_time < $1::timestamptz LIMIT $2::int) RETURNING conversation_id::text", before, limit)
	if err != nil {
		return nil, err
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

// ---------------------------------------------------------------------------
// 预算
// ---------------------------------------------------------------------------

type budgetRepo struct {
	data *basedata.Data
}

// NewBudgetRepo new a budgetRepo.
func NewBudgetRepo(data *basedata.Data) *budgetRepo { return &budgetRepo{data: data} }

// Add implements biz.BudgetRepo.
func (r *budgetRepo) Add(ctx context.Context, profileID, period string, tasks int32, prompt, completion int64) error {
	if _, err := r.data.Pool.Exec(ctx,
		`INSERT INTO agent_budget (profile_id, period, tasks, prompt_tokens, completion_tokens, update_time) VALUES ($1, $2, $3, $4, $5, now())
		 ON CONFLICT (profile_id, period) DO UPDATE SET tasks = agent_budget.tasks + EXCLUDED.tasks, prompt_tokens = agent_budget.prompt_tokens + EXCLUDED.prompt_tokens,
		   completion_tokens = agent_budget.completion_tokens + EXCLUDED.completion_tokens, update_time = now()`,
		profileID, period, tasks, prompt, completion); err != nil {
		return fmt.Errorf("add agent budget: %w", err)
	}
	return nil
}

// Get implements biz.BudgetRepo.
func (r *budgetRepo) Get(ctx context.Context, profileID, period string) (*biz.Budget, error) {
	var b biz.Budget
	err := r.data.Pool.QueryRow(ctx, "SELECT tasks, prompt_tokens, completion_tokens FROM agent_budget WHERE profile_id = $1 AND period = $2", profileID, period).Scan(&b.Tasks, &b.PromptTokens, &b.CompletionTokens)
	if errors.Is(err, pgx.ErrNoRows) {
		return &biz.Budget{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get agent budget: %w", err)
	}
	return &b, nil
}

// Total implements biz.BudgetRepo.
func (r *budgetRepo) Total(ctx context.Context, period string) (*biz.Budget, error) {
	var b biz.Budget
	if err := r.data.Pool.QueryRow(ctx, "SELECT COALESCE(sum(tasks), 0)::int, COALESCE(sum(prompt_tokens), 0)::bigint, COALESCE(sum(completion_tokens), 0)::bigint FROM agent_budget WHERE period = $1", period).
		Scan(&b.Tasks, &b.PromptTokens, &b.CompletionTokens); err != nil {
		return nil, fmt.Errorf("total agent budget: %w", err)
	}
	return &b, nil
}
