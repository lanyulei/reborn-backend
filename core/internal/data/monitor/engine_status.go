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
	_ biz.EngineReader = (*engineRepo)(nil)
	_ biz.EngineIssues = (*engineRepo)(nil)
)

// engineRepo 引擎运行状态的只读查询（心跳、任务、发件箱都由评估引擎写；时间比较一律用数据库时钟）。
type engineRepo struct {
	data *basedata.Data
}

// NewEngineRepo new an engineRepo.
func NewEngineRepo(data *basedata.Data) *engineRepo { return &engineRepo{data: data} }

// maxEngineInstances 返回的引擎实例数上限。
const maxEngineInstances = 200

// Instances implements biz.EngineReader.
func (r *engineRepo) Instances(ctx context.Context, alive, stale time.Duration) ([]*biz.EngineInstance, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT i.instance, i.roles, i.version, i.leader, i.last_seen >= now() - make_interval(secs => $1::float8) AS alive,
		        i.start_time, i.last_seen, i.task_count, i.eval_lag_ms, i.eval_count, i.fail_count
		 FROM monitor_engine_instance i WHERE i.last_seen >= now() - make_interval(secs => $2::float8)
		 ORDER BY alive DESC, i.instance LIMIT $3`, alive.Seconds(), stale.Seconds(), maxEngineInstances)
	if err != nil {
		return nil, fmt.Errorf("list monitor engine instances: %w", err)
	}
	defer rows.Close()
	out := make([]*biz.EngineInstance, 0, 8)
	for rows.Next() {
		var it biz.EngineInstance
		var roles string
		if err := rows.Scan(&it.Instance, &roles, &it.Version, &it.Leader, &it.Alive, &it.StartTime, &it.LastSeen,
			&it.TaskCount, &it.EvalLagMS, &it.EvalCount, &it.FailCount); err != nil {
			return nil, fmt.Errorf("scan monitor engine instance: %w", err)
		}
		it.Roles = biz.SplitEngineRoles(roles)
		// 已不存活的实例上残留的选主标记不可信（它没机会清掉）。
		it.Leader = it.Leader && it.Alive
		out = append(out, &it)
	}
	return out, rows.Err()
}

// Summary implements biz.EngineReader。评估任务只计启用且未删除的规则（停用规则的任务行由引擎稍后清理，
// 不算积压）；逾期 = 应评估时刻已过去两个评估周期；最大延迟 = 最久未被评估的到期任务已逾期多久。
func (r *engineRepo) Summary(ctx context.Context) (*biz.EngineSummary, error) {
	var s biz.EngineSummary
	err := r.data.Pool.QueryRow(ctx, `
		SELECT t.total, t.overdue, t.failed, t.lag_ms, rt.total, rt.failed,
		       (SELECT count(*) FROM monitor_notify_outbox o WHERE o.status = $1)
		FROM (SELECT count(*) AS total,
		             count(*) FILTER (WHERE t.next_eval_time < now() - make_interval(secs => 2 * r.eval_interval_s)) AS overdue,
		             count(*) FILTER (WHERE t.last_status = $2) AS failed,
		             COALESCE(max(extract(epoch FROM now() - t.next_eval_time) * 1000) FILTER (WHERE t.next_eval_time < now()), 0)::bigint AS lag_ms
		      FROM monitor_eval_task t JOIN monitor_rule r ON r.id = t.rule_id AND r.enabled AND r.delete_time IS NULL) t,
		     (SELECT count(*) AS total, count(*) FILTER (WHERE k.last_status IN ($3, $4)) AS failed
		      FROM monitor_record_task k JOIN monitor_record_rule rr ON rr.id = k.record_rule_id AND rr.enabled AND rr.delete_time IS NULL) rt`,
		mc.OutboxPending, mc.EvalError, mc.RecordQueryError, mc.RecordWriteError).
		Scan(&s.TaskTotal, &s.TaskOverdue, &s.TaskError, &s.MaxLagMS, &s.RecordTaskTotal, &s.RecordTaskError, &s.NotifyPending)
	if err != nil {
		return nil, fmt.Errorf("monitor engine summary: %w", err)
	}
	return &s, nil
}

// Datasources implements biz.EngineIssues。受影响的规则数 = 在该数据源上有评估任务的规则数（逐行子查询，
// 行数以 limit 为界，走 idx_monitor_eval_task_datasource）。
func (r *engineRepo) Datasources(ctx context.Context, limit int) ([]*biz.EngineDatasourceIssue, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT d.id::text, d.name, d.health_error, COALESCE(d.health_since, d.health_time),
		        (SELECT count(*) FROM monitor_eval_task t WHERE t.datasource_id = d.id)
		 FROM monitor_datasource d
		 WHERE d.delete_time IS NULL AND d.enabled AND d.health_status = $1
		 ORDER BY COALESCE(d.health_since, d.health_time) NULLS LAST, d.id LIMIT $2`, mc.HealthFailed, limit)
	if err != nil {
		return nil, fmt.Errorf("list unhealthy monitor datasources: %w", err)
	}
	defer rows.Close()
	out := make([]*biz.EngineDatasourceIssue, 0, 4)
	for rows.Next() {
		var it biz.EngineDatasourceIssue
		if err := rows.Scan(&it.ID, &it.Name, &it.Error, &it.Since, &it.RuleCount); err != nil {
			return nil, fmt.Errorf("scan unhealthy monitor datasource: %w", err)
		}
		out = append(out, &it)
	}
	return out, rows.Err()
}

// Rules implements biz.EngineIssues.
func (r *engineRepo) Rules(ctx context.Context, groups biz.GroupFilter, threshold, limit int) ([]*biz.EngineRuleIssue, error) {
	if groups.Empty() {
		return []*biz.EngineRuleIssue{}, nil
	}
	var a sqlArgs
	where := "t.error_count >= " + a.add(threshold) + groupCond(&a, "r.group_id", groups)
	rows, err := r.data.Pool.Query(ctx,
		`SELECT r.id::text, r.name, r.group_id::text, t.datasource_id::text, COALESCE(d.name, ''), t.last_error, t.error_since, t.error_count
		 FROM monitor_eval_task t
		 JOIN monitor_rule r ON r.id = t.rule_id AND r.enabled AND r.delete_time IS NULL
		 LEFT JOIN monitor_datasource d ON d.id = t.datasource_id
		 WHERE `+where+" ORDER BY t.error_since NULLS LAST, r.id, t.datasource_id LIMIT "+a.add(limit), a.args...)
	if err != nil {
		return nil, fmt.Errorf("list failing monitor rules: %w", err)
	}
	defer rows.Close()
	out := make([]*biz.EngineRuleIssue, 0, 8)
	for rows.Next() {
		var it biz.EngineRuleIssue
		if err := rows.Scan(&it.RuleID, &it.RuleName, &it.GroupID, &it.DatasourceID, &it.DatasourceName, &it.Error, &it.Since, &it.Failures); err != nil {
			return nil, fmt.Errorf("scan failing monitor rule: %w", err)
		}
		out = append(out, &it)
	}
	return out, rows.Err()
}
