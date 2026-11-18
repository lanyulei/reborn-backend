package data

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/monitor/internal/biz"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.WatchRepo    = (*selfmonRepo)(nil)
	_ biz.InstanceRepo = (*selfmonRepo)(nil)
)

// selfmonRepo 平台自监控：实例心跳（monitor_engine_instance）与自监控事件（monitor_engine_event）。
type selfmonRepo struct {
	data *Data
}

// NewSelfmonRepo new a selfmonRepo.
func NewSelfmonRepo(data *Data) *selfmonRepo { return &selfmonRepo{data: data} }

// Beat implements biz.InstanceRepo。start_time 只在首次写入时取值。
func (r *selfmonRepo) Beat(ctx context.Context, b *biz.InstanceBeat) error {
	_, err := r.data.Pool.Exec(ctx,
		`INSERT INTO monitor_engine_instance (instance, roles, version, leader, task_count, eval_lag_ms, eval_count, fail_count)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 ON CONFLICT (instance) DO UPDATE
		   SET roles = EXCLUDED.roles, version = EXCLUDED.version, leader = EXCLUDED.leader, last_seen = now(),
		       task_count = EXCLUDED.task_count, eval_lag_ms = EXCLUDED.eval_lag_ms, eval_count = EXCLUDED.eval_count,
		       fail_count = EXCLUDED.fail_count`,
		b.Instance, b.Roles, b.Version, b.Leader, b.TaskCount, b.EvalLag.Milliseconds(), b.EvalCount, b.FailCount)
	if err != nil {
		return fmt.Errorf("upsert engine instance: %w", err)
	}
	return nil
}

// Remove implements biz.InstanceRepo.
func (r *selfmonRepo) Remove(ctx context.Context, instance string) error {
	if _, err := r.data.Pool.Exec(ctx, "DELETE FROM monitor_engine_instance WHERE instance = $1", instance); err != nil {
		return fmt.Errorf("delete engine instance: %w", err)
	}
	return nil
}

// PruneInstances implements biz.WatchRepo.
func (r *selfmonRepo) PruneInstances(ctx context.Context, stale time.Duration) (int, error) {
	tag, err := r.data.Pool.Exec(ctx, "DELETE FROM monitor_engine_instance WHERE last_seen < now() - $1::interval", stale)
	if err != nil {
		return 0, fmt.Errorf("prune engine instances: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// sinceMicros 把故障起始时刻写成去重键里的那段文本（微秒）；须与 biz.engineEventKey 的写法逐字一致。
// extract(epoch …) 对 timestamptz 返回 numeric，乘以一百万后取整是精确的。
const sinceMicros = `(extract(epoch FROM %s) * 1000000)::bigint::text`

// DatasourceOutages implements biz.WatchRepo。只认最近探过的结果（health_time 在阈值之内）：数据源停用期间
// 不探活，重新启用后、第一次探活之前，行上留着的是停用前的旧状态，不能据此上报。
func (r *selfmonRepo) DatasourceOutages(ctx context.Context, after time.Duration, limit int) ([]biz.DatasourceOutage, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT d.id::text, d.name, d.health_error, d.health_since, now()
		 FROM monitor_datasource d
		 WHERE d.enabled AND d.delete_time IS NULL AND d.health_status = $1 AND d.health_since <= now() - $2::interval
		   AND d.health_time > now() - $2::interval
		   AND NOT EXISTS (
		     SELECT 1 FROM monitor_engine_event e
		     WHERE e.dedup_key = $3 || ':' || d.id::text || ':' || `+fmt.Sprintf(sinceMicros, "d.health_since")+`)
		 ORDER BY d.health_since LIMIT $4`,
		monitorcontract.HealthFailed, after, monitorcontract.EventEngineDatasourceDown, limit)
	if err != nil {
		return nil, fmt.Errorf("list datasource outages: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (biz.DatasourceOutage, error) {
		var o biz.DatasourceOutage
		err := row.Scan(&o.DatasourceID, &o.Name, &o.Error, &o.Since, &o.Now)
		return o, err
	})
	if err != nil {
		return nil, fmt.Errorf("list datasource outages: %w", err)
	}
	return out, nil
}

// ReportDatasourceDown implements biz.WatchRepo。FOR UPDATE 锁住数据源行：与探活的状态翻转（WriteHealth 先锁
// 该行）串行——翻转先提交，则这里重新求值时条件不再成立、不写；这里先提交，则翻转看得到这条故障事件。
func (r *selfmonRepo) ReportDatasourceDown(ctx context.Context, since time.Time, ev *biz.EngineEventInsert) (bool, error) {
	detail, err := json.Marshal(&ev.Detail)
	if err != nil {
		return false, fmt.Errorf("encode engine event detail: %w", err)
	}
	tag, err := r.data.Pool.Exec(ctx,
		`INSERT INTO monitor_engine_event (id, type, object_id, dedup_key, detail)
		 SELECT $1::uuid, $2, d.id, $3, $4::jsonb FROM monitor_datasource d
		 WHERE d.id = $5 AND d.enabled AND d.delete_time IS NULL AND d.health_status = $6 AND d.health_since = $7
		 FOR UPDATE OF d
		 ON CONFLICT (dedup_key) DO NOTHING`,
		ev.ID, ev.Type, ev.DedupKey, string(detail), ev.ObjectID, monitorcontract.HealthFailed, since)
	if err != nil {
		return false, fmt.Errorf("report datasource down: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// RuleFailures implements biz.WatchRepo。数据源须同样长时间保持「不是异常」：数据源不可用期间它上面的规则
// 都在报查询错误，那是数据源的事件；数据源刚恢复时任务行上的旧错误还没被下一轮清掉，也不算规则的故障。
func (r *selfmonRepo) RuleFailures(ctx context.Context, after time.Duration, minFailures, limit int) ([]biz.RuleFailure, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT t.rule_id::text, r.name, t.datasource_id::text, d.name, t.last_error, t.error_since, now(), t.error_count
		 FROM monitor_eval_task t
		 JOIN monitor_rule r ON r.id = t.rule_id
		 JOIN monitor_datasource d ON d.id = t.datasource_id
		 WHERE t.error_since <= now() - $1::interval AND t.error_count >= $2
		   AND d.health_status <> $3 AND d.health_since <= now() - $1::interval
		   AND NOT EXISTS (
		     SELECT 1 FROM monitor_engine_event e
		     WHERE e.dedup_key = $4 || ':' || t.rule_id::text || ':' || t.datasource_id::text || ':' || `+
			fmt.Sprintf(sinceMicros, "t.error_since")+`)
		 ORDER BY t.error_since LIMIT $5`,
		after, minFailures, monitorcontract.HealthFailed, monitorcontract.EventEngineRuleFailing, limit)
	if err != nil {
		return nil, fmt.Errorf("list failing rules: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (biz.RuleFailure, error) {
		var f biz.RuleFailure
		err := row.Scan(&f.RuleID, &f.RuleName, &f.DatasourceID, &f.DatasourceName, &f.Error, &f.Since, &f.Now, &f.Failures)
		return f, err
	})
	if err != nil {
		return nil, fmt.Errorf("list failing rules: %w", err)
	}
	return out, nil
}

// ReportRuleFailing implements biz.WatchRepo。FOR UPDATE 锁住任务行：与评估的提交事务（先锁任务行，成功的
// 一轮在同一事务里清掉 error_since 并写恢复事件）串行，道理同 ReportDatasourceDown。一条语句只锁一个任务行。
func (r *selfmonRepo) ReportRuleFailing(ctx context.Context, datasourceID string, since time.Time, ev *biz.EngineEventInsert) (bool, error) {
	detail, err := json.Marshal(&ev.Detail)
	if err != nil {
		return false, fmt.Errorf("encode engine event detail: %w", err)
	}
	tag, err := r.data.Pool.Exec(ctx,
		`INSERT INTO monitor_engine_event (id, type, object_id, dedup_key, detail)
		 SELECT $1::uuid, $2, t.rule_id, $3, $4::jsonb FROM monitor_eval_task t
		 WHERE t.rule_id = $5 AND t.datasource_id = $6 AND t.error_since = $7
		 FOR UPDATE OF t
		 ON CONFLICT (dedup_key) DO NOTHING`,
		ev.ID, ev.Type, ev.DedupKey, string(detail), ev.ObjectID, datasourceID, since)
	if err != nil {
		return false, fmt.Errorf("report rule failing: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}
