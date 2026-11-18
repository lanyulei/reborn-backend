package data

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lanyulei/reborn/backend/kit/dbx"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/monitor/internal/biz"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	_ biz.TaskRepo    = (*taskRepo)(nil)
	_ biz.PlannerRepo = (*plannerRepo)(nil)
)

// leaseTable 一张带租约的任务表（评估任务、记录规则任务）的领取与归还语句。两张表的租约列完全一样
// （next_eval_time / lease_owner / lease_until / last_status / last_eval_time），只有主键与取数据源的方式不同，
// 语句由同一份文本拼出：租约、围栏、顺延标记的口径只有一处。全部片段都是本文件里的常量，不含外部输入。
type leaseTable struct {
	claim, release string
}

// newLeaseTable 拼出一张任务表的语句。keys 是主键列（逗号分隔）；join 把 due 关联到数据源（别名 d）；
// match / matchKey 是「任务行 t 与 due / 与归还参数 k 是同一行」的条件；rule / datasource / errors 是返回的
// 规则 ID、数据源 ID（文本，可为空串）与连续查询错误两列的表达式。
func newLeaseTable(table, keys, join, match, matchKey, rule, datasource, errors string) leaseTable {
	return leaseTable{
		claim: `WITH due AS MATERIALIZED (
		   SELECT ` + keys + `, lease_owner IS NULL AND lease_until IS NOT NULL AS postponed
		   FROM ` + table + `
		   WHERE next_eval_time <= now() AND (lease_until IS NULL OR lease_until < now())
		   ORDER BY next_eval_time LIMIT $2 FOR UPDATE SKIP LOCKED)
		 UPDATE ` + table + ` t
		 SET lease_owner = $1,
		     lease_until = now() + greatest($3::interval,
		       3 * least(COALESCE(d.timeout_ms * interval '1 millisecond', $4::interval), $4::interval) + $5::interval)
		 FROM due ` + join + `
		 WHERE ` + match + `
		 RETURNING ` + rule + `, ` + datasource + `, extract(epoch FROM now() - t.next_eval_time)::float8, t.last_status,
		           t.last_eval_time, due.postponed, ` + errors,
		release: `UPDATE ` + table + ` t
		 SET lease_owner = NULL, lease_until = CASE WHEN k.postponed THEN now() END,
		     next_eval_time = greatest(t.next_eval_time, now() + $2::interval)
		 FROM unnest($3::uuid[], $4::text[], $5::bool[]) AS k(rule_id, datasource_id, postponed)
		 WHERE ` + matchKey + ` AND t.lease_owner = $1`,
	}
}

var (
	evalTasks = newLeaseTable("monitor_eval_task", "rule_id, datasource_id",
		"LEFT JOIN monitor_datasource d ON d.id = due.datasource_id",
		"t.rule_id = due.rule_id AND t.datasource_id = due.datasource_id",
		"t.rule_id = k.rule_id AND t.datasource_id = k.datasource_id::uuid",
		"t.rule_id::text", "t.datasource_id::text", "t.error_since, t.error_count")
	recordTasks = newLeaseTable("monitor_record_task", "record_rule_id",
		`LEFT JOIN monitor_record_rule rr ON rr.id = due.record_rule_id
		 LEFT JOIN monitor_datasource d ON d.id = rr.datasource_id`,
		"t.record_rule_id = due.record_rule_id",
		"t.record_rule_id = k.rule_id",
		"t.record_rule_id::text", "COALESCE(rr.datasource_id::text, '')", "NULL::timestamptz, 0")
)

// taskRepo 任务的领取与租约（评估任务与记录规则任务各一个实例，语句见 leaseTable）。
// 全部时间取数据库 now()，与到期判定同一时钟源。
type taskRepo struct {
	data  *Data
	table leaseTable
}

// NewTaskRepo 评估任务（monitor_eval_task）的 taskRepo。
func NewTaskRepo(data *Data) *taskRepo { return &taskRepo{data: data, table: evalTasks} }

// Claim implements biz.TaskRepo：单语句 SKIP LOCKED 选取到期且无有效租约的任务并写入租约。
// 租约 = max(lease, 3 × min(数据源 timeout_ms, maxTimeout) + margin)；数据源行不存在时按 maxTimeout。
// 三段各至多一个查询超时：等数据源并发槽、判定查询、恢复取值查询（记录规则：等槽、查询、写回）。
// next_eval_time 不动：租约过期的任务仍排在到期队列最前，他人可立刻重领。
// 同时带回任务行记录的上一轮结果与时刻、连续查询错误的起点与次数，以及「被顺延过」的标记（状态机据此
// 判断计时是否跨越了没有观察的轮次）。
//
// 顺延标记不占新列：归还被顺延的任务时 lease_owner 清空而 lease_until 留一个已过期的时刻（Release）。
// 「无属主但 lease_until 非空」只有这一种来历——评估完成的写回与普通归还都把两列一起清空，租约过期的
// 任务属主非空。
func (r *taskRepo) Claim(ctx context.Context, token string, limit int, lease, maxTimeout, margin time.Duration) ([]*biz.Task, error) {
	rows, err := r.data.Pool.Query(ctx, r.table.claim, token, limit, lease, maxTimeout, margin)
	if err != nil {
		return nil, fmt.Errorf("claim tasks: %w", err)
	}
	defer rows.Close()
	tasks := make([]*biz.Task, 0, limit)
	for rows.Next() {
		t := &biz.Task{Token: token}
		var (
			lag                  float64
			lastEval, errorSince *time.Time
		)
		if err := rows.Scan(&t.RuleID, &t.DatasourceID, &lag, &t.LastStatus, &lastEval, &t.Postponed, &errorSince, &t.ErrorCount); err != nil {
			return nil, fmt.Errorf("scan claimed task: %w", err)
		}
		t.Lag = time.Duration(lag * float64(time.Second))
		if lastEval != nil {
			t.LastEval = *lastEval
		}
		if errorSince != nil {
			t.ErrorSince = *errorSince
		}
		tasks = append(tasks, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("claim tasks: %w", err)
	}
	return tasks, nil
}

// Release implements biz.TaskRepo（属主围栏；已被他人重领的任务不受影响）。被顺延过的任务
// （Task.Postponed）留下标记：lease_until = now()，它立刻过期、不妨碍任何人领取，见 Claim。
func (r *taskRepo) Release(ctx context.Context, token string, tasks []*biz.Task, delay time.Duration) error {
	ruleIDs, datasourceIDs, postponed := make([]string, len(tasks)), make([]string, len(tasks)), make([]bool, len(tasks))
	for i, t := range tasks {
		ruleIDs[i], datasourceIDs[i], postponed[i] = t.RuleID, t.DatasourceID, t.Postponed
	}
	if _, err := r.data.Pool.Exec(ctx, r.table.release, token, delay, ruleIDs, datasourceIDs, postponed); err != nil {
		return fmt.Errorf("release tasks: %w", err)
	}
	return nil
}

// plannerRepo 规划器的写入口。
type plannerRepo struct {
	data *Data
}

// NewPlannerRepo new a plannerRepo.
func NewPlannerRepo(data *Data) *plannerRepo { return &plannerRepo{data: data} }

// applies 任务成立的条件：规则启用且未删除，数据源启用且未删除，数据源在规则的范围内。
// 两处语句（补齐 / 清理）共用同一段，保证口径一致。
const applies = `r.enabled AND r.delete_time IS NULL AND d.enabled AND d.delete_time IS NULL
	AND (r.datasource_scope = 2 OR (r.datasource_scope = 1 AND d.id = ANY (r.datasource_ids)))`

// firstEvalJitter 新任务首次评估时刻的抖动上限：一批规则同时启用（导入、启用数据源）时不在同一瞬间
// 全部到期。实际抖动 = random() × min(评估周期, firstEvalJitter)；之后每轮 next = now() + 周期，
// 相位得以保持分散。
const firstEvalJitter = 30 * time.Second

// Materialize implements biz.PlannerRepo。
//
// 补齐是单条幂等语句（ON CONFLICT DO NOTHING）；随后把「规则在该任务上次评估之后被修改过」的任务提前到
// 立即评估——否则修改要等旧的 next_eval_time（长周期规则可达一小时）才生效。评估写回的 last_eval_time
// 晚于 update_time 后条件不再成立，每次修改只提前一次。清理在一个事务里分三条语句：先 DELETE 任务
// （会等在途评估事务释放任务行锁；之后任何评估器的围栏检查都看不到该行），再删判定状态、
// 关闭活跃告警——后两条语句各取新快照，能看到刚提交的那一轮评估写入的行。三者不能合并成一条
// 带数据修改 CTE 的语句：CTE 各分支共用语句开始时的快照，会漏掉等待期间提交的状态与告警。
func (r *plannerRepo) Materialize(ctx context.Context, limit int) (biz.MaterializeResult, error) {
	var res biz.MaterializeResult
	tag, err := r.data.Pool.Exec(ctx,
		`INSERT INTO monitor_eval_task (rule_id, datasource_id, next_eval_time)
		 SELECT r.id, d.id, now() + random() * least(r.eval_interval_s * interval '1 second', $1::interval)
		 FROM monitor_rule r JOIN monitor_datasource d ON `+applies+`
		 WHERE NOT EXISTS (SELECT 1 FROM monitor_eval_task t WHERE t.rule_id = r.id AND t.datasource_id = d.id)
		 ON CONFLICT DO NOTHING`, firstEvalJitter)
	if err != nil {
		return res, fmt.Errorf("insert missing eval tasks: %w", err)
	}
	res.Added = int(tag.RowsAffected())
	// 只动未到期的任务；正被评估的任务不受影响（它的写回会重设 next_eval_time，若它读到的是修改前的规则，
	// 写回的 last_eval_time 早于 update_time，下一轮物化再提前一次）。
	tag, err = r.data.Pool.Exec(ctx,
		`UPDATE monitor_eval_task t SET next_eval_time = now()
		 FROM monitor_rule r
		 WHERE r.id = t.rule_id AND r.delete_time IS NULL AND r.enabled AND t.next_eval_time > now()
		   AND r.update_time > COALESCE(t.last_eval_time, t.create_time)`)
	if err != nil {
		return res, fmt.Errorf("hasten eval tasks of updated rules: %w", err)
	}
	res.Hastened = int(tag.RowsAffected())

	err = dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx,
			`DELETE FROM monitor_eval_task t
			 WHERE (t.rule_id, t.datasource_id) IN (
			   SELECT s.rule_id, s.datasource_id FROM monitor_eval_task s
			   WHERE NOT EXISTS (
			     SELECT 1 FROM monitor_rule r JOIN monitor_datasource d ON d.id = s.datasource_id
			     WHERE r.id = s.rule_id AND `+applies+`)
			   LIMIT $1)
			 RETURNING t.rule_id, t.datasource_id`, limit)
		if err != nil {
			return fmt.Errorf("delete stale eval tasks: %w", err)
		}
		var ruleIDs, datasourceIDs []string
		for rows.Next() {
			var ruleID, datasourceID string
			if err := rows.Scan(&ruleID, &datasourceID); err != nil {
				rows.Close()
				return fmt.Errorf("scan deleted eval task: %w", err)
			}
			ruleIDs, datasourceIDs = append(ruleIDs, ruleID), append(datasourceIDs, datasourceID)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return fmt.Errorf("delete stale eval tasks: %w", err)
		}
		res.Removed = len(ruleIDs)
		if res.Removed == 0 {
			return nil
		}
		if _, err := tx.Exec(ctx,
			`DELETE FROM monitor_eval_state s USING unnest($1::uuid[], $2::uuid[]) AS k(rule_id, datasource_id)
			 WHERE s.rule_id = k.rule_id AND s.datasource_id = k.datasource_id`, ruleIDs, datasourceIDs); err != nil {
			return fmt.Errorf("delete eval states of stale tasks: %w", err)
		}
		closed, err := tx.Query(ctx,
			`UPDATE monitor_alert a SET status = $3, resolve_time = now()
			 FROM unnest($1::uuid[], $2::uuid[]) AS k(rule_id, datasource_id)
			 WHERE a.rule_id = k.rule_id AND a.datasource_id = k.datasource_id AND a.status = $4
			 RETURNING a.id::text`,
			ruleIDs, datasourceIDs, monitorcontract.AlertClosed, monitorcontract.AlertFiring)
		if err != nil {
			return fmt.Errorf("close alerts of stale tasks: %w", err)
		}
		alertIDs, err := pgx.CollectRows(closed, pgx.RowTo[string])
		if err != nil {
			return fmt.Errorf("close alerts of stale tasks: %w", err)
		}
		res.ClosedAlerts = len(alertIDs)
		return closeEvents(ctx, tx, alertIDs)
	})
	return res, err
}

// closeEvents 为一批因规则 / 数据源不再适用而关闭的告警各记一条时间线（关闭，原因 = rule）。
func closeEvents(ctx context.Context, tx pgx.Tx, alertIDs []string) error {
	if len(alertIDs) == 0 {
		return nil
	}
	ids := make([]string, len(alertIDs))
	for i := range ids {
		id, err := idgen.NewID()
		if err != nil {
			return err
		}
		ids[i] = id
	}
	detail, err := json.Marshal(monitorcontract.AlertEventDetail{Cause: monitorcontract.CloseCauseRule})
	if err != nil {
		return fmt.Errorf("encode alert event detail: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO monitor_alert_event (id, alert_id, type, detail)
		 SELECT v.id, v.alert_id, $3, $4::jsonb FROM unnest($1::uuid[], $2::uuid[]) AS v(id, alert_id)`,
		ids, alertIDs, monitorcontract.AlertEventClose, string(detail)); err != nil {
		return fmt.Errorf("record close events: %w", err)
	}
	return nil
}

// recordApplies 记录规则任务成立的条件：规则启用且未删除，其数据源启用且未删除。
const recordApplies = `r.enabled AND r.delete_time IS NULL AND d.enabled AND d.delete_time IS NULL`

// MaterializeRecords implements biz.PlannerRepo。口径同 Materialize：补齐（首次执行时刻带同样的抖动）、
// 规则在上次执行之后被修改过的提前到立即执行、删除不再成立的任务。记录规则没有判定状态与告警，
// 清理只是一条 DELETE（会等在途执行的写回释放任务行锁；之后它的围栏检查看不到该行）。
func (r *plannerRepo) MaterializeRecords(ctx context.Context, limit int) (biz.MaterializeResult, error) {
	var res biz.MaterializeResult
	tag, err := r.data.Pool.Exec(ctx,
		`INSERT INTO monitor_record_task (record_rule_id, next_eval_time)
		 SELECT r.id, now() + random() * least(r.interval_s * interval '1 second', $1::interval)
		 FROM monitor_record_rule r JOIN monitor_datasource d ON d.id = r.datasource_id AND `+recordApplies+`
		 WHERE NOT EXISTS (SELECT 1 FROM monitor_record_task t WHERE t.record_rule_id = r.id)
		 ON CONFLICT DO NOTHING`, firstEvalJitter)
	if err != nil {
		return res, fmt.Errorf("insert missing record tasks: %w", err)
	}
	res.Added = int(tag.RowsAffected())
	tag, err = r.data.Pool.Exec(ctx,
		`UPDATE monitor_record_task t SET next_eval_time = now()
		 FROM monitor_record_rule r
		 WHERE r.id = t.record_rule_id AND r.delete_time IS NULL AND r.enabled AND t.next_eval_time > now()
		   AND r.update_time > COALESCE(t.last_eval_time, t.create_time)`)
	if err != nil {
		return res, fmt.Errorf("hasten record tasks of updated rules: %w", err)
	}
	res.Hastened = int(tag.RowsAffected())
	tag, err = r.data.Pool.Exec(ctx,
		`DELETE FROM monitor_record_task t
		 WHERE t.record_rule_id IN (
		   SELECT s.record_rule_id FROM monitor_record_task s
		   WHERE NOT EXISTS (
		     SELECT 1 FROM monitor_record_rule r JOIN monitor_datasource d ON d.id = r.datasource_id
		     WHERE r.id = s.record_rule_id AND `+recordApplies+`)
		   LIMIT $1)`, limit)
	if err != nil {
		return res, fmt.Errorf("delete stale record tasks: %w", err)
	}
	res.Removed = int(tag.RowsAffected())
	return res, nil
}

// WriteHealth implements biz.PlannerRepo：一个事务批量写回，只触碰 health 四列（引擎对该表仅有这四列的 UPDATE）。
// health_since 记当前状态自何时起保持不变：状态没变沿用原值，变了取 now()。数据源行先按 ID 顺序加锁，
// 与自监控检测写 datasource_down 事件（同样锁该行）串行——从异常转为正常的数据源在同一事务里写恢复事件，
// 且只在那次故障（同一个起始时刻）的 down 事件存在时才写：不会发了故障漏了恢复，也不会没发故障却发恢复。
func (r *plannerRepo) WriteHealth(ctx context.Context, results []biz.HealthResult,
	recovered func(biz.HealthRecovery) (*biz.EngineEventInsert, error),
) (int, error) {
	if len(results) == 0 {
		return 0, nil
	}
	ids, statuses, errs := make([]string, len(results)), make([]int32, len(results)), make([]string, len(results))
	for i, h := range results {
		ids[i], errs[i], statuses[i] = h.DatasourceID, h.Error, monitorcontract.HealthOK
		if h.Error != "" {
			statuses[i] = monitorcontract.HealthFailed
		}
	}
	written := 0
	err := dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		written = 0
		rows, err := tx.Query(ctx,
			`WITH cur AS (
			   SELECT id, health_status, health_since FROM monitor_datasource
			   WHERE id = ANY ($1::uuid[]) AND delete_time IS NULL ORDER BY id FOR UPDATE),
			 upd AS (
			   UPDATE monitor_datasource d
			   SET health_status = v.status, health_error = v.error, health_time = now(),
			       health_since = CASE WHEN cur.health_status = v.status THEN COALESCE(cur.health_since, now()) ELSE now() END
			   FROM unnest($1::uuid[], $2::int4[], $3::text[]) AS v(id, status, error) JOIN cur ON cur.id = v.id
			   WHERE d.id = v.id
			   RETURNING d.id, d.name, cur.health_status AS was, cur.health_since AS since, v.status AS status)
			 SELECT id::text, name, since, now() FROM upd WHERE was = $4 AND status = $5 AND since IS NOT NULL`,
			ids, statuses, errs, monitorcontract.HealthFailed, monitorcontract.HealthOK)
		if err != nil {
			return fmt.Errorf("write datasource health: %w", err)
		}
		recoveries, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (biz.HealthRecovery, error) {
			var rec biz.HealthRecovery
			err := row.Scan(&rec.DatasourceID, &rec.Name, &rec.Since, &rec.Now)
			return rec, err
		})
		if err != nil {
			return fmt.Errorf("write datasource health: %w", err)
		}
		for _, rec := range recoveries {
			ev, err := recovered(rec)
			if err != nil {
				return err
			}
			ok, err := execEngineEvent(ctx, tx, ev)
			if err != nil {
				return err
			}
			if ok {
				written++
			}
		}
		return nil
	})
	return written, err
}

// execEngineEvent 写一条自监控事件，返回是否真的写入（去重键已存在或前置事件不存在时为假）。
func execEngineEvent(ctx context.Context, q interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}, ev *biz.EngineEventInsert,
) (bool, error) {
	sql, args, err := engineEventInsert(ev)
	if err != nil {
		return false, err
	}
	tag, err := q.Exec(ctx, sql, args...)
	if err != nil {
		return false, fmt.Errorf("write engine event: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}
