package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/job"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.ScheduleRepo   = (*scheduleRepo)(nil)
	_ biz.ScheduleQueue  = (*scheduleRepo)(nil)
	_ biz.TriggerRepo    = (*triggerRepo)(nil)
	_ biz.TriggerSecrets = (*triggerRepo)(nil)
	_ biz.TriggerNames   = (*triggerRepo)(nil)
	_ biz.FireRepo       = (*fireRepo)(nil)
	_ biz.SignalQueue    = (*fireRepo)(nil)
)

// ---------------------------------------------------------------------------
// 定时作业
// ---------------------------------------------------------------------------

type scheduleRepo struct {
	data *basedata.Data
}

// NewScheduleRepo new a scheduleRepo.
func NewScheduleRepo(data *basedata.Data) *scheduleRepo { return &scheduleRepo{data: data} }

// scheduleSelect 定时作业查询：模板名称随行取出（已删除的为空）。
const scheduleSelect = `SELECT s.id::text, s.name, s.template_id::text, s.template_version, COALESCE(t.name, ''), s.cron, s.timezone, s.params, s.misfire,
	s.enabled, s.owner::text, s.next_time, s.last_time, COALESCE(s.last_execution_id::text, ''), s.last_error, s.description,
	s.version, s.create_time, s.update_time
	FROM job_schedule s LEFT JOIN job_template t ON t.id = s.template_id AND t.delete_time IS NULL`

func scanSchedule(scan func(dest ...any) error) (*biz.Schedule, error) {
	var s biz.Schedule
	var params []byte
	if err := scan(&s.ID, &s.Name, &s.TemplateID, &s.TemplateVersion, &s.TemplateName, &s.Cron, &s.Timezone, &params, &s.Misfire,
		&s.Enabled, &s.Owner, &s.NextTime, &s.LastTime, &s.LastExecutionID, &s.LastError, &s.Description,
		&s.Version, &s.CreateTime, &s.UpdateTime); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(params, &s.Params); err != nil {
		return nil, fmt.Errorf("decode job schedule params: %w", err)
	}
	return &s, nil
}

func collectSchedules(rows pgx.Rows, capacity int64) ([]*biz.Schedule, error) {
	defer rows.Close()
	items := make([]*biz.Schedule, 0, capacity)
	for rows.Next() {
		s, err := scanSchedule(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("scan job schedule: %w", err)
		}
		items = append(items, s)
	}
	return items, rows.Err()
}

// List implements biz.ScheduleRepo.
func (r *scheduleRepo) List(ctx context.Context, filter biz.ScheduleFilter, page, pageSize int64) ([]*biz.Schedule, int64, error) {
	var a sqlArgs
	where := "s.delete_time IS NULL"
	if filter.Keyword != "" {
		k := a.add(dbx.LikeContains(filter.Keyword))
		where += " AND (s.name ILIKE " + k + " OR s.description ILIKE " + k + ")"
	}
	if filter.TemplateID != "" {
		where += " AND s.template_id = " + a.add(filter.TemplateID)
	}
	if filter.Enabled != nil {
		where += " AND s.enabled = " + a.add(*filter.Enabled)
	}
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM job_schedule s WHERE "+where, a.args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count job schedules: %w", err)
	}
	limit, offset := a.add(pageSize), a.add((page-1)*pageSize)
	rows, err := r.data.Pool.Query(ctx, scheduleSelect+" WHERE "+where+" ORDER BY s.create_time DESC, s.id LIMIT "+limit+" OFFSET "+offset, a.args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list job schedules: %w", err)
	}
	items, err := collectSchedules(rows, pageSize)
	return items, total, err
}

// Get implements biz.ScheduleRepo.
func (r *scheduleRepo) Get(ctx context.Context, id string) (*biz.Schedule, error) {
	s, err := scanSchedule(r.data.Pool.QueryRow(ctx, scheduleSelect+" WHERE s.id = $1 AND s.delete_time IS NULL", id).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrScheduleNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get job schedule: %w", err)
	}
	return s, nil
}

func encodeParams(params map[string]any) ([]byte, error) {
	if params == nil {
		params = map[string]any{}
	}
	raw, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("encode params: %w", err)
	}
	return raw, nil
}

// Write implements biz.ScheduleRepo。
func (r *scheduleRepo) Write(ctx context.Context, op biz.WriteOp, s *biz.Schedule, expectVersion int32, actor string) error {
	if op == biz.OpDelete {
		return softDelete(ctx, r.data.Pool, "job_schedule", s.ID, actor, biz.ErrScheduleNotFound)
	}
	params, err := encodeParams(s.Params)
	if err != nil {
		return err
	}
	wrap := func(err error, what string) error {
		if isUniqueViolation(err, "uk_job_schedule_name") {
			return biz.ErrScheduleNameExists
		}
		return fmt.Errorf("%s job schedule: %w", what, err)
	}
	if op == biz.OpCreate {
		if _, err := r.data.Pool.Exec(ctx,
			`INSERT INTO job_schedule (id, name, template_id, cron, timezone, params, misfire, enabled, owner, next_time, description, create_user, update_user, template_version)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $9, $9, $12)`,
			s.ID, s.Name, s.TemplateID, s.Cron, s.Timezone, params, s.Misfire, s.Enabled, actor, s.NextTime, s.Description, s.TemplateVersion); err != nil {
			return wrap(err, "insert")
		}
		return nil
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if err := lockVersion(ctx, tx, "job_schedule", s.ID, expectVersion, biz.ErrScheduleNotFound); err != nil {
			return err
		}
		// 编辑后负责人改为操作人；上次触发的错误随之清掉（配置已变）。
		if _, err := tx.Exec(ctx,
			`UPDATE job_schedule SET name = $2, template_id = $3, cron = $4, timezone = $5, params = $6, misfire = $7, enabled = $8,
			   owner = $9, next_time = $10, description = $11, last_error = '', template_version = $12,
			   version = version + 1, update_time = now(), update_user = $9 WHERE id = $1`,
			s.ID, s.Name, s.TemplateID, s.Cron, s.Timezone, params, s.Misfire, s.Enabled, actor, s.NextTime, s.Description, s.TemplateVersion); err != nil {
			return wrap(err, "update")
		}
		return nil
	})
}

// Due implements biz.ScheduleQueue。
func (r *scheduleRepo) Due(ctx context.Context, limit int) ([]*biz.Schedule, time.Time, error) {
	var now time.Time
	if err := r.data.Pool.QueryRow(ctx, "SELECT now()").Scan(&now); err != nil {
		return nil, now, fmt.Errorf("read database clock: %w", err)
	}
	rows, err := r.data.Pool.Query(ctx, scheduleSelect+
		" WHERE s.enabled AND s.delete_time IS NULL AND s.next_time <= $1 ORDER BY s.next_time, s.id LIMIT $2", now, limit)
	if err != nil {
		return nil, now, fmt.Errorf("list due job schedules: %w", err)
	}
	items, err := collectSchedules(rows, int64(limit))
	return items, now, err
}

// Advance implements biz.ScheduleQueue：条件更新（next_time 仍是本次的计划时间）保证多实例下只推进一次。
func (r *scheduleRepo) Advance(ctx context.Context, id string, run biz.ScheduleRun) (bool, error) {
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE job_schedule SET next_time = $3, last_time = $2, last_error = $5,
		   last_execution_id = COALESCE($4::uuid, last_execution_id)
		 WHERE id = $1 AND next_time = $2 AND enabled AND delete_time IS NULL`,
		id, run.Fired, run.Next, dbx.NilIfEmpty(run.ExecutionID), run.Error)
	if err != nil {
		return false, fmt.Errorf("advance job schedule: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// ---------------------------------------------------------------------------
// 触发器
// ---------------------------------------------------------------------------

type triggerRepo struct {
	data *basedata.Data
}

// NewTriggerRepo new a triggerRepo.
func NewTriggerRepo(data *basedata.Data) *triggerRepo { return &triggerRepo{data: data} }

// triggerSelect 触发器查询；令牌摘要与密文不读出。
const triggerSelect = `SELECT g.id::text, g.name, g.type, g.template_id::text, g.template_version, COALESCE(t.name, ''), g.params, g.pass_params, g.signal,
	g.alert, g.cooldown_s, g.enabled, g.owner::text, g.last_fire_time, g.description, g.version, g.create_time, g.update_time
	FROM job_trigger g LEFT JOIN job_template t ON t.id = g.template_id AND t.delete_time IS NULL`

func scanTrigger(scan func(dest ...any) error) (*biz.Trigger, error) {
	var t biz.Trigger
	var params, alert []byte
	if err := scan(&t.ID, &t.Name, &t.Type, &t.TemplateID, &t.TemplateVersion, &t.TemplateName, &params, &t.PassParams, &t.Signal,
		&alert, &t.CooldownS, &t.Enabled, &t.Owner, &t.LastFireTime, &t.Description, &t.Version, &t.CreateTime, &t.UpdateTime); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(params, &t.Params); err != nil {
		return nil, fmt.Errorf("decode job trigger params: %w", err)
	}
	if err := json.Unmarshal(alert, &t.Alert); err != nil {
		return nil, fmt.Errorf("decode job trigger alert match: %w", err)
	}
	return &t, nil
}

func collectTriggers(rows pgx.Rows, capacity int64) ([]*biz.Trigger, error) {
	defer rows.Close()
	items := make([]*biz.Trigger, 0, capacity)
	for rows.Next() {
		t, err := scanTrigger(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("scan job trigger: %w", err)
		}
		items = append(items, t)
	}
	return items, rows.Err()
}

// List implements biz.TriggerRepo.
func (r *triggerRepo) List(ctx context.Context, filter biz.TriggerFilter, page, pageSize int64) ([]*biz.Trigger, int64, error) {
	var a sqlArgs
	where := "g.delete_time IS NULL"
	if filter.Keyword != "" {
		k := a.add(dbx.LikeContains(filter.Keyword))
		where += " AND (g.name ILIKE " + k + " OR g.description ILIKE " + k + ")"
	}
	if filter.Type != nil {
		where += " AND g.type = " + a.add(*filter.Type)
	}
	if filter.TemplateID != "" {
		where += " AND g.template_id = " + a.add(filter.TemplateID)
	}
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM job_trigger g WHERE "+where, a.args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count job triggers: %w", err)
	}
	limit, offset := a.add(pageSize), a.add((page-1)*pageSize)
	rows, err := r.data.Pool.Query(ctx, triggerSelect+" WHERE "+where+" ORDER BY g.create_time DESC, g.id LIMIT "+limit+" OFFSET "+offset, a.args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list job triggers: %w", err)
	}
	items, err := collectTriggers(rows, pageSize)
	return items, total, err
}

// Get implements biz.TriggerRepo.
func (r *triggerRepo) Get(ctx context.Context, id string) (*biz.Trigger, error) {
	t, err := scanTrigger(r.data.Pool.QueryRow(ctx, triggerSelect+" WHERE g.id = $1 AND g.delete_time IS NULL", id).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrTriggerNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get job trigger: %w", err)
	}
	return t, nil
}

// Write implements biz.TriggerRepo。
func (r *triggerRepo) Write(ctx context.Context, op biz.WriteOp, t *biz.Trigger, expectVersion int32, actor string, token *biz.TriggerToken) error {
	if op == biz.OpDelete {
		// 软删同时抹掉令牌：已删除的触发器不该再留着可用的凭证。
		tag, err := r.data.Pool.Exec(ctx,
			`UPDATE job_trigger SET delete_time = now(), delete_user = $2, token_hash = NULL, token_sealed = NULL, version = version + 1
			 WHERE id = $1 AND delete_time IS NULL`, t.ID, dbx.NilIfEmpty(actor))
		if err != nil {
			return fmt.Errorf("delete job trigger: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return biz.ErrTriggerNotFound
		}
		return nil
	}
	params, err := encodeParams(t.Params)
	if err != nil {
		return err
	}
	alert, err := json.Marshal(t.Alert)
	if err != nil {
		return fmt.Errorf("encode job trigger alert match: %w", err)
	}
	wrap := func(err error, what string) error {
		if isUniqueViolation(err, "uk_job_trigger_name") {
			return biz.ErrTriggerNameExists
		}
		return fmt.Errorf("%s job trigger: %w", what, err)
	}
	if op == biz.OpCreate {
		var hash, sealed any
		if token != nil {
			hash, sealed = token.Hash, dbx.NilIfEmptyBytes(token.Sealed)
		}
		if _, err := r.data.Pool.Exec(ctx,
			`INSERT INTO job_trigger (id, name, type, template_id, params, pass_params, token_hash, token_sealed, signal, alert,
			   cooldown_s, enabled, owner, description, create_user, update_user, template_version)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $13, $13, $15)`,
			t.ID, t.Name, t.Type, t.TemplateID, params, t.PassParams, hash, sealed, t.Signal, alert,
			t.CooldownS, t.Enabled, actor, t.Description, t.TemplateVersion); err != nil {
			return wrap(err, "insert")
		}
		return nil
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if err := lockVersion(ctx, tx, "job_trigger", t.ID, expectVersion, biz.ErrTriggerNotFound); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE job_trigger SET name = $2, template_id = $3, params = $4, pass_params = $5, signal = $6, alert = $7,
			   cooldown_s = $8, enabled = $9, owner = $10, description = $11, template_version = $12,
			   version = version + 1, update_time = now(), update_user = $10 WHERE id = $1`,
			t.ID, t.Name, t.TemplateID, params, t.PassParams, t.Signal, alert, t.CooldownS, t.Enabled, actor, t.Description, t.TemplateVersion); err != nil {
			return wrap(err, "update")
		}
		return nil
	})
}

// SetToken implements biz.TriggerSecrets.
func (r *triggerRepo) SetToken(ctx context.Context, id string, token biz.TriggerToken, actor string) error {
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE job_trigger SET token_hash = $2, token_sealed = $3, owner = COALESCE($4::uuid, owner),
		   version = version + 1, update_time = now(), update_user = $4
		 WHERE id = $1 AND type = $5 AND delete_time IS NULL`,
		id, token.Hash, dbx.NilIfEmptyBytes(token.Sealed), dbx.NilIfEmpty(actor), jc.TriggerTypeAPI)
	if err != nil {
		return fmt.Errorf("reset job trigger token: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrTriggerNotFound
	}
	return nil
}

// Token implements biz.TriggerSecrets.
func (r *triggerRepo) Token(ctx context.Context, id string) (biz.TriggerToken, error) {
	var t biz.TriggerToken
	err := r.data.Pool.QueryRow(ctx,
		"SELECT token_hash, token_sealed FROM job_trigger WHERE id = $1 AND type = $2 AND delete_time IS NULL AND token_hash IS NOT NULL",
		id, jc.TriggerTypeAPI).Scan(&t.Hash, &t.Sealed)
	if errors.Is(err, pgx.ErrNoRows) {
		return t, biz.ErrTriggerNotFound
	}
	if err != nil {
		return t, fmt.Errorf("get job trigger token: %w", err)
	}
	return t, nil
}

// EnabledAlertTriggers implements biz.TriggerSecrets.
func (r *triggerRepo) EnabledAlertTriggers(ctx context.Context) ([]*biz.Trigger, error) {
	rows, err := r.data.Pool.Query(ctx, triggerSelect+
		" WHERE g.type = $1 AND g.enabled AND g.delete_time IS NULL ORDER BY g.id LIMIT $2", jc.TriggerTypeAlert, maxOptions)
	if err != nil {
		return nil, fmt.Errorf("list alert job triggers: %w", err)
	}
	return collectTriggers(rows, 8)
}

// SourceNames implements biz.TriggerNames（含已删除的对象：历史执行仍要显示来源）。
func (r *triggerRepo) SourceNames(ctx context.Context, scheduleIDs, triggerIDs []string) (map[string]string, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT id::text, name FROM job_schedule WHERE id = ANY($1::uuid[])
		 UNION ALL SELECT id::text, name FROM job_trigger WHERE id = ANY($2::uuid[])`,
		dbx.TextArray(scheduleIDs), dbx.TextArray(triggerIDs))
	if err != nil {
		return nil, fmt.Errorf("list job trigger source names: %w", err)
	}
	return collectNames(rows, "job trigger source name")
}

// ---------------------------------------------------------------------------
// 触发记录与流程信号
// ---------------------------------------------------------------------------

type fireRepo struct {
	data *basedata.Data
}

// NewFireRepo new a fireRepo.
func NewFireRepo(data *basedata.Data) *fireRepo { return &fireRepo{data: data} }

const fireColumns = `f.id::text, f.trigger_id::text, f.ref, f.result, f.reason, COALESCE(f.execution_id::text, ''), f.summary,
	COALESCE(f.workflow_instance_id::text, ''), f.signal, f.signal_state, f.create_time`

func scanFire(scan func(dest ...any) error, extra ...any) (*biz.TriggerFire, error) {
	var f biz.TriggerFire
	dest := append([]any{
		&f.ID, &f.TriggerID, &f.Ref, &f.Result, &f.Reason, &f.ExecutionID, &f.Summary,
		&f.WorkflowInstanceID, &f.Signal, &f.SignalState, &f.CreateTime,
	}, extra...)
	if err := scan(dest...); err != nil {
		return nil, err
	}
	return &f, nil
}

// List implements biz.FireRepo.
func (r *fireRepo) List(ctx context.Context, filter biz.FireFilter, page, pageSize int64) ([]*biz.TriggerFire, int64, error) {
	var a sqlArgs
	where := "true"
	if filter.TriggerID != "" {
		where += " AND f.trigger_id = " + a.add(filter.TriggerID)
	}
	if filter.Result != nil {
		where += " AND f.result = " + a.add(*filter.Result)
	}
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM job_trigger_fire f WHERE "+where, a.args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count job trigger fires: %w", err)
	}
	limit, offset := a.add(pageSize), a.add((page-1)*pageSize)
	rows, err := r.data.Pool.Query(ctx, "SELECT "+fireColumns+`, COALESCE(g.name, '')
		 FROM job_trigger_fire f LEFT JOIN job_trigger g ON g.id = f.trigger_id AND g.delete_time IS NULL
		 WHERE `+where+" ORDER BY f.create_time DESC, f.id DESC LIMIT "+limit+" OFFSET "+offset, a.args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list job trigger fires: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.TriggerFire, 0, pageSize)
	for rows.Next() {
		var name string
		f, err := scanFire(rows.Scan, &name)
		if err != nil {
			return nil, 0, fmt.Errorf("scan job trigger fire: %w", err)
		}
		f.TriggerName = name
		items = append(items, f)
	}
	return items, total, rows.Err()
}

// Begin implements biz.FireRepo。先行锁触发器行：同一触发器的并发触发在此串行化，冷却判定与
// 去重插入因此不会交错（两个实例同时处理两条不同的告警时，冷却期内只有一条能触发）。
func (r *fireRepo) Begin(ctx context.Context, f *biz.TriggerFire, cooldownS int32) (*biz.TriggerFire, bool, error) {
	var existing *biz.TriggerFire
	fresh := false
	err := dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		existing, fresh = nil, false
		var last *time.Time
		var now time.Time
		err := tx.QueryRow(ctx, "SELECT last_fire_time, now() FROM job_trigger WHERE id = $1 AND delete_time IS NULL FOR UPDATE", f.TriggerID).Scan(&last, &now)
		if errors.Is(err, pgx.ErrNoRows) {
			return biz.ErrTriggerNotFound
		}
		if err != nil {
			return fmt.Errorf("lock job trigger: %w", err)
		}
		got, err := scanFire(tx.QueryRow(ctx, "SELECT "+fireColumns+" FROM job_trigger_fire f WHERE f.trigger_id = $1 AND f.ref = $2", f.TriggerID, f.Ref).Scan)
		if err == nil {
			existing = got
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("find job trigger fire: %w", err)
		}
		fresh = true
		// 调用方已定好结果（如告警里的主机不在目标之内 → 跳过）的原样落库；否则先判冷却。
		if f.Result == 0 {
			f.Result = jc.FireSubmitted
			if cooldownS > 0 && last != nil && now.Sub(*last) < time.Duration(cooldownS)*time.Second {
				f.Result, f.SignalState = jc.FireSkipped, jc.SignalNone
				f.Reason = fmt.Sprintf("冷却中：距上次触发不足 %d 秒", cooldownS)
			}
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO job_trigger_fire (id, trigger_id, ref, result, reason, summary, workflow_instance_id, signal, signal_state)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			f.ID, f.TriggerID, f.Ref, f.Result, f.Reason, f.Summary, dbx.NilIfEmpty(f.WorkflowInstanceID), f.Signal, f.SignalState); err != nil {
			return fmt.Errorf("insert job trigger fire: %w", err)
		}
		// 占住冷却窗口：提交执行在事务之外，后来的并发触发要在这里就看到「刚触发过」。
		if f.Result == jc.FireSubmitted {
			if _, err := tx.Exec(ctx, "UPDATE job_trigger SET last_fire_time = now() WHERE id = $1", f.TriggerID); err != nil {
				return fmt.Errorf("touch job trigger: %w", err)
			}
		}
		f.CreateTime = now
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return existing, fresh, nil
}

// Complete implements biz.FireRepo。
func (r *fireRepo) Complete(ctx context.Context, f *biz.TriggerFire) error {
	if _, err := r.data.Pool.Exec(ctx,
		`UPDATE job_trigger_fire SET result = $2, reason = $3, execution_id = $4, signal_state = $5::smallint,
		   signal_next_time = CASE WHEN $5::smallint = 1 THEN now() END WHERE id = $1`,
		f.ID, f.Result, f.Reason, dbx.NilIfEmpty(f.ExecutionID), f.SignalState); err != nil {
		return fmt.Errorf("complete job trigger fire: %w", err)
	}
	return nil
}

// Due implements biz.SignalQueue：作业的全部切片都已结束（没有待审批 / 排队中 / 执行中的）才发信号；
// 状态取各片里最差的一个（数值最大的非成功状态优先，全部成功才是成功）。
func (r *fireRepo) Due(ctx context.Context, limit int) ([]*biz.PendingSignal, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT f.id::text, f.workflow_instance_id::text, f.signal, f.execution_id::text, f.signal_attempts, s.worst, s.all_ok
		 FROM job_trigger_fire f
		 JOIN job_execution e ON e.id = f.execution_id
		 CROSS JOIN LATERAL (
		   SELECT bool_and(x.status NOT IN (1, 2, 9) AND (x.status <> 8 OR x.finish_time < now() - interval '5 minutes')) AS finished, bool_and(x.status = 3) AS all_ok,
		          COALESCE(max(x.status) FILTER (WHERE x.status <> 3), 3) AS worst
		   FROM job_execution x WHERE x.id = e.id OR (e.group_id IS NOT NULL AND x.group_id = e.group_id)
		 ) s
		 WHERE f.signal_state = 1 AND f.signal_next_time <= now() AND s.finished
		 ORDER BY f.signal_next_time LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("list due job signals: %w", err)
	}
	defer rows.Close()
	var items []*biz.PendingSignal
	for rows.Next() {
		var s biz.PendingSignal
		if err := rows.Scan(&s.FireID, &s.InstanceID, &s.Signal, &s.ExecutionID, &s.Attempts, &s.Status, &s.Success); err != nil {
			return nil, fmt.Errorf("scan due job signal: %w", err)
		}
		items = append(items, &s)
	}
	return items, rows.Err()
}

// Mark implements biz.SignalQueue。
func (r *fireRepo) Mark(ctx context.Context, fireID string, state int32, retryIn time.Duration) error {
	if _, err := r.data.Pool.Exec(ctx,
		`UPDATE job_trigger_fire SET signal_state = $2::smallint, signal_attempts = signal_attempts + 1,
		   signal_next_time = CASE WHEN $2::smallint = 1 THEN now() + make_interval(secs => $3) END
		 WHERE id = $1 AND signal_state = 1`, fireID, state, retryIn.Seconds()); err != nil {
		return fmt.Errorf("mark job signal: %w", err)
	}
	return nil
}
