package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lanyulei/reborn/backend/cmdb/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/cmdbcontract"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	"github.com/lanyulei/reborn/backend/kit/idgen"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.TaskRepo      = (*taskRepo)(nil)
	_ biz.SchedulerRepo = (*schedulerRepo)(nil)
)

type taskRepo struct {
	data *Data
}

// NewTaskRepo new a taskRepo.
func NewTaskRepo(data *Data) *taskRepo { return &taskRepo{data: data} }

// Claim implements biz.TaskRepo：单事务内 SKIP LOCKED 选取 → 围栏更新为 running → 建运行记录 →
// 采集源 last_status=running。超过尝试上限的过期任务直接置 failed（含其运行记录）。
func (r *taskRepo) Claim(ctx context.Context, token string, lease time.Duration) (*biz.Task, error) {
	var task *biz.Task
	err := dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		// 先把尝试耗尽的过期任务收尾（幂等）：任务、运行记录与采集源 last_status 同一语句——
		// 否则源会一直显示 running。源只在仍指向该运行且为 running 时改写（不覆盖更新的结果）。
		if _, err := tx.Exec(ctx,
			`WITH dead AS (
			   UPDATE cmdb_sync_task SET status = $1, error = 'lease expired too many times', finish_time = now()
			   WHERE status = $2 AND lease_until < now() AND attempts >= $3 RETURNING run_id, source_id),
			 run AS (
			   UPDATE cmdb_sync_run r SET status = $4, error = 'lease expired too many times', finish_time = now()
			   FROM dead WHERE r.id = dead.run_id AND r.status = $5)
			 UPDATE cmdb_sync_source s SET last_status = $4
			 FROM dead WHERE s.id = dead.source_id AND s.last_run_id = dead.run_id AND s.last_status = $5`,
			cmdbcontract.TaskFailed, cmdbcontract.TaskRunning, cmdbcontract.TaskMaxAttempts, cmdbcontract.RunFailed, cmdbcontract.RunRunning); err != nil {
			return fmt.Errorf("reap dead tasks: %w", err)
		}
		var t biz.Task
		var prevRun *string
		err := tx.QueryRow(ctx,
			`SELECT id, source_id, trigger, attempts, run_id FROM cmdb_sync_task
			 WHERE status = $1 OR (status = $2 AND lease_until < now() AND attempts < $3)
			 ORDER BY create_time LIMIT 1 FOR UPDATE SKIP LOCKED`,
			cmdbcontract.TaskPending, cmdbcontract.TaskRunning, cmdbcontract.TaskMaxAttempts).
			Scan(&t.ID, &t.SourceID, &t.Trigger, &t.Attempts, &prevRun)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("select claimable task: %w", err)
		}
		// 被重领的任务：上一次运行记录置失败（其暂存行由调度器清理）。
		if prevRun != nil {
			if _, err := tx.Exec(ctx, "UPDATE cmdb_sync_run SET status = $1, error = 'lease expired', finish_time = now() WHERE id = $2 AND status = $3",
				cmdbcontract.RunFailed, *prevRun, cmdbcontract.RunRunning); err != nil {
				return fmt.Errorf("fail previous run: %w", err)
			}
		}
		runID, err := idgen.NewID()
		if err != nil {
			return err
		}
		t.Attempts++
		t.Token, t.RunID = token, runID
		if _, err := tx.Exec(ctx,
			`UPDATE cmdb_sync_task SET status = $1, attempts = $2, claimed_by = $3, lease_until = now() + $4::interval, run_id = $5,
			   start_time = COALESCE(start_time, now()) WHERE id = $6`,
			cmdbcontract.TaskRunning, t.Attempts, token, lease, runID, t.ID); err != nil {
			return fmt.Errorf("claim task: %w", err)
		}
		if _, err := tx.Exec(ctx, "INSERT INTO cmdb_sync_run (id, source_id, task_id, status, start_time) VALUES ($1, $2, $3, $4, now())",
			runID, t.SourceID, t.ID, cmdbcontract.RunRunning); err != nil {
			return fmt.Errorf("insert run: %w", err)
		}
		if _, err := tx.Exec(ctx, "UPDATE cmdb_sync_source SET last_status = $1, last_run_id = $2, last_run_time = now() WHERE id = $3",
			cmdbcontract.RunRunning, runID, t.SourceID); err != nil {
			return fmt.Errorf("mark source running: %w", err)
		}
		task = &t
		return nil
	})
	return task, err
}

// Renew implements biz.TaskRepo（属主围栏）。
func (r *taskRepo) Renew(ctx context.Context, task *biz.Task, lease time.Duration) error {
	tag, err := r.data.Pool.Exec(ctx,
		"UPDATE cmdb_sync_task SET lease_until = now() + $1::interval WHERE id = $2 AND claimed_by = $3 AND status = $4",
		lease, task.ID, task.Token, cmdbcontract.TaskRunning)
	if err != nil {
		return fmt.Errorf("renew lease: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrLeaseLost
	}
	return nil
}

// Finish implements biz.TaskRepo：任务终态 + 运行统计 + 采集源 last_* 同一事务，属主围栏。
func (r *taskRepo) Finish(ctx context.Context, task *biz.Task, status string, c biz.Counts, runErr string, samples []cmdbcontract.RunError) error {
	if samples == nil {
		samples = []cmdbcontract.RunError{}
	}
	errs, err := json.Marshal(samples)
	if err != nil {
		return fmt.Errorf("marshal run errors: %w", err)
	}
	runStatus := cmdbcontract.RunSucceeded
	if status != cmdbcontract.TaskSucceeded {
		runStatus = cmdbcontract.RunFailed
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			"UPDATE cmdb_sync_task SET status = $1, error = $2, finish_time = now() WHERE id = $3 AND claimed_by = $4 AND status = $5",
			status, runErr, task.ID, task.Token, cmdbcontract.TaskRunning)
		if err != nil {
			return fmt.Errorf("finish task: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return biz.ErrLeaseLost
		}
		if _, err := tx.Exec(ctx,
			`UPDATE cmdb_sync_run SET status = $1, discovered = $2, created = $3, updated = $4, unchanged = $5, disappeared = $6, retired = $7,
			   failed = $8, error = $9, errors = $10, finish_time = now() WHERE id = $11`,
			runStatus, c.Discovered, c.Created, c.Updated, c.Unchanged, c.Disappeared, c.Retired, c.Failed, runErr, errs, task.RunID); err != nil {
			return fmt.Errorf("finish run: %w", err)
		}
		if _, err := tx.Exec(ctx, "UPDATE cmdb_sync_source SET last_status = $1, last_run_id = $2, last_run_time = now() WHERE id = $3",
			runStatus, task.RunID, task.SourceID); err != nil {
			return fmt.Errorf("update source status: %w", err)
		}
		return nil
	})
}

type schedulerRepo struct {
	data *Data
}

// NewSchedulerRepo new a schedulerRepo.
func NewSchedulerRepo(data *Data) *schedulerRepo { return &schedulerRepo{data: data} }

// EnqueueDue implements biz.SchedulerRepo：到期源插入 schedule 任务（活动任务唯一约束下
// ON CONFLICT DO NOTHING），并把 next_run_time 推进到 now + interval（已有活动任务的源也推进，
// 避免任务结束后立刻再次入队）。
func (r *schedulerRepo) EnqueueDue(ctx context.Context, now time.Time) (int, error) {
	var n int
	err := dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx,
			`SELECT id FROM cmdb_sync_source WHERE enabled AND delete_time IS NULL AND credential IS NOT NULL AND next_run_time <= $1
			 ORDER BY next_run_time FOR UPDATE SKIP LOCKED LIMIT 100`, now)
		if err != nil {
			return fmt.Errorf("select due sources: %w", err)
		}
		ids := make([]string, 0, 16)
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, id := range ids {
			taskID, err := idgen.NewID()
			if err != nil {
				return err
			}
			tag, err := tx.Exec(ctx,
				`INSERT INTO cmdb_sync_task (id, source_id, trigger, status, create_time) VALUES ($1, $2, $3, $4, now())
				 ON CONFLICT (source_id) WHERE status IN ('pending', 'running') DO NOTHING`,
				taskID, id, cmdbcontract.TriggerSchedule, cmdbcontract.TaskPending)
			if err != nil {
				return fmt.Errorf("enqueue task: %w", err)
			}
			n += int(tag.RowsAffected())
			if _, err := tx.Exec(ctx,
				"UPDATE cmdb_sync_source SET next_run_time = $1::timestamptz + make_interval(secs => interval_seconds) WHERE id = $2", now, id); err != nil {
				return fmt.Errorf("advance next_run_time: %w", err)
			}
		}
		return nil
	})
	return n, err
}

// MarkStale implements biz.SchedulerRepo（仅同步来源、未退役、未删除的资源）。
// 停用中的采集源不参与：停用是管理员的主动决定，其资源本来就不会再被同步确认，按周期整批标陈旧
// 只会每小时发一次无从处理的 cmdb.sync.stale 告警。已删除的采集源照旧按周期判定陈旧（core 删除
// 采集源时的既定语义），与它删除前是否启用无关。
func (r *schedulerRepo) MarkStale(ctx context.Context, multiplier int32) (int, error) {
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE cmdb_ci c SET stale = true, update_time = now()
		 FROM cmdb_sync_source s
		 WHERE c.source = 'sync:' || s.id::text AND c.delete_time IS NULL AND NOT c.stale AND c.status <> $1
		   AND (s.enabled OR s.delete_time IS NOT NULL)
		   AND c.last_synced_at < now() - make_interval(secs => s.interval_seconds * $2)`,
		cmdbcontract.CIStatusRetired, multiplier)
	if err != nil {
		return 0, fmt.Errorf("mark stale: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// PurgeStaging implements biz.SchedulerRepo：运行已终态（或不存在）且开始时间早于阈值的暂存行。
// 在途与否以任务租约为准：运行所属任务仍 running 且租约未过期时一律保留，无论它开始了多久——
// 只按开始时间判定的话，staging_retention 一旦小于一次采集 + 对账的总耗时，调度器会在运行中途
// 删掉它前面的暂存批次，缺失的部分随后被对账判为「消失」并在宽限期后退役。租约过期（进程已死、
// 任务将被重领并换新的 run_id）的遗留行仍按原条件清理。
func (r *schedulerRepo) PurgeStaging(ctx context.Context, olderThan time.Duration) (int, error) {
	tag, err := r.data.Pool.Exec(ctx,
		`DELETE FROM cmdb_sync_staging st WHERE NOT EXISTS (
		   SELECT 1 FROM cmdb_sync_run r WHERE r.id = st.run_id AND r.status = $1 AND r.start_time > now() - $2::interval)
		 AND NOT EXISTS (
		   SELECT 1 FROM cmdb_sync_task t WHERE t.run_id = st.run_id AND t.status = $3 AND t.lease_until > now())`,
		cmdbcontract.RunRunning, olderThan, cmdbcontract.TaskRunning)
	if err != nil {
		return 0, fmt.Errorf("purge staging: %w", err)
	}
	return int(tag.RowsAffected()), nil
}
