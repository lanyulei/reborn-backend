package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

// 编译期断言锁住 biz 契约。
var (
	_ biz.JobQueue       = (*jobRepo)(nil)
	_ biz.JobTx          = (*jobRepo)(nil)
	_ biz.JobStats       = (*jobRepo)(nil)
	_ biz.InstanceLookup = (*instanceRepo)(nil)
)

const jobColumns = "id, instance_id, kind, node_id, token_id, task_id, payload, status, due_time, attempts, last_error, lock_until"

// scanJob 扫描作业行。
func scanJob(row pgx.CollectableRow) (*biz.Job, error) {
	var (
		j           biz.Job
		token, task *string
		payload     []byte
		status      int16
		lockUntil   *time.Time
	)
	if err := row.Scan(&j.ID, &j.InstanceID, &j.Kind, &j.NodeID, &token, &task, &payload, &status, &j.DueTime, &j.Attempts, &j.LastError, &lockUntil); err != nil {
		return nil, err
	}
	j.Status = int32(status)
	if token != nil {
		j.TokenID = *token
	}
	if task != nil {
		j.TaskID = *task
	}
	if lockUntil != nil {
		j.LockUntil = *lockUntil
	}
	return &j, json.Unmarshal(payload, &j.Payload)
}

// jobRepo 流程作业队列的 PostgreSQL 实现。
type jobRepo struct {
	data *basedata.Data
}

// NewJobRepo new a jobRepo.
func NewJobRepo(data *basedata.Data) *jobRepo {
	return &jobRepo{data: data}
}

// ClaimDue implements biz.JobQueue：单语句领取——SKIP LOCKED 让并发的执行器各取各的，
// 续上租约后其他执行器在租约期内不会再取到同一条；执行器崩溃则租约过期后可被再次领取。
// 领取即计一次尝试。service 决定领哪一类：自动任务（kind = 'service'）或其余的引擎内部作业。
func (r *jobRepo) ClaimDue(ctx context.Context, service bool, limit int, lease time.Duration) ([]*biz.Job, error) {
	rows, err := r.data.Pool.Query(ctx,
		`UPDATE workflow_job SET lock_until = now() + make_interval(secs => $2), attempts = attempts + 1, update_time = now()
		 WHERE id IN (
			SELECT id FROM workflow_job
			WHERE status = 1 AND due_time <= now() AND (lock_until IS NULL OR lock_until < now())
					AND (kind = 'service') = $3
			ORDER BY due_time LIMIT $1 FOR UPDATE SKIP LOCKED)
		 RETURNING `+jobColumns, limit, lease.Seconds(), service)
	if err != nil {
		return nil, fmt.Errorf("claim workflow jobs: %w", err)
	}
	jobs, err := pgx.CollectRows(rows, scanJob)
	if err != nil {
		return nil, fmt.Errorf("scan workflow job: %w", err)
	}
	return jobs, nil
}

// Renew implements biz.JobQueue：以「租约仍是领取时的那一个」为条件续租，并把新的租约记回 j。
func (r *jobRepo) Renew(ctx context.Context, j *biz.Job, lease time.Duration) (bool, error) {
	var until time.Time
	err := r.data.Pool.QueryRow(ctx,
		`UPDATE workflow_job SET lock_until = now() + make_interval(secs => $3), update_time = now()
		 WHERE id = $1 AND status = 1 AND lock_until = $2 RETURNING lock_until`, j.ID, j.LockUntil, lease.Seconds()).Scan(&until)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("renew workflow job lease: %w", err)
	}
	j.LockUntil = until
	return true, nil
}

// Retry implements biz.JobQueue：只对仍待执行、且租约仍是自己的作业生效；下次执行时间以
// 数据库时钟为准。
func (r *jobRepo) Retry(ctx context.Context, j *biz.Job, delay time.Duration, lastError string) error {
	if _, err := r.data.Pool.Exec(ctx,
		`UPDATE workflow_job SET last_error = $3, due_time = now() + make_interval(secs => $2), lock_until = NULL, update_time = now()
		 WHERE id = $1 AND status = 1 AND lock_until = $4`, j.ID, delay.Seconds(), lastError, j.LockUntil); err != nil {
		return fmt.Errorf("retry workflow job: %w", err)
	}
	return nil
}

// Release implements biz.JobQueue.
func (r *jobRepo) Release(ctx context.Context, j *biz.Job) error {
	if _, err := r.data.Pool.Exec(ctx,
		`UPDATE workflow_job SET attempts = greatest(attempts - 1, 0), lock_until = NULL, update_time = now()
		 WHERE id = $1 AND status = 1 AND lock_until = $2`, j.ID, j.LockUntil); err != nil {
		return fmt.Errorf("release workflow job: %w", err)
	}
	return nil
}

// Purge implements biz.JobQueue：分批删除，单次持锁时间有界。
func (r *jobRepo) Purge(ctx context.Context, before time.Time, limit int) (int, error) {
	tag, err := r.data.Pool.Exec(ctx,
		`DELETE FROM workflow_job WHERE id IN (
			SELECT id FROM workflow_job WHERE status IN (2, 3) AND update_time < $1 LIMIT $2 FOR UPDATE SKIP LOCKED)`, before, limit)
	if err != nil {
		return 0, fmt.Errorf("purge workflow jobs: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// LockTx implements biz.JobTx.
func (r *jobRepo) LockTx(ctx context.Context, tx txrun.Tx, id string) (*biz.Job, error) {
	ptx, ok := basedata.PgxTx(tx)
	if !ok {
		return nil, errForeignTx
	}
	rows, err := ptx.Query(ctx, "SELECT "+jobColumns+" FROM workflow_job WHERE id = $1 FOR UPDATE", id)
	if err != nil {
		return nil, fmt.Errorf("lock workflow job: %w", err)
	}
	j, err := pgx.CollectExactlyOneRow(rows, scanJob)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("scan workflow job: %w", err)
	}
	return j, nil
}

// SettleTx implements biz.JobTx.
func (r *jobRepo) SettleTx(ctx context.Context, tx txrun.Tx, id string, status int32, lastError string) error {
	if _, err := tx.Exec(ctx,
		"UPDATE workflow_job SET status = $2, last_error = $3, lock_until = NULL, update_time = now() WHERE id = $1",
		id, status, lastError); err != nil {
		return fmt.Errorf("settle workflow job: %w", err)
	}
	return nil
}

// ReviveTx implements biz.JobTx.
func (r *jobRepo) ReviveTx(ctx context.Context, tx txrun.Tx, id string) error {
	if _, err := tx.Exec(ctx,
		`UPDATE workflow_job SET status = 1, attempts = 0, due_time = now(), lock_until = NULL, update_time = now()
		 WHERE id = $1 AND status = 4`, id); err != nil {
		return fmt.Errorf("revive workflow job: %w", err)
	}
	return nil
}

// GetInstance implements biz.InstanceLookup：事务内调用时复用事务连接。
func (r *instanceRepo) GetInstance(ctx context.Context, instanceID string) (*biz.Instance, error) {
	var (
		in   biz.Instance
		vars []byte
	)
	err := scanInstance(basedata.QueryerFrom(ctx, r.data.Pool).QueryRow(ctx,
		"SELECT "+instanceColumns+", variables FROM workflow_instance WHERE id = $1", instanceID).Scan, &in, &vars)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrInstanceNotFound
		}
		return nil, fmt.Errorf("get workflow instance: %w", err)
	}
	if err := json.Unmarshal(vars, &in.Variables); err != nil {
		return nil, fmt.Errorf("decode workflow variables: %w", err)
	}
	return &in, nil
}

// Children implements biz.InstanceLookup.
func (r *instanceRepo) Children(ctx context.Context, instanceID string) ([]*biz.ChildRef, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT id, parent_node_id, definition_name, status FROM workflow_instance
		 WHERE parent_instance_id = $1 ORDER BY start_time, id`, instanceID)
	if err != nil {
		return nil, fmt.Errorf("list child instances: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*biz.ChildRef, error) {
		var (
			c      biz.ChildRef
			status int16
		)
		err := row.Scan(&c.ID, &c.NodeID, &c.DefinitionName, &status)
		c.Status = int32(status)
		return &c, err
	})
	if err != nil {
		return nil, fmt.Errorf("scan child instance: %w", err)
	}
	return out, nil
}

// InstancesWithSeats implements biz.InstanceLookup：待处理 / 排队 / 等待加签的席位都算未了结。
func (r *instanceRepo) InstancesWithSeats(ctx context.Context, userID, after string, limit int) ([]string, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT DISTINCT a.instance_id FROM workflow_task_assignee a
		 JOIN workflow_instance i ON i.id = a.instance_id AND i.status = 1
		 WHERE a.user_id = $1 AND a.status IN (1, 2, 9) AND ($3::uuid IS NULL OR a.instance_id > $3::uuid)
		 ORDER BY a.instance_id LIMIT $2`, userID, limit, dbx.NilIfEmpty(after))
	if err != nil {
		return nil, fmt.Errorf("list instances with seats: %w", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("scan instance id: %w", err)
	}
	return ids, nil
}

// Snapshot implements biz.InstanceLookup：事务内调用时复用事务连接。
func (r *instanceRepo) Snapshot(ctx context.Context, instanceID string) (*biz.State, error) {
	return load(ctx, basedata.QueryerFrom(ctx, r.data.Pool), instanceID, false, false)
}

// PendingUsers implements biz.InstanceLookup：该实例及以它为根的各级子流程里，进行中的实例上
// 待处理的用户。
func (r *instanceRepo) PendingUsers(ctx context.Context, instanceID string) ([]string, error) {
	rows, err := basedata.QueryerFrom(ctx, r.data.Pool).Query(ctx,
		`SELECT DISTINCT a.user_id::text FROM workflow_instance i
		 JOIN workflow_task_assignee a ON a.instance_id = i.id
		 WHERE (i.id = $1 OR i.root_id = $1) AND i.status = 1 AND a.status = 1`, instanceID)
	if err != nil {
		return nil, fmt.Errorf("list pending users: %w", err)
	}
	users, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("scan pending user: %w", err)
	}
	return users, nil
}

// SeatsInTree implements biz.InstanceLookup：先由 idx_workflow_instance_root 取出同一棵流程树
// 上的实例，再按 (instance_id, user_id) 探查席位——开销与树的大小成正比，与该用户的席位
// 总数无关。事务内调用时复用事务连接。
func (r *instanceRepo) SeatsInTree(ctx context.Context, instanceID, userID string) ([]biz.SeatRef, error) {
	rows, err := basedata.QueryerFrom(ctx, r.data.Pool).Query(ctx,
		`SELECT a.instance_id::text,
			bool_or(a.user_id = $2 AND a.status = 1 AND t.status = 1 AND i.status = 1)
		 FROM workflow_instance i
		 JOIN workflow_task_assignee a ON a.instance_id = i.id
		 JOIN workflow_task t ON t.id = a.task_id
		 WHERE i.root_id = (SELECT root_id FROM workflow_instance WHERE id = $1)
		   AND (a.user_id = $2 OR a.on_behalf = $2) AND a.status <> $3
		 GROUP BY a.instance_id
		 ORDER BY (a.instance_id = $1) DESC, a.instance_id`, instanceID, userID, biz.AssigneeCancelled)
	if err != nil {
		return nil, fmt.Errorf("list seats in workflow tree: %w", err)
	}
	refs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (biz.SeatRef, error) {
		var ref biz.SeatRef
		err := row.Scan(&ref.InstanceID, &ref.Pending)
		return ref, err
	})
	if err != nil {
		return nil, fmt.Errorf("scan workflow seat: %w", err)
	}
	return refs, nil
}

// Backlog implements biz.JobStats：两个聚合都走部分索引（待执行按到期时间、死信单独一个），
// 开销与积压 / 死信的条数成正比，与作业表总量无关。
func (r *jobRepo) Backlog(ctx context.Context) (biz.JobBacklog, error) {
	var b biz.JobBacklog
	err := r.data.Pool.QueryRow(ctx,
		`SELECT
			(SELECT count(*) FROM workflow_job WHERE status = 1 AND due_time < now() - interval '1 minute'),
			COALESCE((SELECT extract(epoch FROM now() - min(due_time)) FROM workflow_job
				WHERE status = 1 AND due_time < now() - interval '1 minute'), 0)::float8,
			(SELECT count(*) FROM workflow_job WHERE status = 4)`).Scan(&b.Overdue, &b.OldestSeconds, &b.Dead)
	if err != nil {
		return b, fmt.Errorf("workflow job backlog: %w", err)
	}
	return b, nil
}
