package data

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lanyulei/reborn/backend/job/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	_ biz.QueueRepo     = (*executionRepo)(nil)
	_ biz.MaterialRepo  = (*executionRepo)(nil)
	_ biz.PayloadRepo   = (*executionRepo)(nil)
	_ biz.ProgressRepo  = (*executionRepo)(nil)
	_ biz.HousekeepRepo = (*executionRepo)(nil)
)

// executionRepo 执行记录、执行主机、执行日志的读写，以及派发要用的凭据 / 项目只读访问。
// 状态字面量与 kit/jobcontract 一致：1 排队中 2 执行中 4 失败 6 已取消 8 结果未知 9 待审批（引擎不领取、不清扫）。
type executionRepo struct {
	data *Data
}

// NewExecutionRepo new an executionRepo.
func NewExecutionRepo(data *Data) *executionRepo { return &executionRepo{data: data} }

// uniqueViolation PostgreSQL 的唯一约束冲突错误码（SQLSTATE 23505）。
const uniqueViolation = "23505"

// seconds 把时长转成 SQL 里与 interval '1 second' 相乘的实参。
func seconds(d time.Duration) float64 { return d.Seconds() }

// ClaimNext implements biz.QueueRepo：单语句领取——CTE 以 FOR UPDATE SKIP LOCKED 选出最早一条可执行的
// 排队中的执行，外层把它置为执行中。并发的领取互不阻塞、也不会领到同一条。
//
// 选行必须放在 MATERIALIZED 的 CTE 里，不能写成 `WHERE id IN (SELECT … SKIP LOCKED LIMIT 1)`：
// 后者可能被规划成嵌套循环半连接，子查询对外层每一行重新执行一次，而每次重新执行都会跳过本语句
// 刚更新过的行、锁住下一行——LIMIT 1 形同虚设，一次领取会把多条执行置为执行中。
func (r *executionRepo) ClaimNext(ctx context.Context, runnerID, zone, leaseToken string, lease time.Duration) (*biz.Claimed, error) {
	out := &biz.Claimed{}
	err := r.data.Pool.QueryRow(ctx, `
		WITH next AS MATERIALIZED (
			SELECT id FROM job_execution
			WHERE status = 1 AND NOT cancel_requested AND (runner_zone = '' OR runner_zone = $4)
			ORDER BY create_time, id
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE job_execution e
		SET status = 2, runner_id = $1, lease_token = $2,
		    lease_until = now() + $3 * interval '1 second', start_time = now()
		FROM next
		WHERE e.id = next.id
		RETURNING e.id::text, e.spec`,
		runnerID, leaseToken, seconds(lease), zone,
	).Scan(&out.ID, &out.Spec)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim execution: %w", err)
	}
	return out, nil
}

// Fail implements biz.QueueRepo.
func (r *executionRepo) Fail(ctx context.Context, executionID, leaseToken, reason string) error {
	_, err := r.data.Pool.Exec(ctx, `
		UPDATE job_execution SET status = 4, finish_time = now(), error = $3
		WHERE id = $1 AND status = 2 AND lease_token = $2`,
		executionID, leaseToken, reason)
	if err != nil {
		return fmt.Errorf("fail execution: %w", err)
	}
	return nil
}

// Release implements biz.QueueRepo：恢复为领取前的样子（排队中、无节点、无租约、无开始时间）。
//
// 领取之后已被请求取消的执行不回队列：ClaimNext 不会再领取带 cancel_requested 的执行，租约清扫又只看
// 执行中的行——放回去它就永远停在「排队中」（模板禁止并发时还一直占着互斥位）。直接落为已取消
// （节点从未拿到任务，不可能已经启动它）；notify_time 保持为空，结束通知交给 core 按既有逻辑处理。
// 已取消是终态，finish_time 必须有值（ck_job_execution_finish）。
func (r *executionRepo) Release(ctx context.Context, executionID, leaseToken string) error {
	_, err := r.data.Pool.Exec(ctx, `
		UPDATE job_execution
		SET status      = CASE WHEN cancel_requested THEN 6 ELSE 1 END,
		    finish_time = CASE WHEN cancel_requested THEN now() ELSE NULL END,
		    runner_id = NULL, lease_token = '', lease_until = NULL, start_time = NULL
		WHERE id = $1 AND status = 2 AND lease_token = $2`,
		executionID, leaseToken)
	if err != nil {
		return fmt.Errorf("release execution: %w", err)
	}
	return nil
}

// Hosts implements biz.MaterialRepo.
func (r *executionRepo) Hosts(ctx context.Context, executionID string) ([]biz.Host, error) {
	rows, err := r.data.Pool.Query(ctx, `
		SELECT address, port, ci_id::text, name, env FROM job_execution_host
		WHERE execution_id = $1 ORDER BY seq`, executionID)
	if err != nil {
		return nil, fmt.Errorf("query execution hosts: %w", err)
	}
	hosts, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (biz.Host, error) {
		var h biz.Host
		err := row.Scan(&h.Address, &h.Port, &h.CIID, &h.Name, &h.Env)
		return h, err
	})
	if err != nil {
		return nil, fmt.Errorf("scan execution hosts: %w", err)
	}
	return hosts, nil
}

// Credential implements biz.MaterialRepo.
func (r *executionRepo) Credential(ctx context.Context, id string) (*biz.Credential, error) {
	out := &biz.Credential{}
	err := r.data.Pool.QueryRow(ctx, `
		SELECT type, username, secret, become_method, become_user, become_secret
		FROM job_credential WHERE id = $1 AND delete_time IS NULL`, id,
	).Scan(&out.Type, &out.Username, &out.Secret, &out.BecomeMethod, &out.BecomeUser, &out.BecomeSecret)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("query credential: %w", err)
	}
	return out, nil
}

// Project implements biz.MaterialRepo.
func (r *executionRepo) Project(ctx context.Context, id string) (*biz.Project, error) {
	out := &biz.Project{}
	err := r.data.Pool.QueryRow(ctx, `
		SELECT auth_type, username, secret FROM job_project WHERE id = $1 AND delete_time IS NULL`, id,
	).Scan(&out.AuthType, &out.Username, &out.Secret)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("query project: %w", err)
	}
	return out, nil
}

// Payload implements biz.PayloadRepo.
func (r *executionRepo) Payload(ctx context.Context, executionID string) ([]byte, error) {
	var content []byte
	err := r.data.Pool.QueryRow(ctx, `SELECT content FROM job_execution_payload WHERE execution_id = $1`, executionID).Scan(&content)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("query execution payload: %w", err)
	}
	if content == nil {
		content = []byte{}
	}
	return content, nil
}

// lockExecution 锁定执行行并读出判定要用的列；不存在返回 biz.ErrNotFound。
func lockExecution(ctx context.Context, tx pgx.Tx, executionID string) (biz.ExecState, error) {
	var s biz.ExecState
	err := tx.QueryRow(ctx, `
		SELECT status, COALESCE(runner_id::text, ''), lease_token, log_seq, log_bytes, log_truncated, commit_id, cancel_requested
		FROM job_execution WHERE id = $1 FOR UPDATE`, executionID,
	).Scan(&s.Status, &s.RunnerID, &s.LeaseToken, &s.LogSeq, &s.LogBytes, &s.LogTruncated, &s.CommitID, &s.CancelRequested)
	if errors.Is(err, pgx.ErrNoRows) {
		return s, biz.ErrNotFound
	}
	if err != nil {
		return s, fmt.Errorf("lock execution: %w", err)
	}
	return s, nil
}

// Report implements biz.ProgressRepo。
func (r *executionRepo) Report(ctx context.Context, executionID string, decide func(cur biz.ExecState) (*biz.ReportMutation, error)) (revived bool, err error) {
	err = dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		cur, err := lockExecution(ctx, tx, executionID)
		if err != nil {
			return err
		}
		m, err := decide(cur)
		if err != nil {
			return err
		}
		if m.Revive {
			if revived, err = reviveExecution(ctx, tx, executionID); err != nil {
				return err
			}
		}
		if len(m.Chunks) > 0 {
			seqs := make([]int64, len(m.Chunks))
			contents := make([]string, len(m.Chunks))
			for i, c := range m.Chunks {
				seqs[i], contents[i] = c.Seq, c.Content
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO job_execution_log (execution_id, seq, content)
				SELECT $1, u.seq, u.content FROM unnest($2::bigint[], $3::text[]) AS u(seq, content)
				ON CONFLICT DO NOTHING`,
				executionID, seqs, contents); err != nil {
				return fmt.Errorf("insert execution log: %w", err)
			}
		}
		if _, err := tx.Exec(ctx, `
			UPDATE job_execution
			SET log_seq = $2, log_bytes = $3, log_truncated = $4, commit_id = $5,
			    lease_until = now() + $6 * interval '1 second'
			WHERE id = $1`,
			executionID, m.LogSeq, m.LogBytes, m.LogTruncated, m.CommitID, seconds(m.Lease)); err != nil {
			return fmt.Errorf("update execution progress: %w", err)
		}
		return nil
	})
	return revived && err == nil, err
}

// Finish implements biz.ProgressRepo：主机行按 (execution_id, address) 批量更新（未上报的保持未执行），
// 执行行的三个主机统计列从更新后的主机行汇总。
func (r *executionRepo) Finish(ctx context.Context, executionID string, decide func(cur biz.ExecState) (*biz.FinishMutation, error)) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		cur, err := lockExecution(ctx, tx, executionID)
		if err != nil {
			return err
		}
		m, err := decide(cur)
		if err != nil || m == nil {
			return err
		}
		if n := len(m.Hosts); n > 0 {
			addresses := make([]string, n)
			cols := [6][]int32{}
			for i := range cols {
				cols[i] = make([]int32, n)
			}
			for i, h := range m.Hosts {
				addresses[i] = h.Address
				cols[0][i], cols[1][i], cols[2][i], cols[3][i], cols[4][i], cols[5][i] = h.Status, h.OK, h.Changed, h.Failed, h.Unreachable, h.Skipped
			}
			if _, err := tx.Exec(ctx, `
				UPDATE job_execution_host h
				SET status = u.status, ok = u.ok, changed = u.changed, failed = u.failed,
				    unreachable = u.unreachable, skipped = u.skipped
				FROM unnest($2::text[], $3::int[], $4::int[], $5::int[], $6::int[], $7::int[], $8::int[])
				     AS u(address, status, ok, changed, failed, unreachable, skipped)
				WHERE h.execution_id = $1 AND h.address = u.address`,
				executionID, addresses, cols[0], cols[1], cols[2], cols[3], cols[4], cols[5]); err != nil {
				return fmt.Errorf("update execution hosts: %w", err)
			}
		}
		if _, err := tx.Exec(ctx, `
			UPDATE job_execution e
			SET status = $2, finish_time = now(), error = $3,
			    -- 从「结果未知」直接落到真实终态（互斥位被占、没能恢复为执行中）：core 可能已按「结果未知」
			    -- 通知过，置空后它会按真实结果再通知一次。
			    notify_time = CASE WHEN e.status = 8 THEN NULL ELSE e.notify_time END,
			    commit_id = CASE WHEN $4 <> '' THEN $4 ELSE e.commit_id END,
			    host_ok = c.ok, host_failed = c.failed, host_unreachable = c.unreachable
			FROM (
				SELECT count(*) FILTER (WHERE status IN (1, 2)) AS ok,
				       count(*) FILTER (WHERE status = 3) AS failed,
				       count(*) FILTER (WHERE status = 4) AS unreachable
				FROM job_execution_host WHERE execution_id = $1
			) c
			WHERE e.id = $1`,
			executionID, m.Status, m.Error, m.CommitID); err != nil {
			return fmt.Errorf("finish execution: %w", err)
		}
		return nil
	})
}

// reviveExecution 把结果未知的执行恢复为执行中（结果未知 → 执行中，清掉结束时间与原因），返回是否恢复成功。
//
// 模板禁止并发时，执行中的行占用互斥位（uk_job_execution_exclusive：同一模板至多一条排队中 / 执行中 /
// 待审批的互斥执行），而该执行被判为结果未知期间互斥位已释放，core 可能已为同一模板接受了新的执行。此时不能
// 恢复：执行保持结果未知、什么都不清，但本次上报照常收日志、续租、记提交号并应答成功；结束时直接落为
// 真实终态（终态不占互斥位）。
//
// 恢复时一并把 notify_time 置空：core 可能已经为「结果未知」发过结束通知，置空后它会在真正结束时
// 再按最终结果通知一次。
//
// 两道防线：语句自带「没有别的行占着互斥位」的条件（常见情形，不产生错误）；语句放在保存点里执行，
// 条件判定与对方提交之间的竞态最终由唯一索引裁决——撞上唯一约束即回滚到保存点，按「未恢复」处理，
// 外层事务照常提交。
func reviveExecution(ctx context.Context, tx pgx.Tx, executionID string) (bool, error) {
	sp, err := tx.Begin(ctx) // 嵌套事务 = SAVEPOINT
	if err != nil {
		return false, fmt.Errorf("revive execution: savepoint: %w", err)
	}
	tag, err := sp.Exec(ctx, `
		UPDATE job_execution e SET status = 2, finish_time = NULL, error = '', notify_time = NULL
		WHERE e.id = $1 AND e.status = 8 AND (NOT e.exclusive OR NOT EXISTS (
			SELECT 1 FROM job_execution o
			WHERE o.template_id = e.template_id AND o.exclusive AND o.status IN (1, 2, 9) AND o.id <> e.id))`,
		executionID)
	if err != nil {
		if rbErr := sp.Rollback(ctx); rbErr != nil {
			return false, fmt.Errorf("revive execution: rollback savepoint: %w", rbErr)
		}
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
			return false, nil
		}
		return false, fmt.Errorf("revive execution: %w", err)
	}
	if err := sp.Commit(ctx); err != nil {
		return false, fmt.Errorf("revive execution: release savepoint: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

// ExpireLeases implements biz.HousekeepRepo：到期判定与落库时间都取数据库时钟；SKIP LOCKED 让
// 正在处理上报（持有行锁、即将续租）的执行不被误判，多实例并发清扫也互不阻塞。只看执行中的行：
// 已是结果未知的执行（包括节点仍在上报、但因互斥位被占而未能恢复的）不会被再次处理。
// 选行放在 MATERIALIZED 的 CTE 里，理由同 ClaimNext。
func (r *executionRepo) ExpireLeases(ctx context.Context, reason string, limit int) (int, error) {
	tag, err := r.data.Pool.Exec(ctx, `
		WITH expired AS MATERIALIZED (
			SELECT id FROM job_execution
			WHERE status = 2 AND lease_until < now()
			ORDER BY lease_until
			FOR UPDATE SKIP LOCKED
			LIMIT $2
		)
		UPDATE job_execution e SET status = 8, finish_time = now(), error = $1
		FROM expired
		WHERE e.id = expired.id`,
		reason, limit)
	if err != nil {
		return 0, fmt.Errorf("expire execution leases: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// PurgeLogs implements biz.HousekeepRepo：只删已结束（非排队中 / 执行中）的执行的日志块。
func (r *executionRepo) PurgeLogs(ctx context.Context, retention time.Duration, limit int) (int, error) {
	tag, err := r.data.Pool.Exec(ctx, `
		WITH doomed AS MATERIALIZED (
			SELECT g.execution_id, g.seq
			FROM job_execution_log g JOIN job_execution e ON e.id = g.execution_id
			WHERE g.create_time < now() - $1 * interval '1 second' AND e.status NOT IN (1, 2, 9)
			LIMIT $2
		)
		DELETE FROM job_execution_log l
		USING doomed d
		WHERE l.execution_id = d.execution_id AND l.seq = d.seq`,
		seconds(retention), limit)
	if err != nil {
		return 0, fmt.Errorf("purge execution logs: %w", err)
	}
	return int(tag.RowsAffected()), nil
}
