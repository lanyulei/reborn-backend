package data

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/job"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.StatsReader = (*statsRepo)(nil)
	_ biz.NotifyQueue = (*statsRepo)(nil)
	_ biz.Retention   = (*statsRepo)(nil)
)

// statsRepo 执行统计与执行结束通知的领取（都是对 job_execution 的只读 / 单列写）。
type statsRepo struct {
	data *basedata.Data
}

// NewStatsRepo new a statsRepo.
func NewStatsRepo(data *basedata.Data) *statsRepo { return &statsRepo{data: data} }

// 失败口径：失败 / 部分失败 / 超时 / 结果未知。
const failedStatuses = "(4, 5, 7, 8)"

// Stats implements biz.StatsReader。时间范围按提交时间；各项聚合都落在 idx_job_execution_create_time 上。
func (r *statsRepo) Stats(ctx context.Context, since time.Time, timezone string) (*biz.Stats, error) {
	out := &biz.Stats{Timezone: timezone}
	pool := r.data.Pool
	if err := pool.QueryRow(ctx,
		`SELECT count(*), count(*) FILTER (WHERE status NOT IN (1, 2, 9)), count(*) FILTER (WHERE status = 3),
		        COALESCE(avg(extract(epoch FROM finish_time - start_time)) FILTER (WHERE start_time IS NOT NULL AND finish_time IS NOT NULL), 0)::bigint,
		        COALESCE(sum(host_total), 0)
		 FROM job_execution WHERE create_time >= $1`, since).
		Scan(&out.Total, &out.Finished, &out.Success, &out.AvgDurationSeconds, &out.HostRuns); err != nil {
		return nil, fmt.Errorf("job stats totals: %w", err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT (SELECT count(*) FROM job_execution WHERE status IN (1, 2, 9)),
		        (SELECT count(*) FROM job_runner WHERE delete_time IS NULL AND enabled AND last_seen > now() - make_interval(secs => $1)),
		        (SELECT count(*) FROM job_runner WHERE delete_time IS NULL AND enabled)`, biz.RunnerOnlineWindow.Seconds()).
		Scan(&out.Active, &out.RunnersOnline, &out.RunnersTotal); err != nil {
		return nil, fmt.Errorf("job stats live counters: %w", err)
	}
	var err error
	// column 为代码内常量。
	group := func(column string) ([]biz.StatCount, error) {
		rows, err := pool.Query(ctx, "SELECT "+column+", count(*) FROM job_execution WHERE create_time >= $1 GROUP BY 1 ORDER BY 1", since)
		if err != nil {
			return nil, fmt.Errorf("job stats by %s: %w", column, err)
		}
		return pgx.CollectRows(rows, func(row pgx.CollectableRow) (biz.StatCount, error) {
			var key int32
			var c biz.StatCount
			err := row.Scan(&key, &c.Count)
			c.Key = strconv.Itoa(int(key))
			return c, err
		})
	}
	if out.ByStatus, err = group("status"); err != nil {
		return nil, err
	}
	if out.ByKind, err = group("kind"); err != nil {
		return nil, err
	}
	if out.ByTrigger, err = group("trigger"); err != nil {
		return nil, err
	}
	rows, err := pool.Query(ctx,
		`SELECT template_id::text, max(template_name), count(*), count(*) FILTER (WHERE status IN `+failedStatuses+`)
		 FROM job_execution WHERE create_time >= $1 AND template_id IS NOT NULL
		 GROUP BY template_id ORDER BY 3 DESC, 1 LIMIT 10`, since)
	if err != nil {
		return nil, fmt.Errorf("job stats top templates: %w", err)
	}
	if out.TopTemplates, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (biz.StatCount, error) {
		var c biz.StatCount
		err := row.Scan(&c.Key, &c.Name, &c.Count, &c.Failed)
		return c, err
	}); err != nil {
		return nil, fmt.Errorf("job stats top templates: %w", err)
	}
	rows, err = pool.Query(ctx,
		`SELECT to_char(create_time AT TIME ZONE $2, 'YYYY-MM-DD'), count(*), count(*) FILTER (WHERE status = 3),
		        count(*) FILTER (WHERE status IN `+failedStatuses+`)
		 FROM job_execution WHERE create_time >= $1 GROUP BY 1 ORDER BY 1`, since, timezone)
	if err != nil {
		return nil, fmt.Errorf("job stats trend: %w", err)
	}
	if out.Trend, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (biz.StatTrendPoint, error) {
		var p biz.StatTrendPoint
		err := row.Scan(&p.Day, &p.Total, &p.Success, &p.Failed)
		return p, err
	}); err != nil {
		return nil, fmt.Errorf("job stats trend: %w", err)
	}
	return out, nil
}

// ClaimTx implements biz.NotifyQueue：行锁领取一条已结束、未通知的执行并标记已通知（SKIP LOCKED，
// 多实例互不阻塞）。标记与通知事件的写入同事务提交。
func (r *statsRepo) ClaimTx(ctx context.Context, btx biz.Tx) (*biz.FinishedExecution, error) {
	tx, err := pgxTx(btx)
	if err != nil {
		return nil, err
	}
	var e biz.FinishedExecution
	err = tx.QueryRow(ctx,
		`WITH picked AS MATERIALIZED (
		   SELECT id FROM job_execution
		   WHERE notify_time IS NULL AND status NOT IN (1, 2, 9)
		     AND (status <> 8 OR finish_time < now() - interval '5 minutes')
		   ORDER BY finish_time FOR UPDATE SKIP LOCKED LIMIT 1
		 )
		 UPDATE job_execution e SET notify_time = now() FROM picked WHERE e.id = picked.id
		 RETURNING e.id::text, e.template_name, e.kind, e.trigger, e.creator::text, e.status, e.error,
		           e.host_total, e.host_ok, e.host_failed, e.host_unreachable, e.start_time, e.finish_time`).
		Scan(&e.ID, &e.Name, &e.Kind, &e.Trigger, &e.Creator, &e.Status, &e.Error,
			&e.HostTotal, &e.HostOK, &e.HostFailed, &e.HostUnreachable, &e.StartTime, &e.FinishTime)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim finished job execution: %w", err)
	}
	return &e, nil
}

// Purge implements biz.Retention。
func (r *statsRepo) Purge(ctx context.Context, payloadAge, fireAge time.Duration, limit int) (int, error) {
	tag, err := r.data.Pool.Exec(ctx,
		`DELETE FROM job_execution_payload p USING (
		   SELECT q.execution_id FROM job_execution_payload q JOIN job_execution e ON e.id = q.execution_id
		   WHERE e.status NOT IN (1, 2, 9) AND e.finish_time < now() - make_interval(secs => $1) LIMIT $2
		 ) d WHERE p.execution_id = d.execution_id`, payloadAge.Seconds(), limit)
	if err != nil {
		return 0, fmt.Errorf("purge job execution payloads: %w", err)
	}
	n := tag.RowsAffected()
	tag, err = r.data.Pool.Exec(ctx,
		`DELETE FROM job_trigger_fire WHERE id IN (
		   SELECT id FROM job_trigger_fire WHERE create_time < now() - make_interval(secs => $1) AND signal_state <> 1 LIMIT $2)`,
		fireAge.Seconds(), limit)
	if err != nil {
		return 0, fmt.Errorf("purge job trigger fires: %w", err)
	}
	return int(max(n, tag.RowsAffected())), nil
}
