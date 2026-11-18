package data

import (
	"context"
	"errors"
	"fmt"

	"github.com/lanyulei/reborn/backend/job/internal/biz"

	"github.com/jackc/pgx/v5"
)

var _ biz.RunnerRepo = (*runnerRepo)(nil)

// runnerRepo 执行节点的读取与心跳回写（job_runner）。
type runnerRepo struct {
	data *Data
}

// NewRunnerRepo new a runnerRepo.
func NewRunnerRepo(data *Data) *runnerRepo { return &runnerRepo{data: data} }

// Find implements biz.RunnerRepo.
func (r *runnerRepo) Find(ctx context.Context, id string) (*biz.Runner, error) {
	out := &biz.Runner{ID: id}
	err := r.data.Pool.QueryRow(ctx,
		`SELECT zone, enabled, token_hash FROM job_runner WHERE id = $1 AND delete_time IS NULL`, id,
	).Scan(&out.Zone, &out.Enabled, &out.TokenHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("query runner: %w", err)
	}
	return out, nil
}

// Heartbeat implements biz.RunnerRepo：只写引擎被授权的五个心跳列，last_seen 取数据库时钟。
func (r *runnerRepo) Heartbeat(ctx context.Context, id string, hb biz.Heartbeat) (bool, error) {
	var enabled bool
	err := r.data.Pool.QueryRow(ctx, `
		UPDATE job_runner
		SET agent_version = $2, hostname = $3, capacity = $4, running = $5, last_seen = now()
		WHERE id = $1 AND delete_time IS NULL
		RETURNING enabled`,
		id, hb.AgentVersion, hb.Hostname, hb.Capacity, hb.Running,
	).Scan(&enabled)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, biz.ErrNotFound
	}
	if err != nil {
		return false, fmt.Errorf("update runner heartbeat: %w", err)
	}
	return enabled, nil
}
