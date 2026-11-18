package data

import (
	"context"
	"errors"
	"fmt"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/job"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.RunnerReader = (*runnerRepo)(nil)
	_ biz.RunnerWriter = (*runnerRepo)(nil)
	_ biz.RunnerTokens = (*runnerRepo)(nil)
)

// runnerColumns 执行节点查询列序；$1 恒为在线判定窗口的秒数。令牌摘要不读出。
const runnerColumns = `id::text, name, zone, enabled, description,
	COALESCE(last_seen > now() - make_interval(secs => $1), false), last_seen, agent_version, hostname, capacity, running,
	version, create_time, update_time`

func scanRunner(scan func(dest ...any) error) (*biz.Runner, error) {
	var r biz.Runner
	if err := scan(&r.ID, &r.Name, &r.Zone, &r.Enabled, &r.Description, &r.Online, &r.LastSeen, &r.AgentVersion,
		&r.Hostname, &r.Capacity, &r.Running, &r.Version, &r.CreateTime, &r.UpdateTime); err != nil {
		return nil, err
	}
	return &r, nil
}

type runnerRepo struct {
	data *basedata.Data
}

// NewRunnerRepo new a runnerRepo.
func NewRunnerRepo(data *basedata.Data) *runnerRepo { return &runnerRepo{data: data} }

// List implements biz.RunnerReader.
func (r *runnerRepo) List(ctx context.Context, keyword string, page, pageSize int64) ([]*biz.Runner, int64, error) {
	a := sqlArgs{args: []any{biz.RunnerOnlineWindow.Seconds()}}
	where := "delete_time IS NULL"
	countArgs := []any{}
	countWhere := where
	if keyword != "" {
		k := a.add(dbx.LikeContains(keyword))
		where += " AND (name ILIKE " + k + " OR zone ILIKE " + k + " OR hostname ILIKE " + k + ")"
		countWhere += " AND (name ILIKE $1 OR zone ILIKE $1 OR hostname ILIKE $1)"
		countArgs = append(countArgs, dbx.LikeContains(keyword))
	}
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM job_runner WHERE "+countWhere, countArgs...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count job runners: %w", err)
	}
	limit, offset := a.add(pageSize), a.add((page-1)*pageSize)
	rows, err := r.data.Pool.Query(ctx, "SELECT "+runnerColumns+" FROM job_runner WHERE "+where+
		" ORDER BY create_time DESC, id LIMIT "+limit+" OFFSET "+offset, a.args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list job runners: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.Runner, 0, pageSize)
	for rows.Next() {
		item, err := scanRunner(rows.Scan)
		if err != nil {
			return nil, 0, fmt.Errorf("scan job runner: %w", err)
		}
		items = append(items, item)
	}
	return items, total, rows.Err()
}

// Get implements biz.RunnerReader.
func (r *runnerRepo) Get(ctx context.Context, id string) (*biz.Runner, error) {
	item, err := scanRunner(r.data.Pool.QueryRow(ctx,
		"SELECT "+runnerColumns+" FROM job_runner WHERE id = $2 AND delete_time IS NULL", biz.RunnerOnlineWindow.Seconds(), id).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrRunnerNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get job runner: %w", err)
	}
	return item, nil
}

// Zones implements biz.RunnerReader.
func (r *runnerRepo) Zones(ctx context.Context) ([]string, error) {
	rows, err := r.data.Pool.Query(ctx,
		"SELECT DISTINCT zone FROM job_runner WHERE delete_time IS NULL AND zone <> '' ORDER BY zone LIMIT $1", maxOptions)
	if err != nil {
		return nil, fmt.Errorf("list job runner zones: %w", err)
	}
	defer rows.Close()
	zones := make([]string, 0, 8)
	for rows.Next() {
		var z string
		if err := rows.Scan(&z); err != nil {
			return nil, fmt.Errorf("scan job runner zone: %w", err)
		}
		zones = append(zones, z)
	}
	return zones, rows.Err()
}

func runnerWriteErr(err error, what string) error {
	if isUniqueViolation(err, "uk_job_runner_name") {
		return biz.ErrRunnerNameExists
	}
	return fmt.Errorf("%s job runner: %w", what, err)
}

// Create implements biz.RunnerWriter.
func (r *runnerRepo) Create(ctx context.Context, item *biz.Runner, tokenHash []byte, actor string) error {
	_, err := r.data.Pool.Exec(ctx,
		`INSERT INTO job_runner (id, name, zone, token_hash, enabled, description, create_user, update_user)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $7)`,
		item.ID, item.Name, item.Zone, tokenHash, item.Enabled, item.Description, dbx.NilIfEmpty(actor))
	if err != nil {
		return runnerWriteErr(err, "insert")
	}
	return nil
}

// Update implements biz.RunnerWriter。心跳列归执行引擎，不在此写。
func (r *runnerRepo) Update(ctx context.Context, item *biz.Runner, expectVersion int32, actor string) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if err := lockVersion(ctx, tx, "job_runner", item.ID, expectVersion, biz.ErrRunnerNotFound); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE job_runner SET name = $2, zone = $3, enabled = $4, description = $5,
			   version = version + 1, update_time = now(), update_user = $6
			 WHERE id = $1`,
			item.ID, item.Name, item.Zone, item.Enabled, item.Description, dbx.NilIfEmpty(actor)); err != nil {
			return runnerWriteErr(err, "update")
		}
		return nil
	})
}

// Delete implements biz.RunnerWriter。软删同时以空字节覆盖令牌摘要：任何密钥的 SHA-256 都是 32 字节，
// 不可能与之相等（引擎本就只认未删除的节点，这是第二道保险）。
func (r *runnerRepo) Delete(ctx context.Context, id, actor string) error {
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE job_runner SET delete_time = now(), delete_user = $2, token_hash = ''::bytea, version = version + 1
		 WHERE id = $1 AND delete_time IS NULL`, id, dbx.NilIfEmpty(actor))
	if err != nil {
		return fmt.Errorf("delete job runner: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrRunnerNotFound
	}
	return nil
}

// SetToken implements biz.RunnerTokens。
func (r *runnerRepo) SetToken(ctx context.Context, id string, tokenHash []byte, actor string) error {
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE job_runner SET token_hash = $2, version = version + 1, update_time = now(), update_user = $3
		 WHERE id = $1 AND delete_time IS NULL`, id, tokenHash, dbx.NilIfEmpty(actor))
	if err != nil {
		return fmt.Errorf("reset job runner token: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrRunnerNotFound
	}
	return nil
}

// Names implements biz.RunnerTokens（含已删除的节点）。
func (r *runnerRepo) Names(ctx context.Context, ids []string) (map[string]string, error) {
	if len(ids) == 0 {
		return map[string]string{}, nil
	}
	rows, err := r.data.Pool.Query(ctx, "SELECT id::text, name FROM job_runner WHERE id = ANY($1::uuid[])", ids)
	if err != nil {
		return nil, fmt.Errorf("list job runner names: %w", err)
	}
	return collectNames(rows, "job runner name")
}
