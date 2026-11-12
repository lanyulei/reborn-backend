package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/cmdbcontract"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	"github.com/lanyulei/reborn/backend/kit/secretbox"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.SyncSourceRepo   = (*syncSourceRepo)(nil)
	_ biz.SyncTaskRepo     = (*syncTaskRepo)(nil)
	_ biz.SyncRunRepo      = (*syncRunRepo)(nil)
	_ biz.CredentialSealer = (*SyncSecretBox)(nil)
)

// SyncSecretBox 采集源凭据加密器：与通知渠道同一主密钥（notify.master.key）、同一
// kit/secretbox 实现；core 只 Seal（只写不读），数据面只 Open。
type SyncSecretBox struct {
	box *secretbox.Box
}

// NewSyncSecretBox 由启动配置构造。
func NewSyncSecretBox(c *conf.Notify) (*SyncSecretBox, error) {
	box, err := secretbox.New(c.GetMaster().GetKey())
	if err != nil {
		return nil, err
	}
	return &SyncSecretBox{box: box}, nil
}

// Seal implements biz.CredentialSealer.
func (b *SyncSecretBox) Seal(plaintext string) ([]byte, error) {
	out, err := b.box.Seal(plaintext)
	if errors.Is(err, secretbox.ErrUnavailable) {
		return nil, biz.ErrSecretUnavailable
	}
	return out, err
}

type syncSourceRepo struct {
	data *basedata.Data
}

// NewSyncSourceRepo new a syncSourceRepo.
func NewSyncSourceRepo(data *basedata.Data) *syncSourceRepo { return &syncSourceRepo{data: data} }

// syncSourceColumns 采集源查询列序（s = cmdb_sync_source）；active_task 由子查询给出。
const syncSourceColumns = `s.id, s.name, s.provider, s.config, s.credential IS NOT NULL, s.env, s.interval_seconds, s.grace_period_seconds,
	s.enabled, s.next_run_time, s.last_run_id, s.last_run_time, s.last_status,
	EXISTS (SELECT 1 FROM cmdb_sync_task t WHERE t.source_id = s.id AND t.status IN ('pending', 'running')),
	s.webhook_token_hash IS NOT NULL,
	s.version, s.create_user, s.update_user, s.create_time, s.update_time`

func scanSyncSource(scan func(dest ...any) error) (*biz.SyncSource, error) {
	var src biz.SyncSource
	var lastRunID, createUser, updateUser *string
	if err := scan(&src.ID, &src.Name, &src.Provider, &src.Config, &src.HasCredential, &src.Env, &src.IntervalSeconds, &src.GracePeriodSeconds,
		&src.Enabled, &src.NextRunTime, &lastRunID, &src.LastRunTime, &src.LastStatus, &src.ActiveTask, &src.WebhookEnabled,
		&src.Version, &createUser, &updateUser, &src.CreateTime, &src.UpdateTime); err != nil {
		return nil, err
	}
	if lastRunID != nil {
		src.LastRunID = *lastRunID
	}
	if createUser != nil {
		src.CreateUser = *createUser
	}
	if updateUser != nil {
		src.UpdateUser = *updateUser
	}
	return &src, nil
}

// Create implements biz.SyncSourceRepo.
func (r *syncSourceRepo) Create(ctx context.Context, src *biz.SyncSource, credential []byte) error {
	_, err := r.data.Pool.Exec(ctx,
		`INSERT INTO cmdb_sync_source (id, name, provider, config, credential, env, interval_seconds, grace_period_seconds, enabled,
		   next_run_time, last_status, version, create_user, update_user, create_time, update_time)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)`,
		src.ID, src.Name, src.Provider, src.Config, dbx.NilIfEmptyBytes(credential), src.Env, src.IntervalSeconds, src.GracePeriodSeconds, src.Enabled,
		src.NextRunTime, src.LastStatus, src.Version, dbx.NilIfEmpty(src.CreateUser), dbx.NilIfEmpty(src.UpdateUser), src.CreateTime, src.UpdateTime)
	if err != nil {
		if isUniqueViolation(err, "uq_cmdb_sync_source_name") {
			return biz.ErrSourceNameExists
		}
		return fmt.Errorf("insert cmdb sync source: %w", err)
	}
	return nil
}

// Get implements biz.SyncSourceRepo.
func (r *syncSourceRepo) Get(ctx context.Context, id string) (*biz.SyncSource, error) {
	row := r.data.Pool.QueryRow(ctx, "SELECT "+syncSourceColumns+" FROM cmdb_sync_source s WHERE s.id = $1 AND s.delete_time IS NULL", id)
	src, err := scanSyncSource(row.Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrSourceNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get cmdb sync source: %w", err)
	}
	return src, nil
}

// List implements biz.SyncSourceRepo.
func (r *syncSourceRepo) List(ctx context.Context, filter biz.SyncSourceFilter, page, pageSize int64) ([]*biz.SyncSource, int64, error) {
	where := " WHERE s.delete_time IS NULL"
	args := make([]any, 0, 4)
	if filter.Keyword != "" {
		args = append(args, dbx.LikeContains(filter.Keyword))
		where += fmt.Sprintf(" AND s.name ILIKE $%d", len(args))
	}
	if filter.Provider != "" {
		args = append(args, filter.Provider)
		where += fmt.Sprintf(" AND s.provider = $%d", len(args))
	}
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM cmdb_sync_source s"+where, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count cmdb sync sources: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}
	args = append(args, pageSize, (page-1)*pageSize)
	rows, err := r.data.Pool.Query(ctx, "SELECT "+syncSourceColumns+" FROM cmdb_sync_source s"+where+
		fmt.Sprintf(" ORDER BY s.create_time DESC, s.id DESC LIMIT $%d OFFSET $%d", len(args)-1, len(args)), args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list cmdb sync sources: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.SyncSource, 0, pageSize)
	for rows.Next() {
		src, err := scanSyncSource(rows.Scan)
		if err != nil {
			return nil, 0, fmt.Errorf("scan cmdb sync source: %w", err)
		}
		items = append(items, src)
	}
	return items, total, rows.Err()
}

// Update implements biz.SyncSourceRepo（乐观锁；credential nil = 保留）。
func (r *syncSourceRepo) Update(ctx context.Context, src *biz.SyncSource, expectedVersion int64, credential []byte) error {
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE cmdb_sync_source SET name = $1, config = $2, credential = COALESCE($3, credential), env = $4, interval_seconds = $5,
		   grace_period_seconds = $6, enabled = $7, next_run_time = $8, version = version + 1, update_user = $9, update_time = $10
		 WHERE id = $11 AND version = $12 AND delete_time IS NULL`,
		src.Name, src.Config, dbx.NilIfEmptyBytes(credential), src.Env, src.IntervalSeconds, src.GracePeriodSeconds, src.Enabled, src.NextRunTime,
		dbx.NilIfEmpty(src.UpdateUser), src.UpdateTime, src.ID, expectedVersion)
	if err != nil {
		if isUniqueViolation(err, "uq_cmdb_sync_source_name") {
			return biz.ErrSourceNameExists
		}
		return fmt.Errorf("update cmdb sync source: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrSourceVersionConflict
	}
	return nil
}

// SoftDelete implements biz.SyncSourceRepo.
func (r *syncSourceRepo) SoftDelete(ctx context.Context, id string, now time.Time) error {
	tag, err := r.data.Pool.Exec(ctx, "UPDATE cmdb_sync_source SET delete_time = $1, enabled = false, update_time = $1 WHERE id = $2 AND delete_time IS NULL", now, id)
	if err != nil {
		return fmt.Errorf("soft delete cmdb sync source: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrSourceNotFound
	}
	return nil
}

// SetWebhookHash implements biz.SyncSourceRepo.
func (r *syncSourceRepo) SetWebhookHash(ctx context.Context, id string, hash []byte) error {
	tag, err := r.data.Pool.Exec(ctx, "UPDATE cmdb_sync_source SET webhook_token_hash = $1, update_time = now() WHERE id = $2 AND delete_time IS NULL",
		dbx.NilIfEmptyBytes(hash), id)
	if err != nil {
		return fmt.Errorf("set webhook hash: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrSourceNotFound
	}
	return nil
}

type syncTaskRepo struct {
	data *basedata.Data
}

// NewSyncTaskRepo new a syncTaskRepo.
func NewSyncTaskRepo(data *basedata.Data) *syncTaskRepo { return &syncTaskRepo{data: data} }

// Create implements biz.SyncTaskRepo（同源未完成任务由 partial unique index 拒绝）。
func (r *syncTaskRepo) Create(ctx context.Context, task *biz.SyncTask) error {
	_, err := r.data.Pool.Exec(ctx,
		"INSERT INTO cmdb_sync_task (id, source_id, trigger, status, create_user, create_time) VALUES ($1, $2, $3, $4, $5, $6)",
		task.ID, task.SourceID, task.Trigger, task.Status, dbx.NilIfEmpty(task.CreateUser), task.CreateTime)
	if err != nil {
		if isUniqueViolation(err, "uq_cmdb_sync_task_active") {
			return biz.ErrSyncBusy
		}
		if isForeignKeyViolation(err) {
			return biz.ErrSourceNotFound
		}
		return fmt.Errorf("insert cmdb sync task: %w", err)
	}
	return nil
}

type syncRunRepo struct {
	data *basedata.Data
}

// NewSyncRunRepo new a syncRunRepo.
func NewSyncRunRepo(data *basedata.Data) *syncRunRepo { return &syncRunRepo{data: data} }

// ListBySource implements biz.SyncRunRepo（时间倒序）。
func (r *syncRunRepo) ListBySource(ctx context.Context, sourceID string, page, pageSize int64) ([]*biz.SyncRun, int64, error) {
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM cmdb_sync_run WHERE source_id = $1", sourceID).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count cmdb sync runs: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}
	rows, err := r.data.Pool.Query(ctx,
		`SELECT id, source_id, task_id, status, discovered, created, updated, unchanged, disappeared, retired, failed, error, errors, start_time, finish_time
		 FROM cmdb_sync_run WHERE source_id = $1 ORDER BY start_time DESC, id DESC LIMIT $2 OFFSET $3`, sourceID, pageSize, (page-1)*pageSize)
	if err != nil {
		return nil, 0, fmt.Errorf("list cmdb sync runs: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.SyncRun, 0, pageSize)
	for rows.Next() {
		var run biz.SyncRun
		var errs []byte
		if err := rows.Scan(&run.ID, &run.SourceID, &run.TaskID, &run.Status, &run.Discovered, &run.Created, &run.Updated, &run.Unchanged,
			&run.Disappeared, &run.Retired, &run.Failed, &run.Error, &errs, &run.StartTime, &run.FinishTime); err != nil {
			return nil, 0, fmt.Errorf("scan cmdb sync run: %w", err)
		}
		if len(errs) > 0 {
			if err := json.Unmarshal(errs, &run.Errors); err != nil {
				return nil, 0, fmt.Errorf("decode cmdb sync run errors: %w", err)
			}
		}
		if run.Errors == nil {
			run.Errors = []cmdbcontract.RunError{}
		}
		items = append(items, &run)
	}
	return items, total, rows.Err()
}
