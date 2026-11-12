package data

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/lanyulei/reborn/backend/cmdb/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/cmdbcontract"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	"github.com/lanyulei/reborn/backend/kit/idgen"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	_ biz.MaintenanceRepo = (*maintenanceRepo)(nil)
	_ biz.WebhookRepo     = (*webhookRepo)(nil)
)

// maintenanceRepo 历史分区维护（与通知管道同一策略：独立建表 + 短事务 ATTACH；lock_timeout 失败下轮重试）。
type maintenanceRepo struct {
	data *Data
}

// NewMaintenanceRepo new a maintenanceRepo.
func NewMaintenanceRepo(data *Data) *maintenanceRepo { return &maintenanceRepo{data: data} }

// partitionName 月分区命名与 core 迁移一致：<table>_YYYY_MM。
func partitionName(table string, month time.Time) string {
	return fmt.Sprintf("%s_%04d_%02d", table, month.Year(), int(month.Month()))
}

var (
	partitionSuffix = regexp.MustCompile(`_(\d{4})_(\d{2})$`)
	tablePattern    = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)
)

func isDuplicateObject(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42710"
}

// EnsurePartition implements biz.MaintenanceRepo。标识符来自固定表名 + 格式化日期（白名单校验），无外部输入。
func (r *maintenanceRepo) EnsurePartition(ctx context.Context, table string, month time.Time) error {
	if !tablePattern.MatchString(table) {
		return fmt.Errorf("invalid table name %q", table)
	}
	lo := time.Date(month.Year(), month.Month(), 1, 0, 0, 0, 0, time.UTC)
	hi := lo.AddDate(0, 1, 0)
	name := partitionName(table, lo)
	var exists bool
	if err := r.data.Pool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_inherits WHERE inhparent = $1::regclass AND inhrelid = to_regclass($2))", table, name).Scan(&exists); err != nil {
		return fmt.Errorf("check partition %s: %w", name, err)
	}
	if exists {
		return nil
	}
	loS, hiS := lo.Format("2006-01-02"), hi.Format("2006-01-02")
	if _, err := r.data.Pool.Exec(ctx, fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (LIKE %s INCLUDING DEFAULTS INCLUDING CONSTRAINTS)", name, table)); err != nil {
		return fmt.Errorf("create partition table %s: %w", name, err)
	}
	if _, err := r.data.Pool.Exec(ctx, fmt.Sprintf(
		"ALTER TABLE %s ADD CONSTRAINT %s_range CHECK (create_time >= '%s' AND create_time < '%s')", name, name, loS, hiS)); err != nil &&
		!isDuplicateObject(err) {
		return fmt.Errorf("add range check %s: %w", name, err)
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout = '2s'"); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			"WITH moved AS (DELETE FROM %s_default WHERE create_time >= '%s' AND create_time < '%s' RETURNING *) INSERT INTO %s SELECT * FROM moved",
			table, loS, hiS, name)); err != nil {
			return fmt.Errorf("move rows out of default partition: %w", err)
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			"ALTER TABLE %s ATTACH PARTITION %s FOR VALUES FROM ('%s') TO ('%s')", table, name, loS, hiS)); err != nil {
			return fmt.Errorf("attach partition %s: %w", name, err)
		}
		return nil
	})
}

// DropPartitionsBefore implements biz.MaintenanceRepo（DEFAULT 分区不动）。
func (r *maintenanceRepo) DropPartitionsBefore(ctx context.Context, table string, before time.Time) (int, error) {
	if !tablePattern.MatchString(table) {
		return 0, fmt.Errorf("invalid table name %q", table)
	}
	rows, err := r.data.Pool.Query(ctx, "SELECT inhrelid::regclass::text FROM pg_inherits WHERE inhparent = $1::regclass", table)
	if err != nil {
		return 0, fmt.Errorf("list partitions: %w", err)
	}
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			rows.Close()
			return 0, err
		}
		names = append(names, n)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	dropped := 0
	for _, n := range names {
		m := partitionSuffix.FindStringSubmatch(n)
		if m == nil || !tablePattern.MatchString(n) {
			continue
		}
		start, err := time.Parse("2006_01", m[1]+"_"+m[2])
		if err != nil {
			continue
		}
		if start.AddDate(0, 1, 0).After(before) {
			continue
		}
		err = dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout = '2s'"); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, "DROP TABLE IF EXISTS "+n)
			return err
		})
		if err != nil {
			return dropped, fmt.Errorf("drop partition %s: %w", n, err)
		}
		dropped++
	}
	return dropped, nil
}

// webhookRepo 回调接入仓储。
type webhookRepo struct {
	data *Data
}

// NewWebhookRepo new a webhookRepo.
func NewWebhookRepo(data *Data) *webhookRepo { return &webhookRepo{data: data} }

// GetWebhook implements biz.WebhookRepo.
func (r *webhookRepo) GetWebhook(ctx context.Context, sourceID string) (*biz.WebhookTarget, error) {
	var t biz.WebhookTarget
	err := r.data.Pool.QueryRow(ctx,
		"SELECT id, enabled AND credential IS NOT NULL, webhook_token_hash FROM cmdb_sync_source WHERE id = $1 AND delete_time IS NULL", sourceID).
		Scan(&t.ID, &t.Enabled, &t.TokenHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrWebhookSourceNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get webhook target: %w", err)
	}
	return &t, nil
}

// TouchWebhook implements biz.WebhookRepo（单语句条件更新 = 多实例安全的限流门闩）。
func (r *webhookRepo) TouchWebhook(ctx context.Context, sourceID string, minInterval time.Duration) (bool, error) {
	tag, err := r.data.Pool.Exec(ctx,
		"UPDATE cmdb_sync_source SET webhook_last_time = now() WHERE id = $1 AND (webhook_last_time IS NULL OR webhook_last_time < now() - $2::interval)",
		sourceID, minInterval)
	if err != nil {
		return false, fmt.Errorf("touch webhook: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// EnqueueTask implements biz.WebhookRepo（活动任务唯一约束下 ON CONFLICT DO NOTHING，再读回当前任务）。
func (r *webhookRepo) EnqueueTask(ctx context.Context, sourceID, trigger string) (string, bool, error) {
	id, err := idgen.NewID()
	if err != nil {
		return "", false, err
	}
	tag, err := r.data.Pool.Exec(ctx,
		`INSERT INTO cmdb_sync_task (id, source_id, trigger, status, create_time) VALUES ($1, $2, $3, $4, now())
		 ON CONFLICT (source_id) WHERE status IN ('pending', 'running') DO NOTHING`, id, sourceID, trigger, cmdbcontract.TaskPending)
	if err != nil {
		return "", false, fmt.Errorf("enqueue webhook task: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return id, true, nil
	}
	var current string
	if err := r.data.Pool.QueryRow(ctx, "SELECT id FROM cmdb_sync_task WHERE source_id = $1 AND status IN ('pending', 'running') LIMIT 1", sourceID).Scan(&current); err != nil {
		return "", false, fmt.Errorf("find active task: %w", err)
	}
	return current, false, nil
}
