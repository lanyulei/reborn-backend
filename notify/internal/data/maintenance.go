package data

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/lanyulei/reborn/backend/kit/dbx"
	"github.com/lanyulei/reborn/backend/notify/internal/biz"

	"github.com/jackc/pgx/v5"
)

var _ biz.MaintenanceRepo = (*maintenanceRepo)(nil)

// maintenanceRepo 分区维护与行级清理（设计 §5.4；锁策略见评审 #17/#18）。
type maintenanceRepo struct {
	data *Data
}

// NewMaintenanceRepo new a maintenanceRepo.
func NewMaintenanceRepo(data *Data) *maintenanceRepo { return &maintenanceRepo{data: data} }

// partitionName 月分区命名与 core 迁移一致：<table>_YYYY_MM。
func partitionName(table string, month time.Time) string {
	return fmt.Sprintf("%s_%04d_%02d", table, month.Year(), int(month.Month()))
}

var partitionSuffix = regexp.MustCompile(`_(\d{4})_(\d{2})$`)

// EnsurePartition implements biz.MaintenanceRepo：
//  1. 独立建表（LIKE 父表含默认值/约束）并加区间 CHECK（ATTACH 时免扫描）；
//  2. 单短事务 lock_timeout=2s：迁出 DEFAULT 分区中该区间行 → ATTACH。
//
// 不用 CREATE TABLE … PARTITION OF（对父表取 ACCESS EXCLUSIVE 且扫描 DEFAULT，
// 会阻塞 core 嵌在业务事务里的 INSERT）。
func (r *maintenanceRepo) EnsurePartition(ctx context.Context, table string, month time.Time) error {
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
	// 标识符来自固定表名 + 格式化日期，无外部输入；SQL 文本仅拼接标识符与字面量。
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

// DropPartitionsBefore implements biz.MaintenanceRepo：上界 ≤ before 的月分区
// DROP（lock_timeout，失败下轮重试；DEFAULT 分区不动）。
func (r *maintenanceRepo) DropPartitionsBefore(ctx context.Context, table string, before time.Time) (int, error) {
	rows, err := r.data.Pool.Query(ctx,
		"SELECT inhrelid::regclass::text FROM pg_inherits WHERE inhparent = $1::regclass", table)
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
		if m == nil {
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

// DeleteExpiredDedup implements biz.MaintenanceRepo.
func (r *maintenanceRepo) DeleteExpiredDedup(ctx context.Context, limit int) (int, error) {
	tag, err := r.data.Pool.Exec(ctx,
		"DELETE FROM notify_dedup WHERE (biz_id, dedup_key) IN (SELECT biz_id, dedup_key FROM notify_dedup WHERE expire_time < now() LIMIT $1)", limit)
	if err != nil {
		return 0, fmt.Errorf("delete expired dedup: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// DeleteOldDlq implements biz.MaintenanceRepo.
func (r *maintenanceRepo) DeleteOldDlq(ctx context.Context, before time.Time, limit int) (int, error) {
	tag, err := r.data.Pool.Exec(ctx,
		"DELETE FROM notify_dlq WHERE id IN (SELECT id FROM notify_dlq WHERE create_time < $1 LIMIT $2)", before, limit)
	if err != nil {
		return 0, fmt.Errorf("delete old dlq: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// DeleteClosedAggGroups implements biz.MaintenanceRepo（评审 #13）。
func (r *maintenanceRepo) DeleteClosedAggGroups(ctx context.Context, before time.Time, limit int) (int, error) {
	tag, err := r.data.Pool.Exec(ctx,
		`DELETE FROM notify_agg_group WHERE group_key IN (
		   SELECT group_key FROM notify_agg_group
		   WHERE status = 2 AND update_time < $1 AND (suppressed_until IS NULL OR suppressed_until < now()) LIMIT $2)`, before, limit)
	if err != nil {
		return 0, fmt.Errorf("delete closed agg groups: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

func isDuplicateObject(err error) bool {
	var pgErr interface{ SQLState() string }
	if asErr, ok := err.(interface{ SQLState() string }); ok {
		pgErr = asErr
	}
	return pgErr != nil && pgErr.SQLState() == "42710"
}

// DeleteStaleChannelHealth implements biz.MaintenanceRepo：关闭态且久未更新的健康行
// （渠道已删/长期无流量）。
func (r *maintenanceRepo) DeleteStaleChannelHealth(ctx context.Context, before time.Time, limit int) (int, error) {
	tag, err := r.data.Pool.Exec(ctx,
		`DELETE FROM notify_channel_health WHERE channel_id IN (
		   SELECT channel_id FROM notify_channel_health WHERE open_until IS NULL AND update_time < $1 LIMIT $2)`, before, limit)
	if err != nil {
		return 0, fmt.Errorf("delete stale channel health: %w", err)
	}
	return int(tag.RowsAffected()), nil
}
