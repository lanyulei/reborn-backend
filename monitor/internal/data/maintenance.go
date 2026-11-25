package data

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/lanyulei/reborn/backend/kit/dbx"
	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/monitor/internal/biz"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	_ biz.PartitionRepo = (*maintenanceRepo)(nil)
	_ biz.RetentionRepo = (*maintenanceRepo)(nil)
)

// 判定记录表及其分区命名（与迁移 0069 一致：monitor_eval_record_YYYY_MM_DD，UTC 日界）。
const (
	evalRecordTable   = "monitor_eval_record"
	evalRecordDefault = "monitor_eval_record_default"
	// partitionBoundLayout 分区边界字面量带显式 UTC 偏移：边界是绝对时刻，不随会话 TimeZone 漂移。
	partitionBoundLayout = "2006-01-02 15:04:05+00"
	// partitionLockTimeout 分区 DDL 等锁上限：拿不到锁就放弃，下一轮重试，不长时间阻塞评估写入。
	partitionLockTimeout = "SET LOCAL lock_timeout = '2s'"
)

// partitionPattern 只认本服务命名的日分区；也是拼进 DDL 的标识符白名单。
var partitionPattern = regexp.MustCompile(`^monitor_eval_record_(\d{4})_(\d{2})_(\d{2})$`)

// maintenanceRepo 判定记录分区维护与过期数据清理。全部语句幂等，时间取数据库时钟。
type maintenanceRepo struct {
	data *Data
}

// NewMaintenanceRepo new a maintenanceRepo.
func NewMaintenanceRepo(data *Data) *maintenanceRepo { return &maintenanceRepo{data: data} }

func partitionName(day time.Time) string {
	return fmt.Sprintf("%s_%04d_%02d_%02d", evalRecordTable, day.Year(), int(day.Month()), day.Day())
}

func isDuplicateObject(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42710"
}

// today 数据库时钟下的当前 UTC 日期。
func (r *maintenanceRepo) today(ctx context.Context) (time.Time, error) {
	var now time.Time
	if err := r.data.Pool.QueryRow(ctx, "SELECT now()").Scan(&now); err != nil {
		return time.Time{}, fmt.Errorf("read database clock: %w", err)
	}
	now = now.UTC()
	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC), nil
}

// EnsurePartitions implements biz.PartitionRepo.
func (r *maintenanceRepo) EnsurePartitions(ctx context.Context, ahead int) error {
	day, err := r.today(ctx)
	if err != nil {
		return err
	}
	var first error
	for i := 0; i <= ahead; i++ {
		if err := r.ensurePartition(ctx, day.AddDate(0, 0, i)); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// ensurePartition 确保某一天的分区存在：
//  1. 独立建表（LIKE 父表含默认值 / 约束）并加区间 CHECK（ATTACH 时免扫描），属主对齐父表；
//  2. 单短事务（lock_timeout 2s）：锁 DEFAULT 分区 → 迁出其中落在该区间的行 → ATTACH。
//
// 不用 CREATE TABLE … PARTITION OF：它对父表取 ACCESS EXCLUSIVE 并扫描 DEFAULT，会阻塞评估写入。
// 标识符来自固定表名 + 格式化日期，SQL 文本不含任何外部输入。
func (r *maintenanceRepo) ensurePartition(ctx context.Context, day time.Time) error {
	name := partitionName(day)
	var attached bool
	if err := r.data.Pool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_inherits WHERE inhparent = $1::regclass AND inhrelid = to_regclass($2))",
		evalRecordTable, name).Scan(&attached); err != nil {
		return fmt.Errorf("check partition %s: %w", name, err)
	}
	if attached {
		return nil
	}
	lo, hi := day.Format(partitionBoundLayout), day.AddDate(0, 0, 1).Format(partitionBoundLayout)
	if _, err := r.data.Pool.Exec(ctx, fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (LIKE %s INCLUDING DEFAULTS INCLUDING CONSTRAINTS)", name, evalRecordTable)); err != nil {
		return fmt.Errorf("create partition table %s: %w", name, err)
	}
	// 属主对齐父表（迁移设为 monitor_engine_role）：否则由登录账号 A 建的分区，换成账号 B 后无法 DROP。
	var owner, parentOwner string
	if err := r.data.Pool.QueryRow(ctx,
		`SELECT pg_get_userbyid(c.relowner), pg_get_userbyid(p.relowner)
		 FROM pg_class c, pg_class p WHERE c.oid = to_regclass($1) AND p.oid = $2::regclass`, name, evalRecordTable).
		Scan(&owner, &parentOwner); err != nil {
		return fmt.Errorf("read owner of %s: %w", name, err)
	}
	if owner != parentOwner {
		if _, err := r.data.Pool.Exec(ctx, fmt.Sprintf("ALTER TABLE %s OWNER TO %s", name, pgx.Identifier{parentOwner}.Sanitize())); err != nil {
			return fmt.Errorf("align owner of %s: %w", name, err)
		}
	}
	if _, err := r.data.Pool.Exec(ctx, fmt.Sprintf(
		"ALTER TABLE %s ADD CONSTRAINT %s_range CHECK (eval_time >= '%s' AND eval_time < '%s')", name, name, lo, hi)); err != nil &&
		!isDuplicateObject(err) {
		return fmt.Errorf("add range check %s: %w", name, err)
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, partitionLockTimeout); err != nil {
			return err
		}
		// 先锁 DEFAULT：迁出与 ATTACH 之间不得再有行落进该区间（否则 ATTACH 校验失败）。
		// 路由到其它分区的写入不受影响。
		if _, err := tx.Exec(ctx, "LOCK TABLE "+evalRecordDefault+" IN ACCESS EXCLUSIVE MODE"); err != nil {
			return fmt.Errorf("lock default partition: %w", err)
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			"WITH moved AS (DELETE FROM %s WHERE eval_time >= '%s' AND eval_time < '%s' RETURNING *) INSERT INTO %s SELECT * FROM moved",
			evalRecordDefault, lo, hi, name)); err != nil {
			return fmt.Errorf("move rows out of default partition: %w", err)
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			"ALTER TABLE %s ATTACH PARTITION %s FOR VALUES FROM ('%s') TO ('%s')", evalRecordTable, name, lo, hi)); err != nil {
			return fmt.Errorf("attach partition %s: %w", name, err)
		}
		return nil
	})
}

// DropExpiredPartitions implements biz.PartitionRepo：上界 ≤ now() − retention 的日分区逐个 DROP
// （DEFAULT 分区与不认识的子表不动）。
//
// DROP 需要父表与 DEFAULT 分区的 ACCESS EXCLUSIVE（PG 要重算 DEFAULT 的分区约束）。不持有该锁的做法
// 只有 DETACH PARTITION CONCURRENTLY，而它在本表上不可用——PostgreSQL 18 实测：
//   - 父表有 DEFAULT 分区时直接报错 "cannot detach partitions concurrently when a default partition exists"；
//     DEFAULT 分区是分区维护失效时的兜底（写入不会因缺分区而失败），不能为此去掉；
//   - 它也不能在事务块内执行，于是无法与 lock_timeout 的 SET LOCAL 放在一起；
//   - 普通的 DETACH（非 CONCURRENTLY）拿的锁与 DROP 完全一样，多一步没有任何好处。
//
// 所以保留 DROP，把影响压到最小：每个分区一个极短的事务（只改系统目录，文件在提交后才删除），
// lock_timeout 2s——拿不到锁就放弃，且本轮不再尝试其余分区（排队中的 ACCESS EXCLUSIVE 会挡住后来的
// 写入，一轮至多挡这一次），下一轮重试。评估写入都是短事务，正常情况下几毫秒内即可拿到锁。
func (r *maintenanceRepo) DropExpiredPartitions(ctx context.Context, retention time.Duration) (int, error) {
	var cutoff time.Time
	if err := r.data.Pool.QueryRow(ctx, "SELECT now() - $1::interval", retention).Scan(&cutoff); err != nil {
		return 0, fmt.Errorf("read database clock: %w", err)
	}
	rows, err := r.data.Pool.Query(ctx, "SELECT inhrelid::regclass::text FROM pg_inherits WHERE inhparent = $1::regclass", evalRecordTable)
	if err != nil {
		return 0, fmt.Errorf("list partitions: %w", err)
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return 0, fmt.Errorf("list partitions: %w", err)
	}
	dropped := 0
	for _, name := range names {
		m := partitionPattern.FindStringSubmatch(name)
		if m == nil {
			continue
		}
		day, err := time.Parse("2006_01_02", m[1]+"_"+m[2]+"_"+m[3])
		if err != nil || day.AddDate(0, 0, 1).After(cutoff) {
			continue
		}
		err = dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, partitionLockTimeout); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, "DROP TABLE IF EXISTS "+name)
			return err
		})
		if err != nil {
			return dropped, fmt.Errorf("drop partition %s: %w", name, err)
		}
		dropped++
	}
	return dropped, nil
}

// PurgeDefaultPartition implements biz.PartitionRepo：分区维护失效期间落进 DEFAULT 且已过保留期的行。
func (r *maintenanceRepo) PurgeDefaultPartition(ctx context.Context, retention time.Duration, limit int) (int, error) {
	tag, err := r.data.Pool.Exec(ctx,
		"DELETE FROM "+evalRecordDefault+` WHERE (id, eval_time) IN (
		   SELECT id, eval_time FROM `+evalRecordDefault+` WHERE eval_time < now() - $1::interval LIMIT $2)`, retention, limit)
	if err != nil {
		return 0, fmt.Errorf("purge default partition: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// PurgeAlerts implements biz.RetentionRepo：一条语句删除一批过期的非活跃告警，连同其通知记录、发件箱行、
// 时间线、事件动作、事件指令与升级进度（告警没了，这些行只是孤儿）。SKIP LOCKED 避开正被 core 回写的行，
// 下一批再处理。仍有待派发通知行的事件不清理：core 的派发事务是「发件箱行 → 告警行」，这里是「告警行 →
// 发件箱行」，两边同时命中同一事件会互相等锁；这类事件等它的发件箱行终结、过了保留期再清。
func (r *maintenanceRepo) PurgeAlerts(ctx context.Context, retention time.Duration, limit int) (int, error) {
	tag, err := r.data.Pool.Exec(ctx,
		`WITH old AS MATERIALIZED (
		   SELECT id FROM monitor_alert WHERE status <> $1 AND resolve_time < now() - $2::interval
		     AND NOT EXISTS (SELECT 1 FROM monitor_notify_outbox o WHERE o.alert_id = monitor_alert.id AND o.status = $4)
		   ORDER BY resolve_time LIMIT $3 FOR UPDATE SKIP LOCKED),
		 logs AS (DELETE FROM monitor_notify_log WHERE alert_id IN (SELECT id FROM old)),
		 outbox AS (DELETE FROM monitor_notify_outbox WHERE alert_id IN (SELECT id FROM old)),
		 events AS (DELETE FROM monitor_alert_event WHERE alert_id IN (SELECT id FROM old)),
		 actions AS (DELETE FROM monitor_alert_action WHERE alert_id IN (SELECT id FROM old)),
		 cmds AS (DELETE FROM monitor_alert_cmd WHERE alert_id IN (SELECT id FROM old)),
		 levels AS (DELETE FROM monitor_alert_escalation WHERE alert_id IN (SELECT id FROM old))
		 DELETE FROM monitor_alert WHERE id IN (SELECT id FROM old)`,
		monitorcontract.AlertFiring, retention, limit, monitorcontract.OutboxPending)
	if err != nil {
		return 0, fmt.Errorf("purge expired alerts: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// PurgeOutbox implements biz.RetentionRepo：已派发 / 已丢弃且过了保留期的发件箱行。
func (r *maintenanceRepo) PurgeOutbox(ctx context.Context, retention time.Duration, limit int) (int, error) {
	tag, err := r.data.Pool.Exec(ctx,
		`DELETE FROM monitor_notify_outbox WHERE id IN (
		   SELECT id FROM monitor_notify_outbox WHERE status <> $1 AND done_time < now() - $2::interval LIMIT $3)`,
		monitorcontract.OutboxPending, retention, limit)
	if err != nil {
		return 0, fmt.Errorf("purge outbox: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// PurgeEngineEvents implements biz.RetentionRepo：已派发 / 已丢弃且过了保留期的自监控事件。
func (r *maintenanceRepo) PurgeEngineEvents(ctx context.Context, retention time.Duration, limit int) (int, error) {
	tag, err := r.data.Pool.Exec(ctx,
		`DELETE FROM monitor_engine_event WHERE id IN (
		   SELECT id FROM monitor_engine_event WHERE status <> $1 AND done_time < now() - $2::interval LIMIT $3)`,
		monitorcontract.OutboxPending, retention, limit)
	if err != nil {
		return 0, fmt.Errorf("purge engine events: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// PurgeCommands implements biz.RetentionRepo：已处理且过了保留期的事件指令（告警已先被清理掉的那些）。
func (r *maintenanceRepo) PurgeCommands(ctx context.Context, retention time.Duration, limit int) (int, error) {
	tag, err := r.data.Pool.Exec(ctx,
		`DELETE FROM monitor_alert_cmd WHERE id IN (
		   SELECT id FROM monitor_alert_cmd WHERE status <> $1 AND done_time < now() - $2::interval LIMIT $3)`,
		monitorcontract.CmdPending, retention, limit)
	if err != nil {
		return 0, fmt.Errorf("purge alert commands: %w", err)
	}
	return int(tag.RowsAffected()), nil
}
