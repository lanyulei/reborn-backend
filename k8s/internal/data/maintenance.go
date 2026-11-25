package data

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var _ biz.MaintenanceRepo = (*maintenanceRepo)(nil)

// 审计表及其分区命名（与迁移 0072 一致：k8s_audit_YYYY_MM，UTC 月界）。
const (
	auditTable   = "k8s_audit"
	auditDefault = "k8s_audit_default"
	// partitionBoundLayout 分区边界字面量带显式 UTC 偏移：边界是绝对时刻，不随会话 TimeZone 漂移。
	partitionBoundLayout = "2006-01-02 15:04:05+00"
	// partitionLockTimeout 分区 DDL 等锁上限：拿不到锁就放弃，下一轮重试，不长时间阻塞审计写入。
	partitionLockTimeout = "SET LOCAL lock_timeout = '2s'"
)

// partitionPattern 只认本服务命名的月分区；也是拼进 DDL 的标识符白名单。
var partitionPattern = regexp.MustCompile(`^k8s_audit_(\d{4})_(\d{2})$`)

// 行级过期清理的分批参数。
const (
	// purgeBatch 单条 DELETE 至多删除的行数。过期行很多时（刚启用保留期、长期停机后）单条无界 DELETE 是一个
	// 长事务：行锁与 WAL 积压会拖慢同一张表上的写入；分批后每条语句只锁一小段。
	purgeBatch = 5000
	// purgeBudget 一轮 PurgeExpired 用于行级清理的时间预算：删满一批且预算还没用完才发下一条，预算用尽即停
	// （不算错误），没删完的留给下一轮——清理每小时一轮，积压可以慢慢消化，不必一轮删完。
	purgeBudget = time.Minute
	// purgeSessionMargin 删会话比删录制多留的余量（须大于 purgeBudget）。录制和会话是先后两条语句、各自取
	// 当时的 now()：在两者之间刚好越过保留期的会话，录制没被第一句挑中、会话却被第二句删掉，留下永远清不掉
	// 的孤儿录制。分批之后两句之间最长隔一个预算，这个窗口跟着变宽。所以删会话的条件比删录制多退一个余量：
	// 本轮被删掉的会话，它的录制一定在本轮（或更早）已经被删过。落在余量里的会话多留一轮再删。
	purgeSessionMargin = 5 * time.Minute
)

// 过期条件只写一处：探测「有没有过期行」与分批删除用的是同一个条件（与分批之前的单条 DELETE 完全相同；
// 删会话的条件另见 purgeSessionMargin）。deleteExpired 的语句共用前缀参数，DELETE 的最后一个参数是批大小。
const (
	expiredByExpireTime     = `expire_time IS NOT NULL AND expire_time < now()`
	expiredByEndTime        = `end_time IS NOT NULL AND end_time < now() - make_interval(secs => $1)`
	expiredByEndTimePlusGap = `end_time IS NOT NULL AND end_time < now() - make_interval(secs => $1) - make_interval(secs => $2)`

	hasExpiredTokensSQL     = `SELECT EXISTS (SELECT 1 FROM k8s_agent_token WHERE ` + expiredByExpireTime + `)`
	hasExpiredSessionsSQL   = `SELECT EXISTS (SELECT 1 FROM k8s_exec_session WHERE ` + expiredByEndTimePlusGap + `)`
	hasExpiredRecordsSQL    = `SELECT EXISTS (SELECT 1 FROM k8s_exec_record WHERE session_id IN (SELECT id FROM k8s_exec_session WHERE ` + expiredByEndTime + `))`
	purgeExpiredTokensSQL   = `DELETE FROM k8s_agent_token WHERE id IN (SELECT id FROM k8s_agent_token WHERE ` + expiredByExpireTime + ` LIMIT $1)`
	purgeExpiredSessionsSQL = `DELETE FROM k8s_exec_session WHERE id IN (SELECT id FROM k8s_exec_session WHERE ` + expiredByEndTimePlusGap + ` LIMIT $3)`
	purgeExpiredRecordsSQL  = `DELETE FROM k8s_exec_record WHERE (session_id, seq) IN (
	   SELECT session_id, seq FROM k8s_exec_record
	   WHERE session_id IN (SELECT id FROM k8s_exec_session WHERE ` + expiredByEndTime + `) LIMIT $2)`
)

// maintenanceRepo 下线实例清理、审计分区维护与过期数据清理。全部语句幂等，时间取数据库时钟。
type maintenanceRepo struct {
	data *Data
	// batch / budget 行级过期清理的批大小与本轮时间预算，缺省见 purgeBatch / purgeBudget；
	// 集成测试改小它们来覆盖「分多批」「预算用尽」的分支。
	batch  int
	budget time.Duration
}

// NewMaintenanceRepo new a maintenanceRepo.
func NewMaintenanceRepo(data *Data) *maintenanceRepo {
	return &maintenanceRepo{data: data, batch: purgeBatch, budget: purgeBudget}
}

// deleteExpired 分批删除满足过期条件的行。先探一次：没有过期行就不发 DELETE；之后每条 DELETE 至多删一批，
// 直到不足一批（已清完）、本轮时间预算（deadline）用尽或上下文结束。预算用尽不是错误，剩下的留给下一轮；
// 上下文结束原样返回 ctx 的错误。返回已删除的行数，以及过期的行是否已在本轮清完（预算用尽而停下时为 false）。
func (r *maintenanceRepo) deleteExpired(ctx context.Context, what, existsSQL, deleteSQL string, args []any, deadline time.Time) (int64, bool, error) {
	var expired bool
	if err := r.data.Pool.QueryRow(ctx, existsSQL, args...).Scan(&expired); err != nil {
		return 0, false, fmt.Errorf("%s: %w", what, err)
	}
	if !expired {
		return 0, true, nil
	}
	// 批大小不是正数时「不足一批」永远不成立，循环会停不下来：按缺省值处理。
	batch := purgeBatch
	if r.batch > 0 {
		batch = r.batch
	}
	var total int64
	for {
		tag, err := r.data.Pool.Exec(ctx, deleteSQL, append(slices.Clone(args), batch)...)
		if err != nil {
			return total, false, fmt.Errorf("%s: %w", what, err)
		}
		total += tag.RowsAffected()
		if tag.RowsAffected() < int64(batch) { // 不足一批：已清完
			return total, true, nil
		}
		if err := ctx.Err(); err != nil {
			return total, false, err
		}
		if time.Now().After(deadline) {
			return total, false, nil
		}
	}
}

func partitionName(month time.Time) string {
	return fmt.Sprintf("%s_%04d_%02d", auditTable, month.Year(), int(month.Month()))
}

func isDuplicateObject(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42710"
}

// PurgeInstances implements biz.MaintenanceRepo。归属行不在这里清：租约到期后自然可被领取，
// 提前清空会让「数据库抖动导致心跳短暂中断」的实例丢掉仍在有效期内的租约。
func (r *maintenanceRepo) PurgeInstances(ctx context.Context, deadAfter time.Duration) (int, error) {
	tag, err := r.data.Pool.Exec(ctx,
		"DELETE FROM k8s_instance WHERE heartbeat_time < now() - make_interval(secs => $1)", deadAfter.Seconds())
	if err != nil {
		return 0, fmt.Errorf("purge dead instances: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// thisMonth 数据库时钟下当前 UTC 月的第一天。
func (r *maintenanceRepo) thisMonth(ctx context.Context) (time.Time, error) {
	var now time.Time
	if err := r.data.Pool.QueryRow(ctx, "SELECT now()").Scan(&now); err != nil {
		return time.Time{}, fmt.Errorf("read database clock: %w", err)
	}
	now = now.UTC()
	return time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC), nil
}

// EnsureAuditPartitions implements biz.MaintenanceRepo.
func (r *maintenanceRepo) EnsureAuditPartitions(ctx context.Context, ahead int) error {
	month, err := r.thisMonth(ctx)
	if err != nil {
		return err
	}
	var first error
	for i := 0; i <= ahead; i++ {
		if err := r.ensurePartition(ctx, month.AddDate(0, i, 0)); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// ensurePartition 确保某个月的分区存在（做法同 monitor 的判定记录表）：
//  1. 独立建表（LIKE 父表含默认值 / 约束）并加区间 CHECK（ATTACH 时免扫描），属主对齐父表；
//  2. 单短事务（lock_timeout 2s）：锁 DEFAULT 分区 → 迁出其中落在该区间的行 → ATTACH。
//
// 不用 CREATE TABLE … PARTITION OF：它对父表取 ACCESS EXCLUSIVE 并扫描 DEFAULT，会阻塞审计写入。
// 标识符来自固定表名 + 格式化日期，SQL 文本不含任何外部输入。
func (r *maintenanceRepo) ensurePartition(ctx context.Context, month time.Time) error {
	name := partitionName(month)
	var attached bool
	if err := r.data.Pool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_inherits WHERE inhparent = $1::regclass AND inhrelid = to_regclass($2))",
		auditTable, name).Scan(&attached); err != nil {
		return fmt.Errorf("check partition %s: %w", name, err)
	}
	if attached {
		return nil
	}
	lo, hi := month.Format(partitionBoundLayout), month.AddDate(0, 1, 0).Format(partitionBoundLayout)
	if _, err := r.data.Pool.Exec(ctx, fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (LIKE %s INCLUDING DEFAULTS INCLUDING CONSTRAINTS)", name, auditTable)); err != nil {
		return fmt.Errorf("create partition table %s: %w", name, err)
	}
	var owner, parentOwner string
	if err := r.data.Pool.QueryRow(ctx,
		`SELECT pg_get_userbyid(c.relowner), pg_get_userbyid(p.relowner)
		 FROM pg_class c, pg_class p WHERE c.oid = to_regclass($1) AND p.oid = $2::regclass`, name, auditTable).
		Scan(&owner, &parentOwner); err != nil {
		return fmt.Errorf("read owner of %s: %w", name, err)
	}
	if owner != parentOwner {
		if _, err := r.data.Pool.Exec(ctx, fmt.Sprintf("ALTER TABLE %s OWNER TO %s", name, pgx.Identifier{parentOwner}.Sanitize())); err != nil {
			return fmt.Errorf("align owner of %s: %w", name, err)
		}
	}
	if _, err := r.data.Pool.Exec(ctx, fmt.Sprintf(
		"ALTER TABLE %s ADD CONSTRAINT %s_range CHECK (create_time >= '%s' AND create_time < '%s')", name, name, lo, hi)); err != nil &&
		!isDuplicateObject(err) {
		return fmt.Errorf("add range check %s: %w", name, err)
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, partitionLockTimeout); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "LOCK TABLE "+auditDefault+" IN ACCESS EXCLUSIVE MODE"); err != nil {
			return fmt.Errorf("lock default partition: %w", err)
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			"WITH moved AS (DELETE FROM %s WHERE create_time >= '%s' AND create_time < '%s' RETURNING *) INSERT INTO %s SELECT * FROM moved",
			auditDefault, lo, hi, name)); err != nil {
			return fmt.Errorf("move rows out of default partition: %w", err)
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			"ALTER TABLE %s ATTACH PARTITION %s FOR VALUES FROM ('%s') TO ('%s')", auditTable, name, lo, hi)); err != nil {
			return fmt.Errorf("attach partition %s: %w", name, err)
		}
		return nil
	})
}

// PurgeExpired implements biz.MaintenanceRepo：过期的 Agent 注册令牌恒清理；retention > 0 时再清理
// 上界早于保留期的审计月分区（逐个 DROP，短事务 + lock_timeout）与已结束的终端会话记录及其录制。
//
// 行级清理分批进行（deleteExpired）：删除条件、执行顺序与本轮处理的条数口径都不变，只是每条 DELETE 至多删一批，
// 在本轮的时间预算内循环。
func (r *maintenanceRepo) PurgeExpired(ctx context.Context, retention time.Duration) (int, error) {
	deadline := time.Now().Add(r.budget)
	purged, _, err := r.deleteExpired(ctx, "purge expired agent tokens", hasExpiredTokensSQL, purgeExpiredTokensSQL, nil, deadline)
	if err != nil {
		return 0, err
	}
	total := int(purged)
	// 数据面实例异常退出时，它上面的终端会话来不及写结束时间：超过会话最长时长仍「进行中」的记录
	// 不可能还活着，补上结束时间（否则它们永远显示为进行中，也永远不会被保留期清理）。
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE k8s_exec_session SET end_time = now(), end_reason = 'instance_lost'
		 WHERE end_time IS NULL AND start_time < now() - make_interval(secs => $1)`,
		(k8scontract.ExecMaxDuration + time.Hour).Seconds())
	if err != nil {
		return total, fmt.Errorf("reap orphaned exec sessions: %w", err)
	}
	total += int(tag.RowsAffected())
	if retention <= 0 {
		return total, nil
	}
	secs := retention.Seconds()
	// 先删录制再删会话：录制按「所属会话已过期」挑选，会话删掉之后它的录制就找不到了。
	_, recordsDone, err := r.deleteExpired(ctx, "purge exec records", hasExpiredRecordsSQL, purgeExpiredRecordsSQL, []any{secs}, deadline)
	if err != nil {
		return total, err
	}
	// 本轮没来得及把录制清完（时间预算用尽）就不动会话：否则这些会话剩下的录制会成为永远清不掉的孤儿。
	// 留给下一轮——下一轮先把录制清完，再删会话。
	if recordsDone {
		// 会话的过期条件比录制多退 purgeSessionMargin（见该常量的说明）：$1 保留期、$2 余量、$3 批大小。
		if purged, _, err = r.deleteExpired(ctx, "purge exec sessions", hasExpiredSessionsSQL, purgeExpiredSessionsSQL, []any{secs, purgeSessionMargin.Seconds()}, deadline); err != nil {
			return total, err
		}
		total += int(purged)
	}

	var cutoff time.Time
	if err := r.data.Pool.QueryRow(ctx, "SELECT now() - make_interval(secs => $1)", secs).Scan(&cutoff); err != nil {
		return total, fmt.Errorf("read database clock: %w", err)
	}
	// 取模式名与表名两列：regclass 的文本形式在表不在 search_path 上时带模式前缀（或带引号），
	// 按名字匹配就会漏掉，过期分区永远删不掉。
	rows, err := r.data.Pool.Query(ctx,
		`SELECT n.nspname, c.relname FROM pg_inherits i
		 JOIN pg_class c ON c.oid = i.inhrelid JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE i.inhparent = $1::regclass`, auditTable)
	if err != nil {
		return total, fmt.Errorf("list partitions: %w", err)
	}
	type part struct{ schema, rel string }
	parts, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (part, error) {
		var p part
		return p, row.Scan(&p.schema, &p.rel)
	})
	if err != nil {
		return total, fmt.Errorf("list partitions: %w", err)
	}
	for _, pt := range parts {
		name := pgx.Identifier{pt.schema, pt.rel}.Sanitize()
		m := partitionPattern.FindStringSubmatch(pt.rel)
		if m == nil {
			continue
		}
		month, err := time.Parse("2006_01", m[1]+"_"+m[2])
		if err != nil || month.AddDate(0, 1, 0).After(cutoff) {
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
			return total, fmt.Errorf("drop partition %s: %w", name, err)
		}
		total++
	}
	return total, nil
}
