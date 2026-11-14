package data

import (
	"context"
	"errors"
	"fmt"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// 唯一规则的两把类型级事务 advisory lock（均随事务结束释放）：
//
//   - uniqueRuleDefLockSQL  "定义锁"：规则变更与字段废弃 / 设为敏感互斥，保证规则引用的字段始终可用；
//   - uniqueRuleLockSQL     "键锁"：规则变更取排他，cmdb_ci 的触发器（migrations 0049
//     cmdb_ci_unique_sync）取共享——规则变更期间没有并发的资源写入，重算出的键与规则严格对应。
//     锁键表达式须与触发器内的一致。
//
// 取锁顺序固定为 定义锁 → 键锁，避免死锁。
const (
	uniqueRuleDefLockSQL = `SELECT pg_advisory_xact_lock(hashtextextended('cmdb_unique_rule_def:' || $1, 0))`
	uniqueRuleLockSQL    = `SELECT pg_advisory_xact_lock(hashtextextended('cmdb_unique_rule:' || $1, 0))`
)

// uniqueRuleLockTimeout 等锁上限：键锁要等该类型在途的资源写入事务（如一次同步对账）结束，
// 超时即返回 ErrUniqueRuleBusy 让调用方稍后重试，而不是让新的资源写入在其后长时间排队。
// 包级变量：集成测试缩短以验证超时路径。
var uniqueRuleLockTimeout = 10 * time.Second

// lockUniqueRuleDef 事务内取定义锁（字段废弃 / 设为敏感用）。
func lockUniqueRuleDef(ctx context.Context, tx pgx.Tx, modelCode string) error {
	return lockWithTimeout(ctx, tx, modelCode, uniqueRuleDefLockSQL)
}

// lockUniqueRuleChange 事务内取定义锁 + 键锁（规则变更用）。
func lockUniqueRuleChange(ctx context.Context, tx pgx.Tx, modelCode string) error {
	return lockWithTimeout(ctx, tx, modelCode, uniqueRuleDefLockSQL, uniqueRuleLockSQL)
}

func lockWithTimeout(ctx context.Context, tx pgx.Tx, modelCode string, stmts ...string) error {
	// SET LOCAL 不接受绑定参数；取值来自常量，无注入面。
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL lock_timeout = %d", uniqueRuleLockTimeout.Milliseconds())); err != nil {
		return fmt.Errorf("set lock timeout: %w", err)
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(ctx, stmt, modelCode); err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.LockNotAvailable {
				return biz.ErrUniqueRuleBusy
			}
			return fmt.Errorf("lock cmdb unique rule: %w", err)
		}
	}
	// 锁已到手：后续语句（大类型的重算）不受等锁上限约束。
	if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout = DEFAULT"); err != nil {
		return fmt.Errorf("reset lock timeout: %w", err)
	}
	return nil
}

const uniqueRuleColumns = "id, model_code, name, field_codes, create_time, update_time"

func scanUniqueRule(scan func(dest ...any) error) (*biz.UniqueRule, error) {
	var r biz.UniqueRule
	if err := scan(&r.ID, &r.ModelCode, &r.Name, &r.FieldCodes, &r.CreateTime, &r.UpdateTime); err != nil {
		return nil, err
	}
	if r.FieldCodes == nil {
		r.FieldCodes = []string{}
	}
	return &r, nil
}

func (r *fieldRepo) queryUniqueRules(ctx context.Context, where string, args ...any) ([]*biz.UniqueRule, error) {
	rows, err := r.data.Pool.Query(ctx, "SELECT "+uniqueRuleColumns+" FROM cmdb_model_unique_rule WHERE "+where+" ORDER BY name, id", args...)
	if err != nil {
		return nil, fmt.Errorf("list cmdb unique rules: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.UniqueRule, 0, 4)
	for rows.Next() {
		rule, err := scanUniqueRule(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("scan cmdb unique rule: %w", err)
		}
		items = append(items, rule)
	}
	return items, rows.Err()
}

// ListUniqueRules 类型的全部规则（按名称）。
func (r *fieldRepo) ListUniqueRules(ctx context.Context, modelCode string) ([]*biz.UniqueRule, error) {
	return r.queryUniqueRules(ctx, "model_code = $1", modelCode)
}

// ListUniqueRulesByField 引用了该字段的规则。
func (r *fieldRepo) ListUniqueRulesByField(ctx context.Context, modelCode, fieldCode string) ([]*biz.UniqueRule, error) {
	return r.queryUniqueRules(ctx, "model_code = $1 AND $2 = ANY (field_codes)", modelCode, fieldCode)
}

// GetUniqueRule 按类型 + ID 读取规则。
func (r *fieldRepo) GetUniqueRule(ctx context.Context, modelCode, id string) (*biz.UniqueRule, error) {
	row := r.data.Pool.QueryRow(ctx, "SELECT "+uniqueRuleColumns+" FROM cmdb_model_unique_rule WHERE model_code = $1 AND id = $2", modelCode, id)
	rule, err := scanUniqueRule(row.Scan)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrUniqueRuleNotFound
		}
		return nil, fmt.Errorf("get cmdb unique rule: %w", err)
	}
	return rule, nil
}

// stmtUniqueFieldsUsable 规则字段中当前可用（存在、未废弃、非敏感）的个数；锁内复核用。
const stmtUniqueFieldsUsable = `
SELECT count(*) FROM cmdb_model_field
WHERE model_code = $1 AND code = ANY ($2::text[]) AND NOT deprecated AND NOT sensitive`

// stmtUniqueConflictGroups 存量资源在字段组上取值重复的组（未删除、键非空）。
const stmtUniqueConflictGroups = `
SELECT count(*) OVER (), g.n, g.names
FROM (
    SELECT count(*) AS n, (array_agg(c.name ORDER BY c.name))[1:$4] AS names
    FROM cmdb_ci c
    CROSS JOIN LATERAL (SELECT cmdb_unique_key(c.attrs, $2::text[]) AS key) k
    WHERE c.model_code = $1 AND c.delete_time IS NULL AND k.key IS NOT NULL
    GROUP BY k.key
    HAVING count(*) > 1
) g
ORDER BY g.n DESC, g.names
LIMIT $3`

// stmtUniqueRekey 为规则下的全部未删除资源建键（规则新建 / 字段组变化后；此前该规则无键）。
const stmtUniqueRekey = `
INSERT INTO cmdb_ci_unique (ci_id, rule_id, key)
SELECT c.id, $2::uuid, k.key
FROM cmdb_ci c
CROSS JOIN LATERAL (SELECT cmdb_unique_key(c.attrs, $3::text[]) AS key) k
WHERE c.model_code = $1 AND c.delete_time IS NULL AND k.key IS NOT NULL`

// stmtSyncUniqueFlags 字段的 unique 派生标记 = 被任一规则引用（只改有变化的行）。
const stmtSyncUniqueFlags = `
UPDATE cmdb_model_field f SET "unique" = u.used
FROM (
    SELECT f2.id, EXISTS (
        SELECT 1 FROM cmdb_model_unique_rule r WHERE r.model_code = f2.model_code AND f2.code = ANY (r.field_codes)) AS used
    FROM cmdb_model_field f2 WHERE f2.model_code = $1
) u
WHERE f.id = u.id AND f."unique" <> u.used`

// conflictGroups 查询重复组：返回组数与至多 limit 组样例（每组至多 SampleMax 个名称）。
func conflictGroups(ctx context.Context, q interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}, modelCode string, fieldCodes []string, limit int,
) (int64, []biz.UniqueConflict, error) {
	rows, err := q.Query(ctx, stmtUniqueConflictGroups, modelCode, dbx.TextArray(fieldCodes), limit, biz.UniqueConflictSampleMax)
	if err != nil {
		return 0, nil, fmt.Errorf("check cmdb unique conflicts: %w", err)
	}
	defer rows.Close()
	var total int64
	samples := make([]biz.UniqueConflict, 0, limit)
	for rows.Next() {
		var c biz.UniqueConflict
		if err := rows.Scan(&total, &c.Count, &c.Names); err != nil {
			return 0, nil, fmt.Errorf("scan cmdb unique conflict: %w", err)
		}
		samples = append(samples, c)
	}
	return total, samples, rows.Err()
}

// rekeyUniqueRule 锁内：先数重复（给出可读的冲突规模），无重复再建键。
func rekeyUniqueRule(ctx context.Context, tx pgx.Tx, rule *biz.UniqueRule) error {
	groups, _, err := conflictGroups(ctx, tx, rule.ModelCode, rule.FieldCodes, 1)
	if err != nil {
		return err
	}
	if groups > 0 {
		return biz.UniqueRuleConflictError(groups)
	}
	if _, err := tx.Exec(ctx, stmtUniqueRekey, rule.ModelCode, rule.ID, dbx.TextArray(rule.FieldCodes)); err != nil {
		// 持有键锁时不应再出现重复；仍映射为业务错误而非 500。
		if isUniqueViolation(err, "uq_cmdb_ci_unique_rule_key") {
			return biz.ErrUniqueRuleConflict
		}
		return fmt.Errorf("rekey cmdb unique rule: %w", err)
	}
	return nil
}

// checkUniqueFieldsUsable 锁内复核规则字段仍可用（防与并发的字段废弃 / 敏感标记竞争）。
func checkUniqueFieldsUsable(ctx context.Context, tx pgx.Tx, rule *biz.UniqueRule) error {
	var usable int
	if err := tx.QueryRow(ctx, stmtUniqueFieldsUsable, rule.ModelCode, dbx.TextArray(rule.FieldCodes)).Scan(&usable); err != nil {
		return fmt.Errorf("check cmdb unique rule fields: %w", err)
	}
	if usable != len(rule.FieldCodes) {
		return biz.UniqueRuleFieldsChangedError()
	}
	return nil
}

func mapUniqueRuleWriteError(err error, op string) error {
	switch {
	case isUniqueViolation(err, "uq_cmdb_model_unique_rule_name"), isUniqueViolation(err, "uq_cmdb_model_unique_rule_fields"):
		return biz.ErrUniqueRuleExists
	case isForeignKeyViolation(err):
		return biz.ErrModelNotFound
	default:
		return fmt.Errorf("%s cmdb unique rule: %w", op, err)
	}
}

func syncUniqueFlags(ctx context.Context, tx pgx.Tx, modelCode string) error {
	if _, err := tx.Exec(ctx, stmtSyncUniqueFlags, modelCode); err != nil {
		return fmt.Errorf("sync cmdb field unique flags: %w", err)
	}
	return nil
}

// CreateUniqueRule 新增规则并为存量资源建键（一事务，锁内）。
func (r *fieldRepo) CreateUniqueRule(ctx context.Context, rule *biz.UniqueRule, maxPerModel int) error {
	r.cache.invalidate(rule.ModelCode)
	defer r.cache.invalidate(rule.ModelCode) // 写后再失效：字段的 unique 派生标记随之变化
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if err := lockUniqueRuleChange(ctx, tx, rule.ModelCode); err != nil {
			return err
		}
		var count int
		if err := tx.QueryRow(ctx, "SELECT count(*) FROM cmdb_model_unique_rule WHERE model_code = $1", rule.ModelCode).Scan(&count); err != nil {
			return fmt.Errorf("count cmdb unique rules: %w", err)
		}
		if count >= maxPerModel {
			return biz.UniqueRuleLimitError(maxPerModel)
		}
		if err := checkUniqueFieldsUsable(ctx, tx, rule); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO cmdb_model_unique_rule (id, model_code, name, field_codes, create_time, update_time)
			 VALUES ($1, $2, $3, $4, $5, $6)`,
			rule.ID, rule.ModelCode, rule.Name, dbx.TextArray(rule.FieldCodes), rule.CreateTime, rule.UpdateTime); err != nil {
			return mapUniqueRuleWriteError(err, "insert")
		}
		if err := rekeyUniqueRule(ctx, tx, rule); err != nil {
			return err
		}
		return syncUniqueFlags(ctx, tx, rule.ModelCode)
	})
}

// UpdateUniqueRule 更新名称；rekey 为真时一并替换字段组，并在锁内清空、重算该规则的全部键。
// 仅改名不涉及键：不取锁，也不写 field_codes——调用方手里的字段组可能已被并发的字段组
// 变更取代，写回去会让规则与已有的键脱节。
func (r *fieldRepo) UpdateUniqueRule(ctx context.Context, rule *biz.UniqueRule, rename, rekey bool) error {
	r.cache.invalidate(rule.ModelCode)
	defer r.cache.invalidate(rule.ModelCode)
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if !rekey {
			tag, err := tx.Exec(ctx,
				`UPDATE cmdb_model_unique_rule SET name = $3, update_time = $4 WHERE model_code = $1 AND id = $2`,
				rule.ModelCode, rule.ID, rule.Name, rule.UpdateTime)
			if err != nil {
				return mapUniqueRuleWriteError(err, "update")
			}
			if tag.RowsAffected() == 0 {
				return biz.ErrUniqueRuleNotFound
			}
			return nil
		}
		if err := lockUniqueRuleChange(ctx, tx, rule.ModelCode); err != nil {
			return err
		}
		if err := checkUniqueFieldsUsable(ctx, tx, rule); err != nil {
			return err
		}
		var newName *string
		if rename {
			newName = &rule.Name
		}
		tag, err := tx.Exec(ctx,
			// 未要求改名时保留库中的名称（COALESCE）：并发的改名不被这里手里的旧名称覆盖。
			`UPDATE cmdb_model_unique_rule SET name = COALESCE($3, name), field_codes = $4, update_time = $5 WHERE model_code = $1 AND id = $2`,
			rule.ModelCode, rule.ID, newName, dbx.TextArray(rule.FieldCodes), rule.UpdateTime)
		if err != nil {
			return mapUniqueRuleWriteError(err, "update")
		}
		if tag.RowsAffected() == 0 {
			return biz.ErrUniqueRuleNotFound
		}
		if _, err := tx.Exec(ctx, "DELETE FROM cmdb_ci_unique WHERE rule_id = $1", rule.ID); err != nil {
			return fmt.Errorf("clear cmdb unique keys: %w", err)
		}
		if err := rekeyUniqueRule(ctx, tx, rule); err != nil {
			return err
		}
		return syncUniqueFlags(ctx, tx, rule.ModelCode)
	})
}

// DeleteUniqueRule 删除规则（键随外键级联删除）。
func (r *fieldRepo) DeleteUniqueRule(ctx context.Context, modelCode, id string) error {
	r.cache.invalidate(modelCode)
	defer r.cache.invalidate(modelCode)
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if err := lockUniqueRuleChange(ctx, tx, modelCode); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, "DELETE FROM cmdb_model_unique_rule WHERE model_code = $1 AND id = $2", modelCode, id)
		if err != nil {
			return fmt.Errorf("delete cmdb unique rule: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return biz.ErrUniqueRuleNotFound
		}
		return syncUniqueFlags(ctx, tx, modelCode)
	})
}

// UniqueConflicts 存量资源在字段组上的重复（只读，不取锁）。
func (r *fieldRepo) UniqueConflicts(ctx context.Context, modelCode string, fieldCodes []string, limit int) (int64, []biz.UniqueConflict, error) {
	return conflictGroups(ctx, r.data.Pool, modelCode, fieldCodes, limit)
}

// uniqueRuleRepo 把 fieldRepo 的规则方法适配为 biz.UniqueRuleRepo（两个接口的方法名有重叠：
// Create / Get / Update / ListByModel，故以独立类型暴露；共享 fieldRepo 的字段缓存失效）。
type uniqueRuleRepo struct{ fields *fieldRepo }

var _ biz.UniqueRuleRepo = uniqueRuleRepo{}

// NewUniqueRuleRepo new a uniqueRuleRepo.
func NewUniqueRuleRepo(fields *fieldRepo) uniqueRuleRepo { return uniqueRuleRepo{fields: fields} }

func (u uniqueRuleRepo) ListByModel(ctx context.Context, modelCode string) ([]*biz.UniqueRule, error) {
	return u.fields.ListUniqueRules(ctx, modelCode)
}

func (u uniqueRuleRepo) Get(ctx context.Context, modelCode, id string) (*biz.UniqueRule, error) {
	return u.fields.GetUniqueRule(ctx, modelCode, id)
}

func (u uniqueRuleRepo) Create(ctx context.Context, rule *biz.UniqueRule, maxPerModel int) error {
	return u.fields.CreateUniqueRule(ctx, rule, maxPerModel)
}

func (u uniqueRuleRepo) Update(ctx context.Context, rule *biz.UniqueRule, rename, rekey bool) error {
	return u.fields.UpdateUniqueRule(ctx, rule, rename, rekey)
}

func (u uniqueRuleRepo) Delete(ctx context.Context, modelCode, id string) error {
	return u.fields.DeleteUniqueRule(ctx, modelCode, id)
}

func (u uniqueRuleRepo) Conflicts(ctx context.Context, modelCode string, fieldCodes []string, limit int) (int64, []biz.UniqueConflict, error) {
	return u.fields.UniqueConflicts(ctx, modelCode, fieldCodes, limit)
}

func (u uniqueRuleRepo) ListByField(ctx context.Context, modelCode, fieldCode string) ([]*biz.UniqueRule, error) {
	return u.fields.ListUniqueRulesByField(ctx, modelCode, fieldCode)
}
