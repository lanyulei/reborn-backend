package data

import (
	"context"
	"fmt"

	"github.com/lanyulei/reborn/backend/cmdb/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/cmdbcontract"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

var _ biz.ReconcileRepo = (*reconcileRepo)(nil)

// reconcileRepo 集合 SQL 对账：全部语句以 (run_id, model_code, source) 为范围，参数绑定，
// 动态字段名（人工字段、唯一字段）一律以 text[] 实参经 jsonb 运算符处理，无 SQL 拼接。
type reconcileRepo struct {
	data *Data
}

// NewReconcileRepo new a reconcileRepo.
func NewReconcileRepo(data *Data) *reconcileRepo { return &reconcileRepo{data: data} }

// 历史 request_id 以运行 ID 标记（core 的 X-Request-Id 位置），便于按运行追溯。

// 唯一约束（migrations 0049）：规则存 cmdb_model_unique_rule，资源的键存 cmdb_ci_unique，由
// cmdb_ci 的触发器按 cmdb_unique_key(attrs, 规则字段组) 维护。对账的集合语句须在写入前把会
// 违反规则的行剔除（否则一行冲突整条语句失败），因此这里用同一个数据库函数为"写入后的属性"
// 预先算出各规则下的键，再与 cmdb_ci_unique 及同批其它行比对。
//
// 一致性：ReconcileModel 事务开头取类型级共享 advisory lock（uniqueRuleLockShared，与触发器、
// core 的规则变更同一把锁）——事务内规则集不变，预判与触发器实际写入的口径严格一致。
// 并发的人工写入仍可能在预判之后抢占同一个键：此时触发器的唯一索引兜底、整个类型的对账
// 事务失败并由任务重试，不会写入重复数据。
//
// 写法约束（性能）：暂存表在对账时通常没有统计信息，规划器会把按 run_id / model_code 过滤
// 后的行数估成 1。因此：
//   - 与 cmdb_ci 按 external_id 关联时一律带上 `c.external_id <> ''`——命中部分索引
//     idx_cmdb_ci_external 的谓词，嵌套循环也只是逐行索引探测（否则退化为逐行扫全类型）；
//   - 不做"中间结果 × 中间结果"的关联（估算为 1 行 → 嵌套循环 → 平方复杂度），同批取舍
//     用窗口函数在一次扫描内完成；
//   - 对账前由 worker 调用 StagingRepo.RefreshStats（ANALYZE 暂存表）让行数估算回到真实量级。

// uniqueRuleLockShared 类型级共享锁；锁键表达式须与 migrations 0049 的触发器一致。
const uniqueRuleLockShared = `SELECT pg_advisory_xact_lock_shared(hashtextextended('cmdb_unique_rule:' || $1, 0))`

// cteRules 该类型的唯一规则（$2 = model_code）。
const cteRules = `rules AS (
    SELECT id, field_codes FROM cmdb_model_unique_rule WHERE model_code = $2
)`

// stmtTouch 命中的资源刷新同步时间、清陈旧与消失标记（不改 version：无业务变化）。
const stmtTouch = `
UPDATE cmdb_ci c SET last_synced_at = now(), stale = false, missing_since = NULL
FROM cmdb_sync_staging s
WHERE s.run_id = $1 AND s.model_code = $2 AND c.model_code = $2 AND c.source = $3 AND c.external_id = s.external_id
  AND c.external_id <> '' AND c.delete_time IS NULL`

// stmtUpdate 变化的资源：名称 / 属性（人工字段不覆盖）/ 退役→重新激活，version+1，历史只记差异键。
// 名称或任一唯一规则与其它资源冲突的行跳过（计入失败样本）。冲突按"写入后的属性"判定，分两步：
//  1. cand：名称被库中其它资源占用、或写入后的键与库中其它资源的现有键相同的行剔除。
//     对方若也在本批被改走了该键（链式：A 要 B 的旧值、B 改成新值），本次保守跳过、下次同步
//     自愈；但两条资源互换取值（A 要 B 的、B 要 A 的，或更长的环）每次都互相挡住，不会自愈，
//     持续记入失败样本，需人工先改掉其中一条（改动前的实现同样如此）。
//  2. ranked / changed：其余行之间，写入后键相同 / 名称相同的按 id 取其一。
//
// 第 2 步只在通过第 1 步的行之间比较——被库中资源挡住的行本就不会写入，不能再挡住别的行
// （否则只要它一直被挡，后者就永远写不进去）。同批取舍是保守的：个别行可能要到下一次同步
// 才写入，但每次至少推进一条，必然收敛。
//
// ranked 把每行按规则展开（无规则时保留一行、键为空），key_rn 为该键在本批内的名次、
// name_rk 为该名称的名次；changed 取每行最差的名次，全为 1 才写入。
const stmtUpdate = `
WITH ` + cteRules + `, s AS (
    SELECT external_id, name, attrs FROM cmdb_sync_staging WHERE run_id = $1 AND model_code = $2
), cand AS (
    SELECT c.id, c.name AS old_name, s.name AS new_name, c.status AS old_status,
           CASE WHEN c.status = $6 THEN $7 ELSE c.status END AS new_status,
           c.attrs AS old_attrs, n.new_attrs
    FROM cmdb_ci c JOIN s ON s.external_id = c.external_id
    CROSS JOIN LATERAL (SELECT c.attrs || (s.attrs - $4::text[]) AS new_attrs) n
    WHERE c.model_code = $2 AND c.source = $3 AND c.external_id <> '' AND c.delete_time IS NULL
      AND (c.name <> s.name OR c.status = $6 OR c.attrs <> n.new_attrs)
      AND NOT EXISTS (
          SELECT 1 FROM cmdb_ci o WHERE o.model_code = $2 AND o.id <> c.id AND o.delete_time IS NULL AND o.name = s.name)
      AND NOT EXISTS (
          SELECT 1 FROM rules r JOIN cmdb_ci_unique u ON u.rule_id = r.id AND u.key = cmdb_unique_key(n.new_attrs, r.field_codes)
          WHERE u.ci_id <> c.id)
), ranked AS (
    SELECT ca.*,
           CASE WHEN k.key IS NULL THEN 1
                ELSE row_number() OVER (PARTITION BY r.id, k.key ORDER BY ca.id) END AS key_rn,
           dense_rank() OVER (PARTITION BY ca.new_name ORDER BY ca.id) AS name_rk
    FROM cand ca
    LEFT JOIN rules r ON true
    LEFT JOIN LATERAL (SELECT cmdb_unique_key(ca.new_attrs, r.field_codes) AS key) k ON true
), changed AS (
    SELECT w.* FROM (
        SELECT DISTINCT ON (rk.id) rk.* FROM ranked rk ORDER BY rk.id, greatest(rk.key_rn, rk.name_rk) DESC
    ) w WHERE w.key_rn = 1 AND w.name_rk = 1
), diffs AS (
    SELECT ch.*, d.before_attrs, d.after_attrs
    FROM changed ch
    CROSS JOIN LATERAL (
        SELECT jsonb_object_agg(k, COALESCE(ch.old_attrs -> k, 'null'::jsonb)) AS before_attrs,
               jsonb_object_agg(k, COALESCE(ch.new_attrs -> k, 'null'::jsonb)) AS after_attrs
        FROM jsonb_object_keys(ch.old_attrs || ch.new_attrs) AS k
        WHERE ch.old_attrs -> k IS DISTINCT FROM ch.new_attrs -> k
    ) d
), upd AS (
    UPDATE cmdb_ci c SET name = df.new_name, status = df.new_status, attrs = df.new_attrs,
        version = c.version + 1, update_time = now(), update_user = NULL, last_synced_at = now(), stale = false, missing_since = NULL
    FROM diffs df WHERE c.id = df.id
    RETURNING c.id
), hist AS (
    INSERT INTO cmdb_ci_history (id, ci_id, action, diff, operator, source, request_id, create_time)
    SELECT gen_random_uuid(), df.id, CASE WHEN df.old_status <> df.new_status THEN $8 ELSE $9 END,
        jsonb_build_object(
            'before', '{}'::jsonb
                || CASE WHEN df.old_name <> df.new_name THEN jsonb_build_object('name', df.old_name) ELSE '{}'::jsonb END
                || CASE WHEN df.old_status <> df.new_status THEN jsonb_build_object('status', df.old_status) ELSE '{}'::jsonb END
                || CASE WHEN df.before_attrs IS NOT NULL THEN jsonb_build_object('attrs', df.before_attrs) ELSE '{}'::jsonb END,
            'after', '{}'::jsonb
                || CASE WHEN df.old_name <> df.new_name THEN jsonb_build_object('name', df.new_name) ELSE '{}'::jsonb END
                || CASE WHEN df.old_status <> df.new_status THEN jsonb_build_object('status', df.new_status) ELSE '{}'::jsonb END
                || CASE WHEN df.after_attrs IS NOT NULL THEN jsonb_build_object('attrs', df.after_attrs) ELSE '{}'::jsonb END),
        '', $3, $5, now()
    FROM diffs df
)
SELECT count(*) FROM upd`

// stmtInsert 新资源：外部标识未命中、且名称 / 任一唯一规则不与现有资源或本批其它行冲突的行。
// 键按实际写入的属性（去掉人工归属字段）计算。与 stmtUpdate 同样分两步：cand 先剔除被库中
// 资源占用了名称 / 键的行，再在其余行之间按 external_id 取舍同名（by_name）、同键（ranked / ok）者。
const stmtInsert = `
WITH ` + cteRules + `, cand AS (
    SELECT s.external_id, s.name, a.attrs
    FROM cmdb_sync_staging s
    CROSS JOIN LATERAL (SELECT s.attrs - $4::text[] AS attrs) a
    WHERE s.run_id = $1 AND s.model_code = $2
      AND NOT EXISTS (SELECT 1 FROM cmdb_ci c WHERE c.model_code = $2 AND c.source = $3 AND c.external_id = s.external_id
                        AND c.external_id <> '' AND c.delete_time IS NULL)
      AND NOT EXISTS (SELECT 1 FROM cmdb_ci c WHERE c.model_code = $2 AND c.delete_time IS NULL AND c.name = s.name)
      AND NOT EXISTS (
          SELECT 1 FROM rules r JOIN cmdb_ci_unique u ON u.rule_id = r.id AND u.key = cmdb_unique_key(a.attrs, r.field_codes))
), by_name AS (
    SELECT DISTINCT ON (name) * FROM cand ORDER BY name, external_id
), ranked AS (
    SELECT bn.*,
           CASE WHEN k.key IS NULL THEN 1
                ELSE row_number() OVER (PARTITION BY r.id, k.key ORDER BY bn.external_id) END AS key_rn
    FROM by_name bn
    LEFT JOIN rules r ON true
    LEFT JOIN LATERAL (SELECT cmdb_unique_key(bn.attrs, r.field_codes) AS key) k ON true
), ok AS (
    SELECT w.* FROM (
        SELECT DISTINCT ON (rk.external_id) rk.* FROM ranked rk ORDER BY rk.external_id, rk.key_rn DESC
    ) w WHERE w.key_rn = 1
), ins AS (
    INSERT INTO cmdb_ci (id, model_code, name, status, env, source, external_id, attrs, last_synced_at, stale, version, create_time, update_time)
    SELECT gen_random_uuid(), $2, ok.name, $7, $6, $3, ok.external_id, ok.attrs, now(), false, 1, now(), now() FROM ok
    RETURNING id, name, env, external_id, attrs
), hist AS (
    INSERT INTO cmdb_ci_history (id, ci_id, action, diff, operator, source, request_id, create_time)
    SELECT gen_random_uuid(), ins.id, $8,
        jsonb_build_object('before', '{}'::jsonb, 'after', jsonb_build_object('name', ins.name, 'status', $7, 'env', ins.env, 'externalId', ins.external_id, 'attrs', ins.attrs)),
        '', $3, $5, now()
    FROM ins
)
SELECT count(*) FROM ins`

// stmtFailedSamples 对账后未生效的暂存行，带原因；matched 标记该行是否命中了既有资源
// （被跳过的更新），用于把 Unchanged 从命中数中扣除。在更新 / 新建之后执行：
//   - 未命中既有资源 = 新建被跳过；
//   - 命中但库中的名称或属性仍与暂存不一致 = 更新被跳过（$4 = 人工归属字段，同步不写）。
//
// pending 物化：want_attrs 成为普通列，下面按键探测 cmdb_ci_unique 才能走 (rule_id, key) 索引
// （内联时它是外连接可空侧的表达式，无法作为索引条件）。
// 原因按 违反唯一规则（指明规则）→ 名称占用 → 本批内重复 的顺序给出（同批取舍中落选的行，
// 胜出者此时已落库，原因即"被另一资源占用"）。
const stmtFailedSamples = `
WITH ` + cteRules + `, pending AS MATERIALIZED (
    SELECT s.external_id, s.name, c.id AS ci_id,
           CASE WHEN c.id IS NULL THEN s.attrs - $4::text[] ELSE c.attrs || (s.attrs - $4::text[]) END AS want_attrs
    FROM cmdb_sync_staging s
    LEFT JOIN cmdb_ci c ON c.model_code = $2 AND c.source = $3 AND c.external_id = s.external_id
        AND c.external_id <> '' AND c.delete_time IS NULL
    WHERE s.run_id = $1 AND s.model_code = $2
      AND (c.id IS NULL OR c.name <> s.name OR c.attrs <> (c.attrs || (s.attrs - $4::text[])))
)
SELECT p.external_id, p.name, p.ci_id IS NOT NULL AS matched,
    CASE WHEN hit.rule_name IS NOT NULL
             THEN 'unique rule "' || hit.rule_name || '" violated: another resource has the same ' || hit.fields
         WHEN EXISTS (SELECT 1 FROM cmdb_ci o WHERE o.model_code = $2 AND o.delete_time IS NULL AND o.name = p.name
                        AND o.id IS DISTINCT FROM p.ci_id)
             THEN 'name already used by another resource'
         ELSE 'duplicate name or unique values within this run' END
FROM pending p
LEFT JOIN LATERAL (
    SELECT ur.name AS rule_name, array_to_string(ur.field_codes, ' + ') AS fields
    FROM rules r
    JOIN cmdb_model_unique_rule ur ON ur.id = r.id
    JOIN cmdb_ci_unique u ON u.rule_id = r.id AND u.key = cmdb_unique_key(p.want_attrs, r.field_codes)
    WHERE u.ci_id IS DISTINCT FROM p.ci_id
    ORDER BY ur.name LIMIT 1
) hit ON true
ORDER BY p.external_id`

// conflictStoreLimit 单类型单次对账留存的冲突上限。
const conflictStoreLimit = 2000

// stmtSyncConflicts 整体刷新某来源某类型的冲突清单（与对账同事务）：本次仍冲突的行写入 / 更新
// （首次出现时间保留），不在本次清单里的移除（已解决或不再被采集到）。
const stmtSyncConflicts = `
WITH cur AS (
    SELECT * FROM unnest($3::text[], $4::text[], $5::text[]) AS t(external_id, name, reason)
), gone AS (
    DELETE FROM cmdb_sync_conflict c
    WHERE c.source = $1 AND c.model_code = $2
      AND NOT EXISTS (SELECT 1 FROM cur WHERE cur.external_id = c.external_id)
)
INSERT INTO cmdb_sync_conflict (source, model_code, external_id, name, reason)
SELECT $1, $2, cur.external_id, left(cur.name, 128), left(cur.reason, 512) FROM cur
ON CONFLICT (source, model_code, external_id)
DO UPDATE SET name = EXCLUDED.name, reason = EXCLUDED.reason, last_seen = now()`

// stmtDisappear 本源本类型中本次未出现的资源：首次标记消失时间（已标记的保留原时间）。
const stmtDisappear = `
UPDATE cmdb_ci c SET missing_since = now()
WHERE c.model_code = $2 AND c.source = $3 AND c.delete_time IS NULL AND c.missing_since IS NULL AND c.status <> $4
  AND NOT EXISTS (SELECT 1 FROM cmdb_sync_staging s WHERE s.run_id = $1 AND s.model_code = $2 AND s.external_id = c.external_id)`

// stmtRetire 消失超过宽限期的资源退役并记历史。
const stmtRetire = `
WITH r AS (
    SELECT id, status FROM cmdb_ci
    WHERE model_code = $1 AND source = $2 AND delete_time IS NULL AND status <> $4 AND missing_since IS NOT NULL AND missing_since <= now() - $3::interval
), upd AS (
    UPDATE cmdb_ci c SET status = $4, version = c.version + 1, update_time = now(), update_user = NULL FROM r WHERE c.id = r.id RETURNING c.id
), hist AS (
    INSERT INTO cmdb_ci_history (id, ci_id, action, diff, operator, source, request_id, create_time)
    SELECT gen_random_uuid(), r.id, $6, jsonb_build_object('before', jsonb_build_object('status', r.status), 'after', jsonb_build_object('status', $4::text)), '', $2, $5, now()
    FROM r
)
SELECT id FROM upd`

// stmtCascadeRetire 级联退役：刚退役资源作为 src 的 cascade_retire 关系，其 dst 未退役者一并退役
// （与 core 人工退役的级联语义一致；多级由调用方循环，上限 cascadeMaxDepth）。
const stmtCascadeRetire = `
WITH dep AS (
    SELECT DISTINCT d.id, d.status
    FROM cmdb_relation r
    JOIN cmdb_relation_type t ON t.code = r.type_code AND t.on_delete = $4 AND t.delete_time IS NULL
    JOIN cmdb_ci d ON d.id = r.dst_id AND d.delete_time IS NULL AND d.status <> $2
    WHERE r.src_id = ANY($1::uuid[])
), upd AS (
    UPDATE cmdb_ci c SET status = $2, version = c.version + 1, update_time = now(), update_user = NULL FROM dep WHERE c.id = dep.id RETURNING c.id
), hist AS (
    INSERT INTO cmdb_ci_history (id, ci_id, action, diff, operator, source, request_id, create_time)
    SELECT gen_random_uuid(), dep.id, $5, jsonb_build_object('before', jsonb_build_object('status', dep.status), 'after', jsonb_build_object('status', $2::text)), '', $3, $6, now()
    FROM dep
)
SELECT id FROM upd`

// cascadeMaxDepth 级联退役最大层数（防环）。
const cascadeMaxDepth = 8

// collectIDs 读取单列 id 结果集。
func collectIDs(rows pgx.Rows, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := make([]string, 0, 8)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// clampInt32 行数计数落 int32（统计列类型），超界封顶。
func clampInt32(n int64) int32 {
	if n > int64(^uint32(0)>>1) {
		return int32(^uint32(0) >> 1)
	}
	return int32(n) //nolint:gosec // 上界已在前一分支封顶
}

// ReconcileModel implements biz.ReconcileRepo（一类型一事务）。
func (r *reconcileRepo) ReconcileModel(ctx context.Context, p biz.ReconcileParams) (biz.Counts, []cmdbcontract.RunError, error) {
	var counts biz.Counts
	var samples []cmdbcontract.RunError
	err := dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		manual := dbx.TextArray(p.ManualKeys)
		// 与唯一规则变更互斥：事务内规则集不变（冲突预判与触发器实际写入同口径）。
		if _, err := tx.Exec(ctx, uniqueRuleLockShared, p.ModelCode); err != nil {
			return fmt.Errorf("lock unique rules: %w", err)
		}
		touched, err := tx.Exec(ctx, stmtTouch, p.RunID, p.ModelCode, p.Source)
		if err != nil {
			return fmt.Errorf("touch matched: %w", err)
		}
		var updated int64
		if err := tx.QueryRow(ctx, stmtUpdate, p.RunID, p.ModelCode, p.Source, manual, p.RunID,
			cmdbcontract.CIStatusRetired, cmdbcontract.CIStatusActive, cmdbcontract.ActionTransition, cmdbcontract.ActionUpdate).Scan(&updated); err != nil {
			return fmt.Errorf("update changed: %w", err)
		}
		var created int64
		if err := tx.QueryRow(ctx, stmtInsert, p.RunID, p.ModelCode, p.Source, manual, p.RunID, p.Env,
			cmdbcontract.CIStatusActive, cmdbcontract.ActionCreate).Scan(&created); err != nil {
			return fmt.Errorf("insert new: %w", err)
		}
		rows, err := tx.Query(ctx, stmtFailedSamples, p.RunID, p.ModelCode, p.Source, manual)
		if err != nil {
			return fmt.Errorf("failed samples: %w", err)
		}
		var failed, skipped int32
		// 全部未生效的行（不只样例）留存到冲突表；上限保护内存与单条语句的参数体积。
		var cEIDs, cNames, cReasons []string
		for rows.Next() {
			var e cmdbcontract.RunError
			var matched bool
			if err := rows.Scan(&e.ExternalID, &e.Name, &matched, &e.Reason); err != nil {
				rows.Close()
				return err
			}
			e.ModelCode = p.ModelCode
			failed++
			if matched {
				skipped++
			}
			if len(samples) < cmdbcontract.RunErrorSampleLimit {
				samples = append(samples, e)
			}
			if len(cEIDs) < conflictStoreLimit {
				cEIDs, cNames, cReasons = append(cEIDs, e.ExternalID), append(cNames, e.Name), append(cReasons, e.Reason)
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, stmtSyncConflicts, p.Source, p.ModelCode, dbx.TextArray(cEIDs), dbx.TextArray(cNames), dbx.TextArray(cReasons)); err != nil {
			return fmt.Errorf("store conflicts: %w", err)
		}
		counts.Updated, counts.Created, counts.Failed = clampInt32(updated), clampInt32(created), failed
		// touched 含被更新与被跳过（冲突）的行；无变化 = 命中 - 更新 - 跳过。
		counts.Unchanged = max(clampInt32(touched.RowsAffected())-counts.Updated-skipped, 0)
		if p.SkipDisappear {
			return nil
		}
		gone, err := tx.Exec(ctx, stmtDisappear, p.RunID, p.ModelCode, p.Source, cmdbcontract.CIStatusRetired)
		if err != nil {
			return fmt.Errorf("mark disappeared: %w", err)
		}
		counts.Disappeared = clampInt32(gone.RowsAffected())
		retiredIDs, err := collectIDs(tx.Query(ctx, stmtRetire, p.ModelCode, p.Source, p.Grace, cmdbcontract.CIStatusRetired, p.RunID, cmdbcontract.ActionTransition))
		if err != nil {
			return fmt.Errorf("retire: %w", err)
		}
		counts.Retired = clampInt32(int64(len(retiredIDs)))
		// 级联退役（cascade_retire 关系的下游），逐层直到无新增。
		frontier := retiredIDs
		for level := 0; level < cascadeMaxDepth && len(frontier) > 0; level++ {
			next, err := collectIDs(tx.Query(ctx, stmtCascadeRetire, dbx.TextArray(frontier), cmdbcontract.CIStatusRetired, p.Source,
				cmdbcontract.OnDeleteCascadeRetire, cmdbcontract.ActionTransition, p.RunID))
			if err != nil {
				return fmt.Errorf("cascade retire: %w", err)
			}
			counts.Retired += clampInt32(int64(len(next)))
			frontier = next
		}
		return nil
	})
	return counts, samples, err
}

// HasActive implements biz.ReconcileRepo。
func (r *reconcileRepo) HasActive(ctx context.Context, modelCode, source string) (bool, error) {
	var active bool
	if err := r.data.Pool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM cmdb_ci WHERE model_code = $1 AND source = $2 AND delete_time IS NULL AND status <> $3)",
		modelCode, source, cmdbcontract.CIStatusRetired).Scan(&active); err != nil {
		return false, fmt.Errorf("check active resources: %w", err)
	}
	return active, nil
}

// stmtRelationsCTE 解析暂存声明的关系为 (type, src, dst, me, direction)，两端须为同源同步资源。
const stmtRelationsCTE = `
WITH decl AS (
    SELECT s.model_code, s.external_id, rel ->> 'type' AS type_code, rel ->> 'direction' AS direction,
           rel ->> 'peer_model' AS peer_model, rel ->> 'peer_external_id' AS peer_eid
    FROM cmdb_sync_staging s CROSS JOIN LATERAL jsonb_array_elements(s.relations) AS rel
    WHERE s.run_id = $1
), resolved AS (
    SELECT d.type_code, d.direction, me.id AS me_id,
           CASE WHEN d.direction = $3 THEN me.id ELSE peer.id END AS src_id,
           CASE WHEN d.direction = $3 THEN peer.id ELSE me.id END AS dst_id
    FROM decl d
    JOIN cmdb_ci me ON me.model_code = d.model_code AND me.source = $2 AND me.external_id = d.external_id AND me.delete_time IS NULL
    JOIN cmdb_ci peer ON peer.model_code = d.peer_model AND peer.source = $2 AND peer.external_id = d.peer_eid AND peer.delete_time IS NULL
    JOIN cmdb_relation_type t ON t.code = d.type_code AND t.delete_time IS NULL
    WHERE me.id <> peer.id
)`

// stmtRelationsDelete 本次声明过某类型某方向关系的资源：其同源同类型该侧的其它关系删除（如虚拟机迁移主机）。
const stmtRelationsDelete = stmtRelationsCTE + `
, keys AS (SELECT DISTINCT type_code, direction, me_id FROM resolved)
, del AS (
    DELETE FROM cmdb_relation r USING keys k
    WHERE r.source = $2 AND r.type_code = k.type_code
      AND ((k.direction = $3 AND r.src_id = k.me_id) OR (k.direction <> $3 AND r.dst_id = k.me_id))
      AND NOT EXISTS (SELECT 1 FROM resolved x WHERE x.type_code = r.type_code AND x.src_id = r.src_id AND x.dst_id = r.dst_id)
    RETURNING r.type_code, r.src_id, r.dst_id
)
INSERT INTO cmdb_ci_history (id, ci_id, action, diff, operator, source, request_id, create_time)
SELECT gen_random_uuid(), h.ci_id, $4, h.diff, '', $2, $1, now()
FROM del d
CROSS JOIN LATERAL (
    SELECT d.src_id AS ci_id, jsonb_build_object('relationType', d.type_code, 'direction', $3::text,
        'target', jsonb_build_object('id', dst.id, 'name', dst.name, 'modelCode', dst.model_code)) AS diff
    FROM cmdb_ci dst WHERE dst.id = d.dst_id
    UNION ALL
    SELECT d.dst_id, jsonb_build_object('relationType', d.type_code, 'direction', $5::text,
        'target', jsonb_build_object('id', src.id, 'name', src.name, 'modelCode', src.model_code))
    FROM cmdb_ci src WHERE src.id = d.src_id
) h`

// stmtRelationSides 列出待写关系中受基数约束的端（one_to_one 两端、one_to_many 的 dst 端），
// 锁名与 core 手工建关系一致（cmdb_relation:<type>:<col>:<id>），两条写路径由此互斥。
const stmtRelationSides = stmtRelationsCTE + `
SELECT DISTINCT 'cmdb_relation:' || x.type_code || ':dst_id:' || x.dst_id::text
FROM resolved x JOIN cmdb_relation_type t ON t.code = x.type_code AND t.delete_time IS NULL
WHERE t.cardinality IN ('one_to_one', 'one_to_many')
UNION
SELECT DISTINCT 'cmdb_relation:' || x.type_code || ':src_id:' || x.src_id::text
FROM resolved x JOIN cmdb_relation_type t ON t.code = x.type_code AND t.delete_time IS NULL
WHERE t.cardinality = 'one_to_one'
ORDER BY 1`

// stmtRelationsInsert 缺失的关系插入并记双向历史。受基数约束的端已有其它关系（如手工
// 建立的）或同批重复声明时跳过该条——同步不破坏基数约束，也不因个别冲突整批失败。
const stmtRelationsInsert = stmtRelationsCTE + `
, ins AS (
    INSERT INTO cmdb_relation (id, type_code, src_id, dst_id, source, create_time)
    SELECT gen_random_uuid(), c.type_code, c.src_id, c.dst_id, $2, now()
    FROM (
        SELECT x.type_code, x.src_id, x.dst_id, t.cardinality,
               row_number() OVER (PARTITION BY x.type_code, x.dst_id ORDER BY x.src_id) AS dst_rank,
               row_number() OVER (PARTITION BY x.type_code, x.src_id ORDER BY x.dst_id) AS src_rank
        FROM (SELECT DISTINCT type_code, src_id, dst_id FROM resolved) x
        JOIN cmdb_relation_type t ON t.code = x.type_code AND t.delete_time IS NULL
    ) c
    WHERE (c.cardinality NOT IN ('one_to_one', 'one_to_many') OR (c.dst_rank = 1 AND NOT EXISTS (
              SELECT 1 FROM cmdb_relation r WHERE r.type_code = c.type_code AND r.dst_id = c.dst_id AND r.src_id <> c.src_id)))
      AND (c.cardinality <> 'one_to_one' OR (c.src_rank = 1 AND NOT EXISTS (
              SELECT 1 FROM cmdb_relation r WHERE r.type_code = c.type_code AND r.src_id = c.src_id AND r.dst_id <> c.dst_id)))
    ORDER BY c.type_code, c.src_id, c.dst_id
    ON CONFLICT (type_code, src_id, dst_id) DO NOTHING
    RETURNING type_code, src_id, dst_id
)
INSERT INTO cmdb_ci_history (id, ci_id, action, diff, operator, source, request_id, create_time)
SELECT gen_random_uuid(), h.ci_id, $4, h.diff, '', $2, $1, now()
FROM ins i
CROSS JOIN LATERAL (
    SELECT i.src_id AS ci_id, jsonb_build_object('relationType', i.type_code, 'direction', $3::text,
        'target', jsonb_build_object('id', dst.id, 'name', dst.name, 'modelCode', dst.model_code)) AS diff
    FROM cmdb_ci dst WHERE dst.id = i.dst_id
    UNION ALL
    SELECT i.dst_id, jsonb_build_object('relationType', i.type_code, 'direction', $5::text,
        'target', jsonb_build_object('id', src.id, 'name', src.name, 'modelCode', src.model_code))
    FROM cmdb_ci src WHERE src.id = i.src_id
) h`

// ReconcileRelations implements biz.ReconcileRepo（一事务：先删多余，再补缺失）。
func (r *reconcileRepo) ReconcileRelations(ctx context.Context, runID, source string) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, stmtRelationsDelete, runID, source, cmdbcontract.DirectionOutgoing, cmdbcontract.ActionUnlink, cmdbcontract.DirectionIncoming); err != nil {
			return fmt.Errorf("delete stale relations: %w", err)
		}
		// 受约束端加事务级 advisory lock（按键排序，防死锁），与 core 的手工写入串行。
		rows, err := tx.Query(ctx, stmtRelationSides, runID, source, cmdbcontract.DirectionOutgoing)
		if err != nil {
			return fmt.Errorf("list relation sides: %w", err)
		}
		sides, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return fmt.Errorf("list relation sides: %w", err)
		}
		for _, side := range sides {
			if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", dbx.LockID(side)); err != nil {
				return fmt.Errorf("lock relation side: %w", err)
			}
		}
		if _, err := tx.Exec(ctx, stmtRelationsInsert, runID, source, cmdbcontract.DirectionOutgoing, cmdbcontract.ActionLink, cmdbcontract.DirectionIncoming); err != nil {
			return fmt.Errorf("insert relations: %w", err)
		}
		return nil
	})
}
